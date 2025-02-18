# Part 1: Vision

## Framing the Problem

Estuary collections have schemas: a contract that details what valid
documents of the collection may or may not look like.
The platform enforces this contract whenever documents are written or read.
When teams first create a data flow, there’s a single understanding of schema.
Everybody agrees on it, data flows along, and stakeholders are happy.

This situation typically doesn’t last long.

Soon enough new requirements impact what data is represented or structured,
and rolling out changes to various data flows requires some kind of coordination.
The _kind_ of coordination depends on key questions: which schema, and who owns it?

### Source-Defined Schema

Most captures in Estuary are considered “source defined”.
There is an upstream system or application which has a strong opinion of schema.
The capture’s job is to follow along, updating collection schemas
to reflect the evolving understanding of schema from the source.

Some source systems, such as databases, are able to tell the capture of
schema changes as they happen.
Other systems cannot, and the platform can only infer a useful schema
from the actual documents which it encounters as the capture progresses.

### Target-Defined Schema

Other data flows are "target defined". Here, a downstream system is authoritative
and source documents must be rejected if they’re invalid with respect to its schema.
For example, you might configure an Estuary HTTP Ingest capture to reject WebHook
requests which are invalid with respect to the destination collection schema.

### A Matter of Perspective

Any data process that _uses_ a source-defined schema for analysis or transformation
must necessarily place some expectations on that source
-- at least over the portions it interacts with --
which effectively reinterpret the source as being target-defined.
A `SELECT` query, for example, is an implicit declaration that a set of table columns ought to exist.
If they don't, or if their types or meaning have changed,
your query or DBT model will fail and you must fix it.
Or, worse, it will appear to work but produce incorrect data.

In any meaningful data flow, there exists the potential for
its components to disagree with respect to schema.
Tools in the data integrations space have historically ignored this concern,
choosing to propagate source schema changes downstream no matter what.
This approach just pushes the problem down onto applications or DBT models
which are ill-equipped to rectify or even detect the mismatch.
Robust data pipelines require that owners have tools to detect and resolve
disagreements early in the flow of data,
before "bad" data is propagated into downstream systems.


### A Matter of History

Database Change Data Capture (CDC) leverages the database write-ahead log to extract changes.
While a database table can tell you its current schema,
CDC is concerned with historical events
-- transactions that have committed --
and a curiosity of CDC is that events being processed may have happened
under a _different_ table schema than the one which exists now.
Such events are inexorably part of the table's history,
but are invalid with respect to its current schema,
and CDC tooling must account for this drift.

A related design goal of Estuary collections is that they’re a durable
record of change events which are captured once and may then be read many times.
Collection schema could change over time, but collections must still
usefully represent change events captured long ago under a much different schema.


## Key Platform Features

The Estuary platform has several features to help manage
the domain concerns of source- vs target-defined schema and schema history.


### Write vs Read Schema

Estuary collections have distinct write and read schemas.

A collection write schema constrains new documents being added by a capture or derivation task.
If a task produces an invalid document,
the task is halted rather than allowing the invalid document in to the collection.
To resolve this condition,
either the write schema is updated to permit the document or data is fixed in the source system.

Collection read schemas constrain the existing documents of a collection,
which are being read by a derivation or materialization task.
If a task encounters a collection document which is invalid to its read schema, the task is halted.
Resolution requires updating the read schema to allow the invalid document,
which is also an opportunity to inspect and correct the assumptions of the downstream flow
before it processes data that could violate those assumptions.

Write and read schemas may change at any time and are largely independent of one another.
Each may be very restrictive, or very permissive, as is required by the use case.
In most cases,
schemas are managed by the platform and are a low level building block
for high level coordination automation.


### Auto Discovery

Estuary capture tasks periodically query source systems for current schemas,
and apply those schemas as updates to the write schema of bound collections.


### Auto Inference

Every Estuary collection has an automatically inferred schema,
which is the most-restrictive schema that is valid for all
source-defined schemas encountered over the collection's lifecycle.

Conceptually, an inferred schema is like shrink wrap:
it's exactly the right size to accommodate and enclose all of the collection's historical schemas to-date.
As new schemas are encountered, the platform will enlarge or "widen"
the inferred schema as necessary to fit them.

Where possible, capture connectors inform the platform of updates to
source-defined schemas as they're observed within the source system.
In other cases, an upstream application dictates the schema and
and the capture source system doesn't have a known schema available
(typical with MongoDB, for example).
The platform will additionally widen the inferred schema
as is required to accept every document actually written to the collection.

Inferred schemas do nothing on their own,
but are frequently used within a collection's read schema.

:::note

Auto Discovery is the primary mechanism by which a collection's write schema is updated,
and Auto Inference is the mechanism by which a collections's read schema is updated.

:::


### Column Migrations

The inferred column types of a collection commonly widen over time:
a field which was previously an integer may become numeric,
or a native float might become an arbitrary-precision number.
Wherever possible, Estuary materializations perform automatic migrations of
such widened types through an in-place cast of the existing column.

Materializations never "narrow" column types,
which would require a cast that could fail
(from a number to an integer, for example).
Narrowing the type of an existing column requires a backfill of the materialization binding.
Auto Inference only widens and never narrows the collection schema,
and such backfills can only ever be required due to a user-initiated change.


### Collection Reset

At times the intended structure of a collection has changed so dramatically that
it's necessary to start over
rather than updating the existing collection in-place.
Estuary collections offer a "reset" capability,
which is semantically identical to deleting and then re-creating the collection,
but expressed as a single operation.

When a collection is reset,
it's inferred schema is re-initialized and all historical data is logically dropped
(but remains in its storage bucket).
The configured collection key and any logical partitions may change during a reset.

Any capture or materialization tasks which bind the reset collection
as a target or source will automatically perform a backfill.

::: note

If a task binding is currently disabled,
an automatic backfill will occur in the future should it be re-enabled.
A backfill will similarly occur if the re-enabled collection
was manually deleted and then re-created, as separate operations.

:::


### Integrity Checks

The Estuary control-plane ensures that:

- The types of keyed locations (collection keys, and partition keys) are consistent between the write and read schema.
  - Similarly, that the types of keyed locations do not change once the collection is established.
  - _If either of these checks fail, the only recourse is to reset the collection._
- Materialized field types do not "narrow" in incompatible ways
  - _If they do, the recourse is to backfill the materialization binding, or to reset the collection._
  - Fields marked as "required" must be present, which is a light weight form of target-defined schema.
  - Similarly, derivations are type-checked over the fields they use.
- Advanced: users update collection read schemas with required assertions over source-defined schemas.


### Automated Handling of Failed Checks

If the key or types of a collection change,
its capture may be configured to automatically reset it.
This is driven by the Auto Discovery process.

If a materialization has an incompatible column,
its derivation may be configured to automatically backfill it.

::: note

_Should_ we be? If we handle all widening cases,
and narrowing can only be user-initiated,
shouldn't we require that they explicitly backfill?

:::


# Part 2: Implementation

### Capture protocol: SourcedSchema

We'd add an extension of the capture protocol:

```
  // SourcedSchema notifies the runtime of a source-defined schema of the
  // indicated binding. It's not required that the connector know that the
  // schema has actually changed since a last SourcedSchema. It's encouraged
  // for connectors to emit SourcedSchema liberally, such as on startup,
  // or periodically, or upon encountering a previously unseen column.
  //
  // SourcedSchema may be a partial schema: it may schematize some
  // specific field(s) and not others that are in active use.
  //
  // SourcedSchema should be maximally restrictive. It should disallow
  // `types` and `additionalProperties` which are not explicitly being
  // schematized. The platform will union a SourcedSchema with all other
  // SourcedSchema messages of the binding, as well as additional inference
  // updates required to fit Captured documents.
  //
  // SourcedSchema is transactional. It may be interleaved with zero or more
  // Captured documents, and multiple SourcedSchema messages may be emitted
  // for a single binding, but an emitted SourcedSchema has no effect until
  // it's followed by a Checkpoint.
  message SourcedSchema {
    // Index of the Open binding for which the schema applies.
    uint32 binding = 1;
    // JSON-Schema document.
    string schema_json = 2 [
        (gogoproto.casttype) = "encoding/json.RawMessage",
        json_name = "schema"
    ];
  }
```

- `SourcedSchema` is applied alongside `Captured` documents to widen the current schema.
- Its effects are logged out only after draining the current combiner.
- We may want to intersect its `doc::Shape` with the write schema:
  - Otherwise, we may infer impossible updates that cannot reflect written documents,
    because those writes would cause a schema violation.
  - Consider what happens if `SourcedSchema` says a key type has changed.
- Given a correct `SourcedSchema`, use cases like vector embeddings should "Just Work"
  - `{minItems: 1536, maxItems: 1536}` constraints aren't modified if documents have 1536 items.


### Projection.write_inference

This field **does not** have a functional role in the SQL inference effort.
Instead, `SourcedSchema` would provide an explicit type drawn from the DB itself.

Even given _zero_ actual rows,
if a capture connector can tail schema changes of the source,
we can quickly propagate those changes to correct materialized column types.


### flow://relaxed-write-schema

Inferred collections have historically used:
```
allOf:
 - $ref: flow://write-schema
 - $ref: flow://inferred-schema
```

This doesn't work for schema-full systems like PostreSQL CDC,
because the write schema is **not** a strict subset of every
historical source schema, as happens to be the case with (say) MongoDB.
Yet, we still want write schemas to be a comprehensive schema representing
the current database write contract.
We should _not_, for example, strip away all non-key fields.

We **do** require certain JSON schema keywords which, today, are in the write schema:
  - `title` / `description`
  - `reduce`
  - `default`
  - Conditional validation keywords (`if`, `then`, `else`)
  - Maybe: `secret`, `contentEncoding`, etc.

Some of these keywords could potentially be conveyed via `SourceSchema`
and projected into the `flow://inferred-schema`.
But not all of them:
Conditional validation keywords, in particular,
cannot survive projection into `doc::Shape` and back.

Ergo, we'll need _some_ other schema having these keywords
which is composed with `flow://inferred-schema` to build a read schema.
We could update the capture protocol to have the connector emit such a schema.
However this is work, and an observation is that
this would essentially be the current write schema,
but with validation keywords removed which might conflict with the inferred schema.
This leaves `flow://relaxed-write-schema`,
a "special" schema which resolves to
  - The write schema if there is no inferred schema available, or
  - The write schema with _most_ validations (`type`, `format`, `required`, etc) recursively removed.

It's ... distasteful, but it also appears to work well in practice,
resulting in essentially the same schema as a connector might produce.

Another concern is the brittleness of taking a hack-saw to connector
validation keywords and hoping to not break things.
I don't think this concern holds much merit, however,
because use of `flow://relaxed-write-schema`
implies that the connector _opted in_ to such use,
making it the connector's responsibility
that `Discovered` schemas work with it in practice.


### Evolution ID


Evolution IDs (better name TBD?) are a lifecycle identifier for
a task or collection which changes whenever that task or collection
is deleted and recreated or is reset.
It allows the control plane to reconcile
whether a state is "before reset" and "after reset".

Evolution IDs already exist, but have been un-named to date.
They exist as a "busting" identifier which is suffixed onto
a collection's journal name template and/or task's shard ID template.

We would further surface this identifier so that it can be inspected
and compared when evaluating the before / after state of a
task binding or transform.
Notably, bindings would examine the evolution ID to understand
if a backfill is required.


### Tracking inactive bindings / transforms

This change was already motivated out of a desire for better
re-initialization of materialized tables.

We need to track inactive bindings & transforms which have been active in the past,
for each task.
These would be keyed on the resource path of each binding.
Their retention is partially motivated by the
need to compare the Evolution ID of the source / target collection
to understand whether a backfill is required.


### Resource Path Pointers

All materialization connectors must surface resource path pointers
in their Spec response.
We already do this for captures,
and need to bring materializations to parity.

The `validation` crate must have these available ... somehow ...
so that it can match a current binding model
to its last active or inactive binding specification.