---
sidebar_position: 6
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import Mermaid from '@theme/Mermaid';

# Derivations

A derivation is a [collection](../#collections)
that results from transformations applied to one or more other collections.

Derivations derive data continuously,
reflecting updates to the source collections as they happen.

A derivation has two main components:

* The new collection, which stores the derived data.
* A [catalog task](../#tasks), which applies the transformations to source documents as they become available.

The derivation task is defined by:

* The **transformations** it applies
* Its **register**, which serves as its internal memory
* In many cases, **lambdas**, functions defined in accompanying TypeScript modules
that allow more complex transformations.

![](<derivations-new.svg>)

## Creating derivations

You can create a derivation in your local development environment using flowctl.

Use [`flowctl draft` to begin work with a draft](./flowctl.md#working-with-catalog-drafts),
and manually add [a derivation to the Flow specification file](#specification).

If necessary, [generate a typescript file](#creating-typescript-modules) and define lambda functions there.

## Specification

A derivation is specified as a regular collection with an additional `derivation` stanza:

```yaml
collections:
  # The unique name of the derivation.
  acmeCo/my/derivation:
    schema: my-schema.yaml
    key: [/key]

    # Presence of a `derivation` stanza makes this collection a derivation.
    # Type: object
    derivation:

      # Register definition of the derivation.
      # If not provided, registers have an unconstrained schema
      # and initialize to the `null` value.
      # Optional, type: object
      register:

        # JSON Schema of register documents. As with collection schemas,
        # this is either an inline definition or a relative URL reference.
        # Required, type: string (relative URL form) or object (inline form)
        schema:
          type: integer

        # Initial value taken by a register which has never been updated before.
        # Optional, default: null
        initial: 0

      # TypeScript module that implements any lambda functions invoked by this derivation.
      # Optional, type: object
      typescript:

        # TypeScript module implementing this derivation.
        # Module is either a relative URL of a TypeScript module file (recommended),
        # or an inline representation of a TypeScript module.
        # The file specified will be created when you run `flowctl typescript generate`
        module: acmeModule.ts

        # NPM package dependencies of the module
        # Version strings can take any form understood by NPM.
        # See https://docs.npmjs.com/files/package.json#dependencies
        npmDependencies: {}

      # Transformations of the derivation,
      # specified as a map of named transformations.
      transform:

        # Unique name of the transformation, containing only Unicode
        # Letters and Numbers (no spaces or punctuation).
        myTransformName:

          # Source collection read by this transformation.
          # Required, type: object
          source:
            # Name of the collection to be read.
            # Required.
            name: acmeCo/my/source/collection
            # Partition selector of the source collection.
            # Optional. Default is to read all partitions.
            partitions: {}

          # Delay applied to sourced documents before being processed
          # by this transformation.
          # Default: No delay, pattern: ^\\d+(s|m|h)$
          readDelay: "48h"

          # Shuffle determines the key by which source documents are
          # shuffled (mapped) to a register.
          # Optional, type: object.
          # If not provided, documents are shuffled on the source collection key.
          shuffle:
            # Key is a composite key which is extracted from documents
            # of the source.
            key: [/shuffle/key/one, /shuffle/key/two]

          # Update lambda of the transformation.
          # Optional, type: object
          update: {lambda: typescript}

          # Publish lambda of the transformation.
          # Optional, type: object
          publish: {lambda: typescript}

          # Priority applied to processing documents of this transformation
          # relative to other transformations of the derivation.
          # Default: 0, integer >= 0
          priority: 0

```

## Background

The following sections will refer to the following common example
to illustrate concepts.

Suppose you have an application through which users send one another
some amount of currency, like in-game tokens or dollars or digital kittens:

<Tabs>
<TabItem value="transfers.flow.yaml" default>

```yaml file=./bank/transfers.flow.yaml
```

</TabItem>
<TabItem value="transfers.schema.yaml" default>

```yaml file=./bank/transfers.schema.yaml
```

</TabItem>
</Tabs>

There are many views over this data that you might require,
such as summaries of sender or receiver activity,
or current account balances within your application.

## Transformations

A transformation binds a [source](#sources) collection to a derivation.
As documents of the source collection arrive,
the transformation processes the document
to [publish](#publish-lambdas) new documents,
[update](#update-lambdas) a
[register](#registers),
or both.

Read source documents are [shuffled](#shuffles) on a **shuffle key** to
co-locate the processing of documents that have equal shuffle keys.
The transformation then processes documents by invoking **lambdas**:
user-defined functions that accept documents as arguments
and return documents in response.

A derivation may have many transformations,
and each transformation has a long-lived and stable name.
Each transformation independently reads documents from its
source collection and tracks its own read progress.
More than one transformation can read from the same source collection,
and transformations may also source from their own derivation,
enabling cyclic data-flows and graph algorithms.

Transformations may be added to or removed from a derivation at any time.
This makes it possible to, for example, add a new collection into an
existing multi-way join, or gracefully migrate to a new source
collection without incurring downtime.
However, renaming a running transformation is not possible.
If attempted, the old transformation is dropped and
a new transformation under the new name is created,
which begins reading its source collection all over again.

<Mermaid chart={`
	graph LR;
    d[Derivation];
    t[Transformation];
    r[Registers];
    p[Publish λ];
    u[Update λ];
    c[Sourced Collection];
    d-- has many -->t;
    t-- reads from -->c;
    t-- invokes -->u;
    t-- invokes -->p;
    u-- updates -->r;
    r-- reads -->p;
    d-- indexes -->r;
`}/>

## Sources

The **source** of a transformation is a collection.
As documents are published into the source collection,
they are continuously read and processed by the transformation.

A [partition selector](./advanced/projections.md#partition-selectors) may be provided
to process only a subset of the source collection's logical partitions.
Selectors are efficient: only partitions that match the selector are read,
and Flow can cheaply skip over partitions that don't.

Derivations re-validate their source documents against
the source collection's schema as they are read.
This is because collection schemas may evolve over time,
and could have inadvertently become incompatible with
historical documents of the source collection.
Upon a schema error, the derivation will pause and
give you an opportunity to correct the problem.

You may also provide an alternative **source schema**.
Source schemas aide in processing third-party sources
of data that you don't control,
which can have unexpected schema changes without notice.
You may want to capture this data
with a minimal and very permissive schema.
Then, a derivation can apply a significantly stricter source schema,
which verifies your current expectations of what the data _should_ be.
If those expectations turn out to be wrong,
little harm is done:
your derivation is paused but the capture continues to run.
You must simply update your transformations
to account for the upstream changes and then continue without any data loss.

## Shuffles

As each source document is read, it's shuffled — or equivalently, mapped —
on an extracted key.

If you're familiar with data shuffles in tools like MapReduce, Apache Spark,
or Flink, the concept is very similar.
Flow catalog tasks scale to run across many machines at the same time,
where each machine processes a subset of source documents.
Shuffles let Flow know how to group documents so that they're co-located,
which can increase processing efficiency and reduce data volumes.
They are also used to map source documents to [registers](#registers).

<Mermaid chart={`
	graph LR;
    subgraph s1 [Source Partitions]
      p1>acmeBank\/transfers\/part-1];
      p2>acmeBank\/transfers\/part-2];
    end
    subgraph s2 [Derivation Task Shards]
      t1([task\/shard-1]);
      t2([task\/shard-2]);
    end
    p1-- sender: alice -->t1;
    p1-- sender: bob -->t2;
    p2-- sender: alice -->t1;
    p2-- sender: bob -->t2;
`}/>

If you don't provide a shuffle key,
Flow will shuffle on the source collection key,
which is typically what you want.

If a derivation has more than one transformation,
the shuffle keys of all transformations must align with one another
in terms of the extracted key types (string or integer)
as well as the number of components in a composite key.
For example, one transformation couldn't shuffle transfers on `[/id]`
while another shuffles on `[/sender]`, because `sender` is a string and
`id` an integer.
Similarly mixing a shuffle of `[/sender]` alongside `[/sender, /recipient]`
is prohibited because the keys have different numbers of components.
However, one transformation _can_ shuffle on `[/sender]`
while another shuffles on `[/recipient]`,
as in the examples below.

## Registers

Registers are the internal _memory_ of a derivation.
They are a building block that enable derivations to tackle advanced stateful
streaming computations like multi-way joins, windowing, and transaction processing.
As we've already seen, not all derivations require registers,
but they are essential for a variety of important use cases.

Each register is a document with a user-defined
[schema](schemas.md).
Registers are keyed, and every derivation maintains an index of keys
and their corresponding register documents.
Every source document is mapped to a specific register document
through its extracted [shuffle key](#shuffles).

For example, when shuffling `acmeBank/transfers` on `[/sender]` or `[/recipient]`,
each account ("alice", "bob", or "carol") is allocated its own register.
You might use that register to track a current account balance
given the received inflows and sent outflows of each account.

If you instead shuffle on `[/sender, /recipient]`, each
pair of accounts ("alice -> bob", "alice -> carol", "bob -> carol")
is allocated a register.

Transformations of a derivation may have different shuffle keys,
but the number of key components and their JSON types must agree.
Two transformations could map on [/sender] and [/recipient],
but not [/sender] and [/recipient, /sender].

Registers are best suited for relatively small,
fast-changing documents that are shared within and across
the transformations of a derivation.
The number of registers indexed within a derivation may be very large,
and if a register has never before been used,
it starts with a user-defined initial value.
From there, registers may be modified through an **update lambda**.

:::info
Under the hood, registers are backed by replicated,
embedded RocksDB instances, which co-locate
with the lambda execution contexts that Flow manages.
As contexts are assigned and re-assigned,
their register databases travel with them.

If any single RocksDB instance becomes too large,
Flow is able to perform an online **split**,
which subdivides its contents into two new databases
 — and paired execution contexts — which are re-assigned to other machines.
:::

## Lambdas

Lambdas are user-defined functions that are invoked by derivations.
They accept documents as arguments
and return transformed documents in response.
Lambdas can be used to update registers, publish documents into a derived collection,
or compute a non-trivial shuffle key of a document.

:::info Beta
The ability for lambdas to compute a document's shuffle key is coming soon.
:::

Flow supports TypeScript lambdas, which you define in an accompanying TypeScript module
and reference in a derivation's `typescript` stanzas.
See the [derivation specification](#specification) and [Creating TypeScript modules](#creating-typescript-modules) for more details on how to get started.
TypeScript lambdas are "serverless"; Flow manages the execution and scaling of your Lambda on your behalf.

Alternatively, Flow also supports [remote lambdas](#remote-lambdas), which invoke an HTTP endpoint you provide,
such as an AWS Lambda or Google Cloud Run function.

In terms of the MapReduce functional programming paradigm,
Flow lambdas are mappers,
which map documents into new user-defined shapes.
Reductions are implemented by Flow
using the [reduction annotations](./schemas.md#reduce-annotations) of your collection schemas.

### Publish lambdas

A **publish** lambda publishes documents into the derived collection.

To illustrate first with an example,
suppose you must know the last transfer
from each sender that was over $100:

<Tabs>
<TabItem value="last-large-send.flow.yaml" default>

```yaml file=./bank/last-large-send.flow.yaml
```

</TabItem>
<TabItem value="last-large-send.ts" default>

```typescript file=./bank/last-large-send.ts
```

</TabItem>
<TabItem value="last-large-send-test.flow.yaml" default>

```yaml file=./bank/last-large-send-test.flow.yaml
```

</TabItem>
</Tabs>

This transformation defines a TypeScript **publish** lambda,
which is implemented in an accompanying TypeScript module.
The lambda is invoked as each source transfer document arrives.
It is given the `source` document,
and also includes the a `_register` and `_previous` register, which are not used here.
The lambda outputs zero or more documents,
each of which must conform to the derivation's schema.

As this derivation's collection is keyed on `/sender`,
the last published document (the last large transfer) of each sender is retained.
If it were instead keyed on `/id`,
then _all_ transfers with large amounts would be retained.
In SQL terms, the collection key acts as a GROUP BY.


***

Derivation collection schemas may have
[reduction](schemas.md#reductions) annotations,
and publish lambdas can be combined with reductions in interesting ways.

You may be familiar with `map` and `reduce` functions
built into languages like
[Python](https://book.pythontips.com/en/latest/map_filter.html),
[JavaScript](https://www.freecodecamp.org/news/javascript-map-reduce-and-filter-explained-with-examples/);
and many others,
or have used tools like MapReduce or Spark.
In functional terms, lambdas you write within Flow are "mappers,"
and reductions are always done
by the Flow runtime using your schema annotations.

Suppose you need to know the running
[account balances](https://en.wikipedia.org/wiki/Double-entry_bookkeeping)
of your users given all of their transfers thus far.
Tackle this by _reducing_ the final account balance
for each user from all of the credit and debit amounts of their transfers:

<Tabs>
<TabItem value="balances.flow.yaml" default>

```yaml file=./bank/balances.flow.yaml
```

</TabItem>
<TabItem value="balances.ts" default>

```typescript file=./bank/balances.ts
```

</TabItem>
<TabItem value="balances-test.flow.yaml" default>

```yaml file=./bank/balances-test.flow.yaml
```

</TabItem>
</Tabs>

### Update lambdas

An **update** lambda transforms a source document
into an update of the source document's register.

To again illustrate through an example,
suppose your compliance department wants you to flag
the first transfer a sender sends to a new recipient.
You achieve this by shuffling on pairs of
`[/sender, /recipient]` and using a register
to track whether this account pair has been seen before:

<Tabs>
<TabItem value="first-send.flow.yaml" default>

```yaml file=./bank/first-send.flow.yaml
```

</TabItem>
<TabItem value="first-send.ts" default>

```typescript file=./bank/first-send.ts
```

</TabItem>
<TabItem value="first-send-test.flow.yaml" default>

```yaml file=./bank/first-send-test.flow.yaml
```

</TabItem>
</Tabs>

This transformation uses both a publish and an **update** lambda,
implemented in an accompanying TypeScript module.
The update lambda is invoked first for each `source` document,
and it returns zero or more documents,
which each must conform to the derivation's register schema
(in this case, a simple boolean).

The **publish** lambda is invoked next, and is given the `source`
document as well as the _before_ (`previous`) and _after_ (`_register`)
values of the updated register.
In this case, we don't need the _after_ value:
our update lambda implementation implies that it's always `true`.
The _before_ value, however,
tells us whether this was the very first update of this register,
and by implication was the first transfer for this pair of accounts.

<Mermaid chart={`
  sequenceDiagram
    autonumber
    Derivation->>Update λ: update({sender: alice, recipient: bob})?
    Update λ-->>Derivation: return "true"
    Derivation->>Registers: lookup(key = [alice, bob])?
    Registers-->>Derivation: not found, initialize as "false"
    Derivation-->>Derivation: Register: "false" => "true"
    Derivation-)Registers: store(key = [alice, bob], value = "true")
    Derivation->>Publish λ: publish({sender: alice, recipient: bob}, register = "true", previous = "false")?
    Publish λ-->>Derivation: return {sender: alice, recipient: bob}
`}/>

:::info FAQ
> Why not have one lambda that can return
> a register update _and_ derived documents?

**Performance.**
_Update_ and _publish_ are designed to be
parallelized and pipelined over many source documents simultaneously,
while still giving the appearance and correctness of lambdas
are invoked in strict serial order.
Notice that (1) above doesn't depend on actually knowing the register
value, which doesn't happen until (4).
Many calls like (1) can also happen in parallel,
so long as their applications to the register value (5)
happen in the correct order.
In comparison, a single-lambda design
would require Flow to await each invocation
before it can begin the next.
:::

***

Register schemas may also have
[reduction](schemas.md#reductions) annotations,
and documents returned by update lambdas
are _reduced_ into the current register value.

The compliance department reached out again,
and this time they need you to identify transfers where
the sender's account had insufficient funds.

You manage this by tracking
the running credits and debits of each account in a register.
Then, you enrich each transfer with the account's current balance
and whether the account was overdrawn:

<Tabs>
<TabItem value="flagged-transfers.flow.yaml" default>

```yaml file=./bank/flagged-transfers.flow.yaml
```

</TabItem>
<TabItem value="flagged-transfers.ts" default>

```typescript file=./bank/flagged-transfers.ts
```

</TabItem>
<TabItem value="flagged-transfers-test.flow.yaml" default>

```yaml file=./bank/flagged-transfers-test.flow.yaml
```

</TabItem>
</Tabs>

Source transfers are read twice.
The first read shuffles on `/recipient`
to track account credits,
and the second shuffles on `/sender`
to track account debits and to publish enriched transfer events.
Update lambdas return the amount of credit or debit,
and these amounts are summed
into a derivation register keyed on the account.

<Mermaid chart={`
  sequenceDiagram
    autonumber
    Derivation->>Registers: lookup(key = alice)?
    Registers-->>Derivation: not found, initialize as 0
    Derivation->>Update λ: update({recipient: alice, amount: 50, ...})?
    Update λ-->>Derivation: return +50
    Derivation->>Update λ: update({sender: alice, amount: 75, ...})?
    Update λ-->>Derivation: return -75
    Derivation-->>Derivation: Register: 0 + 50 => 50
    Derivation-->>Derivation: Register: 50 - 75 => -25
    Derivation->>Publish λ: publish({sender: alice, amount: 75, ...}, register = -25, previous = 50)?
    Publish λ-->>Derivation: return {sender: alice, amount: 75, balance: -25, overdrawn: true}
`}/>

### Creating TypeScript modules

To create a new TypeScript module for the lambdas of your derivation,
you can use `flowctl typescript generate` to generate it.
In the derivation specification, choose the name for the new module and
run `flowctl typescript generate`.
Flow creates a module with the name you specified, stubs of the required interfaces,
and TypeScript types that match your schemas.
Update the module with your lambda function bodies,
and proceed to test and deploy your catalog.

Using the example below, `flowctl typescript generate --source=acmeBank.flow.yaml` will generate the stubbed-out acmeBank.ts.

<Tabs>
<TabItem value="acmeBank.flow.yaml" default>

```yaml
collections:
  acmeBank/balances:
    schema: balances.schema.yaml
    key: [/account]

    derivation:
      typescript:
        module: acmeBank.ts
      transform:
        fromTransfers:
          source: { name: acmeBank/transfers }
          publish: { lambda: typescript }
```

</TabItem>
<TabItem value="acmeBank.ts (generated stub)" default>

```typescript
import { IDerivation, Document, Register, FromTransfersSource } from 'flow/acmeBank/balances';

// Implementation for derivation examples/acmeBank.flow.yaml#/collections/acmeBank~1balances/derivation.
export class Derivation implements IDerivation {
     fromTransfersPublish(
        _source: FromTransfersSource,
        _register: Register,
        _previous: Register,
    ): Document[] {
        throw new Error("Not implemented");
    }
}
```

</TabItem>
</Tabs>


[Learn more about TypeScript generation](flowctl.md#typescript-generation)

### NPM dependencies

Your TypeScript modules may depend on other
[NPM packages](https://www.npmjs.com/),
which can be be imported through the `npmDependencies`
stanza of the [derivation spec](#specification).
For example, [moment](https://momentjs.com/) is a common library
for working with times:

<Tabs>
<TabItem value="derivation.flow.yaml" default>

```yaml
derivation:
  typescript:
    module: first-send.ts
    npmDependencies:
      moment: "^2.24"
  transform: { ... }
```

</TabItem>
<TabItem value="first-send.ts" default>

```typescript
import * as moment from 'moment';

// ... use `moment` as per usual.
```

</TabItem>
</Tabs>

Use any version string understood by `package.json`,
which can include local packages, GitHub repository commits, and more.
See [package.json documentation](https://docs.npmjs.com/cli/v8/configuring-npm/package-json#dependencies).

During the catalog build process, Flow gathers NPM dependencies
across all Flow specification files and patches them into the catalog's
managed `package.json`.
Flow organizes its generated TypeScript project structure
for a seamless editing experience out of the box with VS Code
and other common editors.

### Remote lambdas

A remote Lambda is one that you implement and host yourself as a web-accessible endpoint,
typically via a service like [AWS Lambda](https://aws.amazon.com/lambda/) or [Google Cloud Run](https://cloud.google.com/run).
Flow will invoke your remote Lambda as needed,
POST-ing JSON documents to process and expecting JSON documents in the response.

## Processing order

Derivations may have multiple transformations that simultaneously read from
different source collections, or even multiple transformations that read
from the same source collection.

Roughly speaking, the derivation will globally process transformations and
their source documents in the time-based order in which the source documents
were originally written to their source collections.
This means that a derivation started a month ago
and a new copy of the derivation started today,
will process documents in the same order and arrive at the same result.
Derivations are **repeatable**.

More precisely, processing order is stable for each individual shuffle key,
though different shuffle keys may process in different orders if more than
one task shard is used.

Processing order can be attenuated through a **read delay**
or differing transformation **priority**.

## Read delay

A transformation can define a read delay, which will hold back the processing
of its source documents until the time delay condition is met.
For example, a read delay of 15 minutes would mean that a source document
cannot be processed until it was published at least 15 minutes ago.
If the derivation is working through a historical backlog of source documents,
than a delayed transformation will respect its ordering delay relative
to the publishing times of other historical documents also being read.

Event-driven workflows are a great fit for reacting to events as they occur,
but aren’t terribly good at taking action when something _hasn’t_ happened:

> * A user adds a product to their cart, but then doesn’t complete a purchase.
> * A temperature sensor stops producing its expected, periodic measurements.

A common pattern for tackling these workflows in Flow is to
read a source collection without a delay and update a register.
Then, read a collection with a read delay
and determine whether the desired action has happened or not.
For example, source from a collection of sensor readings
and index the last timestamp of each sensor in a register.
Then, source the same collection again with a read delay:
if the register timestamp isn't more recent
than the delayed source reading,
the sensor failed to produce a measurement.

Flow read delays are very efficient and scale better
than managing very large numbers of fine-grain timers.

[Learn more from the Citi Bike idle bikes example](https://github.com/estuary/flow/blob/master/examples/citi-bike/idle-bikes.flow.yaml)

## Read priority

Sometimes it's necessary for _all_ documents of a source collection
to be processed by a transformation before _any_ documents of some
other source collection are processed, regardless of their
relative publishing time.
For example, a collection may have corrections that should be
applied before the historical data of another collection
is re-processed.

Transformation priorities allow you to express the relative
processing priority of a derivation's various transformations.
When priorities are not equal, _all_ available source documents
of a higher-priority transformation
are processed before _any_ source documents
of a lower-priority transformation.

## Where to accumulate?

When you build a derived collection, you must choose where **accumulation** will happen:
whether Flow will reduce into documents held within
your materialization endpoint, or within the derivation's registers.
These two approaches can produce equivalent results,
but they do so in very different ways.

### Accumulate in your database

To accumulate in your materialization endpoint, such as a database,
you define a derivation with a reducible schema
and use only `publish` lambdas and no registers.
The Flow runtime uses your reduction annotations
to combine published documents, which are written to the collection.
It then fully reduces collection documents into the values
stored in the database.
This keeps the materialized table up to date.

A key insight is that the database is
the _only_ stateful system in this scenario,
and Flow uses reductions in two steps:

1. To combine many published documents into intermediate **delta documents**,
   which are the documents written to collection storage.
2. To reduce delta states into the final database-stored document.

For example, consider a collection that’s summing a value:

| Time | DB    | Lambdas           | Derived Document |
| ---- | ----- | ----------------- | ---------------- |
| T0   | **0** | publish(2, 1, 2)  | **5**            |
| T1   | **5** | publish(-2, 1)    | **-1**           |
| T2   | **4** | publish(3, -2, 1) | **2**            |
| T3   | **6** | publish()         |                  |

This works especially well when materializing into a transactional database.
Flow couples its processing transactions with corresponding database transactions,
ensuring end-to-end “exactly once” semantics.

When materializing into a non-transactional store,
Flow is only able to provide weaker “at least once” semantics;
it’s possible that a document may be combined into a database value more than once.
Whether that’s a concern depends a bit on the task at hand.
Some reductions can be applied repeatedly without changing the result (they're "idempotent"),
while in other use cases approximations are acceptable.
For the summing example above,
"at-least-once" semantics could give an incorrect result.

### Accumulate in registers

To accumulate in registers,
you use a derivation that defines a reducible register schema
that's updated through **update** lambdas.
The Flow runtime allocates, manages, and scales durable storage for registers; you don’t have to.
Then you use **publish** lambdas to publish a snapshot of your register value into your collection.

Returning to our summing example:

| Time | Register | Lambdas                             | Derived Document |
| ---- | -------- | ----------------------------------- | ---------------- |
| T0   | **0**    | update(2, 1, 2), publish(register)  | **5**            |
| T1   | **5**    | update(-2, 1), publish(register)    | **4**            |
| T2   | **4**    | update(3, -2, 1), publish(register) | **6**            |
| T3   | **6**    | update()                            |                  |

Register derivations are a great solution for materializations
into non-transactional stores
because the documents they produce
can be applied multiple times without breaking correctness.

They’re also well-suited for materializations into endpoints that aren't stateful,
such as pub/sub systems or Webhooks,
because they can produce fully reduced values as stand-alone updates.

[Learn more in the derivation pattern examples of Flow's repository](
  https://github.com/estuary/flow/tree/master/examples/derive-patterns
)