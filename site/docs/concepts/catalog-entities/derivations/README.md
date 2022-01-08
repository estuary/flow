import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import Mermaid from '@theme/Mermaid';

# Derivations

A derivation is a [collection](../collections.md)
which continuously derives its documents
from transformations that are applied to one or more other source collections.
It defines both the collection of derived documents,
and also a [catalog task](../../#tasks)
which processes documents of source collections as they become available,
transforming them into updates of the derived collection.

In addition to their collection,
derivations are defined by their **transformations** and **registers**.

![](<derivations.svg>)

## Specification

Derivations are specified as a regular collection which has an additional `derivation` stanza:

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
            # JSON Schema to validate against the source collection.
            # If not set, the schema of the source collection is used.
            # Optional, type: string (relative URL form) or object (inline form)
            schema: {}
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

For the sake of explanations in the following sections,
suppose you have an application where users send one another
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
or current account balances
within your application.

The following sections will refer to this example when discussing concepts.

## Transformations

A transformation binds a **source** collection to a derivation.
As documents of the source collection arrive,
the transformation processes the document
to derive new documents, or update a register, or both.

Read source documents are _shuffled_ on a **shuffle key** to
co-locate the processing of documents that have equal shuffle keys.
The transformation then processes documents by invoking **lambdas**:
user-defined functions which accept documents as arguments
and return documents in response.

A derivation may have many transformations,
and each transformation has a long-lived and stable name.
Each transformation independently reads documents from its
source collection and tracks its own read progress.
More than one transformation can read from the same source collection,
and transformations may also source from their own derivation
— enabling cyclic data-flows and graph algorithms.

Transformations may be added or removed from a derivation at any time.
This makes it possible to, for example, add a new collection into an
existing multi-way join, or to gracefully migrate to a new source
collection without incurring downtime.
However renaming a running transformation is not possible:
if attempted, the old transformation is dropped and
a new transformation under the new name is created,
which begins reading its source collection all over again.

## Sources

The **source** of a transformation is a collection.
As documents are published into the source collection,
they are continuously read and processed by the transformation.

A [partition selector](../projections.md#partition-selectors) may be provided
to process only a subset of the logical partitions of the source collection.
Selectors are efficient: only partitions which match the selector are read,
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
that can have unexpected schema changes without notice.
You may want to capture this data
with a minimal and very permissive schema.
Then, a derivation can apply a significantly stricter source schema
which verifies your current expectations of what the data _should_ be.
If those expectations turn out to be wrong,
little harm is done:
your derivation is paused but the capture continues to run.
You next update your transformations
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

If you don't provide a shuffle key
then Flow will shuffle on the source collection key,
which is typically what you want.

If a derivation has more than one transformation,
the shuffle keys of all transformations must align with one another
on the extracted key types (string versus integer)
and also the number of components in a composite key.
For example, one transformation couldn't shuffle transfers on `[/id]`
while another shuffles on `[/sender]`, because `sender` is a string and
`id` an integer.
Similarly mixing a shuffle of `[/sender]` alongside `[/sender, /recipient]`
is prohibited because the keys have different numbers of components.
However, one transformation _can_ shuffle on `[/sender]`
while another shuffles on `[/recipient]`,
as is done in the examples below.

## Publish Lambdas

A **publish** lambda publishes documents into the derived collection.

To illustrate first with an example,
suppose you must know the last transfer
from each sender which was over $100:

<Tabs>
<TabItem value="last-large-send.flow.yaml" default>

```yaml file=./bank/last-large-send.flow.yaml
```

</TabItem>
<TabItem value="last-large-send.flow.ts" default>

```typescript file=./bank/last-large-send.flow.ts
```

</TabItem>
<TabItem value="last-large-send-test.flow.yaml" default>

```yaml file=./bank/last-large-send-test.flow.yaml
```

</TabItem>
</Tabs>

This transformation defines a TypeScript **publish** lambda
which is implemented in an accompanying TypeScript module.
The lambda is invoked as each source transfer document arrives,
and is given the `source` document
as well as a `_register` and `_previous` register which are not used here.
More on [registers](#registers) in a bit.
The lambda outputs zero or more documents,
each of which must conform to the derivation's schema.

As this derivation's collection is keyed on `/sender`,
the last published document (the last large transfer) of each sender is retained.
If it were instead keyed on `/id`,
then _all_ transfers with large amounts would be retained.
In SQL terms, the collection key acts as a GROUP BY.

:::tip
Flow will initialize a TypeScript module for your lambdas if one doesn't exist,
with stubs of the required interfaces
and having TypeScript types that match your schemas.
You just write the function body.

[Learn more about TypeScript generation](../../flowctl.md#typescript-generation)
:::

***

Derivation collection schemas may have
[reduction](../schemas-and-data-reductions.md#reductions) annotations,
and publish lambdas can be combined with reductions in interesting ways.

You may be familiar with `map` and `reduce` functions
built into languages like
[Python](https://book.pythontips.com/en/latest/map_filter.html),
[JavaScript](https://www.freecodecamp.org/news/javascript-map-reduce-and-filter-explained-with-examples/),
and many others,
or have used tools like MapReduce or Spark.
In functional terms, lambdas you write within Flow are "mappers",
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
<TabItem value="balances.flow.ts" default>

```typescript file=./bank/balances.flow.ts
```

</TabItem>
<TabItem value="balances-test.flow.yaml" default>

```yaml file=./bank/balances-test.flow.yaml
```

</TabItem>
</Tabs>

## Registers

Registers are the internal _memory_ of a derivation.
They are a building block which enable derivations to tackle advanced stateful
streaming computations like multi-way joins, windowing, and transaction processing.
As we've already seen, not all derivations require registers
but they are essential for a variety of important use cases.

Each register is a document with a user-defined
[schema](../schemas-and-data-reductions.md).
Registers are keyed, and every derivation maintains an index of keys
and their corresponding register documents.
Every source document is mapped to a specific register document
through its extracted [shuffle key](#shuffles).

For example, when shuffling `acmeBank/transfers` on `[/sender]`
then each account ("alice", "bob", or "carol")
would be allocated its own register.
If you instead shuffle on `[/sender, /recipient]` then each
_pair_ of accounts ("alice -> bob", "alice -> carol", "bob -> carol")
would be allocated a register.

Registers are best suited for relatively small,
fast-changing documents that are shared within and across
the transformations of a derivation.
The number of registers indexed within a derivation may be very large,
and if a register has never before been used
it starts with a user-defined initial value.
From there, registers may be modified through an **update lambda**.

:::info
Under the hood, registers are backed by replicated,
embedded RocksDB instances which co-locate
with the lambda execution contexts that Flow manages.
As contexts are assigned and re-assigned,
their register databases travel with them.

If any single RocksDB instance becomes too large,
Flow is able to perform an online **split**,
which subdivides its contents into two new databases
 — and paired execution contexts — which are re-assigned to other machines.
:::

## Update Lambdas

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
<TabItem value="first-send.flow.ts" default>

```typescript file=./bank/first-send.flow.ts
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
and it returns zero or more documents
which each must conform to the derivation's register schema
(in this case, a simple boolean).

The **publish** lambda is invoked next, and is given the `source`
document as well as the _before_ (`previous`) and _after_ (`_register`)
values of the updated register.
In this case we don't need the _after_ value:
our update lambda implementation implies that it's always `true`.
The _before_ value, however,
tells us whether this was the very first update of this register,
and by implication was the first transfer for this pair of accounts.

<Mermaid chart={`
  sequenceDiagram
    autonumber
    Flow->>Update λ: update({sender: alice, recipient: bob})?
    Update λ-->>Flow: return "true"
    Flow->>Registers: lookup(key = [alice, bob])?
    Registers-->>Flow: not found, initialize as "false"
    Flow-->>Flow: Register: "false" => "true"
    Flow-)Registers: store(key = [alice, bob], value = "true")
    Flow->>Publish λ: publish({sender: alice, recipient: bob}, register = "true", previous = "false")?
    Publish λ-->>Flow: return {sender: alice, recipient: bob}
`}/>

:::info FAQ
> Why not have one lambda that can return
> a register update _and_ derived documents?

**Performance.**
_Update_ and _publish_ are designed to be
parallelized and pipelined over many source documents simultaneously,
while still giving the appearance and correctness of lambdas which
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
[reduction](../schemas-and-data-reductions.md#reductions) annotations,
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
<TabItem value="flagged-transfers.flow.ts" default>

```typescript file=./bank/flagged-transfers.flow.ts
```

</TabItem>
<TabItem value="flagged-transfers-test.flow.yaml" default>

```yaml file=./bank/flagged-transfers-test.flow.yaml
```

</TabItem>
</Tabs>

Source transfers are read twice,
with the first read shuffling on `/recipient`
to track account credits,
and the second shuffling on `/sender`
to track account debits and to publish enriched transfer events.
Update lambdas return the amount of credit or debit,
and these amounts are summed
into a derivation register keyed on the account.

<Mermaid chart={`
  sequenceDiagram
    autonumber
    Flow->>Registers: lookup(key = alice)?
    Registers-->>Flow: not found, initialize as 0
    Flow->>Update λ: update({recipient: alice, amount: 50, ...})?
    Update λ-->>Flow: return +50
    Flow->>Update λ: update({sender: alice, amount: 75, ...})?
    Update λ-->>Flow: return -75
    Flow-->>Flow: Register: 0 + 50 => 50
    Flow-->>Flow: Register: 50 - 75 => -25
    Flow->>Publish λ: publish({sender: alice, amount: 75, ...}, register = -25, previous = 50)?
    Publish λ-->>Flow: return {sender: alice, amount: 75, balance: -25, overdrawn: true}
`}/>

## Processing Order

Derivations may have multiple transformations that simultaneously read from
different source collections, or even multiple transformations that read
from the same source collection.

Roughly speaking, the derivation will globally process transformations and
their source documents in the time-based order by which the source documents
were originally written to their source collections.
This means that a derivation started a month ago,
and a new copy of the derivation started today,
will process documents in the same order and arrive at the same result.
Derivations are **repeatable**.

More precisely, processing order is stable for each individual shuffle key,
though different shuffle keys may process in different orders if more than
one task shard is used.

Processing order can be attenuated through a **read delay**
or differing transformation **priority**.

## Read Delay

A transformation can define a read delay which will hold back the processing
of its source documents until the time delay condition is met.
For example, a read delay of 15 minutes would mean that a source document
cannot be processed until it was published at least 15 minutes ago.
If the derivation is working through a historical backlog of source documents,
than a delayed transformation will respect its ordering delay relative
to the publishing times of other historical documents also being read.

Event-driven workflows a great fit for reacting to events as they occur,
but aren’t terribly good at taking action when something _hasn’t_ happened:

> * A user adds a product to their cart, but then doesn’t complete a purchase.
> * A temperature sensor stops producing its expected, periodic measurements.

A common pattern for tackling these workflows in Flow is to
read a source collection without a delay and update a register.
Then, read a collection with a read delay
and determine whether the desired action has happened or not.
For example, source from a collection a sensor readings
and index the last timestamp of each sensor in a register.
Then, source the same collection again with a read delay:
if the register timestamp isn't more recent
than the delayed source reading,
then the sensor failed to produce a measurement.

Flow read delays are very efficient and scale better
than managing very large numbers of fine-grain timers.

[Learn more from the Citi Bike idle bikes example](https://github.com/estuary/flow/blob/master/examples/citi-bike/idle-bikes.flow.yaml)

## Read Priority

Sometimes its required that _all_ documents of a source collection
are processed by a transformation before _any_ documents of some
other source collection are processed, regardless of their
relative publishing time.
For example, a collection may have corrections which should be
applied before the historical data of another collection
is re-processed.

Transformation priorities allow for expressing the relative
processing priority of a derivations various transformations.
When priorities are not equal, _all_ available source documents
of a higher-priority transformation
are processed before _any_ source documents
of a lower-priority transformation.

## Where to accumulate?

When you build a derived collection, you must choose where **accumulation** will happen:
whether Flow will reduce into documents held within
your materialization endpoint, or within the derivation's registers.
These two approaches can produce equivalent results
but they do so in very different ways.

### Accumulate in the Database

To accumulate in your materialization endpoint, such as a database,
you define a derivation with a reducible schema
and use only `publish` lambdas and no registers.
The Flow runtime uses your reduction annotations
to combine published documents which are written to the collection,
and also to fully reduce collection documents into the values
stored in the database,
keeping the materialized table up to date.

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
Flow is only able to provide weaker “at least once” semantics:
it’s possible that a document may be combined into a database value more than once.
Whether that’s a concern depends a bit on the task at hand.
Some reductions can be applied repeatedly without changing the result (they're "idempotent"),
and in other use-cases approximations are acceptable.
For the summing example above,
"at-least-once" semantics could give an incorrect result.

### Accumulate in Registers

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

They’re also well-suited for materializations into endpoints which aren't stateful,
such as pub/sub systems or Webhooks,
because they can produce fully reduced values as stand-alone updates.

[Learn more in the derivation pattern examples of Flow's repository](
  https://github.com/estuary/flow/tree/master/examples/derive-patterns
)