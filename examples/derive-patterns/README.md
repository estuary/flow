# Derivation Patterns

## Where to accumulate?

When building a derived collection, the central question is where
accumulation will happen: within derivation registers, or within a
materialized database? Both approaches can produce equivalent results,
but they do it in very different ways.

### Accumulate in the Database

To accumulate in the database, you'll define a collection having a reducible
schema with a derivation that uses only "publish" lambdas and no registers.
The Flow runtime uses the provided annotations to reduce new documents into
the collection, and ultimately keep the materialized table up to date.

A key insight is that the database is the _only_ stateful system in this
scenario, and that Flow is making use of reductions in two places:

1. To combine many published documents into partial "delta" states,
   which are the literal documents written to the collection.
2. To reduce "delta" states into the DB-stored value, reaching a final value.

For example, consider a collection that's summing a value:

| Time | DB    | Lambdas           | Derived Document |
| ---- | ----- | ----------------- | ---------------- |
| T0   | **0** | publish(2, 1, 2)  | **5**            |
| T1   | **5** | publish(-2, 1)    | **-1**           |
| T2   | **4** | publish(3, -2, 1) | **2**            |
| T3   | **6** | publish()         |

This works especially well when materializing into a transactional database.
Flow couples its processing transactions with corresponding DB transactions,
ensuring end-to-end "exactly once" semantics.

When materializing into a non-transactional store, Flow is only able
to provide weaker "at least once" semantics: it's possible that a document
may be combined into a DB value more than once. Whether that's a concern
depends a bit on the task at hand. Some reductions can be applied repeatedly
without changing the result ("idempotent"), and some use cases are fine with
_close enough_. For our counter above, it could give an incorrect result.

When materializing into a pub/sub topic, there _is_ no store to hold final values,
and Flow will publish delta states: each a partial update of the (unknown)
final value.

### Accumulate in Registers

Accumulating in registers involves a derivation that defines a reducible
register schema, and uses "update" lambdas.
Registers are arbitrary documents that can be shared and updated by the various
transformations of a derivation. The Flow runtime allocates, manages, and scales
durable storage for registers; you don't have to.

When using registers, the typical pattern is to use reduction annotations
within updates of the register, and to then publish last-write-wins "snapshots"
of the fully reduced value.

Returning to our summing example:

| Time | Register | Lambdas                             | Derived Document |
| ---- | -------- | ----------------------------------- | ---------------- |
| T0   | **0**    | update(2, 1, 2), publish(register)  | **5**            |
| T1   | **5**    | update(-2, 1), publish(register)    | **4**            |
| T2   | **4**    | update(3, -2, 1), publish(register) | **6**            |
| T3   | **6**    | update()                            |

Register derivations are a great solution for materializations into non-
transactional stores, because the documents they produce can be applied
multiple times without breaking correctness.

They're also well suited for materializations that publish into pub/sub,
as they can produce stand-alone updates of a fully-reduced value.

### Example: Summing in DB vs Register

See [summer.flow.yaml](summer.flow.yaml) for a simple example
of summing counts in the database, vs in registers.

## Types of Joins

### Outer Join accumulated in Database

Example of an outer join, which is reduced within a target database table.
This join is "fully reactive": it updates with either source collection,
and reflects the complete accumulation of their documents on both sides.

The literal documents written to the collection are combined delta states,
reflecting changes on one or both sides of the join. These delta states
are then fully reduced into the database table, and no other storage _but_
the table is required by this example.

See [join-outer-flow.yaml](join-outer-flow.yaml).

### Inner Join accumulated in Registers

Example of an inner join, which is reduced within the derivation's registers.
This join is also "fully reactive", updating with either source collection,
and reflects the complete accumulation of their documents on both sides.

The literal documents written to the collection are fully reduced snapshots
of the current join state.

This example _requires_ registers due to the "inner" join requirement,
which dictates that we can't publish anything until _both_ sides of the
join are matched.

See [join-inner.flow.yaml](join-inner.flow.yaml).

### One-sided Join accumulated in Registers

Example of a one-sided join, which publishes a current LHS joined
with an accumulated RHS.

This example is _not_ fully reactive. It publishes only on a LHS document,
paired with a reduced snapshot of the RHS accumulator at that time.

See [join-one-sided.flow.yaml](join-one-sided.yaml).

### Comparing Registers

Suppose we want to take action based on how a register is changing.

For example, suppose we want to detect "zero crossings" of a running sum,
and then filter the source collection to those documents which caused the
sum to cross from positive to negative (or vice versa).

We can use the `previous` register value to do so.
See [zero-crossing.flow.yaml](zero-crossing.flow.yaml).
