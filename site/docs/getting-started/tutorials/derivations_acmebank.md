---
id: derivations_acmebank
title: Implementing Derivations for AcmeBank
---

<head>
    <meta property="og:image" content="https://storage.googleapis.com/estuary-marketing-strapi-uploads/uploads//architecture_6bbaf2c5a6/architecture_6bbaf2c5a6.png" />
</head>

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import Mermaid from '@theme/Mermaid';

# Implementing Derivations for AcmeBank

The following tutorial sections use an illustrative example
to introduce you to derivations, how you might use them, and their common components.
We'll discuss each component in depth in subsequent sections of this page,
but we recommend you start here to get your bearings.

Suppose you have an application through which users send one another
some amount of currency, like in-game tokens or dollars or digital kittens.
You have a `transfers` collection of user-requested transfers,
each sending funds from one account to another:

<Tabs>
<TabItem value="transfers.flow.yaml" default>

```yaml file=./derivations_acmebank_assets/transfers.flow.yaml
```

</TabItem>
<TabItem value="transfers.schema.yaml" default>

```yaml file=./derivations_acmebank_assets/transfers.schema.yaml
```

</TabItem>
</Tabs>

There are many views over this data that you might require,
such as summaries of sender or receiver activity,
or current account balances within your application.

## Filtering Large Transfers

:::note
This section introduces SQLite derivations, SQL lambda blocks and `$parameters`.
:::

Your compliance department has reached out, and they require an understanding
of the last large transfer (if any) made by each user account.

You create a SQL derivation to help them out.
The `transfers` collection is keyed on the transfer `/id`,
so you'll need to re-key your derivation on the `/sender` account.
You also need to filter out transfers that aren't large enough.

Putting this all together:

<Tabs>
<TabItem value="last-large-send.flow.yaml" default>

```yaml file=./derivations_acmebank_assets/last-large-send.flow.yaml
```

</TabItem>
<TabItem value="last-large-send-test.flow.yaml" default>

```yaml file=./derivations_acmebank_assets/last-large-send-test.flow.yaml
```

</TabItem>
</Tabs>

`derive: using: sqlite: {}` tells Flow that collection
`acmeBank/last-large-send` is derived using Flow's SQLite derivation connector.

This derivation has just one transform, which sources from the `transfers` collection.
As source documents become available, they're evaluated by the SQL `lambda`
and its `SELECT` output is published to the derived collection.
Your SQL queries access locations of source documents through [$parameter](../../concepts/derivations.md#parameters) bindings.

The compliance department then materializes this collection to their preferred destination,
for an always up-to-date view indexed by each account.

## Finding New Account Pairs

:::note
This section introduces SQLite migrations and internal task tables.
:::

The fraud team needs your help: they have a new process they must run
the first time some sending account sends funds to a receiving account.
They would like to see only those transfers which reflect a new account pair of (sender, recipient).
To tackle this you need to know which account pairs have been seen before.

SQLite derivations run within the context of a persistent, managed SQLite database.
You can apply database [migrations](../../concepts/derivations.md#migrations) that create whatever tables, triggers, or views you might need.
Then, the statements of your SQL lambda code can `INSERT`, `UPDATE`, or `DELETE`
from those tables, query from them, or any other operation supported by SQLite.
The tables and other schema you create through your migrations
are the [internal state](../../concepts/derivations.md#internal-state) of your task.

<Tabs>
<TabItem value="first-send.flow.yaml" default>

```yaml file=./derivations_acmebank_assets/first-send.flow.yaml
```

</TabItem>
<TabItem value="first-send-test.flow.yaml" default>

```yaml file=./derivations_acmebank_assets/first-send-test.flow.yaml
```

</TabItem>
</Tabs>

This time, the derivation attempts to `INSERT` into the `seen_pairs` table,
and uses SQLite's [RETURNING](https://www.sqlite.org/lang_returning.html)
syntax to only publish documents for rows which were successfully inserted.

You can evolve the internal SQLite tables of your derivation as needed,
by appending SQL blocks which perform a database migration to the `migrations` array.
Any migrations appended to the list are automatically applied by Flow.

## Grouped Windows of Transfers

:::note
This section introduces delayed reads, and applies them to implement a custom window policy.
:::

The fraud team is back, and now needs to know the _other_
transfers which an account has made in the last day.
They want you to enrich each transfer with the grouping of all
transfers initiated by that account in the prior 24 hours.

You may have encountered "windowing" in other tools for stream processing.
Some systems even require that you define a window policy in order to function.
Flow does not use windows, but sometimes you do want a time-bound grouping of recent events.

All collection documents contain a wall-clock timestamp of when they were published.
The transforms of a derivation will generally process source documents in ascending wall-time order.
You can augment this behavior by using a [read delay](../../concepts/derivations.md#read-delay) to refine the relative order in which
source documents are read, which is useful for implementing arbitrary window policies:


<Tabs>
<TabItem value="grouped.flow.yaml" default>

```yaml file=./derivations_acmebank_assets/grouped.flow.yaml title=grouped.flow.yaml
```

</TabItem>
<TabItem value="enrichAndAddToWindow.sql" default>

```sql file=./derivations_acmebank_assets/enrichAndAddToWindow.sql title=enrichAndAddToWindow.sql
```

</TabItem>
<TabItem value="grouped-test.flow.yaml" default>

```yaml file=./derivations_acmebank_assets/grouped-test.flow.yaml title=grouped-test.flow.yaml
```

</TabItem>
</Tabs>

## Approving Transfers

:::note
This section expands usage of SQLite task tables and introduces a recursive data flow.
:::

Your users don't always check if they have sufficient funds before starting a transfer,
and account overdrafts are becoming common.
The product team has tapped you to fix this
by enriching each transfer with an **approve** or **deny** outcome
based on the account balance of the sender.

To do this, you first need to track the sender's current account balance.
Clearly an account balance is debited when it's used to sends funds.
It's also credited when it receives funds.

*But there's a catch*:
an account can only be credited for funds received from **approved** transfers!
This implies you need a collection of transfer outcomes
in order to derive your collection of transfer outcomes 🤯.

This is an example of a self-referential, recursive data-flow.
You may have used tools which require that data flow in a Directed Acyclic Graph (DAG).
Flow does *not* require that your data flows are acyclic,
and it also supports a derivation that reads from itself,
which lets you tackle this task:

<Tabs>
<TabItem value="outcomes.flow.yaml" default>

```yaml file=./derivations_acmebank_assets/outcomes.flow.yaml title=outcomes.flow.yaml
```

</TabItem>
<TabItem value="debitSender.sql" default>

```sql file=./derivations_acmebank_assets/debitSender.sql title=debitSender.sql
```

</TabItem>
<TabItem value="outcomes-test.flow.yaml" default>

```yaml file=./derivations_acmebank_assets/outcomes-test.flow.yaml title=outcomes-test.flow.yaml
```

</TabItem>
</Tabs>


## Current Account Balances

:::note
This section introduces TypeScript derivations and reduction annotations.
:::

Your product team is back, and they want a database table
keyed by account that contains its up-to-date current balance.

As shown in the previous section, you could create
a task table which aggregates each account balance,
and then `SELECT` the current balance after every transfer.
For most use cases, this is a **great** place to start.
For interest and variety, you'll solve this problem using TypeScript.

TypeScript derivations require a `module` which you write.
You don't know how to write that module yet,
so first implement the derivation specification in `balances.flow.yaml`.
Next run the `flowctl generate` command, which generates two files:
* A module stub for you to fill out.
* A file of TypeScript interfaces which are used by your module.

<Tabs>
<TabItem value="balances.flow.yaml" default>

```yaml title=balances.flow.yaml file=./derivations_acmebank_assets/balances.flow.yaml
```

</TabItem>
<TabItem value="Module Stub" default>

```typescript title=balances.ts file=./derivations_acmebank_assets/balances-stub.ts
```

</TabItem>
<TabItem value="Interfaces" default>

```typescript file=./derivations_acmebank_assets/flow_generated/typescript/acmeBank/balances.ts title=flow/acmeBank/balances.ts
```

</TabItem>
</Tabs>

Next fill out the body of your TypeScript module and write a test:


<Tabs>
<TabItem value="balances.ts" default>

```typescript title=balances.ts file=./derivations_acmebank_assets/balances.ts
```

</TabItem>
<TabItem value="balances-test.flow.yaml" default>

```yaml title=balances-test.flow.yaml file=./derivations_acmebank_assets/balances-test.flow.yaml
```

</TabItem>
</Tabs>

One piece is still missing.
Your TypeScript module is publishing the **change** in account balance for each transfer.
That's not the same thing as the **current** balance for each account.

You can ask Flow to sum up the balance changes into a current account balance
through [reduction annotations](../../concepts/schemas.md#reductions).
Here's the balances schema, with `reduce` annotations for summing the account balance:

```yaml title=balances.schema.yaml file=./derivations_acmebank_assets/balances.schema.yaml
```

This section has more moving parts that the previous SQL-based examples.
You might be wondering, why bother? Fair question!
This is just an illustrative example, after all.

While they're more verbose, TypeScript derivations do have certain advantages:

* TypeScript derivations are strongly typed, and those checks often catch meaningful bugs and defects **before** they're deployed.
  Your derivation modules also play nicely with VSCode and other developer tooling.
* TypeScript derivations can use third-party libraries, as well as your native code compiled to WASM.
* TypeScript can be easier when working with nested or complex document structures.

Reduction annotations also have some benefits over task state (like SQLite tables):

* Internal task state is managed by Flow.
  If it grows to be large (say, you have **a lot** of accounts),
  then your task must be scaled and could require performance tuning.
  Reduction annotations, on the other hand, require *no* internal state and are extremely efficient.
* Certain aggregations, such as recursive merging of tree-like structures,
  are much simpler to express through reduction annotations vs implementing yourself.

[See "Where to Accumulate?" for more discussion](../../concepts/derivations.md#where-to-accumulate).