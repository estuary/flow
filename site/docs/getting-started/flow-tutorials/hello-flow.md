---
sidebar_position: 1
description: Create your first end-to-end data flow.
---

# Your first data flow

You can find a more succinct version of this tutorial in the [Flow Template repository](https://github.com/estuary/flow-template).

## Word count in a continuous PostgreSQL materialized view

PostgreSQL is a great open-source database that supports materialized views, but it doesn't offer *continuous* materialized views. In this tutorial, you'll build one with Flow.

How many times have you managed text documents in PostgreSQL and thought to yourself:

> "Gee-whiz, self, I wish I had a table of word counts that was always up-to-date!"

... basically never, right? Well, it's a simple way to get familiar with a powerful concept, so let's do it anyway!

Our data flow will:

* **Capture** data from a `documents` table in the PostgreSQL database.
* Use a **derivation** to compute word and document frequency updates.
* **Materialize** the results back into the table in a a `word_counts` table.

These three processes — captures, derivations, and materializations — comprise the possible **tasks** in any data flow. They're configured in YAML files known as a **catalog specification**. For this example, they've been configured for you. If you'd like, you can check out `flow.yaml` and `word-counts.flow.yaml` to get oriented.

## Set up
These instructions assume you've [set up a development environment](../installation.md) either using Codespaces or on your local machine. Go back and do that first, if necessaary.

## Verify tests

All cutting-edge word count projects should have tests.
Let's make sure the words are, um, counted. Run the following:
```console
$ flowctl test --source word-counts.flow.yaml
```
Wait until you see:
```console
Running  1  tests...
✔️ word-counts.flow.yaml :: acmeCo/tests/word-counts-from-documents

Ran 1 tests, 1 passed, 0 failed
```
Your test performed as expected; now you can deploy the catalog.

## Run It Locally

Start a local, temporary Flow data plane:
```console
$ flowctl temp-data-plane
```

A data plane is a long-lived, multi-tenant, scale-out component that
usually runs in a data center.
Fortunately it also shrinks down to your laptop.

This returns a couple exported addresses, which you'll need in a moment:
```console
export BROKER_ADDRESS=http://localhost:8080
export CONSUMER_ADDRESS=http://localhost:9000
```

Now, deploy the catalog to your data plane:
```console
$ export BROKER_ADDRESS=http://localhost:8080
$ export CONSUMER_ADDRESS=http://localhost:9000
$ flowctl deploy --wait-and-cleanup --source flow.yaml
```

After a moment, you'll see:
```console
Deployment done. Waiting for Ctrl-C to clean up and exit.
```

Flow is now watching the `documents` table, and materializing to `word_counts`.
Start a new terminal window to begin working with the database.

```console
$ psql --host localhost
psql (13.5 (Debian 13.5-0+deb11u1), server 13.2 (Debian 13.2-1.pgdg100+1))
Type "help" for help.
```
The `documents` table is still empty, so you'll populate it with a few phrases:

```console
flow=# insert into documents (body) values ('The cat in the hat.'), ('hat Cat CAT!');
INSERT 0 2
```
Now, you'll take a look at `word_counts` to see the results:

```console
flow=# select word, count, doc_count from word_counts;
 word | count | doc_count
------+-------+-----------
 cat  |     3 |         2
 hat  |     2 |         2
 in   |     1 |         1
 the  |     2 |         1
(4 rows)
```
Say you made a typo in that second value. You can immediately update it:
```console
flow=# update documents set body = 'cat Cat CAT!' where id = 2;
UPDATE 1
flow=# select word, count, doc_count from word_counts order by word;
 word | count | doc_count
------+-------+-----------
 cat  |     4 |         2
 hat  |     1 |         1
 in   |     1 |         1
 the  |     2 |         1
(4 rows)
```
Now, let's clean up the table:
```console
flow=# delete from documents ;
DELETE 2
flow=# select word, count, doc_count from word_counts order by word;
 word | count | doc_count
------+-------+-----------
 cat  |     0 |         0
 hat  |     0 |         0
 in   |     0 |         0
 the  |     0 |         0
(4 rows)
```
The updates you push to `documents` are materialized to `word_counts` with millisecond latency. In effect, you've added a new, powerful capability to PostgreSQL that has a multitude
of real-world and business applications (far beyond counting cats and hats).

When you're done with your testing, exit the data flow by returning to your first console window and pressing **Ctrl-C**.