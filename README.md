[![CI](https://github.com/estuary/flow/workflows/CI/badge.svg)](https://github.com/estuary/flow/actions)
[![Slack](https://img.shields.io/badge/slack-@gazette/dev-yellow.svg?logo=slack)](https://join.slack.com/t/gazette-dev/shared_invite/enQtNjQxMzgyNTEzNzk1LTU0ZjZlZmY5ODdkOTEzZDQzZWU5OTk3ZTgyNjY1ZDE1M2U1ZTViMWQxMThiMjU1N2MwOTlhMmVjYjEzMjEwMGQ)

# Estuary Flow (Preview)

Estuary Flow is a tool for integrating all of the systems you use to produce, process, and consume data.
It unifies today's batch vs streaming paradigms
so that your systems
– current and future –
are synchronized around the same data sets, updating in milliseconds.
With Flow, you:

-   📷 **Capture** data from your systems, services, and SaaS into _collections_:
    continuous datasets that are stored as regular files of your JSON data,
    right in your cloud storage bucket.
    Collections plug into your existing tools:
    Spark, Snowflake, BigQuery, and others, keeping your data portable, flexible, and... yours!

-   🎯 **Materialize** a collection as a view within another system,
    such as a database, key/value store, Webhook API, or pub/sub service.
    Flow back-fills from the collection's history
    and then keeps your system fresh using precise, low-latency updates.

-   🌊 **Derive** new collections by transforming from other collections.
    As with materializations, Flow back-fills an added derivation from the history
    of its source collections, and thereafter keeps it up to date.

    Transformations are uniquely powerful.
    You can tackle the full gamut of stateful stream workflows,
    including aggregations and non-temporal joins,
    and Flow is able to scale to match your data volume without downtime.

![Workflow Overview](https://github.com/estuary/flow/blob/master/images/estuaryOverview.png?raw=true)

## Documentation

-   📖 [Flow documentation](https://docs.estuary.dev/).

-   🧐 Many [examples/](examples/) are available in this repo, covering a range of use cases and techniques.

## Just show me the code

Write declarative YAML and [JSON Schema](https://json-schema.org/):

```YAML
collections:
  # Collection of 💲 transfers between accounts:
  #   {id: 123, sender: alice, recipient: bob, amount: 32.50}
  acmeBank/transfers:
    schema:
      # JSON Schema of collection's documents.
      type: object
      properties:
        id: { type: integer }
        sender: { type: string }
        recipient: { type: string }
        amount: { type: number }
      required: [id, sender, recipient, amount]
    key: [/id]

  # Derived balances of each account:
  #   {account: alice, amount: 67.35}
  acmeBank/balances:
    schema:
      type: object
      properties:
        account: { type: string }
        amount:
          # Flow extends JSON schema with "reduce" annotations.
          # These tell Flow how to combine each document location.
          reduce: { strategy: sum }
          type: number
      required: [account, amount]
      reduce: { strategy: merge }
    key: [/account]

    derivation:
      transform:
        fromTransfers:
          source: { name: acmeBank/transfers }
          # Lambdas are functions that map input documents into output documents.
          # Here we declare a lambda that will map a bank transfer document
          # into a balance update.
          # This declaration tells Flow to look for an associated TypeScript module.
          publish: { lambda: typescript }

materializations:
  acmeBank/database:
    endpoint:
      flowSink:
        # Use a Docker connector to materialize into a PostgreSQL database.
        # Connectors encapsulate the details of how to update a remote system,
        # whether it's a database, a key/value store, pub/sub, or a SaaS API.
        image: ghcr.io/estuary/materialize-postgres:f6f197d
        config:
          # Try this by standing up a local PostgreSQL database.
          # docker run --rm -e POSTGRES_PASSWORD=password -p 5432:5432 postgres -c log_statement=all
          # (Use host: host.docker.internal when running Docker for Windows/Mac).
          host: localhost
          password: password
          database: postgres
          user: postgres
          port: 5432
    bindings:
      # Create and materialize into table `account_balances`.
      - resource:
          table: account_balances
        source: acmeBank/balances

tests:
  Balances reflect transfers:
    - ingest:
        collection: acmeBank/transfers
        documents:
          - { id: 1, sender: alice, recipient: bob, amount: 32.50 }
          - { id: 2, sender: bob, recipient: carly, amount: 10.75 }
    - verify:
        collection: acmeBank/balances
        documents:
          - { account: alice, amount: -32.50 }
          - { account: bob, amount: 21.75 }
          - { account: carly, amount: 10.75 }
```

This file `acmeBank.flow.yaml` declares a `{ lambda: typescript }`, so Flow expects a
corresponding TypeScript module `acmeBank.flow.ts` that export its lambda definition:

```TypeScript
import { collections, interfaces, registers } from 'flow/modules';

// TypeScript types in `flow/modules` are generated from your catalog,
// and Flow will create this file with an implementation stub if it
// doesn't exist: all you have to write is the function body.

export class AcmeBankBalances implements interfaces.AcmeBankBalances {
    fromTransfersPublish(
        source: collections.AcmeBankTransfers,
        // Registers enable stateful workflows, and are part of
        // the interface Flow expects, but aren't used here.
        _register: registers.AcmeBankBalances,
        _previous: registers.AcmeBankBalances,
    ): collections.AcmeBankBalances[] {
        return [
            // A transfer removes from the sender and adds to the recipient.
            { account: source.sender, amount: -source.amount },
            { account: source.recipient, amount: source.amount },
        ];
    }
}
```

Today Flow supports TypeScript modules, which Flow runs on your behalf,
or a JSON HTTP endpoint (such as AWS λ) that you supply.
In the future we'll add support for WebAssembly and OpenAPI.

## How does it work?

Flow builds upon [Gazette](https://gazette.dev) and is by the Gazette authors.
Collections have logical and physical partitions
which are implemented as Gazette journals.
Derivations and materializations leverage the Gazette consumer framework,
which provide durable state management, exactly-once semantics,
high availability, and dynamic scale-out.

Flow collections are both a batch dataset –
they're stored as a structured "data lake" of general-purpose files in cloud storage –
and a stream, able to commit new documents and forward them to readers within milliseconds.
New use cases read directly from cloud storage for high scale back-fills of history,
and seamlessly transition to low-latency streaming on reaching the present.

## Is it "production" yet?

Gazette, on which Flow is built, has been operating at large scale (GB/s)
for many years now and is very stable.

Flow itself is winding down from an intense period of research and development.
Estuary is running production pilots now, but it's early. You should expect that Flow
may fail in ways that halt execution of derivations or materializations. There may
be cases where derivations or materialization must be rebuilt due to bugs within Flow.
Loss of ingested source data, however, is very unlikely.

## How fast is it?

It depends on the use case, of course, but... fast. On a modest machine,
we're seeing performance of complex, real-world use cases
[achieve 10K inputs / second](https://github.com/estuary/flow/tree/docs-examples/examples/segment#extras-2-turn-up-the-heat),
where each input involves many downstream derivations and materializations.
We haven't begun any profile-guided optimization work, though, and this is likely to improve.

Flow mixes a variety of architectural techniques to achieve great throughput without adding latency:

-   Optimistic pipelining, using the natural back-pressure of systems to which data is committed
-   Leveraging `reduce` annotations to group collection documents by-key wherever possible,
    in memory, before writing them out
-   Co-locating derivation states (_registers_) with derivation compute:
    registers live in an embedded RocksDB that's replicated for durability and machine re-assignment.
    They update in memory and only write out at transaction boundaries.
-   Vectorizing the work done in external Remote Procedure Calls (RPCs) and even process-internal operations.
-   Marrying the development velocity of Go with the raw performance of Rust, using a zero-copy
    [CGO service channel](https://github.com/estuary/flow/commit/0fc0ff83fc5c58e01a09a053419f811d4460776e).

## How do I try it?

We have a [project template](https://github.com/estuary/flow-template) with a
[VSCode devcontainer](https://code.visualstudio.com/docs/remote/devcontainerjson-reference)
to jump off from. If you have early access to
[GitHub Codespaces](https://github.com/features/codespaces)
you can try it right from your browser.

A Docker image of the development branch is also available as `quay.io/estuary/flow:dev`.
We'll start more regular releases soon, but not quite yet. We recommend using an alias to run the image:

```console
$ alias flowctl='docker run --rm -it --mount type=bind,source="$(pwd)",target=/home/flow/project --env RUST_LOG -p 8080:8080 quay.io/estuary/flow:dev flowctl'

# Test all examples from the Flow repository.
$ git clone https://github.com/estuary/flow.git && cd flow
$ flowctl test --source examples/all.flow.yaml

# Or you can test & develop from a remote catalog without cloning.
# flowctl will create necessary TypeScript project scaffolding:
$ mkdir ~/tmp && cd ~/tmp
$ flowctl test --source https://raw.githubusercontent.com/estuary/flow/master/examples/all.flow.yaml
```

You interact with Flow through the `flowctl` CLI tool:

-   `flowctl test` runs all tests of a `--source` catalog.
-   `flowctl develop` serves a `--source` catalog as a single local process (✈️ mode).
-   `flowctl apply` updates a production deployment of Flow.

Estuary also provides a fully managed offering of Flow, running in your Kubernetes cluster.
Please [reach out](https://estuary.dev/#contact-us) to us for details.
