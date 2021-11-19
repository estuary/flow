[![CI](https://github.com/estuary/flow/workflows/CI/badge.svg)](https://github.com/estuary/flow/actions)
[![Slack](https://img.shields.io/badge/slack-@gazette/dev-yellow.svg?logo=slack)](https://join.slack.com/t/gazette-dev/shared_invite/enQtNjQxMzgyNTEzNzk1LTU0ZjZlZmY5ODdkOTEzZDQzZWU5OTk3ZTgyNjY1ZDE1M2U1ZTViMWQxMThiMjU1N2MwOTlhMmVjYjEzMjEwMGQ) | **[Docs home](https://docs.estuary.dev/)** | **[Testing setup](https://docs.estuary.dev/getting-started/installation)** | **[Data platform comparison reference](https://docs.estuary.dev/overview/comparisons)** | **[Email list](https://www.estuary.dev/newsletter-signup/)**   

<p align="center">
    <img src ="https://github.com/estuary/flow/blob/master/images/Estuary%20Flow%20(Beta).gif"
     width="300" 
     height="300"/>
         </p>

### Build millisecond-latency, scalable, future-proof data pipelines in minutes.

Estuary Flow is a DataOps platform that integrates all of the systems you use to produce, process, and consume data.

Flow unifies today's batch and streaming paradigmsso that your systems
– current and future – are synchronized around the same datasets, updating in milliseconds.

With a Flow pipeline, you:

-   📷 **Capture** data from your systems, services, and SaaS into _collections_:
    continuous datasets that are stored as regular files of your JSON data,
    right in your cloud storage bucket.

-   🎯 **Materialize** a collection as a view within another system,
    such as a database, key/value store, Webhook API, or pub/sub service.

-   🌊 **Derive** new collections by transforming from other collections, using 
    the full gamut of stateful stream workflow, joins, and agreggations.
    
❗️ Currently, Flow is a CLI-only platform. **Our UI is coming in Q1 of 2022**, and we will continue to grow both the CLI and UI. Flow is a tool meant for *all* stakeholders: engineers, analysts, and everyone in between.❗️

![Workflow Overview](https://github.com/estuary/flow/blob/master/images/estuaryOverview.png?raw=true)

## Documentation

-   📖 [Flow documentation](https://docs.estuary.dev/).

-   🧐 Many [examples/](examples/) are available in this repo, covering a range of use cases and techniques.

## Just show me the code

This simple example shows a CDC **capture** from a [public S3 bucket](https://s3.amazonaws.com/tripdata/index.html). The resulting **collection** is then **materialized** to PostgreSQL. Flow integrates to these endpoints with two of Estuary's real-time [connectors](https://github.com/orgs/estuary/packages?repo_name=connectors), available as docker images.

The spec for this **catalog** is written in declarative YAML and [JSON Schema](https://json-schema.org/):

```YAML
collections:
  acmeCo/tripdata:
    schema:
      properties:
        _meta:
          properties:
            file:
              type: string
            offset:
              minimum: 0
              type: integer
          required:
            - file
            - offset
          type: object
      required:
        - _meta
      type: object
    key: [/_meta/file, /_meta/offset]
    
captures:
  acmeCo/source-s3:
    endpoint:
      airbyteSource:
        image: ghcr.io/estuary/source-s3:0a4373e
        config:
          ascendingKeys: false
          awsAccessKeyId: ""
          awsSecretAccessKey: ""
          bucket: "tripdata"
          endpoint: ""
          matchKeys: "202106-citibike-tripdata.csv.zip"
          prefix: ""
          region: "us-east-1"
    bindings:
      - resource:
          stream: tripdata/
          syncMode: incremental
        target: acmeCo/tripdata
        
materializations: 
  acmeCo/postgres: 
    bindings: 
      - source: acmeCo/tripdata 
        resource: 
          table: trips 
    endpoint:
      postgres: 
        host: localhost 
        password: flow 
        user: flow

```

Today Flow supports TypeScript modules, which Flow runs on your behalf,
or a JSON HTTP endpoint (such as AWS λ) that you supply.
In the future we'll add support for WebAssembly and OpenAPI.

## How does it work?

Flow builds on [Gazette](https://gazette.dev), a streaming broker created by the same founding team. 
Collections have logical and physical partitions
which are implemented as Gazette **journals**.
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
Estuary is running production pilots now, for a select group of beta customers (you can [reach out](https://www.estuary.dev/#get-in-touch) for a free consult with the team). 
For now, we encourage you to use Flow in a testing environment, but you might see unexpected behaviors
or failures simply due to the pace of development.

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
