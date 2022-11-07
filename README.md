[![CI](https://github.com/estuary/flow/workflows/CI/badge.svg)](https://github.com/estuary/flow/actions)
[![Slack](https://img.shields.io/badge/slack-@gazette/dev-yellow.svg?logo=slack)](https://join.slack.com/t/gazette-dev/shared_invite/enQtNjQxMzgyNTEzNzk1LTU0ZjZlZmY5ODdkOTEzZDQzZWU5OTk3ZTgyNjY1ZDE1M2U1ZTViMWQxMThiMjU1N2MwOTlhMmVjYjEzMjEwMGQ) | **[Docs home](https://docs.estuary.dev/)** | **[Testing setup](https://docs.estuary.dev/getting-started/installation)** | **[Data platform comparison reference](https://docs.estuary.dev/overview/comparisons)** | **[Email list](https://www.estuary.dev/newsletter-signup/)**

<p align="center">
    <img src ="site/static/img/estuary-new.png"
     width="250"/>
         </p>

### Build millisecond-latency, scalable, future-proof data pipelines in minutes.

Estuary Flow is a DataOps platform that integrates all of the systems you use to produce, process, and consume data.

Flow unifies today's batch and streaming paradigms so that your systems
– current and future – are synchronized around the same datasets, updating in milliseconds.

With a Flow pipeline, you:

-   📷 **Capture** data from your systems, services, and SaaS into _collections_:
    millisecond-latency datasets that are stored as regular files of JSON data,
    right in your cloud storage bucket.

-   🎯 **Materialize** a collection as a view within another system,
    such as a database, key/value store, Webhook API, or pub/sub service.

-   🌊 **Derive** new collections by transforming from other collections, using
    the full gamut of stateful stream workflow, joins, and aggregations.

❗️ **Our UI-based web application is available**. Sign up for a free account [here](https://go.estuary.dev/sign-up). All functionality can be accessed in both the UI and CLI as a unified platform. Flow is a tool meant to allow *all* data stakeholders to meaningfully collaborate: engineers, analysts, and everyone in between.❗️

![Workflow Overview](site/docs/concepts/at-a-glance.png)

## Documentation

-   📖 [Flow documentation](https://docs.estuary.dev/).

-   🧐 Many [examples/](examples/) are available in this repo, covering a range of use cases and techniques.

## Just show me the code

Flow works with **catalog** specifications, written in declarative YAML and [JSON Schema](https://json-schema.org/):

```YAML
captures:
  # Capture Citi Bike's public system ride data.
  examples/citi-bike/rides-from-s3:
    endpoint:
      connector:
        # Docker image which implements a capture from S3.
        image: ghcr.io/estuary/source-s3:dev
        # Configuration for the S3 connector.
        # This can alternatively be provided as a file, and Flow integrates with
        # https://github.com/mozilla/sops for protecting credentials at rest.
        config:
          # The dataset is public and doesn't require credentials.
          awsAccessKeyId: ""
          awsSecretAccessKey: ""
          region: "us-east-1"
    bindings:
      # Bind files starting with s3://tripdata/JC-201703 into a collection.
      - resource:
          stream: tripdata/JC-201703
          syncMode: incremental
        target: examples/citi-bike/rides

collections:
  # A collection of Citi Bike trips.
  examples/citi-bike/rides:
    key: [/bike_id, /begin/timestamp]
    # JSON schema against which all trips must validate.
    schema: https://raw.githubusercontent.com/estuary/flow/master/examples/citi-bike/ride.schema.yaml
    # Projections relate a tabular structure (like SQL, or the CSV in the "tripdata" bucket)
    # with a hierarchical document like JSON. Here we define projections for the various
    # column headers that Citi Bike uses in their published CSV data. For example some
    # files use "Start Time", and others "starttime": both map to /begin/timestamp
    projections:
      bikeid: /bike_id
      birth year: /birth_year
      end station id: /end/station/id
      end station latitude: /end/station/geo/latitude
      end station longitude: /end/station/geo/longitude
      end station name: /end/station/name
      start station id: /begin/station/id
      start station latitude: /begin/station/geo/latitude
      start station longitude: /begin/station/geo/longitude
      start station name: /begin/station/name
      start time: /begin/timestamp
      starttime: /begin/timestamp
      stop time: /end/timestamp
      stoptime: /end/timestamp
      tripduration: /duration_seconds
      usertype: /user_type

materializations:
  # Materialize rides into a PostgreSQL database.
  examples/citi-bike/to-postgres:
    endpoint:
      connector:
        image: ghcr.io/estuary/materialize-postgres:dev
        config:
          # Try this by standing up a local PostgreSQL database.
          # docker run --rm -e POSTGRES_PASSWORD=password -p 5432:5432 postgres -c log_statement=all
          # (Use host: host.docker.internal when running Docker for Windows/Mac).
          address: localhost:5432
          password: password
          database: postgres
          user: postgres
    bindings:
      # Flow creates a 'citi_rides' table for us and keeps it up to date.
      - source: examples/citi-bike/rides
        resource:
          table: citi_rides

storageMappings:
  # Flow builds out data lakes for your collections in your cloud storage buckets.
  # A storage mapping relates a prefix, like examples/citi-bike/, to a storage location.
  # Here we tell Flow to store everything in one bucket.
  "": { stores: [{ provider: S3, bucket: my-storage-bucket }] }
```

### Run It

❗ These workflows are under active development and may change.
Note that Flow doesn't work yet on Apple M1.
For now, we recommend CodeSpaces or a separate Linux server.

Start a PostgreSQL server on your machine:
```console
$ docker run --rm -e POSTGRES_PASSWORD=password -p 5432:5432 postgres -c log_statement=all
```

Start a Flow data plane on your machine:
```console
$ flowctl-go temp-data-plane
export BROKER_ADDRESS=http://localhost:8080
export CONSUMER_ADDRESS=http://localhost:9000
```

In another tab, apply the exported `BROKER_ADDRESS` and `CONSUMER_ADDRESS`,
and save the example to `flow.yaml`. Then apply it to the data plane:

```console
$ flowctl-go deploy --source flow.yaml
```

You'll see a table created and loaded within your PostgreSQL server.

### Connectors

Captures and materializations use connectors:
plug-able Docker images which implement connectivity to a specific external system.
Estuary is [implementing connectors](https://github.com/orgs/estuary/packages?repo_name=connectors)
on an ongoing basis.
Flow can also run any connector implemented to the AirByte specification.

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

## Building on M1

* To cross-compile `musl` binaries from a darwin arm64 (M1) machine, you need to install `musl-cross` and link it:
  ```
  brew install filosottile/musl-cross/musl-cross
  sudo ln -s /opt/homebrew/opt/musl-cross/bin/x86_64-linux-musl-gcc /usr/local/bin/musl-gcc
  ```

* Install GNU `coreutils` which are used in the build process using:

  ```
  brew install coreutils
  ```

* If you encounter build errors complaining about missing symbols for x86_64 architecture, try setting the following environment variables:
  ```
  export GOARCH=arm64
  export CGO_ENABLED=1
  ```

* If you encounter build errors related to openssl, you probably have openssl 3 installed, rather than openssl 1.1:
  ```
  $ brew uninstall openssl@3
  $ brew install openssl@1.1
  ```
  Also make sure to follow homebrew's prompt about setting `LDFLAGS` and `CPPFLAGS`

* If you encounter build errors complaining about `invalid linker name in argument '-fuse-ld=lld'`, you probably need to install llvm:
  ```
  $ brew install llvm
  ```
  Also make sure to follow homebrew's prompt about adding llvm to your PATH
