[![CircleCI](https://circleci.com/gh/gazette/core.svg?style=svg)](https://circleci.com/gh/gazette/core)
[![GoDoc](https://godoc.org/github.com/gazette/core?status.svg)](http://godoc.org/github.com/gazette/core)
[<img src="https://img.shields.io/badge/slack-@gazette/dev-yellow.svg?logo=slack">](https://join.slack.com/t/gazette-dev/shared_invite/enQtNjQxMzgyNTEzNzk1LTU0ZjZlZmY5ODdkOTEzZDQzZWU5OTk3ZTgyNjY1ZDE1M2U1ZTViMWQxMThiMjU1N2MwOTlhMmVjYjEzMjEwMGQ)
[![Go Report Card](https://goreportcard.com/badge/github.com/gazette/core)](https://goreportcard.com/report/github.com/gazette/core)

![Gazette Logo](docs/logo_with_text.svg "Gazette Logo")

Overview
========

Gazette is infrastructure for building *streaming platforms*: platforms composed
of loosely coupled services, built and operated by distinct teams,
managing and serving large amounts of state, but all communicating continuously
through a common catalog of streamed data. It features a lightweight
container & cloud-native architecture, high availability, and integrates elegantly with
existing batch workflows.

It has served production use cases for nearly five years, with deployments
scaled to millions of streamed messages per second.

Gazette consists of a *broker service* serving **journals**, a byte-oriented and
append-only resource resembling a file, and a *consumers* framework for building
streaming applications in Go.

How It's Different
==================

Gazette provides low-latency, durable, ordered publish/subscribe services, while
*also* serving as the system-of-record for all historical data which has passed
through those streams, no matter how old or large in volume.

It delegates storage and high-volume replay to elastic BLOB stores like S3.
Brokers themselves are ephemeral and disposable. The cluster scales and recovers
from faults in seconds. Its architecture obviates the common need for separate
batch and real-time systems by providing the capabilities and advantages of both.

It features a rich framework for building scaled, available streaming applications
in Go with exactly-once semantics. Applications may process against a remote
database, or may use embedded stores such as RocksDB for fast and tunable
storage of keys & values, or even SQLite for full SQL support. Details like the
durable replication of embedded stores, provisioning of hot standbys, request
routing, and fast fail-over are managed so that users can focus on their
message-driven application behaviors.

Deploy applications which efficiently crunch through months or even years of
historical data, then seamlessly transition to real-time tailing reads. Serve
blazing fast APIs drawing from state stores embedded within applications.
Apply Gazette's "batteries included" command-line tool to make quick
work of integrating existing applications.

Brokers and applications are easily operated by container platforms like
Kubernetes, and offer familiar primitives such as declarative YAML specifications
and a powerful *labels* & *selectors* mechanism for tagging and querying over
objects. Gazette seeks to be cost-efficient by best utilizing cloud pricing
structures and being careful to minimize inter-zone data transfers.

Where to Start
==============

Project [Overview Slides](https://docs.google.com/presentation/d/e/2PACX-1vRq8pwusGbcv1KaoedwfvyKydmO-IBvziXaKQhwFpwCSYt5P7Yn4n5_gWD7XBW2feAlvhZ8-YP4h1uF/pub?start=false&loop=false&delayms=3000)
discuss high-level capabilities and architecture (good for the impatient).

[Brokers: A Tutorial Introduction](docs/broker_tutorial.md) is a walk-through of
key broker features and concepts.

[Design Goals (and Non-Goals)](docs/goals_and_nongoals.md) discusses rationale for the project,
what Gazette strives to be great at, and what Gazette avoids trying to solve.

The project features several runnable example consumer applications:
 * [Finding Cycles in Bike-Share Streams](docs/examples_bike_share.md).
 * [Serving up a Real-Time Language Model](docs/examples_word_count.md).
 * [SHA-summing over Multiplexed File Chunks](docs/examples_stream_sum.md).

[Build and Test](docs/build_and_test.md) the project.

Architecture Briefs:
 * [Exactly-once Semantics in Gazette](docs/exactly_once_semantics.md)
 * [Operational Considerations](docs/operational_considerations.rst)
 * (Others forthcoming).
