[![CircleCI](https://circleci.com/gh/gazette/core.svg?style=svg)](https://circleci.com/gh/gazette/core)
[![GoDoc](https://godoc.org/github.com/gazette/core?status.svg)](http://godoc.org/github.com/gazette/core)
[<img src="https://img.shields.io/badge/slack-@gazette/dev-yellow.svg?logo=slack">](https://join.slack.com/t/gazette-dev/shared_invite/enQtNjQxMzgyNTEzNzk1LTU0ZjZlZmY5ODdkOTEzZDQzZWU5OTk3ZTgyNjY1ZDE1M2U1ZTViMWQxMThiMjU1N2MwOTlhMmVjYjEzMjEwMGQ)

![Gazette Logo](docs/logo_with_text.svg "Gazette Logo")

Overview
========

Gazette is infrastructure for building *streaming platforms*: platforms composed
of loosely coupled services, built and operated by distinct teams,
managing and serving large amounts of state, but all communicating continuously
through a common catalog of streamed data. It features a lightweight
container & cloud-native architecture, high availability, and integrates elegantly with
existing batch workflows.

Gazette consists of a *broker service* serving **journals**, a byte-oriented and
append-only resource resembling a file, and a *consumers* library for building rich
streaming applications in Go.

How It's Different
==================

Gazette provides low-latency, durable, ordered publish/subscribe services, while
*also* serving as the system-of-record for all historical data which has passed
through those streams, no matter how old or large in volume.

It features powerful **labels** and **selectors** mechanisms for tagging and querying
over event streams, similar to those of Kubernetes.

It minimizes operational risk by delegating both storage and high-volume replay
to elastic BLOB stores like S3. Brokers themselves are ephemeral and disposable.
The cluster scales and recovers from faults in seconds.

It seeks to be cost-efficient by best utilizing cloud pricing structures and
being careful to minimize inter-zone data transfers.

It features a rich library for building fault-tolerant, highly available, highly
stateful streaming applications in Go, as well as a "batteries included" command-
line tool which makes quick work of integrating existing applications.

Where to Start
==============

* [Brokers: A Tutorial Introduction](docs/broker_tutorial.md) is a walk-through of
key broker features and concepts.
* [Design Goals (and Non-Goals)](docs/goals_and_nongoals.md)
* Introduction to the *consumers* library and using it to build applications (coming soon).

Architecture Briefs:
 - [Exactly-once Semantics in Gazette](docs/exactly_once_semantics.md)
 - [Operational Considerations](docs/operational_considerations.rst)
 - (Others forthcoming).
 
