
.. image:: docs/_static/logo_with_text.svg
   :alt: Gazette Logo


.. image:: https://circleci.com/gh/gazette/core.svg?style=svg
   :target: https://circleci.com/gh/gazette/core
   :alt: CircleCI
.. image:: https://godoc.org/go.gazette.dev/core?status.svg
   :target: https://godoc.org/go.gazette.dev/core
   :alt: GoDoc
.. image:: https://img.shields.io/badge/slack-@gazette/dev-yellow.svg?logo=slack
   :target: https://join.slack.com/t/gazette-dev/shared_invite/enQtNjQxMzgyNTEzNzk1LTU0ZjZlZmY5ODdkOTEzZDQzZWU5OTk3ZTgyNjY1ZDE1M2U1ZTViMWQxMThiMjU1N2MwOTlhMmVjYjEzMjEwMGQ
   :alt: Slack
.. image:: https://goreportcard.com/badge/github.com/gazette/core
   :target: https://goreportcard.com/report/github.com/gazette/core
   :alt: Go Report Card


Overview
=========

Gazette makes it easy to build platforms that flexibly mix *SQL*, *batch*,
and *millisecond-latency streaming* processing paradigms. It enables teams,
applications, and analysts to work from a common catalog of data in the way
that's most convenient **to them**. Gazette's core abstraction is a "journal"
-- a streaming append log that's represented using regular files in a BLOB
store (i.e., S3).

The magic of this representation is that journals are simultaneously a
low-latency data stream *and* a collection of immutable, organized files
in cloud storage (aka, a data lake) -- a collection which can be directly
integrated into familiar processing tools and SQL engines.

Atop the journal *broker service*, Gazette offers a powerful *consumers
framework* for building streaming applications in Go. Gazette has served
production use cases for nearly five years, with deployments scaled to
millions of streamed records per second.

Where to Start
===============

- Official documentation_, with tutorials and examples.
- API Godocs_.

.. _documentation: https://gazette.dev
.. _Godocs: https://godoc.org/go.gazette.dev/core

