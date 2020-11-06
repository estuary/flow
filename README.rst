
.. image:: https://github.com/estuary/flow/workflows/CI/badge.svg
   :target: https://github.com/estuary/flow/actions?query=workflow%3A%22CI%22
   :alt: Flow Continuous Integration
.. image:: https://img.shields.io/badge/slack-@gazette/dev-yellow.svg?logo=slack
   :target: https://join.slack.com/t/gazette-dev/shared_invite/enQtNjQxMzgyNTEzNzk1LTU0ZjZlZmY5ODdkOTEzZDQzZWU5OTk3ZTgyNjY1ZDE1M2U1ZTViMWQxMThiMjU1N2MwOTlhMmVjYjEzMjEwMGQ
   :alt: Slack

Estuary Flow (Preview)
======================

Estuary Flow unifies technologies and teams around a shared understanding of an organization’s data,
that updates continuously as new data records come in. Flow works with the places where you produce
or consume data today – analytics warehouses, OLTP databases, key/value stores, streaming systems,
or SaaS APIs – keeping them all in sync using incremental, event-driven map/reduce and materializations.

With Flow, you can capture events from e.x. Kenesis or WebSockets; organize them into an S3 “data lake”
that integrates with tools like Spark or Snowflake; transform by mapping individual events into a
stitched profile, and materialize aggregated profiles into a Redis store that reflects updates within
milliseconds. All in about 50 lines of YAML, and with rigorous data validations at every step.

Later, you can define derivations and materializations that will automatically back-fill over months
or even years of historical data, and which then seamlessly transition to low latency updates of new data.

Flow is configuration driven and uses a developer-centric workflow that emphasizes version control,
composition & re-use, rich schematization, and built in testing. Its runtime offers flexible scaling,
and takes best advantage of data reductions and cloud pricing models to offer a surprisingly low
total cost of ownership.

Flow's documentation lives at https://github.com/estuary/docs, and is browse-able at https://estuary.readthedocs.io.