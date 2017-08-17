Gazette
=======

Gazette is a distributed byte-stream transaction engine. It models a concept of
"journals": infinite length, append-only files. See `Architecture Overview`_.

.. _Architecture Overview: docs/architecture_overview.rst


Package Layout
==============

``journal``
  Contains all runtime components for Gazette journals, including Fragment &
  Spool for journal content, Head (serving replications), Tail (reads), and
  Broker (for brokering new writes).

``topic``
  A client library for topic descriptions and partitioned writes.

``consumer``
  A client library for distributed, stateful topic consumption.

``async``
  Implements a simple Promise API.

``gazette``
  Contains server components tied to the service lifetime, and clients.

