API Reference
=================

API docs
---------

Gazette API documentation is primarily hosted as godocs_. Packages of note include:

- client_ for interacting with Gazette brokers.
- consumer_ for core consumer Application interfaces.
- runconsumer_ for opinionated initialization of consumer applications.

.. _godocs:      https://godoc.org/go.gazette.dev/core
.. _client:      https://godoc.org/go.gazette.dev/core/broker/client
.. _consumer:    https://godoc.org/go.gazette.dev/core/consumer
.. _runconsumer: https://godoc.org/go.gazette.dev/core/mainboilerplate/runconsumer

Broker Protocol Buffers & gRPC Service
---------------------------------------

Browse `broker/protocol.proto`_ on Sourcegraph.

.. _broker/protocol.proto: https://sourcegraph.com/github.com/gazette/core/-/blob/broker/protocol/protocol.proto

Consumer Protocol Buffers & gRPC Service
-----------------------------------------

Browse `consumer/protocol.proto`_ on Sourcegraph.

.. _consumer/protocol.proto: https://sourcegraph.com/github.com/gazette/core/-/blob/consumer/protocol/protocol.proto
