Gazette is a distributed byte-stream transaction engine. It models a concept of
"journals": infinite length, append-only files. See [Architecture Overview](
docs/architecture_overview.rst)

Package layout:

* `journal` contains all runtime components for Gazette journals,
 including Fragment & Spool for journal content, Head (serving replications),
 Tail (reads), and Broker (for brokering new writes).
* `message` is a client library providing components for layering message
 serialization upon a journal bytestream. Note that `message` is not used by
 the Gazette server runtime, and alternative message serializations are
 possible.  This package simply provides an opinionated default serialization.
* `topic` is a client library for topic descriptions and partitioned writes.
* `consumer` is a client library for distributed, stateful topic consumption.
* `async` implements a simple Promise API.
* `gazette` contains server components tied to the service lifetime, and clients.

