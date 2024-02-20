Background
==========

Flow offers fully serialized transactions across one or more Flow collections.

Collections are represented within the data plane as one or more physical partitions, each of which is an append-only byte stream. Partitions may be read from any arbitrary byte offset, and may be appended to at the "write head", which is the largest offset written to-date and the next offset to be written by appended content. While collection partitions are byte-oriented, Flow always writes in atomic chunks of one or more documents.

Writes to collection partitions are at-least-once, so Flow layers transaction semantics on top of these at-least-once writes by including sequencing information within a UUID attached to each document (commonly known as the "meta UUID"). Meta UUIDs are V1 UUIDs and include 1. a unique producer ID, 2. a monotonic Clock which is lower-bounded by the current wall-click time, and is further ticked with every generated UUID to ensure monotonicity, and 3. various bit flags which represent transaction semantics.
