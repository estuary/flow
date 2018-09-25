// Package broker implements the broker runtime and protocol.JournalServer APIs
// (Read, Append, Replicate, List, Apply). Its `pipeline` type manages the
// coordination of write transactions, and `resolver` the mapping of journal
// names to Routes of responsible brokers. `replica` is a top-level collection
// of runtime state and maintenance tasks associated with the processing of a
// journal. gRPC proxy support is also implemented by this package.
package broker
