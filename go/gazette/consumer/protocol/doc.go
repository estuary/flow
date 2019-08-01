// Package protocol defines the consumer datamodel, validation behaviors,
// and gRPC APIs which are shared across clients and consumer application servers.
// Datamodel types and APIs are implemented as generated protobuf messages and
// stubs, typically extended with additional parsing, validation, and shared
// implementation behaviors. A central goal of this package to be highly
// exacting in defining allowed "shapes" that types & messages may take (through
// implementations of the Validator interface), providing strong guarantees to
// consumers and clients that messages are well-formed without need for additional
// ad-hoc, repetitive checks (which often become a maintenance burden).
//
// By convention, this package is usually imported as `pc`, short for
// "Protocol of Consumer", due to it's ubiquity and to distinguish it from
// package go.gazette.dev/core/broker/protocol (imported as `pb`). Eg,
//
// import pc "go.gazette.dev/core/consumer/protocol"
//
package protocol
