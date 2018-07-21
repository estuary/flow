// Package protocol defines the core Gazette datamodel, validation behaviors,
// and gRPC APIs which are shared across clients and broker servers. Datamodel
// types and APIs are implemented as generated protobuf messages and stubs,
// typically extended with additional parsing, validation, and shared
// implementation behaviors. A central goal of this package to be highly
// exacting in defining allowed "shapes" that types & messages may take (through
// implementations of the Validator interface), providing strong guarantees to
// brokers and clients that messages are well-formed without need for additional
// ad-hoc, repetitive checks (which often become a maintenance burden).
//
//go:generate protoc -I . -I ../../vendor  --gogo_out=plugins=grpc:. protocol.proto
package protocol
