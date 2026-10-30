// Package protocol defines the core broker datamodel, validation behaviors,
// and gRPC APIs which are shared across clients and broker servers. Datamodel
// types and APIs are implemented as generated protobuf messages and stubs,
// typically extended with additional parsing, validation, and shared
// implementation behaviors. A central goal of this package to be highly
// exacting in defining allowed "shapes" that types & messages may take (through
// implementations of the Validator interface), providing strong guarantees to
// brokers and clients that messages are well-formed without need for additional
// ad-hoc, repetitive checks (which often become a maintenance burden).
//
// The package also implements a gRPC "dispatcher" load balancer which provides
// a tight integration between available routes of a journal and gRPC's
// selection of an appropriate transport for a given RPC. Use of the balancer,
// identified by DispatcherGRPCBalancerName, allows clients and servers to use
// a single *grpc.ClientConn through which all RPCs are dispatched. Context
// metadata, attached via WithDispatch*(), informs the balancer of the set of
// specific servers suitable for serving a request. The balancer can factor
// considerations such as which servers have ready transports, or whether the
// RPC will cross availability zones to make a final transport selection.
//
// By convention, this package is usually imported as `pb`, short for
// "Protocol of Broker", due to it's ubiquity and to distinguish it from
// package go.gazette.dev/core/consumer/protocol (imported as `pc`). Eg,
//
// import pb "go.gazette.dev/core/broker/protocol"
package protocol
