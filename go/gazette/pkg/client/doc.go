// Package client provides implementation for clients of the broker API.
// Notably, it provides io.Reader and io.Writer implementations which map to
// respective broker RPCs. It also provides a routing BrokerClient which directs
// requests to known primary and/or same-zone brokers, reducing the number of
// proxy hops required and, where possible, keeping network traffic within a
// single availability zone.
package client
