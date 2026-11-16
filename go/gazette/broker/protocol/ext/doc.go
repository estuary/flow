// Package ext defines extensions to broker/protocol that depend on keyspace
// and allocator (which in turn depend on etcd).  This package has been separated
// from broker/protocol and broker/client to avoid incurring the dependency on
// etcd for client users.
package ext
