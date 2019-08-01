// Package client provides implementation for clients of the Journal API.
// Notably, it provides io.Reader and io.Writer implementations which map to
// respective broker RPCs. The AppendService builds upon Appender to provide
// asynchronous appends, fine-grain append dependency management, and batching
// of small writes into larger ones. RouteCache is also provided for
// protocol.DispatchRouter compatible caching of discovered Journal Routes,
// allowing clients to direct requests to known primary and/or same-zone brokers,
// reducing the number of proxy hops required and, where possible, keeping
// network traffic within a single networking zone.
package client
