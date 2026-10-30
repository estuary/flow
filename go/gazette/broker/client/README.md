# broker/client

The `broker/client` module provides a comprehensive Go client for interacting with the gRPC Journal service of Gazette brokers. It handles journal specifications, fragments, and byte-streams with support for efficient batching, automatic retries, and performance optimizations.

## Purpose

This module bridges application code and Gazette brokers, providing high-level abstractions for reading from and writing to journals while handling the complexities of distributed systems like routing, retries, and connection management.

## Architecture

The module is organized around four main concerns:

### Reading
- **Reader**: Basic `io.Reader` adapter for single Read RPCs
- **RetryReader**: Resilient reader that automatically restarts Read RPCs on failure

### Writing  
- **Appender**: Direct `io.WriteCloser` adapter for Append RPCs with linearizable semantics
- **AppendService**: Production-ready service with automatic batching, retries, and async operations

### Discovery & Routing
- **WatchedList**: Maintains up-to-date journal listings for dynamic journal sets
- **RouteCache**: Optimizes RPC routing by caching broker assignments

## Key Types

### Reader Types
```go
// Single-use reader for one RPC
reader := NewReader(ctx, client, pb.ReadRequest{
    Journal: "logs/clicks",
    Offset:  1234,
})

// Resilient reader with automatic restarts
retryReader := NewRetryReader(ctx, client, pb.ReadRequest{
    Journal: "logs/clicks", 
})
```

### Writer Types
```go
// Single-use appender
appender := NewAppender(ctx, client, pb.AppendRequest{
    Journal: "logs/events",
})

// Production service with batching and automatic retries
service := NewAppendService(ctx, client)
op := service.StartAppend(pb.AppendRequest{
    Journal: "logs/events",
}, dependencies...)
```

### Discovery
```go
// Watch for journal changes
watchedList := NewWatchedList(ctx, client, pb.ListRequest{
    Selector: labelSelector,
})

// Cache routes for performance
routeCache := NewRouteCache(256, time.Hour)
routedClient := pb.NewRoutedJournalClient(client, routeCache)
```

## Essential Usage Patterns

### Simple Reading
Use `Reader` for single-shot reads where you don't need resilience:
```go
io.Copy(os.Stdout, NewReader(ctx, client, pb.ReadRequest{
    Journal: "logs/events",
    Offset:  1000,
    EndOffset: 2000,
}))
```

### Production Reading  
Use `RetryReader` for long-running consumers that need automatic recovery:
```go
reader := NewRetryReader(ctx, client, pb.ReadRequest{
    Journal: "logs/events",
    Block:   true,
})
// Reader automatically restarts on RPC errors
```

### High-Throughput Writing
Use `AppendService` for production workloads with many small writes:
```go
service := NewAppendService(ctx, client)

for event := range events {
    op := service.StartAppend(pb.AppendRequest{
        Journal: event.Topic,
    })
    
    json.NewEncoder(op.Writer()).Encode(event)
    
    if err := op.Release(); err == nil {
        err = op.Err() // Blocks until committed
    }
}
```

### Dynamic Journal Discovery
Use `WatchedList` for applications that need to react to journal topology changes:
```go
watchedList := NewWatchedList(ctx, client, pb.ListRequest{
    Selector: protocol.ParseLabelSelector("logs=user-events"),
})

for range watchedList.UpdateCh() {
    journals := watchedList.List().Journals
    // React to journal set changes
}
```

## Performance Considerations

- **AppendService** batches concurrent writes and uses disk-backed buffers to minimize GC pressure
- **RouteCache** reduces network hops by directing requests to optimal brokers  
- **RetryReader** maintains read position across broker failovers for seamless recovery
- All operations include Prometheus metrics for observability

## Integration Notes

This module works with the broker protocol definitions (`broker/protocol`) and is commonly used alongside the `message` package for higher-level message abstractions. For consumer applications, see the `consumer` module which builds upon these client primitives.