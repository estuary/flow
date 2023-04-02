// Notes on how we can structure capture middleware:

// Request loop:
//  - Spec / Discover / Validate / Apply: Unseal. Forward request.
//  - Open: Rebuild State. Unseal. Retain explicit-ack. Forward request.
//  - Acknowledge: Notify response loop. Forward iff explicit-ack.

// Response loop:
//  - Spec / Discovered / Validated / Applied: Forward response.
//  - Opened: Acquire State. Re-init combiners. Forward response.
//  - Captured: Validate & add to combiner.
//  - Checkpoint: Reduce checkpoint.
//      If "full": block until Acknowledge notification is ready.
//      If Acknowledge notification is ready:
//          Drain combiner into forwarded Captured.
//          Forward Checkpoint enriched with stats.
