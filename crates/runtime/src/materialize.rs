// Notes on how we can structure materialize middleware:
//
// Request loop:
//  - Spec / Validate / Apply: Unseal. Forward.
//  - Open: Rebuild State. Unseal. Forward.
//  - Load: Acquire shared combiners & combine-right. Forward request iff key is new & not cached.
//  - Flush: Forward.
//      Block awaiting Flushed notification from response loop.
//      Acquire state combiners and drain combiners into forwarded Store requests.
//      Send Flushed stats to response loop.
//  - StartCommit: Forward.
//  - Acknowledge: Forward.
//
//  (Note that Store is never received from Go runtime).
//
// Response loop:
//  - Spec / Validated / Applied / Opened: Forward.
//  - Loaded: Acquire shared combiners & reduce-left.
//  - Flushed:
//       Send Flushed notification to request loop.
//       Block awaiting Flushed stats from request loop.
//       Forward Flushed to runtime enhanced with stats.
//  - StartedCommit: Forward.
//  - Acknowledged: Forward.
