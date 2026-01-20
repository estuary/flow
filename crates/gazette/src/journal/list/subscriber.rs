use super::Fold;
use proto_gazette::broker;
use std::cmp::Ordering;
use std::future::Future;

/// Subscriber receives journal addition and removal notifications during list operations.
///
/// Implementations receive callbacks as journals are added to or removed from the listing.
pub trait Subscriber: Send {
    /// Called when a journal is added to the listing.
    fn add_journal(
        &mut self,
        create_revision: i64,
        journal_spec: broker::JournalSpec,
        mod_revision: i64,
        route: broker::Route,
    ) -> impl Future<Output = crate::Result<()>> + Send;

    /// Called when a journal is removed from the listing.
    fn remove_journal(&mut self, name: String) -> impl Future<Output = crate::Result<()>> + Send;
}

/// A Fold implementation that drives a Subscriber with journal additions and removals.
///
/// SubscriberFold performs a streaming sorted merge between the previous snapshot
/// and incoming chunks to identify which journals have been added or removed.
/// Changes are delivered to the subscriber as each chunk is processed, not accumulated.
///
/// Partial snapshots are preserved across retry attempts - if a chunk fails partway through,
/// subsequent retries will continue from where the subscriber left off rather than replaying
/// all notifications from the beginning.
pub struct SubscriberFold<S: Subscriber> {
    subscriber: S,
    state: State,
}

enum State {
    /// Ready to begin a new snapshot. Contains journal names from the last
    /// successful snapshot (used for diff computation).
    Ready { previous: PackedStrings },

    /// Currently processing a snapshot. Performs streaming merge between
    /// `previous` and incoming chunks.
    Merging {
        /// Current position in previous (index-based iteration).
        previous_index: usize,
        /// Journal names from the previous snapshot.
        previous: PackedStrings,
        /// Last decoded value from previous (empty or `previous_index` - 1).
        previous_last: String,
        /// Journal names from the current snapshot being built.
        current: PackedStrings,
        /// Tail (last) value in current (for delta-encoding next entry).
        current_tail: String,
        /// Running count of additions.
        added: usize,
        /// Running count of removals.
        removed: usize,
    },
}

/// Compact storage for delta-encoded strings.
///
/// Stores strings by only keeping the suffix that differs from the previous string.
/// All suffixes are stored contiguously in a single `String`, with `entries`
/// tracking the prefix length and suffix end position for each entry.
/// This avoids per-entry String allocations and improves cache locality.
#[derive(Default)]
struct PackedStrings {
    /// All suffixes concatenated.
    data: String,
    /// Each entry: (prefix_len, suffix_end).
    /// suffix_start for entry i is suffix_end of entry i-1 (or 0 for i=0).
    entries: Vec<(u32, u32)>,
}

impl<S: Subscriber> SubscriberFold<S> {
    pub fn new(subscriber: S) -> Self {
        Self {
            subscriber,
            state: State::Ready {
                previous: PackedStrings::default(),
            },
        }
    }
}

impl<S: Subscriber> Fold for SubscriberFold<S> {
    type Output = (usize, usize);

    async fn begin(&mut self) {
        self.state = match std::mem::replace(
            &mut self.state,
            State::Ready {
                previous: PackedStrings::default(),
            },
        ) {
            // Normal case: transitioning from Ready to Merging.
            State::Ready { previous } => State::Merging {
                previous_index: 0,
                previous,
                previous_last: String::new(),
                current: PackedStrings::default(),
                current_tail: String::new(),
                added: 0,
                removed: 0,
            },

            // Retry case: failed mid-snapshot. Preserve progress by building
            // new_previous = current ++ previous[previous_index..].
            // This reflects subscriber's current knowledge of which journals exist.
            State::Merging {
                previous: old_previous,
                previous_index: old_previous_index,
                previous_last: mut old_previous_last,
                current,
                current_tail: mut current_last,
                added,
                removed,
            } => {
                let mut new_previous = PackedStrings::default();
                let mut new_previous_tail = String::new();

                // Subscriber has been notified of all changes through `current`.
                // Iterate over it, adding all entries to `new_previous`.
                current_last.clear();
                for i in 0..current.len() {
                    current.decode(i, &mut current_last);
                    new_previous
                        .encode(&current_last, &mut new_previous_tail)
                        .expect("re-encoding previously valid data");
                }

                // Append remaining `old_previous` (which Subscriber still thinks exist).
                // Since journals arrive sorted: max(current) < min(old_previous[old_previous_index..])
                for i in old_previous_index..old_previous.len() {
                    old_previous.decode(i, &mut old_previous_last);
                    new_previous
                        .encode(&old_previous_last, &mut new_previous_tail)
                        .expect("re-encoding previously valid data");
                }

                State::Merging {
                    previous_index: 0,
                    previous: new_previous,
                    previous_last: String::new(),
                    current: PackedStrings::default(),
                    current_tail: String::new(),
                    added,
                    removed,
                }
            }
        };
    }

    async fn chunk(&mut self, resp: broker::ListResponse) -> crate::Result<()> {
        let State::Merging {
            previous_index,
            previous,
            previous_last,
            current,
            current_tail,
            added,
            removed,
        } = &mut self.state
        else {
            return Err(crate::Error::Protocol("chunk called outside Merging state"));
        };

        for entry in resp.journals {
            let spec = entry.spec.ok_or(crate::Error::Protocol(
                "broker::list_response::Journal missing spec",
            ))?;
            let route = entry.route.ok_or(crate::Error::Protocol(
                "broker::list_response::Journal missing route",
            ))?;

            let name = spec.name;
            let mut found_in_previous = false;

            // Journal entries must arrive in sorted order for the merge algorithm to work.
            if &name <= current_tail {
                return Err(crate::Error::Protocol(
                    "broker::ListResponse is not in sorted order",
                ));
            }

            // Merge: emit removals for previous entries < name, find if name exists in previous.
            while *previous_index < previous.len() {
                previous.decode(*previous_index, previous_last);

                match (*previous_last).cmp(&name) {
                    Ordering::Less => {
                        // Previous journal not in current snapshot: removed.
                        self.subscriber
                            .remove_journal(previous_last.clone())
                            .await?;
                        *removed += 1;
                        *previous_index += 1;
                    }
                    Ordering::Equal => {
                        // Journal exists in both snapshots: no change.
                        found_in_previous = true;
                        *previous_index += 1;
                        break;
                    }
                    Ordering::Greater => {
                        // Previous is ahead: current journal is new.
                        break;
                    }
                }
            }

            if !found_in_previous {
                // Journal is new: notify subscriber.
                self.subscriber
                    .add_journal(
                        entry.create_revision,
                        broker::JournalSpec {
                            name: name.clone(),
                            ..spec
                        },
                        entry.mod_revision,
                        route,
                    )
                    .await?;
                *added += 1;
            }

            // Add to current snapshot (delta-encoded against previous entry).
            // This must happen after add_journal succeeds, so that on failure
            // a retry will correctly identify this journal as new.
            current.encode(&name, current_tail)?;
        }

        Ok(())
    }

    async fn finish(&mut self) -> crate::Result<Self::Output> {
        let State::Merging {
            previous_index,
            previous,
            previous_last,
            current,
            current_tail: _,
            added,
            removed,
        } = &mut self.state
        else {
            return Err(crate::Error::Protocol(
                "finish called outside Merging state",
            ));
        };

        // Emit removals for any remaining previous entries.
        // On failure, state remains Merging so begin() can recover.
        loop {
            if *previous_index >= previous.len() {
                break;
            }

            previous.decode(*previous_index, previous_last);
            self.subscriber
                .remove_journal(previous_last.clone())
                .await?;
            *removed += 1;
            *previous_index += 1;
        }

        // `current` is completed, and becomes `previous` for the next snapshot.
        let (added, removed) = (*added, *removed);
        self.state = State::Ready {
            previous: std::mem::take(current),
        };

        Ok((added, removed))
    }
}

impl PackedStrings {
    fn len(&self) -> usize {
        self.entries.len()
    }

    /// Encode and append a string, delta-encoded against `tail`, then update
    /// `tail` to contain the appended value for the next call.
    ///
    /// Returns an error if the accumulated data exceeds 4GB.
    fn encode(&mut self, s: &str, tail: &mut String) -> crate::Result<()> {
        let prefix_len: usize = s
            .chars()
            .zip(tail.chars())
            .take_while(|(a, b)| a == b)
            .map(|(c, _)| c.len_utf8())
            .sum();

        let suffix = &s[prefix_len..];
        self.data.push_str(suffix);

        let suffix_end = u32::try_from(self.data.len())
            .map_err(|_| crate::Error::Protocol("PackedStrings data exceeds 4GB"))?;

        self.entries.push((prefix_len as u32, suffix_end));

        tail.truncate(prefix_len);
        tail.push_str(suffix);
        Ok(())
    }

    /// Decode entry at `index` into `tail`.
    ///
    /// For correct sequential decoding, `tail` should contain the previously
    /// decoded value (entry at index-1, or empty if index=0). It's also safe
    /// to re-decode the same value into `tail` multiple times.
    fn decode(&self, index: usize, tail: &mut String) {
        let (prefix_len, suffix_end) = self.entries[index];
        let suffix_start = if index == 0 {
            0
        } else {
            self.entries[index - 1].1 as usize
        };
        let suffix = &self.data[suffix_start..suffix_end as usize];

        tail.truncate(prefix_len as usize);
        tail.push_str(suffix);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Mock subscriber that records all add/remove calls.
    struct MockSubscriber {
        adds: Vec<String>,
        removes: Vec<String>,
        /// If set, fail after this many operations.
        fail_after: Option<usize>,
        op_count: usize,
    }

    impl MockSubscriber {
        fn new() -> Self {
            Self {
                adds: Vec::new(),
                removes: Vec::new(),
                fail_after: None,
                op_count: 0,
            }
        }

        fn reset(&mut self) {
            self.adds.clear();
            self.removes.clear();
            self.fail_after = None;
            self.op_count = 0;
        }

        fn set_fail_after(&mut self, n: usize) {
            self.fail_after = Some(n);
            self.op_count = 0;
        }

        fn check_fail(&mut self) -> crate::Result<()> {
            self.op_count += 1;
            if let Some(limit) = self.fail_after {
                if self.op_count > limit {
                    return Err(crate::Error::Protocol("simulated failure"));
                }
            }
            Ok(())
        }
    }

    impl Subscriber for MockSubscriber {
        async fn add_journal(
            &mut self,
            _create_revision: i64,
            journal_spec: broker::JournalSpec,
            _mod_revision: i64,
            _route: broker::Route,
        ) -> crate::Result<()> {
            self.check_fail()?;
            self.adds.push(journal_spec.name);
            Ok(())
        }

        async fn remove_journal(&mut self, name: String) -> crate::Result<()> {
            self.check_fail()?;
            self.removes.push(name);
            Ok(())
        }
    }

    fn make_journal(name: &str) -> broker::list_response::Journal {
        broker::list_response::Journal {
            spec: Some(broker::JournalSpec {
                name: name.to_string(),
                ..Default::default()
            }),
            route: Some(broker::Route::default()),
            create_revision: 1,
            mod_revision: 1,
        }
    }

    fn make_response(names: &[&str]) -> broker::ListResponse {
        broker::ListResponse {
            journals: names.iter().map(|n| make_journal(n)).collect(),
            ..Default::default()
        }
    }

    /// Helper to run a complete snapshot through fold.
    async fn run_snapshot(
        fold: &mut SubscriberFold<MockSubscriber>,
        chunks: &[&[&str]],
    ) -> crate::Result<(usize, usize)> {
        fold.begin().await;
        for chunk in chunks {
            fold.chunk(make_response(chunk)).await?;
        }
        fold.finish().await
    }

    // ==================== Basic Operations ====================

    #[tokio::test]
    async fn test_basic_operations() {
        let mut fold = SubscriberFold::new(MockSubscriber::new());

        // First snapshot: all adds
        let (added, removed) = run_snapshot(&mut fold, &[&["a", "b", "c"]]).await.unwrap();
        assert_eq!((added, removed), (3, 0));
        assert_eq!(fold.subscriber.adds, vec!["a", "b", "c"]);

        fold.subscriber.reset();

        // Second snapshot: no changes
        let (added, removed) = run_snapshot(&mut fold, &[&["a", "b", "c"]]).await.unwrap();
        assert_eq!((added, removed), (0, 0));
        assert!(fold.subscriber.adds.is_empty());
        assert!(fold.subscriber.removes.is_empty());

        fold.subscriber.reset();

        // Third snapshot: mixed adds/removes
        let (added, removed) = run_snapshot(&mut fold, &[&["b", "d"]]).await.unwrap();
        assert_eq!((added, removed), (1, 2));
        assert_eq!(fold.subscriber.adds, vec!["d"]);
        assert_eq!(fold.subscriber.removes, vec!["a", "c"]);

        fold.subscriber.reset();

        // Empty snapshot: all removed
        let (added, removed) = run_snapshot(&mut fold, &[]).await.unwrap();
        assert_eq!((added, removed), (0, 2));
        assert_eq!(fold.subscriber.removes, vec!["b", "d"]);
    }

    #[tokio::test]
    async fn test_multiple_chunks() {
        let mut fold = SubscriberFold::new(MockSubscriber::new());

        // Snapshot delivered in multiple chunks
        let (added, removed) = run_snapshot(&mut fold, &[&["a", "b"], &["c", "d"], &["e"]])
            .await
            .unwrap();
        assert_eq!((added, removed), (5, 0));
        assert_eq!(fold.subscriber.adds, vec!["a", "b", "c", "d", "e"]);
    }

    #[tokio::test]
    async fn test_removes_emitted_during_chunk() {
        let mut fold = SubscriberFold::new(MockSubscriber::new());

        // First snapshot: a, c, e
        run_snapshot(&mut fold, &[&["a", "c", "e"]]).await.unwrap();
        fold.subscriber.reset();

        // Second snapshot: b, d, f (interleaved - triggers removals during chunk)
        let (added, removed) = run_snapshot(&mut fold, &[&["b", "d", "f"]]).await.unwrap();
        assert_eq!((added, removed), (3, 3));
        // a removed when seeing b, c removed when seeing d, e removed in finish
        assert_eq!(fold.subscriber.removes, vec!["a", "c", "e"]);
        assert_eq!(fold.subscriber.adds, vec!["b", "d", "f"]);
    }

    // ==================== Input Validation ====================

    #[tokio::test]
    async fn test_input_validation() {
        let mut fold = SubscriberFold::new(MockSubscriber::new());

        // Unsorted input
        fold.begin().await;
        let result = fold.chunk(make_response(&["b", "a"])).await;
        assert!(matches!(result, Err(crate::Error::Protocol(_))));

        // Duplicate across chunks
        let mut fold = SubscriberFold::new(MockSubscriber::new());
        fold.begin().await;
        fold.chunk(make_response(&["a", "b"])).await.unwrap();
        let result = fold.chunk(make_response(&["b", "c"])).await;
        assert!(matches!(result, Err(crate::Error::Protocol(_))));

        // Missing spec
        let mut fold = SubscriberFold::new(MockSubscriber::new());
        fold.begin().await;
        let mut resp = make_response(&["a"]);
        resp.journals[0].spec = None;
        let result = fold.chunk(resp).await;
        assert!(matches!(result, Err(crate::Error::Protocol(_))));

        // Missing route
        let mut fold = SubscriberFold::new(MockSubscriber::new());
        fold.begin().await;
        let mut resp = make_response(&["a"]);
        resp.journals[0].route = None;
        let result = fold.chunk(resp).await;
        assert!(matches!(result, Err(crate::Error::Protocol(_))));
    }

    // ==================== Failure Recovery ====================

    #[tokio::test]
    async fn test_failure_recovery_during_chunk() {
        let mut fold = SubscriberFold::new(MockSubscriber::new());

        // Setup: a, b, c, d, e
        run_snapshot(&mut fold, &[&["a", "b", "c", "d", "e"]])
            .await
            .unwrap();
        fold.subscriber.reset();

        // Fail after 2 ops during transition to [c, d, e, f, g]
        // Operations: remove a (1), remove b (2), then fail on add f (3)
        fold.subscriber.set_fail_after(2);
        fold.begin().await;
        assert!(
            fold.chunk(make_response(&["c", "d", "e", "f", "g"]))
                .await
                .is_err()
        );
        assert_eq!(fold.subscriber.removes, vec!["a", "b"]);
        assert!(fold.subscriber.adds.is_empty());

        // Retry succeeds - subscriber state was [c,d,e], now sees [c,d,e,f,g]
        // Counts accumulate: 2 removes from first attempt + 2 adds from retry
        fold.subscriber.reset();
        let (added, removed) = run_snapshot(&mut fold, &[&["c", "d", "e", "f", "g"]])
            .await
            .unwrap();
        assert_eq!((added, removed), (2, 2));
        assert_eq!(fold.subscriber.adds, vec!["f", "g"]);
    }

    #[tokio::test]
    async fn test_failure_recovery_during_finish() {
        let mut fold = SubscriberFold::new(MockSubscriber::new());

        // Setup: a, b, c, d
        run_snapshot(&mut fold, &[&["a", "b", "c", "d"]])
            .await
            .unwrap();
        fold.subscriber.reset();

        // Transition to [a] - removals happen in finish()
        // Fail after removing b
        fold.subscriber.set_fail_after(1);
        fold.begin().await;
        fold.chunk(make_response(&["a"])).await.unwrap();
        assert!(fold.finish().await.is_err());
        assert_eq!(fold.subscriber.removes, vec!["b"]);

        // Retry - subscriber has [a, c, d] (b was removed)
        // Counts accumulate: 1 remove from first attempt + 2 removes from retry
        fold.subscriber.reset();
        let (added, removed) = run_snapshot(&mut fold, &[&["a"]]).await.unwrap();
        assert_eq!((added, removed), (0, 3));
        assert_eq!(fold.subscriber.removes, vec!["c", "d"]);
    }

    #[tokio::test]
    async fn test_failure_at_first_operation() {
        let mut fold = SubscriberFold::new(MockSubscriber::new());

        // Setup: a, b, c
        run_snapshot(&mut fold, &[&["a", "b", "c"]]).await.unwrap();
        fold.subscriber.reset();

        // Fail immediately on first removal
        fold.subscriber.set_fail_after(0);
        fold.begin().await;
        assert!(fold.chunk(make_response(&["d"])).await.is_err());
        assert!(fold.subscriber.removes.is_empty());

        // Retry succeeds with full state intact
        fold.subscriber.reset();
        let (added, removed) = run_snapshot(&mut fold, &[&["d"]]).await.unwrap();
        assert_eq!((added, removed), (1, 3));
        assert_eq!(fold.subscriber.removes, vec!["a", "b", "c"]);
    }

    #[tokio::test]
    async fn test_failure_on_add() {
        let mut fold = SubscriberFold::new(MockSubscriber::new());

        // Setup: a, c
        run_snapshot(&mut fold, &[&["a", "c"]]).await.unwrap();
        fold.subscriber.reset();

        // Transition to [a, b, c] - fail on adding b
        fold.subscriber.set_fail_after(0);
        fold.begin().await;
        assert!(fold.chunk(make_response(&["a", "b", "c"])).await.is_err());
        assert!(fold.subscriber.adds.is_empty());

        // Retry - b should still be added
        fold.subscriber.reset();
        let (added, removed) = run_snapshot(&mut fold, &[&["a", "b", "c"]]).await.unwrap();
        assert_eq!((added, removed), (1, 0));
        assert_eq!(fold.subscriber.adds, vec!["b"]);
    }

    #[tokio::test]
    async fn test_multiple_consecutive_retries() {
        let mut fold = SubscriberFold::new(MockSubscriber::new());

        // Setup: a, b, c, d, e
        run_snapshot(&mut fold, &[&["a", "b", "c", "d", "e"]])
            .await
            .unwrap();
        fold.subscriber.reset();

        // First attempt: fail after removing a
        fold.subscriber.set_fail_after(1);
        fold.begin().await;
        let _ = fold.chunk(make_response(&["x"])).await;
        assert_eq!(fold.subscriber.removes, vec!["a"]);

        // Second attempt: fail after removing b
        fold.subscriber.reset();
        fold.subscriber.set_fail_after(1);
        fold.begin().await;
        let _ = fold.chunk(make_response(&["x"])).await;
        assert_eq!(fold.subscriber.removes, vec!["b"]);

        // Third attempt succeeds
        // Counts accumulate: 1 remove (a) + 1 remove (b) + 3 removes + 1 add = (1, 5)
        fold.subscriber.reset();
        let (added, removed) = run_snapshot(&mut fold, &[&["x"]]).await.unwrap();
        assert_eq!((added, removed), (1, 5));
        assert_eq!(fold.subscriber.removes, vec!["c", "d", "e"]);
    }

    #[tokio::test]
    async fn test_retry_with_changed_server_state() {
        let mut fold = SubscriberFold::new(MockSubscriber::new());

        // Setup: a, b, c
        run_snapshot(&mut fold, &[&["a", "b", "c"]]).await.unwrap();
        fold.subscriber.reset();

        // First attempt targets [d, e], fail after removing a
        fold.subscriber.set_fail_after(1);
        fold.begin().await;
        let _ = fold.chunk(make_response(&["d", "e"])).await;
        assert_eq!(fold.subscriber.removes, vec!["a"]);

        // Retry with different server state [c, d]
        // Subscriber thinks it has [b, c]
        // Counts accumulate: 1 remove (a) from first attempt + 1 remove (b) + 1 add (d) = (1, 2)
        fold.subscriber.reset();
        let (added, removed) = run_snapshot(&mut fold, &[&["c", "d"]]).await.unwrap();
        assert_eq!((added, removed), (1, 2));
        assert_eq!(fold.subscriber.removes, vec!["b"]);
        assert_eq!(fold.subscriber.adds, vec!["d"]);
    }

    #[tokio::test]
    async fn test_interleaved_failure_recovery() {
        let mut fold = SubscriberFold::new(MockSubscriber::new());

        // Setup: a, c, e
        run_snapshot(&mut fold, &[&["a", "c", "e"]]).await.unwrap();
        fold.subscriber.reset();

        // Transition to [b, d, f] (interleaved)
        // Operations: remove a (1), add b (2), remove c (3), add d (4) - FAIL
        fold.subscriber.set_fail_after(3);
        fold.begin().await;
        assert!(fold.chunk(make_response(&["b", "d", "f"])).await.is_err());
        assert_eq!(fold.subscriber.removes, vec!["a", "c"]);
        assert_eq!(fold.subscriber.adds, vec!["b"]);

        // Retry - subscriber has [b, e]
        // Counts accumulate: 2 removes + 1 add from first attempt, + 2 adds + 1 remove from retry = (3, 3)
        fold.subscriber.reset();
        let (added, removed) = run_snapshot(&mut fold, &[&["b", "d", "f"]]).await.unwrap();
        assert_eq!((added, removed), (3, 3));
        assert_eq!(fold.subscriber.adds, vec!["d", "f"]);
        assert_eq!(fold.subscriber.removes, vec!["e"]);
    }

    // ==================== PackedStrings ====================

    #[tokio::test]
    async fn test_packed_strings() {
        let mut ps = PackedStrings::default();
        let mut tail = String::new();

        // Basic prefix sharing
        ps.encode("topics/users/alice", &mut tail).unwrap();
        ps.encode("topics/users/bob", &mut tail).unwrap();
        ps.encode("topics/users/carol", &mut tail).unwrap();
        ps.encode("other/path", &mut tail).unwrap();

        // Verify sequential decoding
        tail.clear();
        ps.decode(0, &mut tail);
        assert_eq!(tail, "topics/users/alice");
        ps.decode(1, &mut tail);
        assert_eq!(tail, "topics/users/bob");

        // Decode is idempotent for same index (used in retry scenarios)
        ps.decode(1, &mut tail);
        assert_eq!(tail, "topics/users/bob");

        ps.decode(2, &mut tail);
        assert_eq!(tail, "topics/users/carol");
        ps.decode(3, &mut tail);
        assert_eq!(tail, "other/path");
    }

    #[tokio::test]
    async fn test_packed_strings_utf8() {
        // 2-byte UTF-8 (Latin extended)
        let mut ps = PackedStrings::default();
        let mut tail = String::new();
        ps.encode("café/menu/drinks", &mut tail).unwrap();
        ps.encode("café/menu/food", &mut tail).unwrap();
        ps.encode("café/staff", &mut tail).unwrap();

        tail.clear();
        ps.decode(0, &mut tail);
        assert_eq!(tail, "café/menu/drinks");
        ps.decode(1, &mut tail);
        assert_eq!(tail, "café/menu/food");
        ps.decode(2, &mut tail);
        assert_eq!(tail, "café/staff");

        // 3-byte UTF-8 (CJK)
        let mut ps = PackedStrings::default();
        tail.clear();
        ps.encode("日本/東京/渋谷", &mut tail).unwrap();
        ps.encode("日本/東京/新宿", &mut tail).unwrap();
        ps.encode("日本/大阪", &mut tail).unwrap();

        tail.clear();
        ps.decode(0, &mut tail);
        assert_eq!(tail, "日本/東京/渋谷");
        ps.decode(1, &mut tail);
        assert_eq!(tail, "日本/東京/新宿");
        ps.decode(2, &mut tail);
        assert_eq!(tail, "日本/大阪");

        // 4-byte UTF-8 (emoji)
        let mut ps = PackedStrings::default();
        tail.clear();
        ps.encode("music/🎵/classical", &mut tail).unwrap();
        ps.encode("music/🎵/jazz", &mut tail).unwrap();
        ps.encode("music/🎸/rock", &mut tail).unwrap();

        tail.clear();
        ps.decode(0, &mut tail);
        assert_eq!(tail, "music/🎵/classical");
        ps.decode(1, &mut tail);
        assert_eq!(tail, "music/🎵/jazz");
        ps.decode(2, &mut tail);
        assert_eq!(tail, "music/🎸/rock");

        // Prefix boundary with multi-byte chars
        let mut ps = PackedStrings::default();
        tail.clear();
        ps.encode("données_a", &mut tail).unwrap();
        ps.encode("données_b", &mut tail).unwrap();

        tail.clear();
        ps.decode(0, &mut tail);
        assert_eq!(tail, "données_a");
        ps.decode(1, &mut tail);
        assert_eq!(tail, "données_b");

        // Different multi-byte chars after shared prefix
        let mut ps = PackedStrings::default();
        tail.clear();
        ps.encode("test/α", &mut tail).unwrap();
        ps.encode("test/β", &mut tail).unwrap();
        ps.encode("test/γ", &mut tail).unwrap();

        tail.clear();
        ps.decode(0, &mut tail);
        assert_eq!(tail, "test/α");
        ps.decode(1, &mut tail);
        assert_eq!(tail, "test/β");
        ps.decode(2, &mut tail);
        assert_eq!(tail, "test/γ");
    }

    #[tokio::test]
    async fn test_failure_recovery_with_prefix_sharing() {
        let mut fold = SubscriberFold::new(MockSubscriber::new());

        // Setup with long shared prefixes
        run_snapshot(
            &mut fold,
            &[&[
                "a/very/long/shared/prefix/alice",
                "a/very/long/shared/prefix/bob",
                "a/very/long/shared/prefix/carol",
                "a/very/long/shared/prefix/dave",
            ]],
        )
        .await
        .unwrap();
        fold.subscriber.reset();

        // Fail mid-way through removals
        fold.subscriber.set_fail_after(2);
        fold.begin().await;
        let _ = fold.chunk(make_response(&["z"])).await;
        assert_eq!(
            fold.subscriber.removes,
            vec![
                "a/very/long/shared/prefix/alice",
                "a/very/long/shared/prefix/bob"
            ]
        );

        // Retry - subscriber has [carol, dave]
        fold.subscriber.reset();
        let (added, removed) = run_snapshot(&mut fold, &[&["z"]]).await.unwrap();
        // Counts should reflect TOTAL changes across all attempts:
        // - alice, bob removed in first attempt (2)
        // - carol, dave removed in retry (2)
        // - z added in retry (1)
        assert_eq!((added, removed), (1, 4));
        assert_eq!(
            fold.subscriber.removes,
            vec![
                "a/very/long/shared/prefix/carol",
                "a/very/long/shared/prefix/dave"
            ]
        );
    }
}
