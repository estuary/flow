use super::{ReadLines, read::ReadyRead};
use futures::{StreamExt, future, stream};
use proto_gazette::uuid;

/// ReadHeap is the state of a Slice's ongoing journal reads: reads having a
/// ready document are in a prioritized heap, and all others are probing their
/// start or awaiting broker I/O.
pub struct ReadHeap {
    /// Set by the Session's InitialReadsStarted request, which it sends only
    /// after all initial StartRead's have been dispatched.
    initial_reads_started: bool,
    /// Probes of newly-started reads (see `read::start_read`).
    probes: stream::FuturesUnordered<future::BoxFuture<'static, Probed>>,
    /// Reads that are awaiting more data from Gazette brokers.
    pending: stream::FuturesUnordered<stream::StreamFuture<ReadLines>>,
    /// IDs of `pending` reads which aren't tailing: they're still behind
    /// their journal's write head, and may yet preempt the heap top.
    /// Every other pending read is tailing.
    stalled: std::collections::HashSet<u32>,
    /// Ready documents of reads, ordered on (priority, adjusted clock).
    ready: std::collections::BinaryHeap<ReadyReadEntry>,
}

/// Output of a read's probe: its resume and probed (fast-forwarded) offsets,
/// and the read, which begins at the probed offset.
pub type Probed = anyhow::Result<(i64, i64, ReadLines)>;

/// An event of a ReadHeap's probes or pending reads.
pub enum ReadEvent {
    /// A probe of a newly-started read resolved.
    Probed(Probed),
    /// A pending read resolved, and is no longer pending.
    Resolved {
        result: Option<gazette::RetryResult<gazette::journal::read::LinesBatch>>,
        read: ReadLines,
        /// Whether the read was stalled while pending.
        was_stalled: bool,
    },
}

/// The resolved status of the heap's top.
#[derive(Debug, Clone, Copy)]
pub enum ResolvedTop {
    /// A read may yet preempt a current provisional top.
    Unknown,
    /// The heap is empty, and every read is tailing or has stopped at the
    /// idempotent recovery frontier.
    Idle,
    /// The heap top awaits its read delay, and is due after this duration.
    Delayed(std::time::Duration),
    /// The heap top is due.
    Due,
}

/// ReadyReadEntry holds the ordering fields for a ReadyRead.
/// We keep this struct small to optimize heap sift operations.
pub struct ReadyReadEntry {
    /// Binding priority (higher = more urgent).
    pub priority: i32,
    /// Adjusted clock of the document (publication + read_delay).
    pub adjusted_clock: uuid::Clock,
    /// The actual document data, accessed by pointer indirection.
    /// Always `Some` in normal usage; `Option` allows test construction
    /// without a real ReadyRead (Ord only uses `priority` and `adjusted_clock`).
    pub inner: Option<Box<ReadyRead>>,
}

impl ReadHeap {
    pub fn new() -> Self {
        Self {
            initial_reads_started: false,
            probes: Default::default(),
            pending: Default::default(),
            stalled: Default::default(),
            ready: Default::default(),
        }
    }

    /// Determine the status of the heap's top, where `now` is lazily refreshed.
    /// This routine releases the top only if no future read could preempt it,
    /// which might happen if:
    ///
    /// - The Session hasn't told us all journal reads have been started
    /// - A newly-started read is still probing its initial status
    /// - A non-tailing journal, known to have more content, is pending I/O.
    ///
    /// Note that a clock-delayed top can still be preempted by resolution of
    /// a tailing non-delayed read. This is an accepted and best-effort race.
    pub fn resolved_top(&self, now: &mut uuid::Clock) -> ResolvedTop {
        assert!(self.stalled.len() <= self.pending.len());

        if !self.initial_reads_started || !self.probes.is_empty() || !self.stalled.is_empty() {
            return ResolvedTop::Unknown;
        }
        let Some(ReadyReadEntry { adjusted_clock, .. }) = self.ready.peek() else {
            return ResolvedTop::Idle;
        };

        match super::state::clock_delay(adjusted_clock, now, crate::now_clock) {
            Some(delay) => ResolvedTop::Delayed(delay),
            None => ResolvedTop::Due,
        }
    }

    /// The Session has told us that all initially-known reads have started.
    pub fn on_initial_reads_started(&mut self) {
        self.initial_reads_started = true;
    }

    /// Start a probe of a newly-started read.
    pub fn start_probe(&mut self, probe: future::BoxFuture<'static, Probed>) {
        self.probes.push(probe);
    }

    /// Push `read` into the reads awaiting broker I/O, returning whether it
    /// stalls further heap drain (equivalently: it's not tailing).
    pub fn push_pending(&mut self, read: ReadLines) -> bool {
        let stalled = !read.tailing();
        if stalled {
            let is_new = self.stalled.insert(read.id());
            debug_assert!(is_new, "read {} is already stalled", read.id());
        }
        self.pending.push(read.into_future());
        stalled
    }

    /// Next event of a probe or pending read. A resolved read is no longer
    /// pending, nor stalled. Never resolves if there are none.
    pub fn next_read_event(&mut self) -> impl Future<Output = ReadEvent> + '_ {
        std::future::poll_fn(|cx| {
            if let std::task::Poll::Ready(Some(probed)) = self.probes.poll_next_unpin(cx) {
                return std::task::Poll::Ready(ReadEvent::Probed(probed));
            }
            if let std::task::Poll::Ready(Some((result, read))) = self.pending.poll_next_unpin(cx) {
                let was_stalled = self.stalled.remove(&read.id());
                return std::task::Poll::Ready(ReadEvent::Resolved {
                    result,
                    read,
                    was_stalled,
                });
            }
            std::task::Poll::Pending
        })
    }

    pub fn push_ready(&mut self, entry: ReadyReadEntry) {
        self.ready.push(entry);
    }

    pub fn peek_ready(&self) -> Option<&ReadyReadEntry> {
        self.ready.peek()
    }

    pub fn pop_ready(&mut self) -> Option<ReadyReadEntry> {
        self.ready.pop()
    }

    /// Pending reads which are stalled.
    pub fn stalled_reads(&self) -> impl Iterator<Item = &ReadLines> {
        self.pending
            .iter()
            .filter_map(|read| read.get_ref())
            .filter(|read| self.stalled.contains(&read.id()))
    }

    /// Number of pending reads which are stalled.
    pub fn stalled_count(&self) -> usize {
        self.stalled.len()
    }

    /// Number of pending reads which are tailing.
    pub fn tailing_count(&self) -> usize {
        self.pending.len() - self.stalled.len()
    }

    /// Number of reads which are probing, pending, or in the heap.
    pub fn tracked_count(&self) -> usize {
        self.probes.len() + self.pending.len() + self.ready.len()
    }
}

impl std::fmt::Debug for ReadHeap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadHeap")
            .field("initial_reads_started", &self.initial_reads_started)
            .field("probes", &self.probes.len())
            .field("pending", &self.pending.len())
            .field("stalled", &self.stalled.len())
            .field("ready", &self.ready.len())
            .finish()
    }
}

impl Ord for ReadyReadEntry {
    #[inline]
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.priority
            .cmp(&other.priority)
            .then(self.adjusted_clock.cmp(&other.adjusted_clock).reverse())
    }
}

impl PartialOrd for ReadyReadEntry {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for ReadyReadEntry {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

impl Eq for ReadyReadEntry {}

#[cfg(test)]
mod test {
    use super::*;
    use std::cmp::Ordering;
    use std::collections::BinaryHeap;

    fn test_entry(priority: i32, clock: u64) -> ReadyReadEntry {
        ReadyReadEntry {
            priority,
            adjusted_clock: uuid::Clock::from_u64(clock),
            inner: None,
        }
    }

    #[test]
    fn test_heap_ordering() {
        // BinaryHeap is a max-heap: pop() returns the greatest element.
        // ReadyReadEntry::Ord should yield Greatest for max priority,
        // then min adjusted_clock (via .reverse()).

        assert_eq!(
            test_entry(2, 100).cmp(&test_entry(1, 100)),
            Ordering::Greater,
            "higher priority wins"
        );
        assert_eq!(
            test_entry(1, 50).cmp(&test_entry(1, 100)),
            Ordering::Greater,
            "earlier clock wins at same priority"
        );
        assert_eq!(
            test_entry(1, 100).cmp(&test_entry(1, 100)),
            Ordering::Equal,
            "equal"
        );
        assert_eq!(
            test_entry(0, 100).cmp(&test_entry(-1, 50)),
            Ordering::Greater,
            "default priority beats a negative priority"
        );

        // Verify pop order from a real BinaryHeap.
        let mut heap = BinaryHeap::new();
        heap.push(test_entry(1, 200));
        heap.push(test_entry(2, 100));
        heap.push(test_entry(1, 50));
        heap.push(test_entry(2, 300));
        heap.push(test_entry(-1, 10));

        let pops: Vec<_> = std::iter::from_fn(|| heap.pop())
            .map(|e| (e.priority, e.adjusted_clock.as_u64()))
            .collect();

        assert_eq!(
            pops,
            vec![
                (2, 100), // high priority, early clock
                (2, 300), // high priority, late clock
                (1, 50),  // low priority, early clock
                (1, 200), // low priority, late clock
                (-1, 10), // negative priority is drained last
            ]
        );
    }

    #[tokio::test]
    async fn test_top_transitions() {
        let read = |id, tailing| -> ReadLines {
            Box::pin(gazette::journal::read::ReadLines::new(
                stream::pending().boxed(),
                id,
                tailing,
            ))
        };
        // `resolved_top` refreshes `now` from the wall-clock, so clocks are relative to it.
        let base = crate::now_clock().to_unix().0;
        let entry = |offset: i64| ReadyReadEntry {
            priority: 0,
            adjusted_clock: uuid::Clock::from_unix(base.checked_add_signed(offset).unwrap(), 0),
            inner: None,
        };

        let mut heap = ReadHeap::new();
        let mut trace = Vec::new();
        let mut step = |step: &str, heap: &ReadHeap| {
            let mut now = uuid::Clock::from_unix(base, 0);
            let top = match heap.resolved_top(&mut now) {
                // `base` truncates the wall-clock to whole seconds, which
                // round up to recover the delay from `base`.
                ResolvedTop::Delayed(delay) => {
                    format!("Delayed({}s)", delay.as_secs_f64().ceil())
                }
                top => format!("{top:?}"),
            };
            trace.push(format!("{step}: {top}"));
        };

        heap.push_ready(entry(-100));
        step("ready, before InitialReadsStarted", &heap);
        heap.on_initial_reads_started();
        step("ready", &heap);

        let probed = read(0, false);
        heap.start_probe(Box::pin(async { Ok((0, 0, probed)) }));
        step("probing", &heap);
        let ReadEvent::Probed(Ok((_, _, probed))) = heap.next_read_event().await else {
            unreachable!()
        };
        step("probed", &heap);

        assert!(heap.push_pending(probed));
        step("pending, stalled", &heap);
        assert!(!heap.push_pending(read(1, true)));
        step("pending, tailing", &heap);

        assert_eq!(
            (
                heap.stalled_count(),
                heap.tailing_count(),
                heap.tracked_count()
            ),
            (1, 1, 3)
        );
        assert_eq!(
            heap.stalled_reads()
                .map(|read| read.id())
                .collect::<Vec<_>>(),
            vec![0]
        );

        let mut heap = ReadHeap::new();
        heap.on_initial_reads_started();
        heap.push_pending(read(1, true));
        step("tailing", &heap);
        heap.push_ready(entry(3_600));
        step("delayed", &heap);
        heap.push_ready(entry(0));
        step("due", &heap);

        insta::assert_snapshot!(trace.join("\n"), @"
        ready, before InitialReadsStarted: Unknown
        ready: Due
        probing: Unknown
        probed: Due
        pending, stalled: Unknown
        pending, tailing: Unknown
        tailing: Idle
        delayed: Delayed(3600s)
        due: Due
        ");
    }
}
