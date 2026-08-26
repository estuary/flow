//! The dataflow graph and clock scheduler.
//!
//! The `Graph` tracks the data-flow status of a catalog of **derivations only**:
//! a catalog test never runs a capture or a materialization. Ingest steps stand
//! in for captures, and verify steps read collections directly.
//!
//! Three string identities appear throughout, and are never interchangeable:
//!
//! - A **collection** name is a catalog name (`CollectionSpec.name`).
//! - A **template name** is a collection's `partition_template.name` — the
//!   collection name plus its generation ID, and the prefix of every one of its
//!   journal names. It is the only unambiguous journal selector, because
//!   collection names may nest (see [`crate::partitions::template_name`]).
//! - A **derivation** names a scheduled reader. A derivation writes exactly one
//!   collection — its own — so its name *is* that collection's name.

use crate::clock::{Clock, contains_clock, max_clock};
use anyhow::Context;
use std::collections::{BTreeMap, VecDeque};

/// Synthetic test time, in nanoseconds. It has no relation to wall-clock time;
/// it is advanced lazily as a test progresses (see [`Graph::pop_ready_reads`]
/// and [`Graph::completed_advance`]). Ported from Go's `TestTime`
/// (`time.Duration`).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Default)]
pub struct TestTime(pub i64);

impl TestTime {
    pub const ZERO: TestTime = TestTime(0);

    pub fn from_secs(secs: u32) -> Self {
        TestTime(secs as i64 * 1_000_000_000)
    }
}

impl std::ops::Add for TestTime {
    type Output = TestTime;
    fn add(self, rhs: TestTime) -> TestTime {
        TestTime(self.0 + rhs.0)
    }
}

impl std::ops::Sub for TestTime {
    type Output = TestTime;
    fn sub(self, rhs: TestTime) -> TestTime {
        TestTime(self.0 - rhs.0)
    }
}

impl std::fmt::Display for TestTime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Render like Go's time.Duration for readable traces/snapshots.
        write!(
            f,
            "{:?}",
            std::time::Duration::from_nanos(self.0.max(0) as u64)
        )
    }
}

/// A source read of a transform: the reading derivation, the journal-name suffix
/// it appends to read-through clocks, and its read delay.
#[derive(Clone, Debug, PartialEq, Eq)]
struct TransformRead {
    derivation: String,
    /// Suffix appended to read journal names (`;{journal_read_suffix}`).
    suffix: String,
    /// Read delay applied by this transform.
    delay: TestTime,
}

/// A read of a source that a derivation must perform, which may not have
/// happened yet. Ported from Go's `PendingStat`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingRead {
    /// Synthetic time at which the read is unblocked.
    pub ready_at: TestTime,
    /// Name of the reading derivation.
    pub derivation: String,
    /// Clock which this read must read through.
    pub read_through: Clock,
}

/// One transform of a derivation, as consumed by graph construction.
pub struct Transform {
    /// Template name of the source collection read by this transform.
    pub source: String,
    /// Stable read-checkpoint suffix (`derive/{derivation}/{transform}`).
    pub journal_read_suffix: String,
    /// Read delay of this transform.
    pub read_delay: TestTime,
}

/// Maintains the data-flow status of a running catalog of derivations.
pub struct Graph {
    /// Current synthetic test time.
    at_time: TestTime,
    /// Index of each derivation to the template name of the collection it
    /// writes. A derivation writes exactly one collection: its own.
    outputs: BTreeMap<String, String>,
    /// Index of each read collection (by template name) to the transforms that
    /// read it. A derivation can have more than one read of a collection.
    readers: BTreeMap<String, Vec<TransformRead>>,
    /// Index of each derivation to its read-through clock.
    read_through: BTreeMap<String, Clock>,
    /// Pending reads which remain to be performed.
    pending: Vec<PendingRead>,
    /// Overall write progress of the cluster.
    write_clock: Clock,
}

impl Graph {
    /// Construct an empty graph.
    pub fn new() -> Self {
        Graph {
            at_time: TestTime::ZERO,
            outputs: BTreeMap::new(),
            readers: BTreeMap::new(),
            read_through: BTreeMap::new(),
            pending: Vec::new(),
            write_clock: Clock::new(),
        }
    }

    /// Build a graph from built collection specs. Only collections with a
    /// derivation and an enabled shard template are added: a disabled task never
    /// runs, so a verify must not gate on a write it will never make.
    pub fn from_built_collections(
        collections: &[proto_flow::flow::CollectionSpec],
    ) -> anyhow::Result<Self> {
        let mut graph = Graph::new();

        for collection in collections {
            let Some(derivation) = &collection.derivation else {
                continue;
            };
            if derivation
                .shard_template
                .as_ref()
                .map(|s| s.disable)
                .unwrap_or(false)
            {
                continue; // Ignore dataflows of disabled tasks.
            }

            let transforms: Vec<Transform> = derivation
                .transforms
                .iter()
                .map(|t| {
                    let source = t
                        .collection
                        .as_ref()
                        .context("built transform is missing its source collection")?;

                    anyhow::Ok(Transform {
                        source: crate::partitions::template_name(source)?.to_string(),
                        journal_read_suffix: t.journal_read_suffix.clone(),
                        read_delay: TestTime::from_secs(t.read_delay_seconds),
                    })
                })
                .collect::<anyhow::Result<_>>()?;

            graph.add_derivation(
                collection.name.clone(),
                crate::partitions::template_name(collection)?.to_string(),
                &transforms,
            );
        }

        Ok(graph)
    }

    /// Add a derivation which reads `transforms` and writes the collection with
    /// `template_name` (its own), tracking dataflow through it.
    pub fn add_derivation(
        &mut self,
        derivation: String,
        template_name: String,
        transforms: &[Transform],
    ) {
        self.outputs.insert(derivation.clone(), template_name);

        for t in transforms {
            self.readers
                .entry(t.source.clone())
                .or_default()
                .push(TransformRead {
                    derivation: derivation.clone(),
                    suffix: format!(";{}", t.journal_read_suffix),
                    delay: t.read_delay,
                });
        }

        self.read_through.entry(derivation).or_default();
    }

    /// True if there is at least one pending derivation which may directly or
    /// recursively write into the collection *named* `collection`. Gates verify
    /// steps.
    ///
    /// This is a pure topology question and takes a name rather than a template:
    /// only a derivation writes into a collection, and a derivation's name is its
    /// collection's name, so it asks whether `collection` names a derivation
    /// reachable from a pending read. A collection which names no derivation —
    /// one that ingest steps stand in for — can never have a pending write.
    pub fn has_pending_write(&self, collection: &str) -> bool {
        let mut fifo: VecDeque<&String> = VecDeque::new();
        let mut visited: std::collections::BTreeSet<&String> = std::collections::BTreeSet::new();

        for pending in &self.pending {
            if visited.insert(&pending.derivation) {
                fifo.push_back(&pending.derivation);
            }
        }

        while let Some(derivation) = fifo.pop_front() {
            if derivation == collection {
                return true; // Search target found.
            }
            let Some(output) = self.outputs.get(derivation) else {
                continue;
            };
            for r in self.readers.get(output).into_iter().flatten() {
                if visited.insert(&r.derivation) {
                    fifo.push_back(&r.derivation);
                }
            }
        }
        false
    }

    /// Remove and return pending reads whose ready-at time equals the current
    /// test time. Also returns the delta from the current time to the
    /// next-ready pending read (zero if any ready reads were returned), and the
    /// associated derivation — or `None` for both if no pending reads remain.
    /// Used for lazy synthetic-time advancement.
    pub fn pop_ready_reads(&mut self) -> (Vec<PendingRead>, Option<TestTime>, Option<String>) {
        let mut ready = Vec::new();
        let mut next: Option<(TestTime, String)> = None;
        let mut retained = Vec::with_capacity(self.pending.len());

        for read in std::mem::take(&mut self.pending) {
            let delta = read.ready_at - self.at_time;

            match &next {
                Some((next_delta, _)) if *next_delta <= delta => {}
                _ => next = Some((delta, read.derivation.clone())),
            }

            if delta == TestTime::ZERO {
                ready.push(read);
            } else {
                retained.push(read);
            }
        }
        self.pending = retained;

        match next {
            Some((delta, name)) => (ready, Some(delta), Some(name)),
            None => (ready, None, None),
        }
    }

    /// Record a completed ingestion at write clock `write_at`.
    pub fn completed_ingest(&mut self, write_at: &Clock) {
        self.write_clock = max_clock(&self.write_clock, write_at);
        self.project_write(write_at);
    }

    /// Record a completed read of a derivation.
    ///
    /// - `read_through` is a min-reduced clock over read progress across shards;
    ///   its journals include the transform's group-name suffix.
    /// - `write_at` is a max-reduced clock over write progress across shards;
    ///   its journals do *not* include group names.
    pub fn completed_read(&mut self, derivation: &str, read_through: Clock, write_at: &Clock) {
        self.write_clock = max_clock(&self.write_clock, write_at);
        self.read_through
            .insert(derivation.to_string(), read_through);
        self.project_write(write_at);
    }

    /// Project a write onto the readers of every collection whose journals
    /// `write_at` names, enqueuing (or merging into) `PendingRead`s. A journal
    /// belongs to the collection whose template name prefixes it, which is the
    /// identity `readers` is keyed by — so the write itself says which readers
    /// it concerns, and no collection need be named. Skips a reader whose
    /// `read_through` already contains the projected clock — the check that
    /// terminates self-cycles.
    fn project_write(&mut self, write_at: &Clock) {
        // Reads to enqueue, gathered before `self.pending` is touched.
        let mut adds: Vec<PendingRead> = Vec::new();

        for (template_name, readers) in &self.readers {
            let prefix = format!("{template_name}/");

            for r in readers {
                // Map `write_at` into this reader's read-through clock.
                let mut read_through = Clock::new();
                for (journal, &offset) in write_at {
                    if journal.starts_with(&prefix) {
                        read_through.insert(format!("{journal}{}", r.suffix), offset);
                    }
                }

                // An absent entry is an empty clock, which contains only an
                // empty `read_through` — so a write naming no journal of this
                // collection enqueues nothing.
                let already_read = match self.read_through.get(&r.derivation) {
                    Some(existing) => contains_clock(existing, &read_through),
                    None => read_through.is_empty(),
                };
                if already_read {
                    continue; // Transform read not required.
                }

                adds.push(PendingRead {
                    ready_at: self.at_time + r.delay,
                    derivation: r.derivation.clone(),
                    read_through,
                });
            }
        }

        for add in adds {
            // Merge into a read of the same derivation already ready at the
            // same time.
            match self.pending.iter_mut().find(|pending| {
                pending.derivation == add.derivation && pending.ready_at == add.ready_at
            }) {
                Some(pending) => {
                    pending.read_through = max_clock(&pending.read_through, &add.read_through)
                }
                None => self.pending.push(add),
            }
        }
    }

    /// Advance the current synthetic test time by `delta`.
    pub fn completed_advance(&mut self, delta: TestTime) {
        self.at_time = self.at_time + delta;

        for pending in &self.pending {
            if pending.ready_at < self.at_time {
                panic!("time advanced beyond a pending read");
            }
        }
    }

    /// Snapshot of the current global write clock.
    pub fn write_clock(&self) -> &Clock {
        &self.write_clock
    }
}

impl Default for Graph {
    fn default() -> Self {
        Graph::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn clock<const N: usize>(entries: [(&str, i64); N]) -> Clock {
        entries
            .into_iter()
            .map(|(j, o)| (j.to_string(), o))
            .collect()
    }

    /// A transform reading `source` into `derivation`, with the
    /// `derive/{derivation}/{transform}` read suffix the runtime assigns. These
    /// fixtures name collections by a stand-in template that is just the
    /// collection's name, which keeps their journal names short; nesting is
    /// covered separately by [`nested_collection_names_project_separately`].
    fn transform(
        source: &str,
        transform: &str,
        derivation: &str,
        read_delay_secs: u32,
    ) -> Transform {
        Transform {
            source: source.to_string(),
            journal_read_suffix: format!("derive/{derivation}/{transform}"),
            read_delay: TestTime::from_secs(read_delay_secs),
        }
    }

    /// Build a graph from a list of (derivation, transforms). Mirrors
    /// `derivationsFixture` — transforms are grouped by their derivation.
    fn graph_of(derivations: Vec<(&str, Vec<Transform>)>) -> Graph {
        let mut graph = Graph::new();
        for (name, transforms) in derivations {
            graph.add_derivation(name.to_string(), name.to_string(), &transforms);
        }
        graph
    }

    #[test]
    fn graph_antecedents() {
        let mut graph = graph_of(vec![
            ("B", vec![transform("A", "A to B", "B", 0)]),
            ("C", vec![transform("B", "B to C", "C", 0)]),
            ("A", vec![transform("B", "B to A", "A", 0)]),
            ("Y", vec![transform("X", "X to Y", "Y", 0)]),
        ]);

        for c in ["A", "B", "C", "X", "Y"] {
            assert!(!graph.has_pending_write(c), "no pending write for {c}");
        }

        graph.pending.push(PendingRead {
            ready_at: TestTime(1),
            derivation: "B".to_string(),
            read_through: Clock::new(),
        });

        assert!(graph.has_pending_write("A"));
        assert!(graph.has_pending_write("B"));
        assert!(graph.has_pending_write("C"));
        assert!(!graph.has_pending_write("X"));
        assert!(!graph.has_pending_write("Y"));

        graph.pending.push(PendingRead {
            ready_at: TestTime(1),
            derivation: "Y".to_string(),
            read_through: Clock::new(),
        });

        assert!(!graph.has_pending_write("X"));
        assert!(graph.has_pending_write("Y"));
    }

    #[test]
    fn graph_ingest_projection() {
        let mut graph = graph_of(vec![
            ("B", vec![transform("A", "A-to-B", "B", 10)]),
            ("C", vec![transform("A", "A-to-C", "C", 5)]),
        ]);

        // Two ingests into "A" complete, with raced clocks.
        graph.completed_ingest(&clock([("A/foo", 2)]));
        graph.completed_ingest(&clock([("A/foo", 1), ("A/bar", 1)]));

        graph
            .pending
            .sort_by(|a, b| a.derivation.cmp(&b.derivation));

        assert_eq!(
            graph.pending,
            vec![
                PendingRead {
                    ready_at: TestTime::from_secs(10),
                    derivation: "B".to_string(),
                    read_through: clock([
                        ("A/foo;derive/B/A-to-B", 2),
                        ("A/bar;derive/B/A-to-B", 1)
                    ]),
                },
                PendingRead {
                    ready_at: TestTime::from_secs(5),
                    derivation: "C".to_string(),
                    read_through: clock([
                        ("A/foo;derive/C/A-to-C", 2),
                        ("A/bar;derive/C/A-to-C", 1)
                    ]),
                },
            ]
        );

        assert_eq!(graph.write_clock, clock([("A/foo", 2), ("A/bar", 1)]));
    }

    #[test]
    fn read_projection() {
        let mut graph = graph_of(vec![
            ("B", vec![transform("A", "A-to-B", "B", 0)]),
            ("C", vec![transform("B", "B-to-C", "C", 0)]),
        ]);

        graph.completed_read(
            "B",
            clock([("A/data;derive/B/A-to-B", 1)]),
            &clock([("B/data", 2)]),
        );
        graph.completed_read(
            "B",
            clock([("A/data;derive/B/A-to-B", 2)]),
            &clock([("B/data", 1)]),
        );

        assert_eq!(
            graph.read_through["B"],
            clock([("A/data;derive/B/A-to-B", 2)])
        );

        assert_eq!(
            graph.pending,
            vec![PendingRead {
                ready_at: TestTime::ZERO,
                derivation: "C".to_string(),
                read_through: clock([("B/data;derive/C/B-to-C", 2)]),
            }]
        );

        assert_eq!(graph.write_clock, clock([("B/data", 2)]));
    }

    /// A self-cycle reaches fixed-point via `contains_clock`.
    #[test]
    fn projection_already_read() {
        let mut graph = graph_of(vec![(
            "B",
            vec![
                transform("A", "A-to-B", "B", 0),
                transform("B", "B-to-B", "B", 0), // Self-cycle.
            ],
        )]);

        let progress = clock([("A/data;derive/B/A-to-B", 5), ("B/data;derive/B/B-to-B", 6)]);

        // Read of "B" completes, updating progress reading "A" & "B" data.
        graph.completed_read("B", progress.clone(), &clock([("B/data", 6)]));
        // Ingest of "A" completes (contained by `progress`).
        graph.completed_ingest(&clock([("A/data", 5)]));

        // No pending read of B was created (it cycles, but has read its own write).
        assert!(graph.pending.is_empty());
        assert_eq!(graph.write_clock, clock([("A/data", 5), ("B/data", 6)]));

        // Completed ingest & read which *do* require a new read.
        graph.completed_ingest(&clock([("A/data", 50)]));
        graph.completed_read("B", progress.clone(), &clock([("B/data", 60)]));

        assert_eq!(
            graph.pending,
            vec![PendingRead {
                ready_at: TestTime::ZERO,
                derivation: "B".to_string(),
                read_through: clock([
                    ("A/data;derive/B/A-to-B", 50),
                    ("B/data;derive/B/B-to-B", 60)
                ]),
            }]
        );
        assert_eq!(graph.write_clock, clock([("A/data", 50), ("B/data", 60)]));
    }

    /// Lazy time advancement and ready-read popping.
    #[test]
    fn ready_reads() {
        let mut graph = graph_of(vec![(
            "A",
            vec![
                transform("A", "A-to-A", "A", 0),
                transform("A", "A-to-B", "B", 0),
                transform("A", "A-to-C", "C", 0),
            ],
        )]);
        // (The transforms above only exist to register derivations; we install
        // pending fixtures directly, as the Go test does.)

        graph.pending = vec![
            PendingRead {
                ready_at: TestTime(10),
                derivation: "A".to_string(),
                read_through: clock([("a", 1)]),
            },
            PendingRead {
                ready_at: TestTime(10),
                derivation: "B".to_string(),
                read_through: clock([("a", 2)]),
            },
            PendingRead {
                ready_at: TestTime(5),
                derivation: "C".to_string(),
                read_through: clock([("a", 3)]),
            },
        ];

        let (ready, next, name) = graph.pop_ready_reads();
        assert!(ready.is_empty());
        assert_eq!(next, Some(TestTime(5)));
        assert_eq!(name.as_deref(), Some("C"));
        graph.completed_advance(TestTime(4));

        let (ready, next, name) = graph.pop_ready_reads();
        assert!(ready.is_empty());
        assert_eq!(next, Some(TestTime(1)));
        assert_eq!(name.as_deref(), Some("C"));
        graph.completed_advance(TestTime(1));

        let (ready, next, name) = graph.pop_ready_reads();
        assert_eq!(
            ready,
            vec![PendingRead {
                ready_at: TestTime(5),
                derivation: "C".to_string(),
                read_through: clock([("a", 3)])
            }]
        );
        assert_eq!(next, Some(TestTime::ZERO));
        assert_eq!(name.as_deref(), Some("C"));

        let (ready, next, name) = graph.pop_ready_reads();
        assert!(ready.is_empty());
        assert_eq!(next, Some(TestTime(5)));
        assert_eq!(name.as_deref(), Some("A"));
        graph.completed_advance(TestTime(5));

        let (ready, next, name) = graph.pop_ready_reads();
        assert_eq!(
            ready,
            vec![
                PendingRead {
                    ready_at: TestTime(10),
                    derivation: "A".to_string(),
                    read_through: clock([("a", 1)])
                },
                PendingRead {
                    ready_at: TestTime(10),
                    derivation: "B".to_string(),
                    read_through: clock([("a", 2)])
                },
            ]
        );
        assert_eq!(next, Some(TestTime::ZERO));
        assert_eq!(name.as_deref(), Some("A"));

        let (ready, next, name) = graph.pop_ready_reads();
        assert!(ready.is_empty());
        assert_eq!(next, None);
        assert_eq!(name, None);
    }

    /// Derivations-only adaptation of Go's `TestTaskIndexing`: verifies the
    /// `outputs` and `readers` indices, including multiple transforms of one
    /// source and a read delay.
    #[test]
    fn derivation_indexing() {
        let mut graph = Graph::new();
        graph.add_derivation(
            "a/derivation".to_string(),
            "a/derivation/0000".to_string(),
            &[
                Transform {
                    source: "a/source/one/1111".to_string(),
                    journal_read_suffix: "derive/A".to_string(),
                    read_delay: TestTime::ZERO,
                },
                Transform {
                    source: "a/source/one/1111".to_string(),
                    journal_read_suffix: "derive/AA".to_string(),
                    read_delay: TestTime::from_secs(5),
                },
                Transform {
                    source: "a/source/two/2222".to_string(),
                    journal_read_suffix: "derive/B".to_string(),
                    read_delay: TestTime::ZERO,
                },
            ],
        );

        assert_eq!(
            graph.outputs,
            BTreeMap::from([("a/derivation".to_string(), "a/derivation/0000".to_string())])
        );

        assert_eq!(
            graph.readers,
            BTreeMap::from([
                (
                    "a/source/one/1111".to_string(),
                    vec![
                        TransformRead {
                            derivation: "a/derivation".to_string(),
                            suffix: ";derive/A".to_string(),
                            delay: TestTime::ZERO
                        },
                        TransformRead {
                            derivation: "a/derivation".to_string(),
                            suffix: ";derive/AA".to_string(),
                            delay: TestTime::from_secs(5)
                        },
                    ]
                ),
                (
                    "a/source/two/2222".to_string(),
                    vec![TransformRead {
                        derivation: "a/derivation".to_string(),
                        suffix: ";derive/B".to_string(),
                        delay: TestTime::ZERO
                    }],
                ),
            ])
        );
    }

    /// Collection names nest — `test/nest` and `test/nest/inner` are both legal
    /// — which is why collections are identified by template name. Matching on
    /// the bare name would project the child's journals into the transform
    /// reading the parent, feeding `test/nest/inner` its own output as a source.
    #[test]
    fn nested_collection_names_project_separately() {
        let nest = "test/nest/1100";
        let inner = "test/nest/inner/2200";

        let mut graph = Graph::new();
        graph.add_derivation(
            "test/nest".to_string(),
            nest.to_string(),
            &[Transform {
                source: "test/src/0011".to_string(),
                journal_read_suffix: "derive/test/nest/from-src".to_string(),
                read_delay: TestTime::ZERO,
            }],
        );
        graph.add_derivation(
            "test/nest/inner".to_string(),
            inner.to_string(),
            &[Transform {
                source: nest.to_string(),
                journal_read_suffix: "derive/test/nest/inner/from-nest".to_string(),
                read_delay: TestTime::ZERO,
            }],
        );
        graph.add_derivation(
            "test/leaf".to_string(),
            "test/leaf/3300".to_string(),
            &[Transform {
                source: inner.to_string(),
                journal_read_suffix: "derive/test/leaf/from-inner".to_string(),
                read_delay: TestTime::ZERO,
            }],
        );

        // A write clock carrying both nested collections' journals, as the run's
        // global clock does once both tasks have written. Each reader must see
        // only the journals of the collection it actually reads.
        graph.project_write(&clock([
            (&format!("{nest}/pivot=00"), 3),
            (&format!("{inner}/pivot=00"), 7),
        ]));

        assert_eq!(
            graph.pending,
            vec![
                PendingRead {
                    ready_at: TestTime::ZERO,
                    derivation: "test/nest/inner".to_string(),
                    read_through: clock([(
                        &format!("{nest}/pivot=00;derive/test/nest/inner/from-nest"),
                        3
                    )]),
                },
                PendingRead {
                    ready_at: TestTime::ZERO,
                    derivation: "test/leaf".to_string(),
                    read_through: clock([(
                        &format!("{inner}/pivot=00;derive/test/leaf/from-inner"),
                        7
                    )]),
                },
            ]
        );
    }
}
