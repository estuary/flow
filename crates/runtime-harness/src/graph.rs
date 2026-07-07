//! The dataflow graph and clock scheduler, ported faithfully from
//! `go/testing/graph.go`.
//!
//! The `Graph` tracks the data-flow status of a catalog of **derivations
//! only** (V1 constructs it as `NewGraph(nil, collections, nil)` — captures and
//! materializations are excluded and never run during tests). It answers the
//! scheduler's questions: which reads (`PendingStat`s) are unblocked at the
//! current synthetic time, whether a collection still has pending upstream
//! writes (gating verifies), and how far synthetic time must advance to unblock
//! the next read-delayed transform.
//!
//! Clocks are per-journal offsets (see [`crate::clock`]); in the harness these
//! index the in-memory collection store. Read progress clocks carry a
//! `;{journal_read_suffix}` suffix per transform, exactly as Gazette's Stat
//! returns them.

use crate::clock::{Clock, contains_clock, max_clock};
use std::collections::{BTreeMap, VecDeque};

/// A collection name.
pub type Collection = String;
/// A task (derivation) name. In the derivations-only harness this equals the
/// derived collection's name.
pub type TaskName = String;

/// Synthetic test time, in nanoseconds. It has no relation to wall-clock time;
/// it is advanced lazily as a test progresses (see [`Graph::pop_ready_stats`]
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

/// A source read of a transform: the reader task, the journal-name suffix it
/// appends to read-progress clocks, and its read delay.
#[derive(Clone, Debug, PartialEq, Eq)]
struct TaskRead {
    task: TaskName,
    /// Suffix appended to read journal names (`;{journal_read_suffix}`).
    suffix: String,
    /// Read delay applied by this transform.
    delay: TestTime,
}

/// A read of a source that a task must perform, which may not have happened
/// yet. Ported from Go's `PendingStat`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingStat {
    /// Synthetic time at which the task's read is unblocked.
    pub ready_at: TestTime,
    /// Name of the reading task.
    pub task_name: TaskName,
    /// Clock which this stat must read through.
    pub read_through: Clock,
}

/// One transform of a derivation, as consumed by graph construction.
pub struct Transform {
    /// Source collection read by this transform.
    pub source: Collection,
    /// Stable read-checkpoint suffix (`derive/{derivation}/{transform}`).
    pub journal_read_suffix: String,
    /// Read delay of this transform.
    pub read_delay: TestTime,
}

/// Maintains the data-flow status of a running catalog of derivations.
pub struct Graph {
    /// Current synthetic test time.
    at_time: TestTime,
    /// Index of each task to the collections it writes.
    outputs: BTreeMap<TaskName, Vec<Collection>>,
    /// Index of each read collection to the tasks (transforms) that read it.
    /// A task can have more than one read of a collection.
    readers: BTreeMap<Collection, Vec<TaskRead>>,
    /// Index of each task to its read-through clock.
    read_through: BTreeMap<TaskName, Clock>,
    /// Pending reads which remain to be stat-ed.
    pending: Vec<PendingStat>,
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
    /// derivation and an enabled shard template are added (matching V1, which
    /// skips disabled tasks and excludes captures / materializations).
    pub fn from_built_collections(collections: &[proto_flow::flow::CollectionSpec]) -> Self {
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
                .map(|t| Transform {
                    source: t
                        .collection
                        .as_ref()
                        .map(|c| c.name.clone())
                        .unwrap_or_default(),
                    journal_read_suffix: t.journal_read_suffix.clone(),
                    read_delay: TestTime::from_secs(t.read_delay_seconds),
                })
                .collect();

            graph.add_derivation(collection.name.clone(), &transforms);
        }

        graph
    }

    /// Add a derivation (a task which reads `transforms` and writes its own
    /// collection `name`), tracking dataflow through it.
    pub fn add_derivation(&mut self, name: Collection, transforms: &[Transform]) {
        // A derivation writes into its own collection.
        self.outputs
            .entry(name.clone())
            .or_default()
            .push(name.clone());

        for t in transforms {
            self.readers
                .entry(t.source.clone())
                .or_default()
                .push(TaskRead {
                    task: name.clone(),
                    suffix: format!(";{}", t.journal_read_suffix),
                    delay: t.read_delay,
                });
        }

        self.read_through.entry(name).or_default();
    }

    /// True if there is at least one pending task which may directly or
    /// recursively write into `collection`. Gates verify steps: a verify may
    /// only run once no pending write can still land in the collection.
    pub fn has_pending_write(&self, collection: &str) -> bool {
        let mut fifo: VecDeque<TaskName> = VecDeque::new();
        let mut visited: std::collections::BTreeSet<TaskName> = std::collections::BTreeSet::new();

        for pending in &self.pending {
            if visited.insert(pending.task_name.clone()) {
                fifo.push_back(pending.task_name.clone());
            }
        }

        while let Some(task) = fifo.pop_front() {
            // For each collection produced into by `task`, and each `child`
            // task which reads that collection, enqueue the child.
            let Some(outputs) = self.outputs.get(&task) else {
                continue;
            };
            for output in outputs {
                if output == collection {
                    return true; // Search target found.
                }
                if let Some(readers) = self.readers.get(output) {
                    for r in readers {
                        if visited.insert(r.task.clone()) {
                            fifo.push_back(r.task.clone());
                        }
                    }
                }
            }
        }
        false
    }

    /// Remove and return pending stats whose ready-at time equals the current
    /// test time. Also returns the delta from the current time to the
    /// next-ready pending stat (zero if any ready stats were returned), and the
    /// associated task name — or `None` for both if no pending stats remain.
    /// Used for lazy synthetic-time advancement.
    pub fn pop_ready_stats(&mut self) -> (Vec<PendingStat>, Option<TestTime>, Option<TaskName>) {
        let mut ready = Vec::new();
        let mut next: Option<(TestTime, TaskName)> = None;
        let mut retained = Vec::with_capacity(self.pending.len());

        for stat in std::mem::take(&mut self.pending) {
            let delta = stat.ready_at - self.at_time;

            match &next {
                Some((next_delta, _)) if *next_delta <= delta => {}
                _ => next = Some((delta, stat.task_name.clone())),
            }

            if delta == TestTime::ZERO {
                ready.push(stat);
            } else {
                retained.push(stat);
            }
        }
        self.pending = retained;

        match next {
            Some((delta, name)) => (ready, Some(delta), Some(name)),
            None => (ready, None, None),
        }
    }

    /// Record a completed ingestion into `collection` at write clock `write_at`.
    pub fn completed_ingest(&mut self, collection: &str, write_at: &Clock) {
        self.write_clock = max_clock(&self.write_clock, write_at);
        self.project_write(collection, write_at);
    }

    /// Record a completed task stat.
    ///
    /// - `read_through` is a min-reduced clock over read progress across shards;
    ///   its journals include the transform's group-name suffix.
    /// - `write_at` is a max-reduced clock over write progress across shards;
    ///   its journals do *not* include group names.
    pub fn completed_stat(&mut self, task: &str, read_through: Clock, write_at: &Clock) {
        self.write_clock = max_clock(&self.write_clock, write_at);
        self.read_through.insert(task.to_string(), read_through);

        if let Some(outputs) = self.outputs.get(task).cloned() {
            for output in outputs {
                self.project_write(&output, write_at);
            }
        }
    }

    /// Project a write into `collection` onto its readers, enqueuing (or
    /// merging into) `PendingStat`s. Skips a reader whose `read_through` already
    /// contains the projected clock — the check that terminates self-cycles.
    fn project_write(&mut self, collection: &str, write_at: &Clock) {
        let Some(readers) = self.readers.get(collection).cloned() else {
            return;
        };

        let prefix = format!("{collection}/");

        for r in readers {
            // Map `write_at` to its corresponding read-through, filtering to
            // journals scoped to this collection and appending the suffix.
            let mut read_through = Clock::new();
            for (journal, &offset) in write_at {
                if journal.starts_with(&prefix) {
                    read_through.insert(format!("{journal}{}", r.suffix), offset);
                }
            }

            let existing = self.read_through.get(&r.task);
            if let Some(existing) = existing {
                if contains_clock(existing, &read_through) {
                    continue; // Transform stat not required.
                }
            } else if read_through.is_empty() {
                continue;
            }

            let add = PendingStat {
                ready_at: self.at_time + r.delay,
                task_name: r.task.clone(),
                read_through,
            };

            // Fold into a matching PendingStat if one exists, else append.
            let mut found = false;
            for pending in &mut self.pending {
                if pending.task_name == add.task_name && pending.ready_at == add.ready_at {
                    pending.read_through = max_clock(&pending.read_through, &add.read_through);
                    found = true;
                }
            }
            if !found {
                self.pending.push(add);
            }
        }
    }

    /// Advance the current synthetic test time by `delta`.
    pub fn completed_advance(&mut self, delta: TestTime) {
        self.at_time = self.at_time + delta;

        for pending in &self.pending {
            if pending.ready_at < self.at_time {
                panic!("time advanced beyond pending stat");
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

    /// A transform reading `source` into `derivation`, with `journal_read_suffix`
    /// computed as V1 does (`derive/{derivation}/{transform}`). Mirrors
    /// `transformFixture` in `go/testing/graph_test.go`.
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
            graph.add_derivation(name.to_string(), &transforms);
        }
        graph
    }

    /// Port of `TestGraphAntecedents`.
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

        graph.pending.push(PendingStat {
            ready_at: TestTime(1),
            task_name: "B".to_string(),
            read_through: Clock::new(),
        });

        assert!(graph.has_pending_write("A"));
        assert!(graph.has_pending_write("B"));
        assert!(graph.has_pending_write("C"));
        assert!(!graph.has_pending_write("X"));
        assert!(!graph.has_pending_write("Y"));

        graph.pending.push(PendingStat {
            ready_at: TestTime(1),
            task_name: "Y".to_string(),
            read_through: Clock::new(),
        });

        assert!(!graph.has_pending_write("X"));
        assert!(graph.has_pending_write("Y"));
    }

    /// Port of `TestGraphIngestProjection`.
    #[test]
    fn graph_ingest_projection() {
        let mut graph = graph_of(vec![
            ("B", vec![transform("A", "A-to-B", "B", 10)]),
            ("C", vec![transform("A", "A-to-C", "C", 5)]),
        ]);

        // Two ingests into "A" complete, with raced clocks.
        graph.completed_ingest("A", &clock([("A/foo", 2)]));
        graph.completed_ingest("A", &clock([("A/foo", 1), ("A/bar", 1)]));

        graph.pending.sort_by(|a, b| a.task_name.cmp(&b.task_name));

        assert_eq!(
            graph.pending,
            vec![
                PendingStat {
                    ready_at: TestTime::from_secs(10),
                    task_name: "B".to_string(),
                    read_through: clock([
                        ("A/foo;derive/B/A-to-B", 2),
                        ("A/bar;derive/B/A-to-B", 1)
                    ]),
                },
                PendingStat {
                    ready_at: TestTime::from_secs(5),
                    task_name: "C".to_string(),
                    read_through: clock([
                        ("A/foo;derive/C/A-to-C", 2),
                        ("A/bar;derive/C/A-to-C", 1)
                    ]),
                },
            ]
        );

        assert_eq!(graph.write_clock, clock([("A/foo", 2), ("A/bar", 1)]));
    }

    /// Port of `TestStatProjection`.
    #[test]
    fn stat_projection() {
        let mut graph = graph_of(vec![
            ("B", vec![transform("A", "A-to-B", "B", 0)]),
            ("C", vec![transform("B", "B-to-C", "C", 0)]),
        ]);

        graph.completed_stat(
            "B",
            clock([("A/data;derive/B/A-to-B", 1)]),
            &clock([("B/data", 2)]),
        );
        graph.completed_stat(
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
            vec![PendingStat {
                ready_at: TestTime::ZERO,
                task_name: "C".to_string(),
                read_through: clock([("B/data;derive/C/B-to-C", 2)]),
            }]
        );

        assert_eq!(graph.write_clock, clock([("B/data", 2)]));
    }

    /// Port of `TestProjectionAlreadyRead`: a self-cycle reaches fixed-point via
    /// `contains_clock`.
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

        // Stat of "B" completes, updating progress reading "A" & "B" data.
        graph.completed_stat("B", progress.clone(), &clock([("B/data", 6)]));
        // Ingest of "A" completes (contained by `progress`).
        graph.completed_ingest("A", &clock([("A/data", 5)]));

        // No pending stat of B was created (it cycles, but has read its own write).
        assert!(graph.pending.is_empty());
        assert_eq!(graph.write_clock, clock([("A/data", 5), ("B/data", 6)]));

        // Completed ingest & stat which *do* require a new stat.
        graph.completed_ingest("A", &clock([("A/data", 50)]));
        graph.completed_stat("B", progress.clone(), &clock([("B/data", 60)]));

        assert_eq!(
            graph.pending,
            vec![PendingStat {
                ready_at: TestTime::ZERO,
                task_name: "B".to_string(),
                read_through: clock([
                    ("A/data;derive/B/A-to-B", 50),
                    ("B/data;derive/B/B-to-B", 60)
                ]),
            }]
        );
        assert_eq!(graph.write_clock, clock([("A/data", 50), ("B/data", 60)]));
    }

    /// Port of `TestReadyStats`: lazy time advancement and ready-stat popping.
    #[test]
    fn ready_stats() {
        let mut graph = graph_of(vec![(
            "A",
            vec![
                transform("A", "A-to-A", "A", 0),
                transform("A", "A-to-B", "B", 0),
                transform("A", "A-to-C", "C", 0),
            ],
        )]);
        // (The transforms above only exist to register tasks; we install
        // pending fixtures directly, as the Go test does.)

        graph.pending = vec![
            PendingStat {
                ready_at: TestTime(10),
                task_name: "A".to_string(),
                read_through: clock([("a", 1)]),
            },
            PendingStat {
                ready_at: TestTime(10),
                task_name: "B".to_string(),
                read_through: clock([("a", 2)]),
            },
            PendingStat {
                ready_at: TestTime(5),
                task_name: "C".to_string(),
                read_through: clock([("a", 3)]),
            },
        ];

        let (ready, next, name) = graph.pop_ready_stats();
        assert!(ready.is_empty());
        assert_eq!(next, Some(TestTime(5)));
        assert_eq!(name.as_deref(), Some("C"));
        graph.completed_advance(TestTime(4));

        let (ready, next, name) = graph.pop_ready_stats();
        assert!(ready.is_empty());
        assert_eq!(next, Some(TestTime(1)));
        assert_eq!(name.as_deref(), Some("C"));
        graph.completed_advance(TestTime(1));

        let (ready, next, name) = graph.pop_ready_stats();
        assert_eq!(
            ready,
            vec![PendingStat {
                ready_at: TestTime(5),
                task_name: "C".to_string(),
                read_through: clock([("a", 3)])
            }]
        );
        assert_eq!(next, Some(TestTime::ZERO));
        assert_eq!(name.as_deref(), Some("C"));

        let (ready, next, name) = graph.pop_ready_stats();
        assert!(ready.is_empty());
        assert_eq!(next, Some(TestTime(5)));
        assert_eq!(name.as_deref(), Some("A"));
        graph.completed_advance(TestTime(5));

        let (ready, next, name) = graph.pop_ready_stats();
        assert_eq!(
            ready,
            vec![
                PendingStat {
                    ready_at: TestTime(10),
                    task_name: "A".to_string(),
                    read_through: clock([("a", 1)])
                },
                PendingStat {
                    ready_at: TestTime(10),
                    task_name: "B".to_string(),
                    read_through: clock([("a", 2)])
                },
            ]
        );
        assert_eq!(next, Some(TestTime::ZERO));
        assert_eq!(name.as_deref(), Some("A"));

        let (ready, next, name) = graph.pop_ready_stats();
        assert!(ready.is_empty());
        assert_eq!(next, None);
        assert_eq!(name, None);
    }

    /// Derivations-only adaptation of `TestTaskIndexing`: verifies the
    /// `outputs` and `readers` indices, including multiple transforms of one
    /// source and a read delay. (V1's capture / materialization indexing is
    /// intentionally not ported — those tasks never run in tests.)
    #[test]
    fn task_indexing() {
        let graph = graph_of(vec![(
            "a/derivation",
            vec![
                Transform {
                    source: "a/source/one".to_string(),
                    journal_read_suffix: "derive/A".to_string(),
                    read_delay: TestTime::ZERO,
                },
                Transform {
                    source: "a/source/one".to_string(),
                    journal_read_suffix: "derive/AA".to_string(),
                    read_delay: TestTime::from_secs(5),
                },
                Transform {
                    source: "a/source/two".to_string(),
                    journal_read_suffix: "derive/B".to_string(),
                    read_delay: TestTime::ZERO,
                },
            ],
        )]);

        assert_eq!(
            graph.outputs,
            BTreeMap::from([("a/derivation".to_string(), vec!["a/derivation".to_string()])])
        );

        assert_eq!(
            graph.readers,
            BTreeMap::from([
                (
                    "a/source/one".to_string(),
                    vec![
                        TaskRead {
                            task: "a/derivation".to_string(),
                            suffix: ";derive/A".to_string(),
                            delay: TestTime::ZERO
                        },
                        TaskRead {
                            task: "a/derivation".to_string(),
                            suffix: ";derive/AA".to_string(),
                            delay: TestTime::from_secs(5)
                        },
                    ]
                ),
                (
                    "a/source/two".to_string(),
                    vec![TaskRead {
                        task: "a/derivation".to_string(),
                        suffix: ";derive/B".to_string(),
                        delay: TestTime::ZERO
                    }],
                ),
            ])
        );
    }
}
