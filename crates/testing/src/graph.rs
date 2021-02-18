use super::Clock;
use models::{names, tables};
use std::collections::BTreeMap;

/// PendingStat is a derivation's read of its source which
/// may not have happened yet. Field ordering is important:
/// we dequeue PendingStats in the order which they're ready.
#[derive(Debug, Ord, PartialOrd, Eq, PartialEq)]
pub struct PendingStat {
    // Test time at which the transformation's read is unblocked.
    pub ready_at_seconds: u64,
    // Derivation of the transformation.
    pub derivation: names::Collection,
}

/// Graph maintains the data-flow status of a catalog.
pub struct Graph<'a> {
    // Current test time.
    at_seconds: u64,
    // Index of catalog derivations, with read clock and its transforms.
    derivations: BTreeMap<names::Collection, (Clock, Vec<&'a tables::Transform>)>,
    // Pending reads which remain to be stat-ed.
    pending: BTreeMap<PendingStat, Clock>,
    // Overall progress of the cluster.
    write_clock: Clock,
}

impl<'a> Graph<'a> {
    /// Construct a new Graph.
    pub fn new(transforms: &'a [tables::Transform]) -> Self {
        // Initialize |derivations| with empty clocks and collected transforms.
        let mut derivations = BTreeMap::new();
        for tf in transforms {
            derivations
                .entry(tf.derivation.clone())
                .or_insert((Clock::empty(), Vec::new()))
                .1
                .push(tf);
        }

        Self {
            at_seconds: 0,
            derivations,
            pending: BTreeMap::new(),
            write_clock: Default::default(),
        }
    }

    pub fn clock(&self) -> &Clock {
        &self.write_clock
    }

    // Determines if there is at least one PendingStat which will derive into
    // |collection|, or one of it's antecedents.
    pub fn has_pending_parent(&self, collection: &names::Collection) -> bool {
        // |edges| projects from a |derived| collection through its producing transforms,
        // to it's source collections. Note |self.transforms| is ordered by |derivation|.
        let edges = |derived: &names::Collection| {
            self.derivations
                .get(derived)
                .into_iter()
                .map(|d| d.1.iter().map(|tf| &tf.source_collection))
                .flatten()
        };

        // |goal| is true if any PendingStat derives into |collection|,
        // implying we must wait for it to complete before verifying.
        let goal = |collection: &names::Collection| {
            self.pending
                .keys()
                .any(|pending_stat| collection == &pending_stat.derivation)
        };

        pathfinding::directed::bfs::bfs(&collection, |n| edges(n), |n| goal(n)).is_some()
    }

    pub fn pop_ready_stats(&mut self) -> Result<Vec<(PendingStat, Clock)>, Option<u64>> {
        // Split off PendingStats PendingStats having ready_at_seconds == at_seconds
        // into |ready|, while keeping everything else in |pending|.
        let mut ready = self.pending.split_off(&PendingStat {
            ready_at_seconds: self.at_seconds + 1,
            derivation: names::Collection::new(""),
        });
        std::mem::swap(&mut self.pending, &mut ready);

        if !ready.is_empty() {
            Ok(ready.into_iter().collect())
        } else {
            Err(self
                .pending
                .iter()
                .next()
                .map(|(p, _)| p.ready_at_seconds - self.at_seconds))
        }
    }

    /// Tell the driver of a completed ingestion test step.
    #[tracing::instrument(skip(self))]
    pub fn completed_ingest(&mut self, spec: &tables::TestStep, write_clock: Clock) {
        self.write_clock
            .reduce_max(&write_clock.etcd, write_clock.offsets.iter());
        self.project_write(&spec.collection, &write_clock);
    }

    /// Tell the Driver of a completed derivation stat.
    /// * |read_clock| is a min-reduced clock over read progress across derivation shards.
    ///   It's journals include group-name suffixes (as returned from Gazette's Stat).
    /// * |write_clock| is a max-reduced clock over write progress across derivation shards.
    ///   It's journals *don't* include group names (again, as returned from Gazette's Stat).
    #[tracing::instrument(skip(self))]
    pub fn completed_stat(&mut self, read: &PendingStat, read_clock: Clock, write_clock: Clock) {
        self.write_clock
            .reduce_max(&write_clock.etcd, write_clock.offsets.iter());

        // Retain |read_clock| under our derivation to track its progress.
        self.derivations.get_mut(&read.derivation).unwrap().0 = read_clock;

        self.project_write(&read.derivation, &write_clock);
    }

    #[tracing::instrument(skip(self))]
    pub fn completed_advance(&mut self, delta: u64) {
        self.at_seconds += delta;
    }

    fn project_write(&mut self, collection: &names::Collection, write_clock: &Clock) {
        let Self {
            at_seconds,
            write_clock: _,
            pending,
            derivations,
        } = self;

        for (derivation, (read_clock, transforms)) in derivations.iter() {
            for tf in transforms.iter() {
                if &tf.source_collection != collection {
                    continue; // Transform stat not required.
                }

                // Map this |write_clock| into it's equivalent transform read clock.
                let clock = Clock {
                    etcd: write_clock.etcd.clone(),
                    offsets: write_clock
                        .offsets
                        .iter()
                        .map(|(journal, offset)| {
                            (format!("{};{}", journal, tf.group_name()), *offset)
                        })
                        .collect(),
                };

                // Has the derivation already read through |write_clock| ?
                if read_clock.contains(&clock) {
                    continue; // Transform stat not required.
                }

                let stat = PendingStat {
                    ready_at_seconds: *at_seconds + tf.read_delay_seconds.unwrap_or(0) as u64,
                    derivation: derivation.clone(),
                };

                let (stat, clock) = match pending.remove_entry(&stat) {
                    Some((stat, mut lhs_clock)) => {
                        lhs_clock.reduce_max(&clock.etcd, clock.offsets.iter());
                        (stat, lhs_clock)
                    }
                    None => (stat, clock.clone()),
                };

                pending.insert(stat, clock);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{clock_fixture, stat_fixture, step_fixture, transform_fixture};
    use super::{Clock, Graph};
    use models::names;
    use protocol::flow::test_spec::step::Type as TestStepType;

    #[test]
    fn test_antecedents() {
        let transforms = vec![
            transform_fixture("A", "A to B", "B", 0),
            transform_fixture("B", "B to C", "C", 0),
            transform_fixture("B", "B to A", "A", 0),
            transform_fixture("X", "X to Y", "Y", 0),
        ];

        let cname = |s| names::Collection::new(s);

        let mut graph = Graph::new(&transforms);

        assert!(!graph.has_pending_parent(&cname("A")));
        assert!(!graph.has_pending_parent(&cname("B")));
        assert!(!graph.has_pending_parent(&cname("C")));
        assert!(!graph.has_pending_parent(&cname("X")));
        assert!(!graph.has_pending_parent(&cname("Y")));

        graph
            .pending
            .insert(stat_fixture(1, "B"), Default::default());

        assert!(graph.has_pending_parent(&cname("A")));
        assert!(graph.has_pending_parent(&cname("B")));
        assert!(graph.has_pending_parent(&cname("C")));
        assert!(!graph.has_pending_parent(&cname("X")));
        assert!(!graph.has_pending_parent(&cname("Y")));

        graph
            .pending
            .insert(stat_fixture(1, "Y"), Default::default());

        assert!(!graph.has_pending_parent(&cname("X")));
        assert!(graph.has_pending_parent(&cname("Y")));
    }

    #[test]
    fn test_ingest_projection() {
        let transforms = vec![
            transform_fixture("A", "A-to-B", "B", 10),
            transform_fixture("A", "A-to-C", "C", 5),
        ];
        let mut graph = Graph::new(&transforms);

        // Two ingests into "A" complete, with raced Clocks.
        graph.completed_ingest(
            &step_fixture(TestStepType::Ingest, "A"),
            clock_fixture(10, &[("A/foo", 2)]),
        );
        graph.completed_ingest(
            &step_fixture(TestStepType::Ingest, "A"),
            clock_fixture(11, &[("A/foo", 1), ("A/bar", 1)]),
        );

        // Expect PendingStats were created with reduced clocks,
        // and that they order on ascending ready_at_seconds.
        itertools::assert_equal(
            graph.pending.into_iter(),
            vec![
                (
                    stat_fixture(5, "C"),
                    clock_fixture(
                        11,
                        &[("A/foo;derive/C/A-to-C", 2), ("A/bar;derive/C/A-to-C", 1)],
                    ),
                ),
                (
                    stat_fixture(10, "B"),
                    clock_fixture(
                        11,
                        &[("A/foo;derive/B/A-to-B", 2), ("A/bar;derive/B/A-to-B", 1)],
                    ),
                ),
            ]
            .into_iter(),
        );

        assert_eq!(
            graph.write_clock,
            clock_fixture(11, &[("A/foo", 2), ("A/bar", 1)])
        );
    }

    #[test]
    fn test_stat_projection() {
        let transforms = vec![
            transform_fixture("A", "A-to-B", "B", 0),
            transform_fixture("B", "B-to-C", "C", 0),
        ];
        let mut graph = Graph::new(&transforms);

        // Two stats of "B" transformation complete.
        graph.completed_stat(
            &stat_fixture(0, "B"),
            clock_fixture(10, &[("A/data;derive/B/A-to-B", 1)]),
            clock_fixture(10, &[("B/data", 2)]),
        );
        graph.completed_stat(
            &stat_fixture(0, "B"),
            clock_fixture(15, &[("A/data;derive/B/A-to-B", 2)]),
            clock_fixture(20, &[("B/data", 1)]),
        );

        // Expect last read clock was tracked.
        itertools::assert_equal(
            graph.derivations.iter().map(|(_, (c, _))| c),
            &[
                clock_fixture(15, &[("A/data;derive/B/A-to-B", 2)]),
                Clock::empty(),
            ],
        );

        // Expect write clocks were merged into a new pending stat of C.
        itertools::assert_equal(
            graph.pending.into_iter(),
            vec![(
                stat_fixture(0, "C"),
                clock_fixture(20, &[("B/data;derive/C/B-to-C", 2)]),
            )]
            .into_iter(),
        );

        assert_eq!(graph.write_clock, clock_fixture(20, &[("B/data", 2)]));
    }

    #[test]
    fn test_projection_already_read() {
        let transforms = vec![
            transform_fixture("A", "A-to-B", "B", 0),
            transform_fixture("B", "B-to-B", "B", 0), // Self-cycle.
        ];
        let mut graph = Graph::new(&transforms);

        let progress_fixture = clock_fixture(
            4,
            &[("A/data;derive/B/A-to-B", 5), ("B/data;derive/B/B-to-B", 6)],
        );

        // Stat of "B" completes, updating progress on reading "A" & "B" data.
        graph.completed_stat(
            &stat_fixture(0, "B"),
            progress_fixture.clone(),
            clock_fixture(4, &[("B/data", 6)]), // Contained by |progress_fixture|.
        );

        // Ingest of "A" completes.
        graph.completed_ingest(
            &step_fixture(TestStepType::Ingest, "A"),
            clock_fixture(4, &[("A/data", 5)]), // Contained by |progress_fixture|.
        );

        // Expect no pending stat of B was created (though it cycles, it's already read it's own write).
        itertools::assert_equal(graph.pending.iter(), vec![].into_iter());

        assert_eq!(
            graph.write_clock,
            clock_fixture(4, &[("A/data", 5), ("B/data", 6)])
        );

        // Completed ingest & stat which *do* require a new stat.
        graph.completed_ingest(
            &step_fixture(TestStepType::Ingest, "A"),
            clock_fixture(4, &[("A/data", 50)]),
        );
        graph.completed_stat(
            &stat_fixture(0, "B"),
            progress_fixture.clone(),
            clock_fixture(4, &[("B/data", 60)]),
        );

        itertools::assert_equal(
            graph.pending.into_iter(),
            vec![(
                stat_fixture(0, "B"),
                clock_fixture(
                    4,
                    &[
                        ("A/data;derive/B/A-to-B", 50),
                        ("B/data;derive/B/B-to-B", 60),
                    ],
                ),
            )]
            .into_iter(),
        );

        assert_eq!(
            graph.write_clock,
            clock_fixture(4, &[("A/data", 50), ("B/data", 60)])
        );
    }

    #[test]
    fn test_ready_stats() {
        let transforms = vec![
            transform_fixture("A", "A-to-A", "A", 0),
            transform_fixture("A", "A-to-B", "B", 0),
            transform_fixture("A", "A-to-C", "C", 0),
        ];
        let mut graph = Graph::new(&transforms);

        graph.pending.extend(
            vec![
                (stat_fixture(10, "A"), clock_fixture(1, &[])),
                (stat_fixture(10, "B"), clock_fixture(2, &[])),
                (stat_fixture(5, "C"), clock_fixture(3, &[])),
            ]
            .into_iter(),
        );

        assert!(matches!(graph.pop_ready_stats(), Err(Some(5))));
        graph.completed_advance(4);
        assert!(matches!(graph.pop_ready_stats(), Err(Some(1))));
        graph.completed_advance(1);

        assert_eq!(
            graph.pop_ready_stats().unwrap(),
            vec![(stat_fixture(5, "C"), clock_fixture(3, &[]))]
        );

        assert!(matches!(graph.pop_ready_stats(), Err(Some(5))));
        graph.completed_advance(5);

        assert_eq!(
            graph.pop_ready_stats().unwrap(),
            vec![
                (stat_fixture(10, "A"), clock_fixture(1, &[])),
                (stat_fixture(10, "B"), clock_fixture(2, &[])),
            ]
        );

        assert!(matches!(graph.pop_ready_stats(), Err(None)));
    }
}
