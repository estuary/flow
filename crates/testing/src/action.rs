use super::{Clock, Graph, PendingStat};
use models::tables;
use protocol::flow::test_spec::step::Type as TestStepType;

/// Action atom returned by Driver to progress the test.
#[derive(Debug)]
pub enum Action<'a> {
    /// Stat one or more pending shard stats which can now be expected to have completed.
    Stat(Vec<(PendingStat, Clock)>),
    /// Execute an "Ingest" TestStep.
    Ingest(&'a tables::TestStep),
    /// Execute a "Verify" TestStep.
    Verify(&'a tables::TestStep),
    /// Advance test time by the given number of seconds.
    Advance(u64),
}

pub struct Case<'a>(pub &'a [&'a tables::TestStep]);

impl<'a> Action<'a> {
    pub fn next(graph: &mut Graph, case: &mut Case<'a>) -> Option<Self> {
        let advance = match graph.pop_ready_stats() {
            Ok(ready) => return Some(Self::Stat(ready)),
            Err(advance) => advance,
        };

        match (advance, case.0.split_first()) {
            // Test is complete.
            (None, None) => None,
            // No test steps remain, but we have remaining PendingStats to drain.
            (Some(advance), None) => {
                assert!(advance > 0);
                Some(Action::Advance(advance))
            }
            // Ingest test steps always run immediately.
            (
                _,
                Some((
                    ingest
                    @
                    tables::TestStep {
                        step_type: TestStepType::Ingest,
                        ..
                    },
                    tail,
                )),
            ) => {
                // Dequeue and return a next Ingest.
                case.0 = tail;
                Some(Action::Ingest(ingest))
            }
            // Verify test steps run only if no dependent PendingStats remain.
            // Otherwise, we must advance time as much as is needed (and no more!)
            // in order to unblock all antecedent PendingStats.
            (
                advance,
                Some((
                    verify
                    @
                    tables::TestStep {
                        step_type: TestStepType::Verify,
                        ..
                    },
                    tail,
                )),
            ) => match advance {
                Some(advance) if graph.has_pending_parent(&verify.collection) => {
                    Some(Action::Advance(advance))
                }
                _ => {
                    // Dequeue and return a next Verify.
                    case.0 = tail;
                    Some(Action::Verify(verify))
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{clock_fixture, step_fixture, transform_fixture};
    use super::{Action, Case, Graph, TestStepType};

    #[test]
    fn test_action_simulation() {
        let transforms = vec![
            transform_fixture("A", "A-to-B-fast", "B", 0),
            transform_fixture("A", "A-to-B-slow", "B", 3),
            transform_fixture("A", "A-to-Y", "Y", 2),
            transform_fixture("A", "A-to-Z", "Z", 5),
        ];
        let steps = vec![
            step_fixture(TestStepType::Ingest, "A"),
            step_fixture(TestStepType::Verify, "B"),
        ];
        let steps: Vec<_> = steps.iter().collect();

        let mut graph = Graph::new(&transforms);
        let mut case = Case(&steps);

        let expect_stat = |action, derivation: &str| match action {
            Some(Action::Stat(pending))
                if pending.len() == 1 && pending[0].0.derivation.as_str() == derivation =>
            {
                pending
            }
            _ => panic!("expected stat"),
        };

        // Initial ingestion into A.
        assert!(matches!(
            Action::next(&mut graph, &mut case),
            Some(Action::Ingest(_))
        ));
        graph.completed_ingest(&steps[0], clock_fixture(1, &[("A/data", 1)]));

        // Stat of B from "A-to-B-fast" is immediately ready.
        let pending = expect_stat(Action::next(&mut graph, &mut case), "B");
        graph.completed_stat(&pending[0].0, clock_fixture(1, &[]), clock_fixture(1, &[]));

        // We must still advance until transform "A-to-B-slow" can run.
        assert!(matches!(
            Action::next(&mut graph, &mut case),
            Some(Action::Advance(2))
        ));
        graph.completed_advance(2);

        // "A-to-Y" unblocks first.
        let pending = expect_stat(Action::next(&mut graph, &mut case), "Y");
        graph.completed_stat(&pending[0].0, clock_fixture(1, &[]), clock_fixture(1, &[]));

        assert!(matches!(
            Action::next(&mut graph, &mut case),
            Some(Action::Advance(1))
        ));
        graph.completed_advance(1);

        // Now "A-to-B-slow" unblocks.
        let pending = expect_stat(Action::next(&mut graph, &mut case), "B");
        graph.completed_stat(&pending[0].0, clock_fixture(1, &[]), clock_fixture(1, &[]));

        // We may verify B, as no dependent stats remain.
        assert!(matches!(
            Action::next(&mut graph, &mut case),
            Some(Action::Verify(_))
        ));

        // No test steps remain, but we must still drain pending stats.
        assert!(matches!(
            Action::next(&mut graph, &mut case),
            Some(Action::Advance(2))
        ));
        graph.completed_advance(2);

        // "A-to-Z" unblocks.
        let pending = expect_stat(Action::next(&mut graph, &mut case), "Z");
        graph.completed_stat(&pending[0].0, clock_fixture(1, &[]), clock_fixture(1, &[]));

        // All done!
        assert!(matches!(Action::next(&mut graph, &mut case), None));
    }
}
