//! The `Driver` trait and `run_test_case` loop, ported from
//! `go/testing/action.go`.
//!
//! `run_test_case` drives one `TestSpec` to completion against a [`Driver`]:
//! it repeatedly pops and executes ready stats (cascading through the
//! [`Graph`]), runs INGEST steps immediately, runs VERIFY steps only once the
//! target collection has no pending upstream write, and advances synthetic time
//! to unblock the next read-delayed stat when neither steps nor ready stats can
//! otherwise progress. It's done when all steps are consumed and nothing is
//! pending.
//!
//! The `Driver` is abstract so the loop is testable with a mock (as in
//! `action_test.go`); the live implementation drives runtime-next sessions.

use crate::clock::Clock;
use crate::graph::{Graph, PendingStat, TestTime};
use anyhow::Context;
use proto_flow::flow::{TestSpec, test_spec::step::Type as StepType};

/// Executes the IO actions the scheduler requests. `read_through` clocks carry
/// a shuffle suffix; `write_at` clocks do not (mirroring Gazette's Stat).
#[allow(async_fn_in_trait)]
pub trait Driver {
    /// Apply a pending stat which can now be expected to complete. Returns the
    /// task's `(read_through, write_at)` progress.
    async fn stat(&mut self, stat: &PendingStat) -> anyhow::Result<(Clock, Clock)>;

    /// Execute an INGEST step, returning the resulting write clock.
    async fn ingest(&mut self, test: &TestSpec, test_step: usize) -> anyhow::Result<Clock>;

    /// Execute a VERIFY step over documents written in the window `(from, to]`.
    async fn verify(
        &mut self,
        test: &TestSpec,
        test_step: usize,
        from: &Clock,
        to: &Clock,
    ) -> anyhow::Result<()>;

    /// Advance synthetic test time by `delta`.
    async fn advance(&mut self, delta: TestTime) -> anyhow::Result<()>;
}

/// Run one test case using the given `graph` and `driver`. Returns the scope of
/// the last step reached (used for error reporting), on success.
pub async fn run_test_case<D: Driver>(
    graph: &mut Graph,
    driver: &mut D,
    test: &TestSpec,
) -> anyhow::Result<String> {
    let initial = graph.write_clock().clone();
    let mut test_step = 0usize;
    let mut scope = String::new();

    loop {
        let (ready, next_ready, _next_name) = graph.pop_ready_stats();

        for stat in &ready {
            let (read, write) = driver.stat(stat).await.context("driver.stat")?;
            graph.completed_stat(&stat.task_name, read, &write);
        }

        // If we completed stats, loop again to look for more ready stats.
        if !ready.is_empty() {
            continue;
        }

        let step = test.steps.get(test_step);
        if let Some(step) = step {
            scope = step.step_scope.clone();
        }

        // Ingest steps always run immediately.
        if let Some(step) = step
            && step.step_type == StepType::Ingest as i32
        {
            let write = driver.ingest(test, test_step).await.context("ingest")?;
            graph.completed_ingest(&step.collection, &write);
            test_step += 1;
            continue;
        }

        // Verify steps may run only if no dependent pending writes remain.
        if let Some(step) = step
            && step.step_type == StepType::Verify as i32
            && !graph.has_pending_write(&step.collection)
        {
            let to = graph.write_clock().clone();
            driver
                .verify(test, test_step, &initial, &to)
                .await
                .context("verify")?;
            test_step += 1;
            continue;
        }

        // Advance time to unblock the next pending stat.
        if let Some(next_ready) = next_ready {
            driver.advance(next_ready).await.context("driver.advance")?;
            graph.completed_advance(next_ready);
            continue;
        }

        // All steps completed, and no pending stats remain.
        assert_eq!(test_step, test.steps.len(), "unexpected test steps remain",);
        return Ok(scope);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::{Graph, Transform};
    use proto_flow::flow::TestSpec;
    use proto_flow::flow::test_spec::{Step, step::Type as StepType};

    fn transform(source: &str, transform: &str, derivation: &str, delay: u32) -> Transform {
        Transform {
            source: source.to_string(),
            journal_read_suffix: format!("derive/{derivation}/{transform}"),
            read_delay: TestTime::from_secs(delay),
        }
    }

    /// A recording mock: stats/advances/verifies record an event, and ingest
    /// returns a fixed one-document clock. The event log encodes the exact
    /// schedule the loop drives, which we snapshot.
    #[derive(Default)]
    struct MockDriver {
        events: Vec<String>,
    }

    impl Driver for MockDriver {
        async fn stat(&mut self, stat: &PendingStat) -> anyhow::Result<(Clock, Clock)> {
            let journals: Vec<String> = stat.read_through.keys().cloned().collect();
            self.events.push(format!(
                "stat task={} ready_at={} read_through={:?}",
                stat.task_name, stat.ready_at, journals
            ));
            Ok((Clock::new(), Clock::new()))
        }

        async fn ingest(&mut self, test: &TestSpec, test_step: usize) -> anyhow::Result<Clock> {
            let collection = &test.steps[test_step].collection;
            self.events
                .push(format!("ingest step={test_step} collection={collection}"));
            Ok(Clock::from([(format!("{collection}/data"), 1)]))
        }

        async fn verify(
            &mut self,
            test: &TestSpec,
            test_step: usize,
            _from: &Clock,
            _to: &Clock,
        ) -> anyhow::Result<()> {
            let collection = &test.steps[test_step].collection;
            self.events
                .push(format!("verify step={test_step} collection={collection}"));
            Ok(())
        }

        async fn advance(&mut self, delta: TestTime) -> anyhow::Result<()> {
            self.events.push(format!("advance {delta}"));
            Ok(())
        }
    }

    /// Port of `TestTestCaseExecution`: ingest A, then verify B, with four
    /// transforms of varying read delay. The snapshot encodes the cascade of
    /// stats, the lazy time advances, and the verify gating on
    /// `has_pending_write`.
    #[tokio::test]
    async fn test_case_execution() {
        let mut graph = Graph::new();
        graph.add_derivation(
            "B".to_string(),
            &[
                transform("A", "A-to-B-fast", "B", 0),
                transform("A", "A-to-B-slow", "B", 3),
            ],
        );
        graph.add_derivation("Y".to_string(), &[transform("A", "A-to-Y", "Y", 2)]);
        graph.add_derivation("Z".to_string(), &[transform("A", "A-to-Z", "Z", 5)]);

        let test = TestSpec {
            name: "test".to_string(),
            steps: vec![
                Step {
                    step_type: StepType::Ingest as i32,
                    collection: "A".to_string(),
                    ..Default::default()
                },
                Step {
                    step_type: StepType::Verify as i32,
                    collection: "B".to_string(),
                    ..Default::default()
                },
            ],
        };

        let mut driver = MockDriver::default();
        run_test_case(&mut graph, &mut driver, &test).await.unwrap();

        insta::assert_debug_snapshot!(driver.events, @r#"
        [
            "ingest step=0 collection=A",
            "stat task=B ready_at=0ns read_through=[\"A/data;derive/B/A-to-B-fast\"]",
            "advance 2s",
            "stat task=Y ready_at=2s read_through=[\"A/data;derive/Y/A-to-Y\"]",
            "advance 1s",
            "stat task=B ready_at=3s read_through=[\"A/data;derive/B/A-to-B-slow\"]",
            "verify step=1 collection=B",
            "advance 2s",
            "stat task=Z ready_at=5s read_through=[\"A/data;derive/Z/A-to-Z\"]",
        ]
        "#);
    }
}
