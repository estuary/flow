//! The `Driver` trait and the `run_test_case` scheduling loop.
//!
//! `Driver` abstracts the IO the loop requests — read, ingest, verify, advance —
//! so the scheduling itself is unit-testable against a mock which merely records
//! its calls. The live implementation drives runtime-next sessions.

use crate::clock::Clock;
use crate::graph::{Graph, PendingRead, TestTime};
use anyhow::Context;
use proto_flow::flow::{TestSpec, test_spec::step::Type as StepType};

/// Executes the IO actions the scheduler requests. `read_through` clocks carry
/// a shuffle suffix; `write_at` clocks do not (see the [`crate::clock`] docs).
#[allow(async_fn_in_trait)]
pub trait Driver {
    /// Begin a transaction: the writes of every action until the next call
    /// belong to one transaction, and are unordered relative to one another.
    /// Called once per ingest step and once per batch of concurrently-ready
    /// reads — derivations reading at the same synthetic instant are concurrent
    /// transactions in production, and their outputs race.
    fn begin_transaction(&mut self);

    /// Perform a pending read which can now be expected to complete. Returns the
    /// derivation's `(read_through, write_at)` progress.
    async fn read(&mut self, read: &PendingRead) -> anyhow::Result<(Clock, Clock)>;

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

/// Run one test case using the given `graph` and `driver`.
///
/// On failure the graph is still quiesced before returning: a case which fails
/// partway leaves reads pending (and possibly delayed into the future), and
/// those must not leak into the next case — where they would feed a just-reset
/// connector and write inside that case's verify window. The step failure is
/// the error returned; a failure while quiescing is appended to it.
pub async fn run_test_case<D: Driver>(
    graph: &mut Graph,
    driver: &mut D,
    test: &TestSpec,
) -> anyhow::Result<()> {
    let result = run_steps(graph, driver, test).await;
    let Err(err) = &result else {
        return result;
    };

    // Drive pending reads, advancing synthetic time as needed, until none remain.
    let quiesced = async {
        loop {
            let (ready, next_ready, _) = graph.pop_ready_reads();

            if !ready.is_empty() {
                driver.begin_transaction();
            }
            for pending in &ready {
                let (read, write) = driver.read(pending).await.context("driver.read")?;
                graph.completed_read(&pending.derivation, read, &write);
            }
            if !ready.is_empty() {
                continue;
            }
            let Some(next_ready) = next_ready else {
                return anyhow::Ok(());
            };
            driver.advance(next_ready).await.context("driver.advance")?;
            graph.completed_advance(next_ready);
        }
    }
    .await;

    if let Err(quiesce_err) = quiesced {
        anyhow::bail!(
            "{err:#}\n\nalso failed to quiesce pending reads after the failure: {quiesce_err:#}"
        );
    }
    result
}

async fn run_steps<D: Driver>(
    graph: &mut Graph,
    driver: &mut D,
    test: &TestSpec,
) -> anyhow::Result<()> {
    let initial = graph.write_clock().clone();
    let mut test_step = 0usize;

    loop {
        let (ready, next_ready, next_name) = graph.pop_ready_reads();

        if !ready.is_empty() {
            driver.begin_transaction();
        }
        for pending in &ready {
            let (read, write) = driver.read(pending).await.context("driver.read")?;
            graph.completed_read(&pending.derivation, read, &write);
        }

        if !ready.is_empty() {
            continue;
        }

        let step = test.steps.get(test_step);

        // Ingest steps always run immediately.
        if let Some(step) = step
            && step.step_type == StepType::Ingest as i32
        {
            driver.begin_transaction();
            let write = driver.ingest(test, test_step).await.context("ingest")?;
            graph.completed_ingest(&write);
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

        // Advance time to unblock the next pending read.
        if let Some(next_ready) = next_ready {
            tracing::trace!(
                delta = %next_ready,
                derivation = next_name.as_deref().unwrap_or_default(),
                "advancing synthetic test time to unblock the next read",
            );
            driver.advance(next_ready).await.context("driver.advance")?;
            graph.completed_advance(next_ready);
            continue;
        }

        assert_eq!(test_step, test.steps.len(), "unexpected test steps remain",);
        return Ok(());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::{Graph, Transform};
    use proto_flow::flow::TestSpec;
    use proto_flow::flow::test_spec::{Step, step::Type as StepType};

    /// These fixtures name collections by a stand-in template that is just the
    /// collection's name, which keeps their journal names short.
    fn transform(source: &str, transform: &str, derivation: &str, delay: u32) -> Transform {
        Transform {
            source: source.to_string(),
            journal_read_suffix: format!("derive/{derivation}/{transform}"),
            read_delay: TestTime::from_secs(delay),
        }
    }

    /// A recording mock: each action appends an event, and ingest returns a
    /// fixed one-document clock. The event log is the snapshotted schedule.
    #[derive(Default)]
    struct MockDriver {
        events: Vec<String>,
        /// When set, every verify fails.
        fail_verify: bool,
    }

    impl Driver for MockDriver {
        fn begin_transaction(&mut self) {
            self.events.push("begin_transaction".to_string());
        }

        async fn read(&mut self, read: &PendingRead) -> anyhow::Result<(Clock, Clock)> {
            let journals: Vec<String> = read.read_through.keys().cloned().collect();
            self.events.push(format!(
                "read derivation={} ready_at={} read_through={:?}",
                read.derivation, read.ready_at, journals
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
            if self.fail_verify {
                anyhow::bail!("mock verify failure");
            }
            Ok(())
        }

        async fn advance(&mut self, delta: TestTime) -> anyhow::Result<()> {
            self.events.push(format!("advance {delta}"));
            Ok(())
        }
    }

    /// Ingest A, then verify B, with four transforms of varying read delay.
    fn fixture() -> (Graph, TestSpec) {
        let mut graph = Graph::new();
        graph.add_derivation(
            "B".to_string(),
            "B".to_string(),
            &[
                transform("A", "A-to-B-fast", "B", 0),
                transform("A", "A-to-B-slow", "B", 3),
            ],
        );
        graph.add_derivation(
            "Y".to_string(),
            "Y".to_string(),
            &[transform("A", "A-to-Y", "Y", 2)],
        );
        graph.add_derivation(
            "Z".to_string(),
            "Z".to_string(),
            &[transform("A", "A-to-Z", "Z", 5)],
        );

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
        (graph, test)
    }

    /// The snapshot encodes the cascade of reads, the lazy time advances, and
    /// the verify gating on `has_pending_write`.
    #[tokio::test]
    async fn test_case_execution() {
        let (mut graph, test) = fixture();
        let mut driver = MockDriver::default();
        run_test_case(&mut graph, &mut driver, &test).await.unwrap();

        insta::assert_debug_snapshot!(driver.events, @r#"
        [
            "begin_transaction",
            "ingest step=0 collection=A",
            "begin_transaction",
            "read derivation=B ready_at=0ns read_through=[\"A/data;derive/B/A-to-B-fast\"]",
            "advance 2s",
            "begin_transaction",
            "read derivation=Y ready_at=2s read_through=[\"A/data;derive/Y/A-to-Y\"]",
            "advance 1s",
            "begin_transaction",
            "read derivation=B ready_at=3s read_through=[\"A/data;derive/B/A-to-B-slow\"]",
            "verify step=1 collection=B",
            "advance 2s",
            "begin_transaction",
            "read derivation=Z ready_at=5s read_through=[\"A/data;derive/Z/A-to-Z\"]",
        ]
        "#);
    }

    /// A failing verify still quiesces the graph: the delayed read of Z runs
    /// before the case returns, so nothing is pending for the next case.
    #[tokio::test]
    async fn failed_case_quiesces_pending_reads() {
        let (mut graph, test) = fixture();
        let mut driver = MockDriver {
            fail_verify: true,
            ..Default::default()
        };
        let err = run_test_case(&mut graph, &mut driver, &test)
            .await
            .unwrap_err();
        assert_eq!(format!("{err:#}"), "verify: mock verify failure");

        insta::assert_debug_snapshot!(driver.events, @r#"
        [
            "begin_transaction",
            "ingest step=0 collection=A",
            "begin_transaction",
            "read derivation=B ready_at=0ns read_through=[\"A/data;derive/B/A-to-B-fast\"]",
            "advance 2s",
            "begin_transaction",
            "read derivation=Y ready_at=2s read_through=[\"A/data;derive/Y/A-to-Y\"]",
            "advance 1s",
            "begin_transaction",
            "read derivation=B ready_at=3s read_through=[\"A/data;derive/B/A-to-B-slow\"]",
            "verify step=1 collection=B",
            "advance 2s",
            "begin_transaction",
            "read derivation=Z ready_at=5s read_through=[\"A/data;derive/Z/A-to-Z\"]",
        ]
        "#);
        let (ready, next, _) = graph.pop_ready_reads();
        assert!(ready.is_empty() && next.is_none());
    }
}
