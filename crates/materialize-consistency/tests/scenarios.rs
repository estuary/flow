//! The suite's one seam: the scenario runner driving the reference connector.
//!
//! Each scenario is one test, run twice — against the clean reference build, where
//! it must pass, and against its paired defect, where it must fail. Everything
//! beneath this seam is covered transitively: shim framing, trace parsing, the
//! invariant checkers, split driving, task publication, destination reads.
//!
//! There are deliberately no unit seams below the runner. Fragmenting coverage
//! there is precisely how a suite ends up with green units and a blind end-to-end
//! result, which is the failure mode the whole suite exists to prevent.
//!
//! These tests need a running local stack and are excluded from the default
//! nextest profile. Run them with `mise run ci:consistency`.

use materialize_consistency::harness;
use materialize_consistency::scenarios::{self, Scenario, Subject};

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("materialize_consistency=info".parse().unwrap()),
        )
        .with_test_writer()
        .try_init();
}

/// Every scenario reached by a test in this file.
///
/// Duplicating the list is the price of one test function per scenario, which is what
/// gives each its own pass/fail line and lets one be run alone. The test below makes
/// the duplication safe: a scenario added to the table without a test here does not
/// silently never run.
const COVERED: &[&str] = &[
    "baseline",
    "crash-between-commits",
    "replayed-acknowledge",
    "crash-mid-store",
    "crash-at-flush",
    "split-during-store",
    "split-during-commit",
    "counter-split-during-commit",
    "join-after-split",
    "zombie-at-start-commit",
    "counter-resumes-from-destination",
    "counter-reconciles-with-destination",
    "counter-crash-in-split-leader",
    "counter-crash-in-split-non-leader",
    "delta-replay-deduplicated",
    "at-least-once-never-loses",
];

/// A scenario nobody runs is worse than a missing scenario: the table says it is
/// covered. This is how `counter-survives-a-split` was caught having no test.
#[test]
fn every_scenario_is_reached_by_a_test() {
    for scenario in scenarios::all() {
        assert!(
            COVERED.contains(&scenario.name),
            "scenario {} has no test in this file, so it never runs",
            scenario.name,
        );
    }
    for name in COVERED {
        assert!(
            scenarios::all().iter().any(|s| &s.name == name),
            "{name} is listed as covered but is not a scenario",
        );
    }
}

fn scenario(name: &str) -> Scenario {
    scenarios::all()
        .into_iter()
        .find(|s| s.name == name)
        .unwrap_or_else(|| panic!("no scenario named {name}"))
}

/// Run one scenario clean, then against its paired defect, asserting the outcome
/// flips.
///
/// The negative case runs in the same test rather than a separate one on purpose:
/// a checker that goes blind through refactoring then fails a test instead of
/// quietly passing everything.
async fn both_ways(name: &str) {
    init_tracing();

    let scenario = scenario(name);
    let stack = harness::stack::Stack::from_env().expect("stack environment");

    // A real connector named in the environment is run *once*. The second pass exists to
    // prove the harness can tell a good subject from a bad one, and it can only do that
    // against the reference connector, whose defects are switchable. Running a real
    // connector twice would double the cost of every scenario to learn nothing: there is
    // no defective build of it to compare against.
    let external = harness::subject::external()
        .await
        .expect("resolving the subject named in the environment");

    let (connector, subject) = match &external {
        Some(external) => (
            external.connector.clone(),
            Subject {
                connector: vec![external.connector.to_string_lossy().to_string()],
                config: external.config.clone(),
            },
        ),
        None => {
            let connector = stack
                .binary("materialize-reference")
                .expect("the reference connector is built");
            let subject = scenario.subject(&connector, false);
            (connector, subject)
        }
    };

    let clean = harness::run(&scenario, &subject, external.as_ref())
        .await
        .expect("the clean run completes");

    for exempt in &scenario.exempt {
        eprintln!("exempt: [{}] {}", exempt.invariant, exempt.justification);
    }
    // A scenario blocked on the runtime is an *expected failure*: it fails, loudly and
    // with its violation count, and stays failing until the runtime closes the gap. It
    // is not silenced, because a silenced scenario is one nobody looks at again — the
    // violations it reports are the measurement of the gap, and they belong in the
    // output rather than behind a marker.
    //
    // The defect pairing is skipped: a subject that cannot uphold the invariant clean
    // tells us nothing about whether its defect would have been caught.
    if let Some(limitation) = scenario.known_limitation {
        panic!(
            "EXPECTED FAILURE — blocked on a runtime gap, not a connector defect.\n\
             {}\n\n\
             {limitation}\n\n\
             Remove `blocked_on_runtime` from this scenario once the runtime upholds \
             the guarantee above, and it becomes an ordinary passing scenario.",
            clean.summary(),
        );
    }

    assert!(
        clean.passed(),
        "the clean build must uphold {}:\n{}",
        scenario.verifies,
        clean.summary(),
    );
    if !scenario.faults.is_empty() {
        assert!(
            clean.faults_fired > 0,
            "no fault fired, so {name} verified nothing:\n{}",
            clean.summary(),
        );
    }
    eprintln!("clean: {}", clean.summary());

    if external.is_some() {
        return; // Single pass: see above.
    }
    let Some(defect) = scenario.defect else {
        return; // The baseline has nothing to pair with.
    };

    // A defect can surface two ways, and both count as caught: corrupt data, or a
    // task that cannot run at all. `ignore-key-range` is the second kind — two
    // shards fencing each other off means neither can commit — and insisting on a
    // clean verdict would turn a detected defect into a harness error.
    match harness::run(&scenario, &scenario.subject(&connector, true), None).await {
        Ok(defective) => {
            assert!(
                !defective.passed(),
                "{name} did not catch {defect:?}, so it cannot be trusted to catch a real \
                 regression of the same shape:\n{}",
                defective.summary(),
            );
            eprintln!("defective ({defect:?}): {}", defective.summary());

            // This failure was the point, so its debris is not evidence of
            // anything. Only unexpected failures leave a run directory behind.
            let _ = std::fs::remove_dir_all(&defective.run_dir);
        }
        Err(err) => eprintln!("defective ({defect:?}): the task could not run: {err:#}"),
    }
}

#[tokio::test]
async fn baseline() {
    both_ways("baseline").await
}

#[tokio::test]
async fn crash_between_commits() {
    both_ways("crash-between-commits").await
}

#[tokio::test]
async fn replayed_acknowledge() {
    both_ways("replayed-acknowledge").await
}

#[tokio::test]
async fn crash_mid_store() {
    both_ways("crash-mid-store").await
}

#[tokio::test]
async fn crash_at_flush() {
    both_ways("crash-at-flush").await
}

#[tokio::test]
async fn split_during_store() {
    both_ways("split-during-store").await
}

#[tokio::test]
async fn split_during_commit() {
    both_ways("split-during-commit").await
}

#[tokio::test]
async fn counter_split_during_commit() {
    both_ways("counter-split-during-commit").await
}

#[tokio::test]
async fn join_after_split() {
    both_ways("join-after-split").await
}

#[tokio::test]
async fn zombie_at_start_commit() {
    both_ways("zombie-at-start-commit").await
}

#[tokio::test]
async fn counter_resumes_from_destination() {
    both_ways("counter-resumes-from-destination").await
}

#[tokio::test]
async fn counter_reconciles_with_destination() {
    both_ways("counter-reconciles-with-destination").await
}

#[tokio::test]
async fn counter_crash_in_split_leader() {
    both_ways("counter-crash-in-split-leader").await
}

#[tokio::test]
async fn counter_crash_in_split_non_leader() {
    both_ways("counter-crash-in-split-non-leader").await
}

#[tokio::test]
async fn delta_replay_deduplicated() {
    both_ways("delta-replay-deduplicated").await
}

#[tokio::test]
async fn at_least_once_never_loses() {
    both_ways("at-least-once-never-loses").await
}
