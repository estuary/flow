//! The suite's one seam: the scenario runner driving the reference connector.
//!
//! Each scenario is one test, run twice — against the clean reference build, where
//! it must pass, and against its paired defect, where it must fail. Everything
//! beneath this seam is covered transitively: shim framing, trace parsing, the
//! invariant checkers, split driving, task publication, destination reads.
//!
//! These tests need a running local stack and are excluded from the default
//! nextest profile. Run them with `mise run ci:consistency`.

use materialize_consistency::harness;
use materialize_consistency::invariants::Invariant;
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

// Verify that every scenario actually has tests and that every test is part of a scenario, i.e.
// there are no orphan scenarios or tests.
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
    // A scenario narrowed to fewer classes than can pass it is coverage silently lost: against
    // a real connector it reports as a passing test having run nothing. Two reasons to narrow
    // are accepted, and both are stated at the definition:
    //
    // - the *harness* cannot stage the perturbation for another class — `zombie-at-start-commit`
    //   orders its racing instances by their `Open` fences, which a non-fencing class lacks;
    // - a class the perturbation only sometimes exposes is not excluded at all: it runs, and a
    //   failure is attributed to the runtime gap rather than to the connector. See
    //   `RuntimeGap::raced`.
    //
    // Where a class provably cannot pass, `blocked_on_runtime` is used instead: it excuses that
    // class while still running the scenario against every other.
    //
    // This pin only catches a scenario narrowed to *one* class. A narrowing to two, as the split
    // scenarios use, passes it — so read a run's `not-applicable` lines to see what was skipped.
    const SINGLE_CLASS: &[&str] = &["zombie-at-start-commit"];
    for scenario in scenarios::all() {
        assert_eq!(
            scenario.applies_to.len() == 1,
            SINGLE_CLASS.contains(&scenario.name),
            "{}: applies to {:?}. A scenario runnable by one class only must be listed in \
             SINGLE_CLASS with the reasoning at its definition; one listed there must not \
             be widened without removing it.",
            scenario.name,
            scenario.applies_to,
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
async fn both_ways(name: &str) {
    init_tracing();

    let scenario = scenario(name);
    let stack = harness::stack::Stack::from_env().expect("stack environment");

    // A real connector named in the environment is run *once*. The second pass exists to
    // prove the harness can tell a good subject from a bad one, and it can only do that
    // against the reference connector, whose defects are switchable.
    let external = harness::subject::external()
        .await
        .expect("resolving the subject named in the environment");

    // Scenarios run against every class expected to pass them, which is nearly all of
    // them against nearly every class — see `Scenario::applies_to`.
    if let Some(external) = &external {
        let forced = std::env::var_os(harness::subject::ENV_RUN_INAPPLICABLE).is_some();

        if forced && !scenario.applies_to.contains(&external.class) {
            eprintln!(
                "EXPLORATORY: {} does not apply to {:?} and is being run anyway because {} is \
                 set.",
                scenario.name,
                external.class,
                harness::subject::ENV_RUN_INAPPLICABLE,
            );
        }

        if !forced && !scenario.applies_to.contains(&external.class) {
            eprintln!(
                "not-applicable: {} can only be upheld by {:?}; the subject named in \
                 {} implements {:?}.",
                scenario.name,
                scenario.applies_to,
                harness::subject::ENV_SUBJECT_CLASS,
                external.class,
            );
            return;
        }
    }

    // The class actually under test. For the reference connector that is the class the
    // scenario configures it as; for a real connector it is what the environment declared.
    let subject_class = external.as_ref().map_or(scenario.class, |e| e.class);

    let (connector, subject) = match &external {
        Some(external) => (
            external.connector.clone(),
            Subject {
                connector: vec![external.connector.to_string_lossy().to_string()],
                config: external.config.clone(),
                env: external.env.clone(),
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

    // A gap can manifest as a task that cannot run at all, so it produces no invariant
    // violations. This still counts as a gap, which is why the error is examined here rather than
    // left to the marker below: that marker reads an invariant verdict, and this run has none.
    //
    // An `Environment` failure is still excluded. Those say nothing about the subject, so counting
    // one as the gap would let a flaky stack manufacture the expected failure.
    let clean = match harness::run(&scenario, &subject, external.as_ref()).await {
        Ok(clean) => clean,
        Err(err) => {
            let environmental = err.chain().any(|e| e.is::<harness::Environment>());

            match &scenario.known_limitation {
                // Reached only by race, so the failure is *attributed* to the runtime rather
                // than the connector — and still fails the run. An observation nobody is forced
                // to read is one nobody reads: the gap is real when it lands.
                //
                // The cost is accepted deliberately: a `documentCounter` subject will fail this
                // scenario on the runs where the window is hit, and the message says why so the
                // failure is not mistaken for a connector defect. See `RuntimeGap::raced`.
                Some(gap)
                    if gap.raced && gap.classes.contains(&subject_class) && !environmental =>
                {
                    panic!(
                        "RUNTIME GAP OBSERVED — not a connector defect, and not this scenario's \
                         fault either. The task could not run to a verdict:\n  {err:#}\n\n\
                         This scenario reaches the gap only by race, and this run reached it. \
                         Declared for {:?}, of which the subject is {subject_class:?}.\n\n{}",
                        gap.classes, gap.detail,
                    );
                }
                Some(gap) if gap.classes.contains(&subject_class) && !environmental => panic!(
                    "EXPECTED FAILURE — blocked on a runtime gap, not a connector defect.\n\
                     The task could not run to a verdict, which is how this gap manifests for a \
                     subject that refuses rather than guesses:\n  {err:#}\n\n\
                     Expected to fail for {:?}, of which the subject is {subject_class:?}.\n\n\
                     {}\n\n\
                     Remove `blocked_on_runtime` from this scenario once the runtime upholds the \
                     guarantee above.",
                    gap.classes, gap.detail,
                ),
                _ => panic!("the clean run completes: {err:#}"),
            }
        }
    };

    // Printed with the count each one suppressed, because an exemption that never fires is
    // paperwork rather than a weakened guarantee.
    //
    // And a zero is not on its own grounds to delete an exemption: it may mean the violation
    // is *rare* rather than impossible. Deleting needs an argument that the violation cannot
    // occur, with the count as corroboration.
    for exempt in &scenario.exempt {
        // An exemption written about another class did not apply, and saying so is the point: it
        // means this subject was held to *more* than the scenario's own class is, which a silent
        // omission would leave looking like the exemption simply never fired.
        //
        // Whether it is held *in full* is a different question, and is read from the run's
        // effective exemptions rather than assumed: a real subject also carries the blanket
        // monotonicity exemption its read earns it, so scoping the scenario's own out leaves that
        // invariant exempt anyway.
        if let Some(classes) = exempt.classes {
            if !classes.contains(&subject_class) {
                let covered = clean
                    .exemptions
                    .iter()
                    .any(|e| e.invariant == exempt.invariant);
                eprintln!(
                    "held: [{}] is exempt only for {:?}, and the subject is {subject_class:?} — {}",
                    exempt.invariant,
                    classes,
                    match covered {
                        false => "so it was held to this invariant in full".to_string(),
                        true => "though another exemption still covers this invariant".to_string(),
                    },
                );
                continue;
            }
        }

        let suppressed = clean
            .exempted
            .iter()
            .filter(|v| v.invariant == exempt.invariant)
            .count();
        // A ceiling is reported only when it is actually enforced. Ceilings are per invariant and
        // the broadest claim governs, so an unbounded exemption for the same invariant lifts this
        // one's.
        let lifted_by_broader = scenario
            .exempt
            .iter()
            .any(|e| e.invariant == exempt.invariant && e.max_suppressed.is_none())
            || external.is_some() && exempt.invariant == Invariant::Monotonicity;

        eprintln!(
            "exempt: [{}] suppressed {suppressed}{} violation(s): {}",
            exempt.invariant,
            match exempt.max_suppressed {
                Some(_) if lifted_by_broader =>
                    " (ceiling lifted by a broader exemption)".to_string(),
                Some(max) => format!(" of at most {max}"),
                None => String::new(),
            },
            exempt.justification,
        );
    }
    // A scenario blocked on the runtime is an *expected failure* for the classes the gap
    // exposes: it fails, loudly and with its violation count, and stays failing until the
    // runtime closes the gap. It is not silenced, because a silenced scenario is one nobody
    // looks at again — the violations it reports are the measurement of the gap, and they
    // belong in the output rather than behind a marker.
    //
    // A class the gap does not expose falls through to the ordinary assertions below and
    // must pass. That is the more useful half of such a scenario: it is the standing
    // evidence that the perturbation is survivable at all, and therefore that the gap is
    // the runtime's rather than the suite asking for something impossible.
    //
    // The defect pairing is skipped for an exposed class: a subject that cannot uphold the
    // invariant clean tells us nothing about whether its defect would have been caught.
    //
    // Which half of this is reported depends on what the run actually did, because a declared gap
    // is a claim about the runtime and the runtime changes. Asserting the failure unconditionally
    // would report "EXPECTED FAILURE" over a run that passed — so the day the gap closes, the
    // suite would keep printing the old diagnosis of a run that no longer matches it, and the
    // declaration would never be removed. So a pass here fails too, with the opposite message.
    if let Some(gap) = &scenario.known_limitation {
        if gap.raced && gap.classes.contains(&subject_class) {
            // A raced gap is not asserted in the *pass* direction — missing the window is the
            // common case and says nothing — but violations mean the window was hit, and that
            // fails the run with the gap named as the cause.
            assert!(
                clean.passed(),
                "RUNTIME GAP OBSERVED — not a connector defect, and not this scenario's fault \
                 either. This scenario reaches the gap only by race, and this run reached it.\n\
                 {}\n\n\
                 Declared for {:?}, of which the subject is {subject_class:?}.\n\n{}",
                clean.summary(),
                gap.classes,
                gap.detail,
            );
            eprintln!(
                "gap not reached this run: {} declares a raced runtime gap for \
                 {subject_class:?} and this run missed the window, so the pass is evidence \
                 about this run only.",
                scenario.name,
            );
            return;
        }
        if gap.classes.contains(&subject_class) {
            assert!(
                !clean.passed(),
                "UNEXPECTED PASS — this scenario declares a runtime gap it no longer hits.\n\
                 {}\n\n\
                 Declared to fail for {:?}, of which the subject is {subject_class:?}:\n\n\
                 {}\n\n\
                 Confirm over a few runs that this is not the gap being intermittent, then \
                 remove `blocked_on_runtime` and let it be an ordinary passing scenario.",
                clean.summary(),
                gap.classes,
                gap.detail,
            );
            panic!(
                "EXPECTED FAILURE — blocked on a runtime gap, not a connector defect.\n\
                 {}\n\n\
                 Expected to fail for {:?}, of which the subject is {subject_class:?}.\n\n\
                 {}\n\n\
                 Remove `blocked_on_runtime` from this scenario once the runtime upholds \
                 the guarantee above, and it becomes an ordinary passing scenario.",
                clean.summary(),
                gap.classes,
                gap.detail,
            );
        }
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
        // A run that *failed* can still be the defect being caught — `ignore-key-range` leaves
        // two shards fencing each other so neither commits. But that only holds once the fault
        // has fired. Before it, a failure is the environment: a publish the stack refused, a
        // warmup gate that timed out, a split that never landed, a fault that never fired.
        Err(err) => {
            let unexercised = err
                .chain()
                .find_map(|e| e.downcast_ref::<harness::Environment>());

            if let Some(why) = unexercised {
                panic!(
                    "{name}'s defective half did not exercise {defect:?}, because {why}. That is \
                     the environment, not the subject — and passing here would leave this \
                     scenario's checkers unverified while reporting green:\n{err:#}",
                );
            }
            eprintln!("defective ({defect:?}): the task could not run after its fault: {err:#}");
        }
    }
}

/// One test per scenario, and the covered-names list, from a single declaration.
///
/// Each scenario needs its own test function so that it gets its own pass/fail line and can be
/// run alone by name.
macro_rules! scenario_tests {
    ($($name:literal => $test:ident,)*) => {
        const COVERED: &[&str] = &[$($name),*];

        $(
            #[tokio::test]
            async fn $test() {
                both_ways($name).await
            }
        )*
    };
}

scenario_tests! {
    "baseline" => baseline,
    "crash-between-commits" => crash_between_commits,
    "crash-mid-store" => crash_mid_store,
    "crash-at-flush" => crash_at_flush,
    "split-during-store" => split_during_store,
    "split-during-commit" => split_during_commit,
    "split-after-commit-before-apply" => split_after_commit_before_apply,
    "recovery-applies-committed-work" => recovery_applies_committed_work,
    "split-lands-on-prepared-transaction" => split_lands_on_prepared_transaction,
    "join-after-split" => join_after_split,
    "zombie-at-start-commit" => zombie_at_start_commit,
    "destination-ahead-of-checkpoint" => destination_ahead_of_checkpoint,
    "recovery-reconciles-with-destination" => recovery_reconciles_with_destination,
    "crash-in-split-leader" => crash_in_split_leader,
    "crash-in-split-non-leader" => crash_in_split_non_leader,
    "at-least-once-never-loses" => at_least_once_never_loses,
}
