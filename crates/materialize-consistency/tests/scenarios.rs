//! The suite's one seam: the scenario runner driving the reference connector.
//!
//! Each scenario is one test, run twice — against the clean reference build, where
//! it must pass, and against its paired defect, where it must fail. Everything
//! beneath this seam is covered transitively: shim framing, trace parsing, the
//! invariant checkers, split driving, task publication, destination reads.
//!
//! Unit seams below this one exist only where something can be wrong in a way that makes a
//! scenario *pass* — the invariant checkers above all. What is deliberately absent is a seam
//! that would let a scenario be replaced by unit coverage, which is how a suite ends up with
//! green units and a blind end-to-end result.
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

/// A scenario nobody runs is worse than a missing scenario: the table says it is covered. This
/// is how an earlier split scenario was caught having no test at all.
///
/// `COVERED` comes from `scenario_tests!` at the foot of this file, so it names exactly the
/// scenarios that have a test. What remains for this guard is the other direction: a scenario
/// added to `scenarios::all()` and never given a test.
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
    // - the perturbation reaches a class's exposure only by *race*, so asking would report the
    //   runtime's gap as the connector's defect on some runs — `MEMBERSHIP_CHANGE_FAIRLY_ASKED`.
    //
    // Where a class provably cannot pass, `blocked_on_runtime` is used instead: it excuses that
    // class while still running the scenario against every other.
    //
    // This pin covers single-class narrowings only. A two-class narrowing like
    // `MEMBERSHIP_CHANGE_FAIRLY_ASKED` is invisible to it, which is why the README lists the
    // exclusions and a run prints its own `not-applicable` lines.
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

    // Scenarios run against every class expected to pass them, which is nearly all of
    // them against nearly every class — see `Scenario::applies_to`. A scenario is skipped
    // only where its own class is the only one that can succeed, because there the failure
    // would measure the mismatch rather than the connector.
    if let Some(external) = &external {
        if !scenario.applies_to.contains(&external.class) {
            eprintln!(
                "not-applicable: {} can only be upheld by {:?}; the subject named in \
                 {} implements {:?}. Nothing was run.",
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

    // Printed with the count each one suppressed, because an exemption that never fires is
    // paperwork rather than a weakened guarantee — and until this was reported there was no
    // way to tell the two apart.
    //
    // Trust these counts on a *reference* run only. A real subject also gets the blanket
    // monotonicity exemption, which matches the same violations, so a scenario-level
    // monotonicity exemption is credited for work the blanket one would have done anyway.
    //
    // And a zero is not on its own grounds to delete an exemption: it may mean the violation
    // is *rare* rather than impossible. Deleting needs an argument that the violation cannot
    // occur, with the count as corroboration — `at-least-once-never-loses`'s conservation
    // exemption measures zero on most runs and is still load-bearing, while the two removed
    // in 5525ae9c19f were unreachable by construction as well as unmeasured.
    for exempt in &scenario.exempt {
        let suppressed = clean
            .exempted
            .iter()
            .filter(|v| v.invariant == exempt.invariant)
            .count();
        eprintln!(
            "exempt: [{}] suppressed {suppressed} violation(s): {}",
            exempt.invariant, exempt.justification,
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
    if let Some(gap) = &scenario.known_limitation {
        if gap.classes.contains(&subject_class) {
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
        // two shards fencing each other so neither commits, and insisting on a violation list
        // would turn a detected defect into a harness error. But that only holds once the fault
        // has fired. Before it, a failure is the environment: a publish the stack refused, a
        // warmup gate that timed out, a split that never landed, a fault that never fired.
        //
        // Both are typed, so this asks for evidence rather than accepting the absence of a
        // clean result as evidence. That default was the wrong way round: a stack degrading
        // between the clean and defective halves silently vacated the pairing, which is the
        // exact regression the pairing exists to detect.
        Err(err) => {
            let unexercised = err.chain().find_map(|e| {
                if e.is::<harness::stack::PublishFailed>() {
                    Some("the stack would not publish it")
                } else if e.is::<harness::BeforeFault>() {
                    Some("it failed before its fault fired")
                } else {
                    None
                }
            });

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
/// run alone by name — but written out by hand, that was fourteen identical bodies beside a
/// separately-maintained list of the same names. The list could then claim a scenario was
/// covered while no test existed to run it, which `every_scenario_is_reached_by_a_test` could
/// not detect: it compared the list against the scenario table, and the test functions were
/// nowhere in the comparison.
///
/// Emitting both from one place makes that unrepresentable. A name cannot appear in `COVERED`
/// without the test that runs it, because the same macro invocation produces both.
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
    "split-lands-on-prepared-transaction" => split_lands_on_prepared_transaction,
    "join-after-split" => join_after_split,
    "zombie-at-start-commit" => zombie_at_start_commit,
    "destination-ahead-of-checkpoint" => destination_ahead_of_checkpoint,
    "recovery-reconciles-with-destination" => recovery_reconciles_with_destination,
    "crash-in-split-leader" => crash_in_split_leader,
    "crash-in-split-non-leader" => crash_in_split_non_leader,
    "at-least-once-never-loses" => at_least_once_never_loses,
}
