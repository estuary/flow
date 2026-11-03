//! Process-owned recovery of a `STATE_DIR`: a pass at once, then another
//! `PAUSE` after each ends, until the owner drops its `Recovery`.
//!
//! Failures belong to the process log. `Logged` rate-limits persistent
//! problems, and `Metrics` summarize completed passes.

use super::{Vmm, launch, release};
use std::time::{Duration, Instant, SystemTime};

/// From the end of one pass to the start of the next.
pub(crate) const PAUSE: Duration = Duration::from_secs(30);
/// How long a problem which persists goes unlogged, once logged.
const QUIET: Duration = Duration::from_secs(600);

/// A process's recovery of one `STATE_DIR`, which stops when dropped. A pass
/// under way is abandoned where it is: a record is removed only once all it
/// names is gone, so the next pass, in this process or another, finishes it.
pub struct Recovery(tokio::task::JoinHandle<()>);

impl Drop for Recovery {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// This process's recovery passes, for its metrics.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Metrics {
    /// Whether this process has started a `Recovery`.
    pub started: bool,
    /// Passes which left a problem.
    pub failed_passes: u64,
    /// Problems the last pass left: each a resource, record or listing to
    /// retry, or a record or resource for an operator to inspect.
    pub pending: u64,
    /// When the last pass which left no problem ended, in seconds since the
    /// Unix epoch, or zero if none has.
    pub last_success: u64,
}

static METRICS: std::sync::Mutex<Metrics> = std::sync::Mutex::new(Metrics {
    started: false,
    failed_passes: 0,
    pending: 0,
    last_success: 0,
});

pub(super) fn metrics() -> Metrics {
    *METRICS.lock().unwrap()
}

pub(super) fn spawn(vmm: Vmm, runtime: &tokio::runtime::Handle) -> Recovery {
    METRICS.lock().unwrap().started = true;
    Recovery(runtime.spawn(async move {
        let logged = std::sync::Mutex::new(Logged::default());
        schedule(|| pass(&vmm, &logged)).await
    }))
}

async fn schedule<F: std::future::Future>(mut pass: impl FnMut() -> F) {
    loop {
        pass().await;
        tokio::time::sleep(PAUSE).await;
    }
}

/// One pass, whose problems are logged as `logged` allows. Returns how many
/// problems it left.
pub(super) async fn pass(vmm: &Vmm, logged: &std::sync::Mutex<Logged>) -> u64 {
    let problems = match launch::check_state_owner(std::path::Path::new(&vmm.state_dir)) {
        Ok(()) => release::recover(&vmm.podman, &vmm.state_dir).await,
        Err(err) => vec![release::Problem {
            message: "the VMM state directory is unsafe to recover, so nothing is released",
            subject: vmm.state_dir.clone(),
            error: format!("{err:#}"),
        }],
    };

    let ended = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("after the epoch")
        .as_secs();
    let pending = problems.len() as u64;
    {
        let mut metrics = METRICS.lock().unwrap();
        *metrics = metrics.passed(pending, ended);
    }

    for problem in logged.lock().unwrap().admit(&problems, Instant::now()) {
        tracing::error!(
            state_dir = %vmm.state_dir,
            subject = %problem.subject,
            error = %problem.error,
            repeats_after = ?QUIET,
            "{}",
            problem.message,
        );
    }
    pending
}

impl Metrics {
    fn passed(self, pending: u64, ended: u64) -> Self {
        Self {
            started: self.started,
            failed_passes: self.failed_passes + u64::from(pending != 0),
            pending,
            last_success: if pending == 0 {
                ended
            } else {
                self.last_success
            },
        }
    }
}

/// When each problem was last logged, by its message and subject.
#[derive(Default)]
pub(super) struct Logged(std::collections::HashMap<(&'static str, String), Instant>);

impl Logged {
    /// Which of a pass's `problems` to log at `now`: each not logged within
    /// `QUIET`. A problem the pass no longer has is forgotten, so that it's
    /// logged at once should it return.
    fn admit<'p>(
        &mut self,
        problems: &'p [release::Problem],
        now: Instant,
    ) -> Vec<&'p release::Problem> {
        let mut seen = std::collections::HashMap::new();
        let mut admitted = Vec::new();
        for problem in problems {
            let key = (problem.message, problem.subject.clone());
            let logged = match self.0.get(&key) {
                Some(logged) if now.duration_since(*logged) < QUIET => *logged,
                _ => {
                    admitted.push(problem);
                    now
                }
            };
            seen.insert(key, logged);
        }
        self.0 = seen;
        admitted
    }
}

#[cfg(test)]
mod test {
    use super::{Logged, Metrics, PAUSE, QUIET};
    use std::time::{Duration, Instant};

    /// Cancellation must abandon an in-flight pass without scheduling another.
    #[tokio::test(start_paused = true)]
    async fn passes_are_scheduled_after_the_last_ends() {
        let began = tokio::time::Instant::now();
        let events = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let running = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let durations = [5, 0, 40, 3600];

        let task = tokio::spawn({
            let (events, running) = (events.clone(), running.clone());
            let mut n = 0;
            super::schedule(move || {
                let (events, running) = (events.clone(), running.clone());
                let lasts = Duration::from_secs(durations[n]);
                n += 1;
                async move {
                    let overlapped = running.swap(true, std::sync::atomic::Ordering::SeqCst);
                    let at = |what: &str| format!("{what} at {:?}", began.elapsed());
                    events.lock().unwrap().push(at(&format!("pass {n} began")));
                    assert!(!overlapped, "pass {n} overlaps another");
                    tokio::time::sleep(lasts).await;
                    events.lock().unwrap().push(at(&format!("pass {n} ended")));
                    running.store(false, std::sync::atomic::Ordering::SeqCst);
                }
            })
        });

        // Into the fourth pass, which would last an hour.
        tokio::time::sleep(PAUSE * 3 + Duration::from_secs(45 + 60)).await;
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        tokio::time::sleep(PAUSE * 10).await;

        insta::assert_debug_snapshot!(events.lock().unwrap(), @r#"
        [
            "pass 1 began at 0ns",
            "pass 1 ended at 5s",
            "pass 2 began at 35s",
            "pass 2 ended at 35s",
            "pass 3 began at 65s",
            "pass 3 ended at 105s",
            "pass 4 began at 135s",
        ]
        "#);
    }

    #[test]
    fn repeated_problems_are_logged_quietly() {
        let problem = |message: &'static str, subject: &str, error: &str| super::release::Problem {
            message,
            subject: subject.to_string(),
            error: error.to_string(),
        };
        let network = problem("failed", "network fv_0", "in use");
        let volume = problem("failed", "volume fv_0", "in use");
        let malformed = problem("malformed", "/state/fv_1.owner", "is version 2");

        let start = Instant::now();
        let mut logged = Logged::default();
        let mut table = Vec::new();
        for (at, problems) in [
            (0, vec![&network, &malformed]),
            (30, vec![&network, &malformed, &volume]),
            (60, vec![&malformed]),
            (90, vec![&network, &malformed]),
            (QUIET.as_secs() - 1, vec![&network, &malformed]),
            (QUIET.as_secs(), vec![&network, &malformed]),
            (QUIET.as_secs() + 30, vec![]),
            (QUIET.as_secs() + 60, vec![&malformed]),
        ] {
            let problems: Vec<_> = problems.into_iter().cloned().collect();
            let admitted: Vec<String> = logged
                .admit(&problems, start + Duration::from_secs(at))
                .into_iter()
                .map(|problem| format!("{}: {}", problem.message, problem.subject))
                .collect();
            table.push(format!("{at}s: {admitted:?}"));
        }
        insta::assert_debug_snapshot!(table, @r#"
        [
            "0s: [\"failed: network fv_0\", \"malformed: /state/fv_1.owner\"]",
            "30s: [\"failed: volume fv_0\"]",
            "60s: []",
            "90s: [\"failed: network fv_0\"]",
            "599s: []",
            "600s: [\"malformed: /state/fv_1.owner\"]",
            "630s: []",
            "660s: [\"malformed: /state/fv_1.owner\"]",
        ]
        "#);
    }

    #[test]
    fn metrics_follow_passes() {
        let mut metrics = Metrics {
            started: true,
            ..Default::default()
        };
        let mut table = Vec::new();
        for (pending, ended) in [(0, 100), (2, 130), (3, 160), (0, 190), (1, 220)] {
            metrics = metrics.passed(pending, ended);
            table.push(format!("{pending} problems at {ended}: {metrics:?}"));
        }
        insta::assert_debug_snapshot!(table, @r#"
        [
            "0 problems at 100: Metrics { started: true, failed_passes: 0, pending: 0, last_success: 100 }",
            "2 problems at 130: Metrics { started: true, failed_passes: 1, pending: 2, last_success: 100 }",
            "3 problems at 160: Metrics { started: true, failed_passes: 2, pending: 3, last_success: 100 }",
            "0 problems at 190: Metrics { started: true, failed_passes: 2, pending: 0, last_success: 190 }",
            "1 problems at 220: Metrics { started: true, failed_passes: 3, pending: 1, last_success: 190 }",
        ]
        "#);
    }
}
