//! Pure throttle policy that decides when a collection's journals are
//! persistently rate-limited enough to warrant an automatic split.
//!
//! # Model
//!
//! Per journal we maintain a time-decayed exponentially-weighted moving average
//! (EWMA) of times we've been throttled.  Note that we don't care exactly how
//! much we've been throttled, we treat all throttling events equally.
//!
//! We target a threshold of ~10% throttled transactions over the last ~10 minutes, which
//! is enough to indicate a persistent problem without being too twitchy.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

/// Decay time constant of the throttle EWMA.  Set to be approximately half the time period.
const TAU: Duration = Duration::from_secs(5 * 60);

/// EWMA value at or above which a journal is considered persistently throttled.
/// ~0.10 == ~10% of transactions saw throttling over the last ~`TAU`.
const THRESHOLD: f64 = 0.10;

/// Quiet period after a split attempt during which a journal won't re-trigger.
/// This is a per-shard, per-journal-name quiet period — not a global split rate
/// limit. Each shard runs its own policy, so this isn't an ironclad limit
const COOLDOWN: Duration = Duration::from_secs(30 * 60);

/// Minimum span a journal must be observed before it may trigger a split.
const MIN_OBSERVATION_SPAN: Duration = Duration::from_secs(2 * 60);

/// Maximum age of a journal's most recent sample for it to still be eligible to
/// split.
const MAX_STALENESS: Duration = Duration::from_secs(5 * 60);

/// Upper bound on the `dt` used to compute `alpha`, which caps `alpha` itself.
/// Prevents a single throttled sample after a long idle gap from spiking the EWMA
const MAX_SAMPLE_DT: Duration = Duration::from_secs(30);

/// Tunable parameters of the policy. Defaults are the module constants; held as
/// a struct so a future task-flag could override them per-task.
#[derive(Debug, Clone, Copy)]
pub struct Config {
    pub tau: Duration,
    pub threshold: f64,
    pub cooldown: Duration,
    pub min_observation_span: Duration,
    pub max_sample_dt: Duration,
    pub max_staleness: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            tau: TAU,
            threshold: THRESHOLD,
            cooldown: COOLDOWN,
            min_observation_span: MIN_OBSERVATION_SPAN,
            max_sample_dt: MAX_SAMPLE_DT,
            max_staleness: MAX_STALENESS,
        }
    }
}

/// Per-journal throttle state. Decay is applied lazily on each [`observe`],
/// using the elapsed time since the previous sample, so a stored `ewma` is the
/// value as of `last_ts`.
///
/// [`observe`]: SplitPolicy::observe
#[derive(Debug, Clone, Copy)]
struct JournalThrottle {
    /// Time-decayed EWMA of the throttled-write boolean, as of `last_ts`.
    ewma: f64,
    /// Timestamp of the most recent sample.
    last_ts: Instant,
    /// Earliest instant at which this journal may trigger a split.
    not_before: Instant,
}

/// Tracks per-journal throttle pressure and reports which journals are due for a split.
#[derive(Debug, Default)]
pub struct SplitPolicy {
    journals: BTreeMap<String, JournalThrottle>,
    /// Journals that are "max auto split" already, so we ignore altogether.
    ignore: BTreeSet<String>,
    config: Config,
}

impl SplitPolicy {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub fn with_config(config: Config) -> Self {
        Self {
            journals: BTreeMap::new(),
            ignore: BTreeSet::new(),
            config,
        }
    }

    /// Record one per-transaction sample for `journal`
    pub fn observe(&mut self, journal: &str, throttled: bool, now: Instant) {
        // Journals at the minimum splittable width are a terminal off-ramp:
        // never fed, so they can never accumulate pressure or become due.
        if self.ignore.contains(journal) {
            return;
        }

        let tau_secs = self.config.tau.as_secs_f64();
        let max_dt = self.config.max_sample_dt;
        let min_span = self.config.min_observation_span;

        let throttle = if let Some(throttle) = self.journals.get_mut(journal) {
            throttle
        } else if throttled {
            // Only start tracking a journal if it has been throttled at least once.
            self.journals
                .entry(journal.to_owned())
                .or_insert_with(|| JournalThrottle {
                    ewma: 0.0,
                    last_ts: now,
                    not_before: now + min_span,
                })
        } else {
            return;
        };

        // Clamp `dt` to cap `alpha`.  Prevent the first sample after a long game
        // from spiking the EWMA all the way to 1.0 and triggering a split immediately.
        let dt = now.saturating_duration_since(throttle.last_ts).min(max_dt);
        let alpha = 1.0 - (-dt.as_secs_f64() / tau_secs).exp();

        let sample = if throttled { 1.0 } else { 0.0 };
        throttle.ewma += alpha * (sample - throttle.ewma);
        throttle.last_ts = now;
    }

    /// Returns the journals currently due for a split: over threshold, past the
    /// cold-start span, and not in cooldown. Ordered by journal name.
    pub(crate) fn due_for_split(&self, now: Instant) -> Vec<&str> {
        self.journals
            .iter()
            .filter(|(_, t)| self.is_due(t, now))
            .map(|(name, _)| name.as_str())
            .collect()
    }

    /// Whether a single `journal` is due for a split right now. Unknown journals
    /// are never due.
    pub fn should_split(&self, journal: &str, now: Instant) -> bool {
        self.journals
            .get(journal)
            .is_some_and(|t| self.is_due(t, now))
    }

    fn is_due(&self, throttle: &JournalThrottle, now: Instant) -> bool {
        // `not_before` gates both cold start and cooldown. The staleness check
        // ensures the journal is *currently* being written
        now >= throttle.not_before
            && throttle.ewma > self.config.threshold
            && now.saturating_duration_since(throttle.last_ts) <= self.config.max_staleness
    }

    /// Mark that a split was attempted for `journal` (whether or not it
    /// succeeded). Starts a cooldown and resets the journal's EWMA so pressure
    /// must re-accumulate before it can trigger again.
    pub(crate) fn mark_attempted(&mut self, journal: &str, now: Instant) {
        if let Some(throttle) = self.journals.get_mut(journal) {
            throttle.ewma = 0.0;
            throttle.last_ts = now;
            throttle.not_before = now + self.config.cooldown;
        }
    }

    /// Drop all state for `journal`
    pub(crate) fn forget(&mut self, journal: &str) {
        self.journals.remove(journal);
    }

    /// Ignore a given journal
    pub(crate) fn ignore(&mut self, journal: &str) {
        self.ignore.insert(journal.to_string());
        self.forget(journal);
    }

    /// Current EWMA for `journal`, as of its most recent sample. For metrics and
    /// tests; `None` if the journal is untracked.
    #[allow(dead_code)]
    fn throttle_ewma(&self, journal: &str) -> Option<f64> {
        self.journals.get(journal).map(|t| t.ewma)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Advanceable fake clock: a fixed base `Instant` plus an explicit offset.
    struct TestClock {
        base: Instant,
        now: Instant,
    }

    impl TestClock {
        fn new() -> Self {
            let base = Instant::now();
            Self { base, now: base }
        }
        /// Advance the clock and return the new `now`.
        fn advance(&mut self, by: Duration) -> Instant {
            self.now += by;
            self.now
        }
        fn now(&self) -> Instant {
            self.now
        }
        /// Elapsed since base, for assertions/scenarios.
        #[allow(dead_code)]
        fn elapsed(&self) -> Duration {
            self.now.saturating_duration_since(self.base)
        }
    }

    const J: &str = "acmeCo/collection/pivot=00";

    /// A journal throttled on every transaction crosses the threshold once the
    /// cold-start span has elapsed.
    #[test]
    fn steady_state_throttling_triggers() {
        let mut policy = SplitPolicy::new();
        let mut clock = TestClock::new();

        // First sample establishes the baseline; subsequent samples every 10s.
        policy.observe(J, true, clock.now());
        for _ in 0..30 {
            let now = clock.advance(Duration::from_secs(10));
            policy.observe(J, true, now);
        }
        // 300s of constant throttling: well past the 120s cold start and the
        // ~63s it takes the EWMA to reach 0.10.
        assert!(policy.throttle_ewma(J).unwrap() > THRESHOLD);
        assert_eq!(policy.due_for_split(clock.now()), vec![J]);
        assert!(policy.should_split(J, clock.now()));
    }

    /// Throttling on a small fraction of transactions settles below threshold
    /// and never triggers.
    #[test]
    fn intermittent_stays_under() {
        let mut policy = SplitPolicy::new();
        let mut clock = TestClock::new();

        policy.observe(J, false, clock.now());
        // ~5% of transactions throttled (1 in 20), sampled every 10s for 1000s.
        for i in 0..100 {
            let now = clock.advance(Duration::from_secs(10));
            policy.observe(J, i % 20 == 0, now);
            assert!(
                policy.due_for_split(now).is_empty(),
                "should never trigger at ~5% throttling (ewma={})",
                policy.throttle_ewma(J).unwrap(),
            );
        }
        assert!(policy.throttle_ewma(J).unwrap() < THRESHOLD);
    }

    /// A single throttled sample after a long idle gap must NOT trigger: the
    /// alpha cap bounds how far one post-idle sample can move the EWMA.
    #[test]
    fn idle_gap_then_one_sample_does_not_trigger() {
        let mut policy = SplitPolicy::new();
        let mut clock = TestClock::new();

        // First sample (baseline), then a 10-hour idle gap, then one throttled
        // sample. Without the alpha cap, dt this large gives alpha ≈ 1 and the
        // EWMA would jump to ~1.0; with the cap it can reach at most ~0.095.
        policy.observe(J, true, clock.now());
        let now = clock.advance(Duration::from_secs(10 * 60 * 60));
        policy.observe(J, true, now);

        let ewma = policy.throttle_ewma(J).unwrap();
        assert!(
            ewma < THRESHOLD,
            "capped single post-idle sample must stay under threshold, got {ewma}",
        );
        assert!(policy.due_for_split(now).is_empty());
    }

    /// Even when the EWMA is already over threshold, a journal can't trigger
    /// until it has been observed for the cold-start span.
    #[test]
    fn cold_start_suppresses_then_releases() {
        let mut policy = SplitPolicy::new();
        let mut clock = TestClock::new();

        // Hammer with throttled samples every 10s. The EWMA crosses 0.10 around
        // 63s, but the cold-start span is 120s.
        policy.observe(J, true, clock.now());
        for _ in 0..9 {
            let now = clock.advance(Duration::from_secs(10));
            policy.observe(J, true, now);
        }
        // ~90s elapsed: over threshold but still inside cold start.
        assert!(policy.throttle_ewma(J).unwrap() > THRESHOLD);
        assert!(
            policy.due_for_split(clock.now()).is_empty(),
            "cold start must suppress despite ewma over threshold",
        );

        // Cross the cold-start span; now it's due.
        for _ in 0..5 {
            let now = clock.advance(Duration::from_secs(10));
            policy.observe(J, true, now);
        }
        assert!(clock.elapsed() > MIN_OBSERVATION_SPAN);
        assert_eq!(policy.due_for_split(clock.now()), vec![J]);
    }

    /// After `mark_attempted`, the EWMA resets and cooldown suppresses
    /// re-triggering even under continued throttling, until cooldown expires.
    #[test]
    fn cooldown_resets_ewma_and_suppresses() {
        let mut policy = SplitPolicy::new();
        let mut clock = TestClock::new();

        // Drive it to a triggering state.
        policy.observe(J, true, clock.now());
        for _ in 0..30 {
            let now = clock.advance(Duration::from_secs(10));
            policy.observe(J, true, now);
        }
        assert!(policy.should_split(J, clock.now()));

        // Attempt the split: EWMA resets, cooldown begins.
        let mark = clock.now();
        policy.mark_attempted(J, mark);
        assert_eq!(policy.throttle_ewma(J), Some(0.0));
        assert!(!policy.should_split(J, mark));

        // Continued throttling within the cooldown window rebuilds the EWMA but
        // must not trigger.
        for _ in 0..30 {
            let now = clock.advance(Duration::from_secs(10));
            policy.observe(J, true, now);
        }
        assert!(policy.throttle_ewma(J).unwrap() > THRESHOLD);
        assert!(
            !policy.should_split(J, clock.now()),
            "cooldown must suppress re-trigger",
        );

        // Past cooldown, a still-hot journal is due again.
        let now = clock.advance(COOLDOWN);
        policy.observe(J, true, now);
        assert!(policy.should_split(J, now));
    }

    /// A journal that crosses the threshold but then stops being written ages
    /// out: once its most recent sample is older than `max_staleness` it's no
    /// longer due, even though its EWMA stays frozen above threshold (decay only
    /// happens on a sample). This is what stops a quiet — or deleted — journal
    /// from triggering a stale split.
    #[test]
    fn stale_journal_is_not_due() {
        let mut policy = SplitPolicy::new();
        let mut clock = TestClock::new();

        // Drive it well over threshold and past the cold-start span.
        policy.observe(J, true, clock.now());
        for _ in 0..30 {
            let now = clock.advance(Duration::from_secs(10));
            policy.observe(J, true, now);
        }
        assert!(policy.should_split(J, clock.now()));

        // No further samples: the journal stopped being written. Its EWMA is
        // still frozen above threshold, but past `max_staleness` it's not due.
        let later = clock.advance(MAX_STALENESS + Duration::from_secs(1));
        assert!(
            policy.throttle_ewma(J).unwrap() > THRESHOLD,
            "EWMA stays frozen without samples",
        );
        assert!(!policy.should_split(J, later));
        assert!(policy.due_for_split(later).is_empty());
    }

    /// A journal that stops being throttled decays back below threshold as quiet
    /// samples arrive over time.
    #[test]
    fn decay_pulls_quiet_journal_under() {
        let mut policy = SplitPolicy::new();
        let mut clock = TestClock::new();

        // Get it well over threshold.
        policy.observe(J, true, clock.now());
        for _ in 0..30 {
            let now = clock.advance(Duration::from_secs(10));
            policy.observe(J, true, now);
        }
        assert!(policy.should_split(J, clock.now()));

        // Now quiet for ~1000s (> TAU): decay should pull it back under.
        for _ in 0..100 {
            let now = clock.advance(Duration::from_secs(10));
            policy.observe(J, false, now);
        }
        assert!(policy.throttle_ewma(J).unwrap() < THRESHOLD);
        assert!(!policy.should_split(J, clock.now()));
    }

    /// `due_for_split` reports multiple hot journals in name order and omits
    /// quiet ones.
    #[test]
    fn due_for_split_reports_only_hot_journals() {
        let mut policy = SplitPolicy::new();
        let mut clock = TestClock::new();

        let hot_a = "acmeCo/c/pivot=00";
        let hot_b = "acmeCo/c/pivot=ff";
        let quiet = "acmeCo/c/pivot=80";

        policy.observe(hot_a, true, clock.now());
        policy.observe(hot_b, true, clock.now());
        policy.observe(quiet, false, clock.now());
        for i in 0..30 {
            let now = clock.advance(Duration::from_secs(10));
            policy.observe(hot_a, true, now);
            policy.observe(hot_b, true, now);
            policy.observe(quiet, i % 30 == 0, now); // negligibly throttled
        }

        assert_eq!(policy.due_for_split(clock.now()), vec![hot_a, hot_b]);
    }

    /// A journal on the `ignore` off-ramp is never fed: `observe` is a no-op
    /// for it, so it accumulates no state and can never become due — even under
    /// constant throttling.
    #[test]
    fn ignore_journal_is_never_observed() {
        let mut policy = SplitPolicy::new();
        let mut clock = TestClock::new();

        // Stream F would set this; seed it directly here to exercise the skip.
        policy.ignore.insert(J.to_string());

        policy.observe(J, true, clock.now());
        for _ in 0..30 {
            let now = clock.advance(Duration::from_secs(10));
            policy.observe(J, true, now);
        }

        // Skipped entirely: no entry was ever created, so it can't be due.
        assert!(policy.throttle_ewma(J).is_none());
        assert!(policy.due_for_split(clock.now()).is_empty());
        assert!(!policy.should_split(J, clock.now()));
    }

    /// `ignore` drops a journal's accumulated state and permanently suppresses
    /// it: even a journal that was already due stops being due, and continued
    /// throttling never re-accumulates pressure or re-triggers.
    #[test]
    fn ignore_drops_state_and_suppresses_future_observes() {
        let mut policy = SplitPolicy::new();
        let mut clock = TestClock::new();

        // Drive the journal to a triggering state.
        policy.observe(J, true, clock.now());
        for _ in 0..30 {
            let now = clock.advance(Duration::from_secs(10));
            policy.observe(J, true, now);
        }
        assert!(policy.should_split(J, clock.now()));

        // Ignoring it forgets its state, so it's immediately no longer due.
        policy.ignore(J);
        assert!(policy.throttle_ewma(J).is_none());
        assert!(!policy.should_split(J, clock.now()));

        // Continued throttling is now a no-op: no state re-accumulates and it
        // can never become due again.
        for _ in 0..30 {
            let now = clock.advance(Duration::from_secs(10));
            policy.observe(J, true, now);
        }
        assert!(policy.throttle_ewma(J).is_none());
        assert!(policy.due_for_split(clock.now()).is_empty());
        assert!(!policy.should_split(J, clock.now()));
    }

    /// `forget` drops a journal's state entirely.
    #[test]
    fn forget_drops_state() {
        let mut policy = SplitPolicy::new();
        let clock = TestClock::new();

        policy.observe(J, true, clock.now());
        assert!(policy.throttle_ewma(J).is_some());
        policy.forget(J);
        assert!(policy.throttle_ewma(J).is_none());
        assert!(policy.due_for_split(clock.now()).is_empty());
    }
}
