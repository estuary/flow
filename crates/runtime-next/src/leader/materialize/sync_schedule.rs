//! Evaluation of a materialization's [`models::SyncSchedule`]: the wall-clock
//! instant at which a held-open transaction is next permitted to commit.
//!
//! The model type and its validation live in `models::sync_schedule`; the
//! offset and clock parsers are shared from there so the grammar can't drift
//! between validation and enforcement.

use models::sync_schedule::{parse_clock, parse_offset};

/// A [`models::SyncSchedule`] compiled for evaluation: parsed timezone, clock
/// times, and day masks. Construction validates the model in full, so a
/// compiled schedule cannot fail to evaluate.
pub struct CompiledSchedule {
    /// The validated schedule model, retained for logging.
    pub model: models::SyncSchedule,
    /// Base interval, in seconds.
    base: i64,
    /// Parsed zone and windows, present together iff the model has windows.
    zoned: Option<(Zone, Vec<Window>)>,
}

impl CompiledSchedule {
    /// Compile `model`, re-running its full validation: builds already ran it,
    /// so a spec that somehow slipped through fails the task cleanly at
    /// startup rather than misbehaving at evaluation time.
    pub fn new(model: models::SyncSchedule) -> Result<Self, String> {
        model.validate()?;

        let base = model.base_interval.as_secs() as i64;
        let zoned = if model.windows.is_empty() {
            None
        } else {
            // Validation guarantees these parses succeed.
            let timezone = model.timezone.as_deref().unwrap_or_default();
            let zone =
                Zone::parse(timezone).ok_or_else(|| format!("invalid timezone {timezone:?}"))?;

            let windows = model
                .windows
                .iter()
                .map(|window| {
                    let (Some(start), Some(end)) = (
                        ClockTime::parse(&window.start),
                        ClockTime::parse(&window.end),
                    ) else {
                        return Err(format!(
                            "invalid window times {:?}..{:?}",
                            window.start, window.end
                        ));
                    };
                    Ok(Window {
                        interval: window.interval.as_secs() as i64,
                        start,
                        end,
                        days: active_days(window.days.as_deref()),
                    })
                })
                .collect::<Result<Vec<_>, String>>()?;

            Some((zone, windows))
        };

        Ok(Self { model, base, zoned })
    }

    /// The wall-clock instant at which a transaction opened at `after` is next
    /// permitted to commit, or `None` when no delay applies (the effective
    /// interval is zero for the regime containing `after`). `seed` selects the
    /// deterministic jitter phase.
    pub fn next_fire_after(
        &self,
        after: chrono::DateTime<chrono::Utc>,
        seed: u64,
    ) -> Option<chrono::DateTime<chrono::Utc>> {
        // Deterministic jitter: a single absolute offset in [0, 24h) shared
        // across regimes, spreading load off common boundaries. A zero seed
        // applies none; equal seeds coincide (shared-destination coalescing).
        let jitter = (seed % 86_400) as i64;

        // Without windows, the base interval is effective 24/7.
        let Some((zone, windows)) = &self.zoned else {
            return fire(after, self.base, jitter);
        };

        // The regime at `after`: the active window's interval, else the base.
        // Windows cannot overlap, but a DST fall-back fold can momentarily
        // satisfy two membership tests, so the first in configured order wins.
        let active = windows.iter().position(|w| w.active(zone, after));
        let this_period = active.map(|ind| windows[ind].interval).unwrap_or(self.base);

        // Within the current regime, use its cadence.
        let n = fire(after, this_period, jitter)?;

        // The fire may overleap regime transitions, but must not land past the
        // start of any FASTER regime -- that regime's cadence must begin on
        // time, and the hold itself prevents the commit that would otherwise
        // re-anchor (e.g. a short slower window between this regime and a
        // faster one would be hopped entirely, delaying the faster cadence by
        // up to a full period). So walk the transitions in (after, n] and
        // clamp to the first one where a faster regime takes over; crossing
        // into slower regimes uses the computed instant as-is.
        let (mut cursor, mut cursor_active) = (after, active);
        loop {
            // The nearest regime transition after `cursor`: out of its active
            // window, or into the earliest-starting one.
            let bound = match cursor_active {
                Some(ind) => next_end_of_window(zone, &windows[ind], cursor),
                None => windows
                    .iter()
                    .map(|w| next_start_of_window(zone, w, cursor))
                    .min()
                    .expect("windows is non-empty"),
            };
            if bound > n {
                return Some(n);
            }

            // The regime taking over at `bound` (a window may begin exactly
            // where another ends).
            let bound_active = windows.iter().position(|w| w.active(zone, bound));
            let bound_period = bound_active
                .map(|ind| windows[ind].interval)
                .unwrap_or(self.base);
            if bound_period < this_period {
                return Some(bound);
            }

            (cursor, cursor_active) = (bound, bound_active);
        }
    }
}

/// The next instant strictly after `after` on the epoch-relative grid
/// `{ k*period + jitter }`, or `None` when `period` is zero (no hold).
fn fire(
    after: chrono::DateTime<chrono::Utc>,
    period: i64,
    jitter: i64,
) -> Option<chrono::DateTime<chrono::Utc>> {
    if period == 0 {
        return None;
    }
    let after = after.timestamp();
    let elapsed = (after - jitter).div_euclid(period);
    chrono::DateTime::from_timestamp((elapsed + 1) * period + jitter, 0)
}

/// A window compiled for evaluation: its interval in seconds, parsed clock
/// times, and Sunday-indexed day mask.
struct Window {
    interval: i64,
    start: ClockTime,
    end: ClockTime,
    days: [bool; 7],
}

impl Window {
    /// Whether this window is active at `dt`.
    fn active(&self, zone: &Zone, dt: chrono::DateTime<chrono::Utc>) -> bool {
        let (_, weekday) = zone.local_parts(dt);
        self.days[weekday.num_days_from_sunday() as usize]
            && between(zone, self.start, self.end, dt)
    }
}

/// A timezone for the window/day checks: an IANA zone or a fixed `+HH:MM`
/// offset.
enum Zone {
    Iana(chrono_tz::Tz),
    Fixed(chrono::FixedOffset),
}

impl Zone {
    /// Parse an IANA name or `+HH:MM` offset, returning `None` if neither.
    fn parse(s: &str) -> Option<Zone> {
        if let Ok(tz) = s.parse::<chrono_tz::Tz>() {
            return Some(Zone::Iana(tz));
        }
        parse_offset(s).map(Zone::Fixed)
    }

    /// Local calendar date and weekday of `dt` as observed in this zone.
    fn local_parts(
        &self,
        dt: chrono::DateTime<chrono::Utc>,
    ) -> (chrono::NaiveDate, chrono::Weekday) {
        use chrono::Datelike;
        match self {
            Zone::Iana(tz) => {
                let l = dt.with_timezone(tz);
                (l.date_naive(), l.weekday())
            }
            Zone::Fixed(off) => {
                let l = dt.with_timezone(off);
                (l.date_naive(), l.weekday())
            }
        }
    }

    /// The UTC instant of `h:m` local time on `date` in this zone.
    fn local_hms(&self, date: chrono::NaiveDate, h: u32, m: u32) -> chrono::DateTime<chrono::Utc> {
        let naive = date.and_hms_opt(h, m, 0).unwrap();
        match self {
            Zone::Iana(tz) => resolve_local(tz, naive),
            Zone::Fixed(off) => resolve_local(off, naive),
        }
    }
}

/// Resolve a local wall-clock time to a UTC instant: the earliest instant whose
/// local clock reads at or after `naive`. An ambiguous local time (a fall-back
/// fold) takes the earlier of its two instants; a nonexistent one (a
/// spring-forward gap) takes the instant at which the gap ends.
///
/// The gap end is found by bisection rather than assuming the usual one hour:
/// `Antarctica/Troll` jumps two hours (a fixed one-hour step lands still inside
/// the gap and has nothing to resolve) and `Australia/Lord_Howe` jumps thirty
/// minutes (a one-hour step overshoots the gap's end). Overshoot is not merely
/// imprecise, it is non-monotone -- in `America/New_York` on a spring-forward
/// date, local 02:30 would step to 03:30 while local 03:00 stays 03:00 -- so a
/// 02:30-03:00 window would resolve its start after its end, which [`between`]
/// then reads as a midnight-straddling window active nearly all day.
///
/// Bisection is exact here because nonexistent local times form a single
/// contiguous run, making "resolves" monotone over the search range.
fn resolve_local<Tz: chrono::TimeZone>(
    tz: &Tz,
    naive: chrono::NaiveDateTime,
) -> chrono::DateTime<chrono::Utc> {
    let resolve = |shift: i64| {
        tz.from_local_datetime(&(naive + chrono::Duration::seconds(shift)))
            .earliest()
    };
    if let Some(dt) = resolve(0) {
        return dt.with_timezone(&chrono::Utc);
    }

    // Invariant: `lo` is within the gap and `hi` is past its end.
    let (mut lo, mut hi) = (0, 24 * 60 * 60);
    while hi - lo > 1 {
        let mid = lo + (hi - lo) / 2;
        if resolve(mid).is_some() {
            hi = mid;
        } else {
            lo = mid;
        }
    }
    resolve(hi)
        .expect("bisection presumes no DST gap approaches a full day")
        .with_timezone(&chrono::Utc)
}

/// A time of day (hour, minute) for a window, in the schedule's zone.
#[derive(Clone, Copy)]
struct ClockTime {
    h: u32,
    m: u32,
}

impl ClockTime {
    fn parse(s: &str) -> Option<ClockTime> {
        parse_clock(s).map(|(h, m)| ClockTime { h, m })
    }

    /// The UTC instant of this time of day on the local date of `dt`.
    fn on(self, zone: &Zone, dt: chrono::DateTime<chrono::Utc>) -> chrono::DateTime<chrono::Utc> {
        let (date, _) = zone.local_parts(dt);
        zone.local_hms(date, self.h, self.m)
    }

    /// The first instant strictly after `dt` having this time of day.
    fn next(self, zone: &Zone, dt: chrono::DateTime<chrono::Utc>) -> chrono::DateTime<chrono::Utc> {
        let t = self.on(zone, dt);
        if t > dt {
            return t;
        }
        let (date, _) = zone.local_parts(dt);
        zone.local_hms(date.succ_opt().unwrap(), self.h, self.m)
    }
}

/// Whether `dt` falls within `[start, end)` on its local day, handling a window
/// that straddles midnight.
fn between(
    zone: &Zone,
    start: ClockTime,
    end: ClockTime,
    dt: chrono::DateTime<chrono::Utc>,
) -> bool {
    let clock_start = start.on(zone, dt);
    let clock_end = end.on(zone, dt);
    if clock_start > clock_end {
        dt >= clock_start || dt < clock_end
    } else {
        dt >= clock_start && dt < clock_end
    }
}

/// The instant at/after `dt` at which the (currently active) `window` turns
/// off: the next occurrence of its end time, or an earlier midnight -- in the
/// configured zone -- where its day mask disables it.
fn next_end_of_window(
    zone: &Zone,
    window: &Window,
    dt: chrono::DateTime<chrono::Utc>,
) -> chrono::DateTime<chrono::Utc> {
    let end_next = window.end.next(zone, dt);

    let (date, _) = zone.local_parts(dt);
    let midnight = zone.local_hms(
        date.succ_opt()
            .expect("scheduling dates are far from the calendar's representable bounds"),
        0,
        0,
    );

    if !window.active(zone, midnight) && midnight < end_next {
        midnight
    } else {
        end_next
    }
}

/// The next instant at or after `dt` at which `window` becomes active.
/// The window can only turn on at a local midnight (an enabled day opening
/// under a window that straddles midnight) or at its start time, so scan
/// those boundary instants over the next fifteen local calendar days.
///
/// Fifteen rather than the eight it takes to reach the same weekday next
/// week, because a window whose start and end both fall inside one DST gap
/// resolves to a zero-length (never-active) window on that date alone. If
/// that date is the only enabled weekday, its occurrence is lost and the scan
/// must reach the following week's. A zone transitions at most a few times a
/// year, so two chances at each weekday is always enough.
///
/// Days are enumerated on the zone's local calendar, NOT by fixed 24-hour
/// steps: a spring-forward local day is 23 hours long, so 24-hour stepping
/// can hop over an enabled day entirely (exhausting the scan). Midnight
/// boundaries likewise belong to the configured zone, not UTC.
fn next_start_of_window(
    zone: &Zone,
    window: &Window,
    dt: chrono::DateTime<chrono::Utc>,
) -> chrono::DateTime<chrono::Utc> {
    let (first_date, _) = zone.local_parts(dt);

    for i in 0..15 {
        let date = first_date
            .checked_add_days(chrono::Days::new(i))
            .expect("scheduling dates are far from the calendar's representable bounds");

        // Candidates in ascending order: local midnight, then window start.
        let candidates = [
            zone.local_hms(date, 0, 0),
            zone.local_hms(date, window.start.h, window.start.m),
        ];
        for candidate in candidates {
            if candidate >= dt && window.active(zone, candidate) {
                return candidate;
            }
        }
    }
    unreachable!("an enabled weekday recurs twice within fifteen days")
}

/// The Sunday-indexed mask of days on which a window applies. An absent
/// `days` enables all days; a set one was validated non-empty at build time.
fn active_days(days: Option<&[chrono::Weekday]>) -> [bool; 7] {
    let Some(days) = days else {
        return [true; 7];
    };
    let mut out = [false; 7];
    for day in days {
        out[day.num_days_from_sunday() as usize] = true;
    }
    out
}

#[cfg(test)]
mod test {
    use super::*;
    use chrono::{
        DateTime, Utc,
        Weekday::{Fri, Mon, Sat, Sun, Thu, Tue, Wed},
    };
    use models::SyncSchedule;
    use std::time::Duration;

    fn at(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    /// Compile-and-evaluate shim: tests express schedules as models and
    /// evaluate through the compiled form, exactly as the Task does.
    fn next_fire_after(
        sched: &SyncSchedule,
        after: DateTime<Utc>,
        seed: u64,
    ) -> Option<DateTime<Utc>> {
        CompiledSchedule::new(sched.clone())
            .expect("test schedules compile")
            .next_fire_after(after, seed)
    }

    fn every(secs: u64) -> SyncSchedule {
        SyncSchedule {
            base_interval: Duration::from_secs(secs),
            ..Default::default()
        }
    }

    fn window(
        interval_secs: u64,
        start: &str,
        end: &str,
        days: Option<Vec<chrono::Weekday>>,
    ) -> models::sync_schedule::Window {
        models::sync_schedule::Window {
            interval: Duration::from_secs(interval_secs),
            start: start.to_string(),
            end: end.to_string(),
            days,
        }
    }

    // A 4-hour base interval with a 15-minute window 09:00-17:00, in a
    // fixed-offset zone equal to UTC so instants read directly. All days.
    fn windowed() -> SyncSchedule {
        SyncSchedule {
            base_interval: Duration::from_secs(4 * 60 * 60),
            timezone: Some("+00:00".to_string()),
            windows: vec![window(15 * 60, "09:00", "17:00", None)],
        }
    }

    fn with_window(patch: impl FnOnce(&mut models::sync_schedule::Window)) -> SyncSchedule {
        let mut sched = windowed();
        patch(&mut sched.windows[0]);
        sched
    }

    #[test]
    fn zero_interval_never_holds() {
        // A base interval of zero means "commit as fast as possible": there is
        // no scheduled hold, so there is no next fire instant.
        let sched = every(0);
        assert_eq!(next_fire_after(&sched, at("2026-01-01T00:13:00Z"), 0), None);
    }

    #[test]
    fn fires_align_to_wall_clock_grid_not_the_reference_instant() {
        // 15-minute cadence, 24/7. Two reference instants within the same
        // 15-minute slot yield the SAME next fire: alignment is to the
        // wall-clock grid, matching the doc's "restart at 00:13 -> fire at
        // 00:15, not 00:28". Seed 0 applies no jitter.
        let sched = every(15 * 60);
        let want = at("2026-01-01T00:15:00Z");
        assert_eq!(
            next_fire_after(&sched, at("2026-01-01T00:13:00Z"), 0),
            Some(want)
        );
        assert_eq!(
            next_fire_after(&sched, at("2026-01-01T00:14:30Z"), 0),
            Some(want)
        );
    }

    #[test]
    fn grid_is_aligned_to_the_unix_epoch_not_the_hour() {
        // Fires are at multiples of the period from the Unix epoch, with no
        // hour/day restart. A 45m period
        // yields ..., 00:00, 00:45, 01:30, ..., so a reference at :50 gives
        // 01:30 (not 01:00).
        let sched = every(45 * 60);
        assert_eq!(
            next_fire_after(&sched, at("2026-01-01T00:50:00Z"), 0),
            Some(at("2026-01-01T01:30:00Z")),
        );
    }

    #[test]
    fn jitter_shifts_phase_deterministically_by_seed() {
        // 15m divides the hour, so the zero-jitter grid point after 00:07 is
        // 00:15. A non-zero seed shifts every instant by a fixed phase, but
        // preserves the cadence and is reproducible for the same seed.
        let sched = every(15 * 60);
        let t = at("2026-01-01T00:07:00Z");

        let a = next_fire_after(&sched, t, 42).unwrap();
        assert_eq!(a, next_fire_after(&sched, t, 42).unwrap(), "reproducible");
        assert_ne!(a, at("2026-01-01T00:15:00Z"), "seed shifts off the grid");

        let b = next_fire_after(&sched, a, 42).unwrap();
        assert_eq!(
            b - a,
            chrono::Duration::minutes(15),
            "cadence preserved across the phase shift",
        );
    }

    #[test]
    fn windowed_uses_window_cadence_inside_and_base_outside() {
        let sched = windowed();
        // 12:00 is inside the window: 15-minute cadence (fires stay in-window).
        let f1 = next_fire_after(&sched, at("2026-01-01T12:00:00Z"), 0).unwrap();
        let f2 = next_fire_after(&sched, f1, 0).unwrap();
        assert_eq!(f2 - f1, chrono::Duration::minutes(15), "window cadence");
        // 03:00 is outside: the 4-hour base cadence (fires stay pre-window).
        let g1 = next_fire_after(&sched, at("2026-01-01T03:00:00Z"), 0).unwrap();
        let g2 = next_fire_after(&sched, g1, 0).unwrap();
        assert_eq!(g2 - g1, chrono::Duration::hours(4), "base cadence outside");
    }

    #[test]
    fn base_regime_clamps_to_the_window_open_boundary() {
        // Just before the window opens, the base 4h cadence would overshoot to
        // 12:00; instead the next fire is clamped to the 09:00 window start,
        // since we're crossing into the faster regime.
        let sched = windowed();
        assert_eq!(
            next_fire_after(&sched, at("2026-01-01T08:50:00Z"), 0),
            Some(at("2026-01-01T09:00:00Z")),
        );
    }

    #[test]
    fn window_regime_does_not_clamp_to_the_window_close_boundary() {
        // In the window, approaching the close (entering the SLOWER 4h base
        // regime), the next fire is the normal 15m-grid instant, not clamped to
        // the window end. Window closes 17:10; at 17:05 the next 15m grid point
        // is 17:15, which must be returned as-is.
        let sched = with_window(|w| w.end = "17:10".to_string());
        assert_eq!(
            next_fire_after(&sched, at("2026-01-01T17:05:00Z"), 0),
            Some(at("2026-01-01T17:15:00Z")),
        );
    }

    #[test]
    fn slower_window_clamps_to_the_window_close_boundary() {
        // A window may be SLOWER than the base (e.g. quiet hours). Inside a
        // 4h window with a 15m base, a fire crossing the window close is
        // clamped to it, so the faster base cadence starts on time.
        let sched = SyncSchedule {
            base_interval: Duration::from_secs(15 * 60),
            ..with_window(|w| w.interval = Duration::from_secs(4 * 60 * 60))
        };
        // At 16:50, inside the window, the 4h grid overshoots to 20:00; the
        // window closes at 17:00 and the base regime is faster, so clamp.
        assert_eq!(
            next_fire_after(&sched, at("2026-01-01T16:50:00Z"), 0),
            Some(at("2026-01-01T17:00:00Z")),
        );
    }

    #[test]
    fn disabled_day_uses_base_cadence_all_day() {
        // Window active only Monday-Friday. On Saturday, even at a window time
        // of day, the base 4h cadence applies.
        let sched = with_window(|w| w.days = Some(vec![Mon, Tue, Wed, Thu, Fri]));
        // 2026-01-03 is a Saturday (2026-01-01 is a Thursday).
        let g1 = next_fire_after(&sched, at("2026-01-03T12:00:00Z"), 0).unwrap();
        let g2 = next_fire_after(&sched, g1, 0).unwrap();
        assert_eq!(g2 - g1, chrono::Duration::hours(4));
    }

    #[test]
    fn boundary_clamp_skips_disabled_days() {
        // Window Monday-Friday. On Saturday the base cadence must NOT clamp
        // to a Saturday window start (Saturday is disabled) -- the next window
        // is Monday -- so the Saturday base fire is used as-is.
        let sched = with_window(|w| w.days = Some(vec![Mon, Tue, Wed, Thu, Fri]));
        // 2026-01-03 is a Saturday; 08:00 -> base 4h grid gives 12:00.
        assert_eq!(
            next_fire_after(&sched, at("2026-01-03T08:00:00Z"), 0),
            Some(at("2026-01-03T12:00:00Z")),
        );
    }

    #[test]
    fn adjacent_windows_transition_at_their_shared_boundary() {
        // Two adjacent windows: 15m business hours, then a SLOWER 2h evening
        // window. From the base regime the fire clamps into the first window;
        // inside the first window, a fire crossing 17:00 into the slower
        // evening window is NOT clamped; inside the evening window, a fire
        // crossing 20:00 back into the (faster, 4h vs 2h -- no, base is
        // slower) base is not clamped either.
        let sched = SyncSchedule {
            windows: vec![
                window(15 * 60, "09:00", "17:00", None),
                window(2 * 60 * 60, "17:00", "20:00", None),
            ],
            ..windowed()
        };
        // Base regime at 08:50 clamps to the 09:00 open of the faster window.
        assert_eq!(
            next_fire_after(&sched, at("2026-01-01T08:50:00Z"), 0),
            Some(at("2026-01-01T09:00:00Z")),
        );
        // Inside the 15m window at 16:55: the next grid instant 17:00 lands
        // exactly on the boundary into the SLOWER evening window; no clamp
        // logic applies (17:00 is the grid fire itself).
        assert_eq!(
            next_fire_after(&sched, at("2026-01-01T16:55:00Z"), 0),
            Some(at("2026-01-01T17:00:00Z")),
        );
        // Inside the evening window at 17:05: the 2h grid gives 18:00, still
        // in-window; used as-is.
        assert_eq!(
            next_fire_after(&sched, at("2026-01-01T17:05:00Z"), 0),
            Some(at("2026-01-01T18:00:00Z")),
        );
        // Inside the evening window at 19:05: the 2h grid gives 20:00, the
        // boundary back into the 4h base -- slower, so no clamp; 20:00 is the
        // grid fire itself.
        assert_eq!(
            next_fire_after(&sched, at("2026-01-01T19:05:00Z"), 0),
            Some(at("2026-01-01T20:00:00Z")),
        );
    }

    #[test]
    fn the_nearest_upcoming_window_bounds_the_base_fire() {
        // Two disjoint windows on the same day; from the base regime before
        // both, the clamp target is the EARLIEST window start, not the first
        // in configured order.
        let sched = SyncSchedule {
            windows: vec![
                window(15 * 60, "14:00", "17:00", None),
                window(15 * 60, "06:00", "08:00", None),
            ],
            ..windowed()
        };
        // 05:00 base fire on the 4h grid would be 08:00; the 06:00 window
        // start comes first and is faster, so clamp there.
        assert_eq!(
            next_fire_after(&sched, at("2026-01-01T05:00:00Z"), 0),
            Some(at("2026-01-01T06:00:00Z")),
        );
    }

    #[test]
    fn straddling_window_exits_at_disabled_day_midnight() {
        // A 22:00-06:00 window enabled only on Mondays actually turns off at
        // Tuesday 00:00 local (Tuesday is disabled), not at 06:00. A SLOWER
        // window (8h) with a faster base (15m) must clamp its exit to that
        // midnight -- the configured zone's midnight, not UTC's.
        let sched = SyncSchedule {
            base_interval: Duration::from_secs(15 * 60),
            windows: vec![window(8 * 60 * 60, "22:00", "06:00", Some(vec![Mon]))],
            ..windowed()
        };
        // 2026-01-05 is a Monday. At 23:00 the window is active; its 8h grid
        // fire would be 00:00+ hours away, but the window exits at Tuesday
        // 00:00 into the faster base, so clamp there.
        assert_eq!(
            next_fire_after(&sched, at("2026-01-05T23:00:00Z"), 0),
            Some(at("2026-01-06T00:00:00Z")),
        );
    }

    #[test]
    fn window_follows_local_time_across_dst() {
        // The 09:00 window start is a local time, so its UTC instant shifts by
        // an hour between EST (UTC-5) and EDT (UTC-4). Approaching the open in
        // the base regime clamps to that (DST-adjusted) local 09:00.
        let mut sched = windowed();
        sched.timezone = Some("America/New_York".to_string());
        // Winter (EST): 08:50 EST = 13:50 UTC -> clamp to 09:00 EST = 14:00 UTC.
        assert_eq!(
            next_fire_after(&sched, at("2026-01-15T13:50:00Z"), 0),
            Some(at("2026-01-15T14:00:00Z")),
        );
        // Summer (EDT): 08:50 EDT = 12:50 UTC -> clamp to 09:00 EDT = 13:00 UTC.
        assert_eq!(
            next_fire_after(&sched, at("2026-07-15T12:50:00Z"), 0),
            Some(at("2026-07-15T13:00:00Z")),
        );
    }

    #[test]
    fn dst_spring_forward_gap_resolves_to_the_gap_end() {
        // 2026-03-08: US spring-forward skips 02:00-03:00 local. A window
        // starting at 02:30 (nonexistent that day) opens when the gap ends, at
        // 03:00 EDT (07:00 UTC) -- NOT one hour past the requested time, which
        // would open the window half an hour late. At 01:00 EST (06:00 UTC),
        // the base regime clamps to that window start.
        let mut sched = with_window(|w| w.start = "02:30".to_string());
        sched.timezone = Some("America/New_York".to_string());
        assert_eq!(
            next_fire_after(&sched, at("2026-03-08T06:00:00Z"), 0),
            Some(at("2026-03-08T07:00:00Z")),
        );
    }

    #[test]
    fn dst_gaps_that_are_not_one_hour_resolve_to_their_end() {
        // A gap longer than an hour: Antarctica/Troll jumps UTC+0 to UTC+2 on
        // the last Sunday of March, skipping local 01:00-03:00. A 01:30 start
        // opens at 03:00 local (01:00 UTC); stepping a fixed hour would land
        // at 02:30, still inside the gap, with nothing to resolve.
        let mut sched = with_window(|w| w.start = "01:30".to_string());
        sched.timezone = Some("Antarctica/Troll".to_string());
        assert_eq!(
            next_fire_after(&sched, at("2026-03-29T00:30:00Z"), 0),
            Some(at("2026-03-29T01:00:00Z")),
        );

        // A gap shorter than an hour: Australia/Lord_Howe jumps thirty minutes
        // on the first Sunday of October, skipping local 02:00-02:30. A 02:00
        // start opens at 02:30 local (15:30 UTC the prior day), not 03:00.
        let mut sched = with_window(|w| w.start = "02:00".to_string());
        sched.timezone = Some("Australia/Lord_Howe".to_string());
        assert_eq!(
            next_fire_after(&sched, at("2026-10-03T15:00:00Z"), 0),
            Some(at("2026-10-03T15:30:00Z")),
        );
    }

    #[test]
    fn a_window_wholly_inside_a_dst_gap_is_inactive_rather_than_inverted() {
        // 02:30-03:00 in America/New_York on 2026-03-08. Resolving each bound
        // independently by a fixed hour is non-monotone: the start would move
        // to 03:30 while the end stays 03:00, and `between` reads start > end
        // as a window straddling midnight -- leaving it "active" nearly all
        // day, at the window's cadence. Resolving to the gap end instead
        // collapses both bounds onto 03:00, so the window is simply inactive
        // on the one date its wall-clock hour does not exist.
        let zone = Zone::parse("America/New_York").unwrap();
        let (start, end) = (
            ClockTime::parse("02:30").unwrap(),
            ClockTime::parse("03:00").unwrap(),
        );
        // 09:00 UTC is 04:00 EDT, well outside the window.
        assert!(!between(&zone, start, end, at("2026-03-08T09:00:00Z")));

        // The base cadence therefore governs the whole day: from 09:00 UTC the
        // 4h grid fires at 12:00 UTC, with no clamp to a window boundary.
        let mut sched = with_window(|w| {
            w.start = "02:30".to_string();
            w.end = "03:00".to_string();
        });
        sched.timezone = Some("America/New_York".to_string());
        assert_eq!(
            next_fire_after(&sched, at("2026-03-08T09:00:00Z"), 0),
            Some(at("2026-03-08T12:00:00Z")),
        );
    }

    #[test]
    fn a_collapsed_window_defers_to_its_next_enabled_day() {
        // The 02:30-03:00 window above, enabled only on Sundays, evaluated
        // from the Saturday before: 2026-03-08's occurrence is lost to the
        // gap, so the next start is 2026-03-15. An eight-day scan spans only
        // 03-07 through 03-14 and would find no enabled day at all.
        let mut sched = with_window(|w| {
            w.start = "02:30".to_string();
            w.end = "03:00".to_string();
            w.days = Some(vec![Sun]);
        });
        sched.timezone = Some("America/New_York".to_string());
        // Saturday 07:00 EST (12:00 UTC): the base 4h grid fires at 16:00 UTC,
        // far short of the 2026-03-15 07:30 UTC boundary, so no clamp applies.
        assert_eq!(
            next_fire_after(&sched, at("2026-03-07T12:00:00Z"), 0),
            Some(at("2026-03-07T16:00:00Z")),
        );
    }

    #[test]
    fn local_time_of_day_resolves_monotonically() {
        // The invariant `between` and the window-boundary scans rely on: over
        // a local day, later wall-clock times never resolve to earlier UTC
        // instants. Only DST gaps can violate it, so sweep every date of 2026
        // in the pathological zones at the finest granularity a window can
        // express (one minute).
        for name in [
            "America/New_York",
            "Europe/London",
            "Pacific/Auckland",
            "America/Santiago",
            "Australia/Lord_Howe",
            "Antarctica/Troll",
        ] {
            let zone = Zone::parse(name).unwrap();
            let mut date = chrono::NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
            let stop = chrono::NaiveDate::from_ymd_opt(2027, 1, 1).unwrap();

            while date < stop {
                let mut prev = zone.local_hms(date, 0, 0);
                for minute in 1..24 * 60 {
                    let next = zone.local_hms(date, minute / 60, minute % 60);
                    assert!(
                        next >= prev,
                        "{name} {date} minute {minute}: {next} precedes {prev}"
                    );
                    prev = next;
                }
                date = date.succ_opt().unwrap();
            }
        }
    }

    #[test]
    fn dst_short_day_is_not_skipped_when_scanning_for_the_window() {
        // Regression: enumerating days by fixed 24-hour steps skips the
        // 23-hour spring-forward day. Window only on Sundays; 2026-03-08
        // (a Sunday) is the US spring-forward. From Saturday 23:30 EST the
        // next window start is Sunday 09:00 EDT, but 24h stepping lands past
        // Sunday's local midnight into Monday, never finds an enabled day,
        // and panics.
        let mut sched = with_window(|w| w.days = Some(vec![Sun]));
        sched.timezone = Some("America/New_York".to_string());
        // Saturday 23:30 EST = 04:30 UTC. The base-regime 4h grid fires next
        // at 08:00 UTC, before the Sunday 09:00 EDT (13:00 UTC) window start,
        // so no clamp applies.
        assert_eq!(
            next_fire_after(&sched, at("2026-03-08T04:30:00Z"), 0),
            Some(at("2026-03-08T08:00:00Z")),
        );
    }

    #[test]
    fn straddling_window_activates_at_local_midnight_not_utc() {
        // A 22:00-06:00 window straddles midnight, enabled only on Mondays,
        // in a +05:30 zone. From Sunday evening the regime next begins at
        // Monday 00:00 LOCAL (Sunday 18:30 UTC) -- not at UTC midnight.
        let mut sched = with_window(|w| {
            w.start = "22:00".to_string();
            w.end = "06:00".to_string();
            w.days = Some(vec![Mon]);
        });
        sched.timezone = Some("+05:30".to_string());
        // 2026-01-04 is a Sunday. At 17:00 UTC (22:30 local, inactive since
        // Sunday is disabled) the base 4h grid gives 20:00 UTC, past the
        // Monday-00:00-local boundary at 18:30 UTC, so the fire clamps to
        // that boundary.
        assert_eq!(
            next_fire_after(&sched, at("2026-01-04T17:00:00Z"), 0),
            Some(at("2026-01-04T18:30:00Z")),
        );
    }

    #[test]
    fn sweep_of_pathological_zones_never_panics_and_always_progresses() {
        // Brute-force guard for calendar edge cases: zones with northern and
        // southern DST, midnight-adjacent transitions (Santiago), a 30-minute
        // shift (Lord Howe), and a two-hour one (Troll), crossed with
        // gap-prone and midnight-straddling windows, single enabled days, and
        // a disjoint two-window shape, at anchors stepping across all of 2026.
        // Every evaluation must return an instant strictly after the anchor
        // (and must not panic).
        let zones = [
            "America/New_York",
            "Europe/London",
            "Pacific/Auckland",
            "America/Santiago",
            "Australia/Lord_Howe",
            "Antarctica/Troll",
        ];
        // 02:30 is Lord Howe's gap END, so it alone leaves the sub-hour gap
        // untested; 02:15 opens inside every zone's gap, and 02:15-02:45 sits
        // wholly within one.
        let single_windows = [
            ("09:00", "17:00"),
            ("02:30", "17:00"),
            ("02:15", "17:00"),
            ("02:15", "02:45"),
            ("22:00", "06:00"),
        ];
        let day_specs: [&[chrono::Weekday]; 8] = [
            &[Sun],
            &[Mon],
            &[Tue],
            &[Wed],
            &[Thu],
            &[Fri],
            &[Sat],
            &[Mon, Tue, Wed, Thu, Fri],
        ];

        // Single-window shapes crossed with day masks, plus a fixed disjoint
        // two-window shape (weekday business hours + a weekend straddle).
        let mut shapes: Vec<Vec<models::sync_schedule::Window>> = Vec::new();
        for (start, end) in single_windows {
            for days in day_specs {
                shapes.push(vec![window(15 * 60, start, end, Some(days.to_vec()))]);
            }
        }
        shapes.push(vec![
            window(
                15 * 60,
                "09:00",
                "17:00",
                Some(vec![Mon, Tue, Wed, Thu, Fri]),
            ),
            window(8 * 60 * 60, "22:00", "06:00", Some(vec![Sat])),
        ]);

        for zone in zones {
            for shape in &shapes {
                let sched = SyncSchedule {
                    timezone: Some(zone.to_string()),
                    windows: shape.clone(),
                    ..windowed()
                };
                assert!(sched.validate().is_ok(), "shape must validate: {sched:?}");

                // An odd step (7h13m) walks anchors through varied local
                // times of day over the year.
                let mut anchor = at("2026-01-01T00:00:00Z");
                let stop = at("2027-01-01T00:00:00Z");
                while anchor < stop {
                    let fire = next_fire_after(&sched, anchor, 12_345)
                        .expect("windowed schedules always have a next fire");
                    assert!(
                        fire > anchor,
                        "fire {fire} is not after anchor {anchor} ({zone} {shape:?})"
                    );
                    anchor += chrono::Duration::minutes(7 * 60 + 13);
                }
            }
        }
    }

    #[test]
    fn single_digit_clock_times_evaluate_like_zero_padded_ones() {
        // "9:00" and "09:00" yield identical fire instants (the evaluator's
        // ClockTime parser is not width-sensitive, matching models validation).
        let padded = windowed();
        let unpadded = with_window(|w| w.start = "9:00".to_string());
        // Approaching the window open in the base regime clamps to its start,
        // making the result sensitive to how the start time parsed.
        let t = at("2026-01-01T08:50:00Z");
        assert_eq!(
            next_fire_after(&padded, t, 0),
            next_fire_after(&unpadded, t, 0),
        );
        assert_eq!(
            next_fire_after(&unpadded, t, 0),
            Some(at("2026-01-01T09:00:00Z")),
        );
    }

    #[test]
    fn fire_does_not_hop_a_short_slower_window_into_a_faster_regime() {
        // Overnight batching, a short maintenance blackout, then a fresh 15m
        // daytime base. Anchored at 04:00 inside the 4h overnight window, the
        // 4h grid fires at 08:00 -- hopping the blackout entirely and starting
        // the daytime cadence 90 minutes late, every morning. The walk over
        // regime transitions must clamp to 06:30, where the faster base takes
        // over.
        let sched = SyncSchedule {
            base_interval: Duration::from_secs(15 * 60),
            windows: vec![
                window(4 * 60 * 60, "22:00", "06:00", None),
                window(12 * 60 * 60, "06:00", "06:30", None),
            ],
            ..windowed()
        };
        assert_eq!(
            next_fire_after(&sched, at("2026-01-01T04:00:00Z"), 0),
            Some(at("2026-01-01T06:30:00Z")),
        );

        // The walk clamps to the FIRST faster transition, not a later one:
        // from 05:00 the same applies (4h grid fire at 08:00), still 06:30.
        assert_eq!(
            next_fire_after(&sched, at("2026-01-01T05:00:00Z"), 0),
            Some(at("2026-01-01T06:30:00Z")),
        );
    }

    #[test]
    fn walk_returns_the_grid_fire_when_no_faster_regime_intervenes() {
        // Crossing into slower regimes only: a 15m base fire at 06:00 lands
        // exactly on the blackout boundary; the walk must not clamp or skip.
        let sched = SyncSchedule {
            base_interval: Duration::from_secs(15 * 60),
            windows: vec![window(12 * 60 * 60, "06:00", "06:30", None)],
            ..windowed()
        };
        assert_eq!(
            next_fire_after(&sched, at("2026-01-01T05:50:00Z"), 0),
            Some(at("2026-01-01T06:00:00Z")),
        );
    }

    #[test]
    fn malformed_schedule_fails_to_compile() {
        // A malformed window time is rejected by build-time validation;
        // compilation re-runs that validation, so a spec that slipped through
        // fails the task at startup rather than misbehaving at evaluation.
        let sched = with_window(|w| w.end = "25:99".to_string());
        assert!(CompiledSchedule::new(sched).is_err());
    }

    #[test]
    fn zone_parse_rejects_an_invalid_name() {
        assert!(Zone::parse("Mars/Olympus").is_none());
        assert!(Zone::parse("America/New_York").is_some());
        assert!(Zone::parse("+05:30").is_some());
    }
}
