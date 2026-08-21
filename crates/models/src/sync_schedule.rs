use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// # Sync Schedule
/// Controls how often a materialization commits transactions, trading data
/// freshness for reduced destination compute cost. When caught up, the runtime
/// holds an open transaction (continuing to combine new documents) until the
/// next scheduled instant, rather than committing continuously.
///
/// The base interval applies 24/7, and any number of non-overlapping recurring
/// local-time windows may each be given their own interval, which applies
/// while that window is active.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SyncSchedule {
    /// # Base Interval
    /// Interval between transaction commits when the materialization is caught
    /// up, at all times not covered by a window. A value of zero commits as
    /// fast as possible.
    #[schemars(schema_with = "super::duration_schema")]
    #[serde(with = "humantime_serde")]
    pub base_interval: Duration,
    /// # Timezone
    /// IANA timezone name (e.g. `America/New_York`) or `+HH:MM` offset in
    /// which window times and days are interpreted. Required when windows are
    /// configured.
    #[schemars(with = "String")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timezone: Option<String>,
    /// # Windows
    /// Recurring local-time windows, each with its own commit interval which
    /// applies instead of the base interval while the window is active.
    /// Windows must not overlap.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub windows: Vec<Window>,
}

/// # Window
/// A recurring local-time window and the commit interval effective within it.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Window {
    /// # Interval
    /// Interval between transaction commits while the window is active.
    /// A value of zero commits as fast as possible.
    #[schemars(schema_with = "super::duration_schema")]
    #[serde(with = "humantime_serde")]
    pub interval: Duration,
    /// # Start
    /// Local time of day (`HH:MM`) at which the window opens.
    #[schemars(pattern(CLOCK_PATTERN))]
    pub start: String,
    /// # End
    /// Local time of day (`HH:MM`) at which the window closes. The window may
    /// straddle midnight (`end` earlier than `start`).
    #[schemars(pattern(CLOCK_PATTERN))]
    pub end: String,
    /// # Days
    /// Days of the week on which the window applies (e.g. `[Mon, Fri]`).
    /// All days if not set.
    #[schemars(with = "Vec<chrono::Weekday>")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub days: Option<Vec<chrono::Weekday>>,
}

impl SyncSchedule {
    /// Validate the schedule beyond its structural shape: the timezone is
    /// present (iff windows are) and well-formed, window clock times parse
    /// with distinct start/end, days are non-empty and non-repeating, and no
    /// two windows overlap on the weekly local-time grid.
    pub fn validate(&self) -> Result<(), String> {
        if self.windows.is_empty() {
            if self.timezone.is_some() {
                return Err(
                    "'timezone' applies only to windows; remove it or configure a window"
                        .to_string(),
                );
            }
            return Ok(());
        }

        let Some(timezone) = self.timezone.as_deref() else {
            return Err("must provide 'timezone' when windows are configured".to_string());
        };
        if timezone.parse::<chrono_tz::Tz>().is_err() && parse_offset(timezone).is_none() {
            return Err(format!(
                "invalid timezone {timezone:?} (expected an IANA name like America/New_York, or a +HH:MM offset)"
            ));
        }

        // Weekly wall-clock segments of every window, as (day, start minute,
        // end minute, window index), for the pairwise overlap check.
        let mut segments: Vec<(usize, u32, u32, usize)> = Vec::new();

        for (index, window) in self.windows.iter().enumerate() {
            let Window {
                interval: _,
                start,
                end,
                days,
            } = window;

            let start_hm = parse_clock(start).ok_or_else(|| {
                format!("invalid start {start:?} of window {index} (expected HH:MM)")
            })?;
            let end_hm = parse_clock(end)
                .ok_or_else(|| format!("invalid end {end:?} of window {index} (expected HH:MM)"))?;
            // An equal-times window is empty; compare parsed times, as
            // "9:00" == "09:00".
            if start_hm == end_hm {
                return Err(format!(
                    "start and end of window {index} must be different: got {start:?} and {end:?}"
                ));
            }

            let mut day_mask = [true; 7];
            if let Some(days) = days {
                if days.is_empty() {
                    return Err(format!(
                        "days of window {index} must not be empty (omit it to enable all days)"
                    ));
                }
                day_mask = [false; 7];
                for day in days {
                    let ind = day.num_days_from_sunday() as usize;
                    if day_mask[ind] {
                        return Err(format!(
                            "days of window {index} must not repeat: got {day} twice"
                        ));
                    }
                    day_mask[ind] = true;
                }
            }

            // Expand into per-day segments, splitting a midnight-straddling
            // window into its same-day [00:00, end) and [start, 24:00) pieces.
            let (start_min, end_min) = (start_hm.0 * 60 + start_hm.1, end_hm.0 * 60 + end_hm.1);
            for (day, _) in day_mask.iter().enumerate().filter(|(_, set)| **set) {
                if start_min < end_min {
                    segments.push((day, start_min, end_min, index));
                } else {
                    if end_min > 0 {
                        segments.push((day, 0, end_min, index));
                    }
                    segments.push((day, start_min, 24 * 60, index));
                }
            }
        }

        // Half-open segments sorted by (day, start): each need only be checked
        // against its successor. Adjacency (end == next start) is allowed.
        segments.sort();
        for pair in segments.windows(2) {
            let ((day, _, a_end, a_ind), (b_day, b_start, _, b_ind)) = (pair[0], pair[1]);
            if day == b_day && b_start < a_end && a_ind != b_ind {
                const DAYS: [&str; 7] = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
                return Err(format!(
                    "windows {a_ind} and {b_ind} overlap on {} at {:02}:{:02}",
                    DAYS[day],
                    b_start / 60,
                    b_start % 60,
                ));
            }
        }

        Ok(())
    }
}

/// Parse a `+HH:MM` / `-HH:MM` fixed-offset timezone. Shared with the runtime
/// evaluator so offset parsing can't drift between validation and enforcement.
pub fn parse_offset(s: &str) -> Option<chrono::FixedOffset> {
    let (sign, rest) = match s.strip_prefix('+') {
        Some(rest) => (1, rest),
        None => (-1, s.strip_prefix('-')?),
    };
    let (h, m) = rest.split_once(':')?;
    let (h, m): (i32, i32) = (h.parse().ok()?, m.parse().ok()?);
    if !(0..=23).contains(&h) || !(0..=59).contains(&m) {
        return None;
    }
    chrono::FixedOffset::east_opt(sign * (h * 3600 + m * 60))
}

/// JSON-schema pattern for `HH:MM` clock times, matching [`parse_clock`]'s
/// grammar exactly: a one- or two-digit hour 0-23 and a two-digit minute.
pub const CLOCK_PATTERN: &str = "^([01]?[0-9]|2[0-3]):[0-5][0-9]$";

/// Parse an `HH:MM` time of day into (hour, minute), per [`CLOCK_PATTERN`].
/// Shared with the runtime evaluator so clock parsing can't drift between
/// validation and enforcement.
pub fn parse_clock(s: &str) -> Option<(u32, u32)> {
    let (h, m) = s.split_once(':')?;
    // Match CLOCK_PATTERN exactly: digits only (u32 parsing alone would also
    // accept a leading `+`), a one- or two-digit hour, a two-digit minute.
    if !(1..=2).contains(&h.len())
        || m.len() != 2
        || !h.bytes().all(|b| b.is_ascii_digit())
        || !m.bytes().all(|b| b.is_ascii_digit())
    {
        return None;
    }
    let (h, m): (u32, u32) = (h.parse().ok()?, m.parse().ok()?);
    (h <= 23 && m <= 59).then_some((h, m))
}

#[cfg(test)]
mod test {
    use super::*;
    use chrono::Weekday::{Fri, Mon, Sat, Thu, Tue, Wed};

    fn every(secs: u64) -> SyncSchedule {
        SyncSchedule {
            base_interval: Duration::from_secs(secs),
            ..Default::default()
        }
    }

    fn window(start: &str, end: &str, days: Option<Vec<chrono::Weekday>>) -> Window {
        Window {
            interval: Duration::from_secs(15 * 60),
            start: start.to_string(),
            end: end.to_string(),
            days,
        }
    }

    // A 4-hour base interval with a 15-minute window 09:00-17:00, fixed-offset zone.
    fn windowed() -> SyncSchedule {
        SyncSchedule {
            base_interval: Duration::from_secs(4 * 60 * 60),
            timezone: Some("+00:00".to_string()),
            windows: vec![window("09:00", "17:00", None)],
        }
    }

    fn with_window(patch: impl FnOnce(&mut Window)) -> SyncSchedule {
        let mut sched = windowed();
        patch(&mut sched.windows[0]);
        sched
    }

    #[test]
    fn validate_accepts_a_fixed_schedule() {
        assert!(every(15 * 60).validate().is_ok());
    }

    #[test]
    fn validate_accepts_a_full_window() {
        assert!(windowed().validate().is_ok());
    }

    #[test]
    fn base_interval_is_required_by_serde() {
        // Requiredness is structural: a schedule without baseInterval fails
        // to deserialize, rather than defaulting.
        assert!(serde_json::from_str::<SyncSchedule>("{}").is_err());
    }

    #[test]
    fn window_fields_are_required_by_serde() {
        // A window's interval, start, and end are structurally required
        // together; only days is optional.
        let missing_start = r#"{
            "baseInterval": "4h",
            "timezone": "+00:00",
            "windows": [{"interval": "15m", "end": "17:00"}]
        }"#;
        assert!(serde_json::from_str::<SyncSchedule>(missing_start).is_err());
    }

    #[test]
    fn days_parse_flexibly_and_serialize_canonically() {
        // chrono::Weekday accepts short or long names case-insensitively, and
        // serializes to the canonical three-letter form.
        let sched: SyncSchedule = serde_json::from_str(
            r#"{
                "baseInterval": "4h",
                "timezone": "+00:00",
                "windows": [{
                    "interval": "15m",
                    "start": "09:00",
                    "end": "17:00",
                    "days": ["mon", "Tuesday", "WED"]
                }]
            }"#,
        )
        .unwrap();
        assert!(sched.validate().is_ok());

        let json = serde_json::to_value(&sched).unwrap();
        assert_eq!(
            json.pointer("/windows/0/days").unwrap(),
            &serde_json::json!(["Mon", "Tue", "Wed"]),
        );
    }

    #[test]
    fn validate_requires_a_timezone_with_windows() {
        let sched = SyncSchedule {
            timezone: None,
            ..windowed()
        };
        assert!(sched.validate().is_err());
    }

    #[test]
    fn validate_rejects_a_timezone_without_windows() {
        // A timezone qualifies only windows: with none, it's a leftover that
        // likely signals confusion (e.g. windows deleted but timezone kept).
        let sched = SyncSchedule {
            timezone: Some("America/New_York".to_string()),
            ..every(15 * 60)
        };
        assert!(sched.validate().is_err());
    }

    #[test]
    fn validate_rejects_a_malformed_offset() {
        // +99:99 is out of range.
        let sched = SyncSchedule {
            timezone: Some("+99:99".to_string()),
            ..windowed()
        };
        assert!(sched.validate().is_err());
    }

    #[test]
    fn validate_rejects_an_invalid_iana_name() {
        // IANA timezone names are validated at build time, so a typo errors
        // as early as possible rather than failing the task at startup.
        let sched = SyncSchedule {
            timezone: Some("Mars/Olympus".to_string()),
            ..windowed()
        };
        assert!(sched.validate().is_err());

        let sched = SyncSchedule {
            timezone: Some("America/New_York".to_string()),
            ..windowed()
        };
        assert!(sched.validate().is_ok());
    }

    #[test]
    fn validate_rejects_equal_window_times() {
        let sched = with_window(|w| w.end = "09:00".to_string());
        assert!(sched.validate().is_err());
    }

    #[test]
    fn validate_rejects_equal_window_times_spelled_differently() {
        // "9:00" and "09:00" are the same time of day: comparing raw strings
        // would let this (empty) window through.
        let sched = with_window(|w| {
            w.start = "9:00".to_string();
            w.end = "09:00".to_string();
        });
        assert!(sched.validate().is_err());
    }

    #[test]
    fn clock_times_accept_a_single_digit_hour() {
        // "9:00" parses the same as "09:00" (a leading zero is optional),
        // per CLOCK_PATTERN.
        assert_eq!(parse_clock("9:00"), Some((9, 0)));
        assert_eq!(parse_clock("09:00"), Some((9, 0)));

        let sched = with_window(|w| w.start = "9:00".to_string());
        assert!(sched.validate().is_ok());
    }

    #[test]
    fn clock_times_reject_off_pattern_spellings() {
        // parse_clock matches CLOCK_PATTERN exactly, so the schema annotation
        // and validation can't disagree: minutes are two digits, hours at
        // most two, and only digits are accepted (bare u32 parsing would
        // take a leading `+`).
        for bad in ["9:5", "09:5", "009:00", "+9:00", "9:+05", "24:00", "09:60"] {
            assert_eq!(parse_clock(bad), None, "{bad:?} must not parse");
        }
    }

    #[test]
    fn validate_rejects_empty_days() {
        // An empty days list is a window that can never be active.
        let sched = with_window(|w| w.days = Some(Vec::new()));
        assert!(sched.validate().is_err());
    }

    #[test]
    fn validate_rejects_repeated_days() {
        let sched = with_window(|w| w.days = Some(vec![Mon, Mon]));
        assert!(sched.validate().is_err());
    }

    #[test]
    fn validate_accepts_disjoint_windows() {
        // Adjacent (end == start) and day-disjoint windows are not overlaps.
        let sched = SyncSchedule {
            windows: vec![
                window("09:00", "17:00", Some(vec![Mon, Tue, Wed, Thu, Fri])),
                window("17:00", "20:00", Some(vec![Mon, Tue, Wed, Thu, Fri])),
                window("09:00", "12:00", Some(vec![Sat])),
            ],
            ..windowed()
        };
        assert!(sched.validate().is_ok());
    }

    #[test]
    fn validate_rejects_overlapping_windows() {
        let sched = SyncSchedule {
            windows: vec![
                window("09:00", "17:00", None),
                window("16:00", "20:00", Some(vec![Wed])),
            ],
            ..windowed()
        };
        let err = sched.validate().unwrap_err();
        assert_eq!(err, "windows 0 and 1 overlap on Wed at 16:00");
    }

    #[test]
    fn validate_rejects_overlap_via_a_midnight_straddle() {
        // The straddling window's [00:00, 06:00) piece lands on every day,
        // colliding with an early-morning window.
        let sched = SyncSchedule {
            windows: vec![
                window("22:00", "06:00", None),
                window("05:00", "07:00", None),
            ],
            ..windowed()
        };
        assert!(sched.validate().is_err());
    }
}
