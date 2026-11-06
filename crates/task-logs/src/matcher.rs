use chrono::{DateTime, Utc};
use serde_json::value::RawValue;
use std::borrow::Cow;
use std::ops::ControlFlow;

/// Level of a log line, ordered from most to least severe.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Level {
    Error = 1,
    Warn = 2,
    Info = 3,
    Debug = 4,
    Trace = 5,
}

impl Level {
    pub fn as_str(&self) -> &'static str {
        match self {
            Level::Error => "error",
            Level::Warn => "warn",
            Level::Info => "info",
            Level::Debug => "debug",
            Level::Trace => "trace",
        }
    }

    fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "error" => Level::Error,
            "warn" => Level::Warn,
            "info" => Level::Info,
            "debug" => Level::Debug,
            "trace" => Level::Trace,
            _ => return None,
        })
    }

    fn bit(self) -> u8 {
        1 << (self as u8)
    }
}

/// Filter selects lines of a task's logs. All present conditions must match.
#[derive(Debug, Clone, Default)]
pub struct Filter {
    /// Levels to match. Empty matches every level.
    pub levels: Vec<Level>,
    /// Inclusive lower bound of the line's `ts`.
    pub since: Option<DateTime<Utc>>,
    /// Exclusive upper bound of the line's `ts`.
    pub until: Option<DateTime<Utc>>,
    /// Case-insensitive substring of the line's `message`, or of its `fields`.
    pub text: Option<String>,
}

/// A log line which matched a Filter.
#[derive(Debug, Clone, serde::Serialize)]
pub struct Line {
    /// Journal offset at which the line begins. Unique within the journal.
    pub offset: i64,
    /// Journal offset just past the line's trailing newline.
    pub end: i64,
    pub ts: DateTime<Utc>,
    pub level: Level,
    pub message: String,
    pub fields: Option<Box<RawValue>>,
    pub uuid: String,
}

/// Matcher is a compiled Filter which visits matching lines of
/// newline-aligned journal content.
///
/// Most scanned lines don't match a selective filter, so Matcher first runs a
/// prefilter over the raw bytes of an entire batch, and parses only the lines
/// that it hits. The prefilter can produce false positives but never false
/// negatives, because each hit is confirmed by parsing its line.
pub struct Matcher {
    levels: u8, // Bitmask of `Level::bit()`, or zero to match every level.
    since: Option<DateTime<Utc>>,
    until: Option<DateTime<Utc>>,
    text: Option<Text>,
    prefilter: Option<regex::bytes::Regex>,
}

struct Text {
    // Needle, lower-cased for a case-insensitive comparison.
    lower: String,
    // Can the needle be found within the raw JSON bytes of a line?
    raw: bool,
}

impl Matcher {
    pub fn new(filter: &Filter) -> Self {
        let Filter {
            levels,
            since,
            until,
            text,
        } = filter;

        let levels = levels.iter().fold(0u8, |acc, l| acc | l.bit());
        let all_levels = Level::Error.bit()
            | Level::Warn.bit()
            | Level::Info.bit()
            | Level::Debug.bit()
            | Level::Trace.bit();
        let levels = if levels == all_levels { 0 } else { levels };

        let text = text.as_ref().filter(|t| !t.is_empty()).map(|t| Text {
            lower: t.to_lowercase(),
            raw: is_raw_searchable(t),
        });

        // Prefer the text prefilter: users search for rare strings, while a
        // level such as `info` can match most lines of a journal.
        let prefilter = match &text {
            Some(Text {
                lower, raw: true, ..
            }) => Some(
                regex::bytes::RegexBuilder::new(&regex::escape(lower))
                    .case_insensitive(true)
                    .unicode(false)
                    .build()
                    .expect("escaped literal is a valid regex"),
            ),
            _ if levels != 0 => {
                let alternates: Vec<&str> = [
                    Level::Error,
                    Level::Warn,
                    Level::Info,
                    Level::Debug,
                    Level::Trace,
                ]
                .into_iter()
                .filter(|l| levels & l.bit() != 0)
                .map(|l| l.as_str())
                .collect();

                Some(
                    regex::bytes::Regex::new(&format!(r#""level":"(?:{})""#, alternates.join("|")))
                        .expect("level alternation is a valid regex"),
                )
            }
            _ => None,
        };

        Self {
            levels,
            since: *since,
            until: *until,
            text,
            prefilter,
        }
    }

    /// Visit each matching line of newline-aligned `content`,
    /// which begins at journal `offset`.
    pub fn for_each_match(
        &self,
        content: &[u8],
        offset: i64,
        mut visit: impl FnMut(Line) -> ControlFlow<()>,
    ) -> ControlFlow<()> {
        let Some(prefilter) = &self.prefilter else {
            let mut begin = 0;
            for end in memchr::memchr_iter(b'\n', content) {
                let line = &content[begin..end + 1];
                if let Some(line) = self.confirm(line, offset + begin as i64) {
                    visit(line)?;
                }
                begin = end + 1;
            }
            return ControlFlow::Continue(());
        };

        let mut pos = 0;
        while let Some(hit) = prefilter.find_at(content, pos) {
            let begin =
                memchr::memrchr(b'\n', &content[pos..hit.start()]).map_or(pos, |i| pos + i + 1);
            let end = memchr::memchr(b'\n', &content[hit.end()..])
                .map_or(content.len(), |i| hit.end() + i + 1);

            if let Some(line) = self.confirm(&content[begin..end], offset + begin as i64) {
                visit(line)?;
            }
            pos = end;
        }
        ControlFlow::Continue(())
    }

    /// Parse a candidate line and check it against every condition.
    /// Lines that fail to parse are not matched.
    fn confirm(&self, line: &[u8], offset: i64) -> Option<Line> {
        // A time-bounded scan reads many lines outside of its bounds.
        // Check the timestamp before paying for a full parse.
        if self.since.is_some() || self.until.is_some() {
            let ts = line_ts(line)?;
            if !self.in_bounds(ts) {
                return None;
            }
        }

        let doc: Doc = serde_json::from_slice(line).ok()?;
        let level = Level::parse(doc.level.as_deref()?)?;

        if self.levels != 0 && self.levels & level.bit() == 0 {
            return None;
        }
        let ts = parse_ts(doc.ts.as_deref()?)?;
        if !self.in_bounds(ts) {
            return None;
        }
        let message = doc.message.unwrap_or_default();

        if let Some(text) = &self.text
            && !text.matches(&message, doc.fields)
        {
            return None;
        }

        Some(Line {
            offset,
            end: offset + line.len() as i64,
            ts,
            level,
            message: message.into_owned(),
            fields: doc.fields.map(ToOwned::to_owned),
            uuid: doc.meta.map(|m| m.uuid.into_owned()).unwrap_or_default(),
        })
    }

    /// The `since` and `until` of the Filter.
    pub(crate) fn bounds(&self) -> (Option<DateTime<Utc>>, Option<DateTime<Utc>>) {
        (self.since, self.until)
    }

    fn in_bounds(&self, ts: DateTime<Utc>) -> bool {
        !matches!(self.since, Some(since) if ts < since)
            && !matches!(self.until, Some(until) if ts >= until)
    }
}

impl Text {
    fn matches(&self, message: &str, fields: Option<&RawValue>) -> bool {
        if message.to_lowercase().contains(&self.lower) {
            return true;
        }
        let Some(fields) = fields else {
            return false;
        };
        // A raw-searchable needle is unaffected by JSON escaping,
        // so we can search the encoded `fields` directly.
        if self.raw {
            return fields.get().to_ascii_lowercase().contains(&self.lower);
        }
        let Ok(fields) = serde_json::from_str::<serde_json::Value>(fields.get()) else {
            return false;
        };
        value_contains(&fields, &self.lower)
    }
}

fn value_contains(value: &serde_json::Value, lower: &str) -> bool {
    match value {
        serde_json::Value::String(s) => s.to_lowercase().contains(lower),
        serde_json::Value::Number(n) => n.to_string().contains(lower),
        serde_json::Value::Array(items) => items.iter().any(|v| value_contains(v, lower)),
        serde_json::Value::Object(map) => map
            .iter()
            .any(|(k, v)| k.to_lowercase().contains(lower) || value_contains(v, lower)),
        serde_json::Value::Bool(_) | serde_json::Value::Null => false,
    }
}

/// Can `needle` be found by a case-insensitive search of raw JSON bytes?
/// Go's encoding/json escapes `<`, `>`, and `&`, any encoder escapes `"`, `\`
/// and control characters, and ASCII case folding doesn't extend to other
/// scripts. A needle with any of these must be matched after decoding.
fn is_raw_searchable(needle: &str) -> bool {
    needle
        .bytes()
        .all(|b| (0x20..0x7f).contains(&b) && !matches!(b, b'"' | b'\\' | b'<' | b'>' | b'&'))
}

#[derive(serde::Deserialize)]
struct Doc<'a> {
    #[serde(rename = "_meta", default, borrow)]
    meta: Option<Meta<'a>>,
    #[serde(default, borrow)]
    ts: Option<Cow<'a, str>>,
    #[serde(default, borrow)]
    level: Option<Cow<'a, str>>,
    #[serde(default, borrow)]
    message: Option<Cow<'a, str>>,
    #[serde(default, borrow)]
    fields: Option<&'a RawValue>,
}

#[derive(serde::Deserialize)]
struct Meta<'a> {
    #[serde(borrow)]
    uuid: Cow<'a, str>,
}

/// Extract the top-level `ts` of a log line without parsing all of it.
/// Ops logs are serialized with `_meta` and `shard` ahead of `ts`,
/// and neither has a `ts` property.
pub fn line_ts(line: &[u8]) -> Option<DateTime<Utc>> {
    const NEEDLE: &[u8] = br#""ts":""#;
    let begin = memchr::memmem::find(line, NEEDLE)? + NEEDLE.len();
    let end = begin + memchr::memchr(b'"', &line[begin..])?;
    parse_ts(std::str::from_utf8(&line[begin..end]).ok()?)
}

fn parse_ts(s: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s).ok().map(|ts| ts.to_utc())
}

#[cfg(test)]
mod test {
    use super::*;

    const LINES: &str = concat!(
        r#"{"_meta":{"uuid":"d2c10c34-d6a5-11f1-8000-112233440301"},"shard":{"kind":"capture","name":"acmeCo/source-orders"},"ts":"2026-11-02T10:00:00Z","level":"info","message":"captured documents","fields":{"count":12}}"#,
        "\n",
        r#"{"_meta":{"uuid":"d2c10c35-d6a5-11f1-8000-112233440301"},"shard":{"kind":"capture","name":"acmeCo/source-orders"},"ts":"2026-11-02T10:00:01Z","level":"warn","message":"retrying request","fields":{"status":503,"error":"503 Service Unavailable"}}"#,
        "\n",
        r#"{"_meta":{"uuid":"d2c10c36-d6a5-11f1-8000-112233440301"},"shard":{"kind":"capture","name":"acmeCo/source-orders"},"ts":"2026-11-02T10:00:02Z","level":"error","message":"shard failed","fields":{"error":"container exited (OOMKilled) <=> café"}}"#,
        "\n",
        r#"{"_meta":{"uuid":"d2c10c37-d6a5-11f1-8000-112233440301"},"shard":{"kind":"capture","name":"acmeCo/source-orders"},"ts":"2026-11-02T10:00:03Z","level":"info","message":"Café opened","fields":{}}"#,
        "\n",
        "not json\n",
    );

    fn matches(filter: Filter) -> Vec<(i64, &'static str, String)> {
        let mut out = Vec::new();
        _ = Matcher::new(&filter).for_each_match(LINES.as_bytes(), 1000, |line| {
            out.push((line.offset, line.level.as_str(), line.message));
            ControlFlow::Continue(())
        });
        out
    }

    #[test]
    fn test_filters() {
        let at = |s| Some(parse_ts(s).unwrap());

        assert_eq!(matches(Filter::default()).len(), 4);

        assert_eq!(
            matches(Filter {
                levels: vec![Level::Error, Level::Warn],
                ..Default::default()
            }),
            vec![
                (1211, "warn", "retrying request".to_string()),
                (1456, "error", "shard failed".to_string()),
            ]
        );
        // Raw-searchable text is matched case-insensitively against fields.
        assert_eq!(
            matches(Filter {
                text: Some("SERVICE unavail".to_string()),
                ..Default::default()
            }),
            vec![(1211, "warn", "retrying request".to_string())]
        );
        // Shard fields are prefilter hits, but don't match.
        assert!(
            matches(Filter {
                text: Some("source-orders".to_string()),
                ..Default::default()
            })
            .is_empty()
        );
        // Text that JSON may escape is matched after decoding.
        assert_eq!(
            matches(Filter {
                text: Some("<=> CAFÉ".to_string()),
                ..Default::default()
            }),
            vec![(1456, "error", "shard failed".to_string())]
        );
        assert_eq!(
            matches(Filter {
                text: Some("café".to_string()),
                ..Default::default()
            })
            .len(),
            2
        );
        assert_eq!(
            matches(Filter {
                since: at("2026-11-02T10:00:01Z"),
                until: at("2026-11-02T10:00:03Z"),
                ..Default::default()
            })
            .len(),
            2
        );
    }
}
