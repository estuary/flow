use crate::{ParseConfig, Output, format::ParseResult, ParseError};
use chrono::{DateTime, FixedOffset, SecondsFormat};
use chrono_tz::Tz;
use serde_json::Value;

struct DatetimeSanitizer {
    from: Output,
    default_timezone: Tz,
}

const NAIVE_FORMATS: [&'static str; 4] = [
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%S%.f",
    "%Y-%m-%d %H:%M:%S%.f",
    "%Y-%m-%d %H:%M:%S",
];

const FORMATS: [&'static str; 2] = [
    "%Y-%m-%d %H:%M:%S%.f%:z",
    "%Y-%m-%d %H:%M:%S%:z",
];

fn datetime_to_rfc3339(val: &mut Value, default_timezone: Tz) {
    match val {
        Value::String(s) => {
            let mut parsed: Option<DateTime<FixedOffset>> = None;

            for f in FORMATS {
                parsed = parsed.or_else(||
                    chrono::DateTime::parse_from_str(&s, f).ok()
                )
            }

            if let Some(ts) = parsed {
                *s = ts.to_rfc3339_opts(SecondsFormat::AutoSi, true);
                return
            }

            let mut naive_parsed: Option<DateTime<Tz>> = None;

            for f in NAIVE_FORMATS {
                naive_parsed = naive_parsed.or_else(||
                    chrono::NaiveDateTime::parse_from_str(&s, f).map(|d| d.and_local_timezone(default_timezone).unwrap()).ok()
                )
            }

            if let Some(ts) = naive_parsed {
                *s = ts.to_rfc3339_opts(SecondsFormat::AutoSi, true);
            }
        }

        Value::Array(vec) => {
            vec.iter_mut().for_each(|item| {
                datetime_to_rfc3339(item, default_timezone)
            })
        }

        Value::Object(map) => {
            map.iter_mut().for_each(|(_k, v)| {
                datetime_to_rfc3339(v, default_timezone)
            })
        }

        _ => {}
    }
}

impl Iterator for DatetimeSanitizer {
    type Item = ParseResult;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.from.next()?;
        Some(match next {
            Ok(mut val) => {
                datetime_to_rfc3339(&mut val, self.default_timezone);
                Ok(val)
            }
            Err(e) => {
                Err(ParseError::Parse(Box::new(e)))
            }
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DatetimeSanitizeError {
    #[error("could not parse timezone as a valid IANA timezone")]
    TimezoneParseError(String),
}

pub fn sanitize_datetime(config: &ParseConfig, output: Output) -> Result<Output, DatetimeSanitizeError> {
    let tz: Tz = config.default_timezone.parse().map_err(DatetimeSanitizeError::TimezoneParseError)?;
    let sanitizer = DatetimeSanitizer {
        from: output,
        default_timezone: tz,
    };

    return Ok(Box::new(sanitizer))
}
