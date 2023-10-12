use crate::{ParseConfig, Output, format::ParseResult, ParseError};
use time::macros::format_description;
use serde_json::Value;

struct DatetimeSanitizer {
    from: Output,
    default_offset: time::UtcOffset,
}

// Here we are trying to parse non-ambiguous, non-RFC3339 dates and formatting them as RFC3339
// So we skip any valid RFC3339 in our processing and pass it as-is
fn datetime_to_rfc3339(val: &mut Value, default_offset: time::UtcOffset) {
    match val {
        Value::String(s) => {
            let offset_format = format_description!(
                version = 2,
                "[first
                [[year]-[month]-[day] [hour]:[minute]:[second][optional [.[subsecond]]]Z]
                [[year]-[month]-[day] [hour]:[minute]:[second][optional [.[subsecond]]]z]
                [[year]-[month]-[day] [hour]:[minute]:[second][optional [.[subsecond]]][offset_hour]:[offset_minute]]
                ]"
            );

            let primitive_format = format_description!(
                version = 2,
                "[year]-[month]-[day][optional [T]][optional [ ]][hour]:[minute]:[second][optional [.[subsecond]]]"
            );

            let parsed_with_tz = time::OffsetDateTime::parse(&s, offset_format);
            let parsed_no_tz = time::PrimitiveDateTime::parse(&s, primitive_format);

            if let Ok(parsed) = parsed_with_tz {
                *s = parsed.format(&time::format_description::well_known::Rfc3339).unwrap();
            } else if let Ok(parsed) = parsed_no_tz {
                *s = parsed.assume_offset(default_offset).format(&time::format_description::well_known::Rfc3339).unwrap();
            }
        }

        Value::Array(vec) => {
            vec.iter_mut().for_each(|item| {
                datetime_to_rfc3339(item, default_offset)
            })
        }

        Value::Object(map) => {
            map.iter_mut().for_each(|(_k, v)| {
                datetime_to_rfc3339(v, default_offset)
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
                datetime_to_rfc3339(&mut val, self.default_offset);
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
    #[error("could not parse offset: {0}")]
    OffsetParseError(#[from] time::error::Parse),
}

pub fn sanitize_datetime(config: &ParseConfig, output: Output) -> Result<Output, DatetimeSanitizeError> {
    eprintln!("sanitize_datetime");
    let offset = time::UtcOffset::parse(&config.default_offset, format_description!("[offset_hour]:[offset_minute]")).map_err(DatetimeSanitizeError::OffsetParseError)?;
    eprintln!("offset: {:?}", offset);
    let sanitizer = DatetimeSanitizer {
        from: output,
        default_offset: offset,
    };

    return Ok(Box::new(sanitizer))
}
