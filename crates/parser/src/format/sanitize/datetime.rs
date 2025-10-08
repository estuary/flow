use crate::{Output, ParseConfig, format::ParseResult};
use serde_json::Value;
use time::macros::format_description;

struct DatetimeSanitizer {
    from: Output,
    default_offset: time::UtcOffset,
}

// Here we are trying to parse non-RFC3339 dates
fn datetime_to_rfc3339(val: &mut Value, default_offset: time::UtcOffset) {
    match val {
        Value::String(s) => {
            // We first try to parse a more relaxed format that allows all the different formats we
            // support. At this stage we are trying to see if the value we see is a timestamp that
            // we can parse at all. If we are successful at parsing this value, then we try to
            // parse a more specific format for timestamps *with timezone*. If we are successful,
            // we use the parsed timezone, otherwise we use the default offset provided.
            let primitive_format = format_description!(
                version = 2,
                "[year]-[month]-[day][optional [T]][optional [ ]][hour]:[minute]:[second][optional [.[subsecond]]][optional [Z]][optional [z]][optional [[offset_hour]:[offset_minute]]]"
            );

            let parsed_no_tz = time::PrimitiveDateTime::parse(&s, primitive_format).ok();

            let parsed_with_tz = if parsed_no_tz.is_some() {
                let offset_format = format_description!(
                    version = 2,
                    "[first
                    [[year]-[month]-[day][optional [T]][optional [ ]][hour]:[minute]:[second][optional [.[subsecond]]]Z]
                    [[year]-[month]-[day][optional [T]][optional [ ]][hour]:[minute]:[second][optional [.[subsecond]]]z]
                    [[year]-[month]-[day][optional [T]][optional [ ]][hour]:[minute]:[second][optional [.[subsecond]]][offset_hour]:[offset_minute]]
                    ]"
                );

                time::OffsetDateTime::parse(&s, offset_format).ok()
            } else {
                None
            };

            if let Some(parsed) = parsed_with_tz {
                *s = parsed
                    .format(&time::format_description::well_known::Rfc3339)
                    .unwrap();
            } else if let Some(parsed) = parsed_no_tz {
                *s = parsed
                    .assume_offset(default_offset)
                    .format(&time::format_description::well_known::Rfc3339)
                    .unwrap();
            }
        }

        Value::Array(vec) => vec
            .iter_mut()
            .for_each(|item| datetime_to_rfc3339(item, default_offset)),

        Value::Object(map) => map
            .iter_mut()
            .for_each(|(_k, v)| datetime_to_rfc3339(v, default_offset)),

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
            e => e,
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DatetimeSanitizeError {
    #[error("could not parse offset: {0}")]
    OffsetParseError(#[from] time::error::Parse),
}

pub fn sanitize_datetime(
    config: &ParseConfig,
    output: Output,
) -> Result<Output, DatetimeSanitizeError> {
    let offset = time::UtcOffset::parse(
        &config.default_offset,
        format_description!("[offset_hour]:[offset_minute]"),
    )
    .map_err(DatetimeSanitizeError::OffsetParseError)?;
    let sanitizer = DatetimeSanitizer {
        from: output,
        default_offset: offset,
    };

    return Ok(Box::new(sanitizer));
}
