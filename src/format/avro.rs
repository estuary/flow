use crate::config::ParseConfig;
use crate::decorate::display_ptr;
use crate::format::projection::{build_projections, Projection};
use crate::format::{Output, ParseError, ParseResult, Parser};
use crate::input::Input;
use avro_rs::{schema::SchemaKind, types::Value as AvroValue, Reader, Schema};
use chrono::{NaiveDateTime, NaiveTime};
use json::schema::types;
use serde_json::Value;
use std::collections::BTreeMap;
use std::io;

pub struct AvroParser;

/// Returns a type-erased parser trait object for parsing avro object container files.
pub fn new_parser() -> Box<dyn Parser> {
    Box::new(AvroParser)
}

#[derive(Debug, thiserror::Error)]
pub enum AvroError {
    #[error(
        "unsupported top-level schema type: {0:?}, only records are supported as top-level types"
    )]
    NonRecordSchema(SchemaKind),
    #[error(transparent)]
    Read(avro_rs::Error),
    #[error("invalid floating point value '{0}' for column '{1}'")]
    InvalidFloat(String, String),

    #[error("unable to convert avro to json because the projected location '{0}' (column '{1}') conflicts with the document structure: {2}")]
    ImpossibleDocument(String, String, Value),

    /// Date-like values in avro are essentially just type hints on top of numeric primitives, so
    /// it's distinctly possible for them to be out of range. This error is returned in that case.
    #[error("the column '{2}' value {0} is out of range for an avro {1}")]
    DateTimeOverflow(i64, &'static str, String),
}

impl Parser for AvroParser {
    fn parse(&self, config: &ParseConfig, content: Input) -> Result<Output, ParseError> {
        let iter = AvroIter::from_config_and_input(config, content)?;
        Ok(Box::new(iter))
    }
}

struct AvroIter {
    reader: Reader<'static, Box<dyn io::BufRead>>,
    projections: BTreeMap<String, Projection>,
}

impl AvroIter {
    fn from_config_and_input(config: &ParseConfig, content: Input) -> Result<AvroIter, ParseError> {
        let projections = build_projections(config)?;

        let reader = Reader::new(content.into_buffered_stream(64 * 1024))
            .map_err(|err| ParseError::Parse(Box::new(err)))?;

        tracing::debug!(avro_writer_schema = ?reader.writer_schema(), "parsed avro header");
        match reader.writer_schema() {
            Schema::Record { fields, .. } => {
                let mut resolved = BTreeMap::new();
                for field in fields {
                    let projection = projections.lookup(&field.name);
                    resolved.insert(field.name.clone(), projection);
                }
                tracing::debug!(projections = ?resolved, "resolved projections for avro schema");

                Ok(AvroIter {
                    reader,
                    projections: resolved,
                })
            }
            other => Err(ParseError::Parse(Box::new(AvroError::NonRecordSchema(
                other.into(),
            )))),
        }
    }

    fn record_to_json(&self, record: Vec<(String, AvroValue)>) -> Result<Value, AvroError> {
        let mut json = Value::Object(serde_json::Map::with_capacity(record.len()));

        for (avro_key, avro_value) in record {
            let projection = self
                .projections
                .get(&avro_key)
                .expect("missing projection for avro field");
            let allow_string_repr = projection
                .possible_types
                .map(|t| t.overlaps(types::STRING))
                .unwrap_or(true);
            let json_value = avro_to_json(&avro_key, avro_value, allow_string_repr)?;

            if let Some(loc) = projection.target_location.create(&mut json) {
                *loc = json_value;
            } else {
                return Err(AvroError::ImpossibleDocument(
                    display_ptr(&projection.target_location),
                    avro_key,
                    json.clone(),
                ));
            }
        }
        Ok(json)
    }
}

impl Iterator for AvroIter {
    type Item = ParseResult;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.reader.next()?;
        let result = next
            .map_err(|e| AvroError::Read(e))
            .and_then(|avro_value| match avro_value {
                AvroValue::Record(fields) => self.record_to_json(fields),
                other => Err(AvroError::NonRecordSchema(SchemaKind::from(&other))),
            })
            .map_err(|err| ParseError::Parse(Box::new(err)));
        Some(result)
    }
}

/// Converts an avro Value to a json Value. This function behaves quite differently from the
/// conversion that's done by the `TryFrom<AvroValue> for serde_json::Value` impl. There's a fair
/// number of "logical types" in Avro, which are primitive types with special semantics. These
/// logical types may be converted to JSON in different ways. You could simply convert the
/// primitive type directly to its nearest JSON analogue, but the results will typically be a very
/// "non-standard" way of representing the values in JSON. An example is timestamps, which you'd
/// more commonly represent as RFC3339 strings in JSON. The string representation is the default,
/// but we also handle the case where the JSON schema doesn't permit strings in a particular
/// location by converting the timestamp to a json number.
fn avro_to_json(
    column_name: &str,
    avro_value: avro_rs::types::Value,
    allow_string_repr: bool,
) -> Result<Value, AvroError> {
    use avro_rs::types::Value::*;

    match avro_value {
        Null => Ok(Value::Null),
        Boolean(b) => Ok(Value::Bool(b)),
        Int(i) => Ok(Value::Number(i.into())),
        Long(l) => Ok(Value::Number(l.into())),
        // Floating point numbers require special handling for NAN and +/-inf because those values
        // can _only_ be represented as strings in json. If the json types happen to allow strings,
        // then we'll coerce them into strings, but only for the values that aren't representable
        // as numbers. This provides a path for users to handle NAN by setting `type: [number, string]`
        // in their schema, while still being slightly less magical than automatically coercing
        // everything into strings. This approach also yields better error messages in the common
        // case where the JSON schema _only_ allows numbers and a NAN value makes it into their
        // avro file. In that case, then we can fail now with a really helpful error message,
        // instead of leaving it to a schema validation error later.
        Float(f) => match serde_json::Number::from_f64(f as f64) {
            Some(num) => Ok(Value::Number(num)),
            None if allow_string_repr => Ok(Value::String(f.to_string())),
            None => Err(AvroError::InvalidFloat(
                f.to_string(),
                column_name.to_string(),
            )),
        },
        Double(d) => match serde_json::Number::from_f64(d as f64) {
            Some(num) => Ok(Value::Number(num)),
            None if allow_string_repr => Ok(Value::String(d.to_string())),
            None => Err(AvroError::InvalidFloat(
                d.to_string(),
                column_name.to_string(),
            )),
        },
        Bytes(b) | Fixed(_, b) if allow_string_repr => Ok(Value::String(base64::encode(&b))),
        Bytes(b) | Fixed(_, b) => Ok(Value::Array(b.into_iter().map(Value::from).collect())),
        String(s) => Ok(Value::String(s)),
        Enum(_, s) => Ok(Value::String(s)),
        // union is just a wrapper around a boxed avro Value, so we just unwrap it here and
        // propagate `allow_string_repr`.
        Union(boxed) => avro_to_json(column_name, *boxed, allow_string_repr),

        // Within nested structures, we always allow types to be represented as strings, as that's
        // the more sane default. If we ever wanted to get fancy, we could try to resolve the full
        // pointer to each nested type and lookup the inferred type information, but I'm
        // considering that to not be worth the effort at this stage.
        Array(items) => items
            .into_iter()
            .map(|i| avro_to_json(column_name, i, true))
            .collect::<Result<Vec<_>, _>>()
            .map(Value::Array),
        Map(items) => items
            .into_iter()
            .map(|(k, v)| avro_to_json(column_name, v, true).map(|jv| (k, jv)))
            .collect::<Result<serde_json::Map<_, _>, _>>()
            .map(Value::Object),
        Record(items) => items
            .into_iter()
            .map(|(k, v)| avro_to_json(column_name, v, true).map(|jv| (k, jv)))
            .collect::<Result<serde_json::Map<_, _>, _>>()
            .map(Value::Object),

        Date(d) if allow_string_repr => {
            // Avro date is the number of days since the unix epoch, so we need to do a little
            // converting. The explicit bounds checks are here so that we can return a meaningful
            // error instead of panicing.
            const SECS_PER_DAY: i64 = 86_400;
            let ts = (d as i64)
                .checked_mul(SECS_PER_DAY)
                .and_then(|secs| NaiveDateTime::from_timestamp_opt(secs, 0))
                .ok_or_else(|| {
                    AvroError::DateTimeOverflow(d as i64, "date", column_name.to_string())
                })?;
            Ok(Value::String(ts.date().to_string()))
        }
        TimeMillis(t) if allow_string_repr => {
            time_from_midnight("time-millis", column_name, t as i64, MILLIS_PER_SEC)
        }
        TimeMicros(t) if allow_string_repr => {
            time_from_midnight("time-micros", column_name, t, MICROS_PER_SEC)
        }
        TimestampMillis(t) if allow_string_repr => {
            timestamp_from_unix_epoch("timestamp-millis", column_name, t, MILLIS_PER_SEC)
        }
        TimestampMicros(t) if allow_string_repr => {
            timestamp_from_unix_epoch("timestamp-micros", column_name, t, MICROS_PER_SEC)
        }
        // If !allow_string_repr, then all the date values will be converted directly to json
        // numbers. This allows users to handle any conversions themselves, by disallowing string
        // types in their json schema.
        Date(i) | TimeMillis(i) => Ok(Value::Number(i.into())),
        TimeMicros(i) | TimestampMicros(i) | TimestampMillis(i) => Ok(Value::Number(i.into())),

        Duration(avro_dur) => {
            // avro durations are really weird. We always convert them to json objects, since
            // there's no obvious alternative representation because they encode a number of
            // _months_. I seriously doubt that anyone uses this goofy crap anyway.
            Ok(serde_json::json!({
                "months": u32::from(avro_dur.months()),
                "days": u32::from(avro_dur.days()),
                "millis": u32::from(avro_dur.millis()),
            }))
        }
        Uuid(uuid) => Ok(Value::String(uuid.to_string())),
        // The decimal type in avro_rs does't allow access to the underlying value (which seems
        // like a bug). So we just use the built-in converstion, which results in a json array of
        // the underlying byte values. It probably doesn't matter much, since probably not many
        // people are using these anyway.
        other @ Decimal(_) => Ok(std::convert::TryFrom::try_from(other)
            .expect("converting decimal value to json is infallible")),
    }
}

const MILLIS_PER_SEC: i64 = 1_000;
const MICROS_PER_SEC: i64 = 1_000_000;
const NANOS_PER_SEC: i64 = 1_000_000_000;

fn time_from_midnight(
    avro_type: &'static str,
    column: &str,
    duration: i64,
    units_per_sec: i64,
) -> Result<Value, AvroError> {
    // times don't allow negative values
    let time = Some(to_secs_and_nanos(duration, units_per_sec))
        .filter(|&(secs, _)| secs >= 0)
        .and_then(|(secs, nanos)| NaiveTime::from_num_seconds_from_midnight_opt(secs as u32, nanos))
        .ok_or_else(|| AvroError::DateTimeOverflow(duration, avro_type, column.to_string()))?;

    Ok(Value::String(time.to_string()))
}

/// Accepts a duration in kind of a weird form to enable code re-use for avro's goofy timestamp
/// handling. Avro time-like logical types have different variants for millisecond vs microsecond
/// precision (e.g. timestamp-micros vs timestamp-millis). So the `duration` parameter accepts an
/// integer duration, which may be in either microseconds or milliseconds. The `units_per_sec`
/// parameter then accepts either `MICROS_PER_SEC` or `MILLIS_PER_SEC`, which is the constant used
/// to normalize the duration to seconds and subsecond nanoseconds, which is the format required by
/// the `chrono` crate.
fn to_secs_and_nanos(duration: i64, units_per_sec: i64) -> (i64, u32) {
    let mut secs = duration / units_per_sec;
    let rem = duration % units_per_sec;
    let mut nanos = (rem * NANOS_PER_SEC) / units_per_sec;
    if nanos < 0 {
        // (Then duration must also have been < 0)
        // The `chrono` crate expects timestamps to be given as (potentially negative) seconds
        // with subsecond nanoseconds being always positive. So in order to properly translate
        // a negative timestamp value, we need to subract an additional second and then add a
        // proportional number of subsecond nanoseconds.
        secs -= 1;
        nanos = NANOS_PER_SEC + nanos; // add, since nanos was negative
    }
    (secs, nanos as u32)
}

fn timestamp_from_unix_epoch(
    avro_type: &'static str,
    column: &str,
    duration: i64,
    units_per_second: i64,
) -> Result<Value, AvroError> {
    let (secs, nanos) = to_secs_and_nanos(duration, units_per_second);
    let ts = NaiveDateTime::from_timestamp_opt(secs, nanos)
        .ok_or_else(|| AvroError::DateTimeOverflow(duration, avro_type, column.to_string()))?;
    Ok(Value::String(ts.to_string()))
}

#[cfg(test)]
mod test {
    use super::*;
    use avro_rs::{types::Record, Writer};

    #[test]
    fn test_to_secs_and_nanos() {
        let test_cases = &[
            (2_000_000, MICROS_PER_SEC, (2, 0)),
            (2_000_001, MICROS_PER_SEC, (2, 1_000)),
            (-28_000_001, MICROS_PER_SEC, (-29, 999_999_000)),

            (9_001, MILLIS_PER_SEC, (9, 1_000_000)),
            (-9_999, MILLIS_PER_SEC, (-10, 1_000_000)),
        ];
        for (input, units_per_sec, expected) in test_cases {
            assert_eq!(
                *expected,
                to_secs_and_nanos(*input, *units_per_sec),
                "failed for input: {}, u: {}",
                input,
                units_per_sec
            );
        }
    }

    #[test]
    fn test_conversion_of_avro_logical_types() {
        let schema = avro_rs::Schema::parse_str(
            r#"{
                "name": "therecord",
                "type": "record",
                "fields": [
                    {
                      "name": "id",
                      "type": "string"
                    },
                    {
                      "name": "binary",
                      "type": "bytes"
                    },
                    {
                      "name": "float_string",
                      "type": "float"
                    },
                    {
                      "name": "date_string",
                      "type": "int",
                      "logicalType": "date"
                    },
                    {
                      "name": "time_string",
                      "type": "int",
                      "logicalType": "time-millis"
                    },
                    {
                      "name": "timestamp_string",
                      "type": "long",
                      "logicalType": "timestamp-millis"
                    },
                    {
                      "name": "float",
                      "type": "float"
                    },
                    {
                      "name": "date",
                      "type": "int",
                      "logicalType": "date"
                    },
                    {
                      "name": "time",
                      "type": "int",
                      "logicalType": "time-millis"
                    },
                    {
                      "name": "timestamp",
                      "type": "long",
                      "logicalType": "timestamp-millis"
                    }
                ]
            }"#,
        )
        .unwrap();

        let mut writer = Writer::new(&schema, Vec::with_capacity(1024));
        let mut record = Record::new(&schema).unwrap();
        record.put("id", "first");
        record.put("binary", AvroValue::Bytes(b"some bytes".to_vec()));

        record.put("float_string", AvroValue::Float(f32::NEG_INFINITY));
        record.put("date_string", AvroValue::Date(23456));
        record.put("time_string", AvroValue::TimeMillis(28_000_000));
        record.put(
            "timestamp_string",
            AvroValue::TimestampMillis(1_607_483_647_000i64),
        );

        record.put("float", AvroValue::Float(23.0));
        record.put("date", AvroValue::Date(23456));
        record.put("time", AvroValue::TimeMillis(28_000_000));
        record.put(
            "timestamp",
            AvroValue::TimestampMillis(1_607_483_647_000i64),
        );

        writer.append(record).expect("failed to write");
        let bytes = writer.into_inner().expect("failed to flush");
        let input = Input::Stream(Box::new(std::io::Cursor::new(bytes)));

        let config = ParseConfig {
            // The schema restricts some of the columns to only allow numbers, so we can test that
            // those get converted to numbers. Any logical type columns not mentioned here will get
            // converted as strings, which is the default.
            schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "float": {"type": "number"},
                    "date": {"type": "integer"},
                    "time": {"type": "number"},
                    "timestamp": {"type": "number"}
                }
            }),
            ..Default::default()
        };
        let mut iter = AvroIter::from_config_and_input(&config, input).expect("parse failed");

        // Asserts that we're picking up the right projections
        insta::assert_debug_snapshot!(&iter.projections);

        let json = iter.next().expect("next result").expect("next document");
        let expected = serde_json::json!({
            "binary":"c29tZSBieXRlcw==",
            "date":23456,
            "date_string":"2034-03-22",
            "float":23.0,
            "float_string":"-inf",
            "id":"first",
            "time":28000000,
            "time_string":"07:46:40",
            "timestamp":1607483647000i64,
            "timestamp_string":"2020-12-09 03:14:07"
        });
        assert_eq!(expected, json);
    }
}
