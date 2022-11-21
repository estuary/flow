use super::{Log, LogLevel, Shard};
use serde::Deserialize;
use std::collections::BTreeMap;

/// Decoder decodes instances of Log from raw lines of text.
pub struct Decoder<T>
where
    T: Fn() -> time::OffsetDateTime,
{
    timesource: T,
}

// FlexLog is an implementation detail of a Decoder, and uses
// various serde features to more flexibly parse the components of a Log.
#[derive(Deserialize, Debug)]
struct FlexLog {
    /// Timestamp at which the Log was created.
    #[serde(
        default,
        deserialize_with = "time::serde::rfc3339::option::deserialize",
        alias = "timestamp",
        alias = "time"
    )]
    ts: Option<time::OffsetDateTime>,
    /// Level of the log.
    #[serde(default, alias = "lvl")]
    level: Option<FlexLevel>,
    /// Message of the log.
    #[serde(default, alias = "msg")]
    message: String,
    /// Supplemental fields of the log.
    #[serde(default)]
    fields: BTreeMap<String, Box<serde_json::value::RawValue>>,
    /// Metadata of the shard which created the Log.
    #[serde(default)]
    shard: Option<Shard>,
    /// Spans of the shard.
    #[serde(default)]
    spans: Vec<Log>,
    #[serde(default, flatten)]
    // Extra would ideally be Box<RawValue>, but this doesn't work with serde(flatten).
    // See: https://github.com/serde-rs/json/issues/883
    extra: BTreeMap<String, serde_json::Value>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "lowercase")]
enum FlexLevel {
    #[serde(alias = "TRACE")]
    Trace,
    #[serde(alias = "DEBUG")]
    Debug,
    #[serde(alias = "INFO")]
    Info,
    #[serde(alias = "WARN")]
    Warn,
    #[serde(alias = "ERROR")]
    Error,
}

impl From<FlexLevel> for LogLevel {
    fn from(f: FlexLevel) -> Self {
        match f {
            FlexLevel::Trace => Self::Trace,
            FlexLevel::Debug => Self::Debug,
            FlexLevel::Info => Self::Info,
            FlexLevel::Warn => Self::Warn,
            FlexLevel::Error => Self::Error,
        }
    }
}

impl<T> Decoder<T>
where
    T: Fn() -> time::OffsetDateTime,
{
    /// Build a new Decoder which uses the given `timesource`
    /// when a timestamp cannot be parsed from an input line.
    pub fn new(timesource: T) -> Self {
        Self { timesource }
    }

    /// Map a line of possibly structured log into a Log instance.
    /// All lines, even unstructured ones, are map-able into a Log,
    /// though they may have less structure extracted than we'd ideally like.
    pub fn line_to_log(&self, line: &str) -> Log {
        let Ok(flex) = serde_json::from_str::<FlexLog>(line) else {
            let level = if LIKELY_ERROR_RE.is_match(line) {
                LogLevel::Error
            } else {
                LogLevel::Warn
            };

            return Log {
                ts: (self.timesource)(),
                level,
                message: line.trim_end().to_string(),
                fields: Default::default(),
                shard: None,
                spans: Default::default(),
            };
        };

        let FlexLog {
            ts,
            level,
            mut message,
            mut fields,
            shard,
            spans,
            extra,
        } = flex;

        let ts = match ts {
            Some(ts) => ts,
            None => (self.timesource)(),
        };
        let level = match level {
            Some(level) => level.into(),
            None if LIKELY_ERROR_RE.is_match(line) => LogLevel::Error,
            None => LogLevel::Warn,
        };

        fields.extend(
            extra
                .into_iter()
                .map(|(k, v)| (k, serde_json::value::to_raw_value(&v).unwrap())),
        );

        if message.is_empty() {
            for key in ["message", "msg"] {
                let Some(msg) = fields.get(key)  else { continue };
                let Ok(msg) = serde_json::from_str(msg.get()) else { continue };
                message = msg;
                fields.remove(key);
            }
        }

        Log {
            ts,
            level,
            message,
            fields,
            shard,
            spans,
        }
    }
}

lazy_static::lazy_static! {
    static ref LIKELY_ERROR_RE: regex::Regex = {
        let list = &[
            "error",
            "failed",
            "panic",
            "timeout",
            "unable",
            "unrecognised", // sic for SSH address error messages.
            "unrecognized",
        ];
        // Match any of `list`, case insensitive.
        regex::Regex::new(&format!("(?i){}", list.join("|"))).expect("likely-error list must parse")
    };
}

#[cfg(test)]
mod test {
    use super::{Decoder, Log};

    #[test]
    fn test_decode_log_fixtures() {
        let fixtures = vec![
            // Typical example produced by our Go connectors.
            "{\"fence\":1,\"keyBegin\":0,\"keyEnd\":4294967295,\"level\":\"debug\",\"materialization\":\"examples/stats\",\"msg\":\"Acknowledge finished\",\"time\":\"2022-11-20T17:46:37Z\"}\n",
            // Example of a canonical ops Log that we expect to pass-through unmodified.
            r#"{"ts":"2022-11-20T17:46:36.119850056Z","level":"info","message":"my testing log","shard":{"kind":"capture","name":"the/capture/name","keyBegin":"0000aaaa","rClockBegin":"8899aabb"},"fields":{"five":"5","module":"flow_connector_init","true":"true"},"spans":[{"ts":"2022-11-20T17:46:36.119826426Z","level":"info","message":"my testing span","fields":{"module":"flow_connector_init","the_answer":42}}]}"#,
            // We'll do our best with anything else that looks kinda structured.
            // Fields can be mixed between the outer document and an inner `fields`.
            "{\"hello\":\"world\",\"false\":false,\"fortytwo\":42,\"extra\":\"read all about it\",\"fields\":{\"two\":2}}\t \r\r\n",
            // Structured logs having a likely error regex (but no level) are mapped to error.
            r#"{"msg":"something went bump","error":"couldn't properly frobulate"}"#,
            // Typical terminal error.
            "Unable to scrub the decks: a pirate approaches!\r\n",
            // Left-over printf debugging that becomes a warning.
            "a debug line that doesn't look very scary and is a warning",
            // Actual SSH error we want to flag as such.
            "ssh: debug1: resolve_canonicalize: hostname 123.456.789.10:32100 is an unrecognised address",
            // Structured log with extra gunk.
            r#"{"hello":"world"} !"#,
        ];

        let seq = std::cell::RefCell::new(0);
        let decoder = Decoder::new(|| {
            let mut seq = seq.borrow_mut();
            *seq += 10;
            time::OffsetDateTime::from_unix_timestamp(1660000000 + *seq).unwrap()
        });

        let logs: Vec<Log> = fixtures
            .into_iter()
            .map(|line| decoder.line_to_log(line))
            .collect();

        insta::assert_snapshot!(serde_json::to_string_pretty(&logs).unwrap(), @r###"
        [
          {
            "ts": "2022-11-20T17:46:37Z",
            "level": "debug",
            "message": "Acknowledge finished",
            "fields": {
              "fence": 1,
              "keyBegin": 0,
              "keyEnd": 4294967295,
              "materialization": "examples/stats"
            }
          },
          {
            "ts": "2022-11-20T17:46:36.119850056Z",
            "level": "info",
            "message": "my testing log",
            "fields": {
              "five": "5",
              "module": "flow_connector_init",
              "true": "true"
            },
            "shard": {
              "kind": "capture",
              "name": "the/capture/name",
              "keyBegin": "0000aaaa",
              "rClockBegin": "8899aabb"
            },
            "spans": [
              {
                "ts": "2022-11-20T17:46:36.119826426Z",
                "level": "info",
                "message": "my testing span",
                "fields": {
                  "module": "flow_connector_init",
                  "the_answer": 42
                }
              }
            ]
          },
          {
            "ts": "2022-08-08T23:06:50Z",
            "level": "warn",
            "message": "",
            "fields": {
              "extra": "read all about it",
              "false": false,
              "fortytwo": 42,
              "hello": "world",
              "two": 2
            }
          },
          {
            "ts": "2022-08-08T23:07:00Z",
            "level": "error",
            "message": "something went bump",
            "fields": {
              "error": "couldn't properly frobulate"
            }
          },
          {
            "ts": "2022-08-08T23:07:10Z",
            "level": "error",
            "message": "Unable to scrub the decks: a pirate approaches!"
          },
          {
            "ts": "2022-08-08T23:07:20Z",
            "level": "warn",
            "message": "a debug line that doesn't look very scary and is a warning"
          },
          {
            "ts": "2022-08-08T23:07:30Z",
            "level": "error",
            "message": "ssh: debug1: resolve_canonicalize: hostname 123.456.789.10:32100 is an unrecognised address"
          },
          {
            "ts": "2022-08-08T23:07:40Z",
            "level": "warn",
            "message": "{\"hello\":\"world\"} !"
          }
        ]
        "###);
    }
}
