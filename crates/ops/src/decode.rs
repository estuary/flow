use super::{Log, LogLevel, Shard};
use serde::Deserialize;
use std::collections::BTreeMap;

/// Decoder decodes instances of Log from raw lines of text.
pub struct Decoder<T>
where
    T: Fn() -> std::time::SystemTime,
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
    fields: BTreeMap<String, proto_flow::RawJSONDeserialize>,
    /// Metadata of the shard which created the Log.
    #[serde(default)]
    shard: Option<Shard>,
    /// Spans of the shard.
    #[serde(default)]
    spans: Vec<Log>,
    #[serde(default, flatten)]
    // Extra would ideally be Box<proto_flow::RawJSONDeserialize>,
    // but this doesn't work with serde(flatten).
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
    #[serde(alias = "WARN", alias = "warning", alias = "WARNING")]
    Warn,
    #[serde(
        alias = "ERROR",
        alias = "fatal",
        alias = "FATAL",
        alias = "panic",
        alias = "PANIC"
    )]
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
    T: Fn() -> std::time::SystemTime,
{
    /// Build a new Decoder which uses the given `timesource`
    /// when a timestamp cannot be parsed from an input line.
    pub fn new(timesource: T) -> Self {
        Self { timesource }
    }

    /// Map a line of possibly structured log into a Log instance.
    /// All lines, even unstructured ones, are map-able into a Log,
    /// though they may have less structure extracted than we'd ideally like.
    pub fn line_to_log(&self, line: &str, lookahead: &[u8]) -> (Log, usize) {
        match serde_json::from_str::<FlexLog>(line) {
            Ok(flex) => (self.flexlog_to_log(line, flex), 0),
            Err(_) => self.rawline_to_log(line, lookahead),
        }
    }

    fn flexlog_to_log(&self, line: &str, flex: FlexLog) -> Log {
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
            Some(ts) => ts.into(),
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
                .map(|(k, v)| (k, proto_flow::RawJSONDeserialize(v.to_string().into()))),
        );

        if message.is_empty() {
            for key in ["message", "msg"] {
                let Some(msg) = fields.get(key) else { continue };
                let Ok(msg) = serde_json::from_slice(&msg.0) else {
                    continue;
                };
                message = msg;
                fields.remove(key);
            }
        }

        Log {
            meta: None,
            timestamp: Some(proto_flow::as_timestamp(ts)),
            level: level as i32,
            message,
            fields_json_map: fields.into_iter().map(|(k, v)| (k, v.0)).collect(),
            shard: shard.map(Into::into),
            spans,
        }
    }

    fn rawline_to_log(&self, line: &str, lookahead: &[u8]) -> (Log, usize) {
        let mut message = line[..line.len() - 1].to_string(); // Strip single trailing \n.
        let mut consumed: usize = 0;

        // Attempt to consume additional whole lines of unstructured text.
        // Use peek() to ensure we don't take a last, partial line (without a newline).
        // Note that if `lookahead` happens to end in a newline, then the last item is an empty "".
        // Stop before a next line that parses as a FlexLog.
        let mut it = lookahead.split(|b| *b == b'\n').peekable();
        while let (Some(split), Some(_)) = (it.next(), it.peek()) {
            let line = String::from_utf8_lossy(split);

            if serde_json::from_str::<FlexLog>(&line).is_ok() {
                break;
            }

            consumed += split.len() + 1;
            message += "\n";
            message += &line;
        }
        message.truncate(message.trim_end().len());

        let level = if LIKELY_ERROR_RE.is_match(&message) {
            LogLevel::Error
        } else {
            LogLevel::Warn
        };

        let log = Log {
            meta: None,
            timestamp: Some(proto_flow::as_timestamp((self.timesource)())),
            level: level as i32,
            message,
            fields_json_map: Default::default(),
            shard: None,
            spans: Default::default(),
        };
        (log, consumed)
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
    use std::io::BufRead;

    use super::Decoder;

    #[test]
    fn test_decode_log_fixtures() {
        let fixtures = [
            // Typical example produced by our Go connectors.
            "{\"fence\":1,\"keyBegin\":0,\"keyEnd\":4294967295,\"level\":\"warning\",\"materialization\":\"examples/stats\",\"msg\":\"Acknowledge finished\",\"time\":\"2022-11-20T17:46:37Z\"}",
            // Structured log with extra gunk.
            r#"{"hello":"world"} !"#,
            // Example of a canonical ops Log that we expect to pass-through unmodified.
            r#"{"ts":"2022-11-20T17:46:36.119850056Z","level":"info","message":"my testing log","shard":{"kind":"capture","name":"the/capture/name","keyBegin":"0000aaaa","rClockBegin":"8899aabb"},"fields":{"five":"5","module":"flow_connector_init","true":"true"},"spans":[{"ts":"2022-11-20T17:46:36.119826426Z","level":"info","message":"my testing span","fields":{"module":"flow_connector_init","the_answer":42}}]}"#,
            // Typical terminal error.
            "Unable to scrub the decks: a pirate approaches!\t \t\r\n\n",
            // We'll do our best with anything else that looks kinda structured.
            // Fields can be mixed between the outer document and an inner `fields`.
            "{\"hello\":\"world\",\"false\":false,\"fortytwo\":42,\"extra\":\"read all about it\",\"fields\":{\"two\":2}}\t \r\r",
            // Left-over printf debugging that becomes a warning.
            "\t a debug line that doesn't look very scary and is a warning",
            // Structured logs having a likely error regex (but no level) are mapped to error.
            r#"{"msg":"something went bump","error":"couldn't properly frobulate"}"#,
            // Actual SSH error we want to flag as such.
            "ssh: debug1: resolve_canonicalize: hostname 123.456.789.10:32100 is an unrecognised address",
            // Empty structured log to separate these unstructured examples.
            r#"{}"#,
            // Actual Go stack trace.
            r#"panic: runtime error: index out of range [2] with length 2

goroutine 1 [running]:
main.foobar(...)
	/tmp/sandbox1284167461/prog.go:7
main.main()
	/tmp/sandbox1284167461/prog.go:11 +0x1b

Program exited.


Final line without a newline, which is not grouped into previous lines"#,
        ]
        .join("\n");

        let seq = std::cell::RefCell::new(0);
        let decoder = Decoder::new(|| {
            let mut seq = seq.borrow_mut();
            *seq += 10;
            time::OffsetDateTime::from_unix_timestamp(1660000000 + *seq)
                .unwrap()
                .into()
        });

        let mut logs = Vec::new();

        // Parse logs from the fixture input buffer, with look-ahead handling.
        let mut reader = std::io::BufReader::new(fixtures.as_bytes());
        let mut line = String::new();
        while reader.read_line(&mut line).unwrap() != 0 {
            let (log, lookahead) = decoder.line_to_log(&line, reader.buffer());

            logs.push(log);
            reader.consume(lookahead);
            line.clear();
        }

        insta::assert_snapshot!(serde_json::to_string_pretty(&logs).unwrap(), @r###"
        [
          {
            "ts": "2022-11-20T17:46:37+00:00",
            "level": "warn",
            "message": "Acknowledge finished",
            "fields": {
              "fence": 1,
              "keyBegin": 0,
              "keyEnd": 4294967295,
              "materialization": "examples/stats"
            }
          },
          {
            "ts": "2022-08-08T23:06:50+00:00",
            "level": "warn",
            "message": "{\"hello\":\"world\"} !"
          },
          {
            "shard": {
              "kind": "capture",
              "name": "the/capture/name",
              "keyBegin": "0000aaaa",
              "rClockBegin": "8899aabb"
            },
            "ts": "2022-11-20T17:46:36.119850056+00:00",
            "level": "info",
            "message": "my testing log",
            "fields": {
              "five": "5",
              "module": "flow_connector_init",
              "true": "true"
            },
            "spans": [
              {
                "ts": "2022-11-20T17:46:36.119826426+00:00",
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
            "ts": "2022-08-08T23:07:00+00:00",
            "level": "error",
            "message": "Unable to scrub the decks: a pirate approaches!"
          },
          {
            "ts": "2022-08-08T23:07:10+00:00",
            "level": "warn",
            "fields": {
              "extra": "read all about it",
              "false": false,
              "fortytwo": 42,
              "hello": "world",
              "two": 2
            }
          },
          {
            "ts": "2022-08-08T23:07:20+00:00",
            "level": "warn",
            "message": "\t a debug line that doesn't look very scary and is a warning"
          },
          {
            "ts": "2022-08-08T23:07:30+00:00",
            "level": "error",
            "message": "something went bump",
            "fields": {
              "error": "couldn't properly frobulate"
            }
          },
          {
            "ts": "2022-08-08T23:07:40+00:00",
            "level": "error",
            "message": "ssh: debug1: resolve_canonicalize: hostname 123.456.789.10:32100 is an unrecognised address"
          },
          {
            "ts": "2022-08-08T23:07:50+00:00",
            "level": "warn"
          },
          {
            "ts": "2022-08-08T23:08:00+00:00",
            "level": "error",
            "message": "panic: runtime error: index out of range [2] with length 2\n\ngoroutine 1 [running]:\nmain.foobar(...)\n\t/tmp/sandbox1284167461/prog.go:7\nmain.main()\n\t/tmp/sandbox1284167461/prog.go:11 +0x1b\n\nProgram exited."
          },
          {
            "ts": "2022-08-08T23:08:10+00:00",
            "level": "warn",
            "message": "Final line without a newline, which is not grouped into previous line"
          }
        ]
        "###);
    }
}
