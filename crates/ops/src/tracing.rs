use super::{Log, LogLevel};
use serde_json::json;

// Credit to this blog for a high-level overview for implementing custom tracing layer:
// https://burgers.io/custom-logging-in-rust-using-tracing
//
// Example usage:
//
//   use tracing_subscriber::prelude::*;
//   let env_filter = tracing_subscriber::EnvFilter::from_default_env();
//   tracing_subscriber::registry()
//     .with(ops::tracing::Layer::new(ops::stderr_log_handler).with_filter(env_filter))
//     .init();

pub struct Layer<H, T>(H, T)
where
    H: Fn(&Log),
    T: Fn() -> std::time::SystemTime;

impl<H, T> Layer<H, T>
where
    H: Fn(&Log),
    T: Fn() -> std::time::SystemTime,
{
    pub fn new(handler: H, timesource: T) -> Self {
        Self(handler, timesource)
    }

    pub fn log_from_metadata(&self, metadata: &tracing::Metadata) -> Log {
        let mut log = Log {
            meta: None,
            timestamp: Some(proto_flow::as_timestamp(self.1())),
            level: level_from_tracing(metadata.level()) as i32,
            message: String::new(),
            fields_json_map: Default::default(),
            shard: None,
            spans: Default::default(),
        };

        log.fields_json_map.insert(
            "module".to_string(),
            json!(metadata.target()).to_string().into(),
        );

        log
    }
}

impl<S, H, T> tracing_subscriber::Layer<S> for Layer<H, T>
where
    S: tracing::Subscriber,
    S: for<'lookup> tracing_subscriber::registry::LookupSpan<'lookup>,
    H: Fn(&Log) + 'static,
    T: Fn() -> std::time::SystemTime + 'static,
{
    fn on_new_span(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        id: &tracing::span::Id,
        ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let mut log = self.log_from_metadata(attrs.metadata());
        log.message = attrs.metadata().name().to_string();
        attrs.record(&mut FieldVisitor(&mut log));

        // Get an internal span reference and store `log` as an extension.
        let span = ctx.span(id).unwrap();
        let mut extensions = span.extensions_mut();
        extensions.insert(log);
    }

    fn on_record(
        &self,
        id: &tracing::span::Id,
        values: &tracing::span::Record<'_>,
        ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        // Get the span whose data is being recorded.
        let span = ctx.span(id).unwrap();
        let mut extensions = span.extensions_mut();
        let log: &mut Log = extensions.get_mut::<Log>().unwrap();
        values.record(&mut FieldVisitor(log));
    }

    fn on_event(&self, event: &tracing::Event<'_>, ctx: tracing_subscriber::layer::Context<'_, S>) {
        let mut log = self.log_from_metadata(event.metadata());
        event.record(&mut FieldVisitor(&mut log));

        // Attach context from parent spans, if any.
        if let Some(scope) = ctx.event_scope(event) {
            for span in scope.from_root() {
                let extensions = span.extensions();
                let span = extensions.get::<Log>().unwrap();
                log.spans.push(span.clone());
            }
        }

        self.0(&log)
    }
}

pub struct FieldVisitor<'a>(pub &'a mut Log);

impl<'a> FieldVisitor<'a> {
    fn record_raw<S>(&mut self, field: &tracing::field::Field, value: S)
    where
        S: serde::Serialize + ToString,
    {
        if field.name() == "message" && self.0.message.is_empty() {
            self.0.message = value.to_string();
        } else if let Ok(value) = serde_json::to_vec(&value) {
            self.0
                .fields_json_map
                .insert(field.name().to_string(), value.into());
        } else {
            // If `value` doesn't serialize, fall back to serializing its string representation.
            self.0.fields_json_map.insert(
                field.name().to_string(),
                json!(value.to_string()).to_string().into(),
            );
        }
    }
}

impl<'a> tracing::field::Visit for FieldVisitor<'a> {
    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        self.record_raw(field, value)
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.record_raw(field, value)
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.record_raw(field, value)
    }

    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.record_raw(field, value)
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.record_raw(field, value)
    }

    fn record_error(
        &mut self,
        field: &tracing::field::Field,
        value: &(dyn std::error::Error + 'static),
    ) {
        let parsed = |value: &dyn std::error::Error| {
            let value = format!("{value}");
            match serde_json::from_str::<serde_json::Value>(&value) {
                Ok(v) => v,
                Err(_) => serde_json::Value::String(value),
            }
        };

        if value.source().is_none() {
            self.record_raw(field, parsed(value));
            return;
        }

        let mut chain = Vec::new();
        let mut next = Some(value);
        while let Some(cur) = next {
            chain.push(parsed(cur));
            next = cur.source();
        }

        self.0
            .fields_json_map
            .insert(field.name().to_string(), json!(chain).to_string().into());
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        let stringified = format!("{value:?}");
        match serde_json::from_str::<serde::de::IgnoredAny>(&stringified) {
            Ok(_) => {
                self.0
                    .fields_json_map
                    .insert(field.name().to_string(), stringified.into());
            }
            Err(_) => self.record_raw(field, stringified),
        };
    }
}

pub fn level_from_tracing(lvl: &tracing::Level) -> LogLevel {
    match lvl.as_str() {
        "TRACE" => LogLevel::Trace,
        "DEBUG" => LogLevel::Debug,
        "INFO" => LogLevel::Info,
        "WARN" => LogLevel::Warn,
        "ERROR" => LogLevel::Error,
        other => panic!("{other:?} tracing::Level not handled"),
    }
}

#[cfg(test)]
mod test {

    use super::Layer;
    use serde_json::{json, Value};
    use std::sync::{Arc, Mutex};
    use tracing_subscriber::prelude::*;

    /// ValueError is an error that displays as a JSON value.
    /// Errors having Display implementations which parse as JSON are mapped
    /// into structured fields -- in other words, we avoid nesting a string of
    /// JSON within JSON.
    #[derive(Debug)]
    struct ValueError(Value);

    impl std::error::Error for ValueError {}

    impl std::fmt::Display for ValueError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            self.0.to_string().fmt(f)
        }
    }

    #[tracing::instrument(err, ret, level = "debug")]
    fn some_tracing_instrument_func(
        input: Result<String, anyhow::Error>,
    ) -> Result<String, anyhow::Error> {
        input
    }

    #[test]
    fn test_spans_and_events_are_mapped_to_structured_logs() {
        // Install a tracing subscriber which collects Log instances into `out`,
        // and which uses a stable time source fixture.
        let out = Arc::new(Mutex::new(Vec::new()));
        let out_clone = out.clone();
        let seq = Arc::new(Mutex::new(0));

        let _guard = tracing_subscriber::registry()
            .with(
                Layer::new(
                    move |log| out_clone.lock().unwrap().push(log.clone()),
                    move || {
                        let mut seq = seq.lock().unwrap();
                        *seq += 10;
                        time::OffsetDateTime::from_unix_timestamp(1660000000 + *seq)
                            .unwrap()
                            .into()
                    },
                )
                .with_filter(tracing::level_filters::LevelFilter::DEBUG),
            )
            .set_default();

        let span_one = tracing::debug_span!("first span", the_answer = 42);
        let enter_one = span_one.enter();

        let flat_str_error = anyhow::anyhow!("flat error");
        let flat_str_error: &(dyn std::error::Error + 'static) = flat_str_error.as_ref();

        let flat_json_error = ValueError(json!({"hey i'm":"structured", "threes": 333}));
        let flat_json_error: &(dyn std::error::Error + 'static) = &flat_json_error;

        let span_two = tracing::info_span!(
            "second testing span",
            the_question = "live, the universe, and everything",
            floating = 3.14159,
            flat_str_error,
            flat_json_error,
        );
        let enter_two = span_two.enter();

        let chain_error = anyhow::anyhow!(ValueError(
            json!({"structured": true, "baseball": ["cubs", "sox"]})
        ))
        .context("first context")
        .context("second context");
        let chain_error: &(dyn std::error::Error + 'static) = chain_error.as_ref();

        let a_debug = Some(Some(true));

        tracing::warn!(
            true = true,
            five = 5,
            chain_error,
            ?a_debug,
            "a scary warning"
        );

        std::mem::drop(enter_two);
        tracing::info!("an info message");

        tracing::trace!("a trace event which is filtered out");

        std::mem::drop(enter_one);
        _ = some_tracing_instrument_func(Ok("ok".to_string()));
        _ = some_tracing_instrument_func(Err(anyhow::anyhow!("whoops")));

        tracing::error!("a final error");

        let out = serde_json::to_string_pretty(out.lock().unwrap().as_slice()).unwrap();
        insta::assert_snapshot!(out, @r###"
        [
          {
            "ts": "2022-08-08T23:07:10+00:00",
            "level": "warn",
            "message": "a scary warning",
            "fields": {
              "a_debug": "Some(Some(true))",
              "chain_error": ["second context","first context",{"baseball":["cubs","sox"],"structured":true}],
              "five": 5,
              "module": "ops::tracing::test",
              "true": true
            },
            "spans": [
              {
                "ts": "2022-08-08T23:06:50+00:00",
                "level": "debug",
                "message": "first span",
                "fields": {
                  "module": "ops::tracing::test",
                  "the_answer": 42
                }
              },
              {
                "ts": "2022-08-08T23:07:00+00:00",
                "level": "info",
                "message": "second testing span",
                "fields": {
                  "flat_json_error": {"hey i'm":"structured","threes":333},
                  "flat_str_error": "flat error",
                  "floating": 3.14159,
                  "module": "ops::tracing::test",
                  "the_question": "live, the universe, and everything"
                }
              }
            ]
          },
          {
            "ts": "2022-08-08T23:07:20+00:00",
            "level": "info",
            "message": "an info message",
            "fields": {
              "module": "ops::tracing::test"
            },
            "spans": [
              {
                "ts": "2022-08-08T23:06:50+00:00",
                "level": "debug",
                "message": "first span",
                "fields": {
                  "module": "ops::tracing::test",
                  "the_answer": 42
                }
              }
            ]
          },
          {
            "ts": "2022-08-08T23:07:40+00:00",
            "level": "debug",
            "fields": {
              "module": "ops::tracing::test",
              "return": "ok"
            },
            "spans": [
              {
                "ts": "2022-08-08T23:07:30+00:00",
                "level": "debug",
                "message": "some_tracing_instrument_func",
                "fields": {
                  "input": "Ok(\"ok\")",
                  "module": "ops::tracing::test"
                }
              }
            ]
          },
          {
            "ts": "2022-08-08T23:08:00+00:00",
            "level": "error",
            "fields": {
              "error": "whoops",
              "module": "ops::tracing::test"
            },
            "spans": [
              {
                "ts": "2022-08-08T23:07:50+00:00",
                "level": "debug",
                "message": "some_tracing_instrument_func",
                "fields": {
                  "input": "Err(whoops)",
                  "module": "ops::tracing::test"
                }
              }
            ]
          },
          {
            "ts": "2022-08-08T23:08:10+00:00",
            "level": "error",
            "message": "a final error",
            "fields": {
              "module": "ops::tracing::test"
            }
          }
        ]
        "###);
    }
}
