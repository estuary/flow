use chrono::Utc;
use std::fs::File;
use std::io::{self, Write};
use std::sync::{Arc, Mutex};

/// Setup a global tracing subscriber using the RUST_LOG env variable. This subscriber is only used
/// for tracing that's performed on background threads, since a thread local subscriber is used for
/// all service invocations. This global subscriber is just a fallback so that we don't lose spans
/// and events that happen to be generated from other threads.
pub fn setup_env_tracing() {
    static SUBSCRIBE: std::sync::Once = std::sync::Once::new();

    SUBSCRIBE.call_once(|| {
        let subscriber = tracing_subscriber::FmtSubscriber::builder()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_writer(std::io::stderr)
            .finish();
        tracing::subscriber::set_global_default(subscriber).unwrap();
    });
}

/// Creates a new thread local subscriber, which can be used with the `tracing::dispatcher` to
/// capture logs for a single thread. All logs that pass the filter will be written to the given
/// `MakeWriter`.
///
/// The logs will be formatted in a way that's compatible with the log forwarding in:
/// go/flow/ops/forward_logs.go
/// The logs will be formatted as JSON, with all fields flattened into the top level object. The
/// timestamps will be formatted as RFC3339 with nanosecond precision.
pub fn new_thread_local_subscriber<W>(flow_level_filter: i32, make_writer: W) -> tracing::Dispatch
where
    W: for<'w> tracing_subscriber::fmt::MakeWriter<'w> + Send + Sync + 'static,
{
    let level_filter = level_filter(flow_level_filter);

    let subs = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(level_filter)
        .with_writer(make_writer)
        .json()
        // Without this, many fields (including the message) would get nested inside of a `"fields"`
        // object, which just makes parsing more difficult.
        .flatten_event(true)
        // The default timestamp formatting did not spark joy, so I made this one that writes in
        // RFC3339 with nanosecond precision (and a Z at the end, as god intended).
        .with_timer(TimeFormatter)
        // Using CLOSE span events seems like the best balance between helpfulness and verbosity.
        // Any Spans that are created will only be logged once they're done with (i.e. once a
        // `Future` has been `await`ed). This means that timing information will be recorded for
        // each span, and all fields will have had their values recorded. It also means that there
        // will be only 1 log line per span, so shouldn't be too overwhelming.
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
        // Adds info on the current span to each event emitted from within it. This might be a
        // little verbose, but we don't really use many spans so :shrug:
        .with_current_span(true)
        // This stuff just seems too verbose to be worth it.
        .with_span_list(false)
        .with_thread_ids(false)
        .with_thread_names(false)
        // "target" here refers to the rust module path (typically) from which the trace event
        // originated. It's not clear how useful it really is, especially for users of Flow, so I
        // left it disabled for now. But I could also see an argument for including it, so if
        // that's what you're here to do then go for it.
        .with_target(false)
        .finish();
    tracing::Dispatch::new(subs)
}

/// Ugh, this is annoying, mostly because of [this issue](https://github.com/tokio-rs/tracing/issues/675).
/// Tracing uses a layer of indirection where the actual `Write` impl is created on demand by a
/// `MakeWriter`, which is unable to return a reference. This is why we need our own `Write` impl.
/// The `Arc` and `Mutex` are required because tracing requires it to be `Send` and `Sync`. The
/// mutex probably doesn't prevent logs from being interleaved if multiple threads are using this
/// subscriber, though, so `FileWriter` should not be used with a global subscriber. The
/// `ManuallyDrop` is here because `service::create` says that we should not close this file.
#[derive(Clone)]
pub struct FileWriter(Arc<Mutex<File>>);
impl FileWriter {
    /// Creates a new `FileWriter`, which will write to the given `file_descriptor`. It's the
    /// caller's responsibility to ensure that the given file descriptor is valid, and not being
    /// written to from anywhere else. This is why creating a `FileWriter` is unsafe.
    pub unsafe fn new(file_decriptor: i32) -> FileWriter {
        use std::os::unix::io::FromRawFd;

        let file = File::from_raw_fd(file_decriptor);
        FileWriter(Arc::new(Mutex::new(file)))
    }
}
impl Write for FileWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut guard = self.0.lock().unwrap();
        guard.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        let mut guard = self.0.lock().unwrap();
        guard.flush()
    }

    fn write_vectored(&mut self, bufs: &[io::IoSlice<'_>]) -> io::Result<usize> {
        let mut guard = self.0.lock().unwrap();
        guard.write_vectored(bufs)
    }
}
impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for FileWriter {
    type Writer = Self;

    fn make_writer(&self) -> Self::Writer {
        self.clone()
    }
}

// The plain Fixed::RFC3339 format adds "+0:00" instead of "Z", and I know it's stupid but it
// bothered me and so I "fixed" it using this monstrosity.
struct TimeFormatter;
impl tracing_subscriber::fmt::time::FormatTime for TimeFormatter {
    fn format_time(&self, w: &mut tracing_subscriber::fmt::format::Writer<'_>) -> std::fmt::Result {
        use chrono::format::{Fixed, Item, Numeric::*, Pad::Zero};
        // We specify the format in this way so that we can avoid formatting into an intermediate
        // String.

        const RFC3339: &'static [Item<'static>] = &[
            Item::Numeric(Year, Zero),
            Item::Literal("-"),
            Item::Numeric(Month, Zero),
            Item::Literal("-"),
            Item::Numeric(Day, Zero),
            Item::Literal("T"),
            Item::Numeric(Hour, Zero),
            Item::Literal(":"),
            Item::Numeric(Minute, Zero),
            Item::Literal(":"),
            Item::Numeric(Second, Zero),
            Item::Fixed(Fixed::Nanosecond9),
            Item::Fixed(Fixed::TimezoneOffsetColonZ),
        ];

        write!(w, "{}", Utc::now().format_with_items(RFC3339.iter()))
    }
}

// Takes the `i32` representation of a `protocol::flow::LogLevelFilter` and returns an equivalent
// filter that works with `tracing`.
fn level_filter(flow_level_filter: i32) -> tracing::level_filters::LevelFilter {
    use protocol::flow::LogLevelFilter as FLevel;
    use tracing::level_filters::LevelFilter as TLevel;

    match FLevel::from_i32(flow_level_filter).unwrap_or(FLevel::Warn) {
        FLevel::Off => TLevel::OFF,
        FLevel::Trace => TLevel::TRACE,
        FLevel::Debug => TLevel::DEBUG,
        FLevel::Info => TLevel::INFO,
        FLevel::Warn => TLevel::WARN,
        FLevel::Error => TLevel::ERROR,
        FLevel::Raw => TLevel::ERROR,
    }
}

#[cfg(test)]
pub mod test {
    use super::*;
    use protocol::flow::LogLevelFilter;
    use serde_json::{json, Value};
    use std::io::BufRead;
    use std::sync::{Arc, Mutex};

    #[derive(Clone)]
    pub struct TestWriter(pub Arc<Mutex<Vec<u8>>>);
    impl TestWriter {
        pub fn new() -> TestWriter {
            TestWriter(Arc::new(Mutex::new(Vec::with_capacity(1024))))
        }

        pub fn into_output(self) -> Vec<u8> {
            let mutex = Arc::try_unwrap(self.0)
                .expect("There are still outstanding references to test writer output");
            mutex.into_inner().unwrap()
        }
    }
    impl Write for TestWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            let mut guard = self.0.lock().unwrap();
            guard.extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for TestWriter {
        type Writer = Self;

        fn make_writer(&self) -> Self::Writer {
            self.clone()
        }
    }

    #[test]
    fn no_outputs_produced_when_level_filter_is_off() {
        let writer = TestWriter::new();
        let subscriber = new_thread_local_subscriber(LogLevelFilter::Off as i32, writer.clone());

        tracing::dispatcher::with_default(&subscriber, || {
            tracing_test(7, "hope you don't see me");
        });
        // Drop the subscriber so that we can safely unwrap the reference counted output.
        std::mem::drop(subscriber);
        let bytes = writer.into_output();
        assert!(
            bytes.is_empty(),
            "oh no: {}",
            String::from_utf8_lossy(&bytes)
        );
    }

    #[test]
    fn test_logging_output_format() {
        let writer = TestWriter::new();
        let subscriber = new_thread_local_subscriber(LogLevelFilter::Debug as i32, writer.clone());

        tracing::dispatcher::with_default(&subscriber, || {
            tracing_test(3, "somestr");
        });
        // Drop the subscriber so that we can safely unwrap the reference counted output.
        std::mem::drop(subscriber);
        let bytes = writer.into_output();
        let mut lines = bytes.lines();

        let expected_logs = vec![
            json!({
                "level": "DEBUG",
                "is_empty": false,
                "message": "debug inside nested",
                "span": {"name": "nested_func", "string_arg": "\"the int is 3 and the str is somestr\""},
            }),
            json!({
                "level": "WARN",
                "message": "string might be odd",
                "is_odd": false,
                "span": {"name": "nested_func", "string_arg": "\"the int is 3 and the str is somestr\""},
            }),
            json!({
                "level":"INFO",
                "message":"close",
                "span":{"name":"nested_func", "string_arg": "\"the int is 3 and the str is somestr\""}
            }),
            json!({
                "level": "ERROR",
                "message": "oh no an error",
                "error": "this is only a test",
            }),
        ];

        for (i, expected) in expected_logs.into_iter().enumerate() {
            let line = lines
                .next()
                .unwrap_or_else(|| panic!("missing actual log at index: {}", i))
                .unwrap_or_else(|e| panic!("failed to read actual log at index: {}: {:?}", i, e));
            let actual: Value = serde_json::from_str(&line).unwrap_or_else(|e| {
                panic!(
                    "failed to deserialize actual log line: '{}', error: {}",
                    &line, e
                );
            });
            let actual_map = actual.as_object().expect("expected object");

            let expected_map = expected.as_object().unwrap();
            for (field, expected_val) in expected_map.iter() {
                assert_eq!(
                    Some(expected_val),
                    actual_map.get(field),
                    "mismatched {} for document {}, expected: {}, actual: {}",
                    field,
                    i,
                    expected,
                    actual
                );
            }
        }
        let next_line = lines.next();
        assert!(
            next_line.is_none(),
            "expected no more logs, got: {:?}",
            next_line
        );
    }

    fn tracing_test(int_arg: i64, str_arg: &str) -> bool {
        tracing::trace!(dummy_arg = "dummy", "this should not be logged");
        let s = format!("the int is {} and the str is {}", int_arg, str_arg);
        let b = nested_func(s);
        let err = std::io::Error::new(std::io::ErrorKind::Other, "this is only a test");
        tracing::error!(error = %err, "oh no an error");
        b
    }

    #[tracing::instrument]
    fn nested_func(string_arg: String) -> bool {
        tracing::debug!(is_empty = string_arg.is_empty(), "debug inside nested");
        let is_odd = string_arg.len() % 2 == 0;
        tracing::warn!(is_odd, "string might be odd");
        is_odd
    }
}
