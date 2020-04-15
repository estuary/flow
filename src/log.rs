use std::sync::Once;
use slog::{o, Drain};
use slog_async;

pub fn log() -> &slog::Logger {
    _INIT.call_once(|| {
        let decorator = slog_term::PlainDecorator::new(std::io::stdout());
        let drain = slog_term::CompactFormat::new(decorator).build().fuse();
        let drain = slog_async::Async::new(drain).build().fuse();
        _log = Some(slog::Logger::root(drain, o!("version" => "0.5")));
    });
    &_log.unwrap()
}

static _INIT: Once = Once::new();
static _log: Option<slog::Logger> = None;