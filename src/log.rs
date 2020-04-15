use slog::{o, Drain};
use slog_async;
use std::sync::Once;

pub fn log() -> &'static slog::Logger {
    _LOG_INIT.call_once(|| {
        let decorator = slog_term::TermDecorator::new().build();
        let drain = slog_term::FullFormat::new(decorator).build().fuse();
        let drain = slog_async::Async::new(drain).build().fuse();
        unsafe {
            _LOG = Some(slog::Logger::root(drain, o!("app" => "derive")));
        }
    });
    unsafe { _LOG.as_ref().unwrap() }
}

static _LOG_INIT: Once = Once::new();
static mut _LOG: Option<slog::Logger> = None;
