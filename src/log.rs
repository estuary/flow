
/*
use slog::{o, Drain};
use std::sync::Once;

pub fn log() -> &'static slog::Logger {
    _LOG_INIT.call_once(|| {
        /*
        // Fancy version, that processes in another thread but can lose messages
        // with quick-running tests.
        let decorator = slog_term::TermDecorator::new().stderr().build();
        let drain = slog_term::FullFormat::new(decorator).build().fuse();
        let drain = slog_async::Async::new(drain).build().fuse();
        */
        let decorator = slog_term::PlainSyncDecorator::new(std::io::stderr());
        let drain = slog_term::FullFormat::new(decorator).build().fuse();
        unsafe {
            _LOG = Some(slog::Logger::root(drain, o!("app" => "derive")));
        }
    });
    unsafe { _LOG.as_ref().unwrap() }
}

static _LOG_INIT: Once = Once::new();
static mut _LOG: Option<slog::Logger> = None;
*/
