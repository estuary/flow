use proto_flow::ops;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;
use tracing_subscriber::prelude::*;

/// TokioContext manages a tokio::Runtime that names its threads under a given thread name,
/// and forwards its tracing events to a provided log handler.
pub struct TokioContext {
    runtime: Option<tokio::runtime::Runtime>,
    set_log_level_fn: Arc<dyn Fn(ops::log::Level) + Send + Sync>,
}

impl TokioContext {
    /// Build a new TokioContext and associated tokio::Runtime,
    /// having the `thread_name_prefix` and `worker_threads`.
    /// Threads of the context are initialized with a tracing Subscriber
    /// configured with `initial_log_level`.
    pub fn new<L>(
        initial_log_level: ops::log::Level,
        log_handler: L,
        thread_name_prefix: String,
        worker_threads: usize,
    ) -> Self
    where
        L: Fn(&ops::Log) + Send + Sync + 'static,
    {
        // Map the input thread name into unique thread names suffixed with their millisecond start time.
        let thread_name_fn = move || {
            let millis = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis();
            format!("{thread_name_prefix}-{}", millis)
        };

        // Dynamically configurable ops::log::Level, as a shared atomic.
        let log_level = std::sync::Arc::new(AtomicI32::new(initial_log_level as i32));

        // Function closure which allows for changing the dynamic log level.
        let log_level_clone = log_level.clone();
        let set_log_level = Arc::new(move |level: ops::log::Level| {
            log_level_clone.store(level as i32, Ordering::Relaxed)
        });

        // Build a tracing_subscriber::Filter which uses our dynamic log level.
        let log_filter = tracing_subscriber::filter::DynFilterFn::new(move |metadata, _cx| {
            let cur_level = match metadata.level().as_str() {
                "TRACE" => ops::log::Level::Trace as i32,
                "DEBUG" => ops::log::Level::Debug as i32,
                "INFO" => ops::log::Level::Info as i32,
                "WARN" => ops::log::Level::Warn as i32,
                "ERROR" => ops::log::Level::Error as i32,
                _ => ops::log::Level::UndefinedLevel as i32,
            };

            if let Some(path) = metadata.module_path() {
                // Hyper / HTTP/2 debug logs are just too noisy and not very useful.
                if path.starts_with("h2::") && cur_level >= ops::log::Level::Debug as i32 {
                    return false;
                }
            }

            cur_level <= log_level.load(Ordering::Relaxed)
        });

        // Configure a tracing::Dispatch, which is a type-erased form of a tracing::Subscriber,
        // that gathers tracing events & spans and logs them to `log_handler`.
        let log_dispatch: tracing::Dispatch = tracing_subscriber::registry()
            .with(
                ::ops::tracing::Layer::new(log_handler, std::time::SystemTime::now)
                    .with_filter(log_filter),
            )
            .into();

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(worker_threads)
            .enable_all()
            .thread_name_fn(thread_name_fn)
            .on_thread_start(move || {
                let guard = tracing::dispatcher::set_default(&log_dispatch);
                Self::DISPATCH_GUARD.with(|cell| cell.set(Some(guard)));
            })
            .on_thread_stop(|| {
                Self::DISPATCH_GUARD.with(|cell| cell.take());
            })
            .build()
            .unwrap();

        Self {
            runtime: Some(runtime),
            set_log_level_fn: set_log_level,
        }
    }

    /// Return a function closure which dynamically updates the configured log level for tracing events.
    pub fn set_log_level_fn(&self) -> Arc<dyn Fn(ops::log::Level) + Send + Sync> {
        self.set_log_level_fn.clone()
    }

    thread_local!(static DISPATCH_GUARD: std::cell::Cell<Option<tracing::dispatcher::DefaultGuard>> = std::cell::Cell::new(None));
}

impl Deref for TokioContext {
    type Target = tokio::runtime::Runtime;
    fn deref(&self) -> &Self::Target {
        self.runtime.as_ref().unwrap()
    }
}
impl DerefMut for TokioContext {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.runtime.as_mut().unwrap()
    }
}

impl Drop for TokioContext {
    fn drop(&mut self) {
        // Explicitly call Runtime::shutdown_background as an alternative to calling Runtime::Drop.
        // This shuts down the runtime without waiting for blocking background tasks to complete,
        // which is good because they sometimes never will. Consider a blocking call to read from stdin,
        // where the sender is itself waiting for us to exit or write to our stdout.
        // (Note that tokio::io maps AsyncRead of file descriptors to blocking tasks under the hood).
        self.runtime.take().unwrap().shutdown_background();
    }
}
