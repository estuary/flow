use std::ops::{Deref, DerefMut};

// TaskRuntime is a tokio Runtime that names its threads under a given thread name,
// and forwards its tracing events and spans to a provided log file with a filter.
pub struct TaskRuntime {
    runtime: Option<tokio::runtime::Runtime>,
}

impl TaskRuntime {
    pub fn new(thread_name: String, log_dispatch: tracing::Dispatch) -> Self {
        // Map the input thread name into unique thread names suffixed with their millisecond start time.
        let thread_name_fn = move || {
            let millis = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis();
            format!("{thread_name}:{}", millis)
        };

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
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
        }
    }

    thread_local!(static DISPATCH_GUARD: std::cell::Cell<Option<tracing::dispatcher::DefaultGuard>> = std::cell::Cell::new(None));
}

impl Deref for TaskRuntime {
    type Target = tokio::runtime::Runtime;
    fn deref(&self) -> &Self::Target {
        self.runtime.as_ref().unwrap()
    }
}
impl DerefMut for TaskRuntime {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.runtime.as_mut().unwrap()
    }
}

impl Drop for TaskRuntime {
    fn drop(&mut self) {
        // Explicitly call Runtime::shutdown_background as an alternative to calling Runtime::Drop.
        // This shuts down the runtime without waiting for blocking background tasks to complete,
        // which is good because they likely never will. Consider a blocking call to read from stdin,
        // where the sender is itself waiting for us to exit or write to our stdout.
        // (Note that tokio::io maps AsyncRead of file descriptors to blocking tasks under the hood).
        self.runtime.take().unwrap().shutdown_background();
    }
}
