pub use cgo::{Out, Service};
use std::sync;
use tracing_subscriber::prelude::*;

/// InN is a variadic input which invokes itself against a Service.
pub trait InN {
    fn invoke<S: Service>(
        self: &Self,
        svc: &mut S,
        arena: &mut Vec<u8>,
        out: &mut Vec<Out>,
    ) -> Result<(), S::Error>;
}

/// Input frame produced from CGO, which is a single service invocation.
/// 16 bytes, or 1/4 of a typical cache line.
#[repr(C)]
pub struct In1 {
    data_ptr: *const u8,
    data_len: u32,
    code: u32,
}

impl InN for In1 {
    #[inline]
    fn invoke<S: Service>(
        self: &Self,
        svc: &mut S,
        arena: &mut Vec<u8>,
        out: &mut Vec<Out>,
    ) -> Result<(), S::Error> {
        let data = if self.data_len == 0 {
            std::ptr::NonNull::dangling().as_ptr()
        } else {
            self.data_ptr
        };
        svc.invoke(
            self.code,
            unsafe { std::slice::from_raw_parts(data, self.data_len as usize) },
            arena,
            out,
        )
    }
}

/// Four invocations, composed into one struct.
/// 64 bytes, or one typical cache line.
#[repr(C)]
pub struct In4 {
    in0: In1,
    in1: In1,
    in2: In1,
    in3: In1,
}

impl InN for In4 {
    #[inline]
    fn invoke<S: Service>(
        self: &Self,
        svc: &mut S,
        arena: &mut Vec<u8>,
        out: &mut Vec<Out>,
    ) -> Result<(), S::Error> {
        self.in0.invoke(svc, arena, out)?;
        self.in1.invoke(svc, arena, out)?;
        self.in2.invoke(svc, arena, out)?;
        self.in3.invoke(svc, arena, out)
    }
}

/// Sixteen invocations, composed into one struct.
/// 256 bytes, or four typical cache lines.
#[repr(C)]
pub struct In16 {
    in0: In4,
    in1: In4,
    in2: In4,
    in3: In4,
}

impl InN for In16 {
    #[inline]
    fn invoke<S: Service>(
        self: &Self,
        svc: &mut S,
        arena: &mut Vec<u8>,
        out: &mut Vec<Out>,
    ) -> Result<(), S::Error> {
        self.in0.invoke(svc, arena, out)?;
        self.in1.invoke(svc, arena, out)?;
        self.in2.invoke(svc, arena, out)?;
        self.in3.invoke(svc, arena, out)
    }
}

/// Opaque pointer for a Service instance in the ABI.
#[repr(C)]
pub struct ServiceImpl {
    _private: [u8; 0],
}

/// Channel is shared between CGO and Rust, and holds details
/// about the language interconnect.
#[repr(C)]
pub struct Channel {
    // Opaque service pointer.
    svc_impl: *mut ServiceImpl,

    // Output memory arena, exposed to CGO.
    arena_ptr: *mut u8,
    arena_len: usize,
    arena_cap: usize,

    // Output frame codes & arena offsets, exposed to CGO.
    out_ptr: *mut Out,
    out_len: usize,
    out_cap: usize,

    // Final error returned by the Service.
    err_ptr: *mut u8,
    err_len: usize,
    err_cap: usize,

    // The tracing::Dispatch that will be used during channel invocations.
    // Dispatch is the type-erased form that is used by the tracing crate.
    // Once the channel is created, we no longer care about the specific
    // implementation of its wrapped tracing::Subscriber.
    // The representation here is just a plain pointer so the the Dispatch
    // type doesn't need to be defined in libbindings.h.
    tracing_dispatch: *mut u8,
}

/// Create a new Service instance, wrapped in an owning Channel.
/// This is intended to be monomorphized by each Service implementation, and exposed via cbindgen.
/// See the UpperCase service for an example. All logs will be written to the
/// `log_dest_file_descriptor`. This file descriptor will be closed when the service is destroyed.
/// If the `log_dest_file_descriptor` is <= 0, then logging will be disabled entirely for this
/// service. Each service must use a unique `log_dest_file_descriptor` to avoid interleaved logs
/// making the JSON output unparsable.
#[inline]
pub fn create<S: Service>(log_level: i32, log_dest_fd: i32) -> *mut Channel {
    // Use service creation as a common entry hook through which we can install global tracing.
    // The global subscriber initialized here will only be used as a fallback for events that
    // are produced from other threads (of which there should be none), so this is really just
    // a bit of insurance to make sure we don't miss anything important.
    static GLOBAL_SUBSCRIBE: sync::Once = sync::Once::new();

    GLOBAL_SUBSCRIBE.call_once(|| {
        let subscriber = tracing_subscriber::FmtSubscriber::builder()
            .with_max_level(tracing::level_filters::LevelFilter::DEBUG)
            .with_writer(std::io::stderr)
            .finish();
        tracing::subscriber::set_global_default(subscriber).unwrap();
    });

    // Map the `log_level`, given as an ops::LogLevel i32, into a tracing EnvFilter.
    let env_filter = format!(
        // TODO(johnny): I suspect we'll want to refine this environment filter,
        // for example by raising the level of third-party packages that produce
        // excessive output at debug or trace.
        "{}",
        ops::LogLevel::try_from(log_level)
            .unwrap_or(ops::LogLevel::Debug)
            .as_str_name()
    );
    let env_filter = match tracing_subscriber::EnvFilter::try_new(&env_filter) {
        Ok(f) => f,
        Err(err) => {
            tracing::error!(
                error = &err as &dyn std::error::Error,
                "failed to parse log-level EnvFilter"
            );
            tracing_subscriber::EnvFilter::new(env_filter)
        }
    };

    // Now initialize a tracing::Dispatch to which all events of this channel will be sent.
    // If `log_dest_file_descriptor` is zero, the resulting Dispatch is a no-op.
    let dispatch = if log_dest_fd > 0 {
        // Re-hydrate a fs::File from the descriptor passed from Go.
        // Rust (and this Channel) now own this fs::File and will close it on Drop.
        let log_dest = unsafe {
            use std::os::unix::io::FromRawFd;
            std::fs::File::from_raw_fd(log_dest_fd)
        };
        let handler =
            ops::new_encoded_json_write_handler(sync::Arc::new(sync::Mutex::new(log_dest)));
        let layer = ops::tracing::Layer::new(handler, std::time::SystemTime::now);

        tracing_subscriber::registry()
            .with(layer.with_filter(env_filter))
            .into()
    } else {
        tracing::Dispatch::none()
    };
    let dispatch = Box::into_raw(Box::new(dispatch));

    let svc_impl = Box::new(S::create());
    let svc_impl = Box::leak(svc_impl) as *mut S as *mut ServiceImpl;

    let ch = Box::new(Channel {
        svc_impl,
        arena_ptr: std::ptr::NonNull::dangling().as_ptr(),
        arena_len: 0,
        arena_cap: 0,
        out_ptr: std::ptr::NonNull::dangling().as_ptr(),
        out_len: 0,
        out_cap: 0,
        err_ptr: std::ptr::NonNull::dangling().as_ptr(),
        err_len: 0,
        err_cap: 0,
        tracing_dispatch: dispatch as *mut u8,
    });
    Box::leak(ch)
}

/// Invoke a Service with one input.
/// This is intended to be monomorphized by each Service implementation,
/// and exposed via cbindgen.  See the UpperCase service for an example.
#[inline]
pub fn invoke<S: Service, I: InN>(ch: *mut Channel, i: I) {
    let ch = unsafe { &mut *ch };

    if ch.err_cap != 0 {
        return; // If an error has been set, further invocations are no-ops.
    }

    let mut arena = unsafe { Vec::<u8>::from_raw_parts(ch.arena_ptr, ch.arena_len, ch.arena_cap) };
    let mut out = unsafe { Vec::<Out>::from_raw_parts(ch.out_ptr, ch.out_len, ch.out_cap) };
    let mut err_str = unsafe { String::from_raw_parts(ch.err_ptr, ch.err_len, ch.err_cap) };
    let svc_impl = unsafe { &mut *(ch.svc_impl as *mut S) };

    let dispatch = unsafe { &*(ch.tracing_dispatch as *mut tracing::Dispatch) };
    tracing::dispatcher::with_default(dispatch, || {
        let result = i.invoke(svc_impl, &mut arena, &mut out);

        if let Err(err) = result {
            // Include the error in logged error event.
            // Our ops::tracing::Layer implementation will extract the error
            // chain (if any) into a structured error, and will further attempt
            // to de-nest errors having a JSON Display implementation.
            tracing::error!(error = &err as &dyn std::error::Error, "{}", err);

            // Also pass the errors to Go as a terminal Debug error string.
            // When anyhow::Error is used (recommended!),
            // this is a pretty-printed error with its full causal chain.
            err_str = format!("{:?}", anyhow::Error::new(err));
        }
    });

    ch.arena_ptr = arena.as_mut_ptr();
    ch.arena_cap = arena.capacity();
    ch.arena_len = arena.len();
    std::mem::forget(arena);

    ch.out_ptr = out.as_mut_ptr();
    ch.out_cap = out.capacity();
    ch.out_len = out.len();
    std::mem::forget(out);

    ch.err_ptr = err_str.as_mut_ptr();
    ch.err_cap = err_str.capacity();
    ch.err_len = err_str.len();
    std::mem::forget(err_str);
}

/// Drop a Service and its Channel.
/// This is intended to be monomorphized by each Service implementation,
/// and exposed via cbindgen.  See the UpperCase service for an example.
#[inline]
pub fn drop<S: Service>(ch: *mut Channel) {
    let Channel {
        // Opaque service pointer.
        svc_impl,

        // Output frame codes & arena offsets, exposed to CGO.
        arena_ptr,
        arena_len,
        arena_cap,

        out_ptr,
        out_len,
        out_cap,

        err_ptr,
        err_len,
        err_cap,

        tracing_dispatch,
    } = *unsafe { Box::from_raw(ch) };

    let dispatch = unsafe { &*(tracing_dispatch as *mut tracing::Dispatch) };
    tracing::dispatcher::with_default(dispatch, || {
        tracing::trace!("dropped service");
    });

    // Drop svc_impl, arena, out, and tracing subscriber.
    _ = unsafe { Box::from_raw(svc_impl as *mut S) };
    _ = unsafe { Vec::<u8>::from_raw_parts(arena_ptr, arena_len, arena_cap) };
    _ = unsafe { Vec::<Out>::from_raw_parts(out_ptr, out_len, out_cap) };
    _ = unsafe { String::from_raw_parts(err_ptr, err_len, err_cap) };
    _ = unsafe { Box::from_raw(tracing_dispatch as *mut tracing::Dispatch) };
}
