use crate::logging::{new_thread_local_subscriber, FileWriter};
pub use protocol::cgo::{self, Out, Service};

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
        svc.invoke(
            self.code,
            unsafe { std::slice::from_raw_parts(self.data_ptr, self.data_len as usize) },
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

    // The tracing Dispatch that will be used for logging when this channel is used to invoke a
    // service. Dispatch is the type-erased form that is used by the tracing crate. Once the
    // channel is created, we no longer care about the specific implementation of the Subscriber.
    // The representation here is just a plain pointer so the the Dispatch type doesn't need to be
    // defined in libbindings.h.
    log_subscriber: *mut u8,
}

/// Create a new Service instance, wrapped in an owning Channel.
/// This is intended to be monomorphized by each Service implementation, and exposed via cbindgen.
/// See the UpperCase service for an example. All logs will be written to the
/// `log_dest_file_descriptor`. This file descriptor will be closed when the service is destroyed.
/// If the `log_dest_file_descriptor` is <= 0, then logging will be disabled entirely for this
/// service. Each service must use a unique `log_dest_file_descriptor` to avoid interleaved logs
/// making the JSON output unparseable.
#[inline]
pub fn create<S: Service>(log_level_filter: i32, log_dest_file_descriptor: i32) -> *mut Channel {
    // Use service creation as a common entry hook through which we can install global tracing and
    // logging. The global subscriber that's initialized here will only be used as a fallback for
    // logs that are produced from other threads, so this is really just a bit of insurance to make
    // sure we don't miss anything important.
    crate::logging::setup_env_tracing();

    // Now initialize the subscriber that will forward all the logs for this service to the
    // `log_dest_file_descriptor`, if logging is enabled. If disabled, then we'll set a no-op
    // subscriber.
    let subscriber = if log_dest_file_descriptor > 0 {
        new_thread_local_subscriber(log_level_filter, unsafe {
            FileWriter::new(log_dest_file_descriptor)
        })
    } else {
        tracing::Dispatch::none()
    };
    let dispatch = Box::into_raw(Box::new(subscriber));

    let svc_impl = Box::new(S::create());
    let svc_impl = Box::leak(svc_impl) as *mut S as *mut ServiceImpl;

    let ch = Box::new(Channel {
        svc_impl,
        arena_ptr: 0 as *mut u8,
        arena_len: 0,
        arena_cap: 0,
        out_ptr: 0 as *mut Out,
        out_len: 0,
        out_cap: 0,
        err_ptr: 0 as *mut u8,
        err_len: 0,
        err_cap: 0,
        log_subscriber: dispatch as *mut u8,
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

    let dispatch = unsafe { &*(ch.log_subscriber as *mut tracing::Dispatch) };
    let r = tracing::dispatcher::with_default(dispatch, || {
        let result = i.invoke(svc_impl, &mut arena, &mut out);
        if let Err(err) = &result {
            // Errors get passed to the go side as a simple string, but we'd ideally like to
            // include JSON representation of the error in the logs. So we always log errors here
            // on the Rust side so that logs can include all the gory details, but the error
            // that's returned to Go has the more human-readable representation.
            //
            // For now, we are serializing errors to a JSON string and setting them as the error
            // field. Yes, this is gross because that field will be a string containing json
            // instead of an actual json object. But the tracing project is planning to support
            // serialization of nested objects soon, so we should be able to switch this with an
            // impl that will serialize the error as a json object.
            // see: https://github.com/tokio-rs/tracing/issues/1570
            let err_json = serde_json::to_string(err).expect("serializing error cannot fail");
            tracing::error!(error = err_json.as_str(), "{}", err);
        }
        result
    });
    if let Err(err) = r {
        // Set terminal error string.
        err_str = format!("{:?}", anyhow::Error::new(err));
    }

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

        log_subscriber,
    } = *unsafe { Box::from_raw(ch) };

    // Drop svc_impl, arena, out, and tracing subscriber.
    unsafe { Box::from_raw(svc_impl as *mut S) };
    unsafe { Vec::<u8>::from_raw_parts(arena_ptr, arena_len, arena_cap) };
    unsafe { Vec::<Out>::from_raw_parts(out_ptr, out_len, out_cap) };
    unsafe { String::from_raw_parts(err_ptr, err_len, err_cap) };
    unsafe { Box::from_raw(log_subscriber as *mut tracing::Dispatch) };
}
