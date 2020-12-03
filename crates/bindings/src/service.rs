/// Service is a trait implemented by services which may be called from Go.
pub trait Service {
    /// Create a new instance of the Service.
    fn create() -> Self;
    /// Invoke with the given op-code & |data| payload.
    /// It extends |arena| with any returned []byte data, and pushes output messages onto |out|.
    fn invoke(&mut self, code: u32, data: &[u8], arena: &mut Vec<u8>, out: &mut Vec<Out>);
}

/// Input frame produced from CGO, which is a single service invocation.
/// 16 bytes, or 1/4 of a typical cache line.
#[repr(C)]
pub struct In1 {
    data_ptr: *const u8,
    data_len: u32,
    code: u32,
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

/// Sixteen invocations, composed into one struct.
/// 256 bytes, or four typical cache lines.
#[repr(C)]
pub struct In16 {
    in00: In1,
    in01: In1,
    in02: In1,
    in03: In1,

    in04: In1,
    in05: In1,
    in06: In1,
    in07: In1,

    in08: In1,
    in09: In1,
    in10: In1,
    in11: In1,

    in12: In1,
    in13: In1,
    in14: In1,
    in15: In1,
}

/// Output
#[repr(C)]
pub struct Out {
    pub code: u32,
    pub begin: u32,
    pub end: u32,
}

/// Opaque pointer for a Service instance.
#[repr(C)]
pub struct ServiceImpl {
    _private: [u8; 0],
}

/// Channel is shared between CGO and Rust, and holds details
/// about the language interconnect.
#[repr(C)]
pub struct Channel {
    // Output memory arena, exposed to CGO.
    arena_ptr: *mut u8,
    arena_len: usize,
    arena_cap: usize,

    // Output frame codes & arena offsets, exposed to CGO.
    out_ptr: *mut Out,
    out_len: usize,
    out_cap: usize,

    // Opaque service pointer.
    svc_impl: *mut ServiceImpl,
}

/// Create a new Service instance, wrapped in an owning Channel.
/// This is intended to be monomorphized by each Service implementation,
/// and exposed via cbindgen.  See the UpperCase service for an example.
#[inline]
pub fn create<S: Service>() -> *mut Channel {
    let svc_impl = Box::new(S::create());
    let svc_impl = Box::leak(svc_impl) as *mut S as *mut ServiceImpl;

    let ch = Box::new(Channel {
        arena_ptr: 0 as *mut u8,
        arena_len: 0,
        arena_cap: 0,
        out_ptr: 0 as *mut Out,
        out_len: 0,
        out_cap: 0,
        svc_impl,
    });
    Box::leak(ch)
}

/// Invoke a Service with one input.
/// This is intended to be monomorphized by each Service implementation,
/// and exposed via cbindgen.  See the UpperCase service for an example.
#[inline]
pub fn invoke1<S: Service>(ch: *mut Channel, i: In1) {
    let ch = unsafe { &mut *ch };
    let mut arena = unsafe { Vec::<u8>::from_raw_parts(ch.arena_ptr, ch.arena_len, ch.arena_cap) };
    let mut out = unsafe { Vec::<Out>::from_raw_parts(ch.out_ptr, ch.out_len, ch.out_cap) };
    let svc_impl = unsafe { &mut *(ch.svc_impl as *mut S) };

    invoke::<S>(&i, svc_impl, &mut arena, &mut out);

    ch.arena_ptr = arena.as_mut_ptr();
    ch.arena_cap = arena.capacity();
    ch.arena_len = arena.len();
    std::mem::forget(arena);

    ch.out_ptr = out.as_mut_ptr();
    ch.out_cap = out.capacity();
    ch.out_len = out.len();
    std::mem::forget(out);
}

/// Invoke a Service with four inputs.
/// This is intended to be monomorphized by each Service implementation,
/// and exposed via cbindgen.  See the UpperCase service for an example.
#[inline]
pub fn invoke4<S: Service>(ch: *mut Channel, i: In4) {
    let ch = unsafe { &mut *ch };
    let mut arena = unsafe { Vec::<u8>::from_raw_parts(ch.arena_ptr, ch.arena_len, ch.arena_cap) };
    let mut out = unsafe { Vec::<Out>::from_raw_parts(ch.out_ptr, ch.out_len, ch.out_cap) };
    let svc_impl = unsafe { &mut *(ch.svc_impl as *mut S) };

    invoke::<S>(&i.in0, svc_impl, &mut arena, &mut out);
    invoke::<S>(&i.in1, svc_impl, &mut arena, &mut out);
    invoke::<S>(&i.in2, svc_impl, &mut arena, &mut out);
    invoke::<S>(&i.in3, svc_impl, &mut arena, &mut out);

    ch.arena_ptr = arena.as_mut_ptr();
    ch.arena_cap = arena.capacity();
    ch.arena_len = arena.len();
    std::mem::forget(arena);

    ch.out_ptr = out.as_mut_ptr();
    ch.out_cap = out.capacity();
    ch.out_len = out.len();
    std::mem::forget(out);
}

/// Invoke a Service with sixteen inputs.
/// This is intended to be monomorphized by each Service implementation,
/// and exposed via cbindgen.  See the UpperCase service for an example.
#[inline]
pub fn invoke16<S: Service>(ch: *mut Channel, i: In16) {
    let ch = unsafe { &mut *ch };
    let mut arena = unsafe { Vec::<u8>::from_raw_parts(ch.arena_ptr, ch.arena_len, ch.arena_cap) };
    let mut out = unsafe { Vec::<Out>::from_raw_parts(ch.out_ptr, ch.out_len, ch.out_cap) };
    let svc_impl = unsafe { &mut *(ch.svc_impl as *mut S) };

    invoke::<S>(&i.in00, svc_impl, &mut arena, &mut out);
    invoke::<S>(&i.in01, svc_impl, &mut arena, &mut out);
    invoke::<S>(&i.in02, svc_impl, &mut arena, &mut out);
    invoke::<S>(&i.in03, svc_impl, &mut arena, &mut out);

    invoke::<S>(&i.in04, svc_impl, &mut arena, &mut out);
    invoke::<S>(&i.in05, svc_impl, &mut arena, &mut out);
    invoke::<S>(&i.in06, svc_impl, &mut arena, &mut out);
    invoke::<S>(&i.in07, svc_impl, &mut arena, &mut out);

    invoke::<S>(&i.in08, svc_impl, &mut arena, &mut out);
    invoke::<S>(&i.in09, svc_impl, &mut arena, &mut out);
    invoke::<S>(&i.in10, svc_impl, &mut arena, &mut out);
    invoke::<S>(&i.in11, svc_impl, &mut arena, &mut out);

    invoke::<S>(&i.in12, svc_impl, &mut arena, &mut out);
    invoke::<S>(&i.in13, svc_impl, &mut arena, &mut out);
    invoke::<S>(&i.in14, svc_impl, &mut arena, &mut out);
    invoke::<S>(&i.in15, svc_impl, &mut arena, &mut out);

    ch.arena_ptr = arena.as_mut_ptr();
    ch.arena_cap = arena.capacity();
    ch.arena_len = arena.len();
    std::mem::forget(arena);

    ch.out_ptr = out.as_mut_ptr();
    ch.out_cap = out.capacity();
    ch.out_len = out.len();
    std::mem::forget(out);
}

/// Drop a Service and its Channel.
/// This is intended to be monomorphized by each Service implementation,
/// and exposed via cbindgen.  See the UpperCase service for an example.
#[inline]
pub fn drop<S: Service>(ch: *mut Channel) {
    let Channel {
        arena_ptr: arena,
        arena_len,
        arena_cap,

        // Output frame codes & arena offsets, exposed to CGO.
        out_ptr: out,
        out_len,
        out_cap,

        // Opaque service pointer.
        svc_impl,
    } = *unsafe { Box::from_raw(ch) };

    // Drop svc_impl, arena, and out.
    unsafe { Box::from_raw(svc_impl as *mut S) };
    unsafe { Vec::<u8>::from_raw_parts(arena, arena_len, arena_cap) };
    unsafe { Vec::<Out>::from_raw_parts(out, out_len, out_cap) };
}

// Helper for dispatching a service invocation.
#[inline]
fn invoke<S: Service>(in1: &In1, svc: &mut S, arena: &mut Vec<u8>, out: &mut Vec<Out>) {
    svc.invoke(
        in1.code,
        unsafe { std::slice::from_raw_parts(in1.data_ptr, in1.data_len as usize) },
        arena,
        out,
    )
}
