use crate::service::{self, Channel, Service};

/// UpperCase is an example Service that returns its inputs as upper-case
/// ASCII, along with a running sum of the number of bytes upper-cased
/// (returned with each response code).
pub struct UpperCase {
    sum_length: u32,
}

impl Service for UpperCase {
    fn create() -> Self {
        Self { sum_length: 0 }
    }

    fn invoke(
        &mut self,
        _code: u32,
        data: &[u8],
        arena: &mut Vec<u8>,
        out: &mut Vec<service::Out>,
    ) {
        let begin = arena.len() as u32;
        arena.extend(data.iter().map(u8::to_ascii_uppercase));

        out.push(service::Out {
            code: self.sum_length,
            begin,
            end: arena.len() as u32,
        });

        self.sum_length += data.len() as u32;
    }
}

// Define cbindgen <=> CGO hooks for driving the UpperCase service.

#[no_mangle]
pub extern "C" fn upper_case_create() -> *mut Channel {
    service::create::<UpperCase>()
}
#[no_mangle]
pub extern "C" fn upper_case_invoke1(ch: *mut Channel, i: service::In1) {
    service::invoke1::<UpperCase>(ch, i)
}
#[no_mangle]
pub extern "C" fn upper_case_invoke4(ch: *mut Channel, i: service::In4) {
    service::invoke4::<UpperCase>(ch, i)
}
#[no_mangle]
pub extern "C" fn upper_case_invoke16(ch: *mut Channel, i: service::In16) {
    service::invoke16::<UpperCase>(ch, i)
}
#[no_mangle]
pub extern "C" fn upper_case_drop(ch: *mut Channel) {
    service::drop::<UpperCase>(ch)
}

/// upper_case_naive is not part of UpperCase's service interface.
/// It's here for comparative benchmarking with a more traditional CGO call style.
#[no_mangle]
pub extern "C" fn upper_case_naive(
    _code: u32,
    in_ptr: *const u8,
    in_len: u32,
    out_ptr: &mut *const u8,
    out_len: &mut u32,
) -> u32 {
    let in_ = unsafe { std::slice::from_raw_parts(in_ptr, in_len as usize) };

    // SAFETY: this is a test-only function which is called from a single,
    // single-threaded Go benchmark.
    unsafe {
        UPPER_CASE_NAIVE_STORAGE.clear();
        UPPER_CASE_NAIVE_STORAGE.extend(in_.iter().map(u8::to_ascii_uppercase));
        *out_ptr = UPPER_CASE_NAIVE_STORAGE.as_ptr();
        *out_len = UPPER_CASE_NAIVE_STORAGE.len() as u32;
    };

    in_.len() as u32
}

static mut UPPER_CASE_NAIVE_STORAGE: Vec<u8> = Vec::new();
