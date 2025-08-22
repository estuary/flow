use crate::service::{self, Channel, Service, ServiceImpl};
use std::io::Write;

/// UpperCase is an example Service that returns its inputs as upper-case
/// ASCII, along with a running sum of the number of bytes upper-cased
/// (returned with each response code).
pub struct UpperCase {
    sum_length: u32,
}

#[derive(Debug, thiserror::Error, serde::Serialize)]
#[error("{message}")]
pub struct UpperCaseError {
    code: u32,
    message: String,
}

impl Service for UpperCase {
    type Error = UpperCaseError;

    fn create() -> Self {
        Self { sum_length: 0 }
    }

    fn invoke(
        &mut self,
        code: u32,
        data: &[u8],
        arena: &mut Vec<u8>,
        out: &mut Vec<service::Out>,
    ) -> Result<(), Self::Error> {
        if data == b"whoops" {
            return Err(UpperCaseError {
                code,
                message: String::from("whoops"),
            });
        }

        let begin = arena.len() as u32;
        arena.extend(data.iter().map(u8::to_ascii_uppercase));
        self.sum_length += data.len() as u32;

        tracing::debug!(
            data_len = data.len(),
            sum_len = self.sum_length,
            "making stuff uppercase"
        );

        out.push(service::Out {
            code: self.sum_length,
            begin,
            end: arena.len() as u32,
        });

        Ok(())
    }
}

// Define cbindgen <=> CGO hooks for driving the UpperCase service.

#[unsafe(no_mangle)]
pub extern "C" fn upper_case_create(log_level: i32, log_dest_fd: i32) -> *mut Channel {
    service::create::<UpperCase>(log_level, log_dest_fd)
}
#[unsafe(no_mangle)]
pub extern "C" fn upper_case_invoke1(ch: *mut Channel, i: service::In1) {
    service::invoke::<UpperCase, _>(ch, i)
}
#[unsafe(no_mangle)]
pub extern "C" fn upper_case_invoke4(ch: *mut Channel, i: service::In4) {
    service::invoke::<UpperCase, _>(ch, i)
}
#[unsafe(no_mangle)]
pub extern "C" fn upper_case_invoke16(ch: *mut Channel, i: service::In16) {
    service::invoke::<UpperCase, _>(ch, i)
}
#[unsafe(no_mangle)]
pub extern "C" fn upper_case_drop(ch: *mut Channel) {
    service::drop::<UpperCase>(ch)
}

/// UpperCaseNaive is not part of UpperCase's service interface.
/// It's here for comparative benchmarking with a more traditional CGO call style.

struct UpperCaseNaive {
    sum_length: u32,
    arena: Vec<u8>,
}

#[unsafe(no_mangle)]
pub extern "C" fn create_upper_case_naive() -> *mut ServiceImpl {
    Box::leak(Box::new(UpperCaseNaive {
        sum_length: 0,
        arena: Vec::new(),
    })) as *mut UpperCaseNaive as *mut ServiceImpl
}

#[unsafe(no_mangle)]
pub extern "C" fn upper_case_naive(
    svc: *mut ServiceImpl,
    _code: u32,
    in_ptr: *const u8,
    in_len: u32,
    out_ptr: &mut *const u8,
    out_len: &mut u32,
) -> u32 {
    let svc = unsafe { &mut *(svc as *mut UpperCaseNaive) };
    let in_ = unsafe { std::slice::from_raw_parts(in_ptr, in_len as usize) };

    svc.arena.clear();

    let code = if in_ == b"whoops" {
        let err = std::io::Error::new(std::io::ErrorKind::Other, "whoops");
        write!(svc.arena, "{:?}", err).unwrap();
        std::u32::MAX
    } else {
        svc.arena.extend(in_.iter().map(u8::to_ascii_uppercase));
        svc.sum_length += in_.len() as u32;
        svc.sum_length
    };

    *out_ptr = svc.arena.as_ptr();
    *out_len = svc.arena.len() as u32;
    code
}
