
use estuary_json_ext::{message as msg, ptr};

#[derive(Debug)]
#[repr(C)]
#[allow(non_camel_case_types)]
pub enum status_t {
    OK,
    UTF8_PARSE_ERROR,
    MSG_JSON_PARSE_ERROR,
    MSG_UUID_BAD_LOCATION,
    MSG_UUID_NOT_A_STRING,
    MSG_UUID_PARSE_ERROR,
    JSON_PTR_NOT_ROOTED,
}

macro_rules! try_ok {
    ( $x:expr ) => {
        {
            match $x {
                Ok(x) => x,
                Err(e) => return e.into()
            }
        }
    };
}

#[no_mangle]
pub extern fn status_description(status: status_t, out: *mut u8, out_cap: usize) -> usize {
    let mut out = unsafe { Vec::from_raw_parts(out, 0, out_cap) };
    out.extend(format!("{:?}", status).as_bytes().iter().take(out_cap));

    let len = out.len();
    std::mem::forget(out);
    len
}

use status_t::*;

impl From<std::str::Utf8Error> for status_t {
    fn from(_e: std::str::Utf8Error) -> status_t {
        UTF8_PARSE_ERROR
    }
}

impl From<msg::Error> for status_t {
    fn from(e: msg::Error) -> status_t {
        use msg::Error::*;
        match e {
            JsonErr(_) => MSG_JSON_PARSE_ERROR,
            UuidBadLocation => MSG_UUID_BAD_LOCATION,
            UuidNotAString => MSG_UUID_NOT_A_STRING,
            UuidErr(_) => MSG_UUID_PARSE_ERROR,
        }
    }
}

impl From<ptr::Error> for status_t {
    fn from(e: ptr::Error) -> status_t {
        use ptr::Error::*;
        match e {
            NotRooted => JSON_PTR_NOT_ROOTED,
        }
    }
}