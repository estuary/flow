use estuary_json_ext::{message as msg, ptr};

#[derive(Debug)]
#[repr(C)]
#[allow(non_camel_case_types)]
pub enum status_t {
    EST_OK,
    EST_UTF8_PARSE_ERROR,
    EST_MSG_JSON_PARSE_ERROR,
    EST_MSG_UUID_BAD_LOCATION,
    EST_MSG_UUID_NOT_A_STRING,
    EST_MSG_UUID_PARSE_ERROR,
    EST_JSON_PTR_NOT_ROOTED,
}

macro_rules! try_ok {
    ( $x:expr ) => {{
        match $x {
            Ok(x) => x,
            Err(e) => return e.into(),
        }
    }};
}

use status_t::*;

impl From<std::str::Utf8Error> for status_t {
    fn from(_e: std::str::Utf8Error) -> status_t {
        EST_UTF8_PARSE_ERROR
    }
}

impl From<msg::Error> for status_t {
    fn from(e: msg::Error) -> status_t {
        use msg::Error::*;
        match e {
            JsonErr(_) => EST_MSG_JSON_PARSE_ERROR,
            UuidBadLocation => EST_MSG_UUID_BAD_LOCATION,
            UuidNotAString => EST_MSG_UUID_NOT_A_STRING,
            UuidErr(_) => EST_MSG_UUID_PARSE_ERROR,
        }
    }
}

impl From<ptr::Error> for status_t {
    fn from(e: ptr::Error) -> status_t {
        use ptr::Error::*;
        match e {
            NotRooted => EST_JSON_PTR_NOT_ROOTED,
        }
    }
}

#[no_mangle]
pub extern "C" fn est_status_description(status: status_t, out: *mut u8, out_cap: usize) -> usize {
    let mut out = unsafe { Vec::from_raw_parts(out, 0, out_cap) };
    out.extend(format!("{:?}", status).as_bytes().iter().take(out_cap));

    let len = out.len();
    std::mem::forget(out);
    len
}
