
use estuary_json_ext::{message as msg, ptr};
use std::os::raw::c_char;
use std::ffi::CStr;
use std::convert::TryFrom;
use super::status_t;

#[allow(non_camel_case_types)]
pub enum builder_t {}

#[allow(non_camel_case_types)]
pub enum message_t {}

#[allow(non_camel_case_types)]
#[repr(C)]
pub struct uuid_t {
    bytes: [u8; 16]
}

#[allow(non_camel_case_types)]
#[repr(C)]
pub struct buffer_t {
    ptr: *mut u8,
    len: usize,
    cap: usize,
}

impl From<Vec<u8>> for buffer_t {
    fn from(mut v: Vec<u8>) -> Self {
        let b = buffer_t { ptr: v.as_mut_ptr(), len: v.len(), cap: v.capacity() };
        std::mem::forget(v);
        b
    }
}

impl From<buffer_t> for Vec<u8> {
    fn from(b: buffer_t) -> Self {
        unsafe { Vec::from_raw_parts(b.ptr, b.len, b.cap) }
    }
}

#[no_mangle]
pub extern fn buffer_drop(b: buffer_t) {
    Vec::<u8>::from(b);
}

#[no_mangle]
pub extern "C" fn msg_builder_new(uuid_ptr: *const c_char, out: *mut *mut builder_t) -> status_t {
    assert!(!uuid_ptr.is_null());
    let uuid_ptr = unsafe { CStr::from_ptr(uuid_ptr) };
    let uuid_ptr = try_ok!(uuid_ptr.to_str());
    let uuid_ptr = try_ok!(ptr::Pointer::try_from(uuid_ptr));

    unsafe { *out = Box::into_raw(Box::new(msg::Builder::new(uuid_ptr))) as *mut builder_t; }
    status_t::OK
}

#[no_mangle]
pub extern fn msg_builder_drop(b: *mut builder_t) {
    assert!(!b.is_null());
    unsafe { Box::from_raw(b as *mut msg::Builder); }
}

#[no_mangle]
pub extern fn msg_builder_build(b: *const builder_t) -> *mut message_t {
    let b = unsafe { &*(b as *const msg::Builder) };
    let m = b.build();

    Box::into_raw(Box::new(m)) as *mut message_t
}

#[no_mangle]
pub extern fn msg_get_uuid(m: *const message_t) -> uuid_t {
    let m = unsafe { &*(m as *const msg::Message) };
    uuid_t{ bytes: *m.get_uuid().as_bytes() }
}

#[no_mangle]
pub extern fn msg_set_uuid(m: *mut message_t, to: uuid_t) {
    let m = unsafe { &mut *(m as *mut msg::Message) };
    m.set_uuid(uuid::Uuid::from_bytes(to.bytes));
}

#[no_mangle]
pub extern fn msg_marshal_json(m: *const message_t) -> buffer_t {
    let m = unsafe { &*(m as *const msg::Message) };
    serde_json::to_vec(&m.doc).unwrap().into()
}

#[no_mangle]
pub extern fn msg_drop(m: *mut message_t) {
    assert!(!m.is_null());
    unsafe { Box::from_raw(m as *mut msg::Message); }
}