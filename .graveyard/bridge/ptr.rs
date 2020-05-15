use estuary_json_ext::ptr;
use std::convert::TryFrom;
use std::ffi::CStr;
use std::os::raw::c_char;

use super::status_t;

#[allow(non_camel_case_types)]
pub enum json_ptr_t {}

impl AsRef<ptr::Pointer> for json_ptr_t {
    fn as_ref(&self) -> &ptr::Pointer {
        let p: *const json_ptr_t = self;
        unsafe { &*(p as *const ptr::Pointer) }
    }
}

#[no_mangle]
pub extern "C" fn est_json_ptr_new(uuid_ptr: *const c_char, out: *mut *mut json_ptr_t) -> status_t {
    assert!(!uuid_ptr.is_null());
    let uuid_ptr = unsafe { CStr::from_ptr(uuid_ptr) };
    let uuid_ptr = try_ok!(uuid_ptr.to_str());
    let uuid_ptr = try_ok!(ptr::Pointer::try_from(uuid_ptr));

    unsafe {
        *out = Box::into_raw(Box::new(uuid_ptr)) as *mut json_ptr_t;
    }
    status_t::EST_OK
}

#[no_mangle]
pub extern "C" fn est_json_ptr_drop(p: *mut json_ptr_t) {
    assert!(!p.is_null());
    unsafe {
        Box::from_raw(p as *mut ptr::Pointer);
    }
}
