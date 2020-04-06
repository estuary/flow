use estuary_json as ej;
use estuary_json_ext::{message as msg, ptr};
use fxhash;
use serde_json as sj;
use std::hash::Hasher;
use std::io::Write;

use super::{json_ptr_t, status_t};

#[allow(non_camel_case_types)]
pub enum msg_t {}

impl AsRef<msg::Message> for msg_t {
    fn as_ref(&self) -> &msg::Message {
        let p: *const msg_t = self;
        unsafe { &*(p as *const msg::Message) }
    }
}

impl AsMut<msg::Message> for msg_t {
    fn as_mut(&mut self) -> &mut msg::Message {
        let p: *mut msg_t = self;
        unsafe { &mut *(p as *mut msg::Message) }
    }
}

#[allow(non_camel_case_types)]
#[repr(C)]
pub struct uuid_t {
    bytes: [u8; 16],
}

impl From<uuid::Uuid> for uuid_t {
    fn from(o: uuid::Uuid) -> uuid_t {
        uuid_t {
            bytes: *o.as_bytes(),
        }
    }
}

impl From<uuid_t> for uuid::Uuid {
    fn from(o: uuid_t) -> uuid::Uuid {
        uuid::Uuid::from_bytes(o.bytes)
    }
}

#[allow(non_camel_case_types)]
#[repr(C)]
pub enum type_t {
    EST_DOES_NOT_EXIST,
    EST_NULL,
    EST_TRUE,
    EST_FALSE,
    EST_UNSIGNED,
    EST_SIGNED,
    EST_FLOAT,
    EST_STRING,
    EST_OBJECT,
    EST_ARRAY,
}

#[allow(non_camel_case_types)]
#[repr(C)]
pub struct extract_field_t<'p> {
    ptr: &'p json_ptr_t,
    type_: type_t,
    unsigned: u64,
    signed: i64,
    float: f64,
    begin: u32,
    end: u32,
}

#[no_mangle]
pub extern "C" fn est_msg_new(uuid_ptr: &json_ptr_t) -> *mut msg_t {
    let msg = msg::Message::new(uuid_ptr.as_ref().clone());
    Box::into_raw(Box::new(msg)) as *mut msg_t
}

#[no_mangle]
pub extern "C" fn est_msg_new_acknowledgement(m: &msg_t) -> *mut msg_t {
    let msg = msg::Message::new(m.as_ref().uuid_ptr.clone());
    Box::into_raw(Box::new(msg)) as *mut msg_t
}

#[no_mangle]
pub extern "C" fn est_msg_get_uuid(m: &msg_t) -> uuid_t {
    m.as_ref().get_uuid().into()
}

#[no_mangle]
pub extern "C" fn est_msg_set_uuid(m: &mut msg_t, to: uuid_t) {
    m.as_mut().set_uuid(to.into());
}

#[no_mangle]
pub extern "C" fn est_msg_marshal_json(m: &msg_t, buf: *mut u8, buf_len: usize) -> usize {
    let mut bw = unsafe { super::BufWriter::new(buf, buf_len) };
    sj::to_writer(&mut bw, &m.as_ref().doc).unwrap();
    bw.write("\n".as_bytes()).unwrap();
    bw.n_written
}

#[no_mangle]
pub extern "C" fn est_msg_unmarshal_json(
    m: &mut msg_t,
    buf: *const u8,
    buf_len: usize,
) -> status_t {
    let buf = unsafe { std::slice::from_raw_parts(buf, buf_len) };
    let m = m.as_mut();

    // Take & re-use existing Pointer.
    let ptr = std::mem::replace(&mut m.uuid_ptr, ptr::Pointer::new());
    *m = try_ok!(msg::Message::from_json_slice(ptr, buf));
    status_t::EST_OK
}

#[no_mangle]
pub extern "C" fn est_msg_extract_fields<'p>(
    m: &msg_t,
    fields: *mut extract_field_t<'p>,
    fields_len: usize,
    buf: *mut u8,
    buf_len: usize,
) -> usize {
    let m: &msg::Message = m.as_ref();
    let fields = unsafe { std::slice::from_raw_parts_mut(fields, fields_len) };
    let mut bw = unsafe { super::BufWriter::new(buf, buf_len) };

    for field in fields.iter_mut() {
        let value = field.ptr.as_ref().query(&m.doc);

        field.begin = bw.n_written as u32;
        field.type_ = match value {
            None => type_t::EST_DOES_NOT_EXIST,
            Some(sj::Value::Null) => type_t::EST_NULL,
            Some(sj::Value::Bool(true)) => type_t::EST_TRUE,
            Some(sj::Value::Bool(false)) => type_t::EST_FALSE,
            Some(sj::Value::Number(n)) => match n.into() {
                ej::Number::Unsigned(n) => {
                    field.unsigned = n;
                    type_t::EST_UNSIGNED
                }
                ej::Number::Signed(n) => {
                    field.signed = n;
                    type_t::EST_SIGNED
                }
                ej::Number::Float(n) => {
                    field.float = n;
                    type_t::EST_FLOAT
                }
            },
            Some(sj::Value::String(s)) => {
                bw.write(s.as_bytes()).unwrap();
                field.end = bw.n_written as u32;
                type_t::EST_STRING
            }
            Some(arr @ sj::Value::Array(_)) => {
                sj::to_writer(&mut bw, arr).unwrap();
                field.end = bw.n_written as u32;
                type_t::EST_ARRAY
            }
            Some(obj @ sj::Value::Object(_)) => {
                sj::to_writer(&mut bw, obj).unwrap();
                field.end = bw.n_written as u32;
                type_t::EST_OBJECT
            }
        };
    }
    bw.n_written
}

#[no_mangle]
pub extern "C" fn est_msg_hash_fields(
    m: &msg_t,
    ptrs: *const *const json_ptr_t,
    ptrs_len: usize,
) -> u64 {
    let m: &msg::Message = m.as_ref();
    let ptrs: &[*const json_ptr_t] = unsafe { std::slice::from_raw_parts(ptrs, ptrs_len) };

    let mut hasher = fxhash::FxHasher64::default();

    for ptr in ptrs.iter().copied() {
        let ptr: &ptr::Pointer = unsafe { &*ptr }.as_ref();
        let value = ptr.query(&m.doc).unwrap_or(&sj::Value::Null);
        let span = ej::de::walk(value, &mut ej::NoopWalker).unwrap();
        hasher.write_u64(span.hashed);
    }
    hasher.finish()
}

#[no_mangle]
pub extern "C" fn est_msg_drop(m: *mut msg_t) {
    assert!(!m.is_null());
    unsafe {
        Box::from_raw(m as *mut msg::Message);
    }
}
