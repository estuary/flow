#[macro_use]
pub mod status;
pub use status::status_t;

pub mod ptr;
pub use ptr::json_ptr_t;

struct BufWriter<'b> {
    buf: &'b mut [u8],
    n_written: usize,
}

impl<'b> BufWriter<'b> {
    unsafe fn new(buf: *mut u8, buf_len: usize) -> BufWriter<'b> {
        let buf = std::slice::from_raw_parts_mut(buf, buf_len);
        BufWriter { buf, n_written: 0 }
    }
}

impl<'b> std::io::Write for BufWriter<'b> {
    fn write(&mut self, d: &[u8]) -> std::io::Result<usize> {
        self.buf.write(d).unwrap();
        self.n_written += d.len();
        Ok(d.len())
    }

    fn write_all(&mut self, d: &[u8]) -> std::io::Result<()> {
        self.buf.write(d).unwrap();
        self.n_written += d.len();
        Ok(())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub mod message;

/*
#[repr(C)]
pub enum Result {
    Ok(*mut demo::Message),
    Err,
}

#[no_mangle]
pub extern "C" fn result_message(r: &mut Result) -> *mut demo::Message {
    match r {
        Result::Ok(m) => *m,
        _ => panic!("not OK")
    }
}

#[repr(C)]
#[derive(Debug)]
pub struct Slice_c_char {
    pointer: *const c_char,
    length: usize,
}

#[no_mangle]
pub extern "C" fn message_new(content: Slice_c_char, other: c_int) -> Result {
    println!("rust sizes: {:?} {:?}", std::mem::size_of::<Slice_c_char>(), std::mem::size_of::<Result>());
    println!("call to message_new content {:?} other {}", content, other);

    let content = unsafe { std::slice::from_raw_parts(content.pointer as *const u8, content.length as usize) };
    let content = match std::str::from_utf8(content) {
        Ok(s) => s,
        Err(std::str::Utf8Error { .. }) => return Result::Err,
    };
    let m = Box::new(demo::Message::new(content, other));
    Result::Ok(Box::into_raw(m))
}

#[no_mangle]
pub extern "C" fn message_free(ptr: *mut demo::Message) {
    unsafe {
        Box::from_raw(ptr);
    }
}

#[no_mangle]
pub extern "C" fn message_length(msg: &demo::Message) -> usize {
    msg.length()
}

#[no_mangle]
pub extern "C" fn message_extend(msg: &mut demo::Message) {
    msg.extend()
}

*/
