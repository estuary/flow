use super::Output;

// Implement Output delegates for use from C++.
impl Output {
    fn as_mut_ptr(&mut self) -> *mut u8 {
        self.v.as_mut_ptr()
    }
    fn capacity(&self) -> usize {
        self.v.capacity()
    }
    fn len(&self) -> usize {
        self.v.len()
    }
    fn reserve(&mut self, additional: usize) {
        self.v.reserve(additional);
    }
    unsafe fn set_len(&mut self, len: usize) {
        self.v.set_len(len)
    }
}

#[cxx::bridge]
mod ffi {

    extern "Rust" {
        type Output;
        fn as_mut_ptr(&mut self) -> *mut u8;
        fn capacity(&self) -> usize;
        fn len(&self) -> usize;
        fn reserve(&mut self, additional: usize);
        unsafe fn set_len(&mut self, len: usize);
    }

    unsafe extern "C++" {
        include!("simd-doc/src/ffi/simd-doc.hpp");

        type Parser;
        fn new_parser(capacity: usize) -> UniquePtr<Parser>;
        fn parse<'a>(self: Pin<&mut Parser>, input: &[u8], output: &mut Output) -> Result<()>;
    }
}

// ffi::Parser is safe to Send across threads (but is not Sync).
unsafe impl Send for ffi::Parser {}

pub(crate) use ffi::{new_parser, Parser};
