use super::Out;

#[cxx::bridge]
mod ffi {

    extern "Rust" {
        type Out;
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

        fn transcode_many<'a>(
            self: Pin<&mut Parser>,
            input: &mut [u8],
            output: &mut Out,
        ) -> Result<usize>;
    }
}

pub(crate) use ffi::{new_parser, Parser};

impl super::Parser {
    pub fn parse_simd<'a>(
        &mut self,
        input: &mut Vec<u8>,
        output: &mut Out,
    ) -> Result<(), cxx::Exception> {
        static PAD: [u8; 64] = [0; 64];

        // We must pad `input` with requisite extra bytes.
        input.extend_from_slice(&PAD);
        input.truncate(input.len() - PAD.len());

        if input.is_empty() {
            return Ok(());
        }

        let consumed = self.0.pin_mut().transcode_many(&mut *input, output)?;
        input.drain(..consumed);

        Ok(())
    }
}

// Parser is safe to Send across threads (but is not Sync).
unsafe impl Send for Parser {}
