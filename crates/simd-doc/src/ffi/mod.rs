use super::Out;

#[cxx::bridge]
mod ffi {

    extern "Rust" {
        type Out;
        fn len(&self) -> usize;
        unsafe fn extend(&mut self, data: *const u8, len: usize);
        fn begin(&mut self, source_offset: usize);
        fn finish(&mut self);
    }

    unsafe extern "C++" {
        include!("simd-doc/src/ffi/simd-doc.h");

        type SimdParser;
        fn new_parser(capacity: usize) -> UniquePtr<SimdParser>;

        fn parse_many<'a>(
            self: Pin<&mut SimdParser>,
            input: &mut [u8],
            output: &mut Out,
        ) -> Result<usize>;
    }
}

pub(crate) use ffi::{new_parser, SimdParser};

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

        let consumed = self.0.pin_mut().parse_many(&mut *input, output)?;
        input.drain(..consumed);

        Ok(())
    }
}
