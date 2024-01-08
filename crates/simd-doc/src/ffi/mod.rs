#[cxx::bridge]
mod ffi {

    unsafe extern "C++" {
        include!("simd-doc/src/ffi/simd-doc.h");

        type parser;

        fn new_parser(capacity: usize) -> UniquePtr<parser>;

        fn parse_many<'a>(
            input: &mut [u8],
            parser: &mut UniquePtr<parser>,
            out: &mut Vec<u64>,
        ) -> Result<usize>;
    }
}

pub(crate) use ffi::{new_parser, parser};

impl super::Parser {
    pub fn parse_simd<'a>(&mut self, input: &mut Vec<u8>) -> Result<Vec<u8>, cxx::Exception> {
        // We must pad `input` with requisite extra bytes.
        let input_len = input.len();
        input.extend_from_slice(&[0; 64]);
        input.truncate(input_len);

        if input_len == 0 {
            return Ok(Vec::new());
        }

        let mut out = Vec::new();

        let consumed = ffi::parse_many(&mut *input, &mut self.0, &mut out)?;
        input.drain(..consumed);

        // Swizzle `out` from Vec<u64> => Vec<u8>.
        let out = unsafe {
            let v = Vec::from_raw_parts(
                out.as_mut_ptr() as *mut u8,
                out.len() * 8,
                out.capacity() * 8,
            );
            std::mem::forget(out);
            v
        };

        Ok(out)
    }
}
