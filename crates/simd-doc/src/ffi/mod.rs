#[cxx::bridge]
mod ffi {

    unsafe extern "C++" {
        include!("simd-doc/src/ffi/simd-doc.h");

        type parser;

        fn new_parser(capacity: usize) -> UniquePtr<parser>;

        fn parse_many<'a>(
            input: &mut [u8],
            output: &mut Vec<u64>,
            parser: &mut UniquePtr<parser>,
        ) -> Result<usize>;
    }
}

use bytes::Buf;
pub(crate) use ffi::{new_parser, parser};

impl super::Parser {
    pub fn parse_simd<'a>(
        &mut self,
        input: &mut Vec<u8>,
        output: &mut Vec<(u32, doc::OwnedArchivedNode)>,
    ) -> Result<(), cxx::Exception> {
        // We must pad `input` with requisite extra bytes.
        let input_len = input.len();
        input.extend_from_slice(&[0; 64]);
        input.truncate(input_len);

        if input_len == 0 {
            return Ok(());
        }
        let mut buf = Vec::with_capacity(input.len() / 6);

        let consumed = ffi::parse_many(&mut *input, &mut buf, &mut self.0)?;
        input.drain(..consumed);

        // Swizzle `buf` from Vec<u64> => Vec<u8>.
        let mut buf = unsafe {
            let v = Vec::from_raw_parts(
                buf.as_mut_ptr() as *mut u8,
                buf.len() * 8,
                buf.capacity() * 8,
            );
            std::mem::forget(buf);
            v
        };
        // And again into bytes::Bytes after shrinking.
        buf.shrink_to_fit();
        let mut buf: bytes::Bytes = buf.into();

        while !buf.is_empty() {
            let header = u64::from_le_bytes(buf[0..8].try_into().unwrap());
            buf.advance(8); // Consume header.

            let offset = (header >> 32) as u32;
            let len = (header & 0xffffffff) as usize; // Length in 64-bit words.

            output.push((offset, unsafe {
                doc::OwnedArchivedNode::new(buf.split_to(len * 8))
            }));
        }

        Ok(())
    }
}
