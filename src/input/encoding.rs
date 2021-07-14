//! The encoding module contains utilities for handling a variety of encodings of source data.
//! Generally, these functions will "just work" with common encodings, but there are some notable
//! limitations:
//!
//! - UTF-32(BE/LE) encodings are _not_ supported at all because they are not supported by the
//!   `encoding_rs` crate. This can be implemented in the future, but is being left out of scope
//!   for now.
//! - `detect_encoding` will have a hard time with UTF-16 contents that don't include a BOM.
//!

use crate::config::EncodingRef;
use chardetng::EncodingDetector;
use encoding_rs::{Decoder, DecoderResult, UTF_8};
use std::io;
use unicode_bom::Bom;

/*
Temporarily keeping this function around, since it's not yet clear whether we need it.
/// Determines if the given buffer is valid if interpreted in the given encoding by attempting to
/// transcode the buffer to utf-8 and returning false on the first malformed byte.
fn is_buffer_valid_as(buf: &[u8], encoding: &'static Encoding) -> bool {
    let mut decoder = encoding.new_decoder_with_bom_removal();
    let dest = &mut [0u8; 512];
    let mut input = buf;
    while !input.is_empty() {
        let (result, nin, nout) = decoder.decode_to_utf8_without_replacement(input, dest, false);
        if let DecoderResult::Malformed(_, _) = result {
            return false;
        }
        if nout == 0 {
            return true;
        }
        input = &input[nin..];
    }
    true
}
*/

/// Determine which encoding to use based on a prefix of the content contained within `buffer`.
/// "Guess" is the correct term for what we're doing here, because determining the correct encoding
/// can be quite difficult if you consider UTF-8 to be a permissible candidate. This function is
/// expected to get somewhat more accurate as the size of `buffer` increases, but it's not really
/// clear yet what an optimal size is.
/// This function will always prefer to return the encoding corresponding to the BOM, if one is present.
/// The BOM is more or less required at this point in order to differntiate UTF-16 from UTF-8 since
/// `null` is a valid code unit in both encodings. In the future, we may wish to apply some
/// heuristic based on null bytes occurring at even/odd intervals in order to better detect UTF-16
/// contents without a BOM. But for now, proper detection of UTF-16 will probably require the
/// presence of a BOM.
#[tracing::instrument(skip(buffer))]
pub fn detect_encoding(buffer: &[u8]) -> EncodingRef {
    // If a BOM is present, then we'll use whatever is indicated by the BOM.
    let from_bom = match Bom::from(buffer) {
        Bom::Utf8 => Some(UTF_8), // weird, but ok
        Bom::Utf16Be => Some(encoding_rs::UTF_16BE),
        Bom::Utf16Le => Some(encoding_rs::UTF_16LE),
        _ => None,
    };

    let result = from_bom.unwrap_or_else(|| {
        let mut detector = EncodingDetector::new();
        detector.feed(buffer, false);
        detector.guess(None, true)
    });
    tracing::debug!(
        encoding = result.name(),
        fromBOM = from_bom.is_some(),
        "finished detecting encoding"
    );
    EncodingRef::from(result)
}

/// An `io::Read` impl that transcodes from any `encoding_rs::Encoding` into UTF-8. The transcoding
/// is always strict in that an error will be returned if any byte from the source is not valid in
/// the given encoding, and no substitution character will be used for invalid bytes. Any BOM from
/// the `src` will be removed, if present.
pub struct TranscodingReader {
    src: Box<dyn io::Read>,
    decoder: Decoder,
    input_buffer: Vec<u8>,
    output_buffer: Vec<u8>,
    output_buffer_pos: usize,
    read_pos: usize,
    write_pos: usize,
    input_done: bool,
}

const TINY_OUTPUT_LEN: usize = 4;

impl TranscodingReader {
    /// Creates a new TranscodingReader that wraps the given `src`. The `buffer_size` determines
    /// the size of the intermediate buffer that's used in between reading from `src` and feeding
    /// data into the decoder. This `buffer_size` must be at least 4 and this function will panic
    /// if you pass anything smaller!
    pub fn with_buffer_size(
        src: Box<dyn io::Read>,
        encoding: EncodingRef,
        buffer_size: usize,
    ) -> TranscodingReader {
        assert!(
            buffer_size >= 4,
            "cannot use buffer_size < 4 with TranscodingReader"
        );
        TranscodingReader {
            src,
            decoder: encoding.encoding().new_decoder_with_bom_removal(),
            input_buffer: vec![0; buffer_size],
            output_buffer: Vec::with_capacity(TINY_OUTPUT_LEN),
            output_buffer_pos: 0,
            read_pos: 0,
            write_pos: 0,
            input_done: false,
        }
    }

    fn fill(&mut self) -> io::Result<()> {
        // If the buffer is nearly empty, then there may be a small number of unused bytes
        // at the end, which we will move to the beginning. This is because the decoder may not
        // be able to consume all bytes in a buffer if it doesn't end on a valid codepoint
        // boundary.
        if self.write_pos > self.read_pos {
            for (i, src) in (self.read_pos..self.write_pos).enumerate() {
                self.input_buffer[i] = self.input_buffer[src];
            }
        }
        self.write_pos -= self.read_pos;
        self.read_pos = 0;
        while self.write_pos < self.input_buffer.len() {
            let TranscodingReader {
                ref mut src,
                input_buffer: ref mut buffer,
                ref mut write_pos,
                ..
            } = self;
            match src.read(&mut buffer[*write_pos..]) {
                Ok(0) => {
                    self.input_done = true;
                    return Ok(());
                }
                Ok(n) => *write_pos += n,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => { /* just retry */ }
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    fn write_tiny_output_buffer(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let mut n = 0usize;
        for (i, b) in (self.output_buffer_pos..self.output_buffer.len())
            .take(buf.len())
            .enumerate()
        {
            buf[i] = self.output_buffer[b];
            n += 1;
        }
        self.output_buffer_pos += n;
        // If we were able to copy all the data, then clear the output buffer.
        if self.output_buffer_pos >= self.output_buffer.len() {
            self.output_buffer.clear();
            self.output_buffer_pos = 0;
        }
        // Return if we've written any data, and let the caller call `read` again.
        // This is to simplify compliance with the contract of Read, which says that `buf` must
        // not be modified if an error is returned.
        Ok(n)
    }

    fn is_done(&self) -> bool {
        self.input_done && self.read_pos >= self.write_pos && self.output_buffer.is_empty()
    }
}

impl io::Read for TranscodingReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.is_done() || buf.is_empty() {
            return Ok(0);
        }

        // If we have buffered output, write that here. This buffer is only ever used for handling
        // reads into tiny buffers.
        if self.output_buffer_pos < self.output_buffer.len() {
            return self.write_tiny_output_buffer(buf);
        }

        // Might we need to fill the input buffer?
        if self.read_pos >= self.write_pos && !self.input_done {
            self.fill()?;
        }

        let is_tiny_buf = buf.len() < TINY_OUTPUT_LEN;
        loop {
            let TranscodingReader {
                ref input_buffer,
                decoder,
                output_buffer,
                read_pos,
                write_pos,
                output_buffer_pos,
                input_done,
                ..
            } = self;

            // If the read destination is very small, then we'll need to transcode into a small output
            // buffer, since the decoder requires a minimum output buffer length of 4.
            let dest_buffer = if is_tiny_buf {
                // We know that the output buffer is empty at this point because we check and write
                // that first, returning early if it was non-empty.
                *output_buffer_pos = 0;
                output_buffer.extend_from_slice(&[0; TINY_OUTPUT_LEN]);
                &mut output_buffer[..]
            } else {
                &mut *buf
            };
            let (result, input_bytes, output_bytes) = decoder.decode_to_utf8_without_replacement(
                &input_buffer[*read_pos..*write_pos],
                dest_buffer,
                *input_done,
            );
            *read_pos += input_bytes;
            if is_tiny_buf {
                output_buffer.truncate(output_bytes);
                return self.write_tiny_output_buffer(buf);
            }
            // If we've made any progress, then just return it here and wait for another call to
            // read.
            if output_bytes > 0 {
                return Ok(output_bytes);
            }
            match result {
                DecoderResult::Malformed(_, _) => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "input bytes were not valid for encoding: '{}'",
                            self.decoder.encoding().name()
                        ),
                    ));
                }
                DecoderResult::InputEmpty if !self.input_done => {
                    self.fill()?;
                }
                DecoderResult::InputEmpty => {
                    // This block handles the case where a read ends right at the buffer boundary
                    // and a subsequent read of `src` returns `Ok(0)`. I haven't yet figured
                    // out how to exercise this path in a test, though :/.
                    debug_assert!(self.read_pos == self.write_pos);
                    return Ok(0);
                }
                DecoderResult::OutputFull => {
                    unreachable!("Cannot get an OutputFull result without having output_bytes > 0");
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::fs;
    use std::io::Read;

    #[test]
    fn transcoder_converts_input_to_utf_8() {
        let cases = &[
            (
                "tests/examples/utf-16be-without-bom.csv",
                encoding_rs::UTF_16BE,
            ),
            ("tests/examples/latin1.csv", encoding_rs::WINDOWS_1252),
            ("tests/examples/valid-utf-8.csv", encoding_rs::UTF_8),
        ];
        for &(input_file, source_encoding) in cases {
            let file = fs::File::open(input_file).expect("failed to open test file");
            // Use a really small buffer so that we can exercise the filling and shifting logic.
            let mut subject = TranscodingReader::with_buffer_size(
                Box::new(file),
                EncodingRef::from(source_encoding),
                4,
            );
            let mut result = String::new();
            subject.read_to_string(&mut result).expect("failed to read");
            assert!(
                result.starts_with(
                    r#""Darby's","level's","Reasoner's","towered","Truman's","operations""#
                ),
                "unexpected result: {}",
                result
            );
        }
    }

    #[test]
    fn encoding_is_detected() {
        use crate::input::Input;

        let cases = &[
            // Correctly guessed:
            ("tests/examples/valid-utf-16be.csv", encoding_rs::UTF_16BE),
            ("tests/examples/valid-utf-16le.csv", encoding_rs::UTF_16LE),
            ("tests/examples/valid-utf-8.csv", encoding_rs::UTF_8),
            ("tests/examples/valid-shift-jis.csv", encoding_rs::SHIFT_JIS),
            //
            // not-so-correctly guessed:
            ("tests/examples/latin1.csv", encoding_rs::UTF_8),
            (
                "tests/examples/utf-16be-without-bom.csv",
                encoding_rs::UTF_8,
            ),
        ];
        for &(path, expected) in cases {
            let file = fs::File::open(path).expect("failed to open test file");
            // 512 bytes is a little arbitrary. We can adjust this number upward if we add test
            // cases that aren't properly detected, since in the real world we'd probably peek at
            // at least a couple of kilobytes.
            let (contents, _) = Input::File(file)
                .peek(512)
                .expect("failed to read file contents");
            let guess = detect_encoding(&contents);
            assert_eq!(
                guess.encoding(),
                expected,
                "guessed wrong encoding of '{}' based on {} bytes",
                path,
                contents.len()
            );
        }
    }
}
