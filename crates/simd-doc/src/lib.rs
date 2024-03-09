use rkyv::ser::Serializer;

mod ffi;
pub mod transcoded;
pub use transcoded::Transcoded;

#[cfg(test)]
mod tests;

/// Parser is a very fast parser for JSON documents that transcodes directly
/// into instances of doc::ArchivedNode.
///
/// In the common case it uses simdjson to parse documents and directly
/// transcodes from simdjson's DOM into a byte representation that exactly
/// matches doc::ArchivedNode.
///
/// On my available hardware (a several-year-old Xeon, and a Macbook Air M2)
/// it achieves throughput of 800-950 MB per second in this happy-path case.
///
/// For large documents (greater than one megabyte) it falls back to serde_json
/// for parsing.
pub struct Parser {
    buf: Vec<u8>,
    ffi: cxx::UniquePtr<ffi::Parser>,
    offset: i64,
    parsed: Vec<(i64, doc::HeapNode<'static>)>,
}

impl Parser {
    pub fn new() -> Self {
        Self {
            buf: Vec::new(),
            // We must choose what the maximum capacity (and document size) of the
            // parser will be. This value shouldn't be too large, or it negatively
            // impacts parser performance. According to the simdjson docs, 1MB is
            // something of a sweet spot. Inputs larger than this capacity will
            // trigger the fallback handler.
            ffi: ffi::new_parser(1_000_000),
            offset: 0,
            parsed: Vec::new(),
        }
    }

    /// Parse a JSON document, which may have arbitrary whitespace,
    /// from `input` and return its doc::HeapNode representation.
    ///
    /// parse_one() cannot be called after a call to parse_chunk()
    /// or transcode_chunk() which retained a partial line remainder.
    /// Generally, a Parser should be used for working with single
    /// documents or working chunks of documents, but not both.
    pub fn parse_one<'s, 'a>(
        &'s mut self,
        input: &[u8],
        alloc: &'a doc::Allocator,
    ) -> Result<doc::HeapNode<'a>, std::io::Error> {
        // Safety: we'll transmute back to lifetime 'a prior to return.
        let alloc: &'static doc::Allocator = unsafe { std::mem::transmute(alloc) };

        assert!(
            self.buf.is_empty(),
            "internal buffer is non-empty (incorrect mixed use of parse_one() with parse() or transcode())"
        );
        self.buf.extend_from_slice(input);

        if let Err(err) = parse_simd(
            &mut self.buf,
            self.offset,
            alloc,
            &mut self.parsed,
            &mut self.ffi,
        ) {
            self.parsed.clear(); // Clear a partial simd parsing.
            tracing::debug!(%err, "simdjson JSON parsing failed; using fallback");
            () = parse_fallback(&mut self.buf, self.offset, alloc, &mut self.parsed)?;
        }

        if self.parsed.len() != 1 {
            let len = self.parsed.len();
            self.parsed.clear();

            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("expected one document, but parsed {len}"),
            ));
        }

        self.buf.clear();

        Ok(self.parsed.pop().unwrap().1)
    }

    /// Parse newline-delimited JSON documents of `chunk` into equivalent
    /// doc::HeapNode representations. `offset` is the offset of the first
    /// `chunk` byte within the context of its source stream, and is mapped into
    /// enumerated offsets of each transcoded output document.
    ///
    /// `chunk` may end with a partial document, in which case the partial
    /// document is held back and is expected to be continued by the `chunk`
    /// of a following call to `parse_chunk()`.
    pub fn parse_chunk<'s, 'a>(
        &'s mut self,
        chunk: &[u8],
        offset: i64,
        alloc: &'a doc::Allocator,
    ) -> Result<std::vec::Drain<'s, (i64, doc::HeapNode<'a>)>, std::io::Error> {
        // Safety: we'll transmute back to lifetime 'a prior to return.
        let alloc: &'static doc::Allocator = unsafe { std::mem::transmute(alloc) };

        let Some(last_newline) = self.prepare_chunk(chunk, offset)? else {
            return Ok(self.parsed.drain(..)); // Nothing to parse yet. drain(..) is empty.
        };
        if let Err(err) = parse_simd(
            &mut self.buf,
            self.offset,
            alloc,
            &mut self.parsed,
            &mut self.ffi,
        ) {
            self.parsed.clear(); // Clear a partial simd parsing.
            tracing::debug!(%err, "simdjson JSON parsing failed; using fallback");
            () = parse_fallback(&mut self.buf, self.offset, alloc, &mut self.parsed)?;
        }

        self.offset += self.buf.len() as i64;
        self.buf.clear();
        self.buf.extend_from_slice(&chunk[last_newline + 1..]);

        Ok(self.parsed.drain(..))
    }

    /// Transcode newline-delimited JSON documents of `chunk` into equivalent
    /// doc::ArchivedNode representations. `offset` is the offset of the first
    /// `chunk` byte within the context of its source stream, and is mapped into
    /// enumerated offsets of each transcoded output document.
    ///
    /// `chunk` may end with a partial document, in which case the partial
    /// document is held back and is expected to be continued by the `chunk`
    /// of a following call to `transcode()`.
    ///
    /// `pre_allocated` is a potentially pre-allocated buffer which is cleared
    /// and used within the returned Transcoded instance.
    pub fn transcode_chunk(
        &mut self,
        chunk: &[u8],
        offset: i64,
        pre_allocated: rkyv::AlignedVec,
    ) -> Result<Transcoded, std::io::Error> {
        let mut output = Transcoded {
            v: pre_allocated,
            offset: self.offset,
        };
        output.v.clear();

        let Some(last_newline) = self.prepare_chunk(chunk, offset)? else {
            return Ok(output); // Nothing to parse yet. `output` is empty.
        };
        if let Err(err) = transcode_simd(&mut self.buf, &mut output, &mut self.ffi) {
            output.v.clear(); // Clear a partial simd transcoding.
            tracing::debug!(%err, "simdjson JSON parsing failed; using fallback");
            output.v = transcode_fallback(&mut self.buf, std::mem::take(&mut output.v))?;
        }

        self.offset += self.buf.len() as i64;
        self.buf.clear();
        self.buf.extend_from_slice(&chunk[last_newline + 1..]);

        Ok(output)
    }

    #[inline]
    fn prepare_chunk(
        &mut self,
        input: &[u8],
        offset: i64,
    ) -> Result<Option<usize>, std::io::Error> {
        if self.buf.is_empty() {
            self.offset = offset;
        } else if self.offset + self.buf.len() as i64 != offset {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "parser has {} bytes of document prefix at offset {}, but got unexpected input offset {offset}",
                    self.buf.len(), self.offset
                ),
            ));
        };

        let Some(last_newline) = memchr::memrchr(b'\n', &input) else {
            // Neither `self.buf` nor `input` contain a newline,
            // and together reflect only a partial document.
            self.buf.extend_from_slice(input);
            return Ok(None);
        };

        // Complete a series of whole documents by appending through the final newline.
        // The remainder, which doesn't contain a newline, is held back for now.
        self.buf.extend_from_slice(&input[..last_newline + 1]);

        Ok(Some(last_newline))
    }
}

// Safety: field Parser.parsed is naively unsafe to Send.
// However, we maintain an invariant that Parser.parsed is empty unless:
// * A call to parse_one() or parse_chunk() is currently on the stack, or
// * A caller to parse_chunk() still holds an un-dropped Drain<> returned by Parser.parse_chunk.
//
// In both cases a borrow of Parser MUST be held by the caller, which means Parser
// cannot be sent between threads anyway. If that borrow is then dropped,
// then Drain<> will remove all contents from Parser.parsed.
unsafe impl Send for Parser {}

fn parse_simd<'a>(
    input: &mut Vec<u8>,
    offset: i64,
    alloc: &'a doc::Allocator,
    output: &mut Vec<(i64, doc::HeapNode<'a>)>,
    parser: &mut cxx::UniquePtr<ffi::Parser>,
) -> Result<(), cxx::Exception> {
    pad(input);

    let mut node = doc::HeapNode::Null;

    // Safety: Allocator, HeapNode, and Parsed are repr(transparent) wrappers.
    let alloc: &'a ffi::Allocator = unsafe { std::mem::transmute(alloc) };
    let node: &mut ffi::HeapNode<'a> = unsafe { std::mem::transmute(&mut node) };
    let output: &mut ffi::Parsed<'a> = unsafe { std::mem::transmute(output) };

    parser.pin_mut().parse(input, offset, alloc, node, output)
}

fn transcode_simd(
    input: &mut Vec<u8>,
    output: &mut Transcoded,
    parser: &mut cxx::UniquePtr<ffi::Parser>,
) -> Result<(), cxx::Exception> {
    pad(input);
    parser.pin_mut().transcode(input, output)
}

fn parse_fallback<'a>(
    input: &[u8],
    offset: i64,
    alloc: &'a doc::Allocator,
    output: &mut Vec<(i64, doc::HeapNode<'a>)>,
) -> Result<(), serde_json::Error> {
    let mut r = input;

    while let Some(skip) = r.iter().position(|c| !c.is_ascii_whitespace()) {
        r = &r[skip..];

        let offset = offset + input.len() as i64 - r.len() as i64;
        let mut deser = serde_json::Deserializer::from_reader(&mut r);
        let node = doc::HeapNode::from_serde(&mut deser, &alloc)?;

        output.push((offset, node));
    }

    Ok(())
}

fn transcode_fallback(
    input: &[u8],
    mut v: rkyv::AlignedVec,
) -> Result<rkyv::AlignedVec, serde_json::Error> {
    let mut alloc = doc::HeapNode::allocator_with_capacity(input.len());
    let mut r = input;

    while let Some(skip) = r.iter().position(|c| !c.is_ascii_whitespace()) {
        r = &r[skip..];

        let offset = input.len() as u32 - r.len() as u32;
        let mut deser = serde_json::Deserializer::from_reader(&mut r);
        let node = doc::HeapNode::from_serde(&mut deser, &alloc)?;

        // Write the document header (offset and length placeholder).
        v.extend_from_slice(&offset.to_le_bytes());
        v.extend_from_slice(&[0; 4]); // Length placeholder.
        let start_len = v.len();

        // Serialize HeapNode into ArchivedNode by extending our `output.v` buffer.
        let mut ser = rkyv::ser::serializers::AllocSerializer::<512>::new(
            rkyv::ser::serializers::AlignedSerializer::new(v),
            Default::default(),
            Default::default(),
        );
        ser.serialize_value(&node)
            .expect("rkyv serialization cannot fail");
        v = ser.into_serializer().into_inner();

        // Update the document header, now that we know the actual length.
        let len = ((v.len() - start_len) as u32).to_le_bytes();
        (&mut v[start_len - 4..start_len]).copy_from_slice(&len);

        alloc.reset();
    }

    Ok(v)
}

#[inline]
fn pad(input: &mut Vec<u8>) {
    static PAD: [u8; 64] = [0; 64]; // Required extra bytes for safe usage of simdjson.
    input.extend_from_slice(&PAD);
    input.truncate(input.len() - PAD.len());
}
