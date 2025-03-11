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
    ffi: cxx::UniquePtr<ffi::Parser>,
    // Complete, newline-separate documents which are ready to parse.
    // This buffer always ends with a newline or is empty.
    whole: Vec<u8>,
    // Partial document for which we're awaiting a newline.
    // This buffer never contains any newlines.
    partial: Vec<u8>,
    // Offset of the first byte of `whole` or `partial` within the external stream.
    offset: i64,
    // Interior buffer used to hold parsed HeapNodes.
    // It's allocated but always empty between calls (drained upon parse() return).
    parsed: Vec<(doc::HeapNode<'static>, i64)>,
}

impl Parser {
    /// Return a new, empty Parser.
    pub fn new() -> Self {
        Self {
            // We must choose what the maximum capacity (and document size) of the
            // parser will be. This value shouldn't be too large, or it negatively
            // impacts parser performance. According to the simdjson docs, 1MB is
            // something of a sweet spot. Inputs larger than this capacity will
            // trigger the fallback handler.
            ffi: ffi::new_parser(1_000_000),
            whole: Vec::new(),
            partial: Vec::new(),
            offset: 0,
            parsed: Vec::new(),
        }
    }

    /// Parse a JSON document, which may have arbitrary whitespace,
    /// from `input` and return its doc::HeapNode representation.
    ///
    /// parse_one() cannot be called unless the Parser is completely empty,
    /// with no internal remainder from prior calls to chunk(), parse(),
    /// and transcode(). Generally, a Parser should be used for working with
    /// single documents or working chunks of documents, but not both.
    pub fn parse_one<'s, 'a>(
        &'s mut self,
        input: &[u8],
        alloc: &'a doc::Allocator,
    ) -> Result<doc::HeapNode<'a>, std::io::Error> {
        // Safety: we'll transmute back to lifetime 'a prior to return.
        let alloc: &'static doc::Allocator = unsafe { std::mem::transmute(alloc) };

        let mut scratch = String::new();
        let input = fixup_031125(input, &mut scratch);

        assert!(
            self.whole.is_empty(),
            "internal buffer is non-empty (incorrect mixed use of parse_one() with chunk())"
        );
        let mut buf = std::mem::take(&mut self.whole);
        buf.extend_from_slice(input);
        buf.push(b'\n');

        if let Err(err) = parse_simd(&mut buf, 0, alloc, &mut self.parsed, &mut self.ffi) {
            self.parsed.clear(); // Clear a partial simd parsing.
            tracing::debug!(%err, "simdjson JSON parse-one failed; using fallback");

            let mut de = serde_json::Deserializer::from_slice(&buf);
            let node = doc::HeapNode::from_serde(&mut de, &alloc)?;
            () = de.end()?;
            self.parsed.push((node, 0));
        }
        let mut parsed = self.parsed.drain(..);

        if parsed.len() != 1 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("expected one document, but parsed {}", parsed.len()),
            ));
        }

        // Re-use allocated capacity.
        buf.clear();
        self.whole = buf;

        Ok(parsed.next().unwrap().0)
    }

    /// Supply Parser with the next chunk of newline-delimited JSON document content.
    ///
    /// `chunk_offset` is the offset of the first `chunk` byte within the
    /// context of its external source stream.
    ///
    /// `chunk` may end with a partial document, or only contain part of a
    /// single document, in which case the partial document is expected to
    /// be continued by a following call to chunk().
    pub fn chunk(&mut self, chunk: &[u8], chunk_offset: i64) -> Result<(), std::io::Error> {
        let enqueued = self.whole.len() + self.partial.len();

        let result = if enqueued == 0 {
            self.offset = chunk_offset; // We're empty. Allow the offset to jump.
            Ok(())
        } else if chunk_offset == self.offset + enqueued as i64 {
            Ok(()) // Chunk is contiguous.
        } else {
            let err = std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "parser has {enqueued} bytes of document prefix starting at offset {}, but got {}-byte chunk at unexpected input offset {chunk_offset}",
                    self.offset, chunk.len(),
                ),
            );

            // Clear previous state to allow best-effort continuation.
            self.whole.clear();
            self.partial.clear();
            self.offset = chunk_offset;

            Err(err)
        };

        let Some(last_newline) = memchr::memrchr(b'\n', &chunk) else {
            // If `chunk` doesn't contain a newline, it cannot complete a document.
            self.partial.extend_from_slice(chunk);
            return result;
        };

        if self.whole.is_empty() {
            std::mem::swap(&mut self.whole, &mut self.partial);
            self.whole.extend_from_slice(&chunk[..last_newline + 1]);
            self.partial.extend_from_slice(&chunk[last_newline + 1..]);
        } else {
            self.whole.extend_from_slice(&self.partial);
            self.whole.extend_from_slice(&chunk[..last_newline + 1]);

            self.partial.clear();
            self.partial.extend_from_slice(&chunk[last_newline + 1..]);
        }

        result
    }

    /// Transcode newline-delimited JSON documents into equivalent
    /// doc::ArchivedNode representations. `buffer` is a potentially
    /// pre-allocated buffer which is cleared and used within the returned
    /// Transcoded instance.
    ///
    /// transcode() may return fewer documents than are available if an error
    /// is encountered in the input. Callers should repeatedly poll transcode()
    /// until it returns an empty Ok(Transcoded) in order to consume all
    /// documents and errors.
    pub fn transcode_many(
        &mut self,
        buffer: rkyv::AlignedVec,
    ) -> Result<Transcoded, (std::io::Error, std::ops::Range<i64>)> {
        let mut output = Transcoded {
            v: buffer,
            offset: self.offset,
        };
        output.v.clear();

        if self.whole.is_empty() {
            return Ok(output);
        }
        // Reserve 2x because transcodings use more bytes then raw JSON.
        output.v.reserve(2 * self.whole.len());

        let (consumed, maybe_err) =
            match transcode_simd(&mut self.whole, &mut output, &mut self.ffi) {
                Err(exception) => {
                    output.v.clear(); // Clear a partial simd transcoding.
                    tracing::debug!(%exception, "simdjson JSON transcoding failed; using fallback");

                    let (consumed, v, maybe_err) =
                        transcode_fallback(&self.whole, self.offset, std::mem::take(&mut output.v));
                    output.v = v;

                    (consumed, maybe_err)
                }
                Ok(()) => (self.whole.len(), None),
            };

        self.offset += consumed as i64;
        self.whole.drain(..consumed);

        if let Some(err) = maybe_err {
            return Err(err);
        }
        Ok(output)
    }

    /// Parse newline-delimited JSON documents into equivalent doc::HeapNode
    /// representations, backed by `alloc`.
    ///
    /// parse() returns the begin offset of the document sequence,
    /// and an iterator of a parsed document and the input offset of its
    /// *following* document. The caller can use the returned begin offset
    /// and iterator offsets to compute the [begin, end) offset extents
    /// of each parsed document.
    ///
    /// parse() may return fewer documents than are available if an error
    /// is encountered in the input. Callers should repeatedly poll parse()
    /// until it returns Ok with an empty iterator in order to consume all
    /// documents and errors.
    pub fn parse_many<'s, 'a>(
        &'s mut self,
        alloc: &'a doc::Allocator,
    ) -> Result<
        (i64, std::vec::Drain<'s, (doc::HeapNode<'a>, i64)>),
        (std::io::Error, std::ops::Range<i64>),
    > {
        // Safety: we'll transmute back to lifetime 'a prior to return.
        let alloc: &'static doc::Allocator = unsafe { std::mem::transmute(alloc) };

        if self.whole.is_empty() {
            return Ok((self.offset, self.parsed.drain(..))); // Nothing to parse yet. drain(..) is empty.
        };

        let (consumed, maybe_err) = match parse_simd(
            &mut self.whole,
            self.offset,
            alloc,
            &mut self.parsed,
            &mut self.ffi,
        ) {
            Err(exception) => {
                self.parsed.clear(); // Clear a partial simd parsing.
                tracing::debug!(%exception, "simdjson JSON parsing failed; using fallback");

                parse_fallback(&self.whole, self.offset, alloc, &mut self.parsed)
            }
            Ok(()) => (self.whole.len(), None),
        };

        let begin = self.offset;
        self.offset += consumed as i64;
        self.whole.drain(..consumed);

        if let Some(err) = maybe_err {
            return Err(err);
        }
        Ok((begin, self.parsed.drain(..)))
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
    output: &mut Vec<(doc::HeapNode<'a>, i64)>,
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

// To be removed after we've cleared through bad data from an incident.
fn fixup_031125<'a>(input: &'a [u8], scratch: &'a mut String) -> &'a [u8] {
    let Ok(input_str) = std::str::from_utf8(input) else {
        return input;
    };
    if !input_str.contains(r#""collection_generation_id":"#) {
        return input;
    }

    static RE: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
        regex::Regex::new(r#""collection_generation_id":\s?\d+e\d+,"#).unwrap()
    });
    *scratch = RE
        .replace_all(input_str, |caps: &regex::Captures| {
            " ".repeat(caps[0].len())
        })
        .into_owned();

    assert_eq!(input.len(), scratch.as_bytes().len());
    scratch.as_bytes()
}

fn parse_fallback<'a>(
    input: &[u8],
    offset: i64,
    alloc: &'a doc::Allocator,
    output: &mut Vec<(doc::HeapNode<'a>, i64)>,
) -> (usize, Option<(std::io::Error, std::ops::Range<i64>)>) {
    let mut consumed = 0;

    let mut scratch = String::new();
    let mut input = fixup_031125(input, &mut scratch);

    while !input.is_empty() {
        let pivot = memchr::memchr(b'\n', &input).expect("input always ends with newline") + 1;

        let mut de = serde_json::Deserializer::from_slice(&input[..pivot]);
        match doc::HeapNode::from_serde(&mut de, &alloc).and_then(|node| {
            () = de.end()?;
            Ok(node)
        }) {
            Ok(node) => {
                input = &input[pivot..];
                consumed += pivot;
                output.push((node, offset + consumed as i64));
            }
            // Surface an error encountered at the very first document.
            Err(err) if consumed == 0 => {
                return (pivot, Some((err.into(), offset..offset + pivot as i64)))
            }
            // Otherwise, return early with the documents we did parse.
            // We'll encounter the error again on our next call and return it then.
            Err(_err) => break,
        }
    }

    (consumed, None)
}

fn transcode_fallback(
    input: &[u8],
    offset: i64,
    mut v: rkyv::AlignedVec,
) -> (
    usize,
    rkyv::AlignedVec,
    Option<(std::io::Error, std::ops::Range<i64>)>,
) {
    let mut scratch = String::new();
    let mut input = fixup_031125(input, &mut scratch);

    let mut alloc = doc::HeapNode::allocator_with_capacity(input.len());
    let mut consumed = 0;

    while !input.is_empty() {
        let pivot = memchr::memchr(b'\n', &input).expect("input always ends with newline") + 1;

        let mut de = serde_json::Deserializer::from_slice(&input[..pivot]);
        match doc::HeapNode::from_serde(&mut de, &alloc).and_then(|node| {
            () = de.end()?;
            Ok(node)
        }) {
            Ok(node) => {
                input = &input[pivot..];
                consumed += pivot;

                // Write the document header (next interior offset and length placeholder).
                v.extend_from_slice(&(consumed as u32).to_le_bytes());
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
            // Surface an error encountered at the very first document.
            Err(err) if consumed == 0 => {
                return (pivot, v, Some((err.into(), offset..offset + pivot as i64)))
            }
            // Otherwise, return early with the documents we did parse.
            // We'll encounter the error again on our next call and return it then.
            Err(_err) => break,
        }
    }

    (consumed, v, None)
}

#[inline]
fn pad(input: &mut Vec<u8>) {
    static PAD: [u8; 64] = [0; 64]; // Required extra bytes for safe usage of simdjson.
    input.extend_from_slice(&PAD);
    input.truncate(input.len() - PAD.len());
}
