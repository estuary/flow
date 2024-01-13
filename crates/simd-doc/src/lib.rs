mod ffi;

pub struct Parser(cxx::UniquePtr<ffi::parser>);

impl Parser {
    pub fn new() -> Self {
        // We must choose what the maximum capacity (and document size) of the
        // parser will be. This value shouldn't be too large, or it negatively
        // impacts parser performance. According to the simdjson docs, 1MB is
        // something of a sweet spot. Inputs larger than this capacity will
        // trigger the fallback handler.
        Self(ffi::new_parser(1_000_000))
    }

    pub fn parse<'a>(
        &mut self,
        alloc: &'a doc::Allocator,
        docs: &mut Vec<(usize, doc::HeapNode<'a>)>,
        input: &mut Vec<u8>,
    ) -> Result<(), serde_json::Error> {
        if let Err(err) = self.parse_simd(alloc, docs, input) {
            tracing::debug!(%err, "simdjson JSON parsing failed; trying serde");
            docs.clear();
            return self.parse_serde(alloc, docs, input);
        };
        Ok(())
    }

    pub fn parse_serde<'a>(
        &mut self,
        alloc: &'a doc::Allocator,
        docs: &mut Vec<(usize, doc::HeapNode<'a>)>,
        input: &mut Vec<u8>,
    ) -> Result<(), serde_json::Error> {
        let mut consumed = 0;
        while let Some(mut delta) = memchr::memchr(b'\n', &input[consumed..]) {
            delta += 1;
            let mut deser =
                serde_json::Deserializer::from_slice(&input[consumed..consumed + delta]);
            docs.push((consumed, doc::HeapNode::from_serde(&mut deser, &alloc)?));
            consumed += delta;
        }
        input.drain(..consumed);
        Ok(())
    }
}
