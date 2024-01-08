use rkyv::ser::Serializer;

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

    pub fn parse<'a>(&mut self, input: &mut Vec<u8>) -> Result<Vec<u8>, serde_json::Error> {
        match self.parse_simd(input) {
            Ok(out) => Ok(out),
            Err(err) => {
                tracing::debug!(%err, "simdjson JSON parsing failed; trying serde");
                self.parse_serde(input)
            }
        }
    }

    pub fn parse_serde<'a>(&mut self, input: &mut Vec<u8>) -> Result<Vec<u8>, serde_json::Error> {
        let out = rkyv::AlignedVec::with_capacity(input.len());
        let mut out = rkyv::ser::serializers::AllocSerializer::<4096>::new(
            rkyv::ser::serializers::AlignedSerializer::new(out),
            Default::default(),
            Default::default(),
        );
        let mut alloc = doc::Allocator::new();

        let mut consumed = 0;
        while let Some(mut delta) = memchr::memchr(b'\n', &input[consumed..]) {
            delta += 1;
            let mut deser =
                serde_json::Deserializer::from_slice(&input[consumed..consumed + delta]);
            let node = doc::HeapNode::from_serde(&mut deser, &alloc)?;

            out.serialize_value(&node)
                .expect("rkyv serialization cannot fail");
            alloc.reset();

            consumed += delta;
        }
        input.drain(..consumed);

        let out = out.into_serializer();
        let out = out.into_inner();
        Ok(out.into())
    }
}
