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

    pub fn parse<'a>(
        &mut self,
        input: &mut Vec<u8>,
        output: &mut Vec<(u32, doc::OwnedArchivedNode)>,
    ) -> Result<(), serde_json::Error> {
        if let Err(err) = self.parse_simd(input, output) {
            tracing::debug!(%err, "simdjson JSON parsing failed; trying serde");
            return self.parse_serde(input, output);
        }
        Ok(())
    }

    pub fn parse_serde<'a>(
        &mut self,
        input: &mut Vec<u8>,
        output: &mut Vec<(u32, doc::OwnedArchivedNode)>,
    ) -> Result<(), serde_json::Error> {
        let mut alloc = doc::Allocator::with_capacity(input.len());
        let mut offset = 0;

        while let Some(mut pivot) = memchr::memchr(b'\n', &input[offset..]) {
            pivot += 1;

            let mut deser = serde_json::Deserializer::from_slice(&input[offset..offset + pivot]);
            let node = doc::HeapNode::from_serde(&mut deser, &alloc)?;

            let buf =
                rkyv::AlignedVec::with_capacity(alloc.allocated_bytes() - alloc.chunk_capacity());
            let mut buf = rkyv::ser::serializers::AllocSerializer::<4096>::new(
                rkyv::ser::serializers::AlignedSerializer::new(buf),
                Default::default(),
                Default::default(),
            );

            buf.serialize_value(&node)
                .expect("rkyv serialization cannot fail");
            alloc.reset();

            let mut buf = buf.into_serializer().into_inner();
            buf.shrink_to_fit();

            output.push((offset as u32, unsafe {
                doc::OwnedArchivedNode::new(buf.into_vec().into())
            }));

            offset += pivot;
        }
        input.drain(..offset);

        Ok(())
    }
}
