use core::slice;
use rkyv::ser::Serializer;

mod ffi;

pub struct Out {
    v: rkyv::AlignedVec,
    header: usize,
}

impl Out {
    pub fn new() -> Self {
        Self::with_capacity(0)
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            v: rkyv::AlignedVec::with_capacity(capacity),
            header: 0,
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.v.len()
    }

    #[inline]
    pub fn iter<'s>(&'s self) -> IterOut<'s> {
        IterOut {
            v: self.v.as_slice(),
        }
    }

    #[inline]
    pub fn clear(&mut self) {
        self.v.clear()
    }

    /*
    pub fn into_owned_iter(self) -> impl Iterator<Item = (u32, doc::OwnedArchivedNode)> {
    }
    */

    #[inline]
    fn extend(&mut self, data: *const u8, len: usize) {
        let s = unsafe { slice::from_raw_parts(data, len) };
        self.v.extend_from_slice(s)
    }

    #[inline]
    fn begin(&mut self, source_offset: usize) {
        self.v
            .extend_from_slice(&(source_offset as u32).to_le_bytes());
        self.header = self.v.len();
        self.v.extend_from_slice(&[0; 4]);
    }

    #[inline]
    fn finish(&mut self) {
        let v = ((self.len() - self.header - 4) as u32).to_le_bytes();
        (&mut self.v[self.header..self.header + 4]).copy_from_slice(&v)
    }
}

pub struct IterOut<'s> {
    v: &'s [u8],
}

impl<'s> Iterator for IterOut<'s> {
    type Item = (u32, &'s [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        if self.v.is_empty() {
            return None;
        }

        let offset = u32::from_le_bytes(self.v[0..4].try_into().unwrap());
        let len = u32::from_le_bytes(self.v[4..8].try_into().unwrap()) as usize;
        let doc = &self.v[8..len + 8];

        self.v = &self.v[8 + len..];
        Some((offset, doc))
    }
}

pub struct Parser(cxx::UniquePtr<ffi::SimdParser>);

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
        output: &mut Out,
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
        output: &mut Out,
    ) -> Result<(), serde_json::Error> {
        let mut alloc = doc::Allocator::with_capacity(input.len());
        let mut offset = 0;

        while let Some(mut pivot) = memchr::memchr(b'\n', &input[offset..]) {
            pivot += 1;

            let mut deser = serde_json::Deserializer::from_slice(&input[offset..offset + pivot]);
            let node = doc::HeapNode::from_serde(&mut deser, &alloc)?;

            output.begin(offset);
            let mut ser = rkyv::ser::serializers::AllocSerializer::<512>::new(
                rkyv::ser::serializers::AlignedSerializer::new(std::mem::take(&mut output.v)),
                Default::default(),
                Default::default(),
            );

            ser.serialize_value(&node)
                .expect("rkyv serialization cannot fail");
            output.v = ser.into_serializer().into_inner();

            output.finish();
            alloc.reset();

            offset += pivot;
        }
        input.drain(..offset);

        Ok(())
    }
}
