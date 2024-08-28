/// Transcoded is a sequence of ArchiveNode documents
/// transcoded from an input JSON buffer.
pub struct Transcoded {
    /// Begin input offset of this Transcoded instance.
    pub offset: i64,

    pub(crate) v: rkyv::AlignedVec,
}

impl Transcoded {
    pub fn is_empty(&self) -> bool {
        self.v.is_empty()
    }

    pub fn iter<'s>(&'s self) -> IterOut<'s> {
        IterOut {
            v: self.v.as_slice(),
            offset: self.offset,
        }
    }

    pub fn into_iter(mut self) -> OwnedIterOut {
        // `v` won't be re-used. Release as much excess capacity as possible.
        self.v.shrink_to_fit();

        OwnedIterOut {
            v: self.v.into_vec().into(),
            offset: self.offset,
        }
    }

    pub fn into_inner(self) -> rkyv::AlignedVec {
        self.v
    }
}

pub struct IterOut<'s> {
    v: &'s [u8],
    offset: i64,
}

impl<'s> Iterator for IterOut<'s> {
    type Item = (&'s [u8], i64);

    fn next(&mut self) -> Option<Self::Item> {
        if self.v.is_empty() {
            return None;
        }

        let next_offset = self.offset + u32::from_le_bytes(self.v[0..4].try_into().unwrap()) as i64;
        let len = u32::from_le_bytes(self.v[4..8].try_into().unwrap()) as usize;
        let doc = &self.v[8..len + 8];

        self.v = &self.v[8 + len..];
        Some((doc, next_offset))
    }
}

pub struct OwnedIterOut {
    v: bytes::Bytes,
    offset: i64,
}

impl OwnedIterOut {
    pub fn empty() -> Self {
        Self {
            v: bytes::Bytes::new(),
            offset: 0,
        }
    }
}

impl Iterator for OwnedIterOut {
    type Item = (doc::OwnedArchivedNode, i64);

    fn next(&mut self) -> Option<Self::Item> {
        use bytes::Buf;

        if self.v.is_empty() {
            return None;
        }

        let next_offset = self.offset + u32::from_le_bytes(self.v[0..4].try_into().unwrap()) as i64;
        let len = u32::from_le_bytes(self.v[4..8].try_into().unwrap()) as usize;

        self.v.advance(8);
        let doc = self.v.split_to(len);

        Some((unsafe { doc::OwnedArchivedNode::new(doc) }, next_offset))
    }
}
