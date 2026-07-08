use super::ArchivedNode;

/// Portable little-endian u64, the alignment unit of rkyv archive buffers.
pub type U64Le = rkyv::rend::u64_le;

/// A pre-serialized `ArchivedNode` buffer, aligned to u64, stored on the heap
/// (typically in a bump allocator). Wraps the raw `&[U64Le]` representation,
/// using `U64Le` (little-endian u64) because rkyv archives use a consistent
/// LE representation across platforms.
///
/// The rkyv derive with `AsVec` produces `ArchivedEmbedded` containing an
/// `ArchivedVec<U64Le>`, writing the complete serialized buffer as opaque
/// sub-data.
#[derive(rkyv::Archive, rkyv::Serialize)]
#[rkyv(archived = ArchivedEmbedded)]
#[repr(transparent)]
pub struct HeapEmbedded<'a>(#[rkyv(with = rkyv::with::AsVec)] &'a [U64Le]);

impl<'a> HeapEmbedded<'a> {
    /// Build a HeapEmbedded from a backing buffer known to contain
    /// a valid rkyv archive of an `ArchivedNode`.
    ///
    /// Safety: the caller MUST have prior knowledge that `buffer` is a valid
    /// rkyv archive containing an `ArchivedNode`. If it's not, the result is
    /// undefined behavior.
    pub unsafe fn from_buffer(buffer: &'a [U64Le]) -> Self {
        HeapEmbedded(buffer)
    }

    /// Access the embedded `ArchivedNode`.
    pub fn get(&self) -> &ArchivedNode {
        unsafe { rkyv::access_unchecked::<ArchivedNode>(self.as_bytes()) }
    }

    /// Return the raw bytes backing this embedded document.
    pub fn as_bytes(&self) -> &[u8] {
        let s = self.0;
        unsafe { core::slice::from_raw_parts(s.as_ptr() as *const u8, s.len() * 8) }
    }

    /// Return the underlying `&[U64Le]` slice.
    pub fn as_u64le_slice(&self) -> &[U64Le] {
        self.0
    }
}

impl ArchivedEmbedded<'_> {
    /// Access the embedded `ArchivedNode`.
    pub fn get(&self) -> &ArchivedNode {
        unsafe { rkyv::access_unchecked::<ArchivedNode>(self.as_bytes()) }
    }

    /// Return the raw bytes backing this embedded document.
    pub fn as_bytes(&self) -> &[u8] {
        let s = self.0.as_slice();
        unsafe { core::slice::from_raw_parts(s.as_ptr() as *const u8, s.len() * 8) }
    }

    /// Return the underlying `&[U64Le]` slice.
    pub fn as_u64le_slice(&self) -> &[U64Le] {
        self.0.as_slice()
    }

    /// Promote the ArchivedEmbedded to a HeapEmbedded backed by the Allocator.
    pub fn to_heap<'alloc>(&self, alloc: &'alloc crate::Allocator) -> HeapEmbedded<'alloc> {
        let src = self.0.as_slice();
        HeapEmbedded(alloc.alloc_slice_copy(src))
    }
}

impl std::fmt::Debug for HeapEmbedded<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("HeapEmbedded")
            .field(&self.as_bytes().len())
            .finish()
    }
}

impl std::fmt::Debug for ArchivedEmbedded<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("ArchivedEmbedded")
            .field(&self.as_bytes().len())
            .finish()
    }
}
