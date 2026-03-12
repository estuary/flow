use super::{ArchivedNode, HeapEmbedded, HeapNode, HeapRoot};
use std::sync::Arc;

/// OwnedNode is an enum over OwnedArchivedNode and OwnedHeapRoot.
pub enum OwnedNode {
    Archived(OwnedArchivedNode),
    Heap(OwnedHeapRoot),
}

/// OwnedArchivedNode is an owned, aligned Bytes buffer holding an ArchivedNode.
pub struct OwnedArchivedNode(bytes::Bytes);

impl OwnedArchivedNode {
    /// Build an OwnedArchivedNode around a serialized buffer.
    /// The caller must ensure the buffer is aligned and a valid ArchivedNode.
    pub unsafe fn new(buf: bytes::Bytes) -> Self {
        const EXPECT_ALIGN: usize = core::mem::align_of::<ArchivedNode>();
        let actual_align = (buf.as_ptr() as usize) & (EXPECT_ALIGN - 1);

        assert_eq!(
            actual_align, 0,
            "ArchivedNode requires that buffers be aligned to {}",
            EXPECT_ALIGN
        );

        Self(buf)
    }

    #[inline]
    pub fn get<'s>(&'s self) -> &'s ArchivedNode {
        // Cast `backing` into its archived type.
        unsafe { rkyv::access_unchecked::<ArchivedNode>(&self.0) }
    }

    pub fn bytes(&self) -> &bytes::Bytes {
        &self.0
    }
}

/// OwnedHeapRoot wraps a HeapRoot (which may be a live HeapNode tree or a
/// HeapEmbedded archived buffer) together with its backing bump allocator.
pub struct OwnedHeapRoot {
    root: HeapRoot<'static>,
    _zz_alloc: Arc<bumpalo::Bump>,
}

impl OwnedHeapRoot {
    /// Build an OwnedHeapRoot around a HeapRoot and its backing Bump allocator.
    /// The caller must ensure the HeapRoot is entirely allocated within the
    /// argument Bump (both heap tree pointers and embedded buffer pointers).
    pub unsafe fn new<'s>(root: HeapRoot<'s>, alloc: Arc<bumpalo::Bump>) -> Self {
        // Safety: `root` is backed by `alloc`, which is an owned reference
        // to the Bump and is stored alongside `root`.
        let root = unsafe { std::mem::transmute::<HeapRoot<'s>, HeapRoot<'static>>(root) };

        Self {
            root,
            _zz_alloc: alloc,
        }
    }

    /// Dispatch to either a by-value HeapNode or a HeapEmbedded.
    #[inline]
    pub fn access<'s>(&'s self) -> Result<HeapNode<'s>, HeapEmbedded<'s>> {
        self.root.access()
    }
}

// impl Drop to disallow destructuring of OwnedHeapRoot,
// because that can separate the lifetimes of `root` and `_alloc`.
impl Drop for OwnedHeapRoot {
    fn drop(&mut self) {}
}

// OwnedHeapRoot is Send because we maintain a coupled lifetime between HeapRoot
// and its backing allocator, and they're sent together.
unsafe impl Send for OwnedHeapRoot {}
