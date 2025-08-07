use super::{ArchivedNode, HeapNode};
use std::sync::Arc;

/// OwnedNode is an enum over OwnedArchivedNode and OwnedHeapNode.
pub enum OwnedNode {
    Archived(OwnedArchivedNode),
    Heap(OwnedHeapNode),
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

pub struct OwnedHeapNode {
    node: HeapNode<'static>,
    _zz_alloc: Arc<bumpalo::Bump>,
}

impl OwnedHeapNode {
    /// Build an OwnedHeapNode around a HeapNode and its backing Bump allocator.
    /// The caller must ensure the HeapNode is entirely allocated within the argument Bump.
    pub unsafe fn new<'s>(node: HeapNode<'s>, alloc: Arc<bumpalo::Bump>) -> Self {
        // Safety: `node` is backed by `alloc`, which is an owned reference
        // to the Bump and is stored alongside `node`.
        let node = unsafe { std::mem::transmute::<HeapNode<'s>, HeapNode<'static>>(node) };

        Self {
            node,
            _zz_alloc: alloc,
        }
    }

    #[inline]
    pub fn get<'s>(&'s self) -> &'s HeapNode<'s> {
        &self.node
    }
}

// impl Drop to disallow destructuring of OwnedHeapNode,
// because that can separate the lifetimes of `node` and `_alloc`.
impl Drop for OwnedHeapNode {
    fn drop(&mut self) {}
}

// OwnedHeapNode is Send because we maintain a coupled lifetime between HeapNode
// and its backing allocator, and they're sent together.
unsafe impl Send for OwnedHeapNode {}
