use super::{heap, AsNode, BumpStr, BumpVec, Field, Fields, HeapNode, Node};

// `rkyv` generates types that mirror the 'alloc lifetime parameter,
// but this lifetime has no meaning (as far as I can tell).
// The only meaningful lifetime for ArchiveNode is that its
// references &ArchiveNode must live no longer than its backing buffer.
pub type ArchivedField = heap::ArchivedField<'static>;
pub type ArchivedNode = heap::ArchivedNode<'static>;

impl<'alloc> HeapNode<'alloc> {
    /// to_archive serializes a HeapNode into an aligned, heap-allocated buffer.
    /// This function is a convenience for the common "just serialize it, please?" case.
    /// Feel free to use your own rkyv::Serializer and view this implementation as mere guidance.
    pub fn to_archive(&self) -> rkyv::util::AlignedVec<16> {
        rkyv::to_bytes::<rkyv::rancor::Error>(self).unwrap()
    }
}

impl ArchivedNode {
    // from_archive casts the given (aligned) byte buffer to an ArchivedNode,
    // without any copy or deserialization.
    pub fn from_archive<'buf>(buf: &'buf [u8]) -> &'buf Self {
        const EXPECT_ALIGN: usize = core::mem::align_of::<ArchivedNode>();
        let actual_align = (buf.as_ptr() as usize) & (EXPECT_ALIGN - 1);

        assert_eq!(
            actual_align, 0,
            "from_buffer requires that buffers be aligned to {}",
            EXPECT_ALIGN
        );
        unsafe { rkyv::access_unchecked::<ArchivedNode>(buf) }
    }
}

impl<'alloc> rkyv::Archive for BumpStr<'alloc> {
    type Archived = rkyv::string::ArchivedString;
    type Resolver = rkyv::string::StringResolver;

    fn resolve(&self, resolver: Self::Resolver, out: rkyv::Place<Self::Archived>) {
        Self::Archived::resolve_from_str(self, resolver, out);
    }
}

impl<'alloc, S> rkyv::Serialize<S> for BumpStr<'alloc>
where
    S: rkyv::ser::Allocator + rkyv::ser::Writer + rkyv::rancor::Fallible + ?Sized,
    <S as rkyv::rancor::Fallible>::Error: rkyv::rancor::Source,
{
    #[inline]
    fn serialize(
        &self,
        serializer: &mut S,
    ) -> Result<Self::Resolver, <S as rkyv::rancor::Fallible>::Error> {
        Self::Archived::serialize_from_str(self, serializer)
    }
}

impl<'alloc, T: rkyv::Archive> rkyv::Archive for BumpVec<'alloc, T>
where
    T: rkyv::Archive + std::fmt::Debug,
{
    type Archived = rkyv::vec::ArchivedVec<T::Archived>;
    type Resolver = rkyv::vec::VecResolver;

    #[inline]
    fn resolve(&self, resolver: Self::Resolver, out: rkyv::Place<Self::Archived>) {
        Self::Archived::resolve_from_slice(self, resolver, out);
    }
}

impl<'alloc, S, T> rkyv::Serialize<S> for BumpVec<'alloc, T>
where
    S: rkyv::ser::Allocator + rkyv::ser::Writer + rkyv::rancor::Fallible + ?Sized,
    <S as rkyv::rancor::Fallible>::Error: rkyv::rancor::Source,
    T: rkyv::Serialize<S> + std::fmt::Debug,
{
    #[inline]
    fn serialize(
        &self,
        serializer: &mut S,
    ) -> Result<Self::Resolver, <S as rkyv::rancor::Fallible>::Error> {
        Self::Archived::serialize_from_slice(self, serializer)
    }
}

impl AsNode for ArchivedNode {
    type Fields = [ArchivedField];

    // We *always* want this inline, because the caller will next match
    // over our returned Node, and (when inline'd) the optimizer can
    // collapse the chained `match` blocks into one.
    #[inline(always)]
    fn as_node<'a>(&'a self) -> Node<'a, Self> {
        match self {
            ArchivedNode::Array(a) => Node::Array(a.as_slice()),
            ArchivedNode::Bool(b) => Node::Bool(*b),
            ArchivedNode::Bytes(b) => Node::Bytes(b),
            ArchivedNode::Float(n) => Node::Float(n.to_native()),
            ArchivedNode::NegInt(n) => Node::NegInt(n.to_native()),
            ArchivedNode::Null => Node::Null,
            ArchivedNode::Object(o) => Node::Object(o.as_slice()),
            ArchivedNode::PosInt(n) => Node::PosInt(n.to_native()),
            ArchivedNode::String(s) => Node::String(s),
        }
    }
}

impl Fields<ArchivedNode> for [ArchivedField] {
    type Field<'a> = &'a ArchivedField;
    type Iter<'a> = std::slice::Iter<'a, ArchivedField>;

    fn get<'a>(&'a self, property: &str) -> Option<Self::Field<'a>> {
        match self.binary_search_by(|l| l.property.as_ref().cmp(property)) {
            Ok(ind) => Some(&self[ind]),
            Err(_) => None,
        }
    }

    #[inline]
    fn len(&self) -> usize {
        <[ArchivedField]>::len(self)
    }
    #[inline]
    fn iter<'a>(&'a self) -> Self::Iter<'a> {
        <[ArchivedField]>::iter(self)
    }
}

impl<'a> Field<'a, ArchivedNode> for &'a ArchivedField {
    #[inline(always)]
    fn property(&self) -> &'a str {
        &self.property
    }
    #[inline(always)]
    fn value(&self) -> &'a ArchivedNode {
        &self.value
    }
}
