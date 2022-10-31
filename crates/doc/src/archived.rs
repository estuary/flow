use super::{heap, AsNode, Field, Fields, HeapNode, Node};

use rkyv::ser::Serializer;

// `rkyv` generates types that mirror the 'alloc lifetime parameter,
// but this lifetime has no meaning (as far as I can tell).
// The only meaningful lifetime for ArchiveDoc is that its references
// &ArchiveDoc live no longer than its backing buffer.
pub type ArchivedDoc = heap::ArchivedDoc<'static>;
pub type ArchivedField = heap::ArchivedField<'static>;
pub type ArchivedNode = heap::ArchivedNode<'static>;

impl<'alloc> HeapNode<'alloc> {
    /// to_archive serializes a HeapNode into an aligned, heap-allocated buffer.
    /// This function is a convenience for the common "just serialize it, please?" case.
    /// Feel free to use your own rkyv::Serializer and view this implementation as mere guidance.
    pub fn to_archive(&self) -> rkyv::AlignedVec {
        let mut serializer = rkyv::ser::serializers::AllocSerializer::<4096>::default();
        serializer.serialize_value(self).unwrap();
        serializer.into_serializer().into_inner()
    }
}

impl ArchivedNode {
    // from_archive casts the given (aligned) byte buffer to an ArchivedNode,
    // without any copy or deserialization.
    pub fn from_archive<'buf>(buf: &'buf [u8]) -> &'buf Self {
        let expect_align = core::mem::align_of::<ArchivedNode>();
        let actual_align = (buf.as_ptr() as usize) & (expect_align - 1);

        assert_eq!(
            actual_align, 0,
            "from_buffer requires that buffers be aligned to {}",
            expect_align
        );
        unsafe { rkyv::archived_root::<HeapNode>(buf) }
    }
}

impl<'alloc> rkyv::Archive for heap::SharedString<'alloc> {
    type Archived =
        rkyv::rc::ArchivedRc<<str as rkyv::ArchiveUnsized>::Archived, SharedStringFlavor>;
    type Resolver = rkyv::rc::RcResolver<<str as rkyv::ArchiveUnsized>::MetadataResolver>;

    unsafe fn resolve(&self, pos: usize, resolver: Self::Resolver, out: *mut Self::Archived) {
        Self::Archived::resolve_from_ref(self.0, pos, resolver, out);
    }
}
pub struct SharedStringFlavor;

impl<'alloc, S> rkyv::Serialize<S> for heap::SharedString<'alloc>
where
    S: rkyv::ser::Serializer + rkyv::ser::SharedSerializeRegistry + ?Sized,
{
    #[inline]
    fn serialize(&self, serializer: &mut S) -> Result<Self::Resolver, S::Error> {
        Self::Archived::serialize_from_ref(self.0, serializer)
    }
}

impl<'alloc> rkyv::Archive for heap::OwnedString<'alloc> {
    type Archived = rkyv::string::ArchivedString;
    type Resolver = rkyv::string::StringResolver;

    unsafe fn resolve(&self, pos: usize, resolver: Self::Resolver, out: *mut Self::Archived) {
        Self::Archived::resolve_from_str(self.0, pos, resolver, out);
    }
}

impl<'alloc, S> rkyv::Serialize<S> for heap::OwnedString<'alloc>
where
    S: rkyv::ser::Serializer + rkyv::ser::SharedSerializeRegistry + ?Sized,
{
    #[inline]
    fn serialize(&self, serializer: &mut S) -> Result<Self::Resolver, S::Error> {
        Self::Archived::serialize_from_str(self.0, serializer)
    }
}

impl<'alloc, T: rkyv::Archive> rkyv::Archive for heap::BumpVec<'alloc, T>
where
    T: rkyv::Archive + std::fmt::Debug,
{
    type Archived = rkyv::vec::ArchivedVec<T::Archived>;
    type Resolver = rkyv::vec::VecResolver;

    #[inline]
    unsafe fn resolve(&self, pos: usize, resolver: Self::Resolver, out: *mut Self::Archived) {
        Self::Archived::resolve_from_slice(self.0.as_slice(), pos, resolver, out);
    }
}

impl<'alloc, S, T> rkyv::Serialize<S> for heap::BumpVec<'alloc, T>
where
    S: rkyv::ser::Serializer
        + rkyv::ser::ScratchSpace
        + rkyv::ser::SharedSerializeRegistry
        + ?Sized,
    T: rkyv::Serialize<S> + std::fmt::Debug,
{
    #[inline]
    fn serialize(&self, serializer: &mut S) -> Result<Self::Resolver, S::Error> {
        Self::Archived::serialize_from_slice(self.0.as_slice(), serializer)
    }
}

impl AsNode for ArchivedNode {
    type Fields = [ArchivedField];

    fn as_node<'a>(&'a self) -> Node<'a, Self> {
        match self {
            ArchivedNode::Array(a) => Node::Array(a.as_slice()),
            ArchivedNode::Bool(b) => Node::Bool(*b),
            ArchivedNode::Bytes(b) => Node::Bytes(b),
            ArchivedNode::Float(n) => Node::Number(json::Number::Float(n.value())),
            ArchivedNode::NegInt(n) => Node::Number(json::Number::Signed(n.value())),
            ArchivedNode::Null => Node::Null,
            ArchivedNode::Object(o) => Node::Object(o.as_slice()),
            ArchivedNode::PosInt(n) => Node::Number(json::Number::Unsigned(n.value())),
            ArchivedNode::StringOwned(s) => Node::String(s),
            ArchivedNode::StringShared(s) => Node::String(s),
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

    fn len(&self) -> usize {
        <[ArchivedField]>::len(self)
    }

    fn iter<'a>(&'a self) -> Self::Iter<'a> {
        <[ArchivedField]>::iter(self)
    }
}

impl<'a> Field<'a, ArchivedNode> for &'a ArchivedField {
    fn property(&self) -> &'a str {
        &self.property
    }
    fn value(&self) -> &'a ArchivedNode {
        &self.value
    }
}
