use super::{heap::OwnedString, heap::SharedString, HeapNode};
use std::cell::UnsafeCell;
use std::collections::HashSet;

/// Deduper deduplicates the strings which occur within a document.
/// This deduplication reduces both the heap and archived serialization
/// size of the Doc, at the cost of indexing tables for deduplication
/// and also within a rkyv serialization pass.
/// For this reason, we always deduplicate object properties but we employ
/// selectively de-duplicate document strings.
pub struct Deduper<'alloc> {
    alloc: &'alloc bumpalo::Bump,
    // Safety: Deduper is not Sync and we never lend a reference to `table`.
    table: UnsafeCell<HashSet<SharedString<'alloc>>>,
}

impl<'alloc> HeapNode<'alloc> {
    // new_allocator builds a bumpalo::Bump allocator for use in building HeapNodes.
    // It's a trivial helper which can reduce type imports.
    pub fn new_allocator() -> bumpalo::Bump {
        bumpalo::Bump::new()
    }

    // new_allocator_with_capacity builds a bumpalo::Bump allocator for use in
    // building HeapNodes. It's a trivial helper which can reduce type imports.
    pub fn new_allocator_with_capacity(cap: usize) -> bumpalo::Bump {
        bumpalo::Bump::with_capacity(cap)
    }

    /// new_deduper builds a Deduper for use in the construction of HeapNode instances.
    /// It's merely a convenience for Deduper::new_in to reduce type imports.
    pub fn new_deduper(alloc: &'alloc bumpalo::Bump) -> Deduper<'alloc> {
        Deduper::new_in(alloc)
    }
}

impl<'alloc> Deduper<'alloc> {
    /// new_in builds a new Deduper over the given Bump allocator.
    pub fn new_in(alloc: &'alloc bumpalo::Bump) -> Self {
        Self {
            alloc,
            table: UnsafeCell::new(HashSet::new()),
        }
    }

    /// alloc_shared_string deduplicates its argument into a common ShardString.
    pub fn alloc_shared_string(&self, s: impl AsRef<str>) -> SharedString<'alloc> {
        let table = unsafe { &mut *self.table.get() };
        let s = s.as_ref();

        if let Some(ptr) = table.get(s) {
            return *ptr;
        }
        let s = SharedString(self.alloc.alloc_str(s));

        table.insert(s);
        s
    }

    /// alloc_string selects and returns a representation for it's argument:
    /// either as a de-duplicated string (Doc::SharedString)
    /// or an owned string (Doc::OwnedString).
    pub fn alloc_string(&self, s: impl AsRef<str>) -> HeapNode<'alloc> {
        let table = unsafe { &mut *self.table.get() };
        let s = s.as_ref();

        // If the string is large then it's likely (but not guaranteed) to be unique,
        // and we bypass the indexing overhead of interning it.
        //
        // If the string is small enough, it will be inlined by rkyv::ArchivedString
        // (the archival delegate of OwnedString) into the space that would otherwise
        // be used for an out-of-line pointer offset. rkyv::ArchivedRc<str> (the
        // delegate of SharedString) does not have this optimization, so in this case
        // a ShardString representation would actually cost us space in the archival.
        if s.len() > BIG_STRING_LIMIT || s.len() <= rkyv::string::repr::INLINE_CAPACITY {
            return HeapNode::StringOwned(OwnedString(self.alloc.alloc_str(s)));
        }

        if let Some(ptr) = table.get(s) {
            return HeapNode::StringShared(*ptr);
        }

        // This string is not interned and we're over our soft limit.
        if table.len() >= INTERN_TABLE_SOFT_LIMIT {
            return HeapNode::StringOwned(OwnedString(self.alloc.alloc_str(s)));
        }

        let s = SharedString(self.alloc.alloc_str(s));
        table.insert(s);

        HeapNode::StringShared(s)
    }
}

/// Upon the table reaching INTERN_TABLE_SOFT_LIMIT, a Deduper will no longer add new document strings.
/// Object properties will continue to be interned.
const INTERN_TABLE_SOFT_LIMIT: usize = 4096;
/// Strings larger than BIG_STRING_LIMIT will not be de-duplicated into the intern table,
/// as they are likely to be unique.
const BIG_STRING_LIMIT: usize = 128;

// SharedString must implement Borrow for HashSet lookups.
impl<'alloc> std::borrow::Borrow<str> for SharedString<'alloc> {
    fn borrow(&self) -> &str {
        self.0
    }
}
