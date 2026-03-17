# Embedded ArchivedNode Integration into doc::Combiner

## Background

The `shuffle` crate's log reader provides zero-copy access to documents as
`doc::ArchivedEmbedded` — pre-serialized rkyv archives backed by block buffers.
The `flowctl raw shuffle` harness currently reads these and serializes them to
NDJSON on stdout, but the next integration step is to load them into a
`doc::Combiner` for reduction, then drain combined results per transaction.

Today, feeding a shuffle document into the Combiner requires deserializing the
`ArchivedNode` into a `HeapNode` tree (`HeapNode::from_node`), which allocates
many small nodes in the bump allocator. In the common case — documents that pass
through without key overlap — this deserialization is pure waste. The document
enters as a compact archived buffer and could be spilled or drained as-is.

## Goal

Allow `HeapEntry` to carry _either_ a live `HeapNode` tree or an opaque
`ArchivedEmbedded` buffer. In the fast path (no key overlap, no reduction),
the archived bytes flow through the Combiner untouched: no deserialization,
no validation, no redaction, no re-serialization on spill. Only when reduction
is required do we promote to `HeapNode`.

## Key Types

### HeapRoot

A flattened enum replacing `HeapNode` inside `HeapEntry`. It has all the same
variants as `HeapNode`, plus one additional variant:

```rust
pub enum HeapRoot<'alloc> {
    Array(i32, BumpVec<'alloc, HeapNode<'alloc>>),
    Bool(bool),
    Bytes(BumpVec<'alloc, u8>),
    Float(f64),
    NegInt(i64),
    Null,
    Object(i32, BumpVec<'alloc, HeapField<'alloc>>),
    PosInt(u64),
    String(BumpStr<'alloc>),
    Embedded(*const U64Le, u32),  // pointer + length in U64Le units
}
```

**Size**: 16 bytes, same as `HeapNode`. The `Embedded` payload is 12 bytes
(8-byte pointer + 4-byte count), equal to the largest existing variants
(`Array` and `Object` at `i32` + `BumpVec` = 12 bytes). `HeapEntry` remains
32 bytes — two entries per cache line.

The flattened design is necessary. A wrapping approach
(`Heap(HeapNode)` / `Embedded(...)`) would inflate to 24 bytes because
`HeapNode` has no usable niche for enum optimization.

The pointer is `*const U64Le` (not `*const u8`) to enforce 8-byte alignment
at the type level and match `HeapEmbedded`'s `&[U64Le]` slice representation.
The count is in `U64Le` units, giving a 32 GiB maximum per document.

### HeapRoot::access()

```rust
fn access<'a>(&'a self) -> Result<HeapNode<'alloc>, HeapEmbedded<'a>>
```

Returns `HeapNode` **by value** (a 16-byte bit-copy of handle data — pointers
into the bump allocator) or `HeapEmbedded` **by value** (reconstructed from the
raw pointer and length). Cannot return `&HeapNode` by reference because
`HeapRoot` is a different enum with different discriminants.

The by-value `HeapNode` is a shallow copy of handles (`BumpStr`, `BumpVec`),
not the underlying tree. This is the same pattern `LazyNode::borrow` uses via
`transmute_copy`. The copied `HeapNode` still points into the bump allocator.

`HeapEmbedded` provides:
- `.as_bytes() -> &[u8]` for direct byte copying (spill path)
- `.get() -> &ArchivedNode` for tree access (validation, comparison, serialization)

### OwnedHeapRoot

Replaces `OwnedHeapNode`. Wraps `HeapRoot` + `Arc<Bump>`, same safety invariants:
the `Arc<Bump>` keeps the allocator alive, and both heap tree pointers and
embedded buffer pointers are allocated within it.

```rust
pub struct OwnedHeapRoot {
    root: HeapRoot<'static>,
    _zz_alloc: Arc<bumpalo::Bump>,
}
```

Callers use `access()` to dispatch. `OwnedNode::Heap(OwnedHeapRoot)` — no new
variant needed.

Needs `Drop` impl (to prevent destructuring, same as current `OwnedHeapNode`)
and `unsafe impl Send`.

## New MemTable Add Path

A new method on `MemTable`, similar to `add()`, for documents arriving from
the shuffle reader:

```rust
pub fn add_embedded<'s>(
    &'s self,
    binding: u16,
    packed_key_prefix: &[u8; 16],
    valid: bool,
    embedded: &ArchivedEmbedded<'_>,
) -> Result<(), Error>
```

Behavior:
1. **Fail fast on invalid**: if `!valid`, run full validation immediately to
   produce and return the rich `FailedValidation` error. The shuffle pipeline's
   `flags & 0x0001` provides the valid bit.
2. **Build Meta from packed key prefix directly**: the shuffle's
   `packed_key_prefix` (16 bytes) is the same encoding produced by
   `Extractor::extract_all` in the slice actor. Take the first 13 bytes for
   `Meta`, skipping key re-extraction from the document entirely.
3. **Copy archived bytes into bump allocator**:
   ```rust
   let src: &[U64Le] = archived_embedded.0.as_slice();
   let dst = alloc.alloc_slice_copy(src);
   HeapRoot::Embedded(dst.as_ptr(), dst.len() as u32)
   ```
4. Push `HeapEntry { meta, root }` and trigger compaction if needed.

**Key encoding compatibility**: the shuffle slice actor and the Combiner's
`Spec.keys[binding]` must use identical `Extractor` instances and `extract_all`
encoding. Both derive from the collection's key pointers, so they should match.
Worth a debug assertion.

## Compact Path

Compact works over `HeapRoot` instead of `HeapNode`. Changes:

- **`sort_ord`**: calls `Extractor::compare_key`, which needs `AsNode`. For
  Embedded, dispatch through `ArchivedNode` via `access()`. This is cold —
  `Meta`'s packed prefix resolves ordering for the vast majority of comparisons.

- **`maybe_reduce`**: validate and reduce via `access()`:
  ```rust
  let lazy = match entry.root.access() {
      Ok(heap_node) => LazyNode::Heap(&heap_node),   // stack copy
      Err(embedded) => LazyNode::Node(embedded.get()), // &ArchivedNode
  };
  ```
  Validation runs on `ArchivedNode` (implements `AsNode`), producing reduction
  annotations. `reduce::reduce` returns `HeapNode<'alloc>`, so the result
  becomes a heap variant — Embedded entries are promoted through reduction.

## Spill Path

### Validation/Redaction Loop (memtable.rs)

The pre-spill loop validates and redacts `!front()` entries. Embedded entries
skip, matching on the variant:

```rust
for doc in sorted.iter_mut() {
    if doc.meta.front() || matches!(doc.root, HeapRoot::Embedded(..)) {
        continue;
    }
    // validate and redact...
}
```

Rationale: Embedded entries were already validated by the shuffle pipeline and
don't require redaction (they originate from source journals, not user input).

### SpillWriter::write_segment

The hot serialization loop branches on the variant:

```rust
match root {
    HeapRoot::Embedded(ptr, len_u64s) => {
        let embedded = unsafe {
            HeapEmbedded::from_buffer(
                std::slice::from_raw_parts(ptr, len_u64s as usize)
            )
        };
        raw_buf.extend_from_slice(embedded.as_bytes());
    }
    _ => {
        // Existing rkyv serialization path for HeapNode variants.
        raw_buf = rkyv::api::low::to_bytes_in_with_alloc(...);
    }
}
```

The entry header format is unchanged (Meta bytes + doc_len). Only the document
body differs in how it's produced. The spill read-back side (`Entry::parse`,
`SpillDrainer`) requires no changes — the bytes are a valid rkyv archive
regardless of how they were produced.

**Alignment**: the entry header is 24 bytes (3x8), so the document body starts
at an 8-byte-aligned offset within the `AlignedVec<8>`. On read-back,
`OwnedArchivedNode::new` asserts 8-byte alignment, which holds. Worth a debug
assertion.

## MemDrainer Path

Same optimizations as the spill path apply to `MemDrainer::drain_next`:

- **Unreduced Embedded entries** skip validation and redaction (match on variant)
- Survive as `OwnedHeapRoot` with the `Embedded` variant, wrapped in
  `OwnedNode::Heap(owned_heap_root)`
- Downstream consumers call `access()` to dispatch for serialization

- **Reduced entries** are `HeapNode` (reduction output), follow the existing
  validate/redact/wrap path

## Downstream Consumer Changes

All consumers of `OwnedNode` currently dispatch on `Heap` vs `Archived`:

```rust
match &owned_node {
    OwnedNode::Heap(n) => do_thing(n.get()),      // &HeapNode
    OwnedNode::Archived(n) => do_thing(n.get()),   // &ArchivedNode
}
```

With `OwnedHeapRoot`, the `Heap` arm calls `access()`:

```rust
OwnedNode::Heap(n) => match n.access() {
    Ok(heap_node) => do_thing(&heap_node),
    Err(embedded) => do_thing(embedded.get()),
}
```

Affected call sites (all doing serialization via `SerPolicy::on_owned`):
- `runtime::combine::protocol` — serialize published docs
- `runtime::derive::protocol` — serialize derived docs
- `runtime::materialize::protocol` — serialize stored docs
- `runtime::capture::protocol` — serialize captured docs
- `doc::ser::SerOwned` — the serialization dispatch itself
- `doc::extractor::extract_all_owned` — key extraction from owned nodes
- `doc::shape::widen_owned` — shape widening

One exception: `runtime::rocksdb.rs:485` destructures `OwnedNode::Heap` and
expects `HeapNode::Array`. This is an internal-only MemTable (RocksDB checkpoint
merging), never fed from the shuffle pipeline, so Embedded entries won't appear.
It would call `access().unwrap()`.

## Summary of the Fast Path

An `ArchivedEmbedded` from the shuffle reader that has no key overlap:

1. **Add**: copy `&[U64Le]` into bump allocator, build `Meta` from packed prefix.
   No key extraction, no validation, no deserialization.
2. **Compact**: sorted and merged by `Meta` prefix (no full key comparison needed
   if prefix is unique). No validation, no reduction.
3. **Spill**: skip validation/redaction loop. `extend_from_slice` the archived
   bytes directly into the raw buffer. No rkyv serialization.
4. **Or Drain** (no spill): skip validation/redaction, wrap as `OwnedHeapRoot`,
   serialize via `ArchivedNode` on output.

The only work done on the document bytes is two memcpys: once into the bump
allocator on add, once into the spill buffer (or output serialization) on drain.
