use super::{DrainedDoc, Error, HeapEntry, Meta, Spec, SpillWriter};
use crate::{
    Encoding, Extractor, HeapEmbedded, HeapNode, HeapRoot, LazyNode, OwnedHeapRoot, OwnedNode,
    redact, reduce, validation,
};
use bumpalo::Bump;
use std::cell::UnsafeCell;
use std::cmp::Ordering;
use std::io;
use std::sync::Arc;

/// MemTable is an in-memory combiner of HeapDocs.
/// It requires heap memory for storing and reducing documents.
/// After some threshold amount of allocation, a MemTable should
/// be spilled into SpillWriter.
pub struct MemTable {
    // Careful! Order matters. We must drop all the usages of `zz_alloc`
    // before we drop the allocator itself. Note that Rust drops struct fields
    // in declaration order, which we rely upon here:
    // https://doc.rust-lang.org/reference/destructors.html#destructors
    //
    // Safety: MemTable never lends a reference to `entries`.
    entries: UnsafeCell<Entries>,
    zz_alloc: Bump,
}

// Safety: MemTable is safe to Send because `entries` never has lent references,
// and we're sending `entries` and its corresponding Bump allocator together.
// It would _not_ be safe to separately send `entries` and the allocator,
// and so we do not do that.
unsafe impl Send for MemTable {}

struct Entries {
    // Queued documents are in any order.
    queued: Vec<HeapEntry<'static>>,
    // Sorted documents ordered on (binding, key, !front) so that
    // for each binding and key, front() documents are first.
    sorted: Vec<HeapEntry<'static>>,
    // Specification of the combine operation.
    spec: Spec,
    // Scratch space for extracting key tuples.
    scratch: bytes::BytesMut,
}

impl Entries {
    fn should_compact(&mut self) -> bool {
        // Rationale for these heuristics:
        //
        // * In the common case where every key is unique, we want each successive
        //   compaction to have equal numbers of sorted and queued documents,
        //   such that the number of output documents doubles each time.
        //   This amortizes the cost of sorting and merging, much like an LSM tree.
        //
        // * In cases where there _is_ a lot of reduction, it's often inefficient
        //   to reduce a small document into a much larger document. Instead we
        //   want to take advantage of the associative property of reductions,
        //   and first reduce a bunch of small documents together before we reduce
        //   their (larger) combination into a (still larger) left-hand document.
        //
        // So, seek to double the number of output documents each time, assuming
        // keys are unique -- and if they aren't that's fine. In the worst case
        // of just one key which is reduced over and over, ensure we're combining
        // over a bunch of (small) right-hand documents before combining into its
        // value in `self.sorted`.
        self.queued.len() >= std::cmp::max(32, self.sorted.len())
    }

    fn compact(&mut self, alloc: &'static Bump) -> Result<(), Error> {
        // `sort_ord` orders over (binding, key, stale, !front):
        // For each (binding, key), stale entries sort first, then front()
        // entries, and we further rely on sort preserving the order in which
        // entries were added. This maintains the left-to-right associative
        // ordering of reductions. Stale is a group boundary, so compaction
        // never reduces a stale entry with a fresh one.
        //
        // `meta` contains a packed structure that's order-preserving over
        // (binding, key), so we first test it for inequality.
        let sort_ord = |l: &HeapEntry, r: &HeapEntry| -> Ordering {
            (l.meta.0)
                .cmp(&r.meta.0)
                .then_with(|| {
                    // Cold path: Meta prefix was equal, so compare the full key.
                    compare_root_keys(&self.spec.keys[l.meta.binding()], &l.root, &r.root)
                })
                .then_with(|| l.meta.stale().cmp(&r.meta.stale()).reverse())
                .then_with(|| l.meta.front().cmp(&r.meta.front()).reverse())
        };
        let validators = &mut self.spec.validators;

        // Closure which attempts an associative reduction of `index` into `index-1`.
        // If the reduction succeeds then the item at `index` is removed.
        let mut maybe_reduce = |next: &mut Vec<HeapEntry<'_>>, index: usize| -> Result<(), Error> {
            // Stale content is known-dead: never validate or reduce it. Groups
            // are uniformly stale or fresh (per `sort_ord`), so one test suffices.
            if next[index].meta.stale() {
                return Ok(());
            }
            let rhs = &next[index];

            let rhs_outcomes = validate_root(
                &rhs.root,
                &mut validators[rhs.meta.binding()],
                &self.spec.names[rhs.meta.binding()],
                validation::reduce_filter,
            )?;

            let (lhs, rhs) = (&next[index - 1], &next[index]);

            match reduce_roots(
                &lhs.root,
                &rhs.root,
                &rhs_outcomes,
                alloc,
                false, // Compactions are always associative.
            ) {
                Ok((root, _deleted)) => {
                    next[index - 1].root = HeapRoot::from_heap_node(root);
                    next[index - 1].meta.set_known_valid(false); // Must re-validate.
                    next.remove(index);
                    Ok(())
                }
                Err(reduce::Error::NotAssociative) => {
                    next[index - 1].meta.set_not_associative();
                    Ok(())
                }
                Err(err) => Err(Error::Reduce(err)),
            }
        };

        // Sort queued documents. It's important that this be a stable sort
        // to preserve overall left-to-right application order.
        self.queued.sort_by(sort_ord);

        let sorted_len = self.sorted.len();
        let queued_len = self.queued.len();

        let mut next = Vec::with_capacity(sorted_len + queued_len);
        let mut queued = self.queued.drain(..).peekable();
        let mut sorted = self.sorted.drain(..).peekable();

        // `begin` offset of the first in a run of `sort_ord`-equal documents.
        // Note: next.len() - begin is the number of documents in the group.
        let mut begin = 0;
        // `begin_queued` offset of the first queued document in this group.
        // If there are no queued documents, it's next.len().
        let mut begin_queued = 0;

        // We attempt associative reductions to compact down to two documents
        // for each group. We must hold back a reduction into the left-most
        // document because we don't know if it's truly the left-most document
        // for the group, or if there's yet another document out there that we
        // might encounter later.
        //
        // This loop also does extra book-keeping to defer a reduction of
        // `begin_queued` into a prior entry from `sorted` within each group.
        // We hold it back, preferring to instead reduce a second, third,
        // ... N queued document into `begin_queued` first, and only then
        // reducing `begin_queued` into a left-hand entry drawn from `sorted`.
        // This is because `queued` documents are often much smaller and
        // faster to validate, and it's more efficient to reduce a bunch of
        // them together and only then validate & reduce _that_ result into
        // an often-larger prior entry from `sorted` .

        loop {
            // Pop sort_ord() HeapEntry. When equal, take from `sorted` to preserve order.
            let (is_queued, entry) = match (sorted.peek(), queued.peek()) {
                (None, Some(_)) => (true, queued.next().unwrap()),
                (Some(_), None) => (false, sorted.next().unwrap()),
                (Some(l), Some(c)) => {
                    if sort_ord(l, c).is_le() {
                        (false, sorted.next().unwrap())
                    } else {
                        (true, queued.next().unwrap())
                    }
                }
                (None, None) => break,
            };

            // Does `entry` start a new group?
            if !matches!(next.last(), Some(last) if sort_ord(&entry, last).is_eq()) {
                // If we held back an eligible compaction of `begin_queued`, do it now.
                if begin_queued != next.len() && begin_queued - begin > 1 {
                    maybe_reduce(&mut next, begin_queued)?;
                }

                // Reset for this next group.
                (begin, begin_queued) = (next.len(), next.len());
            }

            let index = next.len();
            next.push(entry);

            if !is_queued {
                // `entry` is from `sorted` and is already reduced.
                begin_queued = next.len();
            } else if index != begin_queued && index - begin > 1 {
                // Reduce if `entry` is not `begin_queued` (which is held back)
                // and we have more than one other group document.
                maybe_reduce(&mut next, index)?;
            }
        }

        // Apply deferred reduction of `begin_queued` of the final group.
        if begin_queued != next.len() && begin_queued - begin > 1 {
            maybe_reduce(&mut next, begin_queued)?;
        }

        std::mem::drop(sorted);
        std::mem::drop(queued);
        self.sorted = next;

        tracing::trace!(
            %queued_len,
            %sorted_len,
            next_len = %self.sorted.len(),
            "compacted entries",
        );

        Ok(())
    }
}

impl MemTable {
    pub fn new(spec: Spec) -> Self {
        Self {
            entries: UnsafeCell::new(Entries {
                queued: Vec::new(),
                sorted: Vec::new(),
                spec,
                scratch: bytes::BytesMut::new(),
            }),
            zz_alloc: HeapNode::new_allocator(),
        }
    }

    /// Alloc returns the bump allocator of this MemTable.
    /// Its exposed to allow callers to allocate HeapNode structures
    /// having this MemTable's lifetime, which it can later combine or reduce.
    pub fn alloc(&self) -> &Bump {
        &self.zz_alloc
    }

    /// Add the document to the MemTable.
    pub fn add<'s>(&'s self, binding: u16, root: HeapNode<'s>, front: bool) -> Result<(), Error> {
        self.add_inner(binding, root, front, false)
    }

    /// Add a Loaded document the caller has classified as stale. Like `add` with
    /// `front=true`, but flagged stale: its value is never reduced or emitted,
    /// while its existence transfers onto the fresh entry of the same
    /// (binding, key). See [`super::Accumulator::truncate`].
    pub fn add_stale_front<'s>(&'s self, binding: u16, root: HeapNode<'s>) -> Result<(), Error> {
        self.add_inner(binding, root, true, true)
    }

    fn add_inner<'s>(
        &'s self,
        binding: u16,
        root: HeapNode<'s>,
        front: bool,
        stale: bool,
    ) -> Result<(), Error> {
        // Safety: mutable borrow does not escape this function.
        let entries = unsafe { &mut *self.entries.get() };
        let root = unsafe { std::mem::transmute::<HeapNode<'s>, HeapNode<'static>>(root) };

        () = Extractor::extract_all(
            &root,
            &entries.spec.keys[binding as usize],
            Encoding::Packed,
            &mut entries.scratch,
            None,
        );
        let mut meta = Meta::new(
            binding,
            &entries.scratch,
            front,
            false, // `known_valid`
        );
        if stale {
            meta.set_stale();
        }

        entries.queued.push(HeapEntry {
            meta,
            root: HeapRoot::from_heap_node(root),
        });
        entries.scratch.clear();

        if entries.should_compact() {
            self.compact()
        } else {
            Ok(())
        }
    }

    /// Truncate `binding` within this MemTable in a single pass: drop its
    /// `!front()` entries (pre-boundary sources carry no existence) and flag its
    /// `front()` entries stale in place (value dead, but the body is retained so
    /// drain can match its key against fresh entries). Other bindings untouched.
    pub fn truncate(&self, binding: u16) {
        // Safety: mutable borrow does not escape this function.
        let entries = unsafe { &mut *self.entries.get() };

        let retain = |entry: &mut HeapEntry| -> bool {
            if entry.meta.binding() != binding as usize {
                return true;
            } else if !entry.meta.front() {
                return false;
            }
            entry.meta.set_stale();
            true
        };

        // `sorted` stays sorted: `retain_mut` preserves order, and the binding's
        // survivors are homogeneous stale fronts, which sort ahead of fresh.
        entries.queued.retain_mut(retain);
        entries.sorted.retain_mut(retain);
    }

    /// Add a pre-serialized ArchivedEmbedded document from the shuffle reader.
    /// The packed_key_prefix is the first 16 bytes of the packed key of the document.
    pub fn add_embedded<'s>(
        &'s self,
        binding: u16,
        packed_key_prefix: &[u8; 16],
        embedded: HeapEmbedded<'s>,
        front: bool,
        known_valid: bool,
    ) -> Result<(), Error> {
        // Safety: mutable borrow does not escape this function.
        let entries = unsafe { &mut *self.entries.get() };

        // Debug assertion: verify packed key prefix matches what we'd extract.
        #[cfg(debug_assertions)]
        {
            let keys = &entries.spec.keys[binding as usize];
            Extractor::extract_all(
                embedded.get(),
                keys,
                Encoding::Packed,
                &mut entries.scratch,
                None,
            );
            debug_assert!(
                entries
                    .scratch
                    .starts_with(&packed_key_prefix[..13.min(entries.scratch.len())]),
                "packed_key_prefix mismatch: expected {:?}, got {:?}",
                &packed_key_prefix[..13],
                &entries.scratch[..13.min(entries.scratch.len())],
            );
            entries.scratch.clear();
        }

        let raw = embedded.as_u64le_slice();
        let root = HeapRoot::Embedded(raw.as_ptr(), raw.len() as u32);

        let meta = Meta::from_packed_prefix(binding, packed_key_prefix, front, known_valid);
        entries.queued.push(HeapEntry { meta, root });

        if entries.should_compact() {
            self.compact()
        } else {
            Ok(())
        }
    }

    fn compact(&self) -> Result<(), Error> {
        // Safety: mutable borrow does not escape this function.
        let entries = unsafe { &mut *self.entries.get() };
        let alloc = unsafe { std::mem::transmute::<&Bump, &'static Bump>(&self.zz_alloc) };

        entries.compact(alloc)
    }

    fn try_into_parts(self) -> Result<(Vec<HeapEntry<'static>>, Spec, Bump), Error> {
        let MemTable { entries, zz_alloc } = self;

        // Perform a final compaction, then decompose Entries.
        let mut entries = entries.into_inner();
        let alloc = unsafe { std::mem::transmute::<&Bump, &'static Bump>(&zz_alloc) };
        entries.compact(alloc)?;

        let Entries { sorted, spec, .. } = entries;

        Ok((sorted, spec, zz_alloc))
    }

    /// Convert this MemTable into a MemDrainer.
    pub fn try_into_drainer(self) -> Result<MemDrainer, Error> {
        let (sorted, spec, zz_alloc) = self.try_into_parts()?;

        Ok(MemDrainer {
            in_group: false,
            it: sorted.into_iter().peekable(),
            spec,
            zz_alloc: Arc::new(zz_alloc),
        })
    }

    /// Spill this MemTable into a SpillWriter.
    /// If the MemTable is empty this is a no-op.
    /// `chunk_target_size` is the target size of a serialized chunk of the spilled segment.
    /// In practice, values like 256KB are reasonable.
    pub fn spill<F: io::Read + io::Write + io::Seek>(
        self,
        writer: &mut SpillWriter<F>,
        chunk_target_size: usize,
    ) -> Result<Spec, Error> {
        let (mut sorted, mut spec, alloc) = self.try_into_parts()?;

        // Validate all !front() && !known_valid() documents of the spilled
        // segment, applying "redact" annotations as we go so that no redacted
        // data is written to disk.
        //
        // This also accelerates a common case where no further reduction is
        // required across spilled segments, and we're typically doing this
        // validation in parallel to useful work of an associated connector.
        //
        // We do not validate front() documents now because in the common case
        // they'll be reduced with another document on drain, after which we'll
        // need to validate that reduced output anyway, so validation now is
        // wasted work. If it happens that there is no further reduction then
        // we'll validate the document upon drain.
        //
        // Note there's a trade-off here: this means we may write front() docs
        // to disk before applying redaction, and we want to redact BEFORE that.
        // The justification is that use cases of front() are for documents that
        // have already been redacted (e.g., Loaded docs of a materialization).
        for doc in sorted.iter_mut() {
            if doc.meta.front() || doc.meta.known_valid() {
                continue;
            }
            let outcomes = validate_root(
                &doc.root,
                &mut spec.validators[doc.meta.binding()],
                &spec.names[doc.meta.binding()],
                validation::redact_filter,
            )?;
            doc.meta.set_known_valid(true);

            if outcomes.is_empty() {
                continue; // No need to apply redaction outcomes.
            }

            // We must promote to HeapNode (if not already) to apply redaction.
            let mut heap_node = match doc.root.access() {
                Ok(heap_node) => heap_node,
                Err(embedded) => HeapNode::from_node(embedded.get(), &alloc),
            };
            let _outcome = redact::redact(&mut heap_node, &outcomes, &alloc, &spec.redact_salt)?;

            doc.root = HeapRoot::from_heap_node(heap_node);
        }

        let bytes = writer.write_segment(&sorted, chunk_target_size)?;
        tracing::debug!(
            %bytes,
            entries=%sorted.len(),
            mem_used=%(alloc.allocated_bytes() - alloc.chunk_capacity()),
            "spilled MemTable to disk segment",
        );

        std::mem::drop(sorted); // Now safe to drop.
        std::mem::drop(alloc); // Now safe to drop.

        Ok(spec)
    }
}

pub struct MemDrainer {
    in_group: bool,
    it: std::iter::Peekable<std::vec::IntoIter<HeapEntry<'static>>>,
    spec: Spec,
    zz_alloc: Arc<Bump>, // Careful! Order matters. See MemTable.
}

// Safety: MemDrainer is safe to Send because its iterators never have lent references,
// and we're sending them and their backing Bump allocator together.
unsafe impl Send for MemDrainer {}

impl MemDrainer {
    pub fn drain_next(&mut self) -> Result<Option<DrainedDoc>, Error> {
        let Some(HeapEntry { mut meta, mut root }) = self.it.next() else {
            return Ok(None);
        };

        // Advance past stale entries to the first fresh entry of their key,
        // ORing their front() existence onto it. A stale run with no fresh
        // successor emits nothing.
        let mut stale_front = false;
        while meta.stale() {
            stale_front |= meta.front();

            let Some(next) = self.it.next() else {
                return Ok(None); // Trailing stale run: nothing to emit.
            };

            let same_key = meta.0 == next.meta.0
                && compare_root_keys(&self.spec.keys[meta.binding()], &root, &next.root).is_eq();

            HeapEntry { meta, root } = next;
            self.in_group = false;

            if !same_key {
                // Orphaned stale run: drop its existence (next may differ in binding).
                stale_front = false;
            }
        }

        if stale_front {
            meta.set_front(); // Transfer stale existence onto the fresh output.
        }

        let is_full = self.spec.is_full[meta.binding()];
        let keys = self.spec.keys[meta.binding()].as_ref();
        let name = &self.spec.names[meta.binding()];
        let validator = &mut self.spec.validators[meta.binding()];

        // Attempt to reduce additional entries.
        while let Some(next) = self.it.peek() {
            if meta.0 != next.meta.0 || !compare_root_keys(keys, &root, &next.root).is_eq() {
                self.in_group = false;
                break;
            } else if !is_full && (!self.in_group || meta.not_associative()) {
                // We're performing associative reductions and:
                // * This is the first document of a group, which we cannot reduce into, or
                // * We've already attempted this associative reduction.
                self.in_group = true;
                break;
            }

            let rhs_outcomes =
                validate_root(&next.root, validator, name, validation::reduce_filter)?;

            match reduce_roots(&root, &next.root, &rhs_outcomes, &self.zz_alloc, is_full) {
                Ok((node, deleted)) => {
                    meta.set_deleted(deleted);
                    meta.set_known_valid(false); // Must re-validate.
                    root = HeapRoot::from_heap_node(node);
                    _ = self.it.next().unwrap(); // Discard.
                }
                Err(reduce::Error::NotAssociative) => {
                    meta.set_not_associative();
                    break;
                }
                Err(err) => return Err(Error::Reduce(err)),
            }
        }

        let outcomes = if meta.known_valid() {
            Vec::new() // Skip validation.
        } else {
            meta.set_known_valid(true); // Optimistic.
            validate_root(&root, validator, name, validation::redact_filter)?
        };

        // If we don't need to apply redaction outcomes, return the root as-is.
        if outcomes.is_empty() {
            let root = unsafe { OwnedHeapRoot::new(root, self.zz_alloc.clone()) };

            return Ok(Some(DrainedDoc {
                meta,
                root: OwnedNode::Heap(root),
            }));
        }

        // We must promote to HeapNode (if not already) to apply redaction.
        let mut heap_node = match root.access() {
            Ok(heap_node) => heap_node,
            Err(embedded) => HeapNode::from_node(embedded.get(), &self.zz_alloc),
        };
        let _outcome = redact::redact(
            &mut heap_node,
            &outcomes,
            &self.zz_alloc,
            &self.spec.redact_salt,
        )?;

        let root = unsafe {
            OwnedHeapRoot::new(HeapRoot::from_heap_node(heap_node), self.zz_alloc.clone())
        };

        Ok(Some(DrainedDoc {
            meta,
            root: OwnedNode::Heap(root),
        }))
    }
}

impl Iterator for MemDrainer {
    type Item = Result<DrainedDoc, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.drain_next().transpose()
    }
}

impl MemDrainer {
    pub fn into_spec(self) -> Spec {
        let MemDrainer {
            in_group: _,
            it,
            spec,
            zz_alloc,
        } = self;

        std::mem::drop(it);
        std::mem::drop(zz_alloc);

        spec
    }
}

/// Compare keys of two HeapRoot entries, dispatching through `access()`.
fn compare_root_keys(keys: &[Extractor], l: &HeapRoot, r: &HeapRoot) -> Ordering {
    match (l.access(), r.access()) {
        (Ok(lh), Ok(rh)) => Extractor::compare_key(keys, &lh, &rh),
        (Ok(lh), Err(re)) => Extractor::compare_key(keys, &lh, re.get()),
        (Err(le), Ok(rh)) => Extractor::compare_key(keys, le.get(), &rh),
        (Err(le), Err(re)) => Extractor::compare_key(keys, le.get(), re.get()),
    }
}

/// Validate a HeapRoot, dispatching through `access()`.
fn validate_root<'v, F>(
    root: &HeapRoot,
    validator: &'v mut crate::Validator,
    name: &str,
    filter: F,
) -> Result<Vec<validation::ScopedOutcome<'v>>, Error>
where
    F: Fn(validation::Outcome<'_>) -> Option<validation::Outcome<'_>>,
{
    let valid = match root.access() {
        Ok(heap_node) => validator.validate(&heap_node, &filter),
        Err(embedded) => validator.validate(embedded.get(), &filter),
    };
    valid.map_err(|invalid| Error::FailedValidation(name.to_string(), invalid))
}

/// Reduce two HeapRoot entries, dispatching each through `access()` to
/// build the appropriate LazyNode variant. The by-value HeapNodes from
/// `access()` live on the stack for the duration of the `reduce` call.
fn reduce_roots<'alloc>(
    lhs: &HeapRoot<'alloc>,
    rhs: &HeapRoot<'alloc>,
    rhs_outcomes: &[validation::ScopedOutcome<'_>],
    alloc: &'alloc bumpalo::Bump,
    full: bool,
) -> Result<(HeapNode<'alloc>, bool), reduce::Error> {
    match (lhs.access(), rhs.access()) {
        (Ok(lh), Ok(rh)) => reduce::reduce::<crate::ArchivedNode>(
            LazyNode::Heap(&lh),
            LazyNode::Heap(&rh),
            rhs_outcomes,
            alloc,
            full,
        ),
        (Ok(lh), Err(re)) => reduce::reduce::<crate::ArchivedNode>(
            LazyNode::Heap(&lh),
            LazyNode::Node(re.get()),
            rhs_outcomes,
            alloc,
            full,
        ),
        (Err(le), Ok(rh)) => reduce::reduce::<crate::ArchivedNode>(
            LazyNode::Node(le.get()),
            LazyNode::Heap(&rh),
            rhs_outcomes,
            alloc,
            full,
        ),
        (Err(le), Err(re)) => reduce::reduce::<crate::ArchivedNode>(
            LazyNode::Node(le.get()),
            LazyNode::Node(re.get()),
            rhs_outcomes,
            alloc,
            full,
        ),
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use serde_json::{Value, json};

    use crate::{SerPolicy, Validator, combine::CHUNK_TARGET_SIZE};
    use itertools::Itertools;
    use json::schema::build::build_schema;

    /// Serialize a JSON `Value` into a `HeapEmbedded` backed by the given allocator.
    fn to_embedded<'a>(value: &Value, alloc: &'a Bump) -> crate::HeapEmbedded<'a> {
        let heap_node = HeapNode::from_node(value, alloc);
        let archived = heap_node.to_archive();

        let byte_len = archived.len();
        let u64_len = (byte_len + 7) / 8;
        let buf = alloc.alloc_slice_fill_default::<crate::embedded::U64Le>(u64_len);
        unsafe {
            std::ptr::copy_nonoverlapping(archived.as_ptr(), buf.as_mut_ptr() as *mut u8, byte_len);
        }
        unsafe { crate::HeapEmbedded::from_buffer(buf) }
    }

    /// Add a document to a MemTable via `add_embedded()`, extracting the packed
    /// key prefix from the given extractors.
    fn add_as_embedded(
        memtable: &MemTable,
        binding: u16,
        doc: &Value,
        front: bool,
        keys: &[Extractor],
    ) {
        let embedded = to_embedded(doc, memtable.alloc());
        let mut scratch = bytes::BytesMut::new();
        Extractor::extract_all(embedded.get(), keys, Encoding::Packed, &mut scratch, None);
        let mut packed_prefix = [0u8; 16];
        let copy_len = scratch.len().min(16);
        packed_prefix[..copy_len].copy_from_slice(&scratch[..copy_len]);

        memtable
            .add_embedded(binding, &packed_prefix, embedded, front, false)
            .unwrap();
    }

    /// A full-reduction Spec over `n_bindings` bindings keyed on `/key`, whose
    /// `v` array reduces by append. A closure (not `vec![…; N]`) because
    /// `Validator` isn't `Clone`.
    fn append_merge_spec(n_bindings: usize) -> Spec {
        let binding = || {
            let schema = build_schema(
                &url::Url::parse("http://example/schema").unwrap(),
                &json!({
                    "properties": { "v": { "type": "array", "reduce": { "strategy": "append" } } },
                    "reduce": { "strategy": "merge" }
                }),
            )
            .unwrap();
            (
                true, // Full reduction.
                vec![Extractor::with_default(
                    "/key",
                    &SerPolicy::noop(),
                    json!("def"),
                )],
                "test",
                Validator::new(schema).unwrap(),
            )
        };
        Spec::with_bindings(std::iter::repeat_with(binding).take(n_bindings), Vec::new())
    }

    /// Force the Accumulator's current MemTable into a new spill segment,
    /// leaving a fresh empty MemTable behind.
    fn force_spill(acc: &mut crate::combine::Accumulator) {
        let spec = acc
            .memtable
            .take()
            .unwrap()
            .spill(&mut acc.spill, CHUNK_TARGET_SIZE)
            .unwrap();
        acc.memtable = Some(MemTable::new(spec));
    }

    fn project(doc: DrainedDoc) -> (usize, serde_json::Value, bool) {
        (
            doc.meta.binding(),
            serde_json::to_value(SerPolicy::noop().on_owned(&doc.root)).unwrap(),
            doc.meta.front(),
        )
    }

    #[test]
    fn test_truncate_partition() {
        // Backfill truncation over two bindings: binding 0 is truncated (its
        // pre-boundary sources dropped, stale Loaded rows kept as existence-only
        // fronts) while binding 1 is untouched. The drain output must be
        // identical whether the fixture stays in memory (MemTable::truncate) or
        // is forced through a spill file whose pre-boundary segment is fenced by
        // ordinal (Accumulator::truncate).
        let doc = |key: &str, v: &str| json!({ "key": key, "v": [v] });

        let expected = vec![
            // Two stale Loaded fronts transfer existence once onto the fresh source.
            (0, json!({"key": "exist_once", "v": ["s2"]}), true),
            // Pre-boundary sources dropped; only the fresh source stores.
            (0, json!({"key": "multi", "v": ["a2"]}), false),
            // A dropped pre-boundary source fabricates no existence.
            (0, json!({"key": "no_exist", "v": ["x2"]}), false),
            // A fresh Loaded front reduces with a fresh source; pre-boundary dropped.
            (0, json!({"key": "reduce2", "v": ["r_load", "r_src"]}), true),
            // "orphan" and "zzz_cross" are stale-only and emit nothing; binding 1
            // (never truncated) drains normally.
            (1, json!({"key": "b", "v": ["b0"]}), false),
        ];

        // Add post-boundary arrivals: stale Loaded rows (add_stale_front), one
        // fresh Loaded front, then fresh sources, and an untouched binding 1.
        let add_post = |mt: &MemTable| {
            for (key, v) in [
                ("exist_once", "L0"),
                ("exist_once", "L1"),
                ("orphan", "gone"),
                ("zzz_cross", "gone"),
            ] {
                mt.add_stale_front(0, HeapNode::from_node(&doc(key, v), mt.alloc()))
                    .unwrap();
            }
            mt.add(
                0,
                HeapNode::from_node(&doc("reduce2", "r_load"), mt.alloc()),
                true,
            )
            .unwrap();
            for (key, v) in [
                ("exist_once", "s2"),
                ("multi", "a2"),
                ("no_exist", "x2"),
                ("reduce2", "r_src"),
            ] {
                mt.add(0, HeapNode::from_node(&doc(key, v), mt.alloc()), false)
                    .unwrap();
            }
            mt.add(1, HeapNode::from_node(&doc("b", "b0"), mt.alloc()), false)
                .unwrap();
        };
        // Pre-boundary source documents of binding 0 (dropped/fenced on truncate).
        let pre = [("multi", "a0"), ("no_exist", "x0"), ("reduce2", "r0")];

        // In-memory variant: MemTable::truncate directly between adds.
        {
            let memtable = MemTable::new(append_merge_spec(2));
            for (key, v) in pre {
                memtable
                    .add(
                        0,
                        HeapNode::from_node(&doc(key, v), memtable.alloc()),
                        false,
                    )
                    .unwrap();
            }
            memtable.truncate(0);
            add_post(&memtable);

            let in_memory = memtable
                .try_into_drainer()
                .unwrap()
                .map_ok(project)
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert_eq!(in_memory, expected, "in-memory drain");
        }

        // Spill variant: an Accumulator spills the pre-boundary sources into a
        // segment, Accumulator::truncate fences that segment by ordinal, and
        // post-boundary arrivals (including stale fronts carrying their
        // persisted STALE flag) land in the final segment.
        {
            let mut acc = crate::combine::Accumulator::new(
                append_merge_spec(2),
                tempfile::tempfile().unwrap(),
            )
            .unwrap();
            {
                let mt = acc.memtable().unwrap();
                for (key, v) in pre {
                    mt.add(0, HeapNode::from_node(&doc(key, v), mt.alloc()), false)
                        .unwrap();
                }
            }
            force_spill(&mut acc); // Pre-boundary sources become segment 0.
            acc.truncate(0); // Fence segment 0 for binding 0.
            add_post(acc.memtable().unwrap());

            let spilled = acc
                .into_drainer()
                .unwrap()
                .map_ok(project)
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert_eq!(spilled, expected, "spill drain");
        }
    }

    #[test]
    fn test_truncate_purges_and_flags() {
        // truncate() drops the binding's !front entries and flags its front
        // entries stale in place, across both the compacted `sorted` vec and the
        // uncompacted `queued` vec, leaving other bindings untouched.
        let memtable = MemTable::new(append_merge_spec(2));
        let add = |binding: u16, key: &str, front: bool| {
            let node = HeapNode::from_node(&json!({"key": key, "v": ["x"]}), memtable.alloc());
            memtable.add(binding, node, front).unwrap();
        };

        add(0, "s_front", true);
        add(0, "s_drop", false);
        memtable.compact().unwrap(); // Move the above into `sorted`.
        add(0, "q_front", true);
        add(0, "q_drop", false);
        add(1, "other", false); // Untouched second binding.

        memtable.truncate(0);

        // Safety: no references to `entries` are lent out.
        let entries = unsafe { &*memtable.entries.get() };
        let key_of = |e: &HeapEntry| -> String {
            serde_json::to_value(SerPolicy::noop().on(&e.root.access().unwrap())).unwrap()["key"]
                .as_str()
                .unwrap()
                .to_string()
        };

        // Binding 0: only the two front entries survive, both now stale.
        let mut b0: Vec<(String, bool, bool)> = entries
            .sorted
            .iter()
            .chain(entries.queued.iter())
            .filter(|e| e.meta.binding() == 0)
            .map(|e| (key_of(e), e.meta.front(), e.meta.stale()))
            .collect();
        b0.sort();
        assert_eq!(
            b0,
            vec![
                ("q_front".to_string(), true, true),
                ("s_front".to_string(), true, true),
            ],
        );

        // Binding 1: untouched (fresh, not stale).
        let b1: Vec<(String, bool, bool)> = entries
            .sorted
            .iter()
            .chain(entries.queued.iter())
            .filter(|e| e.meta.binding() == 1)
            .map(|e| (key_of(e), e.meta.front(), e.meta.stale()))
            .collect();
        assert_eq!(b1, vec![("other".to_string(), false, false)]);
    }

    #[test]
    fn test_stale_front_spilled_after_truncate() {
        // A Loaded row flagged stale via add_stale_front AFTER truncate lands in
        // a segment at/above the cutoff, so its staleness rides the persisted
        // flag byte (not the ordinal fence) and it's still discarded on drain,
        // transferring only existence onto the fresh source.
        let mut acc =
            crate::combine::Accumulator::new(append_merge_spec(1), tempfile::tempfile().unwrap())
                .unwrap();
        {
            let mt = acc.memtable().unwrap();
            mt.add(
                0,
                HeapNode::from_node(&json!({"key": "k", "v": ["pre"]}), mt.alloc()),
                false,
            )
            .unwrap();
        }
        force_spill(&mut acc); // "pre" becomes fenced segment 0.
        acc.truncate(0);
        {
            let mt = acc.memtable().unwrap();
            mt.add_stale_front(
                0,
                HeapNode::from_node(&json!({"key": "k", "v": ["stale_load"]}), mt.alloc()),
            )
            .unwrap();
            mt.add(
                0,
                HeapNode::from_node(&json!({"key": "k", "v": ["fresh"]}), mt.alloc()),
                false,
            )
            .unwrap();
        }

        let out = acc
            .into_drainer()
            .unwrap()
            .map_ok(|d| {
                (
                    serde_json::to_value(SerPolicy::noop().on_owned(&d.root)).unwrap(),
                    d.meta.front(),
                )
            })
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(out, vec![(json!({"key": "k", "v": ["fresh"]}), true)]);
    }

    #[test]
    fn test_multiple_truncates() {
        // Truncating one binding repeatedly within a single Accumulator keeps
        // dropping each generation's pre-boundary source, so only the final
        // generation's data drains.
        let mut acc =
            crate::combine::Accumulator::new(append_merge_spec(1), tempfile::tempfile().unwrap())
                .unwrap();
        let add = |acc: &mut crate::combine::Accumulator, v: &str, front: bool| {
            let mt = acc.memtable().unwrap();
            let node = HeapNode::from_node(&json!({"key": "k", "v": [v]}), mt.alloc());
            mt.add(0, node, front).unwrap();
        };

        add(&mut acc, "v0", false);
        acc.truncate(0); // Drops v0.
        add(&mut acc, "v1", false);
        acc.truncate(0); // Drops v1.
        add(&mut acc, "v2", false);
        add(&mut acc, "load", true);

        let out = acc
            .into_drainer()
            .unwrap()
            .map_ok(|d| {
                (
                    serde_json::to_value(SerPolicy::noop().on_owned(&d.root)).unwrap(),
                    d.meta.front(),
                )
            })
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(out, vec![(json!({"key": "k", "v": ["load", "v2"]}), true)]);
    }

    #[test]
    fn test_multiple_truncates_across_segments() {
        // The ordinal cutoff ratchets across real spilled segments: truncate,
        // spill, truncate again. Every pre-boundary segment stays fenced, and a
        // stale front in the earliest (still-fenced) segment transfers its
        // existence onto the final fresh source.
        let mut acc =
            crate::combine::Accumulator::new(append_merge_spec(1), tempfile::tempfile().unwrap())
                .unwrap();
        let add = |acc: &mut crate::combine::Accumulator, v: &str, front: bool| {
            let mt = acc.memtable().unwrap();
            let node = HeapNode::from_node(&json!({"key": "k", "v": [v]}), mt.alloc());
            mt.add(0, node, front).unwrap();
        };

        add(&mut acc, "load", true); // A Loaded front, pre-boundary.
        add(&mut acc, "v0", false);
        force_spill(&mut acc); // Segment 0: [load(front), v0].
        acc.truncate(0); // cutoffs[0] = 1, fencing segment 0.

        add(&mut acc, "v1", false);
        force_spill(&mut acc); // Segment 1: [v1].
        acc.truncate(0); // cutoffs[0] = 2, fencing segments 0 and 1.

        add(&mut acc, "v2", false); // Fresh source in the final segment.

        let out = acc
            .into_drainer()
            .unwrap()
            .map_ok(|d| {
                (
                    serde_json::to_value(SerPolicy::noop().on_owned(&d.root)).unwrap(),
                    d.meta.front(),
                )
            })
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        // v0 and v1 (fenced sources) drop; `load`'s existence (fenced segment 0)
        // transfers onto v2.
        assert_eq!(out, vec![(json!({"key": "k", "v": ["v2"]}), true)]);
    }

    #[test]
    fn test_existence_transfer_across_locations() {
        // Two stale fronts for one key reach drain by different routes — one
        // fenced by an ordinal cutoff, one carrying a persisted STALE flag — and
        // together transfer existence exactly once onto the fresh source.
        let mut acc =
            crate::combine::Accumulator::new(append_merge_spec(1), tempfile::tempfile().unwrap())
                .unwrap();

        {
            let mt = acc.memtable().unwrap();
            mt.add(
                0,
                HeapNode::from_node(&json!({"key": "k", "v": ["load_a"]}), mt.alloc()),
                true,
            )
            .unwrap();
        }
        force_spill(&mut acc); // Segment 0: [load_a(front)].
        acc.truncate(0); // Fences segment 0 → load_a stale by ordinal.

        {
            let mt = acc.memtable().unwrap();
            // A Loaded row classified stale on arrival, after the truncate.
            mt.add_stale_front(
                0,
                HeapNode::from_node(&json!({"key": "k", "v": ["load_b"]}), mt.alloc()),
            )
            .unwrap();
            mt.add(
                0,
                HeapNode::from_node(&json!({"key": "k", "v": ["fresh"]}), mt.alloc()),
                false,
            )
            .unwrap();
        }

        let out = acc
            .into_drainer()
            .unwrap()
            .map_ok(|d| {
                (
                    serde_json::to_value(SerPolicy::noop().on_owned(&d.root)).unwrap(),
                    d.meta.front(),
                )
            })
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        // load_a (fenced) and load_b (persisted flag) drop; existence transfers
        // once onto `fresh`.
        assert_eq!(out, vec![(json!({"key": "k", "v": ["fresh"]}), true)]);
    }

    #[test]
    fn test_truncate_associative() {
        // Truncation composes with associative (non-full) reduction across a
        // spill: fenced stale entries are skipped, existence transfers onto the
        // first fresh entry, and the associative drain emits the leftmost alone.
        let schema = build_schema(
            &url::Url::parse("http://example/schema").unwrap(),
            &json!({
                "properties": { "v": { "type": "array", "reduce": { "strategy": "append" } } },
                "reduce": { "strategy": "merge" }
            }),
        )
        .unwrap();
        let spec = Spec::with_bindings(
            [(
                false, // Associative (not full) reduction.
                vec![Extractor::with_default(
                    "/key",
                    &SerPolicy::noop(),
                    json!("def"),
                )],
                "test",
                Validator::new(schema).unwrap(),
            )],
            Vec::new(),
        );
        let mut acc =
            crate::combine::Accumulator::new(spec, tempfile::tempfile().unwrap()).unwrap();
        let add = |acc: &mut crate::combine::Accumulator, v: &str, front: bool| {
            let mt = acc.memtable().unwrap();
            let node = HeapNode::from_node(&json!({"key": "k", "v": [v]}), mt.alloc());
            mt.add(0, node, front).unwrap();
        };

        add(&mut acc, "load", true); // Loaded front, pre-boundary.
        add(&mut acc, "s", false); // Pre-boundary source.
        force_spill(&mut acc); // Segment 0: [load(front), s].
        acc.truncate(0); // Fences segment 0.
        add(&mut acc, "a", false); // Fresh sources, post-boundary.
        add(&mut acc, "b", false);

        let out = acc
            .into_drainer()
            .unwrap()
            .map_ok(|d| {
                (
                    serde_json::to_value(SerPolicy::noop().on_owned(&d.root)).unwrap(),
                    d.meta.front(),
                )
            })
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        // `load`/`s` (fenced) are gone; existence transfers onto the first fresh
        // entry `a`. Associative drain emits the leftmost alone, then `b`.
        assert_eq!(
            out,
            vec![
                (json!({"key": "k", "v": ["a"]}), true),
                (json!({"key": "k", "v": ["b"]}), false),
            ],
        );
    }

    #[test]
    fn test_memtable_combine_reduce_sequence() {
        let key = vec![Extractor::with_default(
            "/key",
            &SerPolicy::noop(),
            json!("def"),
        )];
        let spec = Spec::with_bindings(
            std::iter::repeat_with(|| {
                let schema = build_schema(
                    &url::Url::parse("http://example/schema").unwrap(),
                    &json!({
                        "properties": {
                            "key": { "type": "string", "default": "def" },
                            "v": {
                                "type": "array",
                                "reduce": { "strategy": "append" }
                            }
                        },
                        "reduce": { "strategy": "merge" }
                    }),
                )
                .unwrap();

                (
                    true, // Full reduction.
                    key.clone(),
                    "source-name",
                    Validator::new(schema).unwrap(),
                )
            })
            .take(2),
            Vec::new(),
        );
        let memtable = MemTable::new(spec);

        // Binding 0 uses `add()` (heap), binding 1 uses `add_embedded()`.
        // This exercises all cross-dispatch arms (Heap×Embedded) in
        // compare_root_keys, validate_root, and reduce_roots during
        // compaction and drain.
        let add_and_compact = |docs: &[(bool, Value)]| {
            for (front, doc) in docs {
                let doc_0 = HeapNode::from_node(doc, memtable.alloc());
                memtable.add(0, doc_0, *front).unwrap();
                add_as_embedded(&memtable, 1, doc, *front, &key);
            }
            memtable.compact().unwrap();
        };

        add_and_compact(&[
            (false, json!({"key": "aaa", "v": ["apple"]})),
            (false, json!({"key": "aaa", "v": ["banana"]})),
            (false, json!({"key": "bbb", "v": ["carrot"]})),
            (true, json!({"key": "ccc", "v": ["grape"]})),
            (false, json!({"key": "def", "v": ["explicit-default"]})),
        ]);

        add_and_compact(&[
            (true, json!({"key": "bbb", "v": ["avocado"]})),
            (false, json!({"key": "bbb", "v": ["raisin"]})),
            (false, json!({"key": "ccc", "v": ["tomato"]})),
            (false, json!({"key": "ccc", "v": ["broccoli"]})),
        ]);

        add_and_compact(&[
            (false, json!({"key": "a", "v": ["before all"]})),
            (false, json!({"key": "ab", "v": ["first between"]})),
            (false, json!({"key": "bc", "v": ["between"]})),
            (false, json!({"key": "d", "v": ["after"]})),
            (false, json!({"v": ["implicit-default"]})), // Missing `key`.
        ]);

        add_and_compact(&[
            (true, json!({"key": "bc", "v": ["second"]})),
            (false, json!({"key": "d", "v": ["all"]})),
        ]);

        let actual = memtable
            .try_into_drainer()
            .unwrap()
            .map_ok(|doc| {
                (
                    doc.meta.binding(),
                    serde_json::to_value(SerPolicy::noop().on_owned(&doc.root)).unwrap(),
                    doc.meta.front(),
                )
            })
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        insta::assert_json_snapshot!(actual, @r###"
        [
          [
            0,
            {
              "key": "a",
              "v": [
                "before all"
              ]
            },
            false
          ],
          [
            0,
            {
              "key": "aaa",
              "v": [
                "apple",
                "banana"
              ]
            },
            false
          ],
          [
            0,
            {
              "key": "ab",
              "v": [
                "first between"
              ]
            },
            false
          ],
          [
            0,
            {
              "key": "bbb",
              "v": [
                "avocado",
                "carrot",
                "raisin"
              ]
            },
            true
          ],
          [
            0,
            {
              "key": "bc",
              "v": [
                "second",
                "between"
              ]
            },
            true
          ],
          [
            0,
            {
              "key": "ccc",
              "v": [
                "grape",
                "tomato",
                "broccoli"
              ]
            },
            true
          ],
          [
            0,
            {
              "key": "d",
              "v": [
                "after",
                "all"
              ]
            },
            false
          ],
          [
            0,
            {
              "key": "def",
              "v": [
                "explicit-default",
                "implicit-default"
              ]
            },
            false
          ],
          [
            1,
            {
              "key": "a",
              "v": [
                "before all"
              ]
            },
            false
          ],
          [
            1,
            {
              "key": "aaa",
              "v": [
                "apple",
                "banana"
              ]
            },
            false
          ],
          [
            1,
            {
              "key": "ab",
              "v": [
                "first between"
              ]
            },
            false
          ],
          [
            1,
            {
              "key": "bbb",
              "v": [
                "avocado",
                "carrot",
                "raisin"
              ]
            },
            true
          ],
          [
            1,
            {
              "key": "bc",
              "v": [
                "second",
                "between"
              ]
            },
            true
          ],
          [
            1,
            {
              "key": "ccc",
              "v": [
                "grape",
                "tomato",
                "broccoli"
              ]
            },
            true
          ],
          [
            1,
            {
              "key": "d",
              "v": [
                "after",
                "all"
              ]
            },
            false
          ],
          [
            1,
            {
              "key": "def",
              "v": [
                "explicit-default",
                "implicit-default"
              ]
            },
            false
          ]
        ]
        "###);
    }

    #[test]
    fn test_merge_patch_sequence() {
        let spec = |is_full| {
            (
                is_full,
                vec![Extractor::new("/k", &SerPolicy::noop())],
                "source-name",
                Validator::new(
                    build_schema(
                        &url::Url::parse("http://example").unwrap(),
                        &reduce::merge_patch_schema(),
                    )
                    .unwrap(),
                )
                .unwrap(),
            )
        };
        let memtable = MemTable::new(Spec::with_bindings(
            [spec(true), spec(false)].into_iter(),
            Vec::new(),
        ));

        let add_and_compact = |loaded: bool, docs: Value| {
            for doc in docs.as_array().unwrap() {
                let d = HeapNode::from_node(doc, memtable.alloc());
                memtable.add(0, d, loaded).unwrap();
                let d = HeapNode::from_node(doc, memtable.alloc());
                memtable.add(1, d, loaded).unwrap();
            }
            memtable.compact().unwrap();
        };

        let inspect = |m: &MemTable| {
            let entries = unsafe { &*m.entries.get() };
            let mut b = String::new();

            for HeapEntry { meta, root } in entries.sorted.iter() {
                b.push_str(&format!(
                    "{meta:?} {}\n",
                    serde_json::to_string(&SerPolicy::debug().on(&root.access().unwrap())).unwrap()
                ));
            }
            b
        };

        add_and_compact(
            false,
            json!([
              {"k": 1, "v": {"a": "b"}},
              {"k": 1, "v": {"c": {"d": 1}}},
              {"k": 2, "v": [1, 2]}
            ]),
        );

        // Reductions must hold back a compaction of the first document in
        // each group.
        insta::assert_snapshot!(inspect(&memtable), @r###"
        Meta(0) {"k":1,"v":{"a":"b"}}
        Meta(0) {"k":1,"v":{"c":{"d":1}}}
        Meta(0) {"k":2,"v":[1,2]}
        Meta(1) {"k":1,"v":{"a":"b"}}
        Meta(1) {"k":1,"v":{"c":{"d":1}}}
        Meta(1) {"k":2,"v":[1,2]}
        "###);

        // Further compactions reduce associatively.
        add_and_compact(
            false,
            json!([
              {"k": 1, "v": {"a": 4}},
              {"k": 1, "v": {"e": "f"}},
              {"k": 1, "v": {"c": {"g": 2}}},
              {"k": 2, "v": "hi"},
            ]),
        );

        insta::assert_snapshot!(inspect(&memtable), @r###"
        Meta(0) {"k":1,"v":{"a":"b"}}
        Meta(0) {"k":1,"v":{"a":4,"c":{"d":1,"g":2},"e":"f"}}
        Meta(0) {"k":2,"v":[1,2]}
        Meta(0) {"k":2,"v":"hi"}
        Meta(1) {"k":1,"v":{"a":"b"}}
        Meta(1) {"k":1,"v":{"a":4,"c":{"d":1,"g":2},"e":"f"}}
        Meta(1) {"k":2,"v":[1,2]}
        Meta(1) {"k":2,"v":"hi"}
        "###);

        add_and_compact(
            false,
            json!([
              {"k": 1, "v": {"a": 5}},
              {"k": 1, "v": {"c": null}},
              {"k": 2, "z": "whoops"},
              {"k": 2, "z": null},
              {"k": 2, "v": false},
            ]),
        );

        insta::assert_snapshot!(inspect(&memtable), @r###"
        Meta(0) {"k":1,"v":{"a":"b"}}
        Meta(0) {"k":1,"v":{"a":5,"c":null,"e":"f"}}
        Meta(0) {"k":2,"v":[1,2]}
        Meta(0) {"k":2,"v":false,"z":null}
        Meta(1) {"k":1,"v":{"a":"b"}}
        Meta(1) {"k":1,"v":{"a":5,"c":null,"e":"f"}}
        Meta(1) {"k":2,"v":[1,2]}
        Meta(1) {"k":2,"v":false,"z":null}
        "###);

        // A non-associative reduction stacks a new entry.
        add_and_compact(
            false,
            json!([
              {"k": 1, "v": {"e": "g"}},
              {"k": 1, "v": {"a": {"n": 1}}}, // Non-associative.
              {"k": 2, "v": true},
            ]),
        );

        insta::assert_snapshot!(inspect(&memtable), @r###"
        Meta(0) {"k":1,"v":{"a":"b"}}
        Meta(0, "NA") {"k":1,"v":{"a":5,"c":null,"e":"f"}}
        Meta(0) {"k":1,"v":{"a":{"n":1},"e":"g"}}
        Meta(0) {"k":2,"v":[1,2]}
        Meta(0) {"k":2,"v":true,"z":null}
        Meta(1) {"k":1,"v":{"a":"b"}}
        Meta(1, "NA") {"k":1,"v":{"a":5,"c":null,"e":"f"}}
        Meta(1) {"k":1,"v":{"a":{"n":1},"e":"g"}}
        Meta(1) {"k":2,"v":[1,2]}
        Meta(1) {"k":2,"v":true,"z":null}
        "###);

        // Multiple non-associative reductions can stack in a single compaction.
        add_and_compact(
            false,
            json!([
              {"k": 1, "v": {"e": "h"}},
              {"k": 1, "v": {"a": {"n": {"nn": 1}}}}, // Stacks on existing entry.
              {"k": 1, "v": {"e": "i"}},
              {"k": 1, "v": {"a": {"n": {"nn": {"nnn": 1}}}}}, // Stacks on queued entry.
              {"k": 1, "v": {"a": {"n": {"z": "z"}}}},
              {"k": 1, "v": {"e": "j"}},
              {"k": 2, "v": false},
              {"k": 2, "v": null},
            ]),
        );

        insta::assert_snapshot!(inspect(&memtable), @r###"
        Meta(0) {"k":1,"v":{"a":"b"}}
        Meta(0, "NA") {"k":1,"v":{"a":5,"c":null,"e":"f"}}
        Meta(0, "NA") {"k":1,"v":{"a":{"n":1},"e":"g"}}
        Meta(0, "NA") {"k":1,"v":{"a":{"n":{"nn":1}},"e":"i"}}
        Meta(0) {"k":1,"v":{"a":{"n":{"nn":{"nnn":1},"z":"z"}},"e":"j"}}
        Meta(0) {"k":2,"v":[1,2]}
        Meta(0) {"k":2,"v":null,"z":null}
        Meta(1) {"k":1,"v":{"a":"b"}}
        Meta(1, "NA") {"k":1,"v":{"a":5,"c":null,"e":"f"}}
        Meta(1, "NA") {"k":1,"v":{"a":{"n":1},"e":"g"}}
        Meta(1, "NA") {"k":1,"v":{"a":{"n":{"nn":1}},"e":"i"}}
        Meta(1) {"k":1,"v":{"a":{"n":{"nn":{"nnn":1},"z":"z"}},"e":"j"}}
        Meta(1) {"k":2,"v":[1,2]}
        Meta(1) {"k":2,"v":null,"z":null}
        "###);

        // We can add documents at the front.
        add_and_compact(
            true,
            json!([
              {"k": 1, "v": {"a": {"init": 1}}},
              {"k": 3, "v": "other"},
            ]),
        );

        insta::assert_snapshot!(inspect(&memtable), @r###"
        Meta(0, "F") {"k":1,"v":{"a":{"init":1}}}
        Meta(0) {"k":1,"v":{"a":"b"}}
        Meta(0, "NA") {"k":1,"v":{"a":5,"c":null,"e":"f"}}
        Meta(0, "NA") {"k":1,"v":{"a":{"n":1},"e":"g"}}
        Meta(0, "NA") {"k":1,"v":{"a":{"n":{"nn":1}},"e":"i"}}
        Meta(0) {"k":1,"v":{"a":{"n":{"nn":{"nnn":1},"z":"z"}},"e":"j"}}
        Meta(0) {"k":2,"v":[1,2]}
        Meta(0) {"k":2,"v":null,"z":null}
        Meta(0, "F") {"k":3,"v":"other"}
        Meta(1, "F") {"k":1,"v":{"a":{"init":1}}}
        Meta(1) {"k":1,"v":{"a":"b"}}
        Meta(1, "NA") {"k":1,"v":{"a":5,"c":null,"e":"f"}}
        Meta(1, "NA") {"k":1,"v":{"a":{"n":1},"e":"g"}}
        Meta(1, "NA") {"k":1,"v":{"a":{"n":{"nn":1}},"e":"i"}}
        Meta(1) {"k":1,"v":{"a":{"n":{"nn":{"nnn":1},"z":"z"}},"e":"j"}}
        Meta(1) {"k":2,"v":[1,2]}
        Meta(1) {"k":2,"v":null,"z":null}
        Meta(1, "F") {"k":3,"v":"other"}
        "###);

        // Documents at the front are also compacted with other front() docs,
        // but we don't compact between front() and !front(), because we don't
        // yet know whether additional front() documents could arrive.
        add_and_compact(
            true,
            json!([
              {"k": 1, "v": {"e": "overridden"}},
              {"k": 1, "v": {"a": {"init": 2}}},
            ]),
        );

        insta::assert_snapshot!(inspect(&memtable), @r###"
        Meta(0, "F") {"k":1,"v":{"a":{"init":1}}}
        Meta(0, "F") {"k":1,"v":{"a":{"init":2},"e":"overridden"}}
        Meta(0) {"k":1,"v":{"a":"b"}}
        Meta(0, "NA") {"k":1,"v":{"a":5,"c":null,"e":"f"}}
        Meta(0, "NA") {"k":1,"v":{"a":{"n":1},"e":"g"}}
        Meta(0, "NA") {"k":1,"v":{"a":{"n":{"nn":1}},"e":"i"}}
        Meta(0) {"k":1,"v":{"a":{"n":{"nn":{"nnn":1},"z":"z"}},"e":"j"}}
        Meta(0) {"k":2,"v":[1,2]}
        Meta(0) {"k":2,"v":null,"z":null}
        Meta(0, "F") {"k":3,"v":"other"}
        Meta(1, "F") {"k":1,"v":{"a":{"init":1}}}
        Meta(1, "F") {"k":1,"v":{"a":{"init":2},"e":"overridden"}}
        Meta(1) {"k":1,"v":{"a":"b"}}
        Meta(1, "NA") {"k":1,"v":{"a":5,"c":null,"e":"f"}}
        Meta(1, "NA") {"k":1,"v":{"a":{"n":1},"e":"g"}}
        Meta(1, "NA") {"k":1,"v":{"a":{"n":{"nn":1}},"e":"i"}}
        Meta(1) {"k":1,"v":{"a":{"n":{"nn":{"nnn":1},"z":"z"}},"e":"j"}}
        Meta(1) {"k":2,"v":[1,2]}
        Meta(1) {"k":2,"v":null,"z":null}
        Meta(1, "F") {"k":3,"v":"other"}
        "###);

        // Drain the combiner. It performs a final round of
        // reductions over:
        //  * The held-back initial document of full-reduction bindings.
        //  * front() vs !front() documents, which were also held back
        //    (we only now know that further front() docs cannot arrive).
        let mut drained = String::new();
        for doc in memtable.try_into_drainer().unwrap() {
            let DrainedDoc { meta, root } = doc.unwrap();
            drained.push_str(&format!(
                "{meta:?} {}\n",
                serde_json::to_string(&SerPolicy::debug().on_owned(&root)).unwrap()
            ));
        }
        insta::assert_snapshot!(drained, @r#"
        Meta(0, "F", "V") {"k":1,"v":{"a":{"n":{"nn":{"nnn":1},"z":"z"}},"e":"j"}}
        Meta(0, "V") {"k":2}
        Meta(0, "F", "V") {"k":3,"v":"other"}
        Meta(1, "F", "V") {"k":1,"v":{"a":{"init":1}}}
        Meta(1, "F", "NA", "V") {"k":1,"v":{"a":5,"c":null,"e":"f"}}
        Meta(1, "NA", "V") {"k":1,"v":{"a":{"n":1},"e":"g"}}
        Meta(1, "NA", "V") {"k":1,"v":{"a":{"n":{"nn":1}},"e":"i"}}
        Meta(1, "V") {"k":1,"v":{"a":{"n":{"nn":{"nnn":1},"z":"z"}},"e":"j"}}
        Meta(1, "V") {"k":2,"v":[1,2]}
        Meta(1, "V") {"k":2,"v":null,"z":null}
        Meta(1, "F", "V") {"k":3,"v":"other"}
        "#);
    }

    #[test]
    fn test_spill_and_validate() {
        let schema = build_schema(
            &url::Url::parse("http://example/schema").unwrap(),
            &json!({
                "properties": {
                    "key": { "type": "string" },
                    "v": { "const": "good" },
                }
            }),
        )
        .unwrap();

        let spec = Spec::with_one_binding(
            true, // Full reduction.
            vec![Extractor::new("/key", &SerPolicy::noop())],
            "source-name",
            Vec::new(),
            Validator::new(schema).unwrap(),
        );
        let memtable = MemTable::new(spec);

        let add = |memtable: &MemTable, front: bool, doc: Value| {
            let doc = HeapNode::from_node(&doc, memtable.alloc());
            memtable.add(0, doc, front).unwrap();
        };

        // While we validate the !front() documents, expect we don't validate front() ones,
        // and will go on to spill a front() document that doesn't match its schema.
        add(&memtable, false, json!({"key": "aaa", "v": "good"}));
        add(&memtable, true, json!({"key": "bbb", "v": "good"}));
        add(&memtable, true, json!({"key": "ccc", "v": "bad"}));

        let mut spill = SpillWriter::new(io::Cursor::new(Vec::new())).unwrap();
        let spec = memtable.spill(&mut spill, CHUNK_TARGET_SIZE).unwrap();

        let (spill, ranges) = spill.into_parts();
        assert_eq!(ranges, vec![0..138]);
        insta::assert_snapshot!(to_hex(spill.get_ref()), @"
        |82000000 08010000 78000002 61616100| ........x...aaa. 00000000
        |01009008 40000000 6b6579ff 01007008| ....@...key...p. 00000010
        |00000061 61610b00 10ff2500 11760a00| ...aaa....%..v.. 00000020
        |02180040 676f6f64 0c000018 00d00600| ...@good........ 00000030
        |00000300 0000c8ff ffff0211 00510002| .............Q.. 00000040
        |62626209 00040200 1c015800 30626262| bbb.......X.0bbb 00000050
        |3f000f58 001d3463 63635300 0102000d| ?..X..4cccS..... 00000060
        |58003f63 63635800 02216261 af000170| X.?cccX..!ba...p 00000070
        |0007b000 50ff0200 0000|              ....P.....       00000080
                                                               0000008a
        ");

        // New MemTable. This time we attempt to spill an invalid, non-reduced document.
        let memtable = MemTable::new(spec);
        add(&memtable, false, json!({"key": "ddd", "v": "bad"}));

        let mut spill = SpillWriter::new(io::Cursor::new(Vec::new())).unwrap();
        let out = memtable.spill(&mut spill, CHUNK_TARGET_SIZE);
        assert!(matches!(out, Err(Error::FailedValidation(n, _)) if n == "source-name"));
    }

    #[test]
    fn test_key_ordering_with_varied_lengths() {
        // Test that the Meta structure with embedded 13-byte key prefix
        // correctly orders keys of various lengths.
        let spec = Spec::with_one_binding(
            true, // Full reduction.
            vec![Extractor::new("/key", &SerPolicy::noop())],
            "test-source",
            Vec::new(),
            Validator::new(
                build_schema(
                    &url::Url::parse("http://example/schema").unwrap(),
                    &json!({
                        "properties": {
                            "key": { "type": "string" },
                            "value": { "type": "integer" }
                        },
                        "required": ["key"],
                        "reduce": { "strategy": "lastWriteWins" }
                    }),
                )
                .unwrap(),
            )
            .unwrap(),
        );
        let memtable = MemTable::new(spec);

        // Test keys with various lengths and patterns
        let test_keys = [
            ("a", 1),
            ("abc", 2),
            ("zebra", 3), // Short (< 13 bytes)
            ("exactly13char", 4),
            ("exactly13diff", 5), // Exactly 13 bytes
            ("same_prefix_1234567890_A", 6),
            ("same_prefix_1234567890_B", 7),
            ("same_prefix_1234567890_C", 8), // Same 13-byte prefix
            ("different_prefix_start_A", 9),
            ("another_long_key_prefix_B", 10), // Different long keys
            ("", 11),
            ("zzzzzzzzzzzzz", 12),
            ("zzzzzzzzzzzzzz", 13), // Edge cases
            ("identical_13c_but_different_after", 14),
            ("identical_13c_and_different_after", 15), // Differ after 13 bytes
        ];

        for (key, value) in test_keys.iter() {
            let doc = HeapNode::from_node(&json!({"key": key, "value": value}), memtable.alloc());
            memtable.add(0, doc, false).unwrap();
        }
        memtable.compact().unwrap();

        let actual: Vec<(String, i32)> = memtable
            .try_into_drainer()
            .unwrap()
            .map_ok(|doc| {
                let json_val = serde_json::to_value(SerPolicy::noop().on_owned(&doc.root)).unwrap();
                let obj = json_val.as_object().unwrap();
                (
                    obj.get("key").unwrap().as_str().unwrap().to_string(),
                    obj.get("value").unwrap().as_u64().unwrap() as i32,
                )
            })
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        // Verify keys are sorted lexicographically
        let mut sorted_keys: Vec<(String, i32)> =
            test_keys.iter().map(|&(k, v)| (k.to_string(), v)).collect();
        sorted_keys.sort_by(|a, b| a.0.cmp(&b.0));

        assert_eq!(actual, sorted_keys);
    }

    #[test]
    fn test_redaction_on_leaving_memtable() {
        let schema_json = json!({
            "properties": {
                "key": { "type": "string" },
                "public": { "type": "string" },
                "secret": { "redact": { "strategy": "block" } },
                "pii": { "redact": { "strategy": "sha256" } }
            },
            "required": ["key"],
            "reduce": { "strategy": "merge" }
        });
        let key = vec![Extractor::new("/key", &SerPolicy::noop())];

        let new_memtable = || {
            // Schema with both Block and Sha256 redaction strategies.
            let schema = build_schema(
                &url::Url::parse("http://example/schema").unwrap(),
                &schema_json,
            )
            .unwrap();

            let spec = Spec::with_one_binding(
                true, // Full reduction.
                key.clone(),
                "test-source",
                b"test-salt".to_vec(),
                Validator::new(schema).unwrap(),
            );
            MemTable::new(spec)
        };

        // Part 1: Expect we redact all !front documents upon spill.
        // (By construction, front documents must have already been redacted).
        // k2 is added as an embedded doc to exercise the Embedded promotion path
        // during spill validation and the embedded write path in SpillWriter.
        {
            let memtable = new_memtable();

            let add = |front: bool, doc: Value| {
                let doc = HeapNode::from_node(&doc, memtable.alloc());
                memtable.add(0, doc, front).unwrap();
            };

            add(
                false,
                json!({"key": "k1", "public": "visible", "secret": "remove-me", "pii": "alice"}),
            );
            // Added as embedded: exercises HeapNode::from_node(embedded.get(), ..)
            // promotion during spill redaction, and the Embedded write path in
            // SpillWriter::write_segment.
            add_as_embedded(
                &memtable,
                0,
                &json!({"key": "k2", "public": "also-visible", "pii": "bob"}),
                false,
                &key,
            );
            add(
                true, // Is front.
                json!({"key": "k3", "public": "front-doc", "secret": "passed", "pii": "passed"}),
            );

            let mut spill = SpillWriter::new(io::Cursor::new(Vec::new())).unwrap();
            let spec = memtable.spill(&mut spill, CHUNK_TARGET_SIZE).unwrap();

            // Read back all spilled documents and verify redaction
            let (spill, ranges) = spill.into_parts();
            let drainer =
                crate::combine::SpillDrainer::new(spec, spill, &ranges, Vec::new().into()).unwrap();

            let docs: String = drainer
                .map(|doc| {
                    let doc = doc.unwrap();
                    serde_json::to_string(&SerPolicy::debug().on_owned(&doc.root)).unwrap()
                })
                .join("\n");

            insta::assert_snapshot!(docs, @r#"
            {"key":"k1","pii":"sha256:e55a039cd18a0ddf1b15d9e6a190d734e36b8a6392af89109d099cd91112628d","public":"visible"}
            {"key":"k2","pii":"sha256:ad5525f56b4cd76a9acc02e5c485361fc7443d6585d35e9624e276cb9260ef37","public":"also-visible"}
            {"key":"k3","pii":"sha256:bf1d9002c41d0b111c7be3ce8fa80fcde9cfec5a4c77835c17dc8e3760d6f276","public":"front-doc"}
            "#);
        }

        // Part 2: Expect drain_next() redacts all documents.
        // This happens after reduction, but documents having no reduction are still redacted.
        // The second "reduced-key" and "z-other-key" are added as embedded to exercise
        // the Embedded promotion path during drain-time redaction.
        {
            let memtable = new_memtable();

            let add = |doc: Value| {
                let doc = HeapNode::from_node(&doc, memtable.alloc());
                memtable.add(0, doc, false).unwrap();
            };

            // These will be reduced together (heap + embedded).
            add(json!({
                "key": "reduced-key",
                "public": "first",
                "pii": "alice"
            }));

            add_as_embedded(
                &memtable,
                0,
                &json!({
                    "key": "reduced-key",
                    "public": "second",
                    "secret": "remove-me"
                }),
                false,
                &key,
            );

            // Different key as embedded to exercise standalone embedded redaction.
            add_as_embedded(
                &memtable,
                0,
                &json!({
                    "key": "z-other-key",
                    "pii": "bob",
                    "secret": "also removed"
                }),
                false,
                &key,
            );

            // Drain and verify redaction happens after reduction
            let drainer = memtable.try_into_drainer().unwrap();

            let docs: String = drainer
                .map(|doc| {
                    let doc = doc.unwrap();
                    serde_json::to_string(&SerPolicy::debug().on_owned(&doc.root)).unwrap()
                })
                .join("\n");

            insta::assert_snapshot!(docs, @r###"
            {"key":"reduced-key","pii":"sha256:e55a039cd18a0ddf1b15d9e6a190d734e36b8a6392af89109d099cd91112628d","public":"second"}
            {"key":"z-other-key","pii":"sha256:ad5525f56b4cd76a9acc02e5c485361fc7443d6585d35e9624e276cb9260ef37"}
            "###);
        }

        // Document fixture that fails schema validation, but also has secrets.
        let invalid_doc = json!({
            "key": "key",
            "public": ["wrong", "type"],
            "secret": "sensitive-data-should-not-leak",
            "pii": "private-info"
        });

        // Part 3: If compact() is performing reductions and a validation failure
        // occurs, all `redact` annotations are applied before surfacing the error.
        {
            let memtable = new_memtable();

            // Add > 2 to trigger reduction during MemTable::compact().
            for _ in 0..3 {
                let doc = HeapNode::from_node(&invalid_doc, memtable.alloc());
                memtable.add(0, doc, false).unwrap();
            }

            let failed = match memtable.compact() {
                Err(Error::FailedValidation(_name, failed)) => failed,
                got => panic!("expected FailedValidation: {got:?}"),
            };

            insta::assert_json_snapshot!(failed, @r###"
            {
              "basic_output": [
                {
                  "absoluteKeywordLocation": "http://example/schema#/properties/public",
                  "detail": "Type mismatch: expected a string",
                  "instanceLocation": "/public",
                  "instanceValue": "<array>"
                }
              ],
              "document": {
                "key": "key",
                "pii": "sha256:10c407a6244a707d65e7d748895fe108742c786093ac67a74c17044ba3815dec",
                "public": [
                  "wrong",
                  "type"
                ]
              }
            }
            "###);
        }

        // Part 4: If drain_next is performing reductions and a validation failure
        // occurs, it also applies `redact` annotations before surfacing an error.
        {
            let memtable = new_memtable();

            // Exactly 2 so that compact() succeeds, but drain_next() fails on attempted full reduction.
            for _ in 0..2 {
                let doc = HeapNode::from_node(&invalid_doc, memtable.alloc());
                memtable.add(0, doc, false).unwrap();
            }

            memtable.compact().expect("no validation error yet");
            let mut drainer = memtable.try_into_drainer().unwrap();

            let failed = match drainer.drain_next() {
                Err(Error::FailedValidation(_name, failed)) => failed,
                _ => panic!("expected FailedValidation"),
            };

            insta::assert_json_snapshot!(failed, @r###"
            {
              "basic_output": [
                {
                  "absoluteKeywordLocation": "http://example/schema#/properties/public",
                  "detail": "Type mismatch: expected a string",
                  "instanceLocation": "/public",
                  "instanceValue": "<array>"
                }
              ],
              "document": {
                "key": "key",
                "pii": "sha256:10c407a6244a707d65e7d748895fe108742c786093ac67a74c17044ba3815dec",
                "public": [
                  "wrong",
                  "type"
                ]
              }
            }
            "###);
        }
    }

    #[test]
    fn test_drainer_into_spec() {
        let spec = Spec::with_one_binding(
            true,
            vec![Extractor::new("/key", &SerPolicy::noop())],
            "test-source",
            Vec::new(),
            Validator::new(
                build_schema(
                    &url::Url::parse("http://example/schema").unwrap(),
                    &json!({
                        "properties": {
                            "key": { "type": "string" },
                            "v": { "type": "integer" }
                        },
                        "reduce": { "strategy": "lastWriteWins" }
                    }),
                )
                .unwrap(),
            )
            .unwrap(),
        );
        let memtable = MemTable::new(spec);

        let doc = HeapNode::from_node(&json!({"key": "aaa", "v": 1}), memtable.alloc());
        memtable.add(0, doc, false).unwrap();

        let drainer = memtable.try_into_drainer().unwrap();
        // Drop the drainer without fully draining and recover the Spec.
        let spec = drainer.into_spec();
        assert_eq!(spec.names, vec!["test-source"]);
    }

    fn to_hex(b: &[u8]) -> String {
        hexdump::hexdump_iter(b)
            .map(|line| format!("{line}"))
            .collect::<Vec<_>>()
            .join("\n")
    }
}
