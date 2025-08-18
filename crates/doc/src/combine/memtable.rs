use super::{DrainedDoc, Error, HeapEntry, Meta, Spec, SpillWriter};
use crate::{reduce, transform, Extractor, HeapNode, LazyNode, OwnedHeapNode, OwnedNode};
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
        // `sort_ord` orders over (binding, key, !front).
        let sort_ord = |l: &HeapEntry, r: &HeapEntry| -> Ordering {
            l.meta
                .binding()
                .cmp(&r.meta.binding())
                .then_with(|| {
                    Extractor::compare_key(&self.spec.keys[l.meta.binding()], &l.root, &r.root)
                })
                .then_with(|| l.meta.front().cmp(&r.meta.front()).reverse())
        };
        let validators = &mut self.spec.validators;

        // Closure which attempts an associative reduction of `index` into `index-1`.
        // If the reduction succeeds then the item at `index` is removed.
        let mut maybe_reduce = |next: &mut Vec<HeapEntry<'_>>, index: usize| -> Result<(), Error> {
            let (lhs, rhs) = (&next[index - 1], &next[index]);
            let &mut (ref mut validator, ref schema) = &mut validators[lhs.meta.binding()];

            let rhs_valid = validator
                .validate(schema.as_ref(), &rhs.root)
                .map_err(Error::SchemaError)?
                .ok()
                .map_err(|err| {
                    Error::FailedValidation(self.spec.names[rhs.meta.binding()].clone(), err)
                })?;

            match reduce::reduce::<crate::ArchivedNode>(
                LazyNode::Heap(&lhs.root),
                LazyNode::Heap(&rhs.root),
                rhs_valid,
                alloc,
                false, // Compactions are always associative.
            ) {
                Ok((root, _deleted)) => {
                    next[index - 1].root = root;
                    next.remove(index);
                    Ok(())
                }
                Err(reduce::Error::NotAssociative) => {
                    next[index - 1].meta.set_not_associative();
                    Ok(())
                }
                Err(err) => Err(Error::Reduction(err)),
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

    /// Parse a JSON document string into a HeapNode using this MemTable's allocator.
    pub fn parse_json_str<'s>(&'s self, doc_json: &str) -> serde_json::Result<HeapNode<'s>> {
        let mut de = serde_json::Deserializer::from_str(doc_json);
        HeapNode::from_serde(&mut de, self.alloc())
    }

    /// Add the document to the MemTable.
    pub fn add<'s>(&'s self, binding: u32, root: HeapNode<'s>, front: bool) -> Result<(), Error> {
        // Safety: mutable borrow does not escape this function.
        let entries = unsafe { &mut *self.entries.get() };
        let root = unsafe { std::mem::transmute::<HeapNode<'s>, HeapNode<'static>>(root) };

        entries.queued.push(HeapEntry {
            meta: Meta::new(binding, front),
            root,
        });

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

        // Validate all !front() documents of the spilled segment.
        //
        // Technically, it's more efficient to defer all validation until we're
        // draining the combiner, and validating now does very slightly slow the
        // `combiner_perf` benchmark because we do extra validations that end up
        // needing to be re-done. But. In the common case we do very little little
        // reduction across spilled segments and when we're adding/spilling documents
        // that happens in parallel to useful work an associated connector is doing.
        // Whereas when we're draining the combiner the connector often can't do other
        // useful work, and total throughput is thus more sensitive to drain performance.
        // This is also a nice, tight loop that takes maximum advantage of processor
        // cache hierarchy and branch prediction as well as memory layout (we read
        // and write transactions in key order so `sorted` is often layed out in
        // ascending order within `alloc`).
        //
        // We do not validate front() documents now because in the common case
        // they'll be reduced with another document on drain, after which we'll
        // need to validate that reduced output anyway, so validation now is
        // wasted work. If it happens that there is no further reduction then
        // we'll validate the document upon drain.
        // 
        // For !front() documents, we validate and may apply transforms.
        let mut to_remove = Vec::new();
        
        for (index, doc) in sorted.iter_mut().enumerate() {
            if !doc.meta.front() {
                let &mut (ref mut validator, ref schema) = &mut spec.validators[doc.meta.binding()];
                let valid = validator
                    .validate(schema.as_ref(), &doc.root)?
                    .ok()
                    .map_err(|err| {
                        Error::FailedValidation(spec.names[doc.meta.binding()].clone(), err)
                    })?;
                
                // Apply transforms to the validated document in-place
                if !transform::transform(&mut doc.root, valid, &alloc)? {
                    // Document was removed by transform
                    to_remove.push(index);
                }
            }
        }
        
        // Remove documents in reverse order
        for index in to_remove.into_iter().rev() {
            sorted.remove(index);
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
        let is_full = self.spec.is_full[meta.binding()];
        let key = self.spec.keys[meta.binding()].as_ref();
        let &mut (ref mut validator, ref schema) = &mut self.spec.validators[meta.binding()];

        // Attempt to reduce additional entries.
        while let Some(next) = self.it.peek() {
            if meta.binding() != next.meta.binding()
                || !Extractor::compare_key(key, &root, &next.root).is_eq()
            {
                self.in_group = false;
                break;
            } else if !is_full && (!self.in_group || meta.not_associative()) {
                // We're performing associative reductions and:
                // * This is the first document of a group, which we cannot reduce into, or
                // * We've already attempted this associative reduction.
                self.in_group = true;
                break;
            }

            let rhs_valid = validator
                .validate(schema.as_ref(), &next.root)
                .map_err(Error::SchemaError)?
                .ok()
                .map_err(|err| {
                    Error::FailedValidation(self.spec.names[next.meta.binding()].clone(), err)
                })?;

            match reduce::reduce::<crate::ArchivedNode>(
                LazyNode::Heap(&root),
                LazyNode::Heap(&next.root),
                rhs_valid,
                &self.zz_alloc,
                is_full,
            ) {
                Ok((node, deleted)) => {
                    meta.set_deleted(deleted);
                    root = node;
                    _ = self.it.next().unwrap(); // Discard.
                }
                Err(reduce::Error::NotAssociative) => {
                    meta.set_not_associative();
                    break;
                }
                Err(err) => return Err(Error::Reduction(err)),
            }
        }

        let valid = validator
            .validate(schema.as_ref(), &root)
            .map_err(Error::SchemaError)?
            .ok()
            .map_err(|err| Error::FailedValidation(self.spec.names[meta.binding()].clone(), err))?;

        // Apply transformations if any are present
        if !transform::transform(&mut root, valid, &self.zz_alloc)? {
            // Entire document was removed by transform
            return Ok(None);
        }

        // Safety: `root` was allocated from `self.zz_alloc`.
        let root = unsafe { OwnedHeapNode::new(root, self.zz_alloc.clone()) };

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

#[cfg(test)]
mod test {
    use super::*;
    use serde_json::{json, Value};

    use crate::{combine::CHUNK_TARGET_SIZE, SerPolicy, Validator};
    use itertools::Itertools;
    use json::schema::build::build_schema;

    #[test]
    fn test_memtable_combine_reduce_sequence() {
        let spec = Spec::with_bindings(
            std::iter::repeat_with(|| {
                let schema = build_schema(
                    url::Url::parse("http://example/schema").unwrap(),
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
                    vec![Extractor::with_default(
                        "/key",
                        &SerPolicy::noop(),
                        json!("def"),
                    )],
                    "source-name",
                    None,
                    Validator::new(schema).unwrap(),
                )
            })
            .take(2),
        );
        let memtable = MemTable::new(spec);

        let add_and_compact = |docs: &[(bool, Value)]| {
            for (front, doc) in docs {
                let doc_0 = HeapNode::from_node(doc, memtable.alloc());
                let doc_1 = HeapNode::from_node(doc, memtable.alloc());
                memtable.add(0, doc_0, *front).unwrap();
                memtable.add(1, doc_1, *front).unwrap();
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
                None,
                Validator::new(
                    build_schema(
                        url::Url::parse("http://example").unwrap(),
                        &reduce::merge_patch_schema(),
                    )
                    .unwrap(),
                )
                .unwrap(),
            )
        };
        let memtable = MemTable::new(Spec::with_bindings([spec(true), spec(false)].into_iter()));

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
                    serde_json::to_string(&SerPolicy::debug().on(root)).unwrap()
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
        insta::assert_snapshot!(drained, @r###"
        Meta(0, "F") {"k":1,"v":{"a":{"n":{"nn":{"nnn":1},"z":"z"}},"e":"j"}}
        Meta(0) {"k":2}
        Meta(0, "F") {"k":3,"v":"other"}
        Meta(1, "F") {"k":1,"v":{"a":{"init":1}}}
        Meta(1, "F", "NA") {"k":1,"v":{"a":5,"c":null,"e":"f"}}
        Meta(1, "NA") {"k":1,"v":{"a":{"n":1},"e":"g"}}
        Meta(1, "NA") {"k":1,"v":{"a":{"n":{"nn":1}},"e":"i"}}
        Meta(1) {"k":1,"v":{"a":{"n":{"nn":{"nnn":1},"z":"z"}},"e":"j"}}
        Meta(1) {"k":2,"v":[1,2]}
        Meta(1) {"k":2,"v":null,"z":null}
        Meta(1, "F") {"k":3,"v":"other"}
        "###);
    }

    #[test]
    fn test_spill_and_validate() {
        let schema = build_schema(
            url::Url::parse("http://example/schema").unwrap(),
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
            None,
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
        assert_eq!(ranges, vec![0..107]);
        insta::assert_snapshot!(to_hex(spill.get_ref()), @r###"
        |63000000 d8000000 c0000000 00400000| c............@.. 00000000
        |006b6579 ff010070 08000000 6161610b| .key...p....aaa. 00000010
        |0010ff1c 0011760a 00021800 40676f6f| ......v.....@goo 00000020
        |640c0000 18009006 000000cc ffffff02| d............... 00000030
        |0d000202 001c8048 00306262 622f000f| .......H.0bbb/.. 00000040
        |48002e3f 63636348 00022162 618f0001| H..?cccH..!ba... 00000050
        |60000790 00500000 000000|            `....P.....      00000060
                                                               0000006b
        "###);

        // New MemTable. This time we attempt to spill an invalid, non-reduced document.
        let memtable = MemTable::new(spec);
        add(&memtable, false, json!({"key": "ddd", "v": "bad"}));

        let mut spill = SpillWriter::new(io::Cursor::new(Vec::new())).unwrap();
        let out = memtable.spill(&mut spill, CHUNK_TARGET_SIZE);
        assert!(matches!(out, Err(Error::FailedValidation(n, _)) if n == "source-name"));
    }

    fn to_hex(b: &[u8]) -> String {
        hexdump::hexdump_iter(b)
            .map(|line| format!("{line}"))
            .collect::<Vec<_>>()
            .join("\n")
    }
}
