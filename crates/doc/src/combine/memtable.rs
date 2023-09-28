use super::{DrainedDoc, Error, HeapEntry, Spec, SpillWriter};
use crate::{Extractor, HeapNode, LazyNode, OwnedHeapNode, OwnedNode};
use bumpalo::Bump;
use std::cell::UnsafeCell;
use std::sync::Arc;
use std::{cmp, io, ops};

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
    // Sorted documents are ordered such that partial combined documents are first,
    // and fully-reduced documents are second. A (binding, key) may appear at most
    // twice, once as combined and again as reduced. We must hold reduced keys
    // separate from combined keys in order to preserve the overall associative
    // order of reductions. We cannot, for example, reduce a reduced LHS with a
    // combined RHS if we might later discover there were other combined
    // documents which are associatively before the RHS we just smashed into LHS.
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
        self.queued.len() >= cmp::max(32, self.sorted.len())
    }

    fn compact(&mut self, alloc: &'static Bump) -> Result<(), Error> {
        // Documents are sorted such that partial-combine documents come before
        // fully-reduced reduce documents, and within each type they're ordered
        // on (binding, key). There are typically fewer fully-reduced documents
        // so, coming second, they're cheaper to split_off() in Self::try_into_drainer().
        let key_cmp = |lhs: &HeapEntry, rhs: &HeapEntry| {
            let c = lhs.reduced.cmp(&rhs.reduced);
            c.then(lhs.binding.cmp(&rhs.binding)).then_with(|| {
                Extractor::compare_key(&self.spec.keys[lhs.binding as usize], &lhs.root, &rhs.root)
            })
        };

        // Sort queued documents. It's important that this be a stable sort,
        // as we're combining in left-to-right application order.
        self.queued.sort_by(key_cmp);

        let sorted_len = self.sorted.len();
        let queued_len = self.queued.len();

        let mut next = Vec::with_capacity(sorted_len + queued_len);
        let mut queued = self.queued.drain(..).peekable();
        let mut sorted = self.sorted.drain(..).peekable();

        while let Some(mut cur) = queued.next() {
            // Emit documents from `sorted` which are less than `cur`.
            // Post-condition: sorted.next() is greater-than or equal to `cur`.
            while matches!(sorted.peek(), Some(peek) if key_cmp(&cur, peek).is_gt()) {
                next.push(sorted.next().unwrap());
            }

            let (validator, ref schema) = &mut self.spec.validators[cur.binding as usize];

            // Look for additional documents of this key from `queued` to combine into `cur`.
            while matches!(queued.peek(), Some(peek) if key_cmp(&cur, peek).is_eq()) {
                let next = queued.next().unwrap();
                (cur.root, cur.reduced) = super::smash(
                    alloc,
                    LazyNode::Heap(cur.root),
                    cur.reduced,
                    LazyNode::Heap(next.root),
                    next.reduced,
                    schema.as_ref(),
                    validator,
                )?;
            }

            // Look for a single matching `sorted` document to combine with `cur`,
            // which comes before it under our application order.
            if matches!(sorted.peek(), Some(peek) if key_cmp(&cur, peek).is_eq()) {
                let prev = sorted.next().unwrap();
                (cur.root, cur.reduced) = super::smash(
                    alloc,
                    LazyNode::Heap(prev.root),
                    prev.reduced,
                    LazyNode::Heap(cur.root),
                    cur.reduced,
                    schema.as_ref(),
                    validator,
                )?;
            }

            next.push(cur);
        }
        // `queued` is now empty. Extend with any `sorted` remainder.
        next.extend(sorted);
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

    /// Add the document to the MemTable.
    pub fn add<'s>(&'s self, binding: u32, root: HeapNode<'s>, reduced: bool) -> Result<(), Error> {
        // Safety: mutable borrow does not escape this function.
        let entries = unsafe { &mut *self.entries.get() };
        let root = unsafe { std::mem::transmute::<HeapNode<'s>, HeapNode<'static>>(root) };

        entries.queued.push(HeapEntry {
            binding,
            reduced,
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
        let (mut sorted, spec, zz_alloc) = self.try_into_parts()?;

        let pivot = sorted.partition_point(|doc| !doc.reduced);
        let other = sorted.split_off(pivot);

        Ok(MemDrainer {
            it1: sorted.into_iter().peekable(),
            it2: other.into_iter().peekable(),
            spec,
            zz_alloc: Arc::new(zz_alloc),
        })
    }

    /// Spill this MemTable into a SpillWriter.
    /// If the MemTable is empty this is a no-op.
    /// The `chunk_target_size` range is the target (begin) and maximum
    /// (end) size of a serialized chunk of the spilled segment.
    /// In practice, values like 256KB..1MB are reasonable.
    pub fn spill<F: io::Read + io::Write + io::Seek>(
        self,
        writer: &mut SpillWriter<F>,
        chunk_target_size: ops::Range<usize>,
    ) -> Result<Spec, Error> {
        let (sorted, mut spec, alloc) = self.try_into_parts()?;

        let docs = sorted.len();
        let pivot = sorted.partition_point(|doc| !doc.reduced);
        let mem_used = alloc.allocated_bytes() - alloc.chunk_capacity();

        // Validate all documents of the spilled segment.
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
        for doc in sorted.iter() {
            let (validator, ref schema) = &mut spec.validators[doc.binding as usize];
            validator
                .validate(schema.as_ref(), &doc.root)?
                .ok()
                .map_err(Error::FailedValidation)?;
        }

        // We write combined and reduced documents into separate segments,
        // because each segment may contain only sorted and unique keys.
        // Put differently, SpillDrainer only concerns itself with combines
        // and reductions _across_ segments and not within them.
        // If there are no combine or reduce documents, these are no-ops.
        let combine_bytes = writer.write_segment(&sorted[..pivot], chunk_target_size.clone())?;
        let reduce_bytes = writer.write_segment(&sorted[pivot..], chunk_target_size)?;

        tracing::debug!(
            %docs,
            %pivot,
            %mem_used,
            %reduce_bytes,
            %combine_bytes,
            "spilled MemTable to disk segment",
        );
        std::mem::drop(alloc); // Now safe to drop.

        Ok(spec)
    }
}

pub struct MemDrainer {
    it1: std::iter::Peekable<std::vec::IntoIter<HeapEntry<'static>>>,
    it2: std::iter::Peekable<std::vec::IntoIter<HeapEntry<'static>>>,
    spec: Spec,
    zz_alloc: Arc<Bump>, // Careful! Order matters. See MemTable.
}

// Safety: MemDrainer is safe to Send because its iterators never have lent references,
// and we're sending them and their backing Bump allocator together.
unsafe impl Send for MemDrainer {}

impl Iterator for MemDrainer {
    type Item = Result<DrainedDoc, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut inner = || {
            // This multi-level match is a little gross, but benchmarks as being
            // significantly faster than Itertools::merge_join_by()
            let HeapEntry {
                binding,
                reduced,
                root,
            } = match (self.it1.peek(), self.it2.peek()) {
                (Some(_), None) => self.it1.next().unwrap(), // Most common case.
                (None, Some(_)) => self.it2.next().unwrap(),
                (None, None) => return Ok(None),
                (Some(lhs), Some(rhs)) => {
                    match lhs.binding.cmp(&rhs.binding).then_with(|| {
                        Extractor::compare_key(
                            &self.spec.keys[lhs.binding as usize],
                            &lhs.root,
                            &rhs.root,
                        )
                    }) {
                        cmp::Ordering::Less => self.it1.next().unwrap(),
                        cmp::Ordering::Greater => self.it2.next().unwrap(),
                        cmp::Ordering::Equal => {
                            let HeapEntry {
                                binding,
                                reduced: lhs_reduced,
                                root: lhs_root,
                            } = self.it1.next().unwrap();

                            let HeapEntry {
                                binding: _,
                                reduced: rhs_reduced,
                                root: rhs_root,
                            } = self.it2.next().unwrap();

                            let (validator, ref schema) =
                                &mut self.spec.validators[binding as usize];

                            let (root, reduced) = super::smash(
                                &self.zz_alloc,
                                LazyNode::Heap(lhs_root),
                                lhs_reduced,
                                LazyNode::Heap(rhs_root),
                                rhs_reduced,
                                schema.as_ref(),
                                validator,
                            )?;

                            HeapEntry {
                                binding,
                                reduced,
                                root,
                            }
                        }
                    }
                }
            };

            let (validator, ref schema) = &mut self.spec.validators[binding as usize];

            validator
                .validate(schema.as_ref(), &root)
                .map_err(Error::SchemaError)?
                .ok()
                .map_err(Error::FailedValidation)?;

            // Safety: `root` was allocated from `self.zz_alloc`.
            let root = unsafe { OwnedHeapNode::new(root, self.zz_alloc.clone()) };

            Ok(Some(DrainedDoc {
                binding,
                reduced,
                root: OwnedNode::Heap(root),
            }))
        };
        inner().transpose()
    }
}

impl MemDrainer {
    pub fn into_spec(self) -> Spec {
        let MemDrainer {
            it1,
            it2,
            spec,
            zz_alloc,
        } = self;

        // This is probably being pedantic, but:
        // ensure that iterators are dropped before the Bump.
        std::mem::drop(it1);
        std::mem::drop(it2);
        std::mem::drop(zz_alloc);

        spec
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use serde_json::{json, Value};

    use crate::Validator;
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
                    vec![Extractor::with_default("/key", json!("def"))],
                    None,
                    Validator::new(schema).unwrap(),
                )
            })
            .take(2),
        );
        let memtable = MemTable::new(spec);

        let add_and_compact = |docs: &[(bool, Value)]| {
            for (full, doc) in docs {
                let doc_0 = HeapNode::from_node(doc, memtable.alloc());
                let doc_1 = HeapNode::from_node(doc, memtable.alloc());
                memtable.add(0, doc_0, *full).unwrap();
                memtable.add(1, doc_1, *full).unwrap();
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
                    doc.binding,
                    serde_json::to_value(doc.root).unwrap(),
                    doc.reduced,
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
}
