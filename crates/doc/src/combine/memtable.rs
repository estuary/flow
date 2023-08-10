use super::{Error, SpillWriter, FLAG_REDUCED};
use crate::{ArchivedNode, Extractor, HeapDoc, HeapNode, LazyNode, Validator};
use bumpalo::Bump;
use std::cell::UnsafeCell;
use std::pin::Pin;
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
    key: Box<[Extractor]>,
    schema: Option<url::Url>,
    zz_alloc: Pin<Box<Bump>>,
}

// Safety: MemTable is safe to Send because `entries` never has lent references,
// and we're sending `entries` and its corresponding Bump allocator together.
// It would _not_ be safe to separately send `entries` and the allocator,
// and so we do not do that.
unsafe impl Send for MemTable {}

struct Entries {
    // Queued documents are in any order.
    queued: Vec<HeapDoc<'static>>,
    // Sorted documents are ordered such that partial combined documents are first,
    // and fully-reduced documents are second. A key may appear at most twice,
    // with different types. We must hold reduced keys separate from combined keys
    // in order to preserve the overall associative order of reductions. We cannot,
    // for example, reduce a reduced LHS with a combined RHS if we might later
    // discover there were other combined documents which are associatively before
    // the RHS we just smashed into LHS.
    sorted: Vec<HeapDoc<'static>>,
    // Owned validator for reductions.
    validator: Validator,
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
        // keys are unique -- and if they aren't that's fine. In the pedantic case
        // of just one key which is reduced over and over, ensure we're combining
        // over a bunch of (small) right-hand documents before combining into its
        // value in `self.sorted`.
        self.queued.len() >= cmp::max(32, self.sorted.len())
    }

    fn compact(
        &mut self,
        alloc: &'static Bump,
        key: &[Extractor],
        schema: Option<&url::Url>,
    ) -> Result<(), Error> {
        // Documents are sorted such that partial-combine documents come before
        // fully-reduced reduce documents, and within each type they're ordered
        // on `self.key`. There are typically fewer reduce documents so they're
        // cheaper to split off of the Vec end when we drain.
        let key_cmp = |lhs: &HeapDoc, rhs: &HeapDoc| {
            let c = (lhs.flags & FLAG_REDUCED != 0).cmp(&(rhs.flags & FLAG_REDUCED != 0));
            if c.is_eq() {
                Extractor::compare_key(key, &lhs.root, &rhs.root)
            } else {
                c
            }
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

            // Look for additional documents of this key from `queued` to combine into `cur`.
            while matches!(queued.peek(), Some(peek) if key_cmp(&cur, peek).is_eq()) {
                let next = queued.next().unwrap();
                cur = super::smash(
                    alloc,
                    LazyNode::Heap(cur.root),
                    cur.flags,
                    LazyNode::Heap(next.root),
                    next.flags,
                    schema,
                    &mut self.validator,
                )?;
            }

            // Look for a single matching `sorted` document to combine with `cur`,
            // which comes before it under our application order.
            if matches!(sorted.peek(), Some(peek) if key_cmp(&cur, peek).is_eq()) {
                let prev = sorted.next().unwrap();
                cur = super::smash(
                    alloc,
                    LazyNode::Heap(prev.root),
                    prev.flags,
                    LazyNode::Heap(cur.root),
                    cur.flags,
                    schema,
                    &mut self.validator,
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
    pub fn new(key: Box<[Extractor]>, schema: Option<url::Url>, validator: Validator) -> Self {
        assert!(!key.is_empty());

        let alloc = Box::pin(HeapNode::new_allocator());

        Self {
            entries: UnsafeCell::new(Entries {
                queued: Vec::new(),
                sorted: Vec::new(),
                validator,
            }),
            key,
            schema,
            zz_alloc: alloc,
        }
    }

    /// Alloc returns the bump allocator of this MemTable.
    /// Its exposed to allow callers to allocate HeapNode structures
    /// having this MemTable's lifetime, which it can later combine or reduce.
    pub fn alloc(&self) -> &Bump {
        &self.zz_alloc
    }

    /// Add the document to the MemTable.
    pub fn add<'s>(&'s self, doc: HeapNode<'s>, reduced: bool) -> Result<(), Error> {
        // Safety: mutable borrow does not escape this function.
        let entries = unsafe { &mut *self.entries.get() };
        let doc = unsafe { std::mem::transmute::<HeapNode<'s>, HeapNode<'static>>(doc) };

        // If `doc` is fully reduced we must validate now and cannot defer.
        // We do defer right-hand documents, as they're validated immediately
        // prior to reduction in order to gather annotations. Or, if they're
        // never reduced, they'll be validated upon being drained.
        //
        // A left-hand document, though, is _not_ validated prior to reduction
        // and it's thus possible that we otherwise skip its validation entirely.
        if reduced {
            entries
                .validator
                .validate(self.schema.as_ref(), &doc)?
                .ok()
                .map_err(Error::FailedValidation)?;
        }

        entries.queued.push(HeapDoc {
            root: doc,
            flags: if reduced { FLAG_REDUCED } else { 0 },
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

        entries.compact(alloc, &self.key, self.schema.as_ref())
    }

    fn try_into_parts(
        self,
    ) -> Result<
        (
            Vec<HeapDoc<'static>>,
            Box<[Extractor]>,
            Option<url::Url>,
            Validator,
            Pin<Box<Bump>>,
        ),
        Error,
    > {
        let MemTable {
            entries,
            key,
            schema,
            zz_alloc,
        } = self;

        // Perform a final compaction, then decompose Entries.
        let mut entries = entries.into_inner();
        let alloc = unsafe { std::mem::transmute::<&Bump, &'static Bump>(&zz_alloc) };
        entries.compact(alloc, &key, schema.as_ref())?;

        let Entries {
            sorted, validator, ..
        } = entries;

        Ok((sorted, key, schema, validator, zz_alloc))
    }

    /// Convert this MemTable into a MemDrainer.
    pub fn try_into_drainer(self) -> Result<MemDrainer, Error> {
        let (mut sorted, key, schema, validator, zz_alloc) = self.try_into_parts()?;

        let pivot = sorted.partition_point(|doc| doc.flags & FLAG_REDUCED == 0);
        let other = sorted.split_off(pivot);

        Ok(MemDrainer {
            it1: sorted.into_iter().peekable(),
            it2: other.into_iter().peekable(),
            key,
            schema,
            validator,
            zz_alloc,
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
    ) -> Result<(Box<[Extractor]>, Option<url::Url>, Validator), Error> {
        let (sorted, key, schema, validator, alloc) = self.try_into_parts()?;

        let docs = sorted.len();
        let pivot = sorted.partition_point(|doc| doc.flags & FLAG_REDUCED == 0);
        let mem_used = alloc.allocated_bytes() - alloc.chunk_capacity();

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
            // TODO(johnny): remove when `mem_used` calculation is accurate.
            mem_alloc=%alloc.allocated_bytes(),
            mem_cap=%alloc.chunk_capacity(),
            "spilled MemTable to disk segment",
        );
        std::mem::drop(alloc); // Now safe to drop.

        Ok((key, schema, validator))
    }
}

pub struct MemDrainer {
    it1: std::iter::Peekable<std::vec::IntoIter<HeapDoc<'static>>>,
    it2: std::iter::Peekable<std::vec::IntoIter<HeapDoc<'static>>>,
    key: Box<[Extractor]>,
    schema: Option<url::Url>,
    validator: Validator,
    zz_alloc: Pin<Box<Bump>>, // Careful! Order matters. See MemTable.
}

// Safety: MemDrainer is safe to Send because its iterators never have lent references,
// and we're sending them and their backing Bump allocator together.
unsafe impl Send for MemDrainer {}

impl MemDrainer {
    /// Drain documents from this MemDrainer by invoking the given callback.
    /// Documents passed to the callback MUST NOT be accessed after it returns.
    /// The callback returns true if it would like to be called further, or false
    /// if a present call to drain_while() should return, yielding back to the caller.
    ///
    /// A future call to drain_while() can then resume the drain operation at
    /// its next ordered document. drain_while() returns true while documents
    /// remain to drain, and false only after all documents have been drained.
    pub fn drain_while<C, CE>(&mut self, mut callback: C) -> Result<bool, CE>
    where
        C: for<'alloc> FnMut(LazyNode<'alloc, 'static, ArchivedNode>, bool) -> Result<bool, CE>,
        CE: From<Error>,
    {
        // Incrementally merge the ordered sequence of documents from `it1` and `it2`.
        loop {
            let doc = match (self.it1.peek(), self.it2.peek()) {
                (None, None) => return Ok(false),
                (Some(_), None) => self.it1.next().unwrap(),
                (None, Some(_)) => self.it2.next().unwrap(),
                (Some(l), Some(r)) => match Extractor::compare_key(&self.key, &l.root, &r.root) {
                    cmp::Ordering::Less => self.it1.next().unwrap(),
                    cmp::Ordering::Greater => self.it2.next().unwrap(),
                    cmp::Ordering::Equal => {
                        let l = self.it1.next().unwrap();
                        let r = self.it2.next().unwrap();

                        super::smash(
                            &self.zz_alloc,
                            LazyNode::Heap(l.root),
                            l.flags,
                            LazyNode::Heap(r.root),
                            r.flags,
                            self.schema.as_ref(),
                            &mut self.validator,
                        )?
                    }
                },
            };

            self.validator
                .validate(self.schema.as_ref(), &doc.root)
                .map_err(Error::SchemaError)?
                .ok()
                .map_err(Error::FailedValidation)?;

            if !callback(LazyNode::Heap(doc.root), doc.flags & FLAG_REDUCED != 0)? {
                return Ok(true);
            }
        }
    }

    pub fn into_parts(self) -> (Box<[Extractor]>, Option<url::Url>, Validator) {
        let MemDrainer {
            it1,
            it2,
            key,
            schema,
            validator,
            zz_alloc,
        } = self;

        // This is probably being pedantic, but:
        // ensure that iterators are dropped before the Bump.
        std::mem::drop(it1);
        std::mem::drop(it2);
        std::mem::drop(zz_alloc);

        (key, schema, validator)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use serde_json::{json, Value};

    use crate::Validator;
    use json::schema::build::build_schema;

    #[test]
    fn test_memtable_combine_reduce_sequence() {
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

        let memtable = MemTable::new(
            vec![Extractor::with_default("/key", json!("def"))].into(),
            None,
            Validator::new(schema).unwrap(),
        );
        let add_and_compact = |docs: &[(bool, Value)]| {
            for (full, doc) in docs {
                let fixture = HeapNode::from_node(doc, memtable.alloc());
                memtable.add(fixture, *full).unwrap();
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

        let mut actual = Vec::new();
        let mut drainer = memtable.try_into_drainer().unwrap();

        loop {
            if !drainer
                .drain_while(|node, full| {
                    let node = serde_json::to_value(&node).unwrap();

                    actual.push((node, full));
                    Ok::<_, Error>(actual.len() % 2 != 0)
                })
                .unwrap()
            {
                break;
            }
        }

        insta::assert_json_snapshot!(actual, @r###"
        [
          [
            {
              "key": "a",
              "v": [
                "before all"
              ]
            },
            false
          ],
          [
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
            {
              "key": "ab",
              "v": [
                "first between"
              ]
            },
            false
          ],
          [
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
