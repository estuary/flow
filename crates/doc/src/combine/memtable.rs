use super::{Error, SpillWriter, REDUCED_FLAG, REVALIDATE_FLAG};
use crate::{
    dedup, reduce,
    validation::{Validation, Validator},
    ArchivedNode, AsNode, HeapDoc, HeapNode, LazyNode, Pointer,
};
use std::cell::UnsafeCell;
use std::collections::BTreeSet;
use std::pin::Pin;
use std::{cmp, io, rc::Rc};

/// MemTable is an in-memory combiner of HeapDocs.
/// It requires heap memory for storing and reducing documents.
/// After some threshold amount of allocation, a MemTable should
/// be spilled into SpillWriter.
pub struct MemTable {
    // Careful! Order matters. We must drop all the usages of `alloc`
    // before we drop `alloc` itself. Note that Rust drops struct fields
    // in declaration order, which we rely upon here:
    // https://doc.rust-lang.org/reference/destructors.html#destructors
    //
    // Safety: MemTable is not Sync and we never lend a reference to `entries`.
    entries: UnsafeCell<BTreeSet<KeyedDoc>>,
    dedup: dedup::Deduper<'static>,
    key: Rc<[Pointer]>,
    schema: url::Url,
    alloc: Pin<Box<bumpalo::Bump>>,
}

/// KeyedDoc is a HeapDoc and the composite JSON-Pointers over which it's combined.
pub struct KeyedDoc {
    key: Rc<[Pointer]>,
    doc: HeapDoc<'static>,
}

// KeyedDoc is ordered on its document's extracted key.
impl Ord for KeyedDoc {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        Pointer::compare(&self.key, &self.doc.root, &other.doc.root)
    }
}
impl PartialOrd for KeyedDoc {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl PartialEq for KeyedDoc {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == cmp::Ordering::Equal
    }
}
impl Eq for KeyedDoc {}

impl MemTable {
    pub fn new(key: Rc<[Pointer]>, schema: url::Url) -> Self {
        assert!(!key.is_empty());

        let alloc = Box::pin(HeapNode::new_allocator());
        let dedup: dedup::Deduper<'_> = HeapNode::new_deduper(&alloc);

        // Transmute Deduper from anonymous lifetime to 'static.
        // Safety: MemTable is a guard over its pinned allocator, and ensures that Deduper
        // and entries are always stored alongside and have the same lifetime as its bump allocator.
        let dedup: dedup::Deduper<'static> = unsafe { std::mem::transmute(dedup) };

        Self {
            alloc,
            dedup,
            key,
            schema,
            entries: UnsafeCell::new(BTreeSet::new()),
        }
    }

    /// Key returns the key by which documents of this MemTable is grouped.
    pub fn key(&self) -> &Rc<[Pointer]> {
        &self.key
    }

    /// Schema returns the schema URL against which MemTable documents are validated.
    pub fn schema(&self) -> &url::Url {
        &self.schema
    }

    /// Alloc returns the bump allocator of this MemTable.
    /// Its exposed to allow callers to allocate HeapNode structures
    /// having this MemTable's lifetime, which it can later combine or reduce.
    pub fn alloc<'s>(&'s self) -> &'s bumpalo::Bump {
        &self.alloc
    }

    /// Dedup returns the Deduper of this MemTable.
    /// As with alloc(), it's exposed to allow callers to build HeapNodes
    /// which this MemTable can then combine or reduce.
    pub fn dedup<'s>(&'s self) -> &dedup::Deduper<'s> {
        // Safety: narrowing from 'static to 's.
        // The allows the caller to build HeapNode<'s>.
        unsafe { std::mem::transmute(&self.dedup) }
    }

    /// Len returns the number of documents in this MemTable.
    pub fn len(&self) -> usize {
        let entries = unsafe { &*self.entries.get() };
        entries.len()
    }

    /// Reduce the fully reduced left-hand document with a partially reduced right-hand
    /// document that's already in the MemTable. It's an error if there is already a fully
    /// reduced right-hand document.
    pub fn reduce_left<'s>(
        &'s self,
        lhs: HeapNode<'s>,
        validator: &mut Validator,
    ) -> Result<(), Error> {
        // Safety: we are not Sync, and mutable borrow does not escape this function.
        let entries = unsafe { &mut *self.entries.get() };

        // Transmute from &'s self lifetime to internal 'static lifetime.
        let alloc = unsafe { std::mem::transmute(self.alloc()) };
        let lhs = unsafe { std::mem::transmute(lhs) };

        // Ensure LHS is valid against the schema.
        Validation::validate(validator, &self.schema, &lhs)?
            .ok()
            .map_err(Error::PreReduceValidation)?;

        let mut entry = KeyedDoc {
            key: self.key.clone(),
            doc: HeapDoc {
                root: lhs,
                flags: REDUCED_FLAG,
            },
        };

        // Look for a corresponding right-hand side document.
        let rhs = match entries.take(&entry) {
            None => {
                // No match? Just take the LHS.
                entries.insert(entry);
                return Ok(());
            }
            Some(KeyedDoc {
                key: _,
                doc:
                    HeapDoc {
                        root: rhs,
                        flags: rhs_flags,
                    },
            }) => {
                if rhs_flags & REDUCED_FLAG != 0 {
                    return Err(Error::AlreadyFullyReduced(
                        serde_json::to_value(entry.doc.root.as_node()).unwrap(),
                    ));
                }
                rhs
            }
        };

        // Validate RHS (again) to gather annotations. Note that it must have already
        // validated in order to have been in the docs set.
        let rhs_valid = Validation::validate(validator, &self.schema, &rhs)?
            .ok()
            .map_err(Error::PostReduceValidation)?;

        entry.doc.root = reduce::reduce(
            LazyNode::Heap::<ArchivedNode>(entry.doc.root),
            LazyNode::Heap(rhs),
            rhs_valid,
            alloc,
            &self.dedup,
            true,
        )?;
        entry.doc.flags |= REVALIDATE_FLAG;

        entries.insert(entry);
        Ok(())
    }

    /// Combine the partial right-hand side document into the left-hand document held by the Combiner.
    pub fn combine_right<'s>(
        &'s self,
        rhs: HeapNode<'s>,
        validator: &mut Validator,
    ) -> Result<(), Error> {
        // Safety: we are not Sync, and mutable borrow does not escape this function.
        let entries = unsafe { &mut *self.entries.get() };

        // Transmute from 's to internal 'static lifetime.
        let alloc = unsafe { std::mem::transmute(self.alloc()) };
        let rhs = unsafe { std::mem::transmute(rhs) };

        let rhs = KeyedDoc {
            key: self.key.clone(),
            doc: HeapDoc {
                root: rhs,
                flags: 0,
            },
        };
        let rhs_valid = Validation::validate(validator, &self.schema, &rhs.doc.root)?
            .ok()
            .map_err(Error::PreReduceValidation)?;

        let lhs = entries.take(&rhs);

        let entry = match lhs {
            // No match: Just take the RHS.
            None => rhs,
            // Match: we must reduce the nodes together.
            Some(KeyedDoc {
                key,
                doc:
                    HeapDoc {
                        root: lhs,
                        flags: lhs_flags,
                    },
            }) => KeyedDoc {
                key,
                doc: HeapDoc {
                    root: reduce::reduce(
                        LazyNode::Heap::<ArchivedNode>(lhs),
                        LazyNode::Heap(rhs.doc.root),
                        rhs_valid,
                        alloc,
                        &self.dedup,
                        lhs_flags & REDUCED_FLAG != 0,
                    )?,
                    flags: lhs_flags | REVALIDATE_FLAG,
                },
            },
        };

        entries.insert(entry);
        Ok(())
    }

    /// Convert this MemTable into a MemDrainer.
    pub fn into_drainer(self) -> MemDrainer {
        let MemTable {
            alloc,
            dedup: _, // Safe to drop now.
            entries,
            key,
            schema,
        } = self;

        MemDrainer {
            _alloc: alloc,
            it: entries.into_inner().into_iter(),
            key,
            schema,
        }
    }

    /// Spill this MemTable into a SpillWriter.
    /// If the MemTable is empty this is a no-op.
    pub fn spill<F: io::Read + io::Write + io::Seek>(
        self,
        writer: &mut SpillWriter<F>,
    ) -> Result<(Rc<[Pointer]>, url::Url), io::Error> {
        let MemTable {
            alloc,
            dedup: _, // Safe to drop now.
            entries,
            key,
            schema,
        } = self;

        let entries = entries.into_inner();
        let docs = entries.len();
        let docs_per_chunk = SpillWriter::<F>::target_docs_per_chunk(&alloc, docs);
        let mem_used = alloc.allocated_bytes() - alloc.chunk_capacity();

        let archive_used = writer.write_segment(
            entries.into_iter().map(|KeyedDoc { doc, .. }| doc),
            docs_per_chunk,
        )?;

        tracing::debug!(%mem_used, %archive_used, %docs, %docs_per_chunk,
            // TODO(johnny): remove when `mem_used` calculation is accurate.
            mem_alloc=%alloc.allocated_bytes(),
            mem_cap=%alloc.chunk_capacity(),
            "spilled MemTable to disk segment");

        Ok((key, schema))
    }
}

pub struct MemDrainer {
    _alloc: Pin<Box<bumpalo::Bump>>,
    it: std::collections::btree_set::IntoIter<KeyedDoc>,
    key: Rc<[Pointer]>,
    schema: url::Url,
}

impl MemDrainer {
    /// Drain documents from this MemDrainer by invoking the given callback.
    /// Documents passed to the callback MUST NOT be accessed after it returns.
    /// The callback returns true if it would like to be called further, or false
    /// if a present call to drain_while() should return, yielding back to the caller.
    ///
    /// A future call to drain_while() can then resume the drain operation at
    /// its next ordered document. drain_while() returns true while documents
    /// remain to drain, and false only after all documents have been drained.
    pub fn drain_while<C, CE>(
        &mut self,
        validator: &mut Validator,
        mut callback: C,
    ) -> Result<bool, CE>
    where
        C: for<'alloc> FnMut(LazyNode<'alloc, 'static, ArchivedNode>, bool) -> Result<bool, CE>,
        CE: From<Error>,
    {
        while let Some(KeyedDoc {
            doc: HeapDoc { root, flags },
            ..
        }) = self.it.next()
        {
            if flags & REVALIDATE_FLAG != 0 {
                // We've reduced multiple documents into this one.
                // Ensure it remains valid to its schema.
                Validation::validate(validator, &self.schema, &root)
                    .map_err(Error::SchemaError)?
                    .ok()
                    .map_err(Error::PostReduceValidation)?;
            }

            if !callback(LazyNode::Heap(root), flags & REDUCED_FLAG != 0)? {
                return Ok(true);
            }
        }

        Ok(false)
    }

    pub fn into_parts(self) -> (Rc<[Pointer]>, url::Url) {
        let MemDrainer {
            _alloc: _,
            it: _,
            key,
            schema,
        } = self;

        (key, schema)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use serde_json::json;

    use crate::{Schema, Validator};
    use json::schema::{build::build_schema, index::IndexBuilder};

    #[test]
    fn test_memtable_combine_reduce_sequence() {
        let schema = json!({
            "properties": {
                "key": { "type": "string" },
                "v": {
                    "type": "array",
                    "reduce": { "strategy": "append" }
                }
            },
            "reduce": { "strategy": "merge" }
        });
        let key: Rc<[Pointer]> = vec![Pointer::from_str("/key")].into();

        let curi = url::Url::parse("http://example/schema").unwrap();
        let schema: Schema = build_schema(curi.clone(), &schema).unwrap();

        let mut index = IndexBuilder::new();
        index.add(&schema).unwrap();
        index.verify_references().unwrap();
        let index = index.into_index();

        let mut validator = Validator::new(&index);
        let memtable = MemTable::new(key, curi);

        let fixtures = vec![
            (false, json!({"key": "aaa", "v": ["apple"]})),
            (false, json!({"key": "aaa", "v": ["banana"]})),
            (false, json!({"key": "bbb", "v": ["carrot"]})),
            (true, json!({"key": "ccc", "v": ["grape"]})),
            (true, json!({"key": "bbb", "v": ["avocado"]})),
            (false, json!({"key": "bbb", "v": ["raisin"]})),
            (false, json!({"key": "ccc", "v": ["tomato"]})),
        ];

        for (full, fixture) in &fixtures {
            let fixture =
                HeapNode::from_node(fixture.as_node(), memtable.alloc(), memtable.dedup());

            if *full {
                memtable.reduce_left(fixture, &mut validator).unwrap();
            } else {
                memtable.combine_right(fixture, &mut validator).unwrap();
            }
        }

        let mut actual = Vec::new();
        let mut drainer = memtable.into_drainer();

        loop {
            if !drainer
                .drain_while(&mut validator, |node, full| {
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
              "key": "ccc",
              "v": [
                "grape",
                "tomato"
              ]
            },
            true
          ]
        ]
        "###);
    }
}
