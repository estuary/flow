use crate::{Extractor, OwnedNode, extractor::PlanKind};
use bytes::BufMut;
use json::Field as _;
use std::sync::atomic::AtomicBool;

/// Pre-compiled extraction plan that collapses repeated `Fields::get` calls
/// into a single linear merge-join when a list of extractors picks many
/// sibling leaves off a common parent object.
///
/// At plan-compile time we detect *blocks*: maximal runs of consecutive
/// sibling-leaf extractors under a common parent with monotonically ascending
/// field names. The plan stores only blocks; the gaps between them (and
/// head/tail of the list) are singles that run through the reference-path
/// unchanged.
#[derive(Debug)]
pub struct ExtractorPlan {
    // Complete list of extractors, with ordering preserved.
    extractors: Box<[Extractor]>,
    // Computed blocks of eligible extractors.
    blocks: Box<[Block]>,
}

#[derive(Debug)]
struct Block {
    // Half-open range `start..start + len` into `ExtractorPlan::extractors`.
    start: usize,
    len: usize,
    // Common parent pointer for the block.
    parent_ptr: json::Pointer,
}

impl ExtractorPlan {
    pub fn new(extractors: &[Extractor]) -> Self {
        let extractors = extractors.to_vec();
        let mut blocks: Vec<Block> = Vec::new();

        let mut i = 0;
        while i < extractors.len() {
            if let Some(block) = eligible_block(&extractors, i) {
                i += block.len;
                blocks.push(block);
            } else {
                i += 1;
            }
        }

        Self {
            extractors: extractors.into_boxed_slice(),
            blocks: blocks.into_boxed_slice(),
        }
    }

    /// Extract a packed tuple representation from an instance of
    /// doc::OwnedNode.
    pub fn extract_all_owned_indicate_truncation(
        &self,
        doc: &OwnedNode,
        out: &mut bytes::BytesMut,
        indicator: &AtomicBool,
    ) {
        match doc {
            OwnedNode::Heap(n) => match n.access() {
                Ok(heap_node) => self.extract_all_indicate_truncation(&heap_node, out, indicator),
                Err(embedded) => {
                    self.extract_all_indicate_truncation(embedded.get(), out, indicator)
                }
            },
            OwnedNode::Archived(n) => self.extract_all_indicate_truncation(n.get(), out, indicator),
        }
    }

    /// Extract a packed tuple representation from an instance of json::AsNode.
    pub fn extract_all_indicate_truncation<N: json::AsNode>(
        &self,
        doc: &N,
        out: &mut bytes::BytesMut,
        indicator: &AtomicBool,
    ) {
        let mut projected_indicator_pos: Option<usize> = None;
        let mut cursor: usize = 0;

        for block in self.blocks.iter() {
            // Write values for non-block extractors in the midst of blocks, or
            // prior to the first block.
            crate::extractor::write_extracted(
                doc,
                &self.extractors[cursor..block.start],
                out,
                indicator,
                &mut projected_indicator_pos,
            );
            // Write values for combined extractors, forming a contiguous block.
            emit_block(
                doc,
                &self.extractors[block.start..block.start + block.len],
                &block.parent_ptr,
                out,
                indicator,
            );
            cursor = block.start + block.len;
        }
        // Trailing non-block extractors. When `blocks` is empty, this is the
        // entire set of extractors.
        crate::extractor::write_extracted(
            doc,
            &self.extractors[cursor..],
            out,
            indicator,
            &mut projected_indicator_pos,
        );

        crate::extractor::finalize_truncation_indicator(out, projected_indicator_pos, indicator);
    }
}

fn emit_block<N: json::AsNode>(
    doc: &N,
    extractors: &[Extractor],
    parent_ptr: &json::Pointer,
    out: &mut bytes::BytesMut,
    indicator: &AtomicBool,
) {
    let parent_fields = parent_ptr.query(doc).and_then(|p| match p.as_node() {
        json::Node::Object(fields) => Some(fields),
        _ => None,
    });

    let mut w = out.writer();
    if let Some(fields) = parent_fields {
        // Parent object is present and not null.
        merge_write_block_extractors::<N, _>(extractors, fields, &mut w, indicator);
    } else {
        // No non-null parent object present, or the parent is not an object at
        // all. Emit each extractor individually, as appropriate for its `None`
        // value.
        for ex in extractors {
            ex.extract_from_resolved_indicate_truncation(None::<&N>, &mut w, indicator)
                .unwrap();
        }
    }
}

/// Two-pointer merge of extractors against a parent object's fields.
fn merge_write_block_extractors<N: json::AsNode, W: std::io::Write>(
    extractors: &[Extractor],
    fields: &(impl json::Fields<N> + ?Sized),
    w: &mut W,
    indicator: &AtomicBool,
) {
    let mut fields_iter = fields.iter();
    let mut field = fields_iter.next();

    for ex in extractors {
        let PlanKind::MergeJoinLeaf { name, .. } = ex.plan_kind() else {
            unreachable!("block eligibility guarantees field name");
        };

        let resolved = loop {
            let Some(f) = field.as_ref() else {
                break None;
            };
            match f.property().cmp(name) {
                std::cmp::Ordering::Less => {
                    field = fields_iter.next();
                }
                std::cmp::Ordering::Equal => break Some(f.value()),
                std::cmp::Ordering::Greater => break None,
            }
        };

        // Writing to BytesMut is infallible.
        ex.extract_from_resolved_indicate_truncation(resolved, w, indicator)
            .unwrap();
    }
}

/// Returns `Some(Block)` if the run at `start` is two or more sibling-leaf
/// extractors under a common parent with monotonically ascending field
/// names; `None` otherwise. Singletons don't form a block: they'd pay the
/// block's setup cost (parent walk, object check, iterator construction)
/// with no sharing benefit, and degrade the field lookup from `Fields::get`
/// to a linear merge advance.
fn eligible_block(extractors: &[Extractor], start: usize) -> Option<Block> {
    let PlanKind::MergeJoinLeaf {
        parent: parent_tokens,
        name: mut prev_name,
    } = extractors.get(start)?.plan_kind()
    else {
        return None;
    };

    let mut len = 1;
    for ex in &extractors[start + 1..] {
        match ex.plan_kind() {
            PlanKind::MergeJoinLeaf { parent, name }
                if parent == parent_tokens && name >= prev_name =>
            {
                prev_name = name;
                len += 1;
            }
            _ => break,
        }
    }

    (len > 1).then(|| Block {
        start,
        len,
        parent_ptr: json::Pointer(parent_tokens.to_vec()),
    })
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::SerPolicy;
    use serde_json::json;

    fn pack_reference(doc: &serde_json::Value, extractors: &[Extractor]) -> bytes::Bytes {
        let mut buf = bytes::BytesMut::new();
        let indicator = AtomicBool::new(false);
        Extractor::extract_all_indicate_truncation(doc, extractors, &mut buf, &indicator);
        buf.freeze()
    }

    fn pack_plan(doc: &serde_json::Value, extractors: &[Extractor]) -> bytes::Bytes {
        let plan = ExtractorPlan::new(extractors);
        let mut buf = bytes::BytesMut::new();
        let indicator = AtomicBool::new(false);
        plan.extract_all_indicate_truncation(doc, &mut buf, &indicator);
        buf.freeze()
    }

    fn assert_plan_matches(doc: &serde_json::Value, extractors: &[Extractor]) {
        let reference = pack_reference(doc, extractors);
        let plan_bytes = pack_plan(doc, extractors);
        assert_eq!(reference, plan_bytes, "plan diverged from reference");
    }

    fn merge_joined_extractor_count(plan: &ExtractorPlan) -> usize {
        plan.blocks.iter().map(|b| b.len).sum()
    }

    fn build_block_doc(field_count: usize) -> serde_json::Value {
        let mut obj = serde_json::Map::new();
        for i in 0..field_count {
            obj.insert(format!("f_{i:03}"), json!(i as i64));
        }
        serde_json::Value::Object(obj)
    }

    #[test]
    fn singleton_not_a_block() {
        let doc = build_block_doc(2);
        let policy = SerPolicy::noop();
        for (n, expected) in [(1, 0), (2, 2)] {
            let extractors: Vec<Extractor> = (0..n)
                .map(|i| Extractor::new(&format!("/f_{i:03}"), &policy))
                .collect();
            let plan = ExtractorPlan::new(&extractors);
            assert_eq!(merge_joined_extractor_count(&plan), expected, "n={n}");
            assert_plan_matches(&doc, &extractors);
        }
    }

    #[test]
    fn empty_extractor_slice() {
        let doc = json!({"a": 1});
        let plan = ExtractorPlan::new(&[]);
        let mut buf = bytes::BytesMut::new();
        let indicator = AtomicBool::new(false);
        plan.extract_all_indicate_truncation(&doc, &mut buf, &indicator);
        assert!(buf.is_empty());
    }

    #[test]
    fn no_blocks_with_truncation() {
        let policy = SerPolicy {
            str_truncate_after: 3,
            ..SerPolicy::noop()
        };
        let doc = json!({"a": "xxxxxx", "b": "short", "c": "yyyyyy"});
        let extractors = vec![
            Extractor::new("/a", &policy),
            Extractor::for_truncation_indicator(),
            Extractor::new("/b", &policy),
            Extractor::new("/c", &policy),
        ];
        assert_plan_matches(&doc, &extractors);
    }

    #[test]
    fn one_block_with_truncation() {
        const N: usize = 4;
        let policy = SerPolicy {
            str_truncate_after: 3,
            ..SerPolicy::noop()
        };
        let mut obj = serde_json::Map::new();
        for i in 0..N {
            obj.insert(format!("f_{i:03}"), json!("xxxxxx"));
        }
        let doc = json!({"inner": serde_json::Value::Object(obj)});
        let mut extractors = vec![Extractor::for_truncation_indicator()];
        for i in 0..N {
            extractors.push(Extractor::new(&format!("/inner/f_{i:03}"), &policy));
        }
        let plan = ExtractorPlan::new(&extractors);
        assert_eq!(merge_joined_extractor_count(&plan), N);
        assert_plan_matches(&doc, &extractors);
    }

    #[test]
    fn indicator_between_siblings() {
        let policy = SerPolicy::noop();
        for (half, expected_joined) in [(1, 0), (4, 8)] {
            let doc = build_block_doc(2 * half);
            let mut extractors: Vec<Extractor> = (0..half)
                .map(|i| Extractor::new(&format!("/f_{i:03}"), &policy))
                .collect();
            extractors.push(Extractor::for_truncation_indicator());
            for i in half..(2 * half) {
                extractors.push(Extractor::new(&format!("/f_{i:03}"), &policy));
            }
            let plan = ExtractorPlan::new(&extractors);
            assert_eq!(
                merge_joined_extractor_count(&plan),
                expected_joined,
                "half={half}"
            );
            assert_plan_matches(&doc, &extractors);
        }
    }

    #[test]
    fn block_with_missing_fields_uses_defaults() {
        const N: usize = 4;
        let doc = build_block_doc(N);
        let policy = SerPolicy::noop();
        let mut extractors: Vec<Extractor> = (0..N)
            .map(|i| Extractor::new(&format!("/f_{i:03}"), &policy))
            .collect();
        for i in 0..N {
            let ptr = format!("/absent_{i}");
            if i < N / 2 {
                extractors.push(Extractor::with_default(
                    &ptr,
                    &policy,
                    json!(format!("d_{i}")),
                ));
            } else {
                // No default.
                extractors.push(Extractor::new(&ptr, &policy));
            }
        }
        let plan = ExtractorPlan::new(&extractors);
        assert_eq!(plan.blocks.len(), 2);
        assert_eq!(merge_joined_extractor_count(&plan), 2 * N);
        assert_plan_matches(&doc, &extractors);
    }

    #[test]
    fn block_with_unusable_parent_uses_defaults() {
        let policy = SerPolicy::noop();
        for (doc, parent) in [
            // Nested parent is not an object.
            (json!({"present": 1}), "/absent_parent"),
            (json!({"arr": [1, 2, 3]}), "/arr"),
            (json!({"p": null}), "/p"),
            (json!({"p": "hi"}), "/p"),
            (json!({"p": 42}), "/p"),
            (json!({"p": true}), "/p"),
            // Root document is not an object.
            (json!([1, 2, 3]), ""),
            (json!(null), ""),
            (json!("hi"), ""),
            (json!(42), ""),
            (json!(true), ""),
        ] {
            const N: usize = 4;
            let extractors: Vec<Extractor> = (0..N)
                .map(|i| {
                    Extractor::with_default(
                        &format!("{parent}/f_{i:03}"),
                        &policy,
                        json!(format!("d_{i}")),
                    )
                })
                .collect();
            let plan = ExtractorPlan::new(&extractors);
            assert_eq!(merge_joined_extractor_count(&plan), N, "parent={parent}");
            assert_plan_matches(&doc, &extractors);
        }
    }

    #[test]
    fn block_with_duplicate_target_names() {
        let doc = json!({
            "a": "A", "b": "B", "c": "C", "d": "D",
            "e": "E", "f": "F", "g": "G", "h": "H",
        });
        let policy = SerPolicy::noop();
        let extractors = vec![
            Extractor::new("/a", &policy),
            Extractor::new("/b", &policy),
            Extractor::new("/b", &policy), // duplicate
            Extractor::new("/c", &policy),
            Extractor::new("/d", &policy),
            Extractor::new("/e", &policy),
            Extractor::new("/f", &policy),
            Extractor::new("/g", &policy),
            Extractor::new("/h", &policy),
        ];
        let plan = ExtractorPlan::new(&extractors);
        assert_eq!(merge_joined_extractor_count(&plan), extractors.len());
        assert_plan_matches(&doc, &extractors);
    }

    #[test]
    fn block_with_uuid_magic_inside() {
        let doc = json!({
            "a": "85bad119-15f2-11ee-8401-43f05f562888",
            "b": "not-a-uuid",
            "c": 42,
            "d": "1878923d-162a-11ee-8401-43f05f562888",
            "e": "extra",
            "f": "1878923d-162a-11ee-8401-43f05f562888",
            "g": "extra",
            "h": "extra",
        });
        let extractors = vec![
            Extractor::for_uuid_v1_date_time("/a"),
            Extractor::for_uuid_v1_date_time("/b"),
            Extractor::for_uuid_v1_date_time("/c"),
            Extractor::for_uuid_v1_date_time("/d"),
            Extractor::new("/e", &SerPolicy::noop()),
            Extractor::for_uuid_v1_date_time("/f"),
            Extractor::new("/g", &SerPolicy::noop()),
            Extractor::new("/h", &SerPolicy::noop()),
        ];
        let plan = ExtractorPlan::new(&extractors);
        assert_eq!(merge_joined_extractor_count(&plan), extractors.len());
        assert_plan_matches(&doc, &extractors);
    }

    #[test]
    fn array_index_suffix_not_eligible() {
        let doc = json!({"arr": ["v0", "v1", "v2", "v3", "v4", "v5", "v6", "v7"]});
        let policy = SerPolicy::noop();
        let extractors: Vec<Extractor> = (0..8)
            .map(|i| Extractor::new(&format!("/arr/{i}"), &policy))
            .collect();
        let plan = ExtractorPlan::new(&extractors);
        assert_eq!(merge_joined_extractor_count(&plan), 0);
        assert_plan_matches(&doc, &extractors);
    }

    #[test]
    fn sort_violations_split_runs() {
        let policy = SerPolicy::noop();
        for (doc, pointers, expected) in [
            (
                // Trailing violation leaves a singleton that doesn't form a block.
                json!({"a":"a","b":"b","c":"c","d":"d","e":"e","f":"f","g":"g","h":"h","A":"A"}),
                &["/a", "/b", "/c", "/d", "/e", "/f", "/g", "/h", "/A"][..],
                8,
            ),
            (
                // All-descending: every run has length 1, no blocks form.
                json!({"a": 1, "b": 2, "c": 3}),
                &["/c", "/b", "/a"][..],
                0,
            ),
        ] {
            let extractors: Vec<Extractor> = pointers
                .iter()
                .map(|p| Extractor::new(*p, &policy))
                .collect();
            let plan = ExtractorPlan::new(&extractors);
            assert_eq!(
                merge_joined_extractor_count(&plan),
                expected,
                "pointers={pointers:?}"
            );
            assert_plan_matches(&doc, &extractors);
        }
    }

    #[test]
    fn adjacent_runs_under_different_parents_dont_fuse() {
        const N: usize = 4;
        let mut obj_a = serde_json::Map::new();
        let mut obj_b = serde_json::Map::new();
        for i in 0..N {
            obj_a.insert(format!("col_{i:03}"), json!(i as i64));
            obj_b.insert(format!("col_{i:03}"), json!(format!("b_{i}")));
        }
        let doc =
            json!({"a": serde_json::Value::Object(obj_a), "b": serde_json::Value::Object(obj_b)});
        let policy = SerPolicy::noop();
        let mut extractors: Vec<Extractor> = (0..N)
            .map(|i| Extractor::new(&format!("/a/col_{i:03}"), &policy))
            .collect();
        for i in 0..N {
            extractors.push(Extractor::new(&format!("/b/col_{i:03}"), &policy));
        }
        let plan = ExtractorPlan::new(&extractors);
        assert_eq!(plan.blocks.len(), 2);
        assert_eq!(merge_joined_extractor_count(&plan), 2 * N);
        assert_plan_matches(&doc, &extractors);
    }
}
