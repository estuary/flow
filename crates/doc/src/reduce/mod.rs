use super::{
    dedup::Deduper,
    lazy::{LazyDestructured, LazyField, LazyNode},
    AsNode, Field, Fields, HeapField, HeapNode, Node, Valid,
};
use itertools::EitherOrBoth;

pub mod strategy;
pub use strategy::Strategy;

pub static DEFAULT_STRATEGY: &Strategy = &Strategy::LastWriteWins;

#[derive(thiserror::Error, Debug, serde::Serialize)]
pub enum Error {
    #[error("'append' strategy expects arrays")]
    AppendWrongType,
    #[error("`sum` resulted in numeric overflow")]
    SumNumericOverflow,
    #[error("'sum' strategy expects numbers")]
    SumWrongType,
    #[error("'merge' strategy expects objects or arrays")]
    MergeWrongType,
    #[error(
        "'set' strategy expects objects having only 'add', 'remove', and 'intersect' properties with consistent object or array types"
    )]
    SetWrongType,

    #[error("while reducing {:?}", .ptr)]
    WithLocation {
        ptr: String,
        #[source]
        detail: Box<Error>,
    },
    #[error("having values LHS: {lhs}, RHS: {rhs}")]
    WithValues {
        lhs: serde_json::Value,
        rhs: serde_json::Value,
        #[source]
        detail: Box<Error>,
    },
}

impl Error {
    fn with_location(self, loc: json::Location) -> Self {
        Error::WithLocation {
            ptr: loc.pointer_str().to_string(),
            detail: Box::new(self),
        }
    }

    fn with_values<L: AsNode, R: AsNode>(
        self,
        lhs: LazyDestructured<'_, '_, L>,
        rhs: LazyDestructured<'_, '_, R>,
    ) -> Self {
        let lhs = match lhs.restructure() {
            Ok(d) => serde_json::to_value(d.as_node()).unwrap(),
            Err(d) => serde_json::to_value(&d).unwrap(),
        };
        let rhs = match rhs.restructure() {
            Ok(d) => serde_json::to_value(d.as_node()).unwrap(),
            Err(d) => serde_json::to_value(&d).unwrap(),
        };

        Error::WithValues {
            lhs,
            rhs,
            detail: Box::new(self),
        }
    }

    fn with_details<L: AsNode, R: AsNode>(
        self,
        loc: json::Location,
        lhs: LazyDestructured<'_, '_, L>,
        rhs: LazyDestructured<'_, '_, R>,
    ) -> Self {
        self.with_location(loc).with_values(lhs, rhs)
    }
}

type Result<T> = std::result::Result<T, Error>;

/// Reduce a RHS document validation into a preceding LHS document.
/// The RHS validation provides reduction annotation outcomes used in the reduction.
/// If |prune|, then LHS is the root-most (or left-most) document in the reduction
/// sequence. Depending on the reduction strategy, additional pruning can be done
/// in this case (i.e., removing tombstones) that isn't possible in a partial
/// non-root reduction.
pub fn reduce<'alloc, N: AsNode>(
    lhs: LazyNode<'alloc, '_, N>,
    rhs: LazyNode<'alloc, '_, N>,
    rhs_valid: Valid,
    alloc: &'alloc bumpalo::Bump,
    dedup: &Deduper<'alloc>,
    full: bool,
) -> Result<HeapNode<'alloc>> {
    let tape = rhs_valid.extract_reduce_annotations();
    let tape = &mut tape.as_slice();

    let reduced = Cursor {
        tape,
        loc: json::Location::Root,
        full,
        lhs,
        rhs,
        alloc,
        dedup,
    }
    .reduce()?;

    assert!(tape.is_empty());
    Ok(reduced)
}

/// Cursor models a joint document location which is being reduced.
pub struct Cursor<'alloc, 'schema, 'tmp, 'l, 'r, L: AsNode, R: AsNode> {
    tape: &'tmp mut Index<'schema>,
    loc: json::Location<'tmp>,
    full: bool,
    lhs: LazyNode<'alloc, 'l, L>,
    rhs: LazyNode<'alloc, 'r, R>,
    alloc: &'alloc bumpalo::Bump,
    dedup: &'tmp Deduper<'alloc>,
}

type Index<'a> = &'a [(&'a Strategy, u64)];

impl<'alloc, L: AsNode, R: AsNode> Cursor<'alloc, '_, '_, '_, '_, L, R> {
    pub fn reduce(self) -> Result<HeapNode<'alloc>> {
        let (strategy, _) = self.tape.first().unwrap();
        strategy.apply(self)
    }
}

fn count_nodes<N: AsNode>(v: &LazyNode<'_, '_, N>) -> usize {
    match v {
        LazyNode::Node(doc) => count_nodes_generic(&doc.as_node()),
        LazyNode::Heap(doc) => count_nodes_heap(doc),
    }
}

fn count_nodes_generic<N: AsNode>(node: &Node<'_, N>) -> usize {
    match node {
        Node::Bool(_) | Node::Null | Node::String(_) | Node::Number(_) | Node::Bytes(_) => 1,
        Node::Array(v) => v
            .iter()
            .fold(1, |c, vv| c + count_nodes_generic(&vv.as_node())),
        Node::Object(v) => v.iter().fold(1, |c, field| {
            c + count_nodes_generic(&field.value().as_node())
        }),
    }
}

// A HeapNode can also be counted as an AsNode, but this is faster (it avoids Node<> conversion).
fn count_nodes_heap(node: &HeapNode<'_>) -> usize {
    match node {
        HeapNode::Bool(_)
        | HeapNode::Bytes(_)
        | HeapNode::Float(_)
        | HeapNode::NegInt(_)
        | HeapNode::Null
        | HeapNode::PosInt(_)
        | HeapNode::StringOwned(_)
        | HeapNode::StringShared(_) => 1,
        HeapNode::Array(v) => v.0.iter().fold(1, |c, vv| c + count_nodes_heap(vv)),
        HeapNode::Object(v) => {
            v.0.iter()
                .fold(1, |c, field| c + count_nodes_heap(&field.value))
        }
    }
}

fn reduce_prop<'alloc, L: AsNode, R: AsNode>(
    tape: &mut Index<'_>,
    loc: json::Location<'_>,
    full: bool,
    eob: EitherOrBoth<LazyField<'alloc, '_, L>, LazyField<'alloc, '_, R>>,
    alloc: &'alloc bumpalo::Bump,
    dedup: &Deduper<'alloc>,
) -> Result<HeapField<'alloc>> {
    match eob {
        EitherOrBoth::Left(lhs) => Ok(lhs.into_heap_field(alloc, dedup)),
        EitherOrBoth::Right(rhs) => {
            let rhs = rhs.into_heap_field(alloc, dedup);
            *tape = &tape[count_nodes_heap(&rhs.value)..];
            Ok(rhs)
        }
        EitherOrBoth::Both(lhs, rhs) => {
            let (property, lhs, rhs) = match (lhs, rhs) {
                (LazyField::Heap(lhs), LazyField::Heap(rhs)) => (
                    lhs.property,
                    LazyNode::Heap(lhs.value),
                    LazyNode::Heap(rhs.value),
                ),
                (LazyField::Heap(lhs), LazyField::Doc(rhs)) => (
                    lhs.property,
                    LazyNode::Heap(lhs.value),
                    LazyNode::Node(rhs.value()),
                ),
                (LazyField::Doc(lhs), LazyField::Heap(rhs)) => (
                    rhs.property,
                    LazyNode::Node(lhs.value()),
                    LazyNode::Heap(rhs.value),
                ),
                (LazyField::Doc(lhs), LazyField::Doc(rhs)) => (
                    dedup.alloc_shared_string(lhs.property()),
                    LazyNode::Node(lhs.value()),
                    LazyNode::Node(rhs.value()),
                ),
            };

            let value = Cursor {
                tape,
                loc: loc.push_prop(property.0),
                full,
                lhs,
                rhs,
                alloc,
                dedup,
            }
            .reduce()?;

            Ok(HeapField { property, value })
        }
    }
}

fn reduce_item<'alloc, L: AsNode, R: AsNode>(
    tape: &mut Index<'_>,
    loc: json::Location<'_>,
    full: bool,
    eob: EitherOrBoth<(usize, LazyNode<'alloc, '_, L>), (usize, LazyNode<'alloc, '_, R>)>,
    alloc: &'alloc bumpalo::Bump,
    dedup: &Deduper<'alloc>,
) -> Result<HeapNode<'alloc>> {
    match eob {
        EitherOrBoth::Left((_, lhs)) => Ok(lhs.into_heap_node(alloc, dedup)),
        EitherOrBoth::Right((_, rhs)) => {
            let rhs = rhs.into_heap_node(alloc, dedup);
            *tape = &tape[count_nodes_heap(&rhs)..];
            Ok(rhs)
        }
        EitherOrBoth::Both((_, lhs), (index, rhs)) => Cursor {
            tape,
            loc: loc.push_item(index),
            full,
            lhs,
            rhs,
            alloc,
            dedup,
        }
        .reduce(),
    }
}

#[cfg(test)]
pub mod test {
    use super::*;

    use crate::{Schema, Validation, Validator};
    use json::schema::{build::build_schema, index::IndexBuilder};
    pub use serde_json::{json, Value};
    use std::error::Error as StdError;

    #[test]
    fn test_node_counting() {
        let alloc = HeapNode::new_allocator();
        let dedup = HeapNode::new_deduper(&alloc);

        let test_case = |fixture: Value, expect: usize| {
            assert_eq!(count_nodes_generic(&fixture.as_node()), expect);
            assert_eq!(
                count_nodes_heap(&HeapNode::from_node(fixture.as_node(), &alloc, &dedup)),
                expect
            );
        };

        test_case(json!(true), 1);
        test_case(json!("string"), 1);
        test_case(json!(1234), 1);
        test_case(Value::Null, 1);

        test_case(json!([]), 1);
        test_case(json!([2, 3, 4]), 4);
        test_case(json!([2, [4, 5]]), 5);

        test_case(json!({}), 1);
        test_case(json!({"2": 2, "3": 3}), 3);
        test_case(json!({"2": 2, "3": {"4": 4, "5": 5}}), 5);

        test_case(
            json!({
                "two": [3, [5, 6], {"eight": 8}],
                "nine": "nine",
                "ten": null,
                "eleven": true,
            }),
            11,
        );
    }

    pub enum Case {
        Partial { rhs: Value, expect: Result<Value> },
        Full { rhs: Value, expect: Result<Value> },
    }
    pub use Case::{Full, Partial};

    pub fn run_reduce_cases(schema: Value, cases: Vec<Case>) {
        let curi = url::Url::parse("http://example/schema").unwrap();
        let schema: Schema = build_schema(curi.clone(), &schema).unwrap();

        let mut index = IndexBuilder::new();
        index.add(&schema).unwrap();
        index.verify_references().unwrap();
        let index = index.into_index();

        let alloc = HeapNode::new_allocator();
        let dedup = HeapNode::new_deduper(&alloc);

        let mut validator = Validator::new(&index);
        let mut lhs: Option<HeapNode<'_>> = None;

        for case in cases {
            let (rhs, expect, prune) = match case {
                Partial { rhs, expect } => (rhs, expect, false),
                Full { rhs, expect } => (rhs, expect, true),
            };
            let rhs_valid = Validation::validate(&mut validator, &curi, &rhs)
                .unwrap()
                .ok()
                .unwrap();

            let lhs_cloned = lhs
                .as_ref()
                .map(|doc| HeapNode::from_node(doc.as_node(), &alloc, &dedup));

            let reduced = match lhs_cloned {
                Some(lhs) => reduce(
                    LazyNode::Heap(lhs),
                    LazyNode::Node(&rhs),
                    rhs_valid,
                    &alloc,
                    &dedup,
                    prune,
                ),
                None => Ok(HeapNode::from_node(rhs.as_node(), &alloc, &dedup)),
            };

            match expect {
                Ok(expect) => {
                    let reduced = reduced.unwrap();
                    assert_eq!(
                        crate::compare(&reduced, &expect),
                        std::cmp::Ordering::Equal,
                        "reduced: {reduced:?} expected: {expect:?}",
                    );
                    lhs = Some(reduced)
                }
                Err(expect) => {
                    let reduced = reduced.unwrap_err();
                    let mut reduced: &dyn StdError = &reduced;

                    while let Some(r) = reduced.source() {
                        reduced = r;
                    }
                    assert_eq!(format!("{}", reduced), format!("{}", expect));
                }
            }
        }
    }
}

mod set;
