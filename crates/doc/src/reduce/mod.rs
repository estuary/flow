use super::{
    lazy::{LazyDestructured, LazyField, LazyNode},
    AsNode, BumpStr, Field, Fields, HeapField, HeapNode, Node, Pointer, Valid,
};
use itertools::EitherOrBoth;
use std::cmp::Ordering;

pub mod strategy;
pub use strategy::Strategy;

mod set;

pub static DEFAULT_STRATEGY: &Strategy = &Strategy::LastWriteWins;

#[derive(thiserror::Error, Debug, serde::Serialize)]
pub enum Error {
    #[error("'append' strategy expects arrays")]
    AppendWrongType,
    #[error("`sum` resulted in numeric overflow")]
    SumNumericOverflow,
    #[error("'sum' strategy expects numbers")]
    SumWrongType,
    #[error("'json-schema-merge' strategy expects objects containing valid JSON schemas. {}", .detail.as_deref().unwrap_or_default())]
    JsonSchemaMergeWrongType { detail: Option<String> },
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
        | HeapNode::String(_) => 1,
        HeapNode::Array(v) => v.iter().fold(1, |c, vv| c + count_nodes_heap(vv)),
        HeapNode::Object(v) => v
            .iter()
            .fold(1, |c, field| c + count_nodes_heap(&field.value)),
    }
}

fn reduce_prop<'alloc, L: AsNode, R: AsNode>(
    tape: &mut Index<'_>,
    loc: json::Location<'_>,
    full: bool,
    eob: EitherOrBoth<LazyField<'alloc, '_, L>, LazyField<'alloc, '_, R>>,
    alloc: &'alloc bumpalo::Bump,
) -> Result<HeapField<'alloc>> {
    match eob {
        EitherOrBoth::Left(lhs) => Ok(lhs.into_heap_field(alloc)),
        EitherOrBoth::Right(rhs) => {
            let rhs = rhs.into_heap_field(alloc);
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
                (LazyField::Heap(lhs), LazyField::Node(rhs)) => (
                    lhs.property,
                    LazyNode::Heap(lhs.value),
                    LazyNode::Node(rhs.value()),
                ),
                (LazyField::Node(lhs), LazyField::Heap(rhs)) => (
                    rhs.property,
                    LazyNode::Node(lhs.value()),
                    LazyNode::Heap(rhs.value),
                ),
                (LazyField::Node(lhs), LazyField::Node(rhs)) => (
                    BumpStr::from_str(lhs.property(), alloc),
                    LazyNode::Node(lhs.value()),
                    LazyNode::Node(rhs.value()),
                ),
            };

            let value = Cursor {
                tape,
                loc: loc.push_prop(&property),
                full,
                lhs,
                rhs,
                alloc,
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
) -> Result<HeapNode<'alloc>> {
    match eob {
        EitherOrBoth::Left((_, lhs)) => Ok(lhs.into_heap_node(alloc)),
        EitherOrBoth::Right((_, rhs)) => {
            let rhs = rhs.into_heap_node(alloc);
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
        }
        .reduce(),
    }
}

// Compare the deep ordering of `lhs` and `rhs` with respect to a composite key,
// specified as a slice of Pointers relative to the respective document roots.
// Pointers that do not exist in a document order before any JSON value that does exist.
//
// WARNING: This routine should *only* be used in the context of schema reductions.
// When comparing document keys, use an Extractor which also considers default value annotations.
//
fn compare_key<'s, 'l, 'r, L: AsNode, R: AsNode>(
    key: &'s [Pointer],
    lhs: &'l L,
    rhs: &'r R,
) -> Ordering {
    key.iter()
        .map(|ptr| match (ptr.query(lhs), ptr.query(rhs)) {
            (Some(lhs), Some(rhs)) => crate::compare(lhs, rhs),
            (None, Some(_)) => Ordering::Less,
            (Some(_), None) => Ordering::Greater,
            (_, _) => Ordering::Equal,
        })
        .find(|o| *o != Ordering::Equal)
        .unwrap_or(Ordering::Equal)
}
fn compare_key_lazy<'alloc, 'l, 'r, L: AsNode, R: AsNode>(
    key: &[Pointer],
    lhs: &LazyNode<'alloc, 'l, L>,
    rhs: &LazyNode<'alloc, 'r, R>,
) -> Ordering {
    match (lhs, rhs) {
        (LazyNode::Heap(lhs), LazyNode::Heap(rhs)) => compare_key(key, lhs, rhs),
        (LazyNode::Heap(lhs), LazyNode::Node(rhs)) => compare_key(key, lhs, *rhs),
        (LazyNode::Node(lhs), LazyNode::Heap(rhs)) => compare_key(key, *lhs, rhs),
        (LazyNode::Node(lhs), LazyNode::Node(rhs)) => compare_key(key, *lhs, *rhs),
    }
}

#[cfg(test)]
pub mod test {
    use super::*;

    use crate::Validator;
    use json::schema::build::build_schema;
    pub use serde_json::{json, Value};
    use std::error::Error as StdError;

    #[test]
    fn test_node_counting() {
        let alloc = HeapNode::new_allocator();

        let test_case = |fixture: Value, expect: usize| {
            assert_eq!(count_nodes_generic(&fixture.as_node()), expect);
            assert_eq!(
                count_nodes_heap(&HeapNode::from_node(fixture.as_node(), &alloc)),
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
        let mut validator = Validator::new(build_schema(curi, &schema).unwrap()).unwrap();
        let alloc = HeapNode::new_allocator();
        let mut lhs: Option<HeapNode<'_>> = None;

        for case in cases {
            let (rhs, expect, prune) = match case {
                Partial { rhs, expect } => (rhs, expect, false),
                Full { rhs, expect } => (rhs, expect, true),
            };
            let rhs_valid = validator.validate(None, &rhs).unwrap().ok().unwrap();

            let lhs_cloned = lhs
                .as_ref()
                .map(|doc| HeapNode::from_node(doc.as_node(), &alloc));

            let reduced = match lhs_cloned {
                Some(lhs) => reduce(
                    LazyNode::Heap(lhs),
                    LazyNode::Node(&rhs),
                    rhs_valid,
                    &alloc,
                    prune,
                ),
                None => Ok(HeapNode::from_node(rhs.as_node(), &alloc)),
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

    #[test]
    fn test_compare_objects() {
        let d1 = &json!({"a": 1, "b": 2, "c": 3});
        let d2 = &json!({"a": 2, "b": 1, "c": 3});

        let (empty, a, b, c) = (|| "".into(), || "/a".into(), || "/b".into(), || "/c".into());

        // No pointers => always equal.
        assert_eq!(compare_key(&[] as &[Pointer], d1, d2), Ordering::Equal);
        // Deep compare of document roots.
        assert_eq!(compare_key(&[empty()], d1, d2), Ordering::Less);
        // Simple key ordering.
        assert_eq!(compare_key(&[a()], d1, d2), Ordering::Less);
        assert_eq!(compare_key(&[b()], d1, d2), Ordering::Greater);
        assert_eq!(compare_key(&[c()], d1, d2), Ordering::Equal);
        // Composite key ordering.
        assert_eq!(compare_key(&[c(), a()], d1, d2), Ordering::Less);
        assert_eq!(compare_key(&[c(), b()], d1, d2), Ordering::Greater);
        assert_eq!(compare_key(&[c(), c()], d1, d2), Ordering::Equal);
        assert_eq!(compare_key(&[c(), c(), c(), a()], d1, d2), Ordering::Less);
    }

    #[test]
    fn test_compare_arrays() {
        let d1 = &json!([1, 2, 3]);
        let d2 = &json!([2, 1, 3]);

        let (empty, zero, one, two) =
            (|| "".into(), || "/0".into(), || "/1".into(), || "/2".into());

        // No pointers => always equal.
        assert_eq!(compare_key(&[] as &[Pointer], d1, d2), Ordering::Equal);
        // Deep compare of document roots.
        assert_eq!(compare_key(&[empty()], d1, d2), Ordering::Less);
        // Simple key ordering.
        assert_eq!(compare_key(&[zero()], d1, d2), Ordering::Less);
        assert_eq!(compare_key(&[one()], d1, d2), Ordering::Greater);
        assert_eq!(compare_key(&[two()], d1, d2), Ordering::Equal);
        // Composite key ordering.
        assert_eq!(compare_key(&[two(), zero()], d1, d2), Ordering::Less);
        assert_eq!(compare_key(&[two(), one()], d1, d2), Ordering::Greater);
        assert_eq!(compare_key(&[two(), two()], d1, d2), Ordering::Equal);
    }

    #[test]
    fn test_compare_missing() {
        let d1 = &json!({"a": null, "c": 3});
        let d2 = &json!({"b": 2});

        assert_eq!(
            compare_key(&["/does/not/exist".into()], d1, d2),
            Ordering::Equal
        );
        // Key exists at |d1| but not |d2|.
        assert_eq!(compare_key(&["/c".into()], d1, d2), Ordering::Greater);
        // Key exists at |d2| but not |d1|.
        assert_eq!(compare_key(&["/b".into()], d1, d2), Ordering::Less);
        // Key exists at |d1| but not |d2|.
        assert_eq!(compare_key(&["/a".into()], d1, d2), Ordering::Greater);
    }
}
