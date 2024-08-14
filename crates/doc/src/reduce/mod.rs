use super::{
    lazy::{LazyField, LazyNode},
    AsNode, BumpStr, Field, Fields, HeapField, HeapNode, Node, Pointer, SerPolicy, Valid,
};
use itertools::EitherOrBoth;
use std::cmp::Ordering;

pub mod strategy;
pub use strategy::Strategy;

mod parsed_number;
use parsed_number::ParsedNumber;

mod schema;
mod set;

pub static DEFAULT_STRATEGY: &Strategy = &Strategy::LastWriteWins(strategy::LastWriteWins {
    delete: false,
    associative: true,
});

#[derive(thiserror::Error, Debug, serde::Serialize)]
pub enum Error {
    #[error("encountered non-associative reduction in an unexpected context")]
    NotAssociative,
    #[error("'append' strategy expects arrays")]
    AppendWrongType,
    #[error("`sum` resulted in numeric overflow")]
    SumNumericOverflow,
    #[error("'sum' strategy expects numbers")]
    SumWrongType,
    #[error(
        "'json-schema-merge' strategy expects objects containing valid JSON schemas: {detail}"
    )]
    JsonSchemaMerge { detail: String },
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
        lhs: Option<LazyNode<'_, '_, L>>,
        rhs: LazyNode<'_, '_, R>,
    ) -> Self {
        let policy = SerPolicy::debug();
        Error::WithValues {
            lhs: serde_json::to_value(lhs.as_ref().map(|n| policy.on_lazy(n))).unwrap(),
            rhs: serde_json::to_value(policy.on_lazy(&rhs)).unwrap(),
            detail: Box::new(self),
        }
    }

    fn with_details<L: AsNode, R: AsNode>(
        self,
        loc: json::Location,
        lhs: Option<LazyNode<'_, '_, L>>,
        rhs: LazyNode<'_, '_, R>,
    ) -> Self {
        self.with_location(loc).with_values(lhs, rhs)
    }
}

type Result<T> = std::result::Result<T, Error>;

/// Reduce a RHS document validation into a preceding LHS document,
/// returning a reduced document and an indication of whether the
/// entire document is to be considered "deleted".
///
/// The RHS validation provides reduction annotation outcomes used in the reduction.
/// If `full`, then LHS is the root-most (or left-most) document in the reduction
/// sequence. Depending on the reduction strategy, additional work can be done
/// in this case (i.e., removing deleted locations) that isn't possible in an
/// associative reduction.
pub fn reduce<'alloc, N: AsNode>(
    lhs: LazyNode<'alloc, '_, N>,
    rhs: LazyNode<'alloc, '_, N>,
    rhs_valid: Valid,
    alloc: &'alloc bumpalo::Bump,
    full: bool,
) -> Result<(HeapNode<'alloc>, bool)> {
    let tape = rhs_valid.extract_reduce_annotations();
    let tape = &mut tape.as_slice();

    let reduced = Cursor {
        tape,
        loc: json::Location::Root,
        full,
        lhs: Some(lhs),
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
    lhs: Option<LazyNode<'alloc, 'l, L>>,
    rhs: LazyNode<'alloc, 'r, R>,
    alloc: &'alloc bumpalo::Bump,
}

type Index<'a> = &'a [(&'a Strategy, u64)];

impl<'alloc, L: AsNode, R: AsNode> Cursor<'alloc, '_, '_, '_, '_, L, R> {
    pub fn reduce(self) -> Result<(HeapNode<'alloc>, bool)> {
        let (strategy, _) = self.tape.first().unwrap();
        strategy.apply(self)
    }
}

fn count_nodes_lazy<N: AsNode>(v: &LazyNode<'_, '_, N>) -> usize {
    match v {
        LazyNode::Node(doc) => count_nodes(*doc),
        LazyNode::Heap(doc) => count_nodes(*doc),
    }
}

fn count_nodes<N: AsNode>(node: &N) -> usize {
    match node.as_node() {
        Node::Bool(_)
        | Node::Bytes(_)
        | Node::Float(_)
        | Node::NegInt(_)
        | Node::Null
        | Node::PosInt(_)
        | Node::String(_) => 1,

        Node::Array(v) => count_nodes_items(v),
        Node::Object(v) => count_nodes_fields::<N>(v),
    }
}

fn count_nodes_items<N: AsNode>(items: &[N]) -> usize {
    items.iter().fold(1, |c, vv| c + count_nodes(vv))
}

fn count_nodes_fields<N: AsNode>(fields: &N::Fields) -> usize {
    fields
        .iter()
        .fold(1, |c, field| c + count_nodes(field.value()))
}

fn reduce_prop<'alloc, L: AsNode, R: AsNode>(
    tape: &mut Index<'_>,
    loc: json::Location<'_>,
    full: bool,
    eob: EitherOrBoth<LazyField<'alloc, '_, L>, LazyField<'alloc, '_, R>>,
    alloc: &'alloc bumpalo::Bump,
) -> Result<(HeapField<'alloc>, bool)> {
    match eob {
        EitherOrBoth::Left(lhs) => Ok((lhs.into_heap_field(alloc), false)),
        EitherOrBoth::Right(rhs) => {
            let (property, rhs) = rhs.into_parts();

            // Map owned vs borrowed cases into BumpStr.
            let property = match property {
                Ok(archive) => BumpStr::from_str(archive, alloc),
                Err(heap) => heap,
            };

            let (value, delete) = Cursor::<'alloc, '_, '_, '_, '_, L, R> {
                tape,
                loc: loc.push_prop(property.as_str()),
                full,
                lhs: None,
                rhs,
                alloc,
            }
            .reduce()?;

            Ok((HeapField { property, value }, delete))
        }
        EitherOrBoth::Both(lhs, rhs) => {
            let (property, lhs, rhs) = match (lhs, rhs) {
                (LazyField::Heap(lhs), LazyField::Heap(rhs)) => (
                    lhs.property,
                    LazyNode::Heap(&lhs.value),
                    LazyNode::Heap(&rhs.value),
                ),
                (LazyField::Heap(lhs), LazyField::Node(rhs)) => (
                    lhs.property,
                    LazyNode::Heap(&lhs.value),
                    LazyNode::Node(rhs.value()),
                ),
                (LazyField::Node(lhs), LazyField::Heap(rhs)) => (
                    rhs.property,
                    LazyNode::Node(lhs.value()),
                    LazyNode::Heap(&rhs.value),
                ),
                (LazyField::Node(lhs), LazyField::Node(rhs)) => (
                    BumpStr::from_str(lhs.property(), alloc),
                    LazyNode::Node(lhs.value()),
                    LazyNode::Node(rhs.value()),
                ),
            };

            let (value, delete) = Cursor {
                tape,
                loc: loc.push_prop(&property),
                full,
                lhs: Some(lhs),
                rhs,
                alloc,
            }
            .reduce()?;

            Ok((HeapField { property, value }, delete))
        }
    }
}

fn reduce_item<'alloc, L: AsNode, R: AsNode>(
    tape: &mut Index<'_>,
    loc: json::Location<'_>,
    full: bool,
    eob: EitherOrBoth<(usize, LazyNode<'alloc, '_, L>), (usize, LazyNode<'alloc, '_, R>)>,
    alloc: &'alloc bumpalo::Bump,
) -> Result<(HeapNode<'alloc>, bool)> {
    match eob {
        EitherOrBoth::Left((_, lhs)) => Ok((lhs.into_heap_node(alloc), false)),
        EitherOrBoth::Right((index, rhs)) => Cursor::<'alloc, '_, '_, '_, '_, L, R> {
            tape,
            loc: loc.push_item(index),
            full,
            lhs: None,
            rhs,
            alloc,
        }
        .reduce(),
        EitherOrBoth::Both((_, lhs), (index, rhs)) => Cursor {
            tape,
            loc: loc.push_item(index),
            full,
            lhs: Some(lhs),
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
fn compare_key<L: AsNode, R: AsNode>(key: &[Pointer], lhs: &L, rhs: &R) -> Ordering {
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

fn compare_key_lazy<L: AsNode, R: AsNode>(
    key: &[Pointer],
    lhs: &LazyNode<'_, '_, L>,
    rhs: &LazyNode<'_, '_, R>,
) -> Ordering {
    match (lhs, rhs) {
        (LazyNode::Heap(lhs), LazyNode::Heap(rhs)) => compare_key(key, *lhs, *rhs),
        (LazyNode::Heap(lhs), LazyNode::Node(rhs)) => compare_key(key, *lhs, *rhs),
        (LazyNode::Node(lhs), LazyNode::Heap(rhs)) => compare_key(key, *lhs, *rhs),
        (LazyNode::Node(lhs), LazyNode::Node(rhs)) => compare_key(key, *lhs, *rhs),
    }
}

fn compare_lazy<L: AsNode, R: AsNode>(
    lhs: &LazyNode<'_, '_, L>,
    rhs: &LazyNode<'_, '_, R>,
) -> Ordering {
    match (lhs, rhs) {
        (LazyNode::Heap(lhs), LazyNode::Heap(rhs)) => crate::compare(*lhs, *rhs),
        (LazyNode::Heap(lhs), LazyNode::Node(rhs)) => crate::compare(*lhs, *rhs),
        (LazyNode::Node(lhs), LazyNode::Heap(rhs)) => crate::compare(*lhs, *rhs),
        (LazyNode::Node(lhs), LazyNode::Node(rhs)) => crate::compare(*lhs, *rhs),
    }
}

/// merge_patch_schema returns a JSON-Schema implementing the RFC-7396 Merge patch algorithm.
pub fn merge_patch_schema() -> serde_json::Value {
    serde_json::json!({
        "$id": "flow://merge-patch-schema",
        "oneOf": [
            {
                "type": "object",
                "reduce": {"strategy": "merge"},
                "additionalProperties": {"$ref": "flow://merge-patch-schema"}
            },
            {
                "type": "null",
                "reduce": {
                    "strategy": "lastWriteWins",
                    "delete": true,
                }
            },
            {
                "type": ["array", "boolean", "number", "string"]
            }
        ]
    })
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
            assert_eq!(count_nodes(&fixture), expect);
            assert_eq!(count_nodes(&HeapNode::from_node(&fixture, &alloc)), expect);
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
            let (rhs, expect, full) = match case {
                Partial { rhs, expect } => (rhs, expect, false),
                Full { rhs, expect } => (rhs, expect, true),
            };
            let rhs_valid = validator.validate(None, &rhs).unwrap().ok().unwrap();

            let reduced = match &lhs {
                Some(lhs) => reduce(
                    LazyNode::Heap(lhs),
                    LazyNode::Node(&rhs),
                    rhs_valid,
                    &alloc,
                    full,
                ),
                None => Ok((HeapNode::from_node(&rhs, &alloc), false)),
            };

            match expect {
                Ok(expect) => {
                    let (reduced, _delete) = reduced.unwrap();

                    // Assert that the serialized string representations are identical.
                    // This catches differences like `1.0` vs `1`, which `compare` would
                    // ignore.
                    let expect_str = serde_json::to_string(&expect).unwrap();
                    let actual_str =
                        serde_json::to_string(&SerPolicy::noop().on(&reduced)).unwrap();
                    assert_eq!(
                        expect_str, actual_str,
                        "comparison failed:\nreduced:\n{actual_str}\nexpected:\n{expect_str}\n"
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
