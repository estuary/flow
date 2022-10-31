use super::{
    count_nodes, count_nodes_generic, count_nodes_heap, reduce_item, reduce_prop, Cursor, Error,
    Result,
};
use crate::{
    heap::BumpVec,
    lazy::{LazyArray, LazyDestructured, LazyObject},
    AsNode, HeapNode, Node, Pointer,
};

#[derive(serde::Serialize, serde::Deserialize, Debug)]
#[serde(tag = "strategy", deny_unknown_fields, rename_all = "camelCase")]
pub enum Strategy {
    /// Append each item of RHS to the end of LHS. RHS must be an array.
    /// LHS must be an array, or may be null, in which case no append is
    /// done and the reduction is a no-op.
    Append,
    /// FirstWriteWins keeps the LHS value.
    FirstWriteWins,
    /// LastWriteWins takes the RHS value.
    LastWriteWins,
    /// Maximize keeps the greater of the LHS & RHS.
    /// A provided key, if present, determines the relative ordering.
    /// If values are equal, they're deeply merged.
    Maximize(Maximize),
    /// Merge the LHS and RHS by recursively reducing shared document locations.
    /// The LHS and RHS must either both be Objects, or both be Arrays.
    ///
    /// If LHS and RHS are Arrays and a merge key is provide, the Arrays *must* be
    /// pre-sorted and de-duplicated by that key. Merge then performs a deep sorted
    /// merge of their respective items, as ordered by the key.
    /// Note that a key of [""] can be applied to use natural item ordering.
    ///
    /// If LHS and RHS are both Arrays and a key is not provided, items of each index
    /// in LHS and RHS are merged together, extending the shorter of the two by taking
    /// items of the longer.
    ///
    /// If LHS and RHS are both Objects then it performs a deep merge of each property.
    Merge(Merge),
    /// Minimize keeps the smaller of the LHS & RHS.
    /// A provided key, if present, determines the relative ordering.
    /// If values are equal, they're deeply merged.
    Minimize(Minimize),
    /// Interpret this location as an update to a set.
    ///
    /// The location *must* be an object having (only) "add", "intersect",
    /// and "remove" properties. Any single property is always allowed.
    ///
    /// An instance with "intersect" and "add" is allowed, and is interpreted
    /// as applying the intersection to the base set, followed by a union of
    /// the additions.
    ///
    /// An instance with "remove" and "add" is also allowed, and is interpreted
    /// as applying the removals to the base set, followed by a union of
    /// the additions.
    ///
    /// "remove" and "intersect" within the same instance is prohibited.
    ///
    /// Set additions are deeply merged. This makes sets behave as associative
    /// maps, where the "value" of a set member can be updated by adding it to
    /// set with a reducible update.
    ///
    /// Set components may be objects, in which case the object property is the
    /// set key, or arrays which are ordered using the Set's key extractor.
    /// Use a key extractor of [""] to apply the natural ordering of scalar
    /// values stored in a sorted array.
    ///
    /// Whether arrays or objects are used, the selected type must always be
    /// consistent across the "add" / "intersect" / "remove" terms of both
    /// sides of the reduction.
    Set(super::set::Set),
    /// Sum the LHS and RHS, both of which must be numbers.
    /// Sum will fail if the operation would result in a numeric overflow
    /// (in other words, the numbers become too large to be represented).
    ///
    /// In the future, we may allow for arbitrary-sized integer and
    /// floating-point representations which use a string encoding scheme.
    Sum,
}

impl std::convert::TryFrom<&serde_json::Value> for Strategy {
    type Error = serde_json::Error;

    fn try_from(v: &serde_json::Value) -> std::result::Result<Self, Self::Error> {
        <Strategy as serde::Deserialize>::deserialize(v)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Maximize {
    #[serde(default)]
    key: Vec<Pointer>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Merge {
    #[serde(default)]
    key: Vec<Pointer>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Minimize {
    #[serde(default)]
    key: Vec<Pointer>,
}

impl Strategy {
    pub fn apply<'alloc, 'schema, L: AsNode, R: AsNode>(
        &'schema self,
        cur: Cursor<'alloc, 'schema, '_, '_, '_, L, R>,
    ) -> Result<HeapNode<'alloc>> {
        match self {
            Strategy::Append => Self::append(cur),
            Strategy::FirstWriteWins => Self::first_write_wins(cur),
            Strategy::LastWriteWins => Self::last_write_wins(cur),
            Strategy::Maximize(max) => Self::maximize(cur, max),
            Strategy::Merge(merge) => Self::merge(cur, merge),
            Strategy::Minimize(min) => Self::minimize(cur, min),
            Strategy::Set(set) => set.apply(cur),
            Strategy::Sum => Self::sum(cur),
        }
    }

    fn append<'alloc, L: AsNode, R: AsNode>(
        cur: Cursor<'alloc, '_, '_, '_, '_, L, R>,
    ) -> Result<HeapNode<'alloc>> {
        let Cursor {
            tape,
            loc,
            lhs,
            rhs,
            alloc,
            dedup,
            full: _,
        } = cur;

        match (lhs.destructure(), rhs.destructure()) {
            (LazyDestructured::Array(lhs), LazyDestructured::Array(rhs)) => {
                *tape = &tape[1..]; // Increment for self.

                let mut arr = BumpVec::with_capacity_in(lhs.len() + rhs.len(), alloc);

                for lhs in lhs.into_iter() {
                    arr.0.push(lhs.into_heap_node(alloc, dedup));
                }
                for rhs in rhs.into_iter() {
                    let rhs = rhs.into_heap_node(alloc, dedup);
                    *tape = &tape[count_nodes_heap(&rhs)..];
                    arr.0.push(rhs)
                }

                Ok(HeapNode::Array(arr))
            }

            // Merge of Null <= Array (takes the null LHS).
            (
                LazyDestructured::ScalarNode(Node::Null)
                | LazyDestructured::ScalarHeap(HeapNode::Null),
                LazyDestructured::Array(LazyArray::Heap(arr)),
            ) => {
                *tape = &tape[count_nodes_heap(&HeapNode::Array(arr))..];
                Ok(HeapNode::Null)
            }
            (
                LazyDestructured::ScalarNode(Node::Null)
                | LazyDestructured::ScalarHeap(HeapNode::Null),
                LazyDestructured::Array(LazyArray::Node(arr)),
            ) => {
                *tape = &tape[count_nodes_generic(&Node::Array(arr))..];
                Ok(HeapNode::Null)
            }

            (lhs, rhs) => Err(Error::with_details(Error::AppendWrongType, loc, lhs, rhs)),
        }
    }

    fn first_write_wins<'alloc, L: AsNode, R: AsNode>(
        cur: Cursor<'alloc, '_, '_, '_, '_, L, R>,
    ) -> Result<HeapNode<'alloc>> {
        *cur.tape = &cur.tape[count_nodes(&cur.rhs)..];
        Ok(cur.lhs.into_heap_node(cur.alloc, cur.dedup))
    }

    fn last_write_wins<'alloc, L: AsNode, R: AsNode>(
        cur: Cursor<'alloc, '_, '_, '_, '_, L, R>,
    ) -> Result<HeapNode<'alloc>> {
        let rhs = cur.rhs.into_heap_node(cur.alloc, cur.dedup);
        *cur.tape = &cur.tape[count_nodes_heap(&rhs)..];
        Ok(rhs)
    }

    fn min_max_helper<'alloc, L: AsNode, R: AsNode>(
        cur: Cursor<'alloc, '_, '_, '_, '_, L, R>,
        key: &[Pointer],
        reverse: bool,
    ) -> Result<HeapNode<'alloc>> {
        let Cursor {
            tape,
            loc,
            full,
            lhs,
            rhs,
            alloc,
            dedup,
        } = cur;

        let ord = match (key.is_empty(), reverse) {
            (false, false) => lhs.compare(key, &rhs),
            (false, true) => rhs.compare(key, &lhs),
            (true, false) => lhs.compare(&[Pointer::empty()], &rhs),
            (true, true) => rhs.compare(&[Pointer::empty()], &lhs),
        };

        use std::cmp::Ordering;

        match ord {
            Ordering::Less => {
                *tape = &tape[count_nodes(&rhs)..];
                Ok(lhs.into_heap_node(alloc, dedup))
            }
            Ordering::Greater => {
                let rhs = rhs.into_heap_node(alloc, dedup);
                *tape = &tape[count_nodes_heap(&rhs)..];
                Ok(rhs)
            }
            Ordering::Equal if key.is_empty() => {
                let rhs = rhs.into_heap_node(alloc, dedup);
                *tape = &tape[count_nodes_heap(&rhs)..];
                Ok(rhs)
            }
            Ordering::Equal => {
                // Lhs and RHS are equal on the chosen key. Deeply merge them.
                let cur = Cursor {
                    tape,
                    loc,
                    full,
                    lhs,
                    rhs,
                    alloc,
                    dedup,
                };
                Self::merge_with_key(cur, &[])
            }
        }
    }

    fn minimize<'alloc, L: AsNode, R: AsNode>(
        cur: Cursor<'alloc, '_, '_, '_, '_, L, R>,
        min: &Minimize,
    ) -> Result<HeapNode<'alloc>> {
        Self::min_max_helper(cur, &min.key, false)
    }

    fn maximize<'alloc, L: AsNode, R: AsNode>(
        cur: Cursor<'alloc, '_, '_, '_, '_, L, R>,
        max: &Maximize,
    ) -> Result<HeapNode<'alloc>> {
        Self::min_max_helper(cur, &max.key, true)
    }

    fn sum<'alloc, L: AsNode, R: AsNode>(
        cur: Cursor<'alloc, '_, '_, '_, '_, L, R>,
    ) -> Result<HeapNode<'alloc>> {
        let Cursor {
            tape,
            loc,
            full: _,
            lhs,
            rhs,
            alloc: _,
            dedup: _,
        } = cur;

        let (lhs, rhs) = (lhs.destructure(), rhs.destructure());

        let ln = match &lhs {
            LazyDestructured::ScalarNode(Node::Number(n)) => *n,
            LazyDestructured::ScalarHeap(HeapNode::PosInt(n)) => json::Number::Unsigned(*n),
            LazyDestructured::ScalarHeap(HeapNode::NegInt(n)) => json::Number::Signed(*n),
            LazyDestructured::ScalarHeap(HeapNode::Float(n)) => json::Number::Float(*n),
            _ => return Err(Error::with_details(Error::SumWrongType, loc, lhs, rhs)),
        };
        let rn = match &rhs {
            LazyDestructured::ScalarNode(Node::Number(n)) => *n,
            LazyDestructured::ScalarHeap(HeapNode::PosInt(n)) => json::Number::Unsigned(*n),
            LazyDestructured::ScalarHeap(HeapNode::NegInt(n)) => json::Number::Signed(*n),
            LazyDestructured::ScalarHeap(HeapNode::Float(n)) => json::Number::Float(*n),
            _ => return Err(Error::with_details(Error::SumWrongType, loc, lhs, rhs)),
        };

        *tape = &tape[1..];

        match json::Number::checked_add(ln, rn) {
            Some(json::Number::Float(n)) => Ok(HeapNode::Float(n)),
            Some(json::Number::Unsigned(n)) => Ok(HeapNode::PosInt(n)),
            Some(json::Number::Signed(n)) => Ok(HeapNode::NegInt(n)),
            None => Err(Error::with_details(
                Error::SumNumericOverflow,
                loc,
                lhs,
                rhs,
            )),
        }
    }

    fn merge<'alloc, L: AsNode, R: AsNode>(
        cur: Cursor<'alloc, '_, '_, '_, '_, L, R>,
        merge: &Merge,
    ) -> Result<HeapNode<'alloc>> {
        Self::merge_with_key(cur, &merge.key)
    }

    fn merge_with_key<'alloc, L: AsNode, R: AsNode>(
        cur: Cursor<'alloc, '_, '_, '_, '_, L, R>,
        key: &[Pointer],
    ) -> Result<HeapNode<'alloc>> {
        let Cursor {
            tape,
            loc,
            lhs,
            rhs,
            alloc,
            dedup,
            full,
        } = cur;

        match (lhs.destructure(), rhs.destructure()) {
            // Merge of Object <= Object.
            (LazyDestructured::Object(lhs), LazyDestructured::Object(rhs)) => {
                *tape = &tape[1..]; // Increment for self.

                let mut fields = BumpVec::with_capacity_in(lhs.len() + rhs.len(), alloc);
                for field in
                    itertools::merge_join_by(lhs.into_iter(), rhs.into_iter(), |lhs, rhs| {
                        lhs.property().cmp(rhs.property())
                    })
                    .map(|eob| reduce_prop(tape, loc, full, eob, alloc, dedup))
                {
                    fields.0.push(field?);
                }
                Ok(HeapNode::Object(fields))
            }
            // Merge of Array <= Array.
            (LazyDestructured::Array(lhs), LazyDestructured::Array(rhs)) => {
                *tape = &tape[1..]; // Increment for self.

                let mut arr = BumpVec::with_capacity_in(lhs.len() + rhs.len(), alloc);
                for item in itertools::merge_join_by(
                    lhs.into_iter().enumerate(),
                    rhs.into_iter().enumerate(),
                    |(lhs_ind, lhs), (rhs_ind, rhs)| {
                        if key.is_empty() {
                            lhs_ind.cmp(rhs_ind)
                        } else {
                            lhs.compare(key, rhs)
                        }
                    },
                )
                .map(|eob| reduce_item(tape, loc, full, eob, alloc, dedup))
                {
                    arr.0.push(item?);
                }
                Ok(HeapNode::Array(arr))
            }

            // Merge of Null <= Array or Object (takes the null LHS).
            (
                LazyDestructured::ScalarNode(Node::Null)
                | LazyDestructured::ScalarHeap(HeapNode::Null),
                LazyDestructured::Array(LazyArray::Heap(arr)),
            ) => {
                *tape = &tape[count_nodes_heap(&HeapNode::Array(arr))..];
                Ok(HeapNode::Null)
            }
            (
                LazyDestructured::ScalarNode(Node::Null)
                | LazyDestructured::ScalarHeap(HeapNode::Null),
                LazyDestructured::Array(LazyArray::Node(arr)),
            ) => {
                *tape = &tape[count_nodes_generic(&Node::Array(arr))..];
                Ok(HeapNode::Null)
            }
            (
                LazyDestructured::ScalarNode(Node::Null)
                | LazyDestructured::ScalarHeap(HeapNode::Null),
                LazyDestructured::Object(LazyObject::Heap(fields)),
            ) => {
                *tape = &tape[count_nodes_heap(&HeapNode::Object(fields))..];
                Ok(HeapNode::Null)
            }
            (
                LazyDestructured::ScalarNode(Node::Null)
                | LazyDestructured::ScalarHeap(HeapNode::Null),
                LazyDestructured::Object(LazyObject::Node(fields)),
            ) => {
                *tape = &tape[count_nodes_generic(&Node::Object::<R>(fields))..];
                Ok(HeapNode::Null)
            }

            (lhs, rhs) => Err(Error::with_details(Error::MergeWrongType, loc, lhs, rhs)),
        }
    }
}

#[cfg(test)]
mod test {
    use super::super::test::*;
    use super::*;

    #[test]
    fn test_append_array() {
        run_reduce_cases(
            json!({
                "if": { "type": "null" },
                "then": { "reduce": { "strategy": "lastWriteWins" } },
                "else": { "reduce": { "strategy": "append" } },
            }),
            vec![
                Partial {
                    rhs: json!([]),
                    expect: Ok(json!([])),
                },
                // Non-array RHS returns an error.
                Partial {
                    rhs: json!("whoops"),
                    expect: Err(Error::AppendWrongType),
                },
                Partial {
                    rhs: json!([0, 1]),
                    expect: Ok(json!([0, 1])),
                },
                Partial {
                    rhs: json!([2, 3, 4]),
                    expect: Ok(json!([0, 1, 2, 3, 4])),
                },
                Partial {
                    rhs: json!([-1, "a"]),
                    expect: Ok(json!([0, 1, 2, 3, 4, -1, "a"])),
                },
                Partial {
                    rhs: json!({}),
                    expect: Err(Error::AppendWrongType),
                },
                Partial {
                    rhs: json!(null),
                    expect: Ok(json!(null)),
                },
                // Append with null LHS is a no-op.
                Partial {
                    rhs: json!([5, 6, 4]),
                    expect: Ok(json!(null)),
                },
            ],
        )
    }

    #[test]
    fn test_last_write_wins() {
        run_reduce_cases(
            json!(true),
            vec![
                Partial {
                    rhs: json!("foo"),
                    expect: Ok(json!("foo")),
                },
                Partial {
                    rhs: json!({"n": 42}),
                    expect: Ok(json!({"n": 42})),
                },
                Partial {
                    rhs: json!(null),
                    expect: Ok(json!(null)),
                },
            ],
        )
    }

    #[test]
    fn test_first_write_wins() {
        run_reduce_cases(
            json!({ "reduce": { "strategy": "firstWriteWins" } }),
            vec![
                Partial {
                    rhs: json!("foo"),
                    expect: Ok(json!("foo")),
                },
                Partial {
                    rhs: json!({"n": 42}),
                    expect: Ok(json!("foo")),
                },
                Partial {
                    rhs: json!(null),
                    expect: Ok(json!("foo")),
                },
            ],
        )
    }

    #[test]
    fn test_minimize_simple() {
        run_reduce_cases(
            json!({ "reduce": { "strategy": "minimize" } }),
            vec![
                Partial {
                    rhs: json!(3),
                    expect: Ok(json!(3)),
                },
                Partial {
                    rhs: json!(4),
                    expect: Ok(json!(3)),
                },
                Partial {
                    rhs: json!(3),
                    expect: Ok(json!(3)),
                },
                Partial {
                    rhs: json!(2),
                    expect: Ok(json!(2)),
                },
            ],
        )
    }

    #[test]
    fn test_maximize_simple() {
        run_reduce_cases(
            json!({ "reduce": { "strategy": "maximize" } }),
            vec![
                Partial {
                    rhs: json!(3),
                    expect: Ok(json!(3)),
                },
                Partial {
                    rhs: json!(4),
                    expect: Ok(json!(4)),
                },
                Partial {
                    rhs: json!(4),
                    expect: Ok(json!(4)),
                },
                Partial {
                    rhs: json!(2),
                    expect: Ok(json!(4)),
                },
            ],
        )
    }

    #[test]
    fn test_minimize_with_deep_merge() {
        run_reduce_cases(
            json!({
                "properties": {
                    "n": {"reduce": {"strategy": "sum"}}
                },
                "reduce": {
                    "strategy": "minimize",
                    "key": ["/k"],
                },
            }),
            vec![
                Partial {
                    rhs: json!({"k": 3, "n": 1}),
                    expect: Ok(json!({"k": 3, "n": 1})),
                },
                Partial {
                    rhs: json!({"k": 4, "n": 1}),
                    expect: Ok(json!({"k": 3, "n": 1})),
                },
                Partial {
                    rhs: json!({"k": 3, "n": 1, "!": true}),
                    expect: Ok(json!({"k": 3, "n": 2, "!": true})),
                },
                Partial {
                    rhs: json!({"k": 4, "n": 1, "!": false}),
                    expect: Ok(json!({"k": 3, "n": 2, "!": true})),
                },
                Partial {
                    rhs: json!({"k": 3, "n": 1}),
                    expect: Ok(json!({"k": 3, "n": 3, "!": true})),
                },
                Partial {
                    rhs: json!({"k": 2, "n": 1}),
                    expect: Ok(json!({"k": 2, "n": 1})),
                },
                // Missing key orders as 'null'.
                Partial {
                    rhs: json!({"n": 1, "whoops": true}),
                    expect: Ok(json!({"n": 1, "whoops": true})),
                },
                Partial {
                    rhs: json!({"k": null, "n": 1}),
                    expect: Ok(json!({"k": null, "n": 2, "whoops": true})),
                },
                // Keys are technically equal, and it attempts to deep-merge.
                Partial {
                    rhs: json!(42),
                    expect: Err(Error::MergeWrongType),
                },
            ],
        )
    }

    #[test]
    fn test_maximize_with_deep_merge() {
        run_reduce_cases(
            json!({
                "items": [
                    {"reduce": {"strategy": "sum"}},
                    {"type": "integer"},
                ],
                "reduce": {
                    "strategy": "maximize",
                    "key": ["/1"],
                },
            }),
            vec![
                Partial {
                    rhs: json!([1, 3]),
                    expect: Ok(json!([1, 3])),
                },
                Partial {
                    rhs: json!([1, 4]),
                    expect: Ok(json!([1, 4])),
                },
                Partial {
                    rhs: json!([1, 3]),
                    expect: Ok(json!([1, 4])),
                },
                Partial {
                    rhs: json!([1, 4, '.']),
                    expect: Ok(json!([2, 4, '.'])),
                },
                // It returns a delegated merge error on equal keys.
                Partial {
                    rhs: json!({"1": 4}),
                    expect: Err(Error::MergeWrongType),
                },
                Partial {
                    rhs: json!([1, 2, "!"]),
                    expect: Ok(json!([2, 4, '.'])),
                },
                Partial {
                    rhs: json!([1, 4, ':']),
                    expect: Ok(json!([3, 4, ':'])),
                },
                // Missing key orders as 'null'.
                Partial {
                    rhs: json!([]),
                    expect: Ok(json!([3, 4, ':'])),
                },
                Partial {
                    rhs: json!(32),
                    expect: Ok(json!([3, 4, ':'])),
                },
            ],
        )
    }

    #[test]
    fn test_sum() {
        run_reduce_cases(
            json!({ "reduce": { "strategy": "sum" } }),
            vec![
                Partial {
                    rhs: json!(0),
                    expect: Ok(json!(0)),
                },
                // Non-numeric RHS returns an error.
                Partial {
                    rhs: json!("whoops"),
                    expect: Err(Error::SumWrongType),
                },
                // Takes initial value.
                Partial {
                    rhs: json!(123),
                    expect: Ok(json!(123)),
                },
                // Add unsigned.
                Partial {
                    rhs: json!(45),
                    expect: Ok(json!(168)),
                },
                // Sum results in overflow.
                Partial {
                    rhs: json!(u64::MAX - 32),
                    expect: Err(Error::SumNumericOverflow),
                },
                // Add signed.
                Partial {
                    rhs: json!(-70),
                    expect: Ok(json!(98)),
                },
                // Add float.
                Partial {
                    rhs: json!(0.1),
                    expect: Ok(json!(98.1)),
                },
                // Back to f64 zero.
                Partial {
                    rhs: json!(-98.1),
                    expect: Ok(json!(0.0)),
                },
                // Add maximum f64.
                Partial {
                    rhs: json!(std::f64::MAX),
                    expect: Ok(json!(std::f64::MAX)),
                },
                // Number which overflows returns an error.
                Partial {
                    rhs: json!(std::f64::MAX / 10.0),
                    expect: Err(Error::SumNumericOverflow),
                },
                // Sometimes changes are too small to represent.
                Partial {
                    rhs: json!(-1.0),
                    expect: Ok(json!(std::f64::MAX)),
                },
                // Sometimes they aren't.
                Partial {
                    rhs: json!(std::f64::MIN / 2.0),
                    expect: Ok(json!(std::f64::MAX / 2.)),
                },
                Partial {
                    rhs: json!(std::f64::MIN / 2.0),
                    expect: Ok(json!(0.0)),
                },
                // Non-numeric type (now with LHS) returns an error.
                Partial {
                    rhs: json!("whoops"),
                    expect: Err(Error::SumWrongType),
                },
            ],
        );
    }

    #[test]
    fn test_merge_array_in_place() {
        run_reduce_cases(
            json!({
                "items": {
                    "reduce": { "strategy": "maximize" },
                },
                "reduce": { "strategy": "merge" },
            }),
            vec![
                Partial {
                    rhs: json!([]),
                    expect: Ok(json!([])),
                },
                // Non-array RHS returns an error.
                Partial {
                    rhs: json!("whoops"),
                    expect: Err(Error::MergeWrongType),
                },
                Partial {
                    rhs: json!([0, 1, 0]),
                    expect: Ok(json!([0, 1, 0])),
                },
                Partial {
                    rhs: json!([3, 0, 2]),
                    expect: Ok(json!([3, 1, 2])),
                },
                Partial {
                    rhs: json!([-1, 0, 4, "a"]),
                    expect: Ok(json!([3, 1, 4, "a"])),
                },
                Partial {
                    rhs: json!([0, 32.6, 0, "b"]),
                    expect: Ok(json!([3, 32.6, 4, "b"])),
                },
                Partial {
                    rhs: json!({}),
                    expect: Err(Error::MergeWrongType),
                },
            ],
        )
    }

    #[test]
    fn test_merge_ordered_scalars() {
        run_reduce_cases(
            json!({
                "if": { "type": "null" },
                "then": { "reduce": { "strategy": "lastWriteWins" } },
                "else": {
                    "reduce": {
                        "strategy": "merge",
                        "key": [""],
                    },
                },
            }),
            vec![
                Partial {
                    rhs: json!([5, 9]),
                    expect: Ok(json!([5, 9])),
                },
                Partial {
                    rhs: json!([7]),
                    expect: Ok(json!([5, 7, 9])),
                },
                Partial {
                    rhs: json!([2, 4, 5]),
                    expect: Ok(json!([2, 4, 5, 7, 9])),
                },
                Partial {
                    rhs: json!([1, 2, 7, 10]),
                    expect: Ok(json!([1, 2, 4, 5, 7, 9, 10])),
                },
                // After reducing null LHS, future merges are no-ops.
                Partial {
                    rhs: json!(null),
                    expect: Ok(json!(null)),
                },
                Partial {
                    rhs: json!([1, 2]),
                    expect: Ok(json!(null)),
                },
            ],
        )
    }

    #[test]
    fn test_deep_merge_ordered_objects() {
        run_reduce_cases(
            json!({
                "items": {
                    "properties": {
                        "k": {"type": "integer"},
                    },
                    "additionalProperties": {
                        "reduce": { "strategy": "sum" },
                    },
                    "reduce": { "strategy": "merge" },
                },
                "reduce": {
                    "strategy": "merge",
                    "key": ["/k"],
                },
            }),
            vec![
                Partial {
                    rhs: json!([{"k": 5, "n": 1}, {"k": 9, "n": 1}]),
                    expect: Ok(json!([{"k": 5, "n": 1}, {"k": 9, "n": 1}])),
                },
                Partial {
                    rhs: json!([{"k": 7, "m": 1}]),
                    expect: Ok(json!([{"k": 5, "n": 1}, {"k": 7, "m": 1}, {"k": 9, "n": 1}])),
                },
                Partial {
                    rhs: json!([{"k": 5, "n": 3}, {"k": 7, "m": 1}]),
                    expect: Ok(json!([{"k": 5, "n": 4}, {"k": 7, "m": 2}, {"k": 9, "n": 1}])),
                },
                Partial {
                    rhs: json!([{"k": 9, "n": -2}]),
                    expect: Ok(json!([{"k": 5, "n": 4}, {"k": 7, "m": 2}, {"k": 9, "n": -1}])),
                },
            ],
        )
    }

    #[test]
    fn test_merge_objects() {
        run_reduce_cases(
            json!({
                "if": { "type": "null" },
                "then": { "reduce": { "strategy": "lastWriteWins" } },
                "else": { "reduce": { "strategy": "merge" } },
            }),
            vec![
                Partial {
                    rhs: json!({"5": 5, "9": 9}),
                    expect: Ok(json!({"5": 5, "9": 9})),
                },
                Partial {
                    rhs: json!({"7": 7}),
                    expect: Ok(json!({"5": 5, "7": 7, "9": 9})),
                },
                Partial {
                    rhs: json!({"2": 2, "4": 4, "5": 55}),
                    expect: Ok(json!({"2": 2, "4": 4, "5": 55, "7": 7, "9": 9})),
                },
                Partial {
                    rhs: json!({"1": 1, "2": 22, "7": 77, "10": 10}),
                    expect: Ok(
                        json!({"1": 1, "2": 22, "4": 4, "5": 55, "7": 77, "9": 9, "10": 10}),
                    ),
                },
                Partial {
                    rhs: json!([1, 2]),
                    expect: Err(Error::MergeWrongType),
                },
                // After reducing null LHS, future merges are no-ops.
                Partial {
                    rhs: json!(null),
                    expect: Ok(json!(null)),
                },
                Partial {
                    rhs: json!({"9": 9}),
                    expect: Ok(json!(null)),
                },
            ],
        )
    }

    #[test]
    fn test_deep_merge_objects() {
        run_reduce_cases(
            json!({
                "reduce": {
                    "strategy": "merge",
                    "key": ["/k"],
                },
                "additionalProperties": {
                    "if": { "type": ["object", "array"] },
                    "then": { "$ref": "#" },
                },
                "items": { "$ref": "#/additionalProperties" }
            }),
            vec![
                Partial {
                    rhs: json!([{"k": "b", "v": [{"k": 5}]}]),
                    expect: Ok(json!([{"k": "b", "v": [{"k": 5}]}])),
                },
                Partial {
                    rhs: json!([
                        {"k": "a", "v": [{"k": 2}]},
                        {"k": "b", "v": [{"k": 3}]}
                    ]),
                    expect: Ok(json!([
                        {"k": "a", "v": [{"k": 2}]},
                        {"k": "b", "v": [{"k": 3}, {"k": 5}]}
                    ])),
                },
                Partial {
                    rhs: json!([
                        {"k": "b", "v": [{"k": 1}, {"k": 5, "d": true}]},
                        {"k": "c", "v": [{"k": 9}]}
                    ]),
                    expect: Ok(json!([
                        {"k": "a", "v": [{"k": 2}]},
                        {"k": "b", "v": [{"k": 1}, {"k": 3}, {"k": 5, "d": true}]},
                        {"k": "c", "v": [{"k": 9}]},
                    ])),
                },
            ],
        )
    }
}
