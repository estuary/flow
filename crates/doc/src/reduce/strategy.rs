use super::{
    compare_key_lazy, compare_lazy, count_nodes, count_nodes_lazy, reduce_item, reduce_prop,
    schema::json_schema_merge, Cursor, Error, ParsedNumber, Result,
};
use crate::{
    lazy::{LazyDestructured, LazyNode},
    AsNode, BumpVec, HeapNode, Node, Pointer,
};
use itertools::EitherOrBoth;

#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq, Clone)]
#[serde(tag = "strategy", deny_unknown_fields, rename_all = "camelCase")]
pub enum Strategy {
    /// Append each item of RHS to the end of LHS. RHS must be an array.
    /// LHS must be an array, or may be null, in which case no append is
    /// done and the reduction is a no-op.
    Append,
    /// FirstWriteWins keeps the LHS value.
    FirstWriteWins(FirstWriteWins),
    /// LastWriteWins takes the RHS value.
    LastWriteWins(LastWriteWins),
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
    /// Deep-merge the JSON schemas in LHS and RHS
    /// both of which must be objects containing valid json schemas.
    JsonSchemaMerge,
}

impl std::convert::TryFrom<&serde_json::Value> for Strategy {
    type Error = serde_json::Error;

    fn try_from(v: &serde_json::Value) -> std::result::Result<Self, Self::Error> {
        <Strategy as serde::Deserialize>::deserialize(v)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Default, PartialEq, Eq, Clone)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct FirstWriteWins {}

#[derive(serde::Serialize, serde::Deserialize, Debug, Default, PartialEq, Eq, Clone)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct LastWriteWins {
    /// Delete marks that this location should be removed from the document.
    /// Deletion is effected only when a document is fully reduced, so partial
    /// reductions of the document will continue to have deleted locations
    /// up until the point where they're reduced into a base document.
    #[serde(default)]
    pub delete: bool,
    /// Associative marks that unequal values are allowed to reduce associatively.
    /// The default is true. When set to false, then unequal left- and right-hand
    /// values may not reduce associatively and both documents must be retained
    /// until a full reduction can be performed.
    /// EXPERIMENTAL: This keyword may be removed in the future.
    #[serde(default = "true_value")]
    pub associative: bool,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Default, PartialEq, Eq, Clone)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Maximize {
    /// Optional, relative JSON Pointer(s) which form the key over which values
    /// are maximized. When omitted, the entire value at this location is used.
    #[serde(default)]
    pub key: Vec<Pointer>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Default, PartialEq, Eq, Clone)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Merge {
    /// Relative JSON Pointer(s) which form the key by which the items of merged
    /// Arrays are ordered. `key` is ignored in Object merge contexts, where the
    /// object property is used instead.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub key: Vec<Pointer>,
    /// Delete marks that this location should be removed from the document.
    /// Deletion is effected only when a document is fully reduced, so partial
    /// reductions of the document will continue to have deleted locations
    /// up until the point where they're reduced into a base document.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub delete: bool,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Default, PartialEq, Eq, Clone)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Minimize {
    /// Optional, relative JSON Pointer(s) which form the key over which values
    /// are minimized. When omitted, the entire value at this location is used.
    #[serde(default)]
    pub key: Vec<Pointer>,
}

impl Strategy {
    pub fn apply<'alloc, 'schema, L: AsNode, R: AsNode>(
        &'schema self,
        cur: Cursor<'alloc, 'schema, '_, '_, '_, L, R>,
    ) -> Result<(HeapNode<'alloc>, bool)> {
        match self {
            Strategy::Append => Ok((Self::append(cur)?, false)),
            Strategy::FirstWriteWins(fww) => Ok((Self::first_write_wins(cur, fww), false)),
            Strategy::JsonSchemaMerge => Ok((json_schema_merge(cur)?, false)),
            Strategy::LastWriteWins(lww) => Self::last_write_wins(cur, lww),
            Strategy::Maximize(max) => Ok((Self::maximize(cur, max)?, false)),
            Strategy::Merge(merge) => Self::merge(cur, merge),
            Strategy::Minimize(min) => Ok((Self::minimize(cur, min)?, false)),
            Strategy::Set(set) => Ok((set.apply(cur)?, false)),
            Strategy::Sum => Ok((Self::sum(cur)?, false)),
        }
    }

    fn append<'alloc, L: AsNode, R: AsNode>(
        cur: Cursor<'alloc, '_, '_, '_, '_, L, R>,
    ) -> Result<HeapNode<'alloc>> {
        let Cursor {
            tape,
            loc,
            full: _,
            lhs,
            rhs,
            alloc,
        } = cur;

        use LazyDestructured as LD;

        match (lhs.as_ref().map(LazyNode::destructure), rhs.destructure()) {
            (Some(LD::Array(lhs)), LD::Array(rhs)) => {
                *tape = &tape[1..]; // Increment for self.

                let mut arr = BumpVec::with_capacity_in(lhs.len() + rhs.len(), alloc);

                for lhs in lhs.into_iter() {
                    arr.push(lhs.into_heap_node(alloc), alloc);
                }
                for rhs in rhs.into_iter() {
                    let rhs = rhs.into_heap_node(alloc);
                    *tape = &tape[count_nodes(&rhs)..];
                    arr.push(rhs, alloc)
                }
                Ok(HeapNode::Array(arr))
            }
            (None, LD::Array(_)) => {
                let rhs = rhs.into_heap_node(alloc);
                *tape = &tape[count_nodes(&rhs)..];
                Ok(rhs)
            }
            (Some(LD::ScalarNode(Node::Null) | LD::ScalarHeap(HeapNode::Null)), LD::Array(_)) => {
                *tape = &tape[count_nodes_lazy(&rhs)..];
                Ok(HeapNode::Null) // Ignores `rhs` and remains `null`.
            }
            _ => Err(Error::with_details(Error::AppendWrongType, loc, lhs, rhs)),
        }
    }

    fn first_write_wins<'alloc, L: AsNode, R: AsNode>(
        cur: Cursor<'alloc, '_, '_, '_, '_, L, R>,
        _fww: &FirstWriteWins,
    ) -> HeapNode<'alloc> {
        let Some(lhs) = cur.lhs else {
            let rhs = cur.rhs.into_heap_node(cur.alloc);
            *cur.tape = &cur.tape[count_nodes(&rhs)..];
            return rhs;
        };

        *cur.tape = &cur.tape[count_nodes_lazy(&cur.rhs)..];
        lhs.into_heap_node(cur.alloc)
    }

    fn last_write_wins<'alloc, L: AsNode, R: AsNode>(
        cur: Cursor<'alloc, '_, '_, '_, '_, L, R>,
        lww: &LastWriteWins,
    ) -> Result<(HeapNode<'alloc>, bool)> {
        if !lww.associative
            && !cur.full
            && matches!(&cur.lhs, Some(lhs) if compare_lazy(lhs, &cur.rhs).is_ne())
        {
            // When marked !associative, partial reductions may only reduce equal values.
            return Err(Error::NotAssociative);
        }
        let rhs = cur.rhs.into_heap_node(cur.alloc);
        *cur.tape = &cur.tape[count_nodes(&rhs)..];
        Ok((rhs, cur.full && lww.delete))
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
        } = cur;

        let Some(lhs) = lhs else {
            let rhs = rhs.into_heap_node(alloc);
            *tape = &tape[count_nodes(&rhs)..];
            return Ok(rhs);
        };

        let ord = match (key.is_empty(), reverse) {
            (false, false) => compare_key_lazy(key, &lhs, &rhs),
            (false, true) => compare_key_lazy(key, &rhs, &lhs),
            (true, false) => compare_lazy(&lhs, &rhs),
            (true, true) => compare_lazy(&rhs, &lhs),
        };

        if ord.is_lt() {
            // Retain the LHS.
            *tape = &tape[count_nodes_lazy(&rhs)..];
            Ok(lhs.into_heap_node(alloc))
        } else if key.is_empty() {
            // When there's no key then each value is a complete and opaque blob,
            // and we simply take the RHS.
            let rhs = rhs.into_heap_node(alloc);
            *tape = &tape[count_nodes(&rhs)..];
            Ok(rhs)
        } else {
            let cur = Cursor {
                tape,
                loc,
                full,
                lhs: if ord.is_eq() { Some(lhs) } else { None },
                rhs,
                alloc,
            };
            Self::merge_with_key(cur, &[])
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
            alloc,
        } = cur;

        use LazyDestructured as LD;
        use ParsedNumber as PN;

        let ln = match lhs.as_ref().map(LazyNode::destructure) {
            None => Some(PN::PosInt(0u64)),
            Some(LD::ScalarNode(Node::PosInt(n))) => Some(PN::PosInt(n)),
            Some(LD::ScalarNode(Node::NegInt(n))) => Some(PN::NegInt(n)),
            Some(LD::ScalarNode(Node::Float(n))) => Some(PN::Float(n)),
            Some(LD::ScalarNode(Node::String(n))) => n.parse().ok().map(PN::Arbitrary),
            Some(LD::ScalarHeap(HeapNode::PosInt(n))) => Some(PN::PosInt(*n)),
            Some(LD::ScalarHeap(HeapNode::NegInt(n))) => Some(PN::NegInt(*n)),
            Some(LD::ScalarHeap(HeapNode::Float(n))) => Some(PN::Float(*n)),
            Some(LD::ScalarHeap(HeapNode::String(n))) => n.parse().ok().map(PN::Arbitrary),
            _ => None,
        };
        let rn = match rhs.destructure() {
            LD::ScalarNode(Node::PosInt(n)) => Some(PN::PosInt(n)),
            LD::ScalarNode(Node::NegInt(n)) => Some(PN::NegInt(n)),
            LD::ScalarNode(Node::Float(n)) => Some(PN::Float(n)),
            LD::ScalarNode(Node::String(n)) => n.parse().ok().map(PN::Arbitrary),
            LD::ScalarHeap(HeapNode::PosInt(n)) => Some(PN::PosInt(*n)),
            LD::ScalarHeap(HeapNode::NegInt(n)) => Some(PN::NegInt(*n)),
            LD::ScalarHeap(HeapNode::Float(n)) => Some(PN::Float(*n)),
            LD::ScalarHeap(HeapNode::String(n)) => n.parse().ok().map(PN::Arbitrary),
            _ => None,
        };
        let (Some(ln), Some(rn)) = (ln, rn) else {
            return Err(Error::with_details(Error::SumWrongType, loc, lhs, rhs));
        };

        *tape = &tape[1..];

        if let Some(n) = PN::checked_add(ln, rn) {
            Ok(n.into_heap_node(alloc))
        } else {
            Err(Error::with_details(
                Error::SumNumericOverflow,
                loc,
                lhs,
                rhs,
            ))
        }
    }

    fn merge<'alloc, L: AsNode, R: AsNode>(
        cur: Cursor<'alloc, '_, '_, '_, '_, L, R>,
        merge: &Merge,
    ) -> Result<(HeapNode<'alloc>, bool)> {
        let delete = cur.full && merge.delete;
        Ok((Self::merge_with_key(cur, &merge.key)?, delete))
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
            full,
        } = cur;

        use LazyDestructured as LD;

        match (lhs.as_ref().map(LazyNode::destructure), rhs.destructure()) {
            // Object <= Object: deep associative merge.
            (Some(LD::Object(lhs)), LD::Object(rhs)) => {
                *tape = &tape[1..]; // Increment for self.

                let mut fields =
                    BumpVec::with_capacity_in(std::cmp::max(lhs.len(), rhs.len()), alloc);

                for eob in itertools::merge_join_by(lhs.into_iter(), rhs.into_iter(), |lhs, rhs| {
                    lhs.property().cmp(rhs.property())
                }) {
                    let (field, delete) = reduce_prop::<L, R>(tape, loc, full, eob, alloc)?;
                    if !delete {
                        fields.push(field, alloc);
                    }
                }
                Ok(HeapNode::Object(fields))
            }
            // !Object <= Object (full reduction)
            (_, LD::Object(rhs)) if full => {
                *tape = &tape[1..]; // Increment for self.

                let mut fields = BumpVec::with_capacity_in(rhs.len(), alloc);

                for rhs in rhs.into_iter() {
                    let (field, delete) =
                        reduce_prop::<L, R>(tape, loc, full, EitherOrBoth::Right(rhs), alloc)?;
                    if !delete {
                        fields.push(field, alloc);
                    }
                }
                Ok(HeapNode::Object(fields))
            }

            // Array <= Array: deep associative merge.
            (Some(LD::Array(lhs)), LD::Array(rhs)) => {
                *tape = &tape[1..]; // Increment for self.

                let mut items =
                    BumpVec::with_capacity_in(std::cmp::max(lhs.len(), rhs.len()), alloc);

                for eob in itertools::merge_join_by(
                    lhs.into_iter().enumerate(),
                    rhs.into_iter().enumerate(),
                    |(lhs_ind, lhs), (rhs_ind, rhs)| {
                        if key.is_empty() {
                            lhs_ind.cmp(rhs_ind)
                        } else {
                            compare_key_lazy(key, lhs, rhs)
                        }
                    },
                ) {
                    let (item, delete) = reduce_item::<L, R>(tape, loc, full, eob, alloc)?;
                    if !delete {
                        items.push(item, alloc);
                    }
                }
                Ok(HeapNode::Array(items))
            }
            // !Array <= Array (full reduction)
            (_, LD::Array(rhs)) if full => {
                *tape = &tape[1..]; // Increment for self.

                let mut items = BumpVec::with_capacity_in(rhs.len(), alloc);

                for rhs in rhs.into_iter().enumerate() {
                    let (item, delete) =
                        reduce_item::<L, R>(tape, loc, full, EitherOrBoth::Right(rhs), alloc)?;
                    if !delete {
                        items.push(item, alloc);
                    }
                }
                Ok(HeapNode::Array(items))
            }

            // !Object <= Object | !Array <= Array (associative reduction)
            (_, LD::Object(_) | LD::Array(_)) => {
                if lhs.is_none() {
                    let rhs = rhs.into_heap_node(alloc);
                    *tape = &tape[count_nodes(&rhs)..];
                    Ok(rhs)
                } else {
                    // Not associative because:
                    // {a: 1} . ("foo" . {b: 2}) == {a: 1, b: 2}
                    // ({a: 1} . "foo") . {b: 2} == {b: 2}
                    Err(Error::NotAssociative)
                }
            }

            _ => Err(Error::with_details(Error::MergeWrongType, loc, lhs, rhs)),
        }
    }
}

fn true_value() -> bool {
    true
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
            json!({ "oneOf": [
                {"type": ["string", "object", "null"], "reduce": { "strategy": "lastWriteWins" } },
                {"type": "integer", "reduce": { "strategy": "lastWriteWins", "associative": false } },
            ]}),
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
                Partial {
                    rhs: json!(42),
                    expect: Err(Error::NotAssociative),
                },
                // Full reduction may take the value.
                Full {
                    rhs: json!(42),
                    expect: Ok(json!(42)),
                },
                // Associative reduction is allowed iff the value doesn't change.
                Partial {
                    rhs: json!(42),
                    expect: Ok(json!(42)),
                },
                Partial {
                    rhs: json!(52),
                    expect: Err(Error::NotAssociative),
                },
                // RHS is allowed to reduce associatively.
                Partial {
                    rhs: json!("foo"),
                    expect: Ok(json!("foo")),
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
                // 'null' orders before an integer.
                Partial {
                    rhs: json!({"k": null, "n": -1}),
                    expect: Ok(json!({"k": null, "n": -1})),
                },
                // Missing key orders before 'null'.
                Partial {
                    rhs: json!({"n": 1, "whoops": true}),
                    expect: Ok(json!({"n": 1, "whoops": true})),
                },
                // Keys are technically equal (both are undefined), and it attempts to deep-merge.
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
                    expect: Err(Error::NotAssociative),
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

        run_reduce_cases(
            json!({ "reduce": { "strategy": "sum" } }),
            vec![
                Partial {
                    rhs: json!(0),
                    expect: Ok(json!(0)),
                },
                // String-encoded numerics coerce to arbitrary precision.
                Partial {
                    rhs: json!("1"),
                    expect: Ok(json!("1")),
                },
                Partial {
                    rhs: json!("9000000000000000000"),
                    expect: Ok(json!("9000000000000000001")),
                },
                Partial {
                    rhs: json!("10000000000000000000"),
                    expect: Ok(json!("19000000000000000001")),
                },
                Partial {
                    rhs: json!(1_233),
                    expect: Ok(json!("19000000000000001234")),
                },
                Partial {
                    rhs: json!(-10_000),
                    expect: Ok(json!("18999999999999991234")),
                },
                Partial {
                    rhs: json!(86753.09),
                    expect: Ok(json!("19000000000000077987.09000000000")),
                },
                Partial {
                    rhs: json!("10203.040506070812"),
                    expect: Ok(json!("19000000000000088190.130506070812")),
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
                // Cannot switch merge type during a non-associative reduction.
                Partial {
                    rhs: json!({}),
                    expect: Err(Error::NotAssociative),
                },
                // But it can switch types during a full reduction.
                Full {
                    rhs: json!({"a": "b"}),
                    expect: Ok(json!({"a": "b"})),
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
                // If LHS is a different type, merges are not associative.
                Partial {
                    rhs: json!(null),
                    expect: Ok(json!(null)),
                },
                Partial {
                    rhs: json!([1, 2]),
                    expect: Err(Error::NotAssociative),
                },
                Full {
                    rhs: json!([1, 2]),
                    expect: Ok(json!([1, 2])),
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
                    rhs: json!("whoops"),
                    expect: Err(Error::MergeWrongType),
                },
                // If LHS is a different type, merges are not associative.
                Partial {
                    rhs: json!(null),
                    expect: Ok(json!(null)),
                },
                Partial {
                    rhs: json!({"9": 9}),
                    expect: Err(Error::NotAssociative),
                },
                Full {
                    rhs: json!({"9": 9}),
                    expect: Ok(json!({"9": 9})),
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

    #[test]
    fn test_merge_array_deletion() {
        run_reduce_cases(
            json!({
                "items": {
                    "properties": {
                        "k": {"type": "integer"},
                    },
                    "if": {
                        "required": ["del"]
                    },
                    "then": {
                        "reduce": {"strategy": "lastWriteWins", "delete": true}
                    }
                },
                "reduce": {
                    "strategy": "merge",
                    "key": ["/k"],
                },
            }),
            vec![
                Partial {
                    rhs: json!([{"k": 5}, {"k": 9}]),
                    expect: Ok(json!([{"k": 5}, {"k": 9}])),
                },
                // When applied associatively, deletions have no effect.
                Partial {
                    rhs: json!([{"k": 5, "del": 1}, {"k": 6}]),
                    expect: Ok(json!([{"k": 5, "del": 1}, {"k": 6}, {"k": 9}])),
                },
                Partial {
                    rhs: json!([{"k": 5, "del": 1}]),
                    expect: Ok(json!([{"k": 5, "del": 1}, {"k": 6}, {"k": 9}])),
                },
                // However full reductions will remove the nested location.
                Full {
                    rhs: json!([{"k": 5, "del": 1}, {"k": 7}]),
                    expect: Ok(json!([{"k": 6}, {"k": 7}, {"k": 9}])),
                },
                Full {
                    rhs: json!([{"k": 6, "del": 1}, {"k": 8}, {"k": 9, "del": 1}]),
                    expect: Ok(json!([{"k": 7}, {"k": 8}])),
                },
            ],
        )
    }

    #[test]
    fn test_merge_object_deletion() {
        run_reduce_cases(
            json!({
                "additionalProperties": {
                    "if": {
                        "const": "del"
                    },
                    "then": {
                        "reduce": {"strategy": "lastWriteWins", "delete": true}
                    }
                },
                "reduce": {
                    "strategy": "merge"
                },
            }),
            vec![
                Partial {
                    rhs: json!({"5": 5, "9": 9}),
                    expect: Ok(json!({"5": 5, "9": 9})),
                },
                // When applied associatively, deletions have no effect.
                Partial {
                    rhs: json!({"5": "del", "6": 6}),
                    expect: Ok(json!({"5": "del", "6": 6, "9": 9})),
                },
                Partial {
                    rhs: json!({"5": "del"}),
                    expect: Ok(json!({"5": "del", "6": 6, "9": 9})),
                },
                // However full reductions will remove the nested location.
                Full {
                    rhs: json!({"5": "del", "7": 7}),
                    expect: Ok(json!({"6": 6, "7": 7, "9": 9})),
                },
                Full {
                    rhs: json!({"6": "del", "8": 8, "9": "del"}),
                    expect: Ok(json!({"7": 7, "8": 8})),
                },
            ],
        )
    }

    #[test]
    fn test_merge_patch_examples() {
        let f1 = json!({
            "a": 32.6,
            "c": {
              "d": [42],
              "f": "g"
            }
        });

        run_reduce_cases(
            super::super::merge_patch_schema(),
            vec![
                Partial {
                    rhs: f1.clone(),
                    expect: Ok(f1.clone()),
                },
                // Okay to change scalars associatively.
                Partial {
                    rhs: json!({
                      "c": { "d": "e" }
                    }),
                    expect: Ok(json!({
                      "a": 32.6,
                      "c": {
                        "d": "e",
                        "f": "g"
                      }
                    })),
                },
                Full {
                    rhs: json!({
                      "a": "z",
                      "c": { "f": null }
                    }),
                    expect: Ok(json!({
                      "a": "z",
                      "c": { "d": "e" }
                    })),
                },
                // Cannot switch from a scalar to a merged type associatively.
                Partial {
                    rhs: json!({ "a": { "1": 1 } }),
                    expect: Err(Error::NotAssociative),
                },
                // But can do it in a full reduction.
                Full {
                    rhs: json!({ "a": { "1": 1 } }),
                    expect: Ok(json!({
                      "a": { "1": 1 },
                      "c": { "d": "e" }
                    })),
                },
                Full {
                    rhs: json!([1, 2]),
                    expect: Ok(json!([1, 2])),
                },
                Full {
                    rhs: json!({"a": {"bb": {"ccc": null}}}),
                    expect: Ok(json!({"a": {"bb": {}}})),
                },
                Full {
                    rhs: json!(null),
                    expect: Ok(json!(null)),
                },
                Full {
                    rhs: json!("fin"),
                    expect: Ok(json!("fin")),
                },
            ],
        )
    }
}
