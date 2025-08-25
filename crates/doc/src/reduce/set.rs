use super::{compare_key, compare_key_lazy, reduce_item, reduce_prop, Error, Result, Tape};
use crate::{
    lazy::{LazyArray, LazyDestructured, LazyField, LazyObject},
    AsNode, BumpStr, BumpVec, Field, Fields, HeapField, HeapNode, LazyNode, Pointer,
};
use itertools::EitherOrBoth;
use std::iter::Iterator;

#[derive(serde::Serialize, serde::Deserialize, Debug, Default, PartialEq, Eq, Clone)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Set {
    #[serde(default)]
    pub key: Vec<Pointer>,
}

/// Permitted, destructured forms that set instances may take.
/// Arrays are strictly ordered as "add", "intersect", "remove".
/// This is sorted order: it's the order in which we'll reduce
/// the RHS, and is the order in which we'll consume recursive
/// annotation tokens of each property within the tape.
pub enum Destructured<'alloc, 'l, 'r, L: AsNode, R: AsNode> {
    Array {
        lhs: [Option<LazyArray<'alloc, 'l, L>>; 3],
        rhs: [Option<LazyArray<'alloc, 'r, R>>; 3],
    },
    Object {
        lhs: [Option<LazyObject<'alloc, 'l, L>>; 3],
        rhs: [Option<LazyObject<'alloc, 'r, R>>; 3],
    },
}

impl<'alloc, 'l, 'r, L: AsNode, R: AsNode> Destructured<'alloc, 'l, 'r, L, R> {
    fn extract(
        loc: json::Location,
        lhs: Option<LazyNode<'alloc, 'l, L>>,
        rhs: LazyNode<'alloc, 'r, R>,
    ) -> Result<Self> {
        // Unwrap required Objects on each side.
        let (lhs, rhs) = match (lhs.as_ref().map(LazyNode::destructure), rhs.destructure()) {
            (Some(LazyDestructured::Object(lhs)), LazyDestructured::Object(rhs)) => (lhs, rhs),
            (None, LazyDestructured::Object(rhs)) => (LazyObject::Heap(&[]), rhs),
            _ => return Err(Error::with_details(Error::SetWrongType, loc, lhs, rhs)),
        };

        // Extract "add", "intersect", and "remove" properties & values
        // from both sides, while dis-allowing both "intersect" and "remove"
        // on a single side. Inner object vs array types are disambiguated
        // for each property, and no other properties are permitted.

        fn unpack<'alloc, 'n, N: AsNode>(
            loc: json::Location,
            obj: LazyObject<'alloc, 'n, N>,
            arr_items: &mut [Option<LazyArray<'alloc, 'n, N>>; 3],
            obj_items: &mut [Option<LazyObject<'alloc, 'n, N>>; 3],
        ) -> Result<()> {
            for field in obj.into_iter() {
                let (property, value) = field.into_parts();

                // Collapse owned vs borrowed cases into one &str.
                let property = match &property {
                    Ok(archive) => archive,
                    Err(heap) => heap.as_str(),
                };

                match (property, value.destructure()) {
                    ("add", LazyDestructured::Object(obj)) => {
                        obj_items[0] = Some(obj);
                    }
                    ("add", LazyDestructured::Array(arr)) => {
                        arr_items[0] = Some(arr);
                    }
                    ("intersect", LazyDestructured::Object(obj)) => {
                        obj_items[1] = Some(obj);
                    }
                    ("intersect", LazyDestructured::Array(arr)) => {
                        arr_items[1] = Some(arr);
                    }
                    ("remove", LazyDestructured::Object(obj)) if obj_items[1].is_none() => {
                        obj_items[2] = Some(obj);
                    }
                    ("remove", LazyDestructured::Array(arr)) if arr_items[1].is_none() => {
                        arr_items[2] = Some(arr);
                    }
                    (property, _) => {
                        return Err(Error::with_location(
                            Error::SetWrongType,
                            loc.push_prop(property),
                        ))
                    }
                }
            }
            Ok(())
        }

        let mut lhs_arr = [None, None, None];
        let mut lhs_obj = [None, None, None];
        unpack(loc, lhs, &mut lhs_arr, &mut lhs_obj)?;

        let mut rhs_arr = [None, None, None];
        let mut rhs_obj = [None, None, None];
        unpack(loc, rhs, &mut rhs_arr, &mut rhs_obj)?;

        Ok(
            match (
                lhs_arr.iter().any(Option::is_some) || rhs_arr.iter().any(Option::is_some),
                lhs_obj.iter().any(Option::is_some) || rhs_obj.iter().any(Option::is_some),
            ) {
                // Cannot mix array and object types.
                (true, true) => return Err(Error::with_location(Error::SetWrongType, loc)),

                (_, true) => Destructured::Object {
                    lhs: lhs_obj,
                    rhs: rhs_obj,
                },
                _ => Destructured::Array {
                    lhs: lhs_arr,
                    rhs: rhs_arr,
                },
            },
        )
    }
}

// Masks for defining merge outcomes and desired outcome filters.
const NONE: u8 = 0;
const LEFT: u8 = 1;
const RIGHT: u8 = 2;
const BOTH: u8 = 4;
const UNION: u8 = 7;

// Builder assists in building a set's constituent terms (add, intersect, remove).
struct Builder<'alloc, 'schema, 'tmp> {
    tape: &'tmp mut Tape<'schema>,
    tape_index: &'tmp mut i32,
    loc: json::Location<'tmp>,
    full: bool,
    key: &'schema [Pointer],
    alloc: &'alloc bumpalo::Bump,
}

impl<'alloc> Builder<'alloc, '_, '_> {
    // Build the vector form of a term, as (LHS op1 SUB) op2 RHS.
    // If !naught, then op1 is LHS - SUB (eg, "remove all in SUB").
    // If naught, then op1 is LHS - SUB' (eg, "remove all *not* in SUB").
    //
    // The |mask| determines op2, which may be an intersection, union,
    // or set difference operation.
    fn vec_term<L: AsNode, R: AsNode>(
        &mut self,
        lhs: Option<LazyArray<'alloc, '_, L>>,
        sub: Option<&LazyArray<'alloc, '_, R>>,
        naught: bool,
        mask: u8,
        rhs: Option<LazyArray<'alloc, '_, R>>,
    ) -> Result<Option<(HeapNode<'alloc>, i32)>> {
        let Self {
            tape,
            tape_index,
            loc,
            full,
            key,
            alloc,
        } = self;

        if rhs.is_some() {
            **tape_index += 1; // Consume |rhs| container.
        } else if lhs.is_none() {
            return Ok(None);
        }

        // Guess an output size and allocate its backing array.
        let lhs_size = lhs.as_ref().map(LazyArray::len).unwrap_or_default();
        let rhs_size = rhs.as_ref().map(LazyArray::len).unwrap_or_default();
        let size_hint = match mask {
            NONE => 0,
            LEFT => lhs_size,
            BOTH => lhs_size,
            RIGHT => rhs_size,
            UNION => lhs_size + rhs_size,
            _ => unreachable!("invalid mask"),
        };
        let mut arr = BumpVec::with_capacity_in(size_hint, alloc);

        fn subtract<'i, 'alloc, 'l, 'r, L: AsNode, R: AsNode + 'r>(
            key: &'i [Pointer],
            left: impl Iterator<Item = LazyNode<'alloc, 'l, L>> + 'i,
            right: impl Iterator<Item = &'r R> + 'i,
            naught: bool,
        ) -> Box<dyn Iterator<Item = LazyNode<'alloc, 'l, L>> + 'i> {
            Box::new(
                itertools::merge_join_by(left, right, |l, r| match l {
                    LazyNode::Node(l) => compare_key(key, *l, *r),
                    LazyNode::Heap(l) => compare_key(key, *l, *r),
                })
                .filter_map(move |eob| match eob {
                    EitherOrBoth::Left(l) if !naught => Some(l),
                    EitherOrBoth::Both(l, _) if naught => Some(l),
                    _ => None,
                }),
            )
        }

        let lhs = lhs.into_iter().flat_map(LazyArray::into_iter);
        let lhs_diff_sub: Box<dyn Iterator<Item = LazyNode<_>>> = match sub {
            Some(LazyArray::Node(arr)) => subtract(key, lhs, arr.iter(), naught),
            Some(LazyArray::Heap(arr)) => subtract(key, lhs, arr.iter(), naught),
            None => Box::new(lhs),
        };
        let mut built_length = 1;

        for eob in itertools::merge_join_by(
            lhs_diff_sub.enumerate(),
            rhs.into_iter().flat_map(LazyArray::into_iter).enumerate(),
            |(_, l), (_, r)| compare_key_lazy(key, l, r),
        ) {
            match eob {
                EitherOrBoth::Left((_, lhs)) if LEFT & mask != 0 => {
                    let (node, child_delta) = lhs.into_heap_node(alloc);
                    arr.push(node, alloc);
                    built_length += child_delta;
                }
                EitherOrBoth::Right((_, rhs)) if RIGHT & mask != 0 => {
                    let (node, child_delta) = rhs.into_heap_node(alloc);
                    arr.push(node, alloc);
                    built_length += child_delta;
                    **tape_index += child_delta;
                }
                EitherOrBoth::Both(_, _) if BOTH & mask != 0 => {
                    let (node, child_delta, _) =
                        reduce_item(tape, &mut **tape_index, *loc, *full, eob, alloc)?;
                    arr.push(node, alloc);
                    built_length += child_delta;
                }
                EitherOrBoth::Left(_) => {
                    // Discard.
                }
                EitherOrBoth::Right((_, rhs)) | EitherOrBoth::Both(_, (_, rhs)) => {
                    **tape_index += rhs.tape_length(); // Discard.
                }
            };
        }

        Ok(Some((HeapNode::Array(built_length, arr), built_length)))
    }

    // Build the map form of a term. Behaves just like vec_term.
    fn map_term<L: AsNode, R: AsNode>(
        &mut self,
        lhs: Option<LazyObject<'alloc, '_, L>>,
        sub: Option<&LazyObject<'alloc, '_, R>>,
        naught: bool,
        mask: u8,
        rhs: Option<LazyObject<'alloc, '_, R>>,
    ) -> Result<Option<(HeapNode<'alloc>, i32)>> {
        let Self {
            tape,
            tape_index,
            loc,
            full,
            key: _,
            alloc,
        } = self;

        if rhs.is_some() {
            **tape_index += 1; // Consume |rhs| container.
        } else if lhs.is_none() {
            return Ok(None);
        }

        // Guess an output size and allocate its backing array.
        let lhs_size = lhs.as_ref().map(LazyObject::len).unwrap_or_default();
        let rhs_size = rhs.as_ref().map(LazyObject::len).unwrap_or_default();
        let size_hint = match mask {
            NONE => 0,
            LEFT => lhs_size,
            BOTH => lhs_size,
            RIGHT => rhs_size,
            UNION => lhs_size + rhs_size,
            _ => unreachable!("invalid mask"),
        };
        let mut fields = BumpVec::with_capacity_in(size_hint, alloc);

        fn subtract<'i, 'alloc, 'l, 'r, L: AsNode, R: AsNode, F: Field<'r, R>>(
            left: impl Iterator<Item = LazyField<'alloc, 'l, L>> + 'i,
            right: impl Iterator<Item = F> + 'i,
            naught: bool,
        ) -> Box<dyn Iterator<Item = LazyField<'alloc, 'l, L>> + 'i> {
            Box::new(
                itertools::merge_join_by(left, right, |l, r| l.property().cmp(r.property()))
                    .filter_map(move |eob| match eob {
                        EitherOrBoth::Left(l) if !naught => Some(l),
                        EitherOrBoth::Both(l, _) if naught => Some(l),
                        _ => None,
                    }),
            )
        }

        let lhs = lhs.into_iter().flat_map(LazyObject::into_iter);
        let lhs_diff_sub: Box<dyn Iterator<Item = LazyField<_>>> = match sub {
            Some(LazyObject::Node(fields)) => subtract(lhs, fields.iter(), naught),
            Some(LazyObject::Heap(fields)) => subtract(lhs, fields.iter(), naught),
            None => Box::new(lhs),
        };
        let mut built_length = 1;

        for eob in itertools::merge_join_by(
            lhs_diff_sub,
            rhs.into_iter().flat_map(LazyObject::into_iter),
            |l, r| l.property().cmp(r.property()),
        ) {
            match eob {
                EitherOrBoth::Left(lhs) if LEFT & mask != 0 => {
                    let (field, child_delta) = lhs.into_heap_field(alloc);
                    fields.push(field, alloc);
                    built_length += child_delta;
                }
                EitherOrBoth::Right(rhs) if RIGHT & mask != 0 => {
                    let (field, child_delta) = rhs.into_heap_field(alloc);
                    fields.push(field, alloc);
                    built_length += child_delta;
                    **tape_index += child_delta;
                }
                EitherOrBoth::Both(_, _) if BOTH & mask != 0 => {
                    let (field, child_delta, _) =
                        reduce_prop(tape, &mut **tape_index, *loc, *full, eob, alloc)?;
                    fields.push(field, alloc);
                    built_length += child_delta;
                }
                EitherOrBoth::Left(_) => {
                    // Discard.
                }
                EitherOrBoth::Right(rhs) | EitherOrBoth::Both(_, rhs) => {
                    let (_property, value) = rhs.into_parts();
                    **tape_index += value.tape_length(); // Discard.
                }
            };
        }

        Ok(Some((HeapNode::Object(built_length, fields), built_length)))
    }
}

impl Set {
    pub fn apply<'alloc, 'schema, L: AsNode, R: AsNode>(
        &'schema self,
        tape: &mut Tape<'schema>,
        tape_index: &mut i32,
        loc: json::Location<'_>,
        full: bool,
        lhs: Option<LazyNode<'alloc, '_, L>>,
        rhs: LazyNode<'alloc, '_, R>,
        alloc: &'alloc bumpalo::Bump,
    ) -> Result<(HeapNode<'alloc>, i32)> {
        *tape_index += 1; // Consume object holding the set.

        let mut bld = Builder {
            tape,
            tape_index,
            loc,
            full,
            key: &self.key,
            alloc,
        };
        let mut out = BumpVec::with_capacity_in(2, alloc);

        let add = "add";
        let intersect = "intersect";
        let remove = "remove";
        let mut built_length = 1;

        let mut push_term = |kind, (term, child_delta)| {
            let value = HeapField {
                property: BumpStr::from_str(kind, alloc),
                value: term,
            };
            out.push(value, alloc);
            built_length += child_delta;
        };

        match Destructured::extract(loc, lhs, rhs)? {
            // I,A reduce I,A
            Destructured::Array {
                lhs: [la, Some(li), None],
                rhs: [ra, Some(ri), None],
            } => {
                // Reduce "add" as: (LA - RI') U RA.
                if let Some(term) = bld.vec_term(la, Some(&ri), true, UNION, ra)? {
                    push_term(add, term);
                }

                // Reduce "intersect" as: LI & RI.
                if let (Some(term), false) = (
                    bld.vec_term(
                        Some(li),
                        None,
                        false,
                        if bld.full { NONE } else { BOTH },
                        Some(ri),
                    )?,
                    bld.full,
                ) {
                    push_term(intersect, term);
                }
            }
            // I,A reduce R,A
            Destructured::Array {
                lhs: [la, Some(li), None],
                rhs: [ra, None, rr],
            } => {
                // Reduce "add" as: (LA - RR) U RA.
                if let Some(term) = bld.vec_term(la, rr.as_ref(), false, UNION, ra)? {
                    push_term(add, term);
                }

                // Reduce "intersect" as: LI - RR.
                if let (Some(term), false) = (
                    bld.vec_term(
                        Some(li),
                        None,
                        false,
                        if bld.full { NONE } else { LEFT },
                        rr,
                    )?,
                    bld.full,
                ) {
                    push_term(intersect, term);
                }
            }
            // R,A reduce I,A
            Destructured::Array {
                lhs: [la, None, lr],
                rhs: [ra, Some(ri), None],
            } => {
                // Reduce "add" as: (LA - RI') U RA.
                if let Some(term) = bld.vec_term(la, Some(&ri), true, UNION, ra)? {
                    push_term(add, term);
                }

                // Reduce "intersect" as: RI - LR.
                if let (Some(term), false) = (
                    bld.vec_term(
                        lr,
                        None,
                        false,
                        if bld.full { NONE } else { RIGHT },
                        Some(ri),
                    )?,
                    bld.full,
                ) {
                    push_term(intersect, term);
                }
            }
            // R,A reduce R,A
            Destructured::Array {
                //props: [add, _, remove],
                lhs: [la, None, lr],
                rhs: [ra, None, rr],
            } => {
                // Reduce "add" as: (LA - RR) U RA.
                if let Some(term) = bld.vec_term(la, rr.as_ref(), false, UNION, ra)? {
                    push_term(add, term);
                }

                // Reduce "remove" as: LR U RR.
                if let (Some(term), false) = (
                    bld.vec_term(lr, None, false, if bld.full { NONE } else { UNION }, rr)?,
                    bld.full,
                ) {
                    push_term(remove, term);
                }
            }

            // I,A reduce I,A
            Destructured::Object {
                lhs: [la, Some(li), None],
                rhs: [ra, Some(ri), None],
            } => {
                // Reduce "add" as: (LA - RI') U RA.
                if let Some(term) = bld.map_term(la, Some(&ri), true, UNION, ra)? {
                    push_term(add, term);
                }

                // Reduce "intersect" as: LI & RI.
                if let (Some(term), false) = (
                    bld.map_term(
                        Some(li),
                        None,
                        false,
                        if bld.full { NONE } else { BOTH },
                        Some(ri),
                    )?,
                    bld.full,
                ) {
                    push_term(intersect, term);
                }
            }
            // I,A reduce R,A
            Destructured::Object {
                lhs: [la, Some(li), None],
                rhs: [ra, None, rr],
            } => {
                // Reduce "add" as: (LA - RR) U RA.
                if let Some(term) = bld.map_term(la, rr.as_ref(), false, UNION, ra)? {
                    push_term(add, term);
                }

                // Reduce "intersect" as: LI - RR.
                if let (Some(term), false) = (
                    bld.map_term(
                        Some(li),
                        None,
                        false,
                        if bld.full { NONE } else { LEFT },
                        rr,
                    )?,
                    bld.full,
                ) {
                    push_term(intersect, term);
                }
            }
            // R,A reduce I,A
            Destructured::Object {
                //props: [add, intersect, _],
                lhs: [la, None, lr],
                rhs: [ra, Some(ri), None],
            } => {
                // Reduce "add" as: (LA - RI') U RA.
                if let Some(term) = bld.map_term(la, Some(&ri), true, UNION, ra)? {
                    push_term(add, term);
                }

                // Reduce "intersect" as: RI - LR.
                if let (Some(term), false) = (
                    bld.map_term(
                        lr,
                        None,
                        false,
                        if bld.full { NONE } else { RIGHT },
                        Some(ri),
                    )?,
                    bld.full,
                ) {
                    push_term(intersect, term);
                }
            }
            // R,A reduce R,A
            Destructured::Object {
                //props: [add, _, remove],
                lhs: [la, None, lr],
                rhs: [ra, None, rr],
            } => {
                // Reduce "add" as: (LA - RR) U RA.
                if let Some(term) = bld.map_term(la, rr.as_ref(), false, UNION, ra)? {
                    push_term(add, term);
                }

                // Reduce "remove" as: LR U RR.
                if let (Some(term), false) = (
                    bld.map_term(lr, None, false, if bld.full { NONE } else { UNION }, rr)?,
                    bld.full,
                ) {
                    push_term(remove, term);
                }
            }

            _ => return Err(Error::with_location(Error::SetWrongType, loc)),
        };

        Ok((HeapNode::Object(built_length, out), built_length))
    }
}

#[cfg(test)]
mod test {
    use super::super::test::*;
    use super::Destructured;
    use crate::LazyNode;

    #[test]
    fn test_destructure_cases() {
        use Destructured::{Array, Object};
        use LazyNode::Node;
        let rt = json::Location::Root;

        // Exercise add / intersect / remove on either side, with property collection.
        assert!(matches!(
            Destructured::extract(
                rt,
                Some(Node(&json!({"add": []}))),
                Node(&json!({"remove": []})),
            )
            .unwrap(),
            Array {
                lhs: [Some(_), None, None],
                rhs: [None, None, Some(_)],
            }
        ));

        assert!(matches!(
            Destructured::extract(
                rt,
                Some(Node(&json!({"remove": []}))),
                Node(&json!({"intersect": []}))
            )
            .unwrap(),
            Array {
                lhs: [None, None, Some(_)],
                rhs: [None, Some(_), None],
            }
        ));

        assert!(matches!(
            Destructured::extract(
                rt,
                Some(Node(&json!({"intersect": []}))),
                Node(&json!({"add": []}))
            )
            .unwrap(),
            Array {
                lhs: [None, Some(_), None],
                rhs: [Some(_), None, None],
            }
        ));

        assert!(matches!(
            Destructured::extract(
                rt,
                Some(Node(&json!({"add": {}}))),
                Node(&json!({"remove": {}}))
            )
            .unwrap(),
            Object {
                lhs: [Some(_), None, None],
                rhs: [None, None, Some(_)],
            }
        ));

        assert!(matches!(
            Destructured::extract(
                rt,
                Some(Node(&json!({"remove": {}}))),
                Node(&json!({"intersect": {}}))
            )
            .unwrap(),
            Object {
                //props: [_, intersect, remove],
                lhs: [None, None, Some(_)],
                rhs: [None, Some(_), None],
            }
        ));

        assert!(matches!(
            Destructured::extract(
                rt,
                Some(Node(&json!({"intersect": {}}))),
                Node(&json!({"add": {}}))
            )
            .unwrap(),
            Object {
                //props: [add, intersect, _],
                lhs: [None, Some(_), None],
                rhs: [Some(_), None, None],
            }
        ));

        // Either side may be empty.
        assert!(matches!(
            Destructured::extract(
                rt,
                Some(Node(&json!({}))),
                Node(&json!({"add": {}, "remove": {}}))
            )
            .unwrap(),
            Object {
                lhs: [None, None, None],
                rhs: [Some(_), None, Some(_)],
            }
        ));
        assert!(matches!(
            Destructured::extract(
                rt,
                Some(Node(&json!({"add": [], "remove": []}))),
                Node(&json!({}))
            )
            .unwrap(),
            Array {
                lhs: [Some(_), None, Some(_)],
                rhs: [None, None, None],
            }
        ));

        // The LHS may be undefined.
        assert!(matches!(
            Destructured::extract(
                rt,
                Option::<LazyNode<Value>>::None,
                Node(&json!({"add": {}}))
            )
            .unwrap(),
            Object {
                lhs: [None, None, None],
                rhs: [Some(_), None, None],
            }
        ));
        assert!(matches!(
            Destructured::extract(
                rt,
                Option::<LazyNode<Value>>::None,
                Node(&json!({"remove": []}))
            )
            .unwrap(),
            Array {
                lhs: [None, None, None],
                rhs: [None, None, Some(_)],
            }
        ));

        // Cases that fail:

        // Mixed types within a side.
        assert!(Destructured::extract(
            rt,
            Some(Node(&json!({"add": {}, "intersect": []}))),
            Node(&json!({})),
        )
        .is_err());
        // Mixed types across sides.
        assert!(Destructured::extract(
            rt,
            Some(Node(&json!({"add": {}}))),
            Node(&json!({"intersect": []}))
        )
        .is_err());
        // Both "intersect" and "remove" on a side.
        assert!(Destructured::extract(
            rt,
            Some(Node(&json!({"intersect": [], "remove": []}))),
            Node(&json!({}))
        )
        .is_err());
        // Not an object.
        assert!(
            Destructured::extract(rt, Some(Node(&json!({"intersect": []}))), Node(&json!(42)))
                .is_err()
        );
    }

    #[test]
    fn test_array_sequence_fixture() {
        run_reduce_cases(
            json!({
                "$defs": {
                    "entry": {
                        "type": "array",
                        "items": [
                            { "type": "integer" },
                            {
                                "type": "integer",
                                "reduce": { "strategy": "sum" },
                            },
                        ],
                        "reduce": { "strategy": "merge" },
                    }
                },
                "properties": {
                    "add": { "items": { "$ref": "#/$defs/entry" } }
                },
                "reduce": {
                    "strategy": "set",
                    "key": ["/0"],
                },
            }),
            vec![
                Partial {
                    rhs: json!({"add": [[55, 1]]}),
                    expect: Ok(json!({"add": [[55, 1]]})),
                },
                Partial {
                    rhs: json!({"add": [[99, 1]]}),
                    expect: Ok(json!({"add": [[55, 1], [99, 1]]})),
                },
                Partial {
                    rhs: json!({"remove": [[99]], "add": [[22, 1], [55, 1]]}),
                    expect: Ok(json!({"remove": [[99]], "add": [[22, 1], [55, 2]]})),
                },
                Partial {
                    rhs: json!({"remove": [[55]], "add": [[22, 3], [55, 1]]}),
                    expect: Ok(json!({"remove": [[55], [99]], "add": [[22, 4], [55, 1]]})),
                },
                // Full reductions prune "remove".
                Full {
                    rhs: json!({"remove": [[88]], "add": [[11, 1], [22, 2]]}),
                    expect: Ok(json!({"add": [[11, 1], [22, 6], [55, 1]]})),
                },
                Full {
                    rhs: json!({"remove": [[55]]}),
                    expect: Ok(json!({"add": [[11, 1], [22, 6]]})),
                },
                Partial {
                    rhs: json!({"intersect": [[22], [33]]}),
                    expect: Ok(json!({"intersect": [[22], [33]], "add": [[22, 6]]})),
                },
                Partial {
                    rhs: json!({"add": [[22, 2], [33, 1]]}),
                    expect: Ok(json!({"intersect": [[22], [33]], "add": [[22, 8], [33, 1]]})),
                },
                Partial {
                    rhs: json!({"intersect": [[33], [44]], "add": [[22, 1], [33, 1]]}),
                    expect: Ok(json!({"intersect": [[33]], "add": [[22, 1], [33, 2]]})),
                },
                Partial {
                    rhs: json!({"remove": [[33]], "add": [[22, 1], [33, 1]]}),
                    expect: Ok(json!({"intersect": [], "add": [[22, 2], [33, 1]]})),
                },
                // Full reductions prune "intersect"
                Full {
                    rhs: json!({"add": [[33, 1]]}),
                    expect: Ok(json!({"add": [[22, 2], [33, 2]]})),
                },
                Partial {
                    rhs: json!({"remove": [[33]]}),
                    expect: Ok(json!({"add": [[22, 2]], "remove": [[33]]})),
                },
            ],
        )
    }

    #[test]
    fn test_object_sequence_fixture() {
        run_reduce_cases(
            json!({
                "properties": {
                    "add": {
                        "additionalProperties": {
                            "type": "integer",
                            "reduce": { "strategy": "sum" },
                        }
                    }
                },
                "reduce": {
                    "strategy": "set",
                },
            }),
            vec![
                Partial {
                    rhs: json!({"add": {"55": 1}}),
                    expect: Ok(json!({"add": {"55": 1}})),
                },
                Partial {
                    rhs: json!({"add": {"99": 1}}),
                    expect: Ok(json!({"add": {"55": 1, "99": 1}})),
                },
                Partial {
                    rhs: json!({"remove": {"99": 0}, "add": {"22": 1, "55": 1}}),
                    expect: Ok(json!({"remove": {"99": 0}, "add": {"22": 1, "55": 2}})),
                },
                Partial {
                    rhs: json!({"remove": {"55": 0}, "add": {"22": 3, "55": 1}}),
                    expect: Ok(json!({"remove": {"55": 0, "99": 0}, "add": {"22": 4, "55": 1}})),
                },
                // Full reductions prune "remove".
                Full {
                    rhs: json!({"remove": {"88": 0}, "add": {"11": 1, "22": 2}}),
                    expect: Ok(json!({"add": {"11": 1, "22": 6, "55": 1}})),
                },
                Full {
                    rhs: json!({"remove": {"55": 0}}),
                    expect: Ok(json!({"add": {"11": 1, "22": 6}})),
                },
                Partial {
                    rhs: json!({"intersect": {"22": 0, "33": 0}}),
                    expect: Ok(json!({"intersect": {"22": 0, "33": 0}, "add": {"22": 6}})),
                },
                Partial {
                    rhs: json!({"add": {"22": 2, "33": 1}}),
                    expect: Ok(json!({"intersect": {"22": 0, "33": 0}, "add": {"22": 8, "33": 1}})),
                },
                Partial {
                    rhs: json!({"intersect": {"33": 0, "44": 0}, "add": {"22": 1, "33": 1}}),
                    expect: Ok(json!({"intersect": {"33": 0}, "add": {"22": 1, "33": 2}})),
                },
                Partial {
                    rhs: json!({"remove": {"33": 0}, "add": {"22": 1, "33": 1}}),
                    expect: Ok(json!({"intersect": {}, "add": {"22": 2, "33": 1}})),
                },
                // Full reductions prune "intersect"
                Full {
                    rhs: json!({"add": {"33": 1}}),
                    expect: Ok(json!({"add": {"22": 2, "33": 2}})),
                },
                Partial {
                    rhs: json!({"remove": {"33":0}}),
                    expect: Ok(json!({"add": {"22":2}, "remove": {"33":0}})),
                },
            ],
        )
    }
}
