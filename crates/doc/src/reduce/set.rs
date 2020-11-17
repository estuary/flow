use super::{reduce_item, reduce_prop, Cursor, Error, Index, Location, Reducer, Result};
use itertools::EitherOrBoth;
use json::json_cmp_at;
use serde_json::{Map, Value};

use super::strategy::Set;

/// Permitted, destructured forms that set instances may take.
/// Arrays are strictly ordered as "add", "intersect", "remove".
/// This is sorted order: it's the order in which we'll reduce
/// the RHS, and is the order in which we'll consume recursive
/// annotation tokens of each property within the tape.
#[derive(Debug)]
pub enum Destructured {
    Array {
        props: [String; 3],
        lhs: [Option<Vec<Value>>; 3],
        rhs: [Option<Vec<Value>>; 3],
    },
    Object {
        props: [String; 3],
        lhs: [Option<Map<String, Value>>; 3],
        rhs: [Option<Map<String, Value>>; 3],
    },
}

impl Destructured {
    fn extract(loc: Location, lhs: Value, rhs: Value) -> Result<Self> {
        // Unwrap required Objects on each side.
        let (lhs, rhs) = match (lhs, rhs) {
            (Value::Object(lhs), Value::Object(rhs)) => (lhs, rhs),
            (_, _) => return Err(Error::at(loc, Error::SetWrongType)),
        };

        // Extract "add", "intersect", and "remove" properties & values
        // from both sides, while dis-allowing both "intersect" and "remove"
        // on a single side. Inner object vs array types are disambiguated
        // for each property, and no other properties are permitted.
        // |props| are property Strings obtained from either side, for re-use.

        let mut props = [String::new(), String::new(), String::new()];
        let mut rhs_arr = [None, None, None];
        let mut rhs_obj = [None, None, None];
        let mut lhs_arr = [None, None, None];
        let mut lhs_obj = [None, None, None];

        for (m, arr_items, obj_items) in &mut [
            (lhs, &mut lhs_arr, &mut lhs_obj),
            (rhs, &mut rhs_arr, &mut rhs_obj),
        ] {
            for (prop, item) in std::mem::take(m).into_iter() {
                match (prop.as_ref(), item) {
                    ("add", Value::Object(obj)) => {
                        props[0] = prop;
                        obj_items[0] = Some(obj);
                    }
                    ("add", Value::Array(arr)) => {
                        props[0] = prop;
                        arr_items[0] = Some(arr);
                    }
                    ("intersect", Value::Object(obj)) => {
                        props[1] = prop;
                        obj_items[1] = Some(obj);
                    }
                    ("intersect", Value::Array(arr)) => {
                        props[1] = prop;
                        arr_items[1] = Some(arr);
                    }
                    ("remove", Value::Object(obj)) if obj_items[1].is_none() => {
                        props[2] = prop;
                        obj_items[2] = Some(obj);
                    }
                    ("remove", Value::Array(arr)) if arr_items[1].is_none() => {
                        props[2] = prop;
                        arr_items[2] = Some(arr);
                    }
                    _ => return Err(Error::at(loc.push_prop(&prop), Error::SetWrongType)),
                }
            }
        }

        Ok(
            match (
                lhs_arr.iter().any(Option::is_some) || rhs_arr.iter().any(Option::is_some),
                lhs_obj.iter().any(Option::is_some) || rhs_obj.iter().any(Option::is_some),
            ) {
                // Cannot mix array and object types.
                (true, true) => return Err(Error::at(loc, Error::SetWrongType)),

                (_, true) => Destructured::Object {
                    props,
                    lhs: lhs_obj,
                    rhs: rhs_obj,
                },
                _ => Destructured::Array {
                    props,
                    lhs: lhs_arr,
                    rhs: rhs_arr,
                },
            },
        )
    }
}

// Masks for defining merge outcomes and desired outcome filters.
const LEFT: u8 = 1;
const RIGHT: u8 = 2;
const BOTH: u8 = 4;
const UNION: u8 = 7;

// Builder assists in building a set's constituent terms (add, intersect, remove).
struct Builder<'i, 'l, 'a, 'k> {
    tape: &'i mut Index<'a>,
    loc: Location<'l>,
    prune: bool,
    key: &'k [String],
}

impl Builder<'_, '_, '_, '_> {
    // Build the vector form of a term, as (LHS op1 SUB) op2 RHS.
    // If !naught, then op1 is LHS - SUB (eg, "remove all in SUB").
    // If naught, then op1 is LHS - SUB' (eg, "remove all *not* in SUB").
    //
    // The |mask| determines op2, which may be an intersection, union,
    // or set difference operation.
    fn vec_term(
        &mut self,
        lhs: Option<Vec<Value>>,
        sub: Option<&Vec<Value>>,
        naught: bool,
        mask: u8,
        rhs: Option<Vec<Value>>,
    ) -> Result<Option<Value>> {
        // Flatten Option<Vec> into Vec.
        let (lhs, rhs, tape_inc) = match (lhs, rhs) {
            (Some(lhs), Some(rhs)) => (lhs, rhs, 1),
            (Some(lhs), None) => (lhs, Vec::new(), 0),
            (None, Some(rhs)) => (Vec::new(), rhs, 1),
            (None, None) => return Ok(None),
        };
        *self.tape = &self.tape[tape_inc..]; // Consume |rhs| container.

        let empty = Vec::new();
        let sub = sub.unwrap_or(&empty);

        // Copy to allow multiple closures to reference |key| but not |self|.
        let key = self.key;

        let lhs_diff_sub =
            itertools::merge_join_by(lhs.into_iter(), sub, |l, r| json_cmp_at(key, l, r))
                .filter_map(|eob| match eob {
                    EitherOrBoth::Left(l) if !naught => Some(l),
                    EitherOrBoth::Both(l, _) if naught => Some(l),
                    _ => None,
                });

        let v = itertools::merge_join_by(
            lhs_diff_sub.enumerate(),
            rhs.into_iter().enumerate(),
            |(_, l), (_, r)| json_cmp_at(key, l, r),
        )
        .map(|eob| {
            let outcome = match &eob {
                EitherOrBoth::Left(_) => LEFT,
                EitherOrBoth::Right(_) => RIGHT,
                EitherOrBoth::Both(_, _) => BOTH,
            };
            Ok((outcome, reduce_item(self.tape, self.loc, self.prune, eob)?))
        })
        .filter_map(|r| match r {
            Ok((outcome, value)) if outcome & mask != 0 => Some(Ok(value)),
            Err(err) => Some(Err(err)),
            Ok(_) => None,
        })
        .collect::<Result<Vec<Value>>>()?;

        Ok(Some(Value::Array(v)))
    }

    // Build the map form of a term. Behaves just like vec_term.
    fn map_term(
        &mut self,
        lhs: Option<Map<String, Value>>,
        sub: Option<&Map<String, Value>>,
        naught: bool,
        mask: u8,
        rhs: Option<Map<String, Value>>,
    ) -> Result<Option<Value>> {
        // Flatten Option<Map> into Map.
        let (lhs, rhs, tape_inc) = match (lhs, rhs) {
            (Some(lhs), Some(rhs)) => (lhs, rhs, 1),
            (Some(lhs), None) => (lhs, Map::new(), 0),
            (None, Some(rhs)) => (Map::new(), rhs, 1),
            (None, None) => return Ok(None),
        };
        *self.tape = &self.tape[tape_inc..]; // Consume |rhs| container.

        let empty = Map::new();
        let sub = sub.unwrap_or(&empty);

        let lhs_diff_sub =
            itertools::merge_join_by(lhs.into_iter(), sub, |(l, _), (r, _)| l.cmp(r)).filter_map(
                |eob| match eob {
                    EitherOrBoth::Left(l) if !naught => Some(l),
                    EitherOrBoth::Both(l, _) if naught => Some(l),
                    _ => None,
                },
            );

        let m = itertools::merge_join_by(lhs_diff_sub, rhs.into_iter(), |(l, _), (r, _)| l.cmp(r))
            .map(|eob| {
                let outcome = match &eob {
                    EitherOrBoth::Left(_) => LEFT,
                    EitherOrBoth::Right(_) => RIGHT,
                    EitherOrBoth::Both(_, _) => BOTH,
                };
                Ok((outcome, reduce_prop(self.tape, self.loc, self.prune, eob)?))
            })
            .filter_map(|r| match r {
                Ok((outcome, value)) if outcome & mask != 0 => Some(Ok(value)),
                Err(err) => Some(Err(err)),
                Ok(_) => None,
            })
            .collect::<Result<Map<_, _>>>()?;

        Ok(Some(Value::Object(m)))
    }
}

impl Reducer for Set {
    fn reduce(&self, cur: Cursor) -> Result<Value> {
        let (tape, loc, prune, lhs, rhs) = match cur {
            Cursor::Both {
                tape,
                loc,
                prune,
                lhs,
                rhs,
            } => (tape, loc, prune, lhs, rhs),
            Cursor::Right {
                tape,
                loc,
                prune,
                rhs,
            } => (tape, loc, prune, Value::Object(Map::new()), rhs),
        };
        *tape = &tape[1..]; // Consume object holding the set.

        let mut bld = Builder {
            tape,
            loc,
            prune,
            key: self.key.as_slice(),
        };
        let mut out = Map::with_capacity(2);

        match Destructured::extract(loc, lhs, rhs)? {
            // I,A reduce I,A
            Destructured::Array {
                props: [add, intersect, _],
                lhs: [la, Some(li), None],
                rhs: [ra, Some(ri), None],
            } => {
                // Reduce "add" as: (LA - RI') U RA.
                if let Some(v) = bld.vec_term(la, Some(&ri), true, UNION, ra)? {
                    out.insert(add, v);
                }
                // Reduce "intersect" as: LI & RI.
                if let (Some(v), false) = (
                    bld.vec_term(Some(li), None, false, BOTH, Some(ri))?,
                    bld.prune,
                ) {
                    out.insert(intersect, v);
                }
            }
            // I,A reduce R,A
            Destructured::Array {
                props: [add, intersect, _],
                lhs: [la, Some(li), None],
                rhs: [ra, None, rr],
            } => {
                // Reduce "add" as: (LA - RR) U RA.
                if let Some(v) = bld.vec_term(la, rr.as_ref(), false, UNION, ra)? {
                    out.insert(add, v);
                }
                // Reduce "intersect" as: LI - RR.
                if let (Some(v), false) =
                    (bld.vec_term(Some(li), None, false, LEFT, rr)?, bld.prune)
                {
                    out.insert(intersect, v);
                }
            }
            // R,A reduce I,A
            Destructured::Array {
                props: [add, intersect, _],
                lhs: [la, None, lr],
                rhs: [ra, Some(ri), None],
            } => {
                // Reduce "add" as: (LA - RI') U RA.
                if let Some(v) = bld.vec_term(la, Some(&ri), true, UNION, ra)? {
                    out.insert(add, v);
                }
                // Reduce "intersect" as: RI - LR.
                if let (Some(v), false) =
                    (bld.vec_term(lr, None, false, RIGHT, Some(ri))?, bld.prune)
                {
                    out.insert(intersect, v);
                }
            }
            // R,A reduce R,A
            Destructured::Array {
                props: [add, _, remove],
                lhs: [la, None, lr],
                rhs: [ra, None, rr],
            } => {
                // Reduce "add" as: (LA - RR) U RA.
                if let Some(v) = bld.vec_term(la, rr.as_ref(), false, UNION, ra)? {
                    out.insert(add, v);
                }
                // Reduce "remove" as: LR U RR.
                if let (Some(v), false) = (bld.vec_term(lr, None, false, UNION, rr)?, bld.prune) {
                    out.insert(remove, v);
                }
            }

            // I,A reduce I,A
            Destructured::Object {
                props: [add, intersect, _],
                lhs: [la, Some(li), None],
                rhs: [ra, Some(ri), None],
            } => {
                // Reduce "add" as: (LA - RI') U RA.
                if let Some(v) = bld.map_term(la, Some(&ri), true, UNION, ra)? {
                    out.insert(add, v);
                }
                // Reduce "intersect" as: LI & RI.
                if let (Some(v), false) = (
                    bld.map_term(Some(li), None, false, BOTH, Some(ri))?,
                    bld.prune,
                ) {
                    out.insert(intersect, v);
                }
            }
            // I,A reduce R,A
            Destructured::Object {
                props: [add, intersect, _],
                lhs: [la, Some(li), None],
                rhs: [ra, None, rr],
            } => {
                // Reduce "add" as: (LA - RR) U RA.
                if let Some(v) = bld.map_term(la, rr.as_ref(), false, UNION, ra)? {
                    out.insert(add, v);
                }
                // Reduce "intersect" as: LI - RR.
                if let (Some(v), false) =
                    (bld.map_term(Some(li), None, false, LEFT, rr)?, bld.prune)
                {
                    out.insert(intersect, v);
                }
            }
            // R,A reduce I,A
            Destructured::Object {
                props: [add, intersect, _],
                lhs: [la, None, lr],
                rhs: [ra, Some(ri), None],
            } => {
                // Reduce "add" as: (LA - RI') U RA.
                if let Some(v) = bld.map_term(la, Some(&ri), true, UNION, ra)? {
                    out.insert(add, v);
                }
                // Reduce "intersect" as: RI - LR.
                if let (Some(v), false) =
                    (bld.map_term(lr, None, false, RIGHT, Some(ri))?, bld.prune)
                {
                    out.insert(intersect, v);
                }
            }
            // R,A reduce R,A
            Destructured::Object {
                props: [add, _, remove],
                lhs: [la, None, lr],
                rhs: [ra, None, rr],
            } => {
                // Reduce "add" as: (LA - RR) U RA.
                if let Some(v) = bld.map_term(la, rr.as_ref(), false, UNION, ra)? {
                    out.insert(add, v);
                }
                // Reduce "remove" as: LR U RR.
                if let (Some(v), false) = (bld.map_term(lr, None, false, UNION, rr)?, bld.prune) {
                    out.insert(remove, v);
                }
            }

            _ => return Err(Error::at(loc, Error::SetWrongType)),
        };

        Ok(Value::Object(out))
    }
}

#[cfg(test)]
mod test {
    use super::super::test::*;
    use super::{Destructured, Location};

    #[test]
    fn test_destructure_cases() {
        use Destructured::{Array, Object};
        let rt = Location::Root;

        // Excercise add / intersect / remove on either side, with property collection.
        let d = Destructured::extract(rt, json!({"add": []}), json!({"remove": []})).unwrap();
        assert!(matches!(d, Array{
                props: [add, _, remove],
                lhs: [Some(_), None, None],
                rhs: [None, None, Some(_)],
            } if add == "add" && remove == "remove"));

        let d = Destructured::extract(rt, json!({"remove": []}), json!({"intersect": []})).unwrap();
        assert!(matches!(d, Array{
                props: [_, intersect, remove],
                lhs: [None, None, Some(_)],
                rhs: [None, Some(_), None],
            } if intersect == "intersect" && remove == "remove"));

        let d = Destructured::extract(rt, json!({"intersect": []}), json!({"add": []})).unwrap();
        assert!(matches!(d, Array{
                props: [add, intersect, _],
                lhs: [None, Some(_), None],
                rhs: [Some(_), None, None],
            } if add == "add" && intersect == "intersect"));

        let d = Destructured::extract(rt, json!({"add": {}}), json!({"remove": {}})).unwrap();
        assert!(matches!(d, Object{
                props: [add, _, remove],
                lhs: [Some(_), None, None],
                rhs: [None, None, Some(_)],
            } if add == "add" && remove == "remove"));

        let d = Destructured::extract(rt, json!({"remove": {}}), json!({"intersect": {}})).unwrap();
        assert!(matches!(d, Object{
                props: [_, intersect, remove],
                lhs: [None, None, Some(_)],
                rhs: [None, Some(_), None],
            } if intersect == "intersect" && remove == "remove"));

        let d = Destructured::extract(rt, json!({"intersect": {}}), json!({"add": {}})).unwrap();
        assert!(matches!(d, Object{
                props: [add, intersect, _],
                lhs: [None, Some(_), None],
                rhs: [Some(_), None, None],
            } if add == "add" && intersect == "intersect"));

        // Either side may be empty.
        let d = Destructured::extract(rt, json!({}), json!({"add": {}, "remove": {}})).unwrap();
        assert!(matches!(d, Object{
            lhs: [None, None, None],
            rhs: [Some(_), None, Some(_)],
            ..
        }));
        let d = Destructured::extract(rt, json!({"add": [], "remove": []}), json!({})).unwrap();
        assert!(matches!(d, Array{
            lhs: [Some(_), None, Some(_)],
            rhs: [None, None, None],
            ..
        }));

        // Cases that fail:

        // Mixed types within a side.
        Destructured::extract(rt, json!({"add": {}, "intersect": []}), json!({})).unwrap_err();
        // Mixed types across sides.
        Destructured::extract(rt, json!({"add": {}}), json!({"intersect": []})).unwrap_err();
        // Both "intersect" and "remove" on a side.
        Destructured::extract(rt, json!({"intersect": [], "remove": []}), json!({})).unwrap_err();
        // Not an object.
        Destructured::extract(rt, json!({"intersect": []}), json!(42)).unwrap_err();
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
            ],
        )
    }
}
