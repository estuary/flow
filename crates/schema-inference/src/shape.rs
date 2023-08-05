use doc::shape::{ArrayShape, ObjProperty, ObjShape, Shape, StringShape};
use itertools::{EitherOrBoth, Itertools};
use json::schema::types;

// TODO(johnny): We are *very* close to being able to define merge()
// in terms of a trivial Shape::union(). The one reason we cannot,
// is because we would have to mark object shapes with additionalProperties: false,
// which is definitely something we should do but not until we have a tight
// feedback / re-publication cycle around inferred-schema violations.
//
// See comment on infer_object_shape()
//
// For now, the difference between merge() and union() is that merge
// does not prune properties present in one schema but not the other,
// where the other schema does not specifically restrict that property
// from existing. merge() also doesn't handle a slew of cases that
// union() *does* handle, and we should seek to remove it.
pub fn merge(lhs: Shape, rhs: Shape) -> Shape {
    let string = match (
        lhs.type_.overlaps(types::STRING),
        rhs.type_.overlaps(types::STRING),
    ) {
        (true, true) => StringShape::union(lhs.string, rhs.string),
        (_, false) => lhs.string,
        (false, true) => rhs.string,
    };
    let array = match (
        lhs.type_.overlaps(types::ARRAY),
        rhs.type_.overlaps(types::ARRAY),
    ) {
        (true, true) => merge_array_shapes(lhs.array, rhs.array),
        (_, false) => lhs.array,
        (false, true) => rhs.array,
    };
    let object = match (
        lhs.type_.overlaps(types::OBJECT),
        rhs.type_.overlaps(types::OBJECT),
    ) {
        (true, true) => merge_obj_shapes(lhs.object, rhs.object),
        (_, false) => lhs.object,
        (false, true) => rhs.object,
    };

    Shape {
        type_: lhs.type_ | rhs.type_,
        array,
        object,
        string,
        ..Shape::anything()
    }
}

fn merge_obj_shapes(lhs: ObjShape, rhs: ObjShape) -> ObjShape {
    // NOTE(johnny): This is an incorrect re-implementation of ObjShape::union(),
    // which omits a slew of important details.
    //
    // The one salient difference that we currently rely on is that
    // this implementation preserves properties in all cases.
    // ObjShape::union(), conversely, will "preserve" a property only if the other
    // side constrains that property to not exist (as otherwise its schema must be
    // widened to the point of being completely unconstrained,
    // which is why ObjShape::union() removes it).

    let properties = itertools::merge_join_by(
        lhs.properties.into_iter(),
        rhs.properties.into_iter(),
        |l, r| Ord::cmp(&l.name, &r.name),
    )
    .map(|eob| match eob {
        EitherOrBoth::Both(l, r) => ObjProperty {
            name: l.name,
            is_required: l.is_required && r.is_required,
            shape: merge(l.shape, r.shape),
        },
        EitherOrBoth::Left(l) => ObjProperty {
            name: l.name,
            is_required: false,
            shape: l.shape,
        },
        EitherOrBoth::Right(r) => ObjProperty {
            name: r.name,
            is_required: false,
            shape: r.shape,
        },
    })
    .collect::<Vec<ObjProperty>>();

    ObjShape {
        properties,
        patterns: vec![],
        additional: None,
    }
}

fn merge_array_shapes(lhs: ArrayShape, rhs: ArrayShape) -> ArrayShape {
    // NOTE(johnny): This is an incorrect re-implementation of ArrayShape::union(),
    // which exists only so that merge_obj_shapes() is called to handled
    // nested sub-objects.
    let tuple = lhs
        .tuple
        .into_iter()
        .zip_longest(rhs.tuple.into_iter())
        .map(|eob| match eob {
            EitherOrBoth::Both(l, r) => merge(l, r),
            EitherOrBoth::Left(l) => l,
            EitherOrBoth::Right(r) => r,
        })
        .collect::<Vec<Shape>>();

    ArrayShape {
        min: None,
        max: None,
        tuple,
        additional: None,
    }
}
