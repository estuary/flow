// This module defines an intersection operation over Shapes.
// Intersected Shapes impose *all* of their constraints,
// like a JSON Schema `allOf` keyword.
use super::*;
use crate::FailedValidation;
use itertools::{EitherOrBoth, Itertools};

impl Reduce {
    fn intersect(self, rhs: Self) -> Self {
        if let Self::Unset = self { rhs } else { self }
    }
}

impl Redact {
    fn intersect(self, rhs: Self) -> Self {
        if let Self::Unset = self { rhs } else { self }
    }
}

impl Provenance {
    fn intersect(self, rhs: Self) -> Self {
        match (self, rhs) {
            (lhs, rhs) if lhs == rhs => lhs,
            // If either side is Unset (unconstrained), take the other.
            (Self::Unset, rhs) => rhs,
            (lhs, Self::Unset) => lhs,
            // Both sides are unequal and also not Unset. Promote to Inline.
            (_, _) => Self::Inline,
        }
    }
}

impl StringShape {
    pub fn intersect(lhs: Self, rhs: Self) -> Self {
        let max_length = match (lhs.max_length, rhs.max_length) {
            (Some(l), Some(r)) => Some(l.min(r)),
            (Some(l), None) => Some(l),
            (None, Some(r)) => Some(r),
            _ => None,
        };
        StringShape {
            content_encoding: lhs.content_encoding.or(rhs.content_encoding),
            content_type: lhs.content_type.or(rhs.content_type),
            format: lhs.format.or(rhs.format),
            min_length: lhs.min_length.max(rhs.min_length),
            max_length,
        }
    }
}

impl ObjShape {
    pub fn intersect(lhs: Self, rhs: Self) -> Self {
        // Destructure to make borrow-checker happy.
        let (
            Self {
                properties: lhs_properties,
                pattern_properties: lhs_patterns,
                additional_properties: lhs_addl,
            },
            Self {
                properties: rhs_properties,
                pattern_properties: rhs_patterns,
                additional_properties: rhs_addl,
            },
        ) = (lhs, rhs);

        let properties = itertools::merge_join_by(
            lhs_properties.into_iter(),
            rhs_properties.into_iter(),
            |l, r| Ord::cmp(&l.name, &r.name),
        )
        .map(|eob| {
            let (name, is_required, l_shape, r_shape) = match eob {
                EitherOrBoth::Both(l, r) => (
                    l.name,
                    l.is_required || r.is_required,
                    l.is_property.then_some(l.shape),
                    r.is_property.then_some(r.shape),
                ),
                EitherOrBoth::Left(l) => (
                    l.name,
                    l.is_required,
                    l.is_property.then_some(l.shape),
                    None,
                ),
                EitherOrBoth::Right(r) => (
                    r.name,
                    r.is_required,
                    None,
                    r.is_property.then_some(r.shape),
                ),
            };

            // For properties on one side but not the other, impute a property for the missing
            // side by examining matching patterns or additional properties.
            let l_shape = l_shape
                .or_else(|| impute_property_shape(&name, &lhs_patterns, lhs_addl.as_deref()));
            let r_shape = r_shape
                .or_else(|| impute_property_shape(&name, &rhs_patterns, rhs_addl.as_deref()));
            let shape = match (l_shape, r_shape) {
                (Some(l), Some(r)) => Some(Shape::intersect(l, r)),
                (Some(s), None) | (None, Some(s)) => Some(s), // Intersection of `s` && any => `s`.
                (None, None) => None,
            };

            ObjProperty {
                name: name.into(),
                is_required,
                is_property: shape.is_some(),
                shape: shape.unwrap_or(Shape::anything()),
            }
        })
        .collect::<Vec<_>>();

        // Merge the set of patterns (all must apply in an intersection).
        let patterns = itertools::merge_join_by(
            lhs_patterns.into_iter(),
            rhs_patterns.into_iter(),
            |l, r| Ord::cmp(l.re.as_str(), r.re.as_str()),
        )
        .map(|eob| match eob {
            EitherOrBoth::Both(l, r) => ObjPattern {
                re: l.re,
                shape: Shape::intersect(l.shape, r.shape),
            },
            EitherOrBoth::Left(l) => l,
            EitherOrBoth::Right(r) => r,
        })
        .collect::<Vec<_>>();

        Self {
            properties,
            pattern_properties: patterns,
            additional_properties: intersect_additional(lhs_addl, rhs_addl),
        }
    }
}

impl ArrayShape {
    fn intersect(lhs: Self, rhs: Self) -> Self {
        let (
            Self {
                min_items: lhs_min,
                max_items: lhs_max,
                tuple: lhs_tuple,
                additional_items: lhs_addl,
            },
            Self {
                min_items: rhs_min,
                max_items: rhs_max,
                tuple: rhs_tuple,
                additional_items: rhs_addl,
            },
        ) = (lhs, rhs);

        // Take the most-restrictive bounds.
        let min = lhs_min.max(rhs_min);
        let max = if lhs_max.and(rhs_max).is_some() {
            lhs_max.min(rhs_max)
        } else {
            lhs_max.or(rhs_max)
        };

        // Derive a tuple which is the longest of the two sides. If the shorter side also
        // supplies additional items, that's used to fill out the tuple to the longer of
        // the two sides. Otherwise, items of the longer side are taken as-is, since
        // items beyond the short-side tuple are unconstrained.
        let tuple = lhs_tuple
            .into_iter()
            .zip_longest(rhs_tuple.into_iter())
            .map(|eob| match eob {
                EitherOrBoth::Both(l, r) => Shape::intersect(l, r),
                EitherOrBoth::Left(l) => match &rhs_addl {
                    Some(r) => Shape::intersect(l, r.as_ref().clone()),
                    None => l,
                },
                EitherOrBoth::Right(r) => match &lhs_addl {
                    Some(l) => Shape::intersect(l.as_ref().clone(), r),
                    None => r,
                },
            })
            .collect::<Vec<_>>();

        Self {
            min_items: min,
            max_items: max,
            tuple,
            additional_items: intersect_additional(lhs_addl, rhs_addl),
        }
    }
}

impl NumericShape {
    fn intersect(lhs: Self, rhs: Self) -> Self {
        let (
            Self {
                minimum: lhs_min,
                maximum: lhs_max,
            },
            Self {
                minimum: rhs_min,
                maximum: rhs_max,
            },
        ) = (lhs, rhs);

        // Take the most-restrictive bounds. Integers are considered more restrictive than
        // floats here, even though for example `5.0` and `5` are considered equal. This helps
        // ensure that the minimum/maximum value representations stay in line with the `type`,
        // since the intersection of `type: number` and `integer` is the narrower `integer` type.
        let min = if let Some((lmin, rmin)) = lhs_min.zip(rhs_min) {
            match lmin.cmp(&rmin) {
                std::cmp::Ordering::Less => Some(rmin),
                std::cmp::Ordering::Equal if !lmin.is_float() => Some(lmin),
                std::cmp::Ordering::Equal => Some(rmin),
                std::cmp::Ordering::Greater => Some(lmin),
            }
        } else {
            lhs_min.max(rhs_min)
        };

        let max = if let Some((lmax, rmax)) = rhs_max.zip(lhs_max) {
            match lmax.cmp(&rmax) {
                std::cmp::Ordering::Less => Some(lmax),
                std::cmp::Ordering::Equal if !lmax.is_float() => Some(lmax),
                std::cmp::Ordering::Equal => Some(rmax),
                std::cmp::Ordering::Greater => Some(rmax),
            }
        } else {
            lhs_max.or(rhs_max)
        };

        Self {
            minimum: min,
            maximum: max,
        }
    }
}

impl Shape {
    pub fn intersect(lhs: Self, rhs: Self) -> Self {
        let mut type_ = lhs.type_ & rhs.type_;
        // The enum intersection is additionally filtered to variants matching
        // the intersected type.
        let enum_ = intersect_enum(type_, lhs.enum_, rhs.enum_);
        // Further tighten type_ to the possible variant types of the intersected
        // enum. For example, consider an intersection of ["a", 1], [1, "b"] where
        // type_ is STRING | INTEGER.
        if let Some(enum_) = &enum_ {
            type_ = type_ & value_types(enum_.iter());
        }

        let title = lhs.title.or(rhs.title);
        let description = lhs.description.or(rhs.description);
        let reduce = lhs.reduce.intersect(rhs.reduce);
        let redact = lhs.redact.intersect(rhs.redact);
        let provenance = lhs.provenance.intersect(rhs.provenance);
        let default = intersect_default(type_, lhs.default, rhs.default);
        let secret = lhs.secret.or(rhs.secret);

        let mut annotations = rhs.annotations;
        annotations.extend(lhs.annotations.into_iter());

        let string = match (
            lhs.type_.overlaps(types::STRING),
            rhs.type_.overlaps(types::STRING),
        ) {
            (true, true) => StringShape::intersect(lhs.string, rhs.string),
            (_, _) => StringShape::new(),
        };
        let array = match (
            lhs.type_.overlaps(types::ARRAY),
            rhs.type_.overlaps(types::ARRAY),
        ) {
            (true, true) => ArrayShape::intersect(lhs.array, rhs.array),
            (_, _) => ArrayShape::new(),
        };
        let object = match (
            lhs.type_.overlaps(types::OBJECT),
            rhs.type_.overlaps(types::OBJECT),
        ) {
            (true, true) => ObjShape::intersect(lhs.object, rhs.object),
            (_, _) => ObjShape::new(),
        };
        let numeric = match (
            lhs.type_.overlaps(types::INT_OR_FRAC),
            rhs.type_.overlaps(types::INT_OR_FRAC),
        ) {
            (true, true) => NumericShape::intersect(lhs.numeric, rhs.numeric),
            (_, _) => NumericShape::new(),
        };

        Self {
            type_,
            enum_,
            title,
            description,
            reduce,
            redact,
            provenance,
            default,
            secret,
            annotations,
            string,
            array,
            object,
            numeric,
        }
    }
}

pub fn intersect_enum(
    type_: types::Set,
    lhs: Option<Vec<Value>>,
    rhs: Option<Vec<Value>>,
) -> Option<Vec<Value>> {
    match (lhs, rhs) {
        (None, None) => None,
        (Some(l), None) | (None, Some(l)) => {
            Some(filter_enums_to_types(type_, l.into_iter()).collect())
        }
        (Some(l), Some(r)) => {
            let it = itertools::merge_join_by(l.into_iter(), r.into_iter(), json::node::compare)
                .filter_map(|eob| match eob {
                    EitherOrBoth::Both(l, _) => Some(l),
                    _ => None,
                });
            let it = filter_enums_to_types(type_, it);
            Some(it.collect())
        }
    }
}

pub fn intersect_default(
    type_: types::Set,
    lhs: Option<Box<(Value, Option<FailedValidation>)>>,
    rhs: Option<Box<(Value, Option<FailedValidation>)>>,
) -> Option<Box<(Value, Option<FailedValidation>)>> {
    match (lhs, rhs) {
        (None, None) => None,
        (Some(l), None) | (None, Some(l)) => {
            if type_.overlaps(types::Set::for_node(&l.as_ref().0)) {
                Some(l)
            } else {
                None
            }
        }
        (Some(l), Some(r)) => {
            if type_.overlaps(types::Set::for_node(&l.as_ref().0)) {
                Some(l)
            } else if type_.overlaps(types::Set::for_node(&r.as_ref().0)) {
                Some(r)
            } else {
                None
            }
        }
    }
}

fn intersect_additional(lhs: Option<Box<Shape>>, rhs: Option<Box<Shape>>) -> Option<Box<Shape>> {
    match (lhs, rhs) {
        (Some(lhs), Some(rhs)) => Some(Box::new(Shape::intersect(
            lhs.as_ref().clone(),
            rhs.as_ref().clone(),
        ))),
        (Some(side), None) | (None, Some(side)) => Some(side),
        (None, None) => None,
    }
}

fn filter_enums_to_types<I: Iterator<Item = Value>>(
    type_: types::Set,
    it: I,
) -> impl Iterator<Item = Value> {
    it.filter(move |val| type_.overlaps(types::Set::for_node(val)))
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn numeric_shape_intersection() {
        let actual = NumericShape::intersect(
            NumericShape {
                minimum: Some(json::Number::from(-5i64)),
                maximum: Some(json::Number::from(5.0)),
            },
            NumericShape {
                minimum: Some(json::Number::from(-5.0f64)),
                maximum: Some(json::Number::from(5u64)),
            },
        );
        assert_eq!(
            NumericShape {
                minimum: Some(json::Number::from(-5i64)),
                maximum: Some(json::Number::from(5u64)),
            },
            actual
        );
        assert!(actual.minimum.is_some_and(|m| !m.is_float()));
        assert!(actual.maximum.is_some_and(|m| !m.is_float()));

        let actual = NumericShape::intersect(
            NumericShape {
                minimum: Some(json::Number::from(-5.0f64)),
                maximum: Some(json::Number::from(5u64)),
            },
            NumericShape {
                minimum: Some(json::Number::from(-5i64)),
                maximum: Some(json::Number::from(5.0f64)),
            },
        );
        assert_eq!(
            NumericShape {
                minimum: Some(json::Number::from(-5i64)),
                maximum: Some(json::Number::from(5u64)),
            },
            actual
        );
        assert!(actual.minimum.is_some_and(|m| !m.is_float()));
        assert!(actual.maximum.is_some_and(|m| !m.is_float()));

        let actual = NumericShape::intersect(
            NumericShape {
                minimum: None,
                maximum: Some(json::Number::from(500u64)),
            },
            NumericShape {
                minimum: Some(json::Number::from(-4i64)),
                maximum: None,
            },
        );
        assert_eq!(
            NumericShape {
                minimum: Some(json::Number::from(-4i64)),
                maximum: Some(json::Number::from(500u64)),
            },
            actual
        );
    }

    #[test]
    fn test_default_intersection() {
        let shape_with_reasonable_default = shape_from(
            r#"
            allOf:
                - type: ["string", "null"]
                  default: "hello"
                - type: "string"
            "#,
        );

        assert_eq!(
            shape_with_reasonable_default.default.unwrap().as_ref().0,
            serde_json::json!("hello")
        );

        let shape = shape_from(
            r#"
            allOf:
                - type: ["string", "null"]
                  default: null
                - type: "string"
            "#,
        );

        assert_eq!(shape.default, None);
    }
}
