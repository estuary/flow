// This module defines a union operation over Shapes.
// Union'ed Shapes impose only their common constraints
// like a JSON Schema `anyOf` keyword.
use super::*;
use itertools::{EitherOrBoth, Itertools};

impl Reduction {
    fn union(self, rhs: Self) -> Self {
        match (self, rhs) {
            (lhs, rhs) if lhs == rhs => lhs,
            // If either side is Unset (unconstrained), so is the union.
            (Self::Unset, _) => Self::Unset,
            (_, Self::Unset) => Self::Unset,
            // Both sides are unequal but also not Unset.
            (_, _) => Self::Multiple,
        }
    }
}

impl Provenance {
    fn union(self, rhs: Self) -> Self {
        match (self, rhs) {
            (lhs, rhs) if lhs == rhs => lhs,
            // If either side is Unset (unconstrained), so is the union.
            (Self::Unset, _) => Self::Unset,
            (_, Self::Unset) => Self::Unset,
            // Both sides are unequal and also not Unset. Promote to Inline.
            (_, _) => Self::Inline,
        }
    }
}

impl StringShape {
    pub fn union(lhs: Self, rhs: Self) -> Self {
        let max_length = match (lhs.max_length, rhs.max_length) {
            (Some(l), Some(r)) => Some(l.max(r)),
            _ => None,
        };

        StringShape {
            content_encoding: union_option(lhs.content_encoding, rhs.content_encoding),
            content_type: union_option(lhs.content_type, rhs.content_type),
            format: Self::union_format(lhs.format, rhs.format),
            max_length,
            min_length: lhs.min_length.min(rhs.min_length),
        }
    }

    fn union_format(lhs: Option<Format>, rhs: Option<Format>) -> Option<Format> {
        match (lhs, rhs) {
            // Generally, keep `format` only if both sides agree.
            (Some(l), Some(r)) if l == r => Some(l),
            // As a special case, we can generalize `format: integer union format: number` => `format: number`.
            (Some(Format::Integer), Some(Format::Number))
            | (Some(Format::Number), Some(Format::Integer)) => Some(Format::Number),
            _ => None,
        }
    }
}

impl ObjShape {
    fn union(lhs: Self, rhs: Self) -> Self {
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

        // Derive the super-set of properties of both sides. As with intersections, for
        // properties on one side but not the other we impute a property for the missing
        // side by examining matching patterns or additional properties.
        let union_imputed = |side: ObjProperty, other: Option<Shape>| {
            if let Some(other) = other {
                Some(ObjProperty {
                    name: side.name,
                    is_required: false,
                    shape: Shape::union(side.shape, other),
                })
            } else {
                // Union of |side| || any => any.
                None
            }
        };
        let properties = itertools::merge_join_by(
            lhs_properties.into_iter(),
            rhs_properties.into_iter(),
            |l, r| Ord::cmp(&l.name, &r.name),
        )
        .filter_map(|eob| match eob {
            EitherOrBoth::Both(l, r) => Some(ObjProperty {
                name: l.name,
                is_required: l.is_required && r.is_required,
                shape: Shape::union(l.shape, r.shape),
            }),
            EitherOrBoth::Left(l) => {
                let r = impute_property_shape(&l.name, &rhs_patterns, rhs_addl.as_deref());
                union_imputed(l, r)
            }
            EitherOrBoth::Right(r) => {
                let l = impute_property_shape(&r.name, &lhs_patterns, lhs_addl.as_deref());
                union_imputed(r, l)
            }
        })
        .collect::<Vec<_>>();

        // Union patterns have exact regex correspondence, and drop others.
        let patterns = itertools::merge_join_by(
            lhs_patterns.into_iter(),
            rhs_patterns.into_iter(),
            |l, r| Ord::cmp(l.re.as_str(), r.re.as_str()),
        )
        .filter_map(|eob| match eob {
            EitherOrBoth::Both(l, r) => Some(ObjPattern {
                re: l.re,
                shape: Shape::union(l.shape, r.shape),
            }),
            EitherOrBoth::Left(l) if rhs_addl.is_some() => Some(ObjPattern {
                re: l.re,
                shape: Shape::union(l.shape, *rhs_addl.clone().unwrap()),
            }),
            EitherOrBoth::Right(r) if lhs_addl.is_some() => Some(ObjPattern {
                re: r.re,
                shape: Shape::union(*lhs_addl.clone().unwrap(), r.shape),
            }),
            _ => None,
        })
        .collect::<Vec<_>>();

        Self {
            properties,
            pattern_properties: patterns,
            additional_properties: union_additional(lhs_addl, rhs_addl),
        }
    }
}

impl ArrayShape {
    fn union(lhs: Self, rhs: Self) -> Self {
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

        // Take the least-restrictive bounds of both.
        let min_items = lhs_min.min(rhs_min);
        let max_items = lhs_max.and(rhs_max).and(lhs_max.max(rhs_max));

        // Derive a tuple which unions the tuples of each side. If the shorter side also
        // supplies additional items, use that to fill out the tuple to the longer
        // of the two sides. Otherwise, the tuple will be the shorter of the two sides,
        // since items beyond the short-side tuple are unconstrained.
        let tuple = lhs_tuple
            .into_iter()
            .zip_longest(rhs_tuple.into_iter())
            .filter_map(|eob| match eob {
                EitherOrBoth::Both(l, r) => Some(Shape::union(l, r)),
                EitherOrBoth::Left(l) => match &rhs_addl {
                    Some(r) => Some(Shape::union(l, r.as_ref().clone())),
                    None => None,
                },
                EitherOrBoth::Right(r) => match &lhs_addl {
                    Some(l) => Some(Shape::union(l.as_ref().clone(), r)),
                    None => None,
                },
            })
            .collect::<Vec<_>>();

        Self {
            min_items,
            max_items,
            tuple,
            additional_items: union_additional(lhs_addl, rhs_addl),
        }
    }
}

impl NumericShape {
    fn union(lhs: Self, rhs: Self) -> Self {
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

        // Take the least-restrictive bounds of both. We must be careful here to always prefer
        // a float value over an integer value if the two are otherwise equal (the `Ord` impl for
        // `Number` ignores such differences). For example, we prefer 5.0 over 5. This ensures that
        // the minimum/maximum values are consistent with the `type` in inferred schemas.
        let min = if let Some((lmin, rmin)) = lhs_min.zip(rhs_min) {
            match lmin.cmp(&rmin) {
                std::cmp::Ordering::Less => Some(lmin),
                std::cmp::Ordering::Equal if lmin.is_float() => Some(lmin),
                std::cmp::Ordering::Equal => Some(rmin),
                std::cmp::Ordering::Greater => Some(rmin),
            }
        } else {
            None
        };
        let max = if let Some((lmax, rmax)) = lhs_max.zip(rhs_max) {
            match lmax.cmp(&rmax) {
                std::cmp::Ordering::Less => Some(rmax),
                std::cmp::Ordering::Equal if lmax.is_float() => Some(lmax),
                std::cmp::Ordering::Equal => Some(rmax),
                std::cmp::Ordering::Greater => Some(lmax),
            }
        } else {
            None
        };

        Self {
            minimum: min,
            maximum: max,
        }
    }
}

impl Shape {
    pub fn union(lhs: Self, rhs: Self) -> Self {
        // If one side of the union cannot exist, the union is trivially the other side.
        if lhs.type_ == types::INVALID {
            return rhs;
        } else if rhs.type_ == types::INVALID {
            return lhs;
        }

        let type_ = lhs.type_ | rhs.type_;
        let enum_ = union_enum(lhs.enum_, rhs.enum_);
        let title = union_option(lhs.title, rhs.title);
        let description = union_option(lhs.description, rhs.description);
        let reduction = lhs.reduction.union(rhs.reduction);
        let provenance = lhs.provenance.union(rhs.provenance);
        let default = union_option(lhs.default, rhs.default);
        let secret = union_option(lhs.secret, rhs.secret);

        // Union of annotations is actually an _intersection_, which yields only
        // the annotations that are guaranteed to apply at a given location.
        let mut annotations = lhs.annotations;
        annotations.retain(|k, v| rhs.annotations.get(k) == Some(&*v));

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
            (true, true) => ArrayShape::union(lhs.array, rhs.array),
            (_, false) => lhs.array,
            (false, true) => rhs.array,
        };
        let object = match (
            lhs.type_.overlaps(types::OBJECT),
            rhs.type_.overlaps(types::OBJECT),
        ) {
            (true, true) => ObjShape::union(lhs.object, rhs.object),
            (_, false) => lhs.object,
            (false, true) => rhs.object,
        };
        let numeric = match (
            lhs.type_.overlaps(types::INT_OR_FRAC),
            rhs.type_.overlaps(types::INT_OR_FRAC),
        ) {
            (true, true) => NumericShape::union(lhs.numeric, rhs.numeric),
            (_, false) => lhs.numeric,
            (false, true) => rhs.numeric,
        };

        Self {
            type_,
            enum_,
            title,
            description,
            reduction,
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

fn union_option<T: Eq>(lhs: Option<T>, rhs: Option<T>) -> Option<T> {
    if lhs == rhs {
        lhs
    } else {
        None
    }
}

fn union_additional(lhs: Option<Box<Shape>>, rhs: Option<Box<Shape>>) -> Option<Box<Shape>> {
    match (lhs, rhs) {
        (Some(lhs), Some(rhs)) => Some(Box::new(Shape::union(
            lhs.as_ref().clone(),
            rhs.as_ref().clone(),
        ))),
        _ => None, // If either side is unrestricted, the union is unrestricted.
    }
}

fn union_enum(lhs: Option<Vec<Value>>, rhs: Option<Vec<Value>>) -> Option<Vec<Value>> {
    if lhs.is_none() || rhs.is_none() {
        return None; // If either side us unconstrained, so is the union.
    }
    let (lhs, rhs) = (lhs.unwrap(), rhs.unwrap());

    Some(
        itertools::merge_join_by(lhs.into_iter(), rhs.into_iter(), crate::compare)
            .map(|eob| match eob {
                EitherOrBoth::Both(l, _) => l,
                EitherOrBoth::Left(l) => l,
                EitherOrBoth::Right(r) => r,
            })
            .collect::<Vec<_>>(),
    )
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn numeric_shape_union() {
        let actual = NumericShape::union(
            NumericShape {
                minimum: Some(json::Number::Signed(-5)),
                maximum: Some(json::Number::Unsigned(5)),
            },
            NumericShape {
                minimum: Some(json::Number::Signed(-4)),
                maximum: Some(json::Number::Float(5.0)),
            },
        );
        assert_eq!(
            NumericShape {
                minimum: Some(json::Number::Signed(-5)),
                maximum: Some(json::Number::Float(5.0)),
            },
            actual
        );
        assert!(actual.maximum.is_some_and(|m| m.is_float()));

        let actual = NumericShape::union(
            NumericShape {
                minimum: None,
                maximum: Some(json::Number::Unsigned(500)),
            },
            NumericShape {
                minimum: Some(json::Number::Signed(-4)),
                maximum: None,
            },
        );
        assert_eq!(
            NumericShape {
                minimum: None,
                maximum: None,
            },
            actual
        );
    }
}
