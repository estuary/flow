use super::{index, types, Annotation, Application, CoreAnnotation, Keyword, Schema, Validation};
use itertools::{self, EitherOrBoth, Itertools};
use regex::Regex;
use serde_json::Value;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Location {
    pub type_: types::Set,
    pub enum_: Option<Vec<Value>>,
    pub title: Option<String>,
    pub description: Option<String>,

    pub string: StringLocation,
    pub array: ArrayLocation,
    pub object: ObjLocation,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StringLocation {
    pub is_base64: Option<bool>,
    pub content_type: Option<String>,
    pub format: Option<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ArrayLocation {
    pub min: Option<usize>,
    pub max: Option<usize>,
    pub tuple: Vec<Location>,
    pub additional: Option<Box<Location>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjLocation {
    pub properties: Vec<ObjProperty>,
    pub patterns: Vec<ObjPattern>,
    pub additional: Option<Box<Location>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjProperty {
    pub name: String,
    pub is_required: bool,
    pub value: Location,
}

#[derive(Clone, Debug)]
pub struct ObjPattern {
    pub re: Regex,
    pub value: Location,
}

impl Eq for ObjPattern {}

impl PartialEq for ObjPattern {
    fn eq(&self, other: &Self) -> bool {
        self.re.as_str() == other.re.as_str() && self.value == other.value
    }
}

impl StringLocation {
    fn intersect(lhs: Self, rhs: Self) -> Self {
        StringLocation {
            is_base64: lhs.is_base64.or(rhs.is_base64),
            content_type: lhs.content_type.or(rhs.content_type),
            format: lhs.format.or(rhs.format),
        }
    }

    fn union(lhs: Self, rhs: Self) -> Self {
        StringLocation {
            is_base64: union_option(lhs.is_base64, rhs.is_base64),
            content_type: union_option(lhs.content_type, rhs.content_type),
            format: union_option(lhs.format, rhs.format),
        }
    }
}

impl ObjLocation {
    fn intersect(lhs: Self, rhs: Self) -> Self {
        // Destructure to make borrow-checker happy.
        let (
            Self {
                properties: lhs_properties,
                patterns: lhs_patterns,
                additional: lhs_addl,
            },
            Self {
                properties: rhs_properties,
                patterns: rhs_patterns,
                additional: rhs_addl,
            },
        ) = (lhs, rhs);

        // Derive the super-set of properties of both sides.
        // For properties on one side but not the other, impute a property for the missing
        // side by examining matching patterns or additional properties.
        let intersect_imputed = |mut side: ObjProperty, other: Option<Location>| {
            if let Some(other) = other {
                side.value = Location::intersect(side.value, other);
            } else {
                // Interset of |side| && any => |side|.
            }
            side
        };
        let properties = itertools::merge_join_by(
            lhs_properties.into_iter(),
            rhs_properties.into_iter(),
            |l, r| Ord::cmp(&l.name, &r.name),
        )
        .map(|eob| match eob {
            EitherOrBoth::Both(l, r) => ObjProperty {
                name: l.name,
                is_required: l.is_required || r.is_required,
                value: Location::intersect(l.value, r.value),
            },
            EitherOrBoth::Left(l) => {
                let r = Self::impute(&l.name, &rhs_patterns, rhs_addl.as_deref());
                intersect_imputed(l, r)
            }
            EitherOrBoth::Right(r) => {
                let l = Self::impute(&r.name, &lhs_patterns, lhs_addl.as_deref());
                intersect_imputed(r, l)
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
                value: Location::intersect(l.value, r.value),
            },
            EitherOrBoth::Left(l) => l,
            EitherOrBoth::Right(r) => r,
        })
        .collect::<Vec<_>>();

        let additional = intersect_additional(lhs_addl, rhs_addl);

        Self {
            properties,
            patterns,
            additional,
        }
    }

    fn union(lhs: Self, rhs: Self) -> Self {
        // Destructure to make borrow-checker happy.
        let (
            Self {
                properties: lhs_properties,
                patterns: lhs_patterns,
                additional: lhs_addl,
            },
            Self {
                properties: rhs_properties,
                patterns: rhs_patterns,
                additional: rhs_addl,
            },
        ) = (lhs, rhs);

        // Derive the super-set of properties of both sides. As with intersections, for
        // properties on one side but not the other we impute a property for the missing
        // side by examining matching patterns or additional properties.
        let union_imputed = |side: ObjProperty, other: Option<Location>| {
            if let Some(other) = other {
                Some(ObjProperty {
                    name: side.name,
                    is_required: false,
                    value: Location::union(side.value, other),
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
                value: Location::union(l.value, r.value),
            }),
            EitherOrBoth::Left(l) => {
                let r = Self::impute(&l.name, &rhs_patterns, rhs_addl.as_deref());
                union_imputed(l, r)
            }
            EitherOrBoth::Right(r) => {
                let l = Self::impute(&r.name, &lhs_patterns, lhs_addl.as_deref());
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
                value: Location::union(l.value, r.value),
            }),
            _ => None,
        })
        .collect::<Vec<_>>();

        let additional = union_additional(lhs_addl, rhs_addl);

        Self {
            properties,
            patterns,
            additional,
        }
    }

    fn apply_patterns_to_properties(self: Self) -> Self {
        let ObjLocation {
            patterns,
            mut properties,
            additional,
        } = self;

        properties = properties
            .into_iter()
            .map(|mut prop| {
                for pattern in patterns.iter() {
                    if !pattern.re.is_match(&prop.name) {
                        continue;
                    }
                    prop.value = Location::intersect(prop.value, pattern.value.clone());
                }
                prop
            })
            .collect::<Vec<_>>();

        ObjLocation {
            patterns,
            properties,
            additional,
        }
    }

    fn impute(
        property: &str,
        patterns: &[ObjPattern],
        additional: Option<&Location>,
    ) -> Option<Location> {
        // Compute the intersection of all matching property patterns.
        let pattern = patterns.iter().fold(None, |prior, pattern| {
            if !pattern.re.is_match(property) {
                prior
            } else if let Some(prior) = prior {
                Some(Location::intersect(prior, pattern.value.clone()))
            } else {
                Some(pattern.value.clone())
            }
        });

        if let Some(pattern) = pattern {
            Some(pattern)
        } else if let Some(addl) = additional {
            Some(addl.clone())
        } else {
            None
        }
    }
}

impl ArrayLocation {
    fn union(lhs: Self, rhs: Self) -> Self {
        let (
            Self {
                min: lhs_min,
                max: lhs_max,
                tuple: lhs_tuple,
                additional: lhs_addl,
            },
            Self {
                min: rhs_min,
                max: rhs_max,
                tuple: rhs_tuple,
                additional: rhs_addl,
            },
        ) = (lhs, rhs);

        // Take the least-restrictive bounds of both.
        let min = lhs_min.and(rhs_min).and(lhs_min.min(rhs_min));
        let max = lhs_max.and(rhs_max).and(lhs_max.max(rhs_max));

        // Derive a tuple which unions the tuples of each side. If the shorter side also
        // supplies additional items, use that to fill out the tuple to the longer
        // of the two sides. Otherwise, the tuple will be the shorter of the two sides,
        // since items beyond the short-side tuple are unconstrained.
        let tuple = lhs_tuple
            .into_iter()
            .zip_longest(rhs_tuple.into_iter())
            .filter_map(|eob| match eob {
                EitherOrBoth::Both(l, r) => Some(Location::union(l, r)),
                EitherOrBoth::Left(l) => match &rhs_addl {
                    Some(r) => Some(Location::union(l, r.as_ref().clone())),
                    None => None,
                },
                EitherOrBoth::Right(r) => match &lhs_addl {
                    Some(l) => Some(Location::union(l.as_ref().clone(), r)),
                    None => None,
                },
            })
            .collect::<Vec<_>>();

        let additional = union_additional(lhs_addl, rhs_addl);

        Self {
            min,
            max,
            tuple,
            additional,
        }
    }

    fn intersect(lhs: Self, rhs: Self) -> Self {
        let (
            Self {
                min: lhs_min,
                max: lhs_max,
                tuple: lhs_tuple,
                additional: lhs_addl,
            },
            Self {
                min: rhs_min,
                max: rhs_max,
                tuple: rhs_tuple,
                additional: rhs_addl,
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
                EitherOrBoth::Both(l, r) => Location::intersect(l, r),
                EitherOrBoth::Left(l) => match &rhs_addl {
                    Some(r) => Location::intersect(l, r.as_ref().clone()),
                    None => l,
                },
                EitherOrBoth::Right(r) => match &lhs_addl {
                    Some(l) => Location::intersect(l.as_ref().clone(), r),
                    None => r,
                },
            })
            .collect::<Vec<_>>();

        let additional = intersect_additional(lhs_addl, rhs_addl);

        Self {
            min,
            max,
            tuple,
            additional,
        }
    }
}

impl Default for ObjLocation {
    fn default() -> Self {
        Self {
            properties: Vec::new(),
            patterns: Vec::new(),
            additional: None,
        }
    }
}

impl Default for Location {
    fn default() -> Self {
        Self {
            type_: types::ANY,
            enum_: None,
            title: None,
            description: None,
            string: StringLocation::default(),
            array: ArrayLocation::default(),
            object: ObjLocation::default(),
        }
    }
}

impl Location {
    pub fn infer<'s, A: Annotation>(
        schema: &'s Schema<A>,
        index: &index::Index<'s, A>,
    ) -> Location {
        // Walk validation and annotation keywords which affect the inference result
        // at the current location.

        let mut loc = Location::default();
        let mut unevaluated_properties: Option<Location> = None;
        let mut unevaluated_items: Option<Location> = None;

        // Walk validation keywords and subordinate applications which influence
        // the present Location.
        for kw in &schema.kw {
            match kw {
                // Type constraints.
                Keyword::Validation(Validation::False) => loc.type_ = types::INVALID,
                Keyword::Validation(Validation::Type(type_set)) => loc.type_ = *type_set,

                // Enum constraints.
                Keyword::Validation(Validation::Const(literal)) => {
                    loc.enum_ = Some(vec![literal.value.clone()])
                }
                Keyword::Validation(Validation::Enum { variants }) => {
                    loc.enum_ = Some(
                        variants
                            .iter()
                            .map(|hl| hl.value.clone())
                            .sorted_by(crate::json_cmp)
                            .collect::<Vec<_>>(),
                    );
                }

                Keyword::Annotation(annot) => match annot.as_core() {
                    Some(CoreAnnotation::Title(t)) => {
                        loc.title = Some(t.clone());
                    }
                    Some(CoreAnnotation::Description(d)) => {
                        loc.description = Some(d.clone());
                    }

                    // String constraints.
                    Some(CoreAnnotation::ContentEncodingBase64) => {
                        loc.string.is_base64 = Some(true);
                    }
                    Some(CoreAnnotation::ContentMediaType(mt)) => {
                        loc.string.content_type = Some(mt.clone());
                    }
                    _ => {} // Other CoreAnnotation. No-op.
                },

                // Array constraints.
                Keyword::Validation(Validation::MinItems(m)) => loc.array.min = Some(*m),
                Keyword::Validation(Validation::MaxItems(m)) => loc.array.max = Some(*m),
                Keyword::Application(Application::Items { index: None }, schema) => {
                    loc.array.additional = Some(Box::new(Location::infer(schema, index)));
                }
                Keyword::Application(Application::Items { index: Some(i) }, schema) => {
                    loc.array.tuple.extend(
                        std::iter::repeat(Location::default()).take(1 + i - loc.array.tuple.len()),
                    );
                    loc.array.tuple[*i] = Location::infer(schema, index);
                }
                Keyword::Application(Application::AdditionalItems, schema) => {
                    loc.array.additional = Some(Box::new(Location::infer(schema, index)));
                }
                Keyword::Application(Application::UnevaluatedItems, schema) => {
                    unevaluated_items = Some(Location::infer(schema, index));
                }

                // Object constraints.
                Keyword::Application(Application::Properties { name, .. }, schema) => {
                    let obj = ObjLocation {
                        properties: vec![ObjProperty {
                            name: name.clone(),
                            is_required: false,
                            value: Location::infer(schema, index),
                        }],
                        patterns: Vec::new(),
                        additional: None,
                    };
                    loc.object = ObjLocation::intersect(loc.object, obj);
                }
                Keyword::Validation(Validation::Required { props, .. }) => {
                    let obj = ObjLocation {
                        properties: props
                            .iter()
                            .sorted()
                            .map(|p| ObjProperty {
                                name: p.clone(),
                                is_required: true,
                                value: Location::default(),
                            })
                            .collect::<Vec<_>>(),
                        patterns: Vec::new(),
                        additional: None,
                    };
                    loc.object = ObjLocation::intersect(loc.object, obj);
                }

                Keyword::Application(Application::PatternProperties { re }, schema) => {
                    let obj = ObjLocation {
                        properties: Vec::new(),
                        patterns: vec![ObjPattern {
                            re: re.clone(),
                            value: Location::infer(schema, index),
                        }],
                        additional: None,
                    };
                    loc.object = ObjLocation::intersect(loc.object, obj);
                }
                Keyword::Application(Application::AdditionalProperties, schema) => {
                    loc.object.additional = Some(Box::new(Location::infer(schema, index)));
                }
                Keyword::Application(Application::UnevaluatedProperties, schema) => {
                    unevaluated_properties = Some(Location::infer(schema, index));
                }

                _ => {} // Other Keyword. No-op.
            }
        }

        // Apply pattern properties to applicable named properties.
        loc.object = loc.object.apply_patterns_to_properties();

        // Restrict enum variants to permitted types of the present schema.
        // We'll keep enforcing this invariant as Locations are intersected,
        // and allowed types are further restricted.
        loc.enum_ = intersect_enum(loc.type_, loc.enum_.take(), None);

        // Presence of an enum term similarly restricts the allowed types that
        // a location may take (since it may only take values of the enum).
        // We also check this again during intersection.
        if let Some(enum_) = &loc.enum_ {
            loc.type_ = loc.type_ & enum_types(enum_.iter());
        }

        // Now, collect inferences from in-place application keywords.
        let mut one_of: Option<Location> = None;
        let mut any_of: Option<Location> = None;
        let mut if_ = false;
        let mut then_: Option<Location> = None;
        let mut else_: Option<Location> = None;

        for kw in &schema.kw {
            match kw {
                Keyword::Application(Application::Ref(uri), _) => {
                    if let Some(schema) = index.fetch(uri) {
                        loc = Location::intersect(loc, Location::infer(schema, index));
                    }
                }
                Keyword::Application(Application::AllOf { .. }, schema) => {
                    loc = Location::intersect(loc, Location::infer(schema, index));
                }
                Keyword::Application(Application::OneOf { .. }, schema) => {
                    let l = Location::infer(schema, index);
                    one_of = Some(match one_of {
                        Some(one_of) => Location::union(one_of, l),
                        None => l,
                    })
                }
                Keyword::Application(Application::AnyOf { .. }, schema) => {
                    let l = Location::infer(schema, index);
                    any_of = Some(match any_of {
                        Some(any_of) => Location::union(any_of, l),
                        None => l,
                    })
                }
                Keyword::Application(Application::If, _) => if_ = true,
                Keyword::Application(Application::Then, schema) => {
                    then_ = Some(Location::infer(schema, index));
                }
                Keyword::Application(Application::Else, schema) => {
                    else_ = Some(Location::infer(schema, index));
                }

                _ => {} // Other Keyword. No-op.
            }
        }

        if let Some(one_of) = one_of {
            loc = Location::intersect(loc, one_of);
        }
        if let Some(any_of) = any_of {
            loc = Location::intersect(loc, any_of);
        }
        if if_ && then_.is_some() && else_.is_some() {
            let then_else = Location::union(then_.unwrap(), else_.unwrap());
            loc = Location::intersect(loc, then_else);
        }

        // Now, and *only* if loc.object.additional or loc.array.additional is
        // otherwise unset, then default to unevalutedProperties / unevaluatedItems.

        if loc.object.additional.is_none() && unevaluated_properties.is_some() {
            loc.object.additional = Some(Box::new(unevaluated_properties.unwrap()));
        }
        if loc.array.additional.is_none() && unevaluated_items.is_some() {
            loc.array.additional = Some(Box::new(unevaluated_items.unwrap()));
        }

        loc
    }

    fn union(lhs: Self, rhs: Self) -> Self {
        let type_ = lhs.type_ | rhs.type_;
        let enum_ = union_enum(lhs.enum_, rhs.enum_);
        let title = union_option(lhs.title, rhs.title);
        let description = union_option(lhs.description, rhs.description);

        let string = match (
            lhs.type_.overlaps(types::STRING),
            rhs.type_.overlaps(types::STRING),
        ) {
            (true, true) => StringLocation::union(lhs.string, rhs.string),
            (_, false) => lhs.string,
            (false, true) => rhs.string,
        };
        let array = match (
            lhs.type_.overlaps(types::ARRAY),
            rhs.type_.overlaps(types::ARRAY),
        ) {
            (true, true) => ArrayLocation::union(lhs.array, rhs.array),
            (_, false) => lhs.array,
            (false, true) => rhs.array,
        };
        let object = match (
            lhs.type_.overlaps(types::OBJECT),
            rhs.type_.overlaps(types::OBJECT),
        ) {
            (true, true) => ObjLocation::union(lhs.object, rhs.object),
            (_, false) => lhs.object,
            (false, true) => rhs.object,
        };

        Self {
            type_,
            enum_,
            title,
            description,
            string,
            array,
            object,
        }
    }

    fn intersect(lhs: Self, rhs: Self) -> Self {
        let mut type_ = lhs.type_ & rhs.type_;
        // The enum intersection is additionally filtered to varaints matching
        // the intersected type.
        let enum_ = intersect_enum(type_, lhs.enum_, rhs.enum_);
        // Further tighten type_ to the possible variant types of the intersected
        // enum. For example, consider an intersection of ["a", 1], [1, "b"] where
        // type_ is STRING | INTEGER.
        if let Some(enum_) = &enum_ {
            type_ = type_ & enum_types(enum_.iter());
        }

        let title = lhs.title.or(rhs.title);
        let description = lhs.description.or(rhs.description);

        let string = match (
            lhs.type_.overlaps(types::STRING),
            rhs.type_.overlaps(types::STRING),
        ) {
            (true, true) => StringLocation::intersect(lhs.string, rhs.string),
            (_, _) => StringLocation::default(),
        };
        let array = match (
            lhs.type_.overlaps(types::ARRAY),
            rhs.type_.overlaps(types::ARRAY),
        ) {
            (true, true) => ArrayLocation::intersect(lhs.array, rhs.array),
            (_, _) => ArrayLocation::default(),
        };
        let object = match (
            lhs.type_.overlaps(types::OBJECT),
            rhs.type_.overlaps(types::OBJECT),
        ) {
            (true, true) => ObjLocation::intersect(lhs.object, rhs.object),
            (_, _) => ObjLocation::default(),
        };

        Self {
            type_,
            enum_,
            title,
            description,
            string,
            array,
            object,
        }
    }
}

fn filter_enums_to_types<I: Iterator<Item = Value>>(
    type_: types::Set,
    it: I,
) -> impl Iterator<Item = Value> {
    it.filter(move |value| match value {
        Value::Null => type_.overlaps(types::NULL),
        Value::Bool(_) => type_.overlaps(types::BOOLEAN),
        Value::Number(n) => {
            let n = crate::Number::from(n);
            match n {
                crate::Number::Float(_) => type_.overlaps(types::NUMBER),
                crate::Number::Signed(_) | crate::Number::Unsigned(_) => {
                    type_.overlaps(types::NUMBER | types::INTEGER)
                }
            }
        }
        Value::String(_) => type_.overlaps(types::STRING),
        Value::Array(_) => type_.overlaps(types::ARRAY),
        Value::Object(_) => type_.overlaps(types::OBJECT),
    })
}

fn enum_types<'v, I: Iterator<Item = &'v Value>>(it: I) -> types::Set {
    it.fold(types::INVALID, |_type, v| {
        let t = match v {
            Value::String(_) => types::STRING,
            Value::Object(_) => types::OBJECT,
            Value::Number(_) => types::NUMBER,
            Value::Null => types::NULL,
            Value::Bool(_) => types::BOOLEAN,
            Value::Array(_) => types::ARRAY,
        };
        t | _type
    })
}

fn intersect_enum(
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
            let it = itertools::merge_join_by(l.into_iter(), r.into_iter(), crate::json_cmp)
                .filter_map(|eob| match eob {
                    EitherOrBoth::Both(l, _) => Some(l),
                    _ => None,
                });
            let it = filter_enums_to_types(type_, it);
            Some(it.collect())
        }
    }
}

fn union_enum(lhs: Option<Vec<Value>>, rhs: Option<Vec<Value>>) -> Option<Vec<Value>> {
    if lhs.is_none() || rhs.is_none() {
        return None; // If either side us unconstrained, so is the union.
    }
    let (lhs, rhs) = (lhs.unwrap(), rhs.unwrap());

    Some(
        itertools::merge_join_by(lhs.into_iter(), rhs.into_iter(), crate::json_cmp)
            .map(|eob| match eob {
                EitherOrBoth::Both(l, _) => l,
                EitherOrBoth::Left(l) => l,
                EitherOrBoth::Right(r) => r,
            })
            .collect::<Vec<_>>(),
    )
}

fn union_option<T: Eq>(lhs: Option<T>, rhs: Option<T>) -> Option<T> {
    if lhs == rhs {
        lhs
    } else {
        None
    }
}

fn union_additional(
    lhs: Option<Box<Location>>,
    rhs: Option<Box<Location>>,
) -> Option<Box<Location>> {
    match (lhs, rhs) {
        (Some(lhs), Some(rhs)) => Some(Box::new(Location::union(
            lhs.as_ref().clone(),
            rhs.as_ref().clone(),
        ))),
        _ => None, // If either side is unrestricted, the union is unrestricted.
    }
}

fn intersect_additional(
    lhs: Option<Box<Location>>,
    rhs: Option<Box<Location>>,
) -> Option<Box<Location>> {
    match (lhs, rhs) {
        (Some(lhs), Some(rhs)) => Some(Box::new(Location::intersect(
            lhs.as_ref().clone(),
            rhs.as_ref().clone(),
        ))),
        (Some(side), None) | (None, Some(side)) => Some(side),
        (None, None) => None,
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use serde_json::{json, Value};
    use serde_yaml;

    #[test]
    fn test_scalar_fields() {
        infer_test(
            &[
                // All fields in local schema.
                r#"
                type: [string, array]
                title: a-title
                description: a-description
                contentEncoding: base64
                contentMediaType: some/thing
                format: email
                "#,
                // Mix of anyOf, oneOf, & ref.
                r#"
                $defs:
                    aDef: {type: [string, array]}
                allOf:
                - title: a-title
                - description: a-description
                anyOf:
                - contentEncoding: base64
                - type: object # Elided (impossible).
                oneOf:
                - contentMediaType: some/thing
                - type: 'null' # Elided (impossible).
                $ref: '#/$defs/aDef'
                format: email
                "#,
            ],
            Location {
                type_: types::STRING | types::ARRAY,
                title: Some("a-title".to_owned()),
                description: Some("a-description".to_owned()),
                string: StringLocation {
                    is_base64: Some(true),
                    content_type: Some("some/thing".to_owned()),
                    format: None, // Not implemented yet.
                },
                ..Location::default()
            },
        );
    }

    #[test]
    fn test_enum_single_type() {
        infer_test(
            &[
                // Type restriction filters local cases.
                "{type: string, enum: [b, 42, a]}",
                // And also filters within an intersection.
                "allOf: [{type: string}, {enum: [a, 42, b]}]",
                "{type: string, anyOf: [{const: a}, {const: 42}, {const: b}]}",
                "{type: string, allOf: [{enum: [a, b, c, d, 1]}, {enum: [e, b, f, a, 1]}]}",
                "allOf: [{enum: [a, 1, b, 2]}, {type: string, enum: [e, b, f, a]}]",
            ],
            Location {
                type_: types::STRING,
                enum_: Some(vec![json!("a"), json!("b")]),
                ..Location::default()
            },
        )
    }

    #[test]
    fn test_enum_multi_type() {
        infer_test(
            &[
                "enum: [42, a, b]",
                "anyOf: [{const: a}, {const: b}, {const: 42}]",
                // Type restriction is dropped during union.
                "oneOf: [{const: b, type: string}, {enum: [a, 42]}]",
            ],
            enum_fixture(json!([42, "a", "b"])),
        )
    }

    #[test]
    fn test_pattern_applies_to_named_property() {
        infer_test(
            &[
                // All within one schema.
                r#"
                properties: {foo: {enum: [dropped, b]}, bar: {const: c}}
                patternProperties: {fo.+: {const: b}}
                required: [bar]
                "#,
                // Intersected across many in-place applications.
                r#"
                $defs:
                    isReq: {required: [bar]}
                $ref: '#/$defs/isReq'
                allOf:
                - properties: {foo: {enum: [dropped, b]}}
                - properties: {bar: {enum: [1, c, 2]}}
                - properties: {bar: {enum: [c, 3, 4]}}
                - patternProperties: {fo.+: {enum: [dropped, a, b]}}
                - patternProperties: {fo.+: {enum: [b, c]}}
                "#,
                // Union of named property with pattern.
                r#"
                oneOf:
                - properties: {foo: false}
                - patternProperties:
                    f.+: {enum: [a, b, c]}
                    other: {enum: [d, e, f]}
                properties: {bar: {const: c}}
                patternProperties: {fo.+: {const: b}} # filter 'foo' from [a, b, c]
                required: [bar]
                "#,
            ],
            Location {
                object: ObjLocation {
                    properties: vec![
                        ObjProperty {
                            name: "bar".to_owned(),
                            is_required: true,
                            value: enum_fixture(json!(["c"])),
                        },
                        ObjProperty {
                            name: "foo".to_owned(),
                            is_required: false,
                            value: enum_fixture(json!(["b"])),
                        },
                    ],
                    patterns: vec![ObjPattern {
                        re: regex::Regex::new("fo.+").unwrap(),
                        value: enum_fixture(json!(["b"])),
                    }],
                    additional: None,
                },
                ..Location::default()
            },
        )
    }

    #[test]
    fn test_additional_properties() {
        infer_test(
            &[
                // Local schema.
                r#"
                properties: {foo: {enum: [a, b]}}
                additionalProperties: {enum: [a, b]}
                "#,
                // Applies to imputed properties on intersection.
                r#"
                properties: {foo: {enum: [a, b, c, d]}}
                allOf:
                - additionalProperties: {enum: [a, b]}
                "#,
                r#"
                additionalProperties: {enum: [a, b]}
                allOf:
                - properties: {foo: {enum: [a, b, c, d]}}
                "#,
                // Applies to imputed properties on union.
                r#"
                oneOf:
                - properties: {foo: {enum: [a]}}
                - additionalProperties: {enum: [a, b, c, d]}
                additionalProperties: {enum: [a, b]}
                "#,
            ],
            Location {
                object: ObjLocation {
                    properties: vec![ObjProperty {
                        name: "foo".to_owned(),
                        is_required: false,
                        value: enum_fixture(json!(["a", "b"])),
                    }],
                    patterns: Vec::new(),
                    additional: Some(Box::new(enum_fixture(json!(["a", "b"])))),
                },
                ..Location::default()
            },
        )
    }

    #[test]
    fn test_tuple_items() {
        infer_test(
            &[
                "items: [{enum: [a, 1]}, {enum: [b, 2]}, {enum: [c, 3]}]",
                // Longest sequence is taken on intersection.
                r#"
                allOf:
                - items: [{enum: [a, 1]}, {}, {enum: [c, 3]}]
                - items: [{}, {enum: [b, 2]}]
                "#,
                // Shortest sequence is taken on union (as the longer item is unconstrained).
                r#"
                anyOf:
                - items: [{const: a}, {const: b}, {const: c}, {const: d}]
                - items: [{const: 1}, {const: 2}, {const: 3}]
                "#,
                // Union of tuple with items or additionalItems extends to the longer
                // sequence. However, additionalItems itself is dropped by the union
                // (as items beyond the union'd tuple are now unconstrained).
                r#"
                anyOf:
                - items: [{const: a}, {const: b}, {const: c}]
                - items: [{const: 1}, {const: 2}]
                  additionalItems: {const: 3}
                "#,
            ],
            Location {
                array: ArrayLocation {
                    tuple: vec![
                        enum_fixture(json!([1, "a"])),
                        enum_fixture(json!([2, "b"])),
                        enum_fixture(json!([3, "c"])),
                    ],
                    ..ArrayLocation::default()
                },
                ..Location::default()
            },
        )
    }

    #[test]
    fn test_uneval_items() {
        infer_test(
            &[
                "additionalItems: {const: a}",
                // UnevaluatedItems is ignored if there's already an inferred
                // additional items term (either locally, or via in-place application).
                r#"
                items: {const: a}
                unevaluatedItems: {const: zz}
                "#,
                r#"
                allOf:
                - items: {const: a}
                unevaluatedItems: {const: zz}
                "#,
                r#"
                allOf:
                - additionalItems: {const: a}
                unevaluatedItems: {const: zz}
                "#,
                // If there's no other term, only then is it promoted.
                "unevaluatedItems: {const: a}",
            ],
            Location {
                array: ArrayLocation {
                    additional: Some(Box::new(enum_fixture(json!(["a"])))),
                    ..ArrayLocation::default()
                },
                ..Location::default()
            },
        );
    }

    #[test]
    fn test_uneval_props() {
        infer_test(
            &[
                "additionalProperties: {const: a}",
                // UnevaluatedProperties is ignored if there's already an inferred
                // additional properties term (either locally, or via in-place application).
                r#"
                additionalProperties: {const: a}
                unevaluatedProperties: {const: zz}
                "#,
                r#"
                allOf:
                - additionalProperties: {const: a}
                unevaluatedProperties: {const: zz}
                "#,
                // If there's no other term, only then is it promoted.
                "unevaluatedProperties: {const: a}",
            ],
            Location {
                object: ObjLocation {
                    additional: Some(Box::new(enum_fixture(json!(["a"])))),
                    ..ObjLocation::default()
                },
                ..Location::default()
            },
        );
    }

    #[test]
    fn test_if_then_else() {
        infer_test(
            &[
                "enum: [a, b]",
                // All of if/then/else must be present for it to have an effect.
                r#"
                if: true
                then: {const: zz}
                enum: [a, b]
                "#,
                r#"
                if: true
                else: {const: zz}
                enum: [a, b]
                "#,
                r#"
                then: {const: yy}
                else: {const: zz}
                enum: [a, b]
                "#,
                // If all are present, we intersect the union of the then/else cases.
                r#"
                if: true
                then: {const: a}
                else: {const: b}
                "#,
            ],
            enum_fixture(json!(["a", "b"])),
        );
    }

    #[test]
    fn test_array_bounds() {
        infer_test(
            &[
                "{minItems: 5, maxItems: 10}",
                // Intersections take more restrictive bounds.
                r#"
                allOf:
                - {minItems: 1, maxItems: 10}
                - {minItems: 5, maxItems: 100}
                "#,
                // Unions take least restrictive bounds.
                r#"
                anyOf:
                - {minItems: 7, maxItems: 10}
                - {minItems: 5, maxItems: 7}
                "#,
            ],
            Location {
                array: ArrayLocation {
                    min: Some(5),
                    max: Some(10),
                    ..ArrayLocation::default()
                },
                ..Location::default()
            },
        )
    }

    #[test]
    fn test_additional_items() {
        infer_test(
            &[
                r#"
                items: [{enum: [a, 1]}, {enum: [b, 2]}, {enum: [c, 3]}]
                additionalItems: {enum: [c, 3]}
                "#,
                // On intersection, items in one tuple but not the other are intersected
                // with additionalItems.
                r#"
                allOf:
                - items: [{enum: [a, 1, z]}, {}, {enum: [c, x, y, 3]}]
                - items: [{}, {enum: [b, 2, z]}]
                  additionalItems: {enum: [c, 3, z]}
                - items: {enum: [a, b, c, 1, 2, 3]}
                "#,
                // On union, items in on tuple but not the other are unioned with
                // additionalItems.
                r#"
                anyOf:
                - items: [{const: a}, {const: b}, {const: c}]
                  additionalItems: {enum: [c]}
                - items: [{const: 1}, {const: 2}]
                  additionalItems: {enum: [3]}
                "#,
            ],
            Location {
                array: ArrayLocation {
                    tuple: vec![
                        enum_fixture(json!([1, "a"])),
                        enum_fixture(json!([2, "b"])),
                        enum_fixture(json!([3, "c"])),
                    ],
                    additional: Some(Box::new(enum_fixture(json!([3, "c"])))),
                    ..ArrayLocation::default()
                },
                ..Location::default()
            },
        )
    }

    #[test]
    fn test_object_union() {
        infer_test(
            &[
                r#"
                properties: {foo: {enum: [a, b]}}
                patternProperties: {bar: {enum: [c, d]}}
                additionalProperties: {enum: [1, 2]}
                "#,
                // Union merges by property or pattern.
                r#"
                oneOf:
                - properties: {foo: {const: a}}
                  patternProperties: {bar: {const: c}}
                  additionalProperties: {const: 1}
                - properties: {foo: {const: b}}
                  patternProperties: {bar: {const: d}}
                  additionalProperties: {const: 2}
                "#,
                // Non-matching properties are dropped during a union as they
                // become unrestricted. Note that if they weren't dropped here,
                // we'd see it in the imputed properties of the intersection.
                r#"
                oneOf:
                - properties:
                    foo: {const: a}
                    other1: {const: dropped}
                - properties:
                    foo: {const: b}
                    other2: {const: dropped}
                properties: {foo: {enum: [a, b, dropped]}}
                patternProperties: {bar: {enum: [c, d]}}
                additionalProperties: {enum: [1, 2]}
                "#,
                // Non-matching patterns are dropped as well.
                r#"
                oneOf:
                - patternProperties:
                    bar: {const: c}
                    other1: {const: dropped}
                - patternProperties:
                    bar: {const: d}
                    other2: {const: dropped}
                properties: {foo: {enum: [a, b]}}
                patternProperties: {bar: {enum: [c, d, dropped]}}
                additionalProperties: {enum: [1, 2]}
                "#,
            ],
            Location {
                object: ObjLocation {
                    properties: vec![ObjProperty {
                        name: "foo".to_owned(),
                        is_required: false,
                        value: enum_fixture(json!(["a", "b"])),
                    }],
                    patterns: vec![ObjPattern {
                        re: regex::Regex::new("bar").unwrap(),
                        value: enum_fixture(json!(["c", "d"])),
                    }],
                    additional: Some(Box::new(enum_fixture(json!([1, 2])))),
                },
                ..Location::default()
            },
        )
    }

    #[test]
    fn test_prop_is_required_variations() {
        infer_test(
            &[
                r#"
                properties: {foo: {type: string}}
                required: [foo]
                "#,
                r#"
                allOf:
                - properties: {foo: {type: string}}
                - required: [foo]
                "#,
                r#"
                allOf:
                - properties: {foo: {type: string}}
                required: [foo]
                "#,
                r#"
                allOf:
                - required: [foo]
                properties: {foo: {type: string}}
                "#,
            ],
            Location {
                object: ObjLocation {
                    properties: vec![ObjProperty {
                        name: "foo".to_owned(),
                        is_required: true,
                        value: Location {
                            type_: types::STRING,
                            ..Location::default()
                        },
                    }],
                    ..ObjLocation::default()
                },
                ..Location::default()
            },
        )
    }

    fn infer_test(cases: &[&str], expect: Location) {
        for case in cases {
            let url = url::Url::parse("http://example/schema").unwrap();
            let schema: Value = serde_yaml::from_str(case).unwrap();
            let schema =
                crate::schema::build::build_schema::<CoreAnnotation>(url.clone(), &schema).unwrap();

            let mut index = index::Index::new();
            index.add(&schema).unwrap();
            index.verify_references().unwrap();

            let actual = Location::infer(index.must_fetch(&url).unwrap(), &index);

            assert_eq!(actual, expect);
        }

        // Additional set operation invariants which should be true,
        // no matter what the Location shape is.

        assert_eq!(
            Location::union(expect.clone(), expect.clone()),
            expect,
            "fixture || fixture == fixture"
        );
        assert_eq!(
            Location::union(Location::default(), expect.clone()),
            Location::default(),
            "any || fixture == any"
        );
        assert_eq!(
            Location::union(expect.clone(), Location::default()),
            Location::default(),
            "fixture || any == any"
        );
        assert_eq!(
            Location::intersect(expect.clone(), expect.clone()),
            expect,
            "fixture && fixture == fixture"
        );
        assert_eq!(
            Location::intersect(Location::default(), expect.clone()),
            expect,
            "any && fixture == fixture"
        );
        assert_eq!(
            Location::intersect(expect.clone(), Location::default()),
            expect,
            "fixture && any == fixture"
        );
    }

    fn enum_fixture(value: Value) -> Location {
        let v = value.as_array().unwrap().clone();
        Location {
            type_: enum_types(v.iter()),
            enum_: Some(v.clone()),
            ..Location::default()
        }
    }
}

#[derive(Debug)]
pub struct Inference {
    pub ptr: String,
    pub is_pattern: bool,
    pub type_set: types::Set,
    pub is_base64: bool,
    pub content_type: Option<String>,
    pub format: Option<String>,
}

fn fold<I>(v: Vec<Inference>, it: I) -> Vec<Inference>
where
    I: Iterator<Item = Inference>,
{
    itertools::merge_join_by(v.into_iter(), it, |lhs, rhs| lhs.ptr.cmp(&rhs.ptr))
        .map(|eob| -> Inference {
            match eob {
                EitherOrBoth::Both(lhs, rhs) => Inference {
                    ptr: lhs.ptr,
                    is_pattern: lhs.is_pattern,
                    type_set: lhs.type_set & rhs.type_set,
                    is_base64: lhs.is_base64 || rhs.is_base64,
                    content_type: if lhs.content_type.is_some() {
                        lhs.content_type
                    } else {
                        rhs.content_type
                    },
                    format: if lhs.format.is_some() {
                        lhs.format
                    } else {
                        rhs.format
                    },
                },
                EitherOrBoth::Left(lhs) => lhs,
                EitherOrBoth::Right(rhs) => rhs,
            }
        })
        .collect()
}

fn prefix<I>(pre: String, is_pattern: bool, it: I) -> impl Iterator<Item = Inference>
where
    I: Iterator<Item = Inference>,
{
    it.map(move |i| Inference {
        ptr: pre.chars().chain(i.ptr.chars()).collect(),
        is_pattern: is_pattern || i.is_pattern,
        type_set: i.type_set,
        is_base64: i.is_base64,
        content_type: i.content_type,
        format: i.format,
    })
}

pub fn extract<'s, A>(
    schema: &'s Schema<A>,
    idx: &index::Index<'s, A>,
    location_must_exist: bool,
) -> Result<impl Iterator<Item = Inference>, index::Error>
where
    A: Annotation,
{
    let mut local = Inference {
        ptr: String::new(),
        is_pattern: false,
        type_set: types::ANY,
        is_base64: false,
        content_type: None,
        format: None,
    };

    let mut min_items = 0;
    let mut required_props = 0;

    // Walk validation and annotation keywords which affect the inference result
    // at the current location.
    for kw in &schema.kw {
        match kw {
            Keyword::Validation(Validation::Type(type_set)) => {
                if location_must_exist {
                    local.type_set = *type_set;
                } else {
                    local.type_set = types::NULL | *type_set;
                }
            }
            Keyword::Validation(Validation::MinItems(m)) => {
                min_items = *m; // Track for later use.
            }
            Keyword::Validation(Validation::Required { props_interned, .. }) => {
                required_props = *props_interned; // Track for later use.
            }
            Keyword::Annotation(annot) => match annot.as_core() {
                Some(CoreAnnotation::ContentEncodingBase64) => {
                    local.is_base64 = true;
                }
                Some(CoreAnnotation::ContentMediaType(mt)) => {
                    local.content_type = Some(mt.clone());
                }
                _ => {} // Other CoreAnnotation. No-op.
            },
            _ => {} // Not a CoreAnnotation. No-op.
        }
    }

    let mut out = vec![local];

    // Repeatedly extract and merge inference results from
    // in-place and child applications.

    for kw in &schema.kw {
        let (app, sub) = match kw {
            Keyword::Application(app, sub) => (app, sub),
            _ => continue, // No-op.
        };

        match app {
            Application::Ref(uri) => {
                out = fold(
                    out,
                    extract(idx.must_fetch(uri)?, idx, location_must_exist)?,
                );
            }
            Application::AllOf { .. } => {
                out = fold(out, extract(sub, idx, location_must_exist)?);
            }
            Application::Properties {
                name,
                name_interned,
            } => {
                let prop_must_exist = location_must_exist && (required_props & name_interned) != 0;

                out = fold(
                    out,
                    prefix(
                        format!("/{}", name),
                        false,
                        extract(sub, idx, prop_must_exist)?,
                    ),
                );
            }
            /*
            Application::PatternProperties{re} => {
                // TODO(johnny): This is probably wrong; fix me!
                let mut pat = re.as_str().to_owned();
                if pat.starts_with("^") {
                    pat.drain(0..1);
                } else {
                    pat = format!(r"[^/]*");
                }

                out = fold(out, prefix(
                    format!("/{}", pat),
                    true,
                    extract(sub, idx, false)?));
            }
            */
            Application::Items { index: None } => {
                out = fold(
                    out,
                    prefix(r"/\d+".to_owned(), true, extract(sub, idx, false)?),
                );
            }
            Application::Items { index: Some(index) } => {
                let item_must_exist = location_must_exist && min_items > *index;

                out = fold(
                    out,
                    prefix(
                        format!("/{}", index),
                        false,
                        extract(sub, idx, item_must_exist)?,
                    ),
                );
            }
            _ => continue,
        };
    }

    Ok(out.into_iter())
}
