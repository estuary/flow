use super::{ptr::Token, Pointer, Schema, SchemaIndex};
use estuary_json::schema::{
    types, Annotation as AnnotationTrait, Application, CoreAnnotation, Keyword, Validation,
};
use itertools::{self, EitherOrBoth, Itertools};
use regex::Regex;
use serde_json::Value;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Shape {
    pub type_: types::Set,
    pub enum_: Option<Vec<Value>>,
    pub title: Option<String>,
    pub description: Option<String>,

    pub string: StringShape,
    pub array: ArrayShape,
    pub object: ObjShape,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StringShape {
    pub is_base64: Option<bool>,
    pub content_type: Option<String>,
    pub format: Option<String>,
    pub max_length: Option<usize>,
    pub min_length: usize,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ArrayShape {
    pub min: Option<usize>,
    pub max: Option<usize>,
    pub tuple: Vec<Shape>,
    pub additional: Option<Box<Shape>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjShape {
    pub properties: Vec<ObjProperty>,
    pub patterns: Vec<ObjPattern>,
    pub additional: Option<Box<Shape>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjProperty {
    pub name: String,
    pub is_required: bool,
    pub shape: Shape,
}

#[derive(Clone, Debug)]
pub struct ObjPattern {
    pub re: Regex,
    pub shape: Shape,
}

impl Eq for ObjPattern {}

impl PartialEq for ObjPattern {
    fn eq(&self, other: &Self) -> bool {
        self.re.as_str() == other.re.as_str() && self.shape == other.shape
    }
}

impl StringShape {
    fn intersect(lhs: Self, rhs: Self) -> Self {
        let max_length = match (lhs.max_length, rhs.max_length) {
            (Some(l), Some(r)) => Some(l.min(r)),
            (Some(l), None) => Some(l),
            (None, Some(r)) => Some(r),
            _ => None,
        };
        StringShape {
            is_base64: lhs.is_base64.or(rhs.is_base64),
            content_type: lhs.content_type.or(rhs.content_type),
            format: lhs.format.or(rhs.format),
            min_length: lhs.min_length.max(rhs.min_length),
            max_length,
        }
    }

    fn union(lhs: Self, rhs: Self) -> Self {
        let max_length = match (lhs.max_length, rhs.max_length) {
            (Some(l), Some(r)) => Some(l.max(r)),
            _ => None,
        };
        StringShape {
            is_base64: union_option(lhs.is_base64, rhs.is_base64),
            content_type: union_option(lhs.content_type, rhs.content_type),
            format: union_option(lhs.format, rhs.format),
            max_length,
            min_length: lhs.min_length.min(rhs.min_length),
        }
    }
}

impl ObjShape {
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
        let intersect_imputed = |mut side: ObjProperty, other: Option<Shape>| {
            if let Some(other) = other {
                side.shape = Shape::intersect(side.shape, other);
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
                shape: Shape::intersect(l.shape, r.shape),
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
                shape: Shape::intersect(l.shape, r.shape),
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
                shape: Shape::union(l.shape, r.shape),
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

    fn apply_patterns_to_properties(self) -> Self {
        let ObjShape {
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
                    prop.shape = Shape::intersect(prop.shape, pattern.shape.clone());
                }
                prop
            })
            .collect::<Vec<_>>();

        ObjShape {
            patterns,
            properties,
            additional,
        }
    }

    fn impute(
        property: &str,
        patterns: &[ObjPattern],
        additional: Option<&Shape>,
    ) -> Option<Shape> {
        // Compute the intersection of all matching property patterns.
        let pattern = patterns.iter().fold(None, |prior, pattern| {
            if !pattern.re.is_match(property) {
                prior
            } else if let Some(prior) = prior {
                Some(Shape::intersect(prior, pattern.shape.clone()))
            } else {
                Some(pattern.shape.clone())
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

impl ArrayShape {
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

        let additional = intersect_additional(lhs_addl, rhs_addl);

        Self {
            min,
            max,
            tuple,
            additional,
        }
    }
}

impl Default for ObjShape {
    fn default() -> Self {
        Self {
            properties: Vec::new(),
            patterns: Vec::new(),
            additional: None,
        }
    }
}

impl Default for Shape {
    fn default() -> Self {
        Self {
            type_: types::ANY,
            enum_: None,
            title: None,
            description: None,
            string: StringShape::default(),
            array: ArrayShape::default(),
            object: ObjShape::default(),
        }
    }
}

impl Shape {
    pub fn infer<'s>(schema: &Schema, index: &SchemaIndex<'s>) -> Shape {
        // Walk validation and annotation keywords which affect the inference result
        // at the current location.

        let mut shape = Shape::default();
        let mut unevaluated_properties: Option<Shape> = None;
        let mut unevaluated_items: Option<Shape> = None;

        // Walk validation keywords and subordinate applications which influence
        // the present Location.
        for kw in &schema.kw {
            match kw {
                // Type constraints.
                Keyword::Validation(Validation::False) => shape.type_ = types::INVALID,
                Keyword::Validation(Validation::Type(type_set)) => shape.type_ = *type_set,

                // Enum constraints.
                Keyword::Validation(Validation::Const(literal)) => {
                    shape.enum_ = Some(vec![literal.value.clone()])
                }
                Keyword::Validation(Validation::Enum { variants }) => {
                    shape.enum_ = Some(
                        variants
                            .iter()
                            .map(|hl| hl.value.clone())
                            .sorted_by(estuary_json::json_cmp)
                            .collect::<Vec<_>>(),
                    );
                }
                Keyword::Validation(Validation::MaxLength(max)) => {
                    shape.string.max_length = Some(*max);
                }
                Keyword::Validation(Validation::MinLength(min)) => {
                    shape.string.min_length = *min;
                }

                Keyword::Annotation(annot) => match annot.as_core() {
                    Some(CoreAnnotation::Title(t)) => {
                        shape.title = Some(t.clone());
                    }
                    Some(CoreAnnotation::Description(d)) => {
                        shape.description = Some(d.clone());
                    }

                    // String constraints.
                    Some(CoreAnnotation::ContentEncodingBase64) => {
                        shape.string.is_base64 = Some(true);
                    }
                    Some(CoreAnnotation::ContentMediaType(mt)) => {
                        shape.string.content_type = Some(mt.clone());
                    }
                    Some(CoreAnnotation::Format(format)) => {
                        shape.string.format = Some(format.clone());
                    }
                    _ => {} // Other CoreAnnotation. No-op.
                },

                // Array constraints.
                Keyword::Validation(Validation::MinItems(m)) => shape.array.min = Some(*m),
                Keyword::Validation(Validation::MaxItems(m)) => shape.array.max = Some(*m),
                Keyword::Application(Application::Items { index: None }, schema) => {
                    shape.array.additional = Some(Box::new(Shape::infer(schema, index)));
                }
                Keyword::Application(Application::Items { index: Some(i) }, schema) => {
                    shape.array.tuple.extend(
                        std::iter::repeat(Shape::default()).take(1 + i - shape.array.tuple.len()),
                    );
                    shape.array.tuple[*i] = Shape::infer(schema, index);
                }
                Keyword::Application(Application::AdditionalItems, schema) => {
                    shape.array.additional = Some(Box::new(Shape::infer(schema, index)));
                }
                Keyword::Application(Application::UnevaluatedItems, schema) => {
                    unevaluated_items = Some(Shape::infer(schema, index));
                }

                // Object constraints.
                Keyword::Application(Application::Properties { name, .. }, schema) => {
                    let obj = ObjShape {
                        properties: vec![ObjProperty {
                            name: name.clone(),
                            is_required: false,
                            shape: Shape::infer(schema, index),
                        }],
                        patterns: Vec::new(),
                        additional: None,
                    };
                    shape.object = ObjShape::intersect(shape.object, obj);
                }
                Keyword::Validation(Validation::Required { props, .. }) => {
                    let obj = ObjShape {
                        properties: props
                            .iter()
                            .sorted()
                            .map(|p| ObjProperty {
                                name: p.clone(),
                                is_required: true,
                                shape: Shape::default(),
                            })
                            .collect::<Vec<_>>(),
                        patterns: Vec::new(),
                        additional: None,
                    };
                    shape.object = ObjShape::intersect(shape.object, obj);
                }

                Keyword::Application(Application::PatternProperties { re }, schema) => {
                    let obj = ObjShape {
                        properties: Vec::new(),
                        patterns: vec![ObjPattern {
                            re: re.clone(),
                            shape: Shape::infer(schema, index),
                        }],
                        additional: None,
                    };
                    shape.object = ObjShape::intersect(shape.object, obj);
                }
                Keyword::Application(Application::AdditionalProperties, schema) => {
                    shape.object.additional = Some(Box::new(Shape::infer(schema, index)));
                }
                Keyword::Application(Application::UnevaluatedProperties, schema) => {
                    unevaluated_properties = Some(Shape::infer(schema, index));
                }

                _ => {} // Other Keyword. No-op.
            }
        }

        // Apply pattern properties to applicable named properties.
        shape.object = shape.object.apply_patterns_to_properties();

        // Restrict enum variants to permitted types of the present schema.
        // We'll keep enforcing this invariant as Locations are intersected,
        // and allowed types are further restricted.
        shape.enum_ = intersect_enum(shape.type_, shape.enum_.take(), None);

        // Presence of an enum term similarly restricts the allowed types that
        // a location may take (since it may only take values of the enum).
        // We also check this again during intersection.
        if let Some(enum_) = &shape.enum_ {
            shape.type_ = shape.type_ & enum_types(enum_.iter());
        }

        // Now, collect inferences from in-place application keywords.
        let mut one_of: Option<Shape> = None;
        let mut any_of: Option<Shape> = None;
        let mut if_ = false;
        let mut then_: Option<Shape> = None;
        let mut else_: Option<Shape> = None;

        for kw in &schema.kw {
            match kw {
                Keyword::Application(Application::Ref(uri), _) => {
                    if let Some(schema) = index.fetch(uri) {
                        shape = Shape::intersect(shape, Shape::infer(schema, index));
                    }
                }
                Keyword::Application(Application::AllOf { .. }, schema) => {
                    shape = Shape::intersect(shape, Shape::infer(schema, index));
                }
                Keyword::Application(Application::OneOf { .. }, schema) => {
                    let l = Shape::infer(schema, index);
                    one_of = Some(match one_of {
                        Some(one_of) => Shape::union(one_of, l),
                        None => l,
                    })
                }
                Keyword::Application(Application::AnyOf { .. }, schema) => {
                    let l = Shape::infer(schema, index);
                    any_of = Some(match any_of {
                        Some(any_of) => Shape::union(any_of, l),
                        None => l,
                    })
                }
                Keyword::Application(Application::If, _) => if_ = true,
                Keyword::Application(Application::Then, schema) => {
                    then_ = Some(Shape::infer(schema, index));
                }
                Keyword::Application(Application::Else, schema) => {
                    else_ = Some(Shape::infer(schema, index));
                }

                _ => {} // Other Keyword. No-op.
            }
        }

        if let Some(one_of) = one_of {
            shape = Shape::intersect(shape, one_of);
        }
        if let Some(any_of) = any_of {
            shape = Shape::intersect(shape, any_of);
        }
        if let (true, Some(then_), Some(else_)) = (if_, then_, else_) {
            let then_else = Shape::union(then_, else_);
            shape = Shape::intersect(shape, then_else);
        }

        // Now, and *only* if loc.object.additional or loc.array.additional is
        // otherwise unset, then default to unevalutedProperties / unevaluatedItems.

        if let (None, Some(unevaluated_properties)) =
            (&shape.object.additional, unevaluated_properties)
        {
            shape.object.additional = Some(Box::new(unevaluated_properties));
        }
        if let (None, Some(unevaluated_items)) = (&shape.array.additional, unevaluated_items) {
            shape.array.additional = Some(Box::new(unevaluated_items));
        }

        shape
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
            (true, true) => StringShape::intersect(lhs.string, rhs.string),
            (_, _) => StringShape::default(),
        };
        let array = match (
            lhs.type_.overlaps(types::ARRAY),
            rhs.type_.overlaps(types::ARRAY),
        ) {
            (true, true) => ArrayShape::intersect(lhs.array, rhs.array),
            (_, _) => ArrayShape::default(),
        };
        let object = match (
            lhs.type_.overlaps(types::OBJECT),
            rhs.type_.overlaps(types::OBJECT),
        ) {
            (true, true) => ObjShape::intersect(lhs.object, rhs.object),
            (_, _) => ObjShape::default(),
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
    it.filter(move |val| type_.overlaps(types::Set::for_value(val)))
}

fn enum_types<'v, I: Iterator<Item = &'v Value>>(it: I) -> types::Set {
    it.fold(types::INVALID, |_type, val| {
        types::Set::for_value(val) | _type
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
            let it = itertools::merge_join_by(l.into_iter(), r.into_iter(), estuary_json::json_cmp)
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
        itertools::merge_join_by(lhs.into_iter(), rhs.into_iter(), estuary_json::json_cmp)
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

fn union_additional(lhs: Option<Box<Shape>>, rhs: Option<Box<Shape>>) -> Option<Box<Shape>> {
    match (lhs, rhs) {
        (Some(lhs), Some(rhs)) => Some(Box::new(Shape::union(
            lhs.as_ref().clone(),
            rhs.as_ref().clone(),
        ))),
        _ => None, // If either side is unrestricted, the union is unrestricted.
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

impl Shape {
    /// Locate the pointer within this Shape. Returns None if the pointed
    /// Shape (or a parent thereof) does not exist.
    pub fn locate(&self, ptr: &Pointer) -> Option<(&Shape, bool)> {
        let mut shape = self;
        let mut must_exist = true;

        for token in ptr.iter() {
            if let Some((next, exists)) = shape.locate_token(token) {
                shape = next;
                must_exist &= exists;
            } else {
                return None;
            }
        }
        Some((shape, must_exist))
    }

    fn locate_token(&self, token: Token) -> Option<(&Shape, bool)> {
        // If this Shape can take a type other than ARRAY or OBJECT,
        // then even if we match this token there's no guarantee that
        // the token must exist (since this location could be another
        // scalar type).
        let mut _ind_string = String::new();

        // First try to resolve a Token::Index to an array location.
        let prop = if let Token::Index(ind) = token {
            if self.type_.overlaps(types::ARRAY) {
                // A sub-item must exist iff this location can _only_
                // be an array, and it's within the minItems bound.
                let must_exist = self.type_ == types::ARRAY && ind < self.array.min.unwrap_or(ind);

                return if ind >= self.array.max.unwrap_or(ind + 1) {
                    None // If outside of the maxItems bound, we can't exist.
                } else if self.array.tuple.len() > ind {
                    Some((&self.array.tuple[ind], must_exist))
                } else if let Some(addl) = &self.array.additional {
                    Some((addl.as_ref(), must_exist))
                } else {
                    None
                };
            } else {
                // We have a Token::Index, but the present location can never be
                // an array. Re-interpret as a property having a string-ized
                // index as property name, and try to resolve that.
                _ind_string = ind.to_string();
                &_ind_string
            }
        } else if let Token::Property(prop) = token {
            prop
        } else {
            return None;
        };

        // Next try to resolve |prop| to an object location.
        if !self.type_.overlaps(types::OBJECT) {
            return None;
        }

        if let Some(f) = self.object.properties.iter().find(|p| p.name == prop) {
            // A property must exist iff this location can _only_ be an object,
            // and it's marked as a required property.
            Some((&f.shape, self.type_ == types::OBJECT && f.is_required))
        } else if let Some(f) = self.object.patterns.iter().find(|p| p.re.is_match(prop)) {
            Some((&f.shape, false))
        } else if let Some(addl) = &self.object.additional {
            Some((addl.as_ref(), false))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod test {
    use super::{super::Annotation, *};
    use estuary_json::schema;
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
            Shape {
                type_: types::STRING | types::ARRAY,
                title: Some("a-title".to_owned()),
                description: Some("a-description".to_owned()),
                string: StringShape {
                    is_base64: Some(true),
                    content_type: Some("some/thing".to_owned()),
                    format: Some("email".to_owned()),
                    max_length: None,
                    min_length: 0,
                },
                ..Shape::default()
            },
        );
    }

    #[test]
    fn test_string_length() {
        infer_test(
            &[
                "{type: string, minLength: 3, maxLength: 33}",
                "{oneOf: [
                  {type: string, minLength: 19, maxLength: 20},
                  {type: string, minLength: 3, maxLength: 20},
                  {type: string, minLength: 20, maxLength: 33}
                ]}",
                "{allOf: [
                  {type: string, maxLength: 60},
                  {type: string, minLength: 3, maxLength: 78},
                  {type: string, minLength: 2, maxLength: 33}
                ]}",
            ],
            Shape {
                type_: types::STRING,
                string: StringShape {
                    min_length: 3,
                    max_length: Some(33),
                    ..Default::default()
                },
                ..Default::default()
            },
        );
    }

    #[test]
    fn test_enum_type_extraction() {
        assert_eq!(
            shape_from("enum: [b, 42, a]").type_,
            types::STRING | types::NUMBER | types::INTEGER
        );
        assert_eq!(
            shape_from("enum: [b, 42.3, a]").type_,
            types::STRING | types::NUMBER
        );
        assert_eq!(
            shape_from("enum: [42.3, {foo: bar}]").type_,
            types::NUMBER | types::OBJECT
        );
        assert_eq!(
            shape_from("enum: [[42], true, null]").type_,
            types::ARRAY | types::BOOLEAN | types::NULL
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
            Shape {
                type_: types::STRING,
                enum_: Some(vec![json!("a"), json!("b")]),
                ..Shape::default()
            },
        );
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
            Shape {
                object: ObjShape {
                    properties: vec![
                        ObjProperty {
                            name: "bar".to_owned(),
                            is_required: true,
                            shape: enum_fixture(json!(["c"])),
                        },
                        ObjProperty {
                            name: "foo".to_owned(),
                            is_required: false,
                            shape: enum_fixture(json!(["b"])),
                        },
                    ],
                    patterns: vec![ObjPattern {
                        re: regex::Regex::new("fo.+").unwrap(),
                        shape: enum_fixture(json!(["b"])),
                    }],
                    additional: None,
                },
                ..Shape::default()
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
            Shape {
                object: ObjShape {
                    properties: vec![ObjProperty {
                        name: "foo".to_owned(),
                        is_required: false,
                        shape: enum_fixture(json!(["a", "b"])),
                    }],
                    patterns: Vec::new(),
                    additional: Some(Box::new(enum_fixture(json!(["a", "b"])))),
                },
                ..Shape::default()
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
            Shape {
                array: ArrayShape {
                    tuple: vec![
                        enum_fixture(json!([1, "a"])),
                        enum_fixture(json!([2, "b"])),
                        enum_fixture(json!([3, "c"])),
                    ],
                    ..ArrayShape::default()
                },
                ..Shape::default()
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
            Shape {
                array: ArrayShape {
                    additional: Some(Box::new(enum_fixture(json!(["a"])))),
                    ..ArrayShape::default()
                },
                ..Shape::default()
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
            Shape {
                object: ObjShape {
                    additional: Some(Box::new(enum_fixture(json!(["a"])))),
                    ..ObjShape::default()
                },
                ..Shape::default()
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
            Shape {
                array: ArrayShape {
                    min: Some(5),
                    max: Some(10),
                    ..ArrayShape::default()
                },
                ..Shape::default()
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
            Shape {
                array: ArrayShape {
                    tuple: vec![
                        enum_fixture(json!([1, "a"])),
                        enum_fixture(json!([2, "b"])),
                        enum_fixture(json!([3, "c"])),
                    ],
                    additional: Some(Box::new(enum_fixture(json!([3, "c"])))),
                    ..ArrayShape::default()
                },
                ..Shape::default()
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
            Shape {
                object: ObjShape {
                    properties: vec![ObjProperty {
                        name: "foo".to_owned(),
                        is_required: false,
                        shape: enum_fixture(json!(["a", "b"])),
                    }],
                    patterns: vec![ObjPattern {
                        re: regex::Regex::new("bar").unwrap(),
                        shape: enum_fixture(json!(["c", "d"])),
                    }],
                    additional: Some(Box::new(enum_fixture(json!([1, 2])))),
                },
                ..Shape::default()
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
            Shape {
                object: ObjShape {
                    properties: vec![ObjProperty {
                        name: "foo".to_owned(),
                        is_required: true,
                        shape: Shape {
                            type_: types::STRING,
                            ..Shape::default()
                        },
                    }],
                    ..ObjShape::default()
                },
                ..Shape::default()
            },
        )
    }

    #[test]
    fn test_locate() {
        let obj = shape_from(
            r#"
        type: object
        properties:
            prop: {const: prop}
            parent:
                type: object
                properties:
                    opt-child: {const: opt-child}
                    req-child: {const: req-child}
                    42: {const: forty-two}
                required: [req-child]
            multi-type:
                type: [object, array]
                properties:
                    child: {const: multi-type-child}
                required: [child]
        required: [parent]

        patternProperties:
            pattern+: {const: pattern}
        additionalProperties: {const: addl-prop}
        "#,
        );

        let arr = shape_from(
            r#"
        type: array
        minItems: 2
        maxItems: 10
        items: [{const: zero}, {const: one}, {const: two}]
        additionalItems: {const: addl-item}
        "#,
        );

        let cases = &[
            (&obj, "/prop", Some(("prop", false))),
            (&obj, "/missing", Some(("addl-prop", false))),
            (&obj, "/parent/opt-child", Some(("opt-child", false))),
            (&obj, "/parent/req-child", Some(("req-child", true))),
            (&obj, "/parent/missing", None),
            (&obj, "/parent/42", Some(("forty-two", false))),
            (&obj, "/pattern", Some(("pattern", false))),
            (&obj, "/patternnnnnn", Some(("pattern", false))),
            (&arr, "/0", Some(("zero", true))),
            (&arr, "/1", Some(("one", true))),
            (&arr, "/2", Some(("two", false))),
            (&arr, "/3", Some(("addl-item", false))),
            (&arr, "/9", Some(("addl-item", false))),
            (&arr, "/10", None),
        ];

        for (shape, ptr, expect) in cases {
            let actual = shape.locate(&Pointer::from(ptr));
            let actual = actual.map(|(shape, exists)| {
                (
                    shape
                        .enum_
                        .as_ref()
                        .unwrap()
                        .first()
                        .unwrap()
                        .as_str()
                        .unwrap(),
                    exists,
                )
            });
            assert_eq!(expect, &actual, "case {:?}", ptr);
        }
    }

    fn infer_test(cases: &[&str], expect: Shape) {
        for case in cases {
            let actual = shape_from(case);
            assert_eq!(actual, expect);
        }

        // Additional set operation invariants which should be true,
        // no matter what the Location shape is.

        assert_eq!(
            Shape::union(expect.clone(), expect.clone()),
            expect,
            "fixture || fixture == fixture"
        );
        assert_eq!(
            Shape::union(Shape::default(), expect.clone()),
            Shape::default(),
            "any || fixture == any"
        );
        assert_eq!(
            Shape::union(expect.clone(), Shape::default()),
            Shape::default(),
            "fixture || any == any"
        );
        assert_eq!(
            Shape::intersect(expect.clone(), expect.clone()),
            expect,
            "fixture && fixture == fixture"
        );
        assert_eq!(
            Shape::intersect(Shape::default(), expect.clone()),
            expect,
            "any && fixture == fixture"
        );
        assert_eq!(
            Shape::intersect(expect.clone(), Shape::default()),
            expect,
            "fixture && any == fixture"
        );
    }

    /*
     * Shape::infer does not currently handle recursive schemas, and will overflow the stack.
     * Eventually, we'll want to have some sane handling of recursive schemas, but we'll have to
     * figure out what that should be.
    #[test]
    fn test_recursive() {
        let shape = shape_from(
            r##"
               type: object
               properties:
                 val: { type: string }
                 a: { $ref: http://example/schema }
                 b: { $ref: http://example/schema }
               "##,
        );
        let pointer = "/a/b/a/a/b/a/val".into();
        let result = shape.locate(&pointer);
    }
    */

    fn shape_from(case: &str) -> Shape {
        let url = url::Url::parse("http://example/schema").unwrap();
        let schema: Value = serde_yaml::from_str(case).unwrap();
        let schema = schema::build::build_schema::<Annotation>(url.clone(), &schema).unwrap();

        let mut index = SchemaIndex::new();
        index.add(&schema).unwrap();
        index.verify_references().unwrap();

        Shape::infer(index.must_fetch(&url).unwrap(), &index)
    }

    fn enum_fixture(value: Value) -> Shape {
        let v = value.as_array().unwrap().clone();
        Shape {
            type_: enum_types(v.iter()),
            enum_: Some(v.clone()),
            ..Shape::default()
        }
    }
}
