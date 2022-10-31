use super::{ptr::Token, reduce, Annotation, Pointer, Schema, SchemaIndex};
use fancy_regex::Regex;
use itertools::{self, EitherOrBoth, Itertools};
use json::{
    json_cmp,
    schema::{formats, types, Application, CoreAnnotation, Keyword, Validation},
    LocatedProperty, Location,
};
use serde_json::Value;
use url::Url;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Shape {
    /// Types that this location may take.
    pub type_: types::Set,
    /// Explicit enumeration of allowed values.
    pub enum_: Option<Vec<Value>>,
    /// Annotated `title` of the location.
    pub title: Option<String>,
    /// Annotated `description` of the location.
    pub description: Option<String>,
    /// Location's `reduce` strategy.
    pub reduction: Reduction,
    /// Does this location's schema flow from a `$ref`?
    pub provenance: Provenance,
    /// Default value of this document location, if any.
    pub default: Option<Value>,
    /// Is this location sensitive? For example, a password or credential.
    pub secret: Option<bool>,

    // Further type-specific inferences:
    pub string: StringShape,
    pub array: ArrayShape,
    pub object: ObjShape,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StringShape {
    pub content_encoding: Option<String>,
    pub content_type: Option<String>,
    pub format: Option<formats::Format>,
    pub max_length: Option<usize>,
    pub min_length: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Reduction {
    // Equivalent to Option::None.
    Unset,

    Append,
    FirstWriteWins,
    LastWriteWins,
    Maximize,
    Merge,
    Minimize,
    Set,
    Sum,

    // Multiple concrete strategies may apply at the location.
    Multiple,
}

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

    fn intersect(self, rhs: Self) -> Self {
        if let Self::Unset = self {
            rhs
        } else {
            self
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Provenance {
    // Equivalent to Option::None.
    Unset,
    // Url of another Schema, which this Schema is wholly drawn from.
    Reference(Url),
    // This location has local applications which constrain its Shape.
    Inline,
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

impl From<&reduce::Strategy> for Reduction {
    fn from(s: &reduce::Strategy) -> Self {
        use reduce::Strategy;

        match s {
            Strategy::Append => Reduction::Append,
            Strategy::FirstWriteWins => Reduction::FirstWriteWins,
            Strategy::LastWriteWins => Reduction::LastWriteWins,
            Strategy::Maximize(_) => Reduction::Maximize,
            Strategy::Minimize(_) => Reduction::Minimize,
            Strategy::Set(_) => Reduction::Set,
            Strategy::Sum => Reduction::Sum,
            Strategy::Merge(_) => Reduction::Merge,
        }
    }
}

impl StringShape {
    const fn new() -> Self {
        Self {
            content_encoding: None,
            content_type: None,
            format: None,
            max_length: None,
            min_length: 0,
        }
    }
    fn intersect(lhs: Self, rhs: Self) -> Self {
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

    fn union(lhs: Self, rhs: Self) -> Self {
        let max_length = match (lhs.max_length, rhs.max_length) {
            (Some(l), Some(r)) => Some(l.max(r)),
            _ => None,
        };
        StringShape {
            content_encoding: union_option(lhs.content_encoding, rhs.content_encoding),
            content_type: union_option(lhs.content_type, rhs.content_type),
            format: union_option(lhs.format, rhs.format),
            max_length,
            min_length: lhs.min_length.min(rhs.min_length),
        }
    }
}

impl ObjShape {
    const fn new() -> Self {
        Self {
            properties: Vec::new(),
            patterns: Vec::new(),
            additional: None,
        }
    }

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
                    if !regex_matches(&pattern.re, &prop.name) {
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
            if !regex_matches(&pattern.re, property) {
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
    const fn new() -> Self {
        Self {
            min: None,
            max: None,
            tuple: Vec::new(),
            additional: None,
        }
    }

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

impl Default for ArrayShape {
    fn default() -> Self {
        Self::new()
    }
}
impl Default for StringShape {
    fn default() -> Self {
        Self::new()
    }
}
impl Default for ObjShape {
    fn default() -> Self {
        Self::new()
    }
}
impl Default for Shape {
    fn default() -> Self {
        Self::new()
    }
}

impl Shape {
    const fn new() -> Self {
        Self {
            type_: types::ANY,
            enum_: None,
            title: None,
            description: None,
            reduction: Reduction::Unset,
            provenance: Provenance::Unset,
            default: None,
            secret: None,
            string: StringShape::new(),
            array: ArrayShape::new(),
            object: ObjShape::new(),
        }
    }

    pub fn infer<'s>(schema: &'s Schema, index: &SchemaIndex<'s>) -> Shape {
        let mut visited = Vec::new();
        Self::infer_inner(schema, index, &mut visited)
    }

    fn infer_inner<'s>(
        schema: &'s Schema,
        index: &SchemaIndex<'s>,
        visited: &mut Vec<&'s Url>,
    ) -> Shape {
        // Walk validation and annotation keywords which affect the inference result
        // at the current location.

        let mut shape = Shape::default();
        let mut unevaluated_properties: Option<Shape> = None;
        let mut unevaluated_items: Option<Shape> = None;

        // Does this schema have any keywords which directly affect its validation
        // or annotation result? `$defs` and `definition` are non-operative keywords
        // and have no effect. We would also give a pass to `$id`for the same reason,
        // but it isn't modeled as a schema keyword.
        //
        // We also give a special pass to `title`, `default`, `description`,
        // and `examples`. Technically these are annotation keywords, and
        // change the post-validation annotation result. As a practical matter,
        // though, Provenance is used to guide generation into static types
        // (whether to nest/inline a definition, or reference an external definition),
        // and excluding these keywords works better for this intended use.
        if !schema.kw.iter().all(|kw| {
            matches!(
                kw,
                Keyword::Application(Application::Ref(_), _)
                | Keyword::Application(Application::Def{ .. }, _)
                | Keyword::Application(Application::Definition{ .. }, _)
                | Keyword::Annotation(Annotation::Core(CoreAnnotation::Default(_)))
                | Keyword::Annotation(Annotation::Core(CoreAnnotation::Description(_)))
                | Keyword::Annotation(Annotation::Core(CoreAnnotation::Examples(_)))
                | Keyword::Annotation(Annotation::Core(CoreAnnotation::Title(_)))
                // An in-place application doesn't *by itself* make this an inline
                // schema. However, if the application's target is Provenance::Inline,
                // note that it's applied intersection will promote this Shape to
                // Provenance::Inline as well.
                | Keyword::Application(Application::AllOf { .. }, _)
                | Keyword::Application(Application::AnyOf { .. }, _)
                | Keyword::Application(Application::OneOf { .. }, _)
                | Keyword::Application(Application::If { .. }, _)
                | Keyword::Application(Application::Then { .. }, _)
                | Keyword::Application(Application::Else { .. }, _)
                | Keyword::Application(Application::Not { .. }, _)
            )
        }) {
            shape.provenance = Provenance::Inline;
        }

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
                            .sorted_by(json_cmp)
                            .collect::<Vec<_>>(),
                    );
                }
                Keyword::Validation(Validation::MaxLength(max)) => {
                    shape.string.max_length = Some(*max);
                }
                Keyword::Validation(Validation::MinLength(min)) => {
                    shape.string.min_length = *min;
                }

                Keyword::Annotation(annot) => match annot {
                    Annotation::Reduce(s) => {
                        shape.reduction = s.into();
                    }
                    Annotation::Core(CoreAnnotation::Title(t)) => {
                        shape.title = Some(t.clone());
                    }
                    Annotation::Core(CoreAnnotation::Description(d)) => {
                        shape.description = Some(d.clone());
                    }
                    Annotation::Core(CoreAnnotation::Default(value)) => {
                        shape.default = Some(value.clone());
                    }

                    // String constraints.
                    Annotation::Core(CoreAnnotation::ContentEncoding(enc)) => {
                        shape.string.content_encoding = Some(enc.clone());
                    }
                    Annotation::Core(CoreAnnotation::ContentMediaType(mt)) => {
                        shape.string.content_type = Some(mt.clone());
                    }
                    Annotation::Core(CoreAnnotation::Format(format)) => {
                        shape.string.format = Some(*format);
                    }
                    Annotation::Core(_) => {} // Other CoreAnnotations are no-ops.

                    // These annotations mostly just influence the UI. Most are ignored for now,
                    // but explicitly mentioned so that a compiler error will force us to check
                    // here as new annotations are added.
                    Annotation::Secret(b) => shape.secret = Some(*b),
                    Annotation::Multiline(_) => {}
                    Annotation::Advanced(_) => {}
                    Annotation::Order(_) => {}
                    Annotation::X(_) => {}
                    Annotation::Discriminator(_) => {}
                },

                // Array constraints.
                Keyword::Validation(Validation::MinItems(m)) => shape.array.min = Some(*m),
                Keyword::Validation(Validation::MaxItems(m)) => shape.array.max = Some(*m),
                Keyword::Application(Application::Items { index: None }, schema) => {
                    shape.array.additional =
                        Some(Box::new(Shape::infer_inner(schema, index, visited)));
                }
                Keyword::Application(Application::Items { index: Some(i) }, schema) => {
                    shape.array.tuple.extend(
                        std::iter::repeat(Shape::default()).take(1 + i - shape.array.tuple.len()),
                    );
                    shape.array.tuple[*i] = Shape::infer_inner(schema, index, visited);
                }
                Keyword::Application(Application::AdditionalItems, schema) => {
                    shape.array.additional =
                        Some(Box::new(Shape::infer_inner(schema, index, visited)));
                }
                Keyword::Application(Application::UnevaluatedItems, schema) => {
                    unevaluated_items = Some(Shape::infer_inner(schema, index, visited));
                }

                // Object constraints.
                Keyword::Application(Application::Properties { name, .. }, schema) => {
                    let obj = ObjShape {
                        properties: vec![ObjProperty {
                            name: name.clone(),
                            is_required: false,
                            shape: Shape::infer_inner(schema, index, visited),
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
                            shape: Shape::infer_inner(schema, index, visited),
                        }],
                        additional: None,
                    };
                    shape.object = ObjShape::intersect(shape.object, obj);
                }
                Keyword::Application(Application::AdditionalProperties, schema) => {
                    shape.object.additional =
                        Some(Box::new(Shape::infer_inner(schema, index, visited)));
                }
                Keyword::Application(Application::UnevaluatedProperties, schema) => {
                    unevaluated_properties = Some(Shape::infer_inner(schema, index, visited));
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
                    let mut referent = if visited.iter().any(|u| u.as_str() == uri.as_str()) {
                        Shape::default() // Don't re-visit this location.
                    } else if let Some(schema) = index.fetch(uri) {
                        visited.push(uri);
                        let referent = Shape::infer_inner(schema, index, visited);
                        visited.pop();
                        referent
                    } else {
                        Shape::default()
                    };

                    // Track this |uri| as a reference, unless its resolved shape is itself
                    // a reference to another schema. In other words, promote the bottom-most
                    // $ref within a hierarchy of $ref's.
                    if !matches!(referent.provenance, Provenance::Reference(_)) {
                        referent.provenance = Provenance::Reference(uri.clone());
                    }

                    shape = Shape::intersect(shape, referent);
                }
                Keyword::Application(Application::AllOf { .. } | Application::Inline, schema) => {
                    shape = Shape::intersect(shape, Shape::infer_inner(schema, index, visited));
                }
                Keyword::Application(Application::OneOf { .. }, schema) => {
                    let l = Shape::infer_inner(schema, index, visited);
                    one_of = Some(match one_of {
                        Some(one_of) => Shape::union(one_of, l),
                        None => l,
                    })
                }
                Keyword::Application(Application::AnyOf { .. }, schema) => {
                    let l = Shape::infer_inner(schema, index, visited);
                    any_of = Some(match any_of {
                        Some(any_of) => Shape::union(any_of, l),
                        None => l,
                    })
                }
                Keyword::Application(Application::If, _) => if_ = true,
                Keyword::Application(Application::Then, schema) => {
                    then_ = Some(Shape::infer_inner(schema, index, visited));
                }
                Keyword::Application(Application::Else, schema) => {
                    else_ = Some(Shape::infer_inner(schema, index, visited));
                }
                Keyword::Application(Application::Not, _schema) => {
                    // TODO(johnny): requires implementing difference.
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
        // otherwise unset, then default to unevaluatedProperties / unevaluatedItems.

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
            reduction,
            provenance,
            default,
            secret,
            string,
            array,
            object,
        }
    }

    pub fn intersect(lhs: Self, rhs: Self) -> Self {
        let mut type_ = lhs.type_ & rhs.type_;
        // The enum intersection is additionally filtered to variants matching
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
        let reduction = lhs.reduction.intersect(rhs.reduction);
        let provenance = lhs.provenance.intersect(rhs.provenance);
        let default = lhs.default.or(rhs.default);
        let secret = lhs.secret.or(rhs.secret);

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
            reduction,
            provenance,
            default,
            secret,
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
            let it = itertools::merge_join_by(l.into_iter(), r.into_iter(), json_cmp).filter_map(
                |eob| match eob {
                    EitherOrBoth::Both(l, _) => Some(l),
                    _ => None,
                },
            );
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
        itertools::merge_join_by(lhs.into_iter(), rhs.into_iter(), json_cmp)
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

/// Exists captures an existence constraint of an Shape location.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum Exists {
    /// The location must exist.
    Must,
    /// The location may exist or be undefined.
    /// Its schema has explicit keywords which allow it to exist
    /// and which may constrain its shape, such as additionalProperties,
    /// items, unevaluatedProperties, or unevaluatedItems.
    May,
    /// The location may exist or be undefined.
    /// Its schema omits any associated keywords, but the specification's
    /// default behavior allows the location to exist.
    Implicit,
    /// The location cannot exist. For example, it's outside of permitted
    /// array bounds, or is a disallowed property, or has an impossible type.
    Cannot,
}

impl Exists {
    // Extend a current path with Exists status, with a sub-location
    // having an applied Exists status.
    pub fn extend(&self, child: Self) -> Self {
        match (*self, child) {
            (Exists::Cannot, _) | (_, Exists::Cannot) => Exists::Cannot,
            (Exists::Implicit, _) | (_, Exists::Implicit) => Exists::Implicit,
            (Exists::May, _) | (_, Exists::May) => Exists::May,
            (Exists::Must, Exists::Must) => Exists::Must,
        }
    }
    pub fn must(&self) -> bool {
        matches!(self, Exists::Must)
    }
    pub fn cannot(&self) -> bool {
        matches!(self, Exists::Cannot)
    }
}

impl Shape {
    /// Locate the pointer within this Shape, and return the referenced Shape
    /// along with its Exists status.
    pub fn locate(&self, ptr: &Pointer) -> (&Shape, Exists) {
        let mut shape = self;
        let mut exists = Exists::Must;

        for token in ptr.iter() {
            let (next_shape, next_exists) = shape.locate_token(token);
            shape = next_shape;
            exists = exists.extend(next_exists);
        }

        // A location could be permitted to exist, but have constraints which
        // are impossible to satisfy. Coerce this case to in-existence.
        if shape.type_ == types::INVALID {
            exists = Exists::Cannot
        }

        (shape, exists)
    }

    fn locate_token(&self, token: &Token) -> (&Shape, Exists) {
        match token {
            Token::Index(index) if self.type_.overlaps(types::ARRAY) => {
                let exists = if self.type_ == types::ARRAY && *index < self.array.min.unwrap_or(0) {
                    // A sub-item must exist iff this location can _only_
                    // be an array, and it's within the minItems bound.
                    Exists::Must
                } else if *index >= self.array.max.unwrap_or(std::usize::MAX) {
                    // It cannot exist if outside the maxItems bound.
                    Exists::Cannot
                } else if self.array.max.is_some()
                    || *index < self.array.tuple.len()
                    || self.array.additional.is_some()
                {
                    // It may exist if there is a defined array maximum that we're within,
                    // or we're within the defined array tuple items, or there is an explicit
                    // constraint on additional items.
                    Exists::May
                } else {
                    // Indices outside of defined tuples can still technically
                    // exist, though that's usually not the intention.
                    Exists::Implicit
                };

                if let Some(tuple) = self.array.tuple.get(*index) {
                    (tuple, exists)
                } else if let Some(addl) = &self.array.additional {
                    (addl.as_ref(), exists)
                } else {
                    (&SENTINEL_SHAPE, exists)
                }
            }
            Token::NextIndex if self.type_.overlaps(types::ARRAY) => (
                self.array
                    .additional
                    .as_ref()
                    .map(AsRef::as_ref)
                    .unwrap_or(&SENTINEL_SHAPE),
                Exists::Cannot,
            ),

            Token::Property(property) if self.type_.overlaps(types::OBJECT) => {
                self.obj_property_location(property)
            }

            Token::Index(index) if self.type_.overlaps(types::OBJECT) => {
                self.obj_property_location(&index.to_string())
            }

            Token::NextIndex if self.type_.overlaps(types::OBJECT) => {
                self.obj_property_location("-")
            }

            // Match arms for cases where types don't overlap.
            Token::Index(_) => (&SENTINEL_SHAPE, Exists::Cannot),
            Token::NextIndex => (&SENTINEL_SHAPE, Exists::Cannot),
            Token::Property(_) => (&SENTINEL_SHAPE, Exists::Cannot),
        }
    }

    fn obj_property_location(&self, prop: &str) -> (&Shape, Exists) {
        if let Some(property) = self.object.properties.iter().find(|p| p.name == prop) {
            let exists = if self.type_ == types::OBJECT && property.is_required {
                // A property must exist iff this location can _only_ be an object,
                // and it's marked as a required property.
                Exists::Must
            } else {
                Exists::May
            };

            (&property.shape, exists)
        } else if let Some(pattern) = self
            .object
            .patterns
            .iter()
            .find(|p| regex_matches(&p.re, prop))
        {
            (&pattern.shape, Exists::May)
        } else if let Some(addl) = &self.object.additional {
            (addl.as_ref(), Exists::May)
        } else {
            (&SENTINEL_SHAPE, Exists::Implicit)
        }
    }

    /// Produce flattened locations of nested items and properties of this Shape,
    /// as tuples of the encoded location JSON Pointer, an indication of whether
    /// the pointer is a pattern, its Shape, and an Exists constraint.
    pub fn locations(&self) -> Vec<(String, bool, &Shape, Exists)> {
        let mut out = Vec::new();
        self.locations_inner(Location::Root, Exists::Must, false, &mut out);
        out
    }

    fn locations_inner<'s>(
        &'s self,
        location: Location<'_>,
        exists: Exists,
        pattern: bool,
        out: &mut Vec<(String, bool, &'s Shape, Exists)>,
    ) {
        let exists = if self.type_ == types::INVALID {
            Exists::Cannot
        } else {
            exists
        };
        out.push((location.pointer_str().to_string(), pattern, self, exists));

        // Traverse sub-locations of this location when it takes an object
        // or array type. As a rule, children must exist only if their parent
        // does, the parent can *only* take the applicable type, and it has
        // validations which require that the child exist.
        //
        // Similarly a location is a pattern if *any* parent is a pattern,
        // so |pattern| can only become true and stay true on a path
        // from parent to child.

        for ObjProperty {
            name,
            shape: child,
            is_required,
        } in &self.object.properties
        {
            let exists = if self.type_ == types::OBJECT && *is_required {
                exists.extend(Exists::Must)
            } else {
                exists.extend(Exists::May)
            };

            child.locations_inner(location.push_prop(name), exists, pattern, out);
        }

        for ObjPattern { re, shape: child } in &self.object.patterns {
            child.locations_inner(
                location.push_prop(re.as_str()),
                exists.extend(Exists::May),
                true,
                out,
            );
        }

        if let Some(child) = &self.object.additional {
            child.locations_inner(
                location.push_prop("*"),
                exists.extend(Exists::May),
                true,
                out,
            );
        }

        let ArrayShape {
            tuple,
            additional: array_additional,
            min: array_min,
            ..
        } = &self.array;

        for (index, child) in tuple.into_iter().enumerate() {
            let exists = if self.type_ == types::ARRAY && index < array_min.unwrap_or(0) {
                exists.extend(Exists::Must)
            } else {
                exists.extend(Exists::May)
            };

            child.locations_inner(location.push_item(index), exists, pattern, out);
        }

        if let Some(child) = array_additional {
            child.locations_inner(
                location.push_end_of_array(),
                exists.extend(Exists::May),
                true,
                out,
            );
        };
    }
}

// Sentinel Shape returned by locate(), which make take any value.
static SENTINEL_SHAPE: Shape = Shape::new();

#[derive(thiserror::Error, Debug, Eq, PartialEq)]
pub enum Error {
    #[error("'{0}' must exist, but is constrained to always be invalid")]
    ImpossibleMustExist(String),
    #[error("'{0}' has reduction strategy, but its parent does not")]
    ChildWithoutParentReduction(String),
    #[error("{0} has 'sum' reduction strategy, restricted to numbers, but has types {1:?}")]
    SumNotNumber(String, types::Set),
    #[error(
        "{0} has 'merge' reduction strategy, restricted to objects & arrays, but has types {1:?}"
    )]
    MergeNotObjectOrArray(String, types::Set),
    #[error("{0} has 'set' reduction strategy, restricted to objects, but has types {1:?}")]
    SetNotObject(String, types::Set),
    #[error(
        "{0} location's parent has 'set' reduction strategy, restricted to 'add'/'remove'/'intersect' properties"
    )]
    SetInvalidProperty(String),
}

impl Shape {
    pub fn inspect(&self) -> Vec<Error> {
        let mut v = Vec::new();
        self.inspect_inner(Location::Root, true, &mut v);
        v
    }

    fn inspect_inner(&self, loc: Location, must_exist: bool, out: &mut Vec<Error>) {
        // Enumerations over array sub-locations.
        let items = self.array.tuple.iter().enumerate().map(|(index, s)| {
            (
                loc.push_item(index),
                self.type_ == types::ARRAY && index < self.array.min.unwrap_or(0),
                s,
            )
        });
        let addl_items = self
            .array
            .additional
            .iter()
            .map(|s| (loc.push_end_of_array(), false, s.as_ref()));

        // Enumerations over object sub-locations.
        let props = self.object.properties.iter().map(|op| {
            (
                loc.push_prop(&op.name),
                self.type_ == types::OBJECT && op.is_required,
                &op.shape,
            )
        });
        let patterns = self
            .object
            .patterns
            .iter()
            .map(|op| (loc.push_prop(op.re.as_str()), false, &op.shape));
        let addl_props = self
            .object
            .additional
            .iter()
            .map(|shape| (loc.push_prop("*"), false, shape.as_ref()));

        if self.type_ == types::INVALID && must_exist {
            out.push(Error::ImpossibleMustExist(loc.pointer_str().to_string()));
        }
        if matches!(self.reduction, Reduction::Sum)
            && self.type_ - types::INT_OR_FRAC != types::INVALID
        {
            out.push(Error::SumNotNumber(
                loc.pointer_str().to_string(),
                self.type_,
            ));
        }
        if matches!(self.reduction, Reduction::Merge)
            && self.type_ - (types::OBJECT | types::ARRAY) != types::INVALID
        {
            out.push(Error::MergeNotObjectOrArray(
                loc.pointer_str().to_string(),
                self.type_,
            ));
        }
        if matches!(self.reduction, Reduction::Set) {
            if self.type_ != types::OBJECT {
                out.push(Error::SetNotObject(
                    loc.pointer_str().to_string(),
                    self.type_,
                ));
            }

            for (loc, _, _) in props.clone().chain(patterns.clone()) {
                if !matches!(loc, Location::Property(LocatedProperty { name, .. })
                        if name == "add" || name == "intersect" || name == "remove")
                {
                    out.push(Error::SetInvalidProperty(loc.pointer_str().to_string()));
                }
            }
        }

        for (loc, child_must_exist, child) in items
            .chain(addl_items)
            .chain(props)
            .chain(patterns)
            .chain(addl_props)
        {
            if matches!(self.reduction, Reduction::Unset)
                && !matches!(child.reduction, Reduction::Unset)
            {
                out.push(Error::ChildWithoutParentReduction(
                    loc.pointer_str().to_string(),
                ))
            }

            child.inspect_inner(loc, must_exist && child_must_exist, out);
        }
    }
}

/// Returns true if the text is a match for the given regex. This function exists primarily so we
/// have a common place to put logging, since there's a weird edge case where `is_match` returns an
/// `Err`. This can happen if a regex uses backtracking and overflows the `backtracking_limit` when
/// matching. We _could_ return an error when that happens, but it's not clear what the caller
/// would do with such an error besides consider the document invalid. The logging might be
/// important, though, since some jerk could potentially use this in a DDOS attack.
fn regex_matches(re: &fancy_regex::Regex, text: &str) -> bool {
    re.is_match(text).unwrap_or_else(|err| {
        tracing::warn!("error testing for regex match during inference: {}", err);
        false
    })
}

#[cfg(test)]
mod test {
    use super::{super::Annotation, *};
    use json::schema::{self, index::IndexBuilder};
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
                reduce: {strategy: firstWriteWins}
                contentEncoding: base64
                contentMediaType: some/thing
                default: john.doe@gmail.com
                format: email
                secret: true
                "#,
                // Mix of anyOf, oneOf, & ref.
                r#"
                $defs:
                  aDef:
                    type: [string, array]
                    secret: true
                allOf:
                - title: a-title
                - description: a-description
                - reduce: {strategy: firstWriteWins}
                - default: john.doe@gmail.com
                anyOf:
                - contentEncoding: base64
                - type: object # Elided (impossible).
                oneOf:
                - contentMediaType: some/thing
                - type: 'null' # Elided (impossible).
                $ref: '#/$defs/aDef'
                format: email
                "#,
                // This construction verifies the union and intersection
                // behaviors of all scalar fields.
                // Note that the final schema has _different_ values for all
                // of the tested scalars within the constituent schemas of its `anyOf`.
                // This is effectively a no-op, because the sub-schemas don't
                // uniformly agree on those annotations, and the union of `something`
                // and `anything` is always `anything`.
                r#"
                allOf:
                  - anyOf:
                    - type: string
                    - type: array
                  - anyOf:
                    - title: a-title
                    - title: a-title
                  - anyOf:
                    - description: a-description
                    - description: a-description
                  - anyOf:
                    - reduce: {strategy: firstWriteWins}
                    - reduce: {strategy: firstWriteWins}
                  - anyOf:
                    - contentEncoding: base64
                    - contentEncoding: base64
                  - anyOf:
                    - contentMediaType: some/thing
                    - contentMediaType: some/thing
                  - anyOf:
                    - default: john.doe@gmail.com
                    - default: john.doe@gmail.com
                  - anyOf:
                    - format: email
                    - format: email
                  - anyOf:
                    - secret: true
                    - secret: true
                  - anyOf:
                    - title: other-title
                    - description: other-description
                    - reduce: {strategy: lastWriteWins}
                    - contentEncoding: not-base64
                    - contentMediaType: wrong/thing
                    - default: jane.doe@gmail.com
                    - format: date-time
                    - secret: false
                "#,
            ],
            Shape {
                type_: types::STRING | types::ARRAY,
                title: Some("a-title".to_owned()),
                description: Some("a-description".to_owned()),
                reduction: Reduction::FirstWriteWins,
                provenance: Provenance::Inline,
                default: Some(Value::String("john.doe@gmail.com".to_owned())),
                secret: Some(true),
                string: StringShape {
                    content_encoding: Some("base64".to_owned()),
                    content_type: Some("some/thing".to_owned()),
                    format: Some(formats::Format::Email),
                    max_length: None,
                    min_length: 0,
                },
                ..Shape::default()
            },
        );
    }

    #[test]
    fn test_multiple_reductions() {
        infer_test(
            &[
                r#"
                oneOf:
                - reduce: {strategy: firstWriteWins}
                - reduce: {strategy: firstWriteWins}
                "#,
                r#"
                anyOf:
                - reduce: {strategy: firstWriteWins}
                - reduce: {strategy: firstWriteWins}
                "#,
                r#"
                if: true
                then: {reduce: {strategy: firstWriteWins}}
                else: {reduce: {strategy: firstWriteWins}}
                "#,
            ],
            Shape {
                reduction: Reduction::FirstWriteWins,
                provenance: Provenance::Inline,
                ..Shape::default()
            },
        );
        // Non-equal annotations are promoted to a Multiple variant.
        infer_test(
            &[
                r#"
                oneOf:
                - reduce: {strategy: firstWriteWins}
                - reduce: {strategy: lastWriteWins}
                "#,
                r#"
                anyOf:
                - reduce: {strategy: firstWriteWins}
                - reduce: {strategy: lastWriteWins}
                "#,
                r#"
                if: true
                then: {reduce: {strategy: firstWriteWins}}
                else: {reduce: {strategy: lastWriteWins}}
                "#,
            ],
            Shape {
                reduction: Reduction::Multiple,
                provenance: Provenance::Inline,
                ..Shape::default()
            },
        );
        // All paths must have an annotation, or it becomes unset.
        infer_test(
            &[
                r#"
                oneOf:
                - reduce: {strategy: firstWriteWins}
                - {}
                "#,
                r#"
                anyOf:
                - reduce: {strategy: firstWriteWins}
                - {}
                "#,
                r#"
                if: true
                then: {reduce: {strategy: firstWriteWins}}
                else: {}
                "#,
            ],
            Shape {
                reduction: Reduction::Unset,
                provenance: Provenance::Unset,
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
                provenance: Provenance::Inline,
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
            types::STRING | types::INTEGER
        );
        assert_eq!(
            shape_from("enum: [b, 42.3, a]").type_,
            types::STRING | types::FRACTIONAL
        );
        assert_eq!(
            shape_from("enum: [42.3, {foo: bar}]").type_,
            types::FRACTIONAL | types::OBJECT
        );
        assert_eq!(
            shape_from("enum: [[42], true, null]").type_,
            types::ARRAY | types::BOOLEAN | types::NULL
        );
        assert_eq!(
            shape_from("anyOf: [{const: 42}, {const: fifty}]").type_,
            types::INTEGER | types::STRING
        );
        assert_eq!(
            shape_from("allOf: [{const: 42}, {const: 52}]").type_,
            types::INVALID // Enum intersection is empty.
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
                provenance: Provenance::Inline,
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
                provenance: Provenance::Inline,
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
                        re: fancy_regex::Regex::new("fo.+").unwrap(),
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
                provenance: Provenance::Inline,
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
                provenance: Provenance::Inline,
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
                provenance: Provenance::Inline,
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
                provenance: Provenance::Inline,
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
                provenance: Provenance::Inline,
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
                // On union, items in one tuple but not the other are union-ed with
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
                provenance: Provenance::Inline,
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
                provenance: Provenance::Inline,
                object: ObjShape {
                    properties: vec![ObjProperty {
                        name: "foo".to_owned(),
                        is_required: false,
                        shape: enum_fixture(json!(["a", "b"])),
                    }],
                    patterns: vec![ObjPattern {
                        re: fancy_regex::Regex::new("bar").unwrap(),
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
                provenance: Provenance::Inline,
                object: ObjShape {
                    properties: vec![ObjProperty {
                        name: "foo".to_owned(),
                        is_required: true,
                        shape: Shape {
                            type_: types::STRING,
                            provenance: Provenance::Inline,
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
                    40two: {const: forty-two}
                    impossible:
                        allOf:
                            - {type: integer}
                            - {type: string}
                required: [req-child]
            multi-type:
                type: [object, array]
                properties:
                    child: {const: multi-type-child}
                required: [child]
            1:
                type: object
                properties:
                    -:
                        type: object
                        properties:
                            2: { const: int-prop }
                        required: ["2"]
                required: ["-"]
        required: [parent, "1"]

        patternProperties:
            pattern+: {const: pattern}
        additionalProperties: {const: addl-prop}
        "#,
        );

        let arr1 = shape_from(
            r#"
        type: array
        minItems: 2
        maxItems: 10
        items: [{const: zero}, {const: one}, {const: two}]
        additionalItems: {const: addl-item}
        "#,
        );

        let arr2 = shape_from(
            r#"
        type: array
        items: [{const: "0"}, {const: "1"}]
        "#,
        );

        let cases = &[
            (&obj, "/1/-/2", ("int-prop", Exists::Must)),
            (&obj, "/prop", ("prop", Exists::May)),
            (&obj, "/missing", ("addl-prop", Exists::May)),
            (&obj, "/parent/opt-child", ("opt-child", Exists::May)),
            (&obj, "/parent/req-child", ("req-child", Exists::Must)),
            (&obj, "/parent/missing", ("<missing>", Exists::Implicit)),
            (&obj, "/parent/40two", ("forty-two", Exists::May)),
            (&obj, "/parent/impossible", ("<missing>", Exists::Cannot)),
            (&obj, "/pattern", ("pattern", Exists::May)),
            (&obj, "/patternnnnnn", ("pattern", Exists::May)),
            (&obj, "/123", ("addl-prop", Exists::May)),
            (&obj, "/-", ("addl-prop", Exists::May)),
            (&arr1, "/0", ("zero", Exists::Must)),
            (&arr1, "/1", ("one", Exists::Must)),
            (&arr1, "/2", ("two", Exists::May)),
            (&arr1, "/3", ("addl-item", Exists::May)),
            (&arr1, "/9", ("addl-item", Exists::May)),
            (&arr1, "/10", ("addl-item", Exists::Cannot)),
            (&arr1, "/-", ("addl-item", Exists::Cannot)),
            (&arr2, "/0", ("0", Exists::May)),
            (&arr2, "/1", ("1", Exists::May)),
            (&arr2, "/123", ("<missing>", Exists::Implicit)),
            (&arr2, "/not-an-index", ("<missing>", Exists::Cannot)),
            (&arr2, "/-", ("<missing>", Exists::Cannot)),
        ];

        for (shape, ptr, expect) in cases {
            let actual = shape.locate(&Pointer::from(ptr));
            let actual = (
                actual
                    .0
                    .enum_
                    .as_ref()
                    .map(|i| i[0].as_str().unwrap())
                    .unwrap_or("<missing>"),
                actual.1,
            );
            assert_eq!(expect, &actual, "case {:?}", ptr);
        }

        let obj_locations = obj.locations();
        let obj_locations = obj_locations
            .iter()
            .map(|(ptr, pattern, shape, exists)| (ptr.as_ref(), *pattern, shape.type_, *exists))
            .collect::<Vec<_>>();

        assert_eq!(
            obj_locations,
            vec![
                ("", false, types::OBJECT, Exists::Must),
                ("/1", false, types::OBJECT, Exists::Must),
                ("/1/-", false, types::OBJECT, Exists::Must),
                ("/1/-/2", false, types::STRING, Exists::Must),
                (
                    "/multi-type",
                    false,
                    types::ARRAY | types::OBJECT,
                    Exists::May
                ),
                ("/multi-type/child", false, types::STRING, Exists::May),
                ("/parent", false, types::OBJECT, Exists::Must),
                ("/parent/40two", false, types::STRING, Exists::May),
                ("/parent/impossible", false, types::INVALID, Exists::Cannot),
                ("/parent/opt-child", false, types::STRING, Exists::May),
                ("/parent/req-child", false, types::STRING, Exists::Must),
                ("/prop", false, types::STRING, Exists::May),
                ("/pattern+", true, types::STRING, Exists::May),
                ("/*", true, types::STRING, Exists::May),
            ]
        );

        let arr_locations = arr1.locations();
        let arr_locations = arr_locations
            .iter()
            .map(|(ptr, pattern, shape, exists)| (ptr.as_ref(), *pattern, shape.type_, *exists))
            .collect::<Vec<_>>();

        assert_eq!(
            arr_locations,
            vec![
                ("", false, types::ARRAY, Exists::Must),
                ("/0", false, types::STRING, Exists::Must),
                ("/1", false, types::STRING, Exists::Must),
                ("/2", false, types::STRING, Exists::May),
                ("/-", true, types::STRING, Exists::May),
            ]
        );
    }

    #[test]
    fn test_union_with_impossible_shape() {
        let obj = shape_from(
            r#"
            oneOf:
            - false
            - type: object
              reduce: {strategy: merge}
              title: testTitle
              description: testDescription
              additionalProperties:
                type: integer
                reduce: {strategy: sum}
        "#,
        );

        assert_eq!(obj.inspect(), vec![]);
        assert_eq!("testTitle", obj.title.as_deref().unwrap_or_default());
        assert_eq!(
            "testDescription",
            obj.description.as_deref().unwrap_or_default()
        );
        assert_eq!(Reduction::Merge, obj.reduction);
        assert!(obj.object.additional.is_some());
    }

    #[test]
    fn test_error_collection() {
        let obj = shape_from(
            r#"
        type: object
        reduce: {strategy: merge}
        properties:
            sum-wrong-type:
                reduce: {strategy: sum}
                type: [number, string]

            must-exist-but-cannot: false
            may-not-exist: false

            nested-obj-or-string:
                type: [object, string]
                properties:
                    must-exist-and-cannot-but-parent-could-be-string: false
                required: [must-exist-and-cannot-but-parent-could-be-string]

            nested-array:
                type: array
                items: [true, false, false]
                minItems: 2

            nested-array-or-string:
                oneOf:
                    - $ref: '#/properties/nested-array'
                    - type: string

        patternProperties:
            merge-wrong-type:
                reduce: {strategy: merge}
                type: boolean

        required: [must-exist-but-cannot, nested-obj-or-string, nested-array, nested-array-or-string]

        additionalProperties:
            type: object
            # Valid child, but parent is missing reduce annotation.
            properties:
                nested-sum:
                    reduce: {strategy: sum}
                    type: integer

        items:
            # Set without type restriction.
            - reduce: {strategy: set}
        additionalItems:
            type: object
            properties:
                add: true
                intersect: true
                whoops1: true
            patternProperties:
                remove: true
                whoops2: true
            reduce: {strategy: set}
        "#,
        );

        assert_eq!(
            obj.inspect(),
            vec![
                Error::SetNotObject("/0".to_owned(), types::ANY),
                Error::SetInvalidProperty("/-/whoops1".to_owned()),
                Error::SetInvalidProperty("/-/whoops2".to_owned()),
                Error::ImpossibleMustExist("/must-exist-but-cannot".to_owned()),
                Error::ImpossibleMustExist("/nested-array/1".to_owned()),
                Error::SumNotNumber(
                    "/sum-wrong-type".to_owned(),
                    types::INT_OR_FRAC | types::STRING
                ),
                Error::MergeNotObjectOrArray("/merge-wrong-type".to_owned(), types::BOOLEAN),
                Error::ChildWithoutParentReduction("/*/nested-sum".to_owned()),
            ]
        );
    }

    #[test]
    fn test_provenance_cases() {
        infer_test(
            &[r#"
                # Mix of $defs and definitions.
                $defs:
                    thing: {type: string}
                definitions:
                    in-place: {type: object}

                properties:
                    a-thing:
                        oneOf:
                            - $ref: '#/$defs/thing'
                            - $ref: '#/$defs/thing'
                        title: Just a thing.
                        default: a-default
                    a-thing-plus:
                        $ref: '#/$defs/thing'
                        minLength: 16
                    multi:
                        type: array
                        items:
                            - $ref: '#/properties/multi/items/1'
                            - $ref: '#/properties/multi/items/2'
                            - {type: integer}

                $ref: '#/definitions/in-place'
                "#],
            Shape {
                type_: types::OBJECT,
                provenance: Provenance::Inline,
                object: ObjShape {
                    properties: vec![
                        ObjProperty {
                            name: "a-thing".to_owned(),
                            is_required: false,
                            shape: Shape {
                                type_: types::STRING,
                                title: Some("Just a thing.".to_owned()),
                                provenance: Provenance::Reference(
                                    Url::parse("http://example/schema#/$defs/thing").unwrap(),
                                ),
                                default: Some(json!("a-default")),
                                ..Shape::default()
                            },
                        },
                        ObjProperty {
                            name: "a-thing-plus".to_owned(),
                            is_required: false,
                            shape: Shape {
                                type_: types::STRING,
                                string: StringShape {
                                    min_length: 16,
                                    ..Default::default()
                                },
                                provenance: Provenance::Inline,
                                ..Default::default()
                            },
                        },
                        ObjProperty {
                            name: "multi".to_owned(),
                            is_required: false,
                            shape: Shape {
                                type_: types::ARRAY,
                                provenance: Provenance::Inline,
                                array: ArrayShape {
                                    tuple: vec![
                                        Shape {
                                            type_: types::INTEGER,
                                            provenance: Provenance::Reference(
                                                // Expect the leaf-most reference is preserved in a multi-level hierarchy.
                                                Url::parse("http://example/schema#/properties/multi/items/2").unwrap(),
                                            ),
                                            ..Default::default()
                                        },
                                        Shape {
                                            type_: types::INTEGER,
                                            provenance: Provenance::Reference(
                                                Url::parse("http://example/schema#/properties/multi/items/2").unwrap(),
                                            ),
                                            ..Default::default()
                                        },
                                        Shape {
                                            type_: types::INTEGER,
                                            provenance: Provenance::Inline,
                                            ..Default::default()
                                        },
                                    ],
                                    ..Default::default()
                                },
                                ..Default::default()
                            },
                        },
                    ],
                    ..Default::default()
                },
                ..Default::default()
            },
        )
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

    #[test]
    fn test_recursive() {
        let shape = shape_from(
            r#"
                $defs:
                    foo:
                        properties:
                            a-bar: { $ref: '#/$defs/bar' }
                    bar:
                        properties:
                            a-foo: { $ref: '#/$defs/foo' }
                properties:
                    root-foo: { $ref: '#/$defs/foo' }
                    root-bar: { $ref: '#/$defs/bar' }
                "#,
        );

        let nested_foo = shape.locate(&"/root-foo/a-bar/a-foo".into());
        let nested_bar = shape.locate(&"/root-bar/a-foo/a-bar".into());

        // When we re-encountered `foo` and `bar`, expect we tracked their provenance
        // but didn't recurse further to apply their Shape contributions.
        assert_eq!(
            nested_foo.0,
            &Shape {
                provenance: Provenance::Reference(
                    Url::parse("http://example/schema#/$defs/foo").unwrap()
                ),
                ..Default::default()
            }
        );
        assert_eq!(
            nested_bar.0,
            &Shape {
                provenance: Provenance::Reference(
                    Url::parse("http://example/schema#/$defs/bar").unwrap()
                ),
                ..Default::default()
            }
        );
    }

    #[test]
    fn test_inline_required_is_transparent() {
        let fill_to = json::schema::intern::MAX_TABLE_SIZE + 7;
        let required: Vec<_> = (0..fill_to).map(|i| i.to_string()).collect();

        let shape = shape_from(
            &json!({
                "required": required,
                "properties": {
                    "9": {"const": "value"} // Overlaps with `required`.
                }
            })
            .to_string(),
        );
        assert_eq!(shape.object.properties.len(), fill_to);
        assert!(shape.object.properties.iter().all(|p| p.is_required));
    }

    fn shape_from(case: &str) -> Shape {
        let url = url::Url::parse("http://example/schema").unwrap();
        let schema: Value = serde_yaml::from_str(case).unwrap();
        let schema = schema::build::build_schema::<Annotation>(url.clone(), &schema).unwrap();

        let mut index = IndexBuilder::new();
        index.add(&schema).unwrap();
        index.verify_references().unwrap();
        let index = index.into_index();

        Shape::infer(index.must_fetch(&url).unwrap(), &index)
    }

    fn enum_fixture(value: Value) -> Shape {
        let v = value.as_array().unwrap().clone();
        Shape {
            type_: enum_types(v.iter()),
            enum_: Some(v.clone()),
            provenance: Provenance::Inline,
            ..Shape::default()
        }
    }
}
