/// This module is concerned with mapping JSON Schemas into a most-equivalent Shape.
/// It builds on the union and intersection operations defined over Shape.
use super::*;
use crate::{reduce, Annotation, Schema, SchemaIndex};
use itertools::Itertools;
use json::schema::{Application, CoreAnnotation, Keyword, Validation};

impl From<&reduce::Strategy> for Reduction {
    fn from(s: &reduce::Strategy) -> Self {
        Self::Strategy(s.clone())
    }
}

impl ObjShape {
    fn apply_patterns_to_properties(self) -> Self {
        let ObjShape {
            pattern_properties: patterns,
            mut properties,
            additional_properties,
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
            pattern_properties: patterns,
            properties,
            additional_properties,
        }
    }
}

impl Shape {
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

        let mut shape = Shape::anything();
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
                            .sorted_by(crate::compare)
                            .collect::<Vec<_>>(),
                    );
                }

                // String constraints.
                Keyword::Validation(Validation::MaxLength(max)) => {
                    shape.string.max_length = Some(*max as u32);
                }
                Keyword::Validation(Validation::MinLength(min)) => {
                    shape.string.min_length = *min as u32;
                }

                // Numeric constraints.
                Keyword::Validation(Validation::Minimum(min)) => {
                    shape.numeric.minimum = Some(*min);
                }
                Keyword::Validation(Validation::Maximum(max)) => {
                    shape.numeric.maximum = Some(*max);
                }

                Keyword::Annotation(annot) => match annot {
                    Annotation::Reduce(s) => {
                        shape.reduction = s.into();
                    }
                    Annotation::Core(CoreAnnotation::Title(t)) => {
                        shape.title = Some(t.as_str().into());
                    }
                    Annotation::Core(CoreAnnotation::Description(d)) => {
                        shape.description = Some(d.as_str().into());
                    }
                    Annotation::Core(CoreAnnotation::Default(value)) => {
                        let default_value = value.clone();

                        let validation_err = crate::Validation::validate(
                            &mut crate::RawValidator::new(index),
                            &schema.curi,
                            &default_value,
                        )
                        .unwrap()
                        .ok()
                        .err();

                        shape.default = Some(Box::new((default_value, validation_err)));
                    }

                    // More string constraints (annotations).
                    Annotation::Core(CoreAnnotation::ContentEncoding(enc)) => {
                        shape.string.content_encoding = Some(enc.as_str().into());
                    }
                    Annotation::Core(CoreAnnotation::ContentMediaType(mt)) => {
                        shape.string.content_type = Some(mt.as_str().into());
                    }
                    Annotation::Core(CoreAnnotation::Format(format)) => {
                        shape.string.format = Some(*format);
                    }
                    Annotation::Core(_) => {} // Other CoreAnnotations are no-ops.

                    // Collect "X-" extended annotations.
                    Annotation::X(key, value) => {
                        shape.annotations.insert(key.clone(), value.clone());
                    }

                    // These annotations mostly just influence the UI. Most are ignored for now,
                    // but explicitly mentioned so that a compiler error will force us to check
                    // here as new annotations are added.
                    Annotation::Secret(b) => shape.secret = Some(*b),
                    Annotation::Multiline(_) => {}
                    Annotation::Advanced(_) => {}
                    Annotation::Order(_) => {}
                    Annotation::Discriminator(_) => {}
                    Annotation::Transform(_) => {} // Transform annotations are handled during combine
                },

                // Array constraints.
                Keyword::Validation(Validation::MinItems(m)) => shape.array.min_items = *m as u32,
                Keyword::Validation(Validation::MaxItems(m)) => {
                    shape.array.max_items = Some(*m as u32)
                }
                Keyword::Application(Application::Items { index: None }, schema) => {
                    shape.array.additional_items =
                        Some(Box::new(Shape::infer_inner(schema, index, visited)));
                }
                Keyword::Application(Application::Items { index: Some(i) }, schema) => {
                    shape.array.tuple.extend(
                        std::iter::repeat(Shape::anything()).take(1 + i - shape.array.tuple.len()),
                    );
                    shape.array.tuple[*i] = Shape::infer_inner(schema, index, visited);
                }
                Keyword::Application(Application::AdditionalItems, schema) => {
                    shape.array.additional_items =
                        Some(Box::new(Shape::infer_inner(schema, index, visited)));
                }
                Keyword::Application(Application::UnevaluatedItems, schema) => {
                    unevaluated_items = Some(Shape::infer_inner(schema, index, visited));
                }

                // Object constraints.
                Keyword::Application(Application::Properties { name, .. }, schema) => {
                    let obj = ObjShape {
                        properties: vec![ObjProperty {
                            name: name.as_str().into(),
                            is_required: false,
                            shape: Shape::infer_inner(schema, index, visited),
                        }],
                        pattern_properties: Vec::new(),
                        additional_properties: None,
                    };
                    shape.object = ObjShape::intersect(shape.object, obj);
                }
                Keyword::Validation(Validation::Required { props, .. }) => {
                    let obj = ObjShape {
                        properties: props
                            .iter()
                            .sorted()
                            .map(|p| ObjProperty {
                                name: p.as_str().into(),
                                is_required: true,
                                shape: Shape::anything(),
                            })
                            .collect::<Vec<_>>(),
                        pattern_properties: Vec::new(),
                        additional_properties: None,
                    };
                    shape.object = ObjShape::intersect(shape.object, obj);
                }

                Keyword::Application(Application::PatternProperties { re }, schema) => {
                    let obj = ObjShape {
                        properties: Vec::new(),
                        pattern_properties: vec![ObjPattern {
                            re: re.clone(),
                            shape: Shape::infer_inner(schema, index, visited),
                        }],
                        additional_properties: None,
                    };
                    shape.object = ObjShape::intersect(shape.object, obj);
                }
                Keyword::Application(Application::AdditionalProperties, schema) => {
                    shape.object.additional_properties =
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
        shape.enum_ = intersect::intersect_enum(shape.type_, shape.enum_.take(), None);

        // Presence of an enum term similarly restricts the allowed types that
        // a location may take (since it may only take values of the enum).
        // We also check this again during intersection.
        if let Some(enum_) = &shape.enum_ {
            shape.type_ = shape.type_ & value_types(enum_.iter());
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
                        Shape::anything() // Don't re-visit this location.
                    } else if let Some(schema) = index.fetch(uri) {
                        visited.push(uri);
                        let referent = Shape::infer_inner(schema, index, visited);
                        visited.pop();
                        referent
                    } else {
                        Shape::anything()
                    };

                    // Track this |uri| as a reference, unless its resolved shape is itself
                    // a reference to another schema. In other words, promote the bottom-most
                    // $ref within a hierarchy of $ref's.
                    if !matches!(referent.provenance, Provenance::Reference(_)) {
                        referent.provenance = Provenance::Reference(Box::new(uri.clone()));
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
            (&shape.object.additional_properties, unevaluated_properties)
        {
            shape.object.additional_properties = Some(Box::new(unevaluated_properties));
        }
        if let (None, Some(unevaluated_items)) = (&shape.array.additional_items, unevaluated_items)
        {
            shape.array.additional_items = Some(Box::new(unevaluated_items));
        }

        shape
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use serde_json::json;

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
                title: Some("a-title".into()),
                description: Some("a-description".into()),
                reduction: Reduction::Strategy(
                    reduce::Strategy::FirstWriteWins(Default::default()),
                ),
                provenance: Provenance::Inline,
                default: Some(Box::new((
                    Value::String("john.doe@gmail.com".to_owned()),
                    None,
                ))),
                secret: Some(true),
                string: StringShape {
                    content_encoding: Some("base64".into()),
                    content_type: Some("some/thing".into()),
                    format: Some(Format::Email),
                    max_length: None,
                    min_length: 0,
                },
                ..Shape::nothing()
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
                reduction: Reduction::Strategy(
                    reduce::Strategy::FirstWriteWins(Default::default()),
                ),
                provenance: Provenance::Inline,
                ..Shape::anything()
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
                ..Shape::anything()
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
                ..Shape::anything()
            },
        );
    }

    #[test]
    fn test_string_length_and_format_number_widening() {
        infer_test(
            &[
                "{type: string, minLength: 3, maxLength: 33, format: number}",
                "{oneOf: [
                  {type: string, minLength: 19, maxLength: 20, format: integer},
                  {type: string, minLength: 3, maxLength: 20, format: number},
                  {type: string, minLength: 20, maxLength: 33, format: integer}
                ]}",
                "{allOf: [
                  {type: string, maxLength: 60},
                  {type: string, minLength: 3, maxLength: 78, format: number},
                  {type: string, minLength: 2, maxLength: 33}
                ]}",
            ],
            Shape {
                type_: types::STRING,
                provenance: Provenance::Inline,
                string: StringShape {
                    min_length: 3,
                    max_length: Some(33),
                    format: Some(Format::Number),
                    ..StringShape::new()
                },
                ..Shape::anything()
            },
        );
    }

    // This test documents the behavior in the corneer case where the
    // intersection of two object schemas forbids an enum property.
    #[test]
    fn test_enum_property_empty_intersection() {
        let shape = shape_from(
            r#"{
            "type": "object",
            "allOf": [
                {
                    "properties": {
                        "foo": {
                            "type": "integer",
                            "enum": [1, 2, 3]
                        }
                    }
                },
                {
                    "properties": {
                        "bar": { "type": "string" }
                    },
                    "additionalProperties": false
                }
            ]
        }"#,
        );

        let foo_shape = shape
            .object
            .properties
            .iter()
            .find(|p| p.name.as_ref() == "foo")
            .unwrap();

        // The `type_` is empty because the property is not allowed to be present.
        // Note that the `enum_` is still `Some`, though the set of values is empty.
        insta::assert_debug_snapshot!(foo_shape, @r###"
        ObjProperty {
            name: "foo",
            is_required: false,
            shape: Shape {
                type_: ,
                enum_: Some(
                    [],
                ),
                title: None,
                description: None,
                reduction: Unset,
                provenance: Inline,
                default: None,
                secret: None,
                annotations: {},
                array: ArrayShape {
                    additional_items: None,
                    max_items: None,
                    min_items: 0,
                    tuple: [],
                },
                numeric: NumericShape {
                    minimum: None,
                    maximum: None,
                },
                object: ObjShape {
                    additional_properties: None,
                    pattern_properties: [],
                    properties: [],
                },
                string: StringShape {
                    content_encoding: None,
                    content_type: None,
                    format: None,
                    max_length: None,
                    min_length: 0,
                },
            },
        }
        "###);
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
                ..Shape::anything()
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
                            name: "bar".into(),
                            is_required: true,
                            shape: enum_fixture(json!(["c"])),
                        },
                        ObjProperty {
                            name: "foo".into(),
                            is_required: false,
                            shape: enum_fixture(json!(["b"])),
                        },
                    ],
                    pattern_properties: vec![ObjPattern {
                        re: regex::Regex::new("fo.+").unwrap(),
                        shape: enum_fixture(json!(["b"])),
                    }],
                    additional_properties: None,
                },
                ..Shape::anything()
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
                        name: "foo".into(),
                        is_required: false,
                        shape: enum_fixture(json!(["a", "b"])),
                    }],
                    pattern_properties: Vec::new(),
                    additional_properties: Some(Box::new(enum_fixture(json!(["a", "b"])))),
                },
                ..Shape::anything()
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
                    ..ArrayShape::new()
                },
                ..Shape::anything()
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
                    additional_items: Some(Box::new(enum_fixture(json!(["a"])))),
                    ..ArrayShape::new()
                },
                ..Shape::anything()
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
                    additional_properties: Some(Box::new(enum_fixture(json!(["a"])))),
                    ..ObjShape::new()
                },
                ..Shape::anything()
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
                - {type: string}
                "#,
            ],
            Shape {
                provenance: Provenance::Inline,
                array: ArrayShape {
                    min_items: 5,
                    max_items: Some(10),
                    ..ArrayShape::new()
                },
                ..Shape::anything()
            },
        )
    }

    #[test]
    fn test_numeric_bounds() {
        infer_test(
            &[
                "{minimum: 5, maximum: 10}",
                // Intersections take more restrictive bounds.
                r#"
                allOf:
                - {minimum: 1, maximum: 10}
                - {minimum: 5, maximum: 100}
                "#,
                // Unions take least restrictive bounds.
                r#"
                anyOf:
                - {minimum: 7, maximum: 10}
                - {minimum: 5, maximum: 7}
                - {type: string}
                "#,
            ],
            Shape {
                provenance: Provenance::Inline,
                numeric: NumericShape {
                    minimum: Some(json::Number::Unsigned(5)),
                    maximum: Some(json::Number::Unsigned(10)),
                },
                ..Shape::anything()
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
                    additional_items: Some(Box::new(enum_fixture(json!([3, "c"])))),
                    ..ArrayShape::new()
                },
                ..Shape::anything()
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
                        name: "foo".into(),
                        is_required: false,
                        shape: enum_fixture(json!(["a", "b"])),
                    }],
                    pattern_properties: vec![ObjPattern {
                        re: regex::Regex::new("bar").unwrap(),
                        shape: enum_fixture(json!(["c", "d"])),
                    }],
                    additional_properties: Some(Box::new(enum_fixture(json!([1, 2])))),
                },
                ..Shape::anything()
            },
        );
        infer_test(
            &[
                // Non-matched properties and patterns are preserved if the
                // opposing sub-schemas have additionalProperties: false.
                r#"
                oneOf:
                - required: [foo]
                  properties:
                    foo: {enum: [a, b]}
                  additionalProperties: false
                - patternProperties: {bar: {enum: [c, d]}}
                  additionalProperties: false
                "#,
            ],
            Shape {
                provenance: Provenance::Inline,
                object: ObjShape {
                    properties: vec![ObjProperty {
                        name: "foo".into(),
                        is_required: false,
                        shape: enum_fixture(json!(["a", "b"])),
                    }],
                    pattern_properties: vec![ObjPattern {
                        re: regex::Regex::new("bar").unwrap(),
                        shape: enum_fixture(json!(["c", "d"])),
                    }],
                    additional_properties: Some(Box::new(Shape::nothing())),
                },
                ..Shape::anything()
            },
        );
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
                        name: "foo".into(),
                        is_required: true,
                        shape: Shape {
                            type_: types::STRING,
                            provenance: Provenance::Inline,
                            ..Shape::anything()
                        },
                    }],
                    ..ObjShape::new()
                },
                ..Shape::anything()
            },
        )
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
        assert!(matches!(
            obj.reduction,
            Reduction::Strategy(reduce::Strategy::Merge(_))
        ));
        assert!(obj.object.additional_properties.is_some());
    }

    #[test]
    fn test_annotation_collection() {
        let obj = shape_from(
            r#"
            type: object
            properties:
                bar:
                    X-bar-top-level: true
                    oneOf:
                        - type: string
                          x-bar-one: oneVal
                          x-bar-two: twoVal
                          x-bar-three: threeVal
                        - type: string
                          x-bar-two: twoVal
                          x-bar-four: fourVal
                foo:
                    X-foo-top-level: false
                    allOf:
                        - type: string
                          x-foo-one: oneVal
                        - type: string
                          x-foo-two: twoVal
                conflicting:
                    description: |-
                        this documents the behavior in the edge case where there's conflicting
                        values for the same annotation. Technically, it would be more correct
                        to use a multi-map and collect both values. But this seems like a weird
                        enough edge case that we can safely ignore it for now and pick one of the
                        values arbitrarily.
                    x-conflicting-ann: yes please
                    anyOf:
                        - x-conflicting-ann: no thankyou
            x-test-top-level: true
            "#,
        );
        insta::assert_debug_snapshot!(obj);
    }

    #[test]
    fn test_default_value_validation() {
        let obj = shape_from(
            r#"
        type: object
        properties:
            scalar-type:
                type: string
                default: 1234

            multi-type:
                type: [string, array]
                default: 1234

            object-type-missing-prop:
                type: object
                properties:
                    requiredProp:
                        type: string
                required: [requiredProp]
                default: { otherProp: "stringValue" }

            object-type-prop-wrong-type:
                type: object
                properties:
                    requiredProp:
                        type: string
                required: [requiredProp]
                default: { requiredProp: 1234 }

            array-wrong-items:
                type: array
                items:
                    type: integer
                default: ["aString"]
        "#,
        );

        insta::assert_debug_snapshot!(obj.inspect());
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
                        anyOf:
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
                            name: "a-thing".into(),
                            is_required: false,
                            shape: Shape {
                                type_: types::STRING,
                                title: Some("Just a thing.".into()),
                                provenance: Provenance::Reference(
                                    Box::new(Url::parse("http://example/schema#/$defs/thing").unwrap()),
                                ),
                                default: Some(Box::new((json!("a-default"), None))),
                                ..Shape::anything()
                            },
                        },
                        ObjProperty {
                            name: "a-thing-plus".into(),
                            is_required: false,
                            shape: Shape {
                                type_: types::STRING,
                                string: StringShape {
                                    min_length: 16,
                                    ..StringShape::new()
                                },
                                provenance: Provenance::Inline,
                                ..Shape::anything()
                            },
                        },
                        ObjProperty {
                            name: "multi".into(),
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
                                                Box::new(Url::parse("http://example/schema#/properties/multi/items/2").unwrap()),
                                            ),
                                            ..Shape::anything()
                                        },
                                        Shape {
                                            type_: types::INTEGER,
                                            provenance: Provenance::Reference(
                                                Box::new(Url::parse("http://example/schema#/properties/multi/items/2").unwrap()),
                                            ),
                                            ..Shape::anything()
                                        },
                                        Shape {
                                            type_: types::INTEGER,
                                            provenance: Provenance::Inline,
                                            ..Shape::anything()
                                        },
                                    ],
                                    ..ArrayShape::new()
                                },
                                ..Shape::anything()
                            },
                        },
                    ],
                    ..ObjShape::new()
                },
                ..Shape::anything()
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
            Shape::union(Shape::anything(), expect.clone()),
            Shape::anything(),
            "any || fixture == any"
        );
        assert_eq!(
            Shape::union(expect.clone(), Shape::anything()),
            Shape::anything(),
            "fixture || any == any"
        );
        assert_eq!(
            Shape::intersect(expect.clone(), expect.clone()),
            expect,
            "fixture && fixture == fixture"
        );
        assert_eq!(
            Shape::intersect(Shape::anything(), expect.clone()),
            expect,
            "any && fixture == fixture"
        );
        assert_eq!(
            Shape::intersect(expect.clone(), Shape::anything()),
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
                provenance: Provenance::Reference(Box::new(
                    Url::parse("http://example/schema#/$defs/foo").unwrap()
                )),
                ..Shape::anything()
            }
        );
        assert_eq!(
            nested_bar.0,
            &Shape {
                provenance: Provenance::Reference(Box::new(
                    Url::parse("http://example/schema#/$defs/bar").unwrap()
                )),
                ..Shape::anything()
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

    #[test]
    fn test_sql_sourced_schema_regression() {
        let shape = shape_from(
            r###"
            $defs:
                MyTable:
                    $anchor: MyTable
                    type: object
                    required: [id]
                    properties:
                        data:
                            description: "(source type: varchar)"
                            type: string
                        id:
                            type: integer
                            description: "(source type: non-nullable int)"
            allOf:
              - required: [_meta]
                properties:
                    _meta:
                        type: object
                        required: [op]
                        additionalProperties: false
                        properties:
                            before:
                                $ref: "#MyTable"
                                unevaluatedProperties: false
                            op: { type: string }
              - $ref: "#MyTable"

            unevaluatedProperties: false
            "###,
        );

        // Expect there are no inspection errors.
        assert_eq!(shape.inspect_closed(), Vec::new());

        // Expect we round-trip to expected JSON schema.
        insta::assert_yaml_snapshot!(super::schema::to_schema(shape), @r###"
        type: object
        properties:
          _meta:
            type: object
            properties:
              before:
                type: object
                properties:
                  data:
                    description: "(source type: varchar)"
                    type: string
                  id:
                    description: "(source type: non-nullable int)"
                    type: integer
                additionalProperties: false
                required:
                  - id
              op:
                type: string
            additionalProperties: false
            required:
              - op
          data:
            description: "(source type: varchar)"
            type: string
          id:
            description: "(source type: non-nullable int)"
            type: integer
        additionalProperties: false
        required:
          - _meta
          - id
        "###);
    }

    fn enum_fixture(value: Value) -> Shape {
        let v = value.as_array().unwrap().clone();
        Shape {
            type_: value_types(v.iter()),
            enum_: Some(v.clone()),
            provenance: Provenance::Inline,
            ..Shape::anything()
        }
    }
}
