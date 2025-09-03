// This module allows for inspecting and recursively enumerating
// the known locations within a Shape.
use super::*;
use crate::{ptr::Token, Pointer};
use json::Location;

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
    pub fn must(&self) -> bool {
        matches!(self, Exists::Must)
    }
    pub fn cannot(&self) -> bool {
        matches!(self, Exists::Cannot)
    }
}

impl Exists {
    // Extend a current path with Exists status, with a sub-location
    // having an applied Exists status.
    fn extend(&self, child: Self) -> Self {
        match (*self, child) {
            (Exists::Cannot, _) | (_, Exists::Cannot) => Exists::Cannot,
            (Exists::Implicit, _) | (_, Exists::Implicit) => Exists::Implicit,
            (Exists::May, _) | (_, Exists::May) => Exists::May,
            (Exists::Must, Exists::Must) => Exists::Must,
        }
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

    /// Produce flattened locations of nested items and properties of this Shape,
    /// as tuples of the encoded location JSON Pointer, an indication of whether
    /// the pointer is a pattern, its Shape, and an Exists constraint.
    pub fn locations(&self) -> Vec<(Pointer, bool, &Shape, Exists)> {
        let mut out = Vec::new();
        self.locations_inner(Location::Root, Exists::Must, false, &mut out);
        out
    }

    fn locate_token(&self, token: &Token) -> (&Shape, Exists) {
        match token {
            Token::Index(index) if self.type_.overlaps(types::ARRAY) => {
                let exists = if self.type_ == types::ARRAY && *index < self.array.min_items as usize
                {
                    // A sub-item must exist iff this location can _only_
                    // be an array, and it's within the minItems bound.
                    Exists::Must
                } else if *index >= self.array.max_items.unwrap_or(u32::MAX) as usize {
                    // It cannot exist if outside the maxItems bound.
                    Exists::Cannot
                } else if self.array.max_items.is_some()
                    || *index < self.array.tuple.len()
                    || self.array.additional_items.is_some()
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
                } else if let Some(addl) = &self.array.additional_items {
                    (addl.as_ref(), exists)
                } else {
                    (&SENTINEL_SHAPE, exists)
                }
            }
            Token::NextIndex if self.type_.overlaps(types::ARRAY) => (
                self.array
                    .additional_items
                    .as_ref()
                    .map(AsRef::as_ref)
                    .unwrap_or(&SENTINEL_SHAPE),
                Exists::Cannot,
            ),

            Token::NextProperty if self.type_.overlaps(types::OBJECT) => (
                self.object
                    .additional_properties
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
            Token::NextProperty => (&SENTINEL_SHAPE, Exists::Cannot),
        }
    }

    fn obj_property_location(&self, prop: &str) -> (&Shape, Exists) {
        if let Some(property) = self.object.properties.iter().find(|p| *p.name == *prop) {
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
            .pattern_properties
            .iter()
            .find(|p| p.re.is_match(prop))
        {
            (&pattern.shape, Exists::May)
        } else if let Some(addl) = &self.object.additional_properties {
            (addl.as_ref(), Exists::May)
        } else {
            (&SENTINEL_SHAPE, Exists::Implicit)
        }
    }

    fn locations_inner<'s>(
        &'s self,
        location: Location<'_>,
        exists: Exists,
        pattern: bool,
        out: &mut Vec<(Pointer, bool, &'s Shape, Exists)>,
    ) {
        let exists = if self.type_ == types::INVALID {
            Exists::Cannot
        } else {
            exists
        };
        out.push((Pointer::from_location(&location), pattern, self, exists));

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

        for ObjPattern { re, shape: child } in &self.object.pattern_properties {
            child.locations_inner(
                location.push_prop(re.as_str()),
                exists.extend(Exists::May),
                true,
                out,
            );
        }

        if let Some(child) = &self.object.additional_properties {
            child.locations_inner(
                location.push_next_property(),
                exists.extend(Exists::May),
                true,
                out,
            );
        }

        let ArrayShape {
            tuple,
            additional_items,
            min_items,
            ..
        } = &self.array;

        for (index, child) in tuple.into_iter().enumerate() {
            let exists = if self.type_ == types::ARRAY && index < *min_items as usize {
                exists.extend(Exists::Must)
            } else {
                exists.extend(Exists::May)
            };

            child.locations_inner(location.push_item(index), exists, pattern, out);
        }

        if let Some(child) = additional_items {
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
static SENTINEL_SHAPE: Shape = Shape::anything();

#[cfg(test)]
mod test {
    use super::*;

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
            (&arr1, "/-", ("<missing>", Exists::Cannot)),
            (&arr2, "/0", ("0", Exists::May)),
            (&arr2, "/1", ("1", Exists::May)),
            (&arr2, "/123", ("<missing>", Exists::Implicit)),
            (&arr2, "/not-an-index", ("<missing>", Exists::Cannot)),
            (&arr2, "/-", ("<missing>", Exists::Cannot)),
        ];

        for &(shape, ptr, expect) in cases {
            let mut_shape = &mut (*shape).clone();

            let actual = mut_shape.locate(&Pointer::from(ptr));
            let actual = (
                actual
                    .0
                    .enum_
                    .as_ref()
                    .map(|i| i[0].as_str().unwrap())
                    .unwrap_or("<missing>"),
                actual.1,
            );
            assert_eq!(expect, actual, "case {:?}", ptr);
        }

        let obj_locations = obj.locations();
        let obj_locations = obj_locations
            .iter()
            .map(|(ptr, pattern, shape, exists)| (ptr.to_string(), *pattern, shape.type_, *exists))
            .collect::<Vec<_>>();

        assert_eq!(
            obj_locations,
            vec![
                ("".to_string(), false, types::OBJECT, Exists::Must),
                ("/1".to_string(), false, types::OBJECT, Exists::Must),
                ("/1/-".to_string(), false, types::OBJECT, Exists::Must),
                ("/1/-/2".to_string(), false, types::STRING, Exists::Must),
                (
                    "/multi-type".to_string(),
                    false,
                    types::ARRAY | types::OBJECT,
                    Exists::May
                ),
                (
                    "/multi-type/child".to_string(),
                    false,
                    types::STRING,
                    Exists::May
                ),
                ("/parent".to_string(), false, types::OBJECT, Exists::Must),
                (
                    "/parent/40two".to_string(),
                    false,
                    types::STRING,
                    Exists::May
                ),
                (
                    "/parent/impossible".to_string(),
                    false,
                    types::INVALID,
                    Exists::Cannot
                ),
                (
                    "/parent/opt-child".to_string(),
                    false,
                    types::STRING,
                    Exists::May
                ),
                (
                    "/parent/req-child".to_string(),
                    false,
                    types::STRING,
                    Exists::Must
                ),
                ("/prop".to_string(), false, types::STRING, Exists::May),
                ("/pattern+".to_string(), true, types::STRING, Exists::May),
                ("/*".to_string(), true, types::STRING, Exists::May),
            ]
        );

        let arr_locations = arr1.locations();
        let arr_locations = arr_locations
            .iter()
            .map(|(ptr, pattern, shape, exists)| (ptr.to_string(), *pattern, shape.type_, *exists))
            .collect::<Vec<_>>();

        assert_eq!(
            arr_locations,
            vec![
                ("".to_string(), false, types::ARRAY, Exists::Must),
                ("/0".to_string(), false, types::STRING, Exists::Must),
                ("/1".to_string(), false, types::STRING, Exists::Must),
                ("/2".to_string(), false, types::STRING, Exists::May),
                ("/-".to_string(), true, types::STRING, Exists::May),
            ]
        );
    }
}
