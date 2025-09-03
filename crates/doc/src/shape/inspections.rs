/// This module implements various inspections which can be performed over Shapes.
use super::*;
use crate::{redact, reduce};
use json::{schema::formats::Format, LocatedProperty, Location};

#[derive(thiserror::Error, Debug, Eq, PartialEq)]
pub enum Error {
    #[error("'{0}' must exist, but is constrained to always be invalid")]
    ImpossibleMustExist(String),
    #[error("'{0}' has reduction strategy, but its parent does not")]
    ChildWithoutParentReduction(String),
    #[error("{0} has 'sum' reduction strategy (restricted to integers, numbers and strings with `format: integer` or `format: number`) but has types {1:?}")]
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
    #[error("{0} default value is invalid: {1}")]
    InvalidDefaultValue(String, crate::FailedValidation),
    #[error("'{0}' is required to prohibit additional properties, but instead allows them")]
    AllowsAdditionalProperties(String),
    #[error("`block` redact strategy cannot be applied at '{0}' because it must exist")]
    BlockRedactionMustExist(String),
    #[error("{0} has 'sha256' redact strategy but cannot be a string (types are {1:?})")]
    Sha256RedactionNotString(String, types::Set),
}

impl Shape {
    /// Inspect the shape for a number of statically-determinable errors.
    pub fn inspect(&self) -> Vec<Error> {
        let mut v = Vec::new();
        self.inspect_inner(Location::Root, true, false, &mut v);
        v
    }

    /// Inspect the shape for a number of statically-determinable errors,
    /// and further require that the schema is "closed": that no schematized
    /// object with properties allows for any additional properties.
    pub fn inspect_closed(&self) -> Vec<Error> {
        let mut v = Vec::new();
        self.inspect_inner(Location::Root, true, true, &mut v);
        v
    }

    fn inspect_inner(
        &self,
        loc: Location,
        must_exist: bool,
        must_be_closed: bool,
        out: &mut Vec<Error>,
    ) {
        // Enumerations over array sub-locations.
        let items = self.array.tuple.iter().enumerate().map(|(index, s)| {
            (
                loc.push_item(index),
                self.type_ == types::ARRAY && index < self.array.min_items as usize,
                s,
            )
        });
        let addl_items = self
            .array
            .additional_items
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
            .pattern_properties
            .iter()
            .map(|op| (loc.push_prop(op.re.as_str()), false, &op.shape));
        let addl_props = self
            .object
            .additional_properties
            .iter()
            .map(|shape| (loc.push_prop("*"), false, shape.as_ref()));

        if self.type_ == types::INVALID && must_exist {
            out.push(Error::ImpossibleMustExist(loc.pointer_str().to_string()));
        }

        if matches!(self.redact, Redact::Strategy(redact::Strategy::Block)) && must_exist {
            out.push(Error::BlockRedactionMustExist(
                loc.pointer_str().to_string(),
            ));
        }

        if matches!(self.redact, Redact::Strategy(redact::Strategy::Sha256))
            && !self.type_.overlaps(types::STRING)
        {
            out.push(Error::Sha256RedactionNotString(
                loc.pointer_str().to_string(),
                self.type_,
            ));
        }

        if must_be_closed
            && self.type_.overlaps(types::OBJECT)
            && !(self.object.properties.is_empty() && self.object.pattern_properties.is_empty())
            && !matches!(
                self.object.additional_properties.as_deref(),
                Some(Self {
                    type_: json::schema::types::INVALID,
                    ..
                })
            )
        {
            out.push(Error::AllowsAdditionalProperties(
                loc.pointer_str().to_string(),
            ));
        }

        // Invalid values for default values.
        if let Some(default) = &self.default {
            if let Some(err) = &default.1 {
                out.push(Error::InvalidDefaultValue(
                    loc.pointer_str().to_string(),
                    err.to_owned(),
                ));
            }
        };

        if matches!(self.reduce, Reduce::Strategy(reduce::Strategy::Sum)) {
            match (self.type_ - types::INT_OR_FRAC, &self.string.format) {
                (types::INVALID, _) => (), // Okay (native numeric only).
                (types::STRING, Some(Format::Number) | Some(Format::Integer)) => (), // Okay (string-formatted numeric).
                (type_, _) => {
                    out.push(Error::SumNotNumber(loc.pointer_str().to_string(), type_));
                }
            }
        }
        if matches!(self.reduce, Reduce::Strategy(reduce::Strategy::Merge(_)))
            && self.type_ - (types::OBJECT | types::ARRAY) != types::INVALID
        {
            out.push(Error::MergeNotObjectOrArray(
                loc.pointer_str().to_string(),
                self.type_,
            ));
        }
        if matches!(self.reduce, Reduce::Strategy(reduce::Strategy::Set(_))) {
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
            if matches!(self.reduce, Reduce::Unset) && !matches!(child.reduce, Reduce::Unset) {
                out.push(Error::ChildWithoutParentReduction(
                    loc.pointer_str().to_string(),
                ))
            }

            child.inspect_inner(loc, must_exist && child_must_exist, must_be_closed, out);
        }
    }
}

#[cfg(test)]
mod test {
    use super::{shape_from, Error};
    use json::schema::types;

    #[test]
    fn test_error_collection() {
        let obj = shape_from(
            r#"
        type: object
        reduce: {strategy: merge}
        redact: {strategy: block}  # Block at root should error
        properties:
            sum-right-type:
                reduce: {strategy: sum}
                type: [number, string]
                format: integer

            sum-wrong-type:
                reduce: {strategy: sum}
                type: [number, string]

            must-exist-but-cannot: false
            may-not-exist: false

            # Sha256 redaction on non-string types
            redact-sha256-on-number:
                type: number
                redact: {strategy: sha256}

            redact-sha256-on-object:
                type: object
                redact: {strategy: sha256}

            # Block redaction on nested optional property (this is OK)
            redact-block-nested-optional:
                type: string
                redact: {strategy: block}

            # Block redaction on nested required property (should error)
            redact-block-nested-required:
                type: string
                redact: {strategy: block}

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

        required:
            - must-exist-but-cannot
            - nested-obj-or-string
            - nested-array
            - nested-array-or-string
            - redact-block-nested-required

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
                Error::BlockRedactionMustExist("".to_owned()),
                Error::SetNotObject("/0".to_owned(), types::ANY),
                Error::SetInvalidProperty("/-/whoops1".to_owned()),
                Error::SetInvalidProperty("/-/whoops2".to_owned()),
                Error::ImpossibleMustExist("/must-exist-but-cannot".to_owned()),
                Error::ImpossibleMustExist("/nested-array/1".to_owned()),
                Error::BlockRedactionMustExist("/redact-block-nested-required".to_owned()),
                Error::Sha256RedactionNotString(
                    "/redact-sha256-on-number".to_owned(),
                    types::INT_OR_FRAC
                ),
                Error::Sha256RedactionNotString(
                    "/redact-sha256-on-object".to_owned(),
                    types::OBJECT
                ),
                Error::SumNotNumber("/sum-wrong-type".to_owned(), types::STRING),
                Error::MergeNotObjectOrArray("/merge-wrong-type".to_owned(), types::BOOLEAN),
                Error::ChildWithoutParentReduction("/*/nested-sum".to_owned()),
            ]
        );
    }

    #[test]
    fn test_closed_shape() {
        let obj = shape_from(
            r#"
        $defs:
            foo:
                type: object
                properties:
                    foo: {type: string}

        type: object
        additionalProperties: false
        properties:
            foo: { $ref: '#/$defs/foo' }
            bar:
                $ref: '#/$defs/foo'
                additionalProperties: false
            baz: { type: integer }
            qux: { $ref: '#/$defs/foo/properties/foo' }
            open-obj: { type: object }
            open-arr: { type: array, items: true }
            other:
                type: array
                items:
                    patternProperties:
                        thing: { type: integer }
        "#,
        );
        assert_eq!(
            obj.inspect_closed(),
            vec![
                Error::AllowsAdditionalProperties("/foo".to_owned()),
                Error::AllowsAdditionalProperties("/other/-".to_owned()),
            ]
        );
    }
}
