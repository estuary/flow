/// This module implements various inspections which can be performed over Shapes.
use super::*;
use crate::reduce::Strategy;
use json::{LocatedProperty, Location};

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
    #[error("{0} default value is invalid: {1}")]
    InvalidDefaultValue(String, crate::FailedValidation),
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

        // Invalid values for default values.
        if let Some(default) = &self.default {
            if let Some(err) = &default.1 {
                out.push(Error::InvalidDefaultValue(
                    loc.pointer_str().to_string(),
                    err.to_owned(),
                ));
            }
        };

        if matches!(self.reduction, Reduction::Strategy(Strategy::Sum))
            && self.type_ - types::INT_OR_FRAC != types::INVALID
        {
            out.push(Error::SumNotNumber(
                loc.pointer_str().to_string(),
                self.type_,
            ));
        }
        if matches!(self.reduction, Reduction::Strategy(Strategy::Merge(_)))
            && self.type_ - (types::OBJECT | types::ARRAY) != types::INVALID
        {
            out.push(Error::MergeNotObjectOrArray(
                loc.pointer_str().to_string(),
                self.type_,
            ));
        }
        if matches!(self.reduction, Reduction::Strategy(Strategy::Set(_))) {
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

#[cfg(test)]
mod test {
    use super::{shape_from, Error};
    use json::schema::types;
    use pretty_assertions::assert_eq;

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
}
