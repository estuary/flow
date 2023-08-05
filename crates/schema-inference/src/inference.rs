use crate::shape;
use doc::shape::{ArrayShape, ObjProperty, ObjShape, Shape, StringShape};
use json::schema::{formats::Format, types};
use serde_json::Value as JsonValue;

pub fn infer_shape(value: &JsonValue) -> Shape {
    let mut shape = Shape {
        type_: types::Set::for_value(value),
        ..Shape::nothing()
    };

    if let JsonValue::String(value) = value {
        shape.string = infer_string_shape(value);
    } else if let JsonValue::Array(inner) = value {
        shape.array = infer_array_shape(inner);
    } else if let JsonValue::Object(values) = value {
        shape.object = infer_object_shape(values);
    }

    shape
}

fn infer_string_shape(value: &str) -> StringShape {
    let format = match value {
        _ if Format::Integer.validate(value).is_ok() => Some(Format::Integer),
        _ if Format::Number.validate(value).is_ok() => Some(Format::Number),
        _ if Format::DateTime.validate(value).is_ok() => Some(Format::DateTime),
        _ if Format::Date.validate(value).is_ok() => Some(Format::Date),
        _ if Format::Uuid.validate(value).is_ok() => Some(Format::Uuid),
        _ => None,
    };

    StringShape {
        format,
        ..StringShape::new()
    }
}

fn infer_array_shape(inner: &[JsonValue]) -> ArrayShape {
    if let Some(shape) = inner
        .iter()
        .map(infer_shape)
        .reduce(|acc, v| shape::merge(acc, v))
    {
        ArrayShape {
            tuple: vec![shape],
            ..ArrayShape::new()
        }
    } else {
        ArrayShape::new()
    }
}

fn infer_object_shape(inner: &serde_json::Map<String, JsonValue>) -> ObjShape {
    let properties = inner
        .iter()
        .map(|(key, value)| ObjProperty {
            name: key.to_owned(),
            // TODO(johnny): Mark `required` once we have a tighter ability
            // to quickly update inferred collection schemas upon a violation.
            is_required: false,
            shape: infer_shape(value),
        })
        .collect();

    ObjShape {
        properties,
        // TODO(johnny): Once we get good at updating inferred schemas on violations,
        // we want to enable additionalProperties: false.
        // This does two things:
        //  1) It allows us to define schema inference strictly in terms of
        //     doc::inference::Shape::union(), since union will preserve properties
        //     on one side and not the other, so long as the other side is constrained
        //     to not exist. This is the *only* reason we define a separate "merge"
        //     operation in this crate.
        //  2) It ensures that, when new properties are added, we first propagate their
        //     projections to downstream materializations *before* we process the first
        //     such document. This is a significant UX improvement for users because
        //     added properties will dynamically be added to materializations, with
        //     all values immediately represented in the ongoing materialization.
        /*
        additional: Some(Box::new(Shape {
            type_: types::INVALID,
            ..Default::default()
        })),
        */
        ..ObjShape::new()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use json::schema::types;
    use serde_json::json;

    #[test]
    fn build_primitive_types() {
        let shape = infer_shape(&json!(true));
        assert!(shape.type_.is_single_scalar_type());
        assert_eq!(types::BOOLEAN, shape.type_);

        let shape = infer_shape(&json!("string".to_string()));
        assert!(shape.type_.is_single_scalar_type());
        assert_eq!(types::STRING, shape.type_);

        let shape = infer_shape(&json!(123));
        assert!(shape.type_.is_single_scalar_type());
        assert_eq!(types::INTEGER, shape.type_);

        let shape = infer_shape(&json!(null));
        assert_eq!(types::NULL, shape.type_);
    }

    #[test]
    fn build_string_types() {
        let shape = infer_shape(&json!("1"));
        assert_eq!(shape.type_, types::STRING);
        assert_eq!(Some(Format::Integer), shape.string.format);

        let shape = infer_shape(&json!("100.00"));
        assert_eq!(shape.type_, types::STRING);
        assert_eq!(Some(Format::Integer), shape.string.format);

        let shape = infer_shape(&json!("1.001"));
        assert_eq!(shape.type_, types::STRING);
        assert_eq!(Some(Format::Number), shape.string.format);

        for t in ["NaN", "Infinity", "-Infinity"] {
            let shape = infer_shape(&json!(t));
            assert_eq!(shape.type_, types::STRING);
            assert_eq!(Some(Format::Number), shape.string.format);
        }

        let shape = infer_shape(&json!("2021-07-08T12:34:56.523Z"));
        assert_eq!(shape.type_, types::STRING);
        assert_eq!(Some(Format::DateTime), shape.string.format);

        let shape = infer_shape(&json!("2021-07-08"));
        assert_eq!(shape.type_, types::STRING);
        assert_eq!(Some(Format::Date), shape.string.format);

        let shape = infer_shape(&json!("34c1506a-a0da-498e-b690-ea5183026979"));
        assert_eq!(shape.type_, types::STRING);
        assert_eq!(Some(Format::Uuid), shape.string.format);

        let shape = infer_shape(&json!(
            "2021-07-08 plus 34c1506a-a0da-498e-b690-ea5183026979 12.34"
        ));
        assert_eq!(shape.type_, types::STRING);
        assert_eq!(None, shape.string.format);
    }

    #[test]
    fn build_array_types() {
        let shape = infer_shape(&json!([1, 2, 3]));
        assert_eq!(types::ARRAY, shape.type_);
        assert_eq!(types::INTEGER, shape.array.tuple[0].type_);

        let shape = infer_shape(&json!([[1], [2.5], ["3"]]));
        assert_eq!(types::ARRAY, shape.type_);
        assert_eq!(types::ARRAY, shape.array.tuple[0].type_);
        assert_eq!(
            types::INT_OR_FRAC | types::STRING,
            shape.array.tuple[0].array.tuple[0].type_
        );

        let shape = infer_shape(&json!([[["3 layers deep"]]]));
        assert_eq!(
            types::STRING,
            shape
                // one
                .array
                .tuple[0]
                // two
                .array
                .tuple[0]
                // three
                .array
                .tuple[0]
                // type of items
                .type_
        );
    }

    #[test]
    fn build_object_types() {
        let shape = infer_shape(&json!({"a": true, "b": null, "c": 3}));
        assert_eq!(types::OBJECT, shape.type_);
        let a = get(&shape.object, "a");
        assert_eq!(types::BOOLEAN, a.shape.type_);
        let b = get(&shape.object, "b");
        assert_eq!(types::NULL, b.shape.type_);
        let c = get(&shape.object, "c");
        assert_eq!(types::INTEGER, c.shape.type_);

        let shape = infer_shape(&json!({"a": {"b": {"c": 3}}}));
        assert_eq!(types::OBJECT, shape.type_);
        let a = get(&shape.object, "a");
        assert_eq!(types::OBJECT, a.shape.type_);
        let b = get(&a.shape.object, "b");
        assert_eq!(types::OBJECT, b.shape.type_);
        let c = get(&b.shape.object, "c");
        assert_eq!(types::INTEGER, c.shape.type_);
    }

    fn get<'o>(object: &'o ObjShape, prop_name: &str) -> &'o ObjProperty {
        object
            .properties
            .iter()
            .find(|p| p.name == prop_name)
            .expect("key to exist")
    }
}
