use doc::inference::{ArrayShape, ObjProperty, ObjShape, StringShape, Shape};
use json::schema::{types, formats::Format};
use serde_json::Value as JsonValue;
use bigdecimal::BigDecimal;
use std::str::FromStr;
use num_bigint::BigInt;

use crate::shape;

pub fn infer_shape(value: &JsonValue) -> Shape {
    match value {
        JsonValue::Bool(value) => infer_bool(value),
        JsonValue::Number(value) => infer_number(value),
        JsonValue::String(value) => infer_string(value.as_ref()),
        JsonValue::Null => infer_null(),
        JsonValue::Array(inner) => infer_array(inner.as_slice()),
        JsonValue::Object(values) => infer_object(values),
    }
}

fn infer_bool(_value: &bool) -> Shape {
    Shape {
        type_: types::BOOLEAN,
        ..Default::default()
    }
}

fn infer_number(value: &serde_json::Number) -> Shape {
    let type_ = if value.is_f64() {
        types::FRACTIONAL
    } else {
        types::INTEGER
    };

    Shape {
        type_,
        ..Default::default()
    }
}

fn infer_string(value: &str) -> Shape {
    let (format, additional_type) = if BigInt::parse_bytes(value.as_bytes(), 10).is_some() {
        (Some(Format::Integer), Some(types::INTEGER))
    } else if BigDecimal::from_str(value).is_ok() || ["NaN", "Infinity", "-Infinity"].contains(&value) {
        (Some(Format::Number), Some(types::INT_OR_FRAC))
    } else {
        (None, None)
    };

    let string = StringShape {
        format,
        ..Default::default()
    };

    let type_ = additional_type.map(|t| types::STRING | t).unwrap_or(types::STRING);

    Shape {
        type_,
        string,
        ..Default::default()
    }
}

fn infer_null() -> Shape {
    Shape {
        type_: types::NULL,
        ..Default::default()
    }
}

fn infer_array(inner: &[JsonValue]) -> Shape {
    if let Some(shape) = inner
        .iter()
        .map(infer_shape)
        .reduce(|acc, v| shape::merge(acc, v))
    {
        Shape {
            type_: types::ARRAY,
            array: ArrayShape {
                tuple: vec![shape],
                ..Default::default()
            },
            ..Default::default()
        }
    } else {
        Shape {
            type_: types::ARRAY,
            ..Default::default()
        }
    }
}

fn infer_object(inner: &serde_json::Map<String, JsonValue>) -> Shape {
    let properties = inner
        .iter()
        .map(|(key, value)| ObjProperty {
            name: key.to_owned(),
            // We don't mark any non-key property as required since it is hard to revert requirement on a
            // property if we later realise that this value can sometimes be null
            is_required: false,
            shape: infer_shape(value),
        })
        .collect();

    Shape {
        type_: types::OBJECT,
        object: ObjShape {
            properties,
            ..Default::default()
        },
        ..Default::default()
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
        assert!(shape.type_.overlaps(types::STRING));
        assert!(shape.type_.overlaps(types::INTEGER));
        assert_eq!(Some(Format::Integer), shape.string.format);

        let shape = infer_shape(&json!("1.0"));
        assert!(shape.type_.overlaps(types::STRING));
        assert!(shape.type_.overlaps(types::INT_OR_FRAC));
        assert_eq!(Some(Format::Number), shape.string.format);

        for t in ["NaN", "Infinity", "-Infinity"] {
            let shape = infer_shape(&json!(t));
            assert!(shape.type_.overlaps(types::STRING));
            assert!(shape.type_.overlaps(types::INT_OR_FRAC));
            assert_eq!(Some(Format::Number), shape.string.format);
        }
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
