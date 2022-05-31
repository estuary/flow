use doc::inference::*;
use json::schema::types;
use serde_json::Value;

#[derive(Debug, thiserror::Error)]
pub enum PropertyError {
    #[error("failed to parse the value: {0}")]
    InvalidValueType(serde_json::Value),
}

pub fn build<'a>(
    property: &'a mut ObjProperty,
    data: &Value,
) -> Result<&'a ObjProperty, PropertyError> {
    match data {
        Value::Bool(_) => property.shape.type_ = types::BOOLEAN,
        Value::Number(_) => property.shape.type_ = types::INT_OR_FRAC,
        Value::String(_) => property.shape.type_ = types::STRING,
        Value::Null => {
            property.is_required = false;
            property.shape = Shape {
                type_: types::NULL,
                ..property.shape.to_owned()
            };
        }
        e => {
            return Err(PropertyError::InvalidValueType(e.to_owned()));
        }
    };

    return Ok(property);
}

#[cfg(test)]
mod test {
    use super::*;
    use doc::inference::ObjProperty;
    use json::schema::types;
    use serde_json::json;

    #[test]
    fn test_different_types() {
        let mut property = ObjProperty {
            name: "test".to_string(),
            is_required: false,
            shape: Shape::default(),
        };

        build(&mut property, &json!(true)).expect("expected a valid value");
        assert_eq!(property.shape.type_.is_single_scalar_type(), true);
        assert_eq!(property.shape.type_.overlaps(types::BOOLEAN), true);

        build(&mut property, &json!("string".to_string())).expect("expected a valid value");
        assert_eq!(property.shape.type_.is_single_scalar_type(), true);
        assert_eq!(property.shape.type_.overlaps(types::STRING), true);

        build(&mut property, &json!(123)).expect("expected a valid value");
        assert_eq!(property.shape.type_.is_single_scalar_type(), true);
        assert_eq!(property.shape.type_.overlaps(types::INT_OR_FRAC), true);

        build(&mut property, &json!(null)).expect("expected a valid value");
        assert_eq!(property.shape.type_.overlaps(types::NULL), true);
    }
}
