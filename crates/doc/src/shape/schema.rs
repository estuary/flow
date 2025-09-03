// This module maps a Shape into a representative JSON Schema.
use super::*;
use json::schema::{keywords, types};
use schemars::Schema;

// TODO(johnny): This *probably* should be an impl Shape { into_schema(self) -> Schema }
// Consider refactoring as such if we're happy with this interface.
pub fn to_schema(shape: Shape) -> Schema {
    to_sub_schema(shape)
}

fn to_sub_schema(shape: Shape) -> Schema {
    let Shape {
        type_,
        enum_,
        title,
        description,
        reduce,
        redact,
        provenance: _, // Not mapped to a schema.
        default,
        secret,
        annotations,
        array,
        numeric,
        object,
        string,
    } = shape;

    let mut out = serde_json::Map::new();

    if type_ == types::INVALID {
        return Schema::from(false);
    } else if type_ == types::ANY {
        // Don't set instance_type.
    } else {
        out.insert("type".to_string(), shape_type_to_schema_type(type_));
    }

    if let Some(enum_values) = enum_ {
        out.insert(
            "enum".to_string(),
            serde_json::Value::Array(enum_values.into_iter().map(|v| v).collect()),
        );
    }

    // Metadata keywords.
    if let Some(t) = title {
        out.insert("title".to_string(), serde_json::json!(t));
    }
    if let Some(d) = description {
        out.insert("description".to_string(), serde_json::json!(d));
    }
    if let Some(d) = default {
        out.insert("default".to_string(), d.0);
    }

    // Object keywords.
    if type_.overlaps(types::OBJECT) {
        let ObjShape {
            properties,
            pattern_properties: patterns,
            additional_properties,
        } = object;

        let mut required = Vec::new();
        let mut properties_map = serde_json::Map::new();

        for ObjProperty {
            name,
            is_required,
            shape,
        } in properties
        {
            if is_required {
                required.push(serde_json::json!(name.clone()));
            }
            properties_map.insert(name.into(), to_sub_schema(shape).into());
        }

        if !properties_map.is_empty() {
            out.insert("properties".to_string(), serde_json::json!(properties_map));
        }
        if !required.is_empty() {
            out.insert("required".to_string(), serde_json::json!(required));
        }

        if !patterns.is_empty() {
            let mut pattern_properties = serde_json::Map::new();
            for ObjPattern { re, shape } in patterns {
                pattern_properties.insert(re.as_str().to_owned(), to_sub_schema(shape).into());
            }
            out.insert(
                "patternProperties".to_string(),
                serde_json::json!(pattern_properties),
            );
        }

        if let Some(addl_props) = additional_properties {
            out.insert(
                "additionalProperties".to_string(),
                to_sub_schema(*addl_props).into(),
            );
        }
    }

    // Array keywords.
    if type_.overlaps(types::ARRAY) {
        let ArrayShape {
            min_items,
            max_items,
            tuple,
            additional_items,
        } = array;

        if min_items != 0 {
            out.insert("minItems".to_string(), serde_json::json!(min_items));
        }
        if let Some(max) = max_items {
            out.insert("maxItems".to_string(), serde_json::json!(max));
        }

        if !tuple.is_empty() {
            let items: Vec<serde_json::Value> =
                tuple.into_iter().map(|s| to_sub_schema(s).into()).collect();
            out.insert("items".to_string(), serde_json::json!(items));

            if let Some(addl) = additional_items {
                out.insert("additionalItems".to_string(), to_sub_schema(*addl).into());
            }
        } else if let Some(addl) = additional_items {
            out.insert("items".to_string(), to_sub_schema(*addl).into());
        }
    }

    // String keywords.
    if type_.overlaps(types::STRING) {
        let StringShape {
            content_encoding,
            content_type,
            format,
            max_length,
            min_length,
        } = string;

        if let Some(encoding) = content_encoding {
            out.insert(
                keywords::CONTENT_ENCODING.to_string(),
                serde_json::json!(encoding),
            );
        }
        if let Some(content_type) = content_type {
            out.insert(
                keywords::CONTENT_MEDIA_TYPE.to_string(),
                serde_json::json!(content_type),
            );
        }
        if let Some(f) = format {
            out.insert("format".to_string(), serde_json::json!(f.to_string()));
        }
        if min_length != 0 {
            out.insert("minLength".to_string(), serde_json::json!(min_length));
        }
        if let Some(max) = max_length {
            out.insert("maxLength".to_string(), serde_json::json!(max));
        }
    }

    // Numeric keywords.
    if type_.overlaps(types::INT_OR_FRAC) {
        let NumericShape { minimum, maximum } = numeric;

        if let Some(num) = minimum {
            out.insert(keywords::MINIMUM.to_string(), num_to_value(num));
        }
        if let Some(num) = maximum {
            out.insert(keywords::MAXIMUM.to_string(), num_to_value(num));
        }
    }

    // Extensions.
    {
        if let Some(true) = secret {
            out.insert("secret".to_string(), serde_json::json!(true));
        }

        match reduce {
            Reduce::Unset | Reduce::Multiple => {}
            Reduce::Strategy(strategy) => {
                out.insert("reduce".to_string(), serde_json::json!(strategy));
            }
        }

        match redact {
            Redact::Unset | Redact::Multiple => {}
            Redact::Strategy(strategy) => {
                out.insert("redact".to_string(), serde_json::json!(strategy));
            }
        }

        out.extend(annotations.into_iter());
    }

    Schema::from(out)
}

fn shape_type_to_schema_type(type_set: types::Set) -> serde_json::Value {
    let v: Vec<&str> = type_set
        .iter()
        .map(|t| match t {
            "fractional" => "number",
            other => other,
        })
        .collect();

    if v.len() == 1 {
        serde_json::json!(v[0])
    } else {
        serde_json::json!(v)
    }
}

fn num_to_value(num: json::Number) -> serde_json::Value {
    use json::Number;
    match num {
        Number::Float(f) => serde_json::json!(f),
        Number::Unsigned(u) => serde_json::json!(u),
        Number::Signed(s) => serde_json::json!(s),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_type_conversions() {
        fn assert_equiv(expected: &str, actual: types::Set) {
            assert_eq!(
                serde_json::json!(expected),
                shape_type_to_schema_type(actual)
            );
        }

        assert_equiv("array", types::ARRAY);
        assert_equiv("boolean", types::BOOLEAN);
        assert_equiv("integer", types::INTEGER);
        assert_equiv("null", types::NULL);
        assert_equiv("number", types::INT_OR_FRAC);
        assert_equiv("object", types::OBJECT);
        assert_equiv("string", types::STRING);
    }

    #[test]
    fn test_round_trip() {
        let fixture = serde_json::json!({
            "$schema": "https://json-schema.org/draft/2019-09/schema",
            "type": "object",
            "properties": {
                "bool": {
                    "type": "boolean",
                    "secret": true
                },
                "an enum": {
                    "enum": [32, 64],
                    "type": "integer",
                    "minimum": -10
                },
                "str": {
                    "type": "string",
                    "minLength": 10,
                    "maxLength": 20,
                    "format": "uuid",
                    "contentEncoding": "base64",
                    "contentMediaType": "application/some.mime",
                    "reduce": {
                        "strategy": "append"
                    },
                    "redact": {
                        "strategy": "sha256"
                    }
                },
                "emptyObj": {
                    "type": "object",
                    "additionalProperties": false,
                    "x-some-extension": [42, "hi"],
                },
                "number": {
                    "type": "number",
                    "minimum": 20,
                    "maximum": 30.0,
                    "default": 25.4,
                    "reduce": {
                        "strategy": "sum",
                    }
                },
                "tuple": {
                    "type": "array",
                    "items": [{"type": "integer", "type": "string"}],
                    "additionalItems": {"type": "number"},
                    "minItems": 1,
                    "maxItems": 5,
                    "reduce": {
                        "strategy": "minimize",
                        "key": ["/1", "/0"]
                    }
                },
                "arr": {
                    "type": "array",
                    "items": {"type": "string"},
                },
                "emptyArray": {
                    "type": "array",
                    "items": false,
                },
                "any": {
                    "redact": {
                        "strategy": "block"
                    }
                },
            },
            "patternProperties": {
                "hello.*": {"type": "boolean"},
            },
            "required": [
                "emptyObj",
                "str",
            ],
            "reduce": {
                "strategy": "merge",
            }
        });

        let curi = url::Url::parse("flow://fixture").unwrap();
        let schema = crate::validation::build_schema(curi, &fixture).unwrap();
        let validator = crate::Validator::new(schema).unwrap();
        let shape = crate::Shape::infer(&validator.schemas()[0], validator.schema_index());
        let output = serde_json::to_value(to_schema(shape)).unwrap();

        // Remove the $schema field from the fixture for comparison
        // since to_schema now returns Schema instead of RootSchema
        let mut fixture_without_schema = fixture.clone();
        fixture_without_schema
            .as_object_mut()
            .unwrap()
            .remove("$schema");

        assert_eq!(fixture_without_schema, output);
    }
}
