// This module maps a Shape into a representative JSON Schema.
use super::*;
use json::schema::{keywords, types};
use schemars::schema::{InstanceType, RootSchema, Schema, SchemaObject, SingleOrVec};

// TODO(johnny): This *probably* should be an impl Shape { into_schema(self) -> RootSchema }
// Consider refactoring as such if we're happy with this interface.
pub fn to_schema(shape: Shape) -> RootSchema {
    RootSchema {
        schema: to_sub_schema(shape).into_object(),
        meta_schema: schemars::gen::SchemaSettings::draft2019_09().meta_schema,
        ..Default::default()
    }
}

fn to_sub_schema(shape: Shape) -> Schema {
    let Shape {
        type_,
        enum_,
        title,
        description,
        reduction,
        provenance: _, // Not mapped to a schema.
        default,
        secret,
        annotations,
        array,
        numeric,
        object,
        string,
    } = shape;

    let mut out = SchemaObject::default();

    if type_ == types::INVALID {
        return Schema::Bool(false);
    } else if type_ == types::ANY {
        // Don't set instance_type.
    } else {
        out.instance_type = Some(shape_type_to_schema_type(type_));
    }

    out.enum_values = enum_;

    // Metadata keywords.
    {
        let out = out.metadata();

        out.title = title.map(Into::into);
        out.description = description.map(Into::into);
        out.default = default.map(|d| d.0);
    }

    // Object keywords.
    if type_.overlaps(types::OBJECT) {
        let ObjShape {
            properties,
            pattern_properties: patterns,
            additional_properties,
        } = object;

        let out = out.object();

        for ObjProperty {
            name,
            is_required,
            shape,
        } in properties
        {
            if is_required {
                out.required.insert(name.clone().into());
            }
            out.properties.insert(name.into(), to_sub_schema(shape));
        }

        for ObjPattern { re, shape } in patterns {
            out.pattern_properties
                .insert(re.as_str().to_owned(), to_sub_schema(shape));
        }

        out.additional_properties = additional_properties.map(|s| Box::new(to_sub_schema(*s)));
    }

    // Array keywords.
    if type_.overlaps(types::ARRAY) {
        let ArrayShape {
            min_items,
            max_items,
            tuple,
            additional_items,
        } = array;

        let out = out.array();

        out.min_items = if min_items != 0 {
            Some(min_items)
        } else {
            None
        };
        out.max_items = max_items;

        if !tuple.is_empty() {
            out.items = Some(SingleOrVec::Vec(
                tuple.into_iter().map(to_sub_schema).collect(),
            ));
            out.additional_items = additional_items.map(|s| Box::new(to_sub_schema(*s)));
        } else if let Some(addl) = additional_items {
            out.items = Some(SingleOrVec::Single(Box::new(to_sub_schema(*addl))));
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
            out.extensions.insert(
                keywords::CONTENT_ENCODING.to_string(),
                serde_json::json!(encoding),
            );
        }
        if let Some(content_type) = content_type {
            out.extensions.insert(
                keywords::CONTENT_MEDIA_TYPE.to_string(),
                serde_json::json!(content_type),
            );
        }
        out.format = format.map(|f| f.to_string());

        let out = out.string();

        out.min_length = if min_length != 0 {
            Some(min_length)
        } else {
            None
        };
        out.max_length = max_length;
    }

    // Numeric keywords.
    if type_.overlaps(types::INT_OR_FRAC) {
        let NumericShape { minimum, maximum } = numeric;

        // The schemars::SchemaObject::number() sub-type has minimum / maximum
        // fields, but they're f64. Use it's extension mechanism to pass-through
        // without loss.

        if let Some(num) = minimum {
            out.extensions
                .insert(keywords::MINIMUM.to_string(), num_to_value(num));
        }
        if let Some(num) = maximum {
            out.extensions
                .insert(keywords::MAXIMUM.to_string(), num_to_value(num));
        }
    }

    // Extensions.
    {
        if let Some(true) = secret {
            out.extensions
                .insert("secret".to_string(), serde_json::json!(true));
        }

        match reduction {
            Reduction::Unset | Reduction::Multiple => {}
            Reduction::Strategy(strategy) => {
                out.extensions
                    .insert("reduce".to_string(), serde_json::json!(strategy));
            }
        }

        out.extensions.extend(annotations.into_iter());
    }

    Schema::Object(out)
}

fn shape_type_to_schema_type(type_set: types::Set) -> SingleOrVec<InstanceType> {
    let mut v = type_set
        .iter()
        .map(parse_instance_type)
        .collect::<Vec<InstanceType>>();

    if v.len() == 1 {
        SingleOrVec::Single(Box::new(v.pop().unwrap()))
    } else {
        SingleOrVec::Vec(v)
    }
}

fn parse_instance_type(input: &str) -> InstanceType {
    match input {
        "array" => InstanceType::Array,
        "boolean" => InstanceType::Boolean,
        "fractional" => InstanceType::Number,
        "integer" => InstanceType::Integer,
        "null" => InstanceType::Null,
        "number" => InstanceType::Number,
        "object" => InstanceType::Object,
        "string" => InstanceType::String,
        other => panic!("unexpected type: {}", other),
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
        fn assert_equiv(expected: InstanceType, actual: types::Set) {
            assert_eq!(
                SingleOrVec::Single(Box::new(expected)),
                shape_type_to_schema_type(actual)
            );
        }

        assert_equiv(InstanceType::Array, types::ARRAY);
        assert_equiv(InstanceType::Boolean, types::BOOLEAN);
        assert_equiv(InstanceType::Integer, types::INTEGER);
        assert_equiv(InstanceType::Null, types::NULL);
        assert_equiv(InstanceType::Number, types::INT_OR_FRAC);
        assert_equiv(InstanceType::Object, types::OBJECT);
        assert_equiv(InstanceType::String, types::STRING);
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
                "any": {},
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

        assert_eq!(fixture, output);
    }
}
