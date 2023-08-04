use crate::inference::Shape;
use json::schema::{
    keywords,
    types::{self, Set},
};
use schemars::{
    gen::SchemaGenerator,
    schema::{InstanceType, RootSchema, Schema, SchemaObject, SingleOrVec},
};
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Default)]
pub struct SchemaBuilder {
    shape: Shape,
}

impl SchemaBuilder {
    pub fn new(shape: Shape) -> Self {
        Self { shape }
    }

    pub fn root_schema(&self) -> RootSchema {
        RootSchema {
            schema: to_schema(&self.shape).into_object(),
            meta_schema: SchemaGenerator::default().settings().meta_schema.clone(),
            ..Default::default()
        }
    }
}

pub fn to_schema(shape: &Shape) -> Schema {
    let mut schema_object = SchemaObject {
        instance_type: Some(shape_type_to_schema_type(shape.type_)),
        ..Default::default()
    };

    schema_object.metadata().title = shape.title.clone();
    schema_object.metadata().description = shape.description.clone();
    schema_object.metadata().default = shape.default.clone().map(|(d, _)| d);
    schema_object.enum_values = shape.enum_.clone();

    if shape.type_.overlaps(types::OBJECT) {
        let mut prop_schemas = BTreeMap::new();
        let mut required = BTreeSet::new();
        for obj_prop in shape.object.properties.iter() {
            prop_schemas.insert(obj_prop.name.clone(), to_schema(&obj_prop.shape));
            if obj_prop.is_required {
                required.insert(obj_prop.name.clone());
            }
        }
        let object = &mut schema_object.object();
        object.properties = prop_schemas;
        object.required = required;

        if let Some(addl) = &shape.object.additional {
            object.additional_properties = Some(Box::new(to_schema(addl)));
        }
    }

    if shape.type_.overlaps(types::ARRAY) {
        let mut array_items = Vec::new();
        for item in shape.array.tuple.iter() {
            array_items.push(to_schema(item));
        }
        if array_items.len() > 0 {
            schema_object.array().items = Some(flatten(array_items));
        }

        if let Some(addl_items) = &shape.array.additional {
            schema_object.array().additional_items = Some(Box::new(to_schema(addl_items)));
        }

        schema_object.array().max_items = shape.array.max.and_then(|max| u32::try_from(max).ok());
        schema_object.array().min_items = shape.array.min.and_then(|max| u32::try_from(max).ok());
    }

    if shape.type_.overlaps(types::STRING) {
        schema_object.format = shape.string.format.map(|f| f.to_string());
        schema_object.string().max_length = shape
            .string
            .max_length
            .and_then(|max| u32::try_from(max).ok());

        if shape.string.min_length > 0 {
            schema_object.string().min_length = shape.string.min_length.try_into().ok();
        }
        if let Some(encoding) = &shape.string.content_encoding {
            schema_object
                .extensions
                .insert(keywords::CONTENT_ENCODING.to_string(), json!(encoding));
        }
        if let Some(content_type) = &shape.string.content_type {
            schema_object.extensions.insert(
                keywords::CONTENT_MEDIA_TYPE.to_string(),
                json!(content_type),
            );
        }
    }

    Schema::Object(schema_object)
}

pub fn shape_type_to_schema_type(type_set: Set) -> SingleOrVec<InstanceType> {
    let instance_types = type_set
        .iter()
        .map(parse_instance_type)
        .collect::<Vec<InstanceType>>();

    flatten(instance_types)
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

fn flatten<T>(mut vec: Vec<T>) -> SingleOrVec<T> {
    if vec.len() == 1 {
        SingleOrVec::Single(Box::new(vec.pop().unwrap()))
    } else {
        SingleOrVec::Vec(vec)
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
}
