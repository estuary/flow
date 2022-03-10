use super::errors::*;
use super::firebolt_types::{BasicType, Column, FireboltType, Table};

use doc::inference::{ArrayShape, Shape};
use doc::Annotation;
use json::schema::{self, index::IndexBuilder, types};
use serde_json::Value;

pub const FAKE_BUNDLE_URL: &str = "https://fake-bundle-schema.estuary.io";

pub fn build_firebolt_schema(schema: &Value) -> Result<Table, Error> {
    let schema_uri =
        url::Url::parse(FAKE_BUNDLE_URL).expect("parse should not fail on hard-coded url");

    let schema = schema::build::build_schema::<Annotation>(schema_uri, &schema)?;

    let mut index = IndexBuilder::new();
    index.add(&schema)?;
    index.verify_references()?;
    let index = index.into_index();
    let shape = Shape::infer(&schema, &index);

    build_table(&shape)
}

fn build_table(shape: &Shape) -> Result<Table, Error> {
    if !shape.type_.overlaps(types::OBJECT) {
        return Err(Error::UnsupportedError {
            message: UNSUPPORTED_NON_OBJECT_ROOT,
            shape: Box::new(shape.clone()),
        });
    }
    let root = &shape.object;
    if !root.additional.is_none() {
        return Err(Error::UnsupportedError {
            message: UNSUPPORTED_OBJECT_ADDITIONAL_FIELDS,
            shape: Box::new(root.clone()),
        });
    }

    if root.properties.is_empty() {
        return Err(Error::UnsupportedError {
            message: UNSUPPORTED_MULTIPLE_OR_UNSPECIFIED_TYPES,
            shape: Box::new(shape.clone()),
        });
    }

    let mut columns: Vec<Column> = Vec::new();
    for prop in &root.properties {
        let typ = build_from_shape(&prop.shape)?;
        if matches!(typ, FireboltType::Array(_)) && !prop.is_required {
            return Err(Error::UnsupportedError {
                message: UNSUPPORTED_NULLABLE_ARRAY,
                shape: Box::new(prop.clone()),
            });
        }
        columns.push(Column {
            key: prop.name.clone(),
            typ: typ,
            nullable: !prop.is_required,
        });
    }

    Ok(Table { columns })
}

fn build_from_shape(shape: &Shape) -> Result<FireboltType, Error> {
    let mut fields = Vec::new();

    if shape.type_.overlaps(types::ARRAY) {
        fields.push(FireboltType::Array(Box::new(build_from_array(
            &shape.array,
        )?)));
    }
    if shape.type_.overlaps(types::BOOLEAN) {
        fields.push(FireboltType::Basic(BasicType::Boolean));
    }
    if shape.type_.overlaps(types::FRACTIONAL) {
        fields.push(FireboltType::Basic(BasicType::Double));
    } else if shape.type_.overlaps(types::INTEGER) {
        fields.push(FireboltType::Basic(BasicType::Int));
    }
    if shape.type_.overlaps(types::STRING) {
        fields.push(FireboltType::Basic(BasicType::Text));
    }

    if fields.len() == 1 {
        // pop will not return None b/c len = 1.
        Ok(fields.pop().unwrap())
    } else {
        Err(Error::UnsupportedError {
            message: UNSUPPORTED_MULTIPLE_OR_UNSPECIFIED_TYPES,
            shape: Box::new(shape.clone()),
        })
    }
}

fn build_from_array(shape: &ArrayShape) -> Result<FireboltType, Error> {
    if !shape.tuple.is_empty() {
        Err(Error::UnsupportedError {
            message: UNSUPPORTED_TUPLE,
            shape: Box::new(shape.clone()),
        })
    } else {
        match &shape.additional {
            None => Err(Error::UnsupportedError {
                message: UNSUPPORTED_MULTIPLE_OR_UNSPECIFIED_TYPES,
                shape: Box::new(shape.clone()),
            }),
            Some(shape) => build_from_shape(shape),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn check_schema_error_error(actual_error: &Error, expected_error_message: &str) {
        assert!(matches!(actual_error, Error::UnsupportedError { .. }));
        if let Error::UnsupportedError { message, shape: _ } = actual_error {
            println!("{}", message);
            println!("{}", expected_error_message);
            assert!(message.contains(expected_error_message))
        }
    }

    #[test]
    fn test_build_firebolt_schema_with_error() {
        let empty_schema_json = json!({});
        check_schema_error_error(
            &build_firebolt_schema(&empty_schema_json).unwrap_err(),
            UNSUPPORTED_MULTIPLE_OR_UNSPECIFIED_TYPES,
        );

        let multiple_types_schema_json = json!({"type": ["integer", "string"]});
        check_schema_error_error(
            &build_firebolt_schema(&multiple_types_schema_json).unwrap_err(),
            UNSUPPORTED_NON_OBJECT_ROOT,
        );

        let int_schema_json = json!({"type": "integer"});
        check_schema_error_error(
            &build_firebolt_schema(&int_schema_json).unwrap_err(),
            UNSUPPORTED_NON_OBJECT_ROOT,
        );

        let simple_array_schema_json = json!({"type": "array", "items": {"type": "string"}});
        check_schema_error_error(
            &build_firebolt_schema(&simple_array_schema_json).unwrap_err(),
            UNSUPPORTED_NON_OBJECT_ROOT,
        );

        let multiple_field_types_schema_json = json!({"type": "object", "properties": { "mul_type": {"type": ["boolean", "integer"] } } });
        check_schema_error_error(
            &build_firebolt_schema(&multiple_field_types_schema_json).unwrap_err(),
            UNSUPPORTED_MULTIPLE_OR_UNSPECIFIED_TYPES,
        );

        let object_additional_field_schema_json = json!(
          {"type": "object", "additionalProperties": {"type": "integer"}, "properties": {"int": {"type": "integer"}}}
        );
        check_schema_error_error(
            &build_firebolt_schema(&object_additional_field_schema_json).unwrap_err(),
            UNSUPPORTED_OBJECT_ADDITIONAL_FIELDS,
        );

        let tuple_field_schema_json = json!({"type": "object", "properties": {"arr": {"type": "array", "items": [{"type": "string"}, {"type": "integer"}]}}});
        check_schema_error_error(
            &build_firebolt_schema(&tuple_field_schema_json).unwrap_err(),
            UNSUPPORTED_TUPLE,
        );

        let optional_array_schema_json = json!({"type": "object", "properties": {"arr": {"type": "array", "items": {"type": "string"}}}});
        check_schema_error_error(
            &build_firebolt_schema(&optional_array_schema_json).unwrap_err(),
            UNSUPPORTED_NULLABLE_ARRAY,
        );
    }

    #[test]
    fn test_build_firebolt_schema_all_types() {
        // TODO(mahdi): how do we handle objects in Firebolt? There are various ways. One possible way:
        // https://docs.firebolt.io/working-with-semi-structured-data/mapping-json-to-table.html#corresponding-firebolt-table-structure
        // Or just store it as a raw JSON in a TEXT field?
        // "array_of_objs": {"type": "array", "items": {"type": "object", "properties": {"arr_field": {"type": "string"}}}},
        // "nested": {"type": "object", "required": [], "properties": {"nested_field": {"type": ["null", "integer"]}}}
        let schema_json = json!(
        {
            "properties":{
                "str": {"type": "string"},
                "str_or_null": {"type": ["string", "null"] },
                "int": {"type": "integer"},
                "int_or_null": {"type": ["integer", "null"] },
                "num": {"type": "number"},
                "num_or_null": {"type": ["number", "null"] },
                "bool": {"type": "boolean"},
                "bool_or_null": {"type": ["boolean", "null"]},
                "enum": {"enum": [1i32,2i32,3i32]},
                "array_of_ints": {"type": "array", "items": {"type": "integer"}},

            },
            "required":["str", "int", "num", "bool", "enum", "array_of_ints"],
            "type":"object"
        });

        let actual = build_firebolt_schema(&schema_json).unwrap();
        assert_eq!(
            actual.to_string(),
            "array_of_ints ARRAY(INT),bool BOOLEAN,bool_or_null BOOLEAN NULL,enum INT,int INT,int_or_null INT NULL,num DOUBLE,num_or_null DOUBLE NULL,str TEXT,str_or_null TEXT NULL"
        );
    }

    #[test]
    fn test_build_firebolt_schema_with_reference() {
        let schema_json = json!({
            "$defs": {
                "__flowInline1":{
                    "$defs":{
                        "anAnchor": {
                            "$anchor": "AnAnchor",
                            "type":"integer"
                        }
                    },
                    "$id": "test://example/int-string.schema",
                    "properties": {
                        "bit": { "type": "boolean" },
                        "int": { "type": "integer" },
                        "str": { "type": "string" }
                    },
                    "required": ["int", "str", "bit"],
                    "type": "object"
                }
            },
            "$id": "test://example/int-string-len.schema",
            "$ref": "test://example/int-string.schema",
            "properties": {
                "arr":{
                    "items":{"$ref": "int-string.schema#AnAnchor"},
                    "type":"array"
                },
                "len":{"type": "integer"}
            },
            "required":["len", "arr"]
        });

        let actual = build_firebolt_schema(&schema_json).unwrap();

        assert_eq!(
            actual.to_string(),
            "arr ARRAY(INT),bit BOOLEAN,int INT,len INT,str TEXT"
        );
    }
}
