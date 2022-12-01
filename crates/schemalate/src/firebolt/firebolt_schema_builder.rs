use super::errors::*;
use super::firebolt_queries::{CreateTable, DropTable, InsertFromTable};
use super::firebolt_types::{Column, FireboltType, Table, TableSchema, TableType};

use doc::inference::Shape;
use doc::{Annotation, Pointer};
use json::schema::{self, types};
use protocol::flow::materialization_spec::Binding;
use protocol::flow::MaterializationSpec;
use serde::{Deserialize, Serialize};

pub const FAKE_BUNDLE_URL: &str = "https://fake-bundle-schema.estuary.io";

#[derive(Serialize, PartialEq, Debug)]
pub struct BindingBundle {
    pub create_table: String,
    pub create_external_table: String,
    pub drop_table: String,
    pub drop_external_table: String,
    pub insert_from_table: String,
}

#[derive(Serialize, PartialEq, Debug)]
pub struct FireboltQueriesBundle {
    bindings: Vec<BindingBundle>,
}

#[derive(Deserialize, Debug)]
pub struct EndpointConfig {
    aws_key_id: String,
    aws_secret_key: String,
    s3_bucket: String,
    s3_prefix: String,
}

#[derive(Deserialize)]
pub struct Resource {
    pub table: String,
    pub table_type: String,
}

fn build_shape_from_schema(schema_str: &str) -> Result<Shape, Error> {
    let schema_uri =
        url::Url::parse("https://estuary.dev").expect("parse should not fail on hard-coded url");

    let parsed_schema = serde_json::from_str(schema_str)?;
    let schema = schema::build::build_schema::<Annotation>(schema_uri, &parsed_schema)?;

    let mut index = schema::index::IndexBuilder::new();
    index.add(&schema)?;
    index.verify_references()?;
    let index = index.into_index();

    Ok(Shape::infer(&schema, &index))
}

pub fn build_firebolt_schema(binding: &Binding) -> Result<TableSchema, Error> {
    let fs = binding.field_selection.as_ref().unwrap();
    let projections = &binding.collection.as_ref().unwrap().projections;
    let schema_str = &binding.collection.as_ref().unwrap().write_schema_json;

    let doc_field = if fs.document.len() > 0 {
        vec![fs.document.clone()]
    } else {
        vec![]
    };
    let fields: Vec<String> = vec![fs.keys.clone(), fs.values.clone(), doc_field].concat();

    let mut columns = Vec::new();
    let schema_shape = build_shape_from_schema(schema_str)?;

    fields.iter().try_for_each(|field| -> Result<(), Error> {
        let projection = projections.iter().find(|p| &p.field == field).unwrap();
        let is_key = fs.keys.contains(field);
        let (shape, exists) = schema_shape.locate(&Pointer::from_str(&projection.ptr));

        let fb_type = projection_type_to_firebolt_type(shape).ok_or(Error::UnknownType {
            r#type: shape.type_.to_string(),
            field: field.clone(),
        })?;

        columns.push(Column {
            key: projection.field.clone(),
            r#type: fb_type,
            nullable: !exists.must() || shape.type_.overlaps(types::NULL),
            is_key,
        });
        Ok(())
    })?;

    Ok(TableSchema { columns })
}

pub fn build_firebolt_queries_bundle(
    spec: MaterializationSpec,
) -> Result<FireboltQueriesBundle, Error> {
    let config: EndpointConfig = serde_json::from_str(&spec.endpoint_spec_json)?;

    let bindings : Result<Vec<BindingBundle>, Error> = spec.bindings.iter().map(|binding| {
        let resource: Resource = serde_json::from_str(&binding.resource_spec_json)?;
        let mut schema = build_firebolt_schema(binding)?;

        let external_table_name = format!("{}_external", resource.table);
        let external_table = Table {
            name: external_table_name.clone(),
            r#type: TableType::External,
            schema: schema.clone(),
        };

        // Add source_file_name column to main table
        schema.columns.push(Column {
            key: "source_file_name".to_string(),
            r#type: FireboltType::Text,
            is_key: false,
            nullable: false,
        });

        let table = Table {
            name: resource.table.clone(),
            r#type: resource.table_type.into(),
            schema: schema.clone(),
        };

        Ok(BindingBundle {
            create_table: CreateTable {
                table: &table,
                if_not_exists: true,
                extra: "",
            }
            .to_string(),
            create_external_table: CreateTable {
                table: &external_table,
                if_not_exists: true,
                extra: format!(
                    "CREDENTIALS = ( AWS_KEY_ID = '{}' AWS_SECRET_KEY = '{}' ) URL = 's3://{}{}' OBJECT_PATTERN = '*.json' TYPE = (JSON)",
                    config.aws_key_id,
                    config.aws_secret_key,
                    config.s3_bucket,
                    config.s3_prefix,
                ).as_str()
            }.to_string(),
            drop_table: DropTable {
                table: &table
            }.to_string(),
            drop_external_table: DropTable {
                table: &external_table
            }.to_string(),
            insert_from_table: InsertFromTable {
                destination: &table,
                source_name: &external_table_name,
            }.to_string(),
        })
    }).collect();

    Ok(FireboltQueriesBundle {
        bindings: bindings?,
    })
}

fn projection_type_to_firebolt_type(shape: &Shape) -> Option<FireboltType> {
    if shape.type_.overlaps(types::STRING) {
        Some(FireboltType::Text)
    } else if shape.type_.overlaps(types::ARRAY) && matches!(shape.array.additional, Some(_)) {
        let inner_type = projection_type_to_firebolt_type(shape.array.additional.as_ref()?)?;
        Some(FireboltType::Array(Box::new(inner_type)))
    } else if shape.type_.overlaps(types::BOOLEAN) {
        Some(FireboltType::Boolean)
    } else if shape.type_.overlaps(types::FRACTIONAL) {
        Some(FireboltType::Double)
    } else if shape.type_.overlaps(types::INTEGER) {
        Some(FireboltType::Int)

    // We store objects as stringified JSON objects
    } else if shape.type_.overlaps(types::OBJECT) {
        Some(FireboltType::Text)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use protocol::flow::{CollectionSpec, FieldSelection, Projection};
    use serde_json::json;

    #[test]
    fn test_build_firebolt_queries_bundle() {
        assert_eq!(
            build_firebolt_queries_bundle(MaterializationSpec {
                endpoint_spec_json: json!({
                    "aws_key_id": "aws_key",
                    "aws_secret_key": "aws_secret",
                    "s3_bucket": "my-bucket",
                    "s3_prefix": "/test"
                }).to_string(),
                bindings: vec![Binding {
                    resource_spec_json: json!({
                        "table": "test_table",
                        "table_type": "fact"
                    }).to_string(),
                    field_selection: Some(FieldSelection {
                        keys: vec!["test".to_string()],
                        ..Default::default()
                    }),
                    collection: Some(CollectionSpec {
                        write_schema_json: json!({
                            "properties": {
                                "test": {"type": "string"},
                            },
                            "required": ["test"],
                            "type": "object"
                        }).to_string(),
                        projections: vec![Projection {
                            field: "test".to_string(),
                            ptr: "/test".to_string(),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            })
            .unwrap(),
            FireboltQueriesBundle {
                bindings: vec![BindingBundle {
                    create_table:
                        "CREATE FACT TABLE IF NOT EXISTS test_table (test TEXT,source_file_name TEXT) PRIMARY INDEX test ;"
                            .to_string(),
                    create_external_table:
                        "CREATE EXTERNAL TABLE IF NOT EXISTS test_table_external (test TEXT)  CREDENTIALS = ( AWS_KEY_ID = 'aws_key' AWS_SECRET_KEY = 'aws_secret' ) URL = 's3://my-bucket/test' OBJECT_PATTERN = '*.json' TYPE = (JSON);".to_string(),
                    drop_table: "DROP TABLE test_table;".to_string(),
                    drop_external_table: "DROP TABLE test_table_external;".to_string(),
                    insert_from_table:
                        "INSERT INTO test_table (test,source_file_name) SELECT test,source_file_name FROM test_table_external WHERE source_file_name IN (?) AND ((SELECT count(*) FROM test_table WHERE source_file_name IN (?)) < 1);".to_string()
                }]
            },
        );
    }

    #[test]
    fn test_build_firebolt_schema() {
        assert_eq!(
            build_firebolt_schema(&Binding {
                field_selection: Some(FieldSelection {
                    keys: vec!["test".to_string()],
                    ..Default::default()
                }),
                collection: Some(CollectionSpec {
                    write_schema_json: json!({
                        "properties": {
                            "test": {"type": "string"},
                        }
                    })
                    .to_string(),
                    projections: vec![Projection {
                        field: "test".to_string(),
                        ptr: "/test".to_string(),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
                ..Default::default()
            })
            .unwrap(),
            TableSchema {
                columns: vec![Column {
                    key: "test".to_string(),
                    r#type: FireboltType::Text,
                    nullable: true,
                    is_key: true,
                }],
            },
        );

        assert_eq!(
            build_firebolt_schema(&Binding {
                field_selection: Some(FieldSelection {
                    keys: vec!["test".to_string()],
                    ..Default::default()
                }),
                collection: Some(CollectionSpec {
                    write_schema_json: json!({
                        "properties": {
                            "test": {"type": "boolean"},
                        }
                    })
                    .to_string(),
                    projections: vec![Projection {
                        field: "test".to_string(),
                        ptr: "/test".to_string(),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
                ..Default::default()
            })
            .unwrap(),
            TableSchema {
                columns: vec![Column {
                    key: "test".to_string(),
                    r#type: FireboltType::Boolean,
                    nullable: true,
                    is_key: true,
                }],
            },
        );

        assert_eq!(
            build_firebolt_schema(&Binding {
                field_selection: Some(FieldSelection {
                    values: vec!["test".to_string()],
                    ..Default::default()
                }),
                collection: Some(CollectionSpec {
                    write_schema_json: json!({
                        "properties": {
                            "test": {"type": "integer"},
                        }
                    })
                    .to_string(),
                    projections: vec![Projection {
                        field: "test".to_string(),
                        ptr: "/test".to_string(),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
                ..Default::default()
            })
            .unwrap(),
            TableSchema {
                columns: vec![Column {
                    key: "test".to_string(),
                    r#type: FireboltType::Int,
                    nullable: true,
                    is_key: false,
                }],
            },
        );

        assert_eq!(
            build_firebolt_schema(&Binding {
                field_selection: Some(FieldSelection {
                    keys: vec!["test".to_string()],
                    ..Default::default()
                }),
                collection: Some(CollectionSpec {
                    write_schema_json: json!({
                        "properties": {
                            "test": {"type": "number"},
                        }
                    })
                    .to_string(),
                    projections: vec![Projection {
                        field: "test".to_string(),
                        ptr: "/test".to_string(),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
                ..Default::default()
            })
            .unwrap(),
            TableSchema {
                columns: vec![Column {
                    key: "test".to_string(),
                    r#type: FireboltType::Double,
                    nullable: true,
                    is_key: true,
                }],
            },
        );

        assert_eq!(
            build_firebolt_schema(&Binding {
                field_selection: Some(FieldSelection {
                    keys: vec!["test".to_string()],
                    ..Default::default()
                }),
                collection: Some(CollectionSpec {
                    write_schema_json: json!({
                        "properties": {
                            "test": {"type": "string"},
                        },
                        "required": ["test"],
                        "type": "object"
                    })
                    .to_string(),
                    projections: vec![Projection {
                        field: "test".to_string(),
                        ptr: "/test".to_string(),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
                ..Default::default()
            })
            .unwrap(),
            TableSchema {
                columns: vec![Column {
                    key: "test".to_string(),
                    r#type: FireboltType::Text,
                    nullable: false,
                    is_key: true,
                }],
            },
        );

        assert_eq!(
            build_firebolt_schema(&Binding {
                field_selection: Some(FieldSelection {
                    keys: vec!["test".to_string()],
                    ..Default::default()
                }),
                collection: Some(CollectionSpec {
                    write_schema_json: json!({
                        "properties": {
                            "test": {
                                "type": "array",
                                "items": {
                                    "type": "string"
                                }
                            },
                        },
                        "required": ["test"],
                        "type": "object"
                    })
                    .to_string(),
                    projections: vec![Projection {
                        field: "test".to_string(),
                        ptr: "/test".to_string(),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
                ..Default::default()
            })
            .unwrap(),
            TableSchema {
                columns: vec![Column {
                    key: "test".to_string(),
                    r#type: FireboltType::Array(Box::new(FireboltType::Text)),
                    nullable: false,
                    is_key: true,
                }],
            },
        );

        assert_eq!(
            build_firebolt_schema(&Binding {
                field_selection: Some(FieldSelection {
                    keys: vec!["obj".to_string()],
                    ..Default::default()
                }),
                collection: Some(CollectionSpec {
                    write_schema_json: json!({
                        "properties": {
                            "obj": {
                                "type": "object",
                                "properties": {
                                    "name": {
                                        "type": "string"
                                    }
                                },
                                "required": ["name"]
                            },
                        },
                        "type": "object",
                        "required": ["obj"]
                    })
                    .to_string(),
                    projections: vec![Projection {
                        field: "obj".to_string(),
                        ptr: "/obj".to_string(),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
                ..Default::default()
            })
            .unwrap(),
            TableSchema {
                columns: vec![Column {
                    key: "obj".to_string(),
                    r#type: FireboltType::Text,
                    nullable: false,
                    is_key: true,
                }],
            },
        );
    }
}
