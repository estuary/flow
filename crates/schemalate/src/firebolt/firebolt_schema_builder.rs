use std::iter::FromIterator;

use super::errors::*;
use super::firebolt_queries::{CreateTable, DropTable, InsertFromTable};
use super::firebolt_types::{Column, FireboltType, Table, TableSchema, TableType};

use json::schema::types;
use protocol::flow::MaterializationSpec;
use protocol::flow::{inference::Exists, materialization_spec::Binding};
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

pub fn build_firebolt_schema(binding: &Binding) -> Result<TableSchema, Error> {
    // TODO / question: is it safe to assume these are available when the driver sends them?
    let fs = binding
        .field_selection
        .clone()
        .ok_or(Error::FieldSelectionMissing)?;
    let projections = binding
        .collection
        .clone()
        .ok_or(Error::CollectionMissing)?
        .projections;

    let doc_field = if fs.document.len() > 0 {
        vec![fs.document]
    } else {
        vec![]
    };
    let keys = fs.keys.clone();
    let fields: Vec<String> = vec![fs.keys, fs.values, doc_field].concat();

    let mut columns = Vec::new();

    let errors = fields
        .iter()
        .map(|field| {
            let projection = projections.iter().find(|p| &p.field == field).unwrap();
            let inference = projection.inference.as_ref().unwrap();
            let is_key = keys.contains(field);
            let r#type = (types::Set::from_iter(inference.types.iter()) - types::NULL)
                .iter()
                .next()
                .unwrap();

            let fb_type = projection_type_to_firebolt_type(r#type).ok_or(Error::UnknownType {
                r#type: r#type.to_string(),
                field: field.clone(),
            })?;

            columns.push(Column {
                key: projection.field.clone(),
                r#type: fb_type,
                nullable: inference.exists != i32::from(Exists::Must)
                    || inference.types.contains(&"null".to_string()),
                is_key,
            });
            Ok(())
        })
        .filter(|r| r.is_err())
        .next();

    if let Some(err) = errors {
        // We have filtered on is_err, so this is a safe unwrap
        return Err(err.unwrap_err());
    }

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

fn projection_type_to_firebolt_type(projection_type: &str) -> Option<FireboltType> {
    match projection_type {
        "string" => Some(FireboltType::Text),
        "integer" => Some(FireboltType::Int),
        "number" => Some(FireboltType::Double),
        "boolean" => Some(FireboltType::Boolean),
        // TODO: how do we get the inner type of Arrays?
        "array" => Some(FireboltType::Array(Box::new(FireboltType::Text))),
        "object" => Some(FireboltType::Text),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use protocol::flow::{CollectionSpec, FieldSelection, Inference, Projection};
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
                        projections: vec![Projection {
                            field: "test".to_string(),
                            inference: Some(Inference {
                                types: vec!["string".to_string()],
                                exists: Exists::Must.into(),
                                ..Default::default()
                            }),
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
                        "INSERT INTO test_table (test,source_file_name) SELECT test,source_file_name FROM test_table_external WHERE source_file_name IN (?);".to_string()
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
                    projections: vec![Projection {
                        field: "test".to_string(),
                        inference: Some(Inference {
                            types: vec!["string".to_string()],
                            ..Default::default()
                        }),
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
                    projections: vec![Projection {
                        field: "test".to_string(),
                        inference: Some(Inference {
                            types: vec!["boolean".to_string()],
                            ..Default::default()
                        }),
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
                    projections: vec![Projection {
                        field: "test".to_string(),
                        inference: Some(Inference {
                            types: vec!["integer".to_string()],
                            ..Default::default()
                        }),
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
                    projections: vec![Projection {
                        field: "test".to_string(),
                        inference: Some(Inference {
                            types: vec!["number".to_string()],
                            ..Default::default()
                        }),
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
                    projections: vec![Projection {
                        field: "test".to_string(),
                        inference: Some(Inference {
                            types: vec!["object".to_string()],
                            ..Default::default()
                        }),
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
                    projections: vec![Projection {
                        field: "test".to_string(),
                        inference: Some(Inference {
                            types: vec!["string".to_string()],
                            exists: Exists::Must.into(),
                            ..Default::default()
                        }),
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
    }
}
