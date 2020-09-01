mod sql;

use self::sql::SqlMaterializationConfig;
use crate::catalog::{self, Collection, Scope};
use crate::specs::build as specs;
use estuary_json::schema::types;
use rusqlite::params;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;

pub struct Materialization {
    pub id: i64,
}

pub fn generate_all_ddl(scope: &Scope, collections: &[Collection]) -> catalog::Result<()> {
    for collection in collections {
        let fields = get_field_projections(scope, collection)?;

        let mut stmt = scope.db.prepare_cached(
            "SELECT collection_name, materialization_id, materialization_name, target_uri, table_name, config_json
            FROM collections NATURAL JOIN materializations
            WHERE collection_id = ?",
        )?;
        let mut rows = stmt.query(params![collection.id])?;
        while let Some(row) = rows.next()? {
            let collection_name: String = row.get(0)?;
            let materialization_id: i64 = row.get(1)?;
            let materialization_name: String = row.get(2)?;
            let target_uri: String = row.get(3)?;
            let table_name: String = row.get(4)?;
            let config_json: String = row.get(5)?;

            let materailization_config: MaterializationConfig =
                serde_json::from_str(config_json.as_str())?;

            let target = MaterializationTarget {
                collection_name,
                materialization_name,
                target_uri,
                table_name,
                target_type: materailization_config.type_name(),
                fields: fields.as_slice(),
            };
            let ddl = materailization_config.generate_ddl(target)?;

            scope.db.execute(
                "INSERT INTO materialization_ddl (materialization_id, ddl) VALUES (?, ?)",
                rusqlite::params![materialization_id, ddl],
            )?;
        }
    }
    Ok(())
}

fn get_field_projections(
    scope: &Scope,
    collection: &Collection,
) -> catalog::Result<Vec<FieldProjection>> {
    let mut stmt = scope.db.prepare_cached(
        "SELECT
            field,
            location_ptr,
            user_provided,
            types_json,
            string_content_type,
            string_content_encoding_is_base64,
            string_max_length,
            is_partition_key,
            is_primary_key
        FROM schema_extracted_fields
        WHERE collection_id = ?;",
    )?;
    let fields = stmt
        .query(rusqlite::params![collection.id])?
        .and_then(|row| {
            Ok(FieldProjection {
                field_name: row.get(0)?,
                location_ptr: row.get(1)?,
                user_provided: row.get(2)?,
                types: row.get::<usize, TypesWrapper>(3)?.0,
                string_content_type: row.get(4)?,
                string_content_encoding_is_base64: row
                    .get::<usize, Option<bool>>(5)?
                    .unwrap_or_default(),
                string_max_length: row.get(6)?,
                is_partition_key: row.get(7)?,
                is_primary_key: row.get(8)?,
            })
        })
        .collect::<catalog::Result<Vec<_>>>()?;
    Ok(fields)
}

struct TypesWrapper(types::Set);
impl rusqlite::types::FromSql for TypesWrapper {
    fn column_result(val: rusqlite::types::ValueRef) -> rusqlite::types::FromSqlResult<Self> {
        match val {
            rusqlite::types::ValueRef::Text(bytes) => {
                let type_names: Vec<&'_ str> = serde_json::from_slice(bytes)
                    .map_err(|err| rusqlite::types::FromSqlError::Other(Box::new(err)))?;
                let mut types = types::INVALID;
                for name in type_names {
                    let ty = types::Set::for_type_name(name)
                        .ok_or(rusqlite::types::FromSqlError::InvalidType)?;
                    types = types | ty;
                }
                Ok(TypesWrapper(types))
            }
            _ => Err(rusqlite::types::FromSqlError::InvalidType),
        }
    }
}

impl Materialization {
    pub fn register(
        scope: &Scope,
        collection: Collection,
        name: &str,
        spec: &specs::Materialization,
    ) -> catalog::Result<Materialization> {
        let conf = MaterializationConfig::from_spec(spec);
        let conf_json = serde_json::to_string(&conf)?;
        let conn = match spec {
            specs::Materialization::Postgres { connection } => connection,
            specs::Materialization::Sqlite { connection } => connection,
        };

        let mut stmt = scope.db.prepare_cached(
            "INSERT INTO materializations (
                materialization_name,
                collection_id,
                target_type,
                target_uri,
                table_name,
                config_json
            ) VALUES (?, ?, ?, ?, ?, ?);",
        )?;
        let params = rusqlite::params![
            name,
            collection.id,
            conf.type_name(),
            conn.uri,
            conn.table,
            conf_json,
        ];
        stmt.execute(params)?;

        let id = scope.db.last_insert_rowid();
        Ok(Materialization { id })
    }
}

/// Materialization configurations are expected to be different for different types of systems.
/// This enum is intended to represent all of the possible shapes and allow serialization as json,
/// which is how this is stored in the catalog database. This configuration is intended to live in
/// the code. It is persisted in order to provide visibility, not as a means of externalizing it.
#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum MaterializationConfig {
    #[serde(rename = "postgres")]
    Postgres(SqlMaterializationConfig),
    #[serde(rename = "sqlite")]
    Sqlite(SqlMaterializationConfig),
}

impl MaterializationConfig {
    pub fn generate_ddl(&self, target: MaterializationTarget) -> Result<String, ProjectionsError> {
        match self {
            MaterializationConfig::Postgres(sql_conf) => sql_conf.generate_ddl(target),
            MaterializationConfig::Sqlite(sql_conf) => sql_conf.generate_ddl(target),
        }
    }

    /// Returns the type of the materialization, which should match the type discriminant stored in
    /// the json itself.
    pub fn type_name(&self) -> &'static str {
        match self {
            MaterializationConfig::Postgres(_) => "postgres",
            MaterializationConfig::Sqlite(_) => "sqlite",
        }
    }

    pub fn from_spec(spec: &specs::Materialization) -> MaterializationConfig {
        match spec {
            specs::Materialization::Postgres { .. } => {
                MaterializationConfig::Postgres(SqlMaterializationConfig::postgres())
            }
            specs::Materialization::Sqlite { .. } => {
                MaterializationConfig::Sqlite(SqlMaterializationConfig::sqlite())
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct FieldProjection {
    pub field_name: String,
    pub location_ptr: String,
    pub user_provided: bool,
    pub types: types::Set,

    pub is_partition_key: bool,
    pub is_primary_key: bool,

    pub string_content_type: Option<String>,
    pub string_content_encoding_is_base64: bool,
    pub string_max_length: Option<i64>,
}

impl FieldProjection {
    pub fn is_nullable(&self) -> bool {
        self.types & types::NULL != types::INVALID
    }
}

impl fmt::Display for FieldProjection {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let source = if self.user_provided {
            "user provided"
        } else {
            "automatically generated"
        };
        let primary_key = if self.is_primary_key {
            ", primary key"
        } else {
            ""
        };
        write!(
            f,
            "field_name: '{}', location_ptr: '{}', possible_types: [{}], source: {}{}",
            self.field_name, self.location_ptr, self.types, source, primary_key
        )
    }
}

#[derive(Debug)]
pub struct MaterializationTarget<'a> {
    pub collection_name: String,
    pub materialization_name: String,
    pub target_type: &'static str,
    pub target_uri: String,
    pub table_name: String,
    pub fields: &'a [FieldProjection],
}

#[derive(Debug)]
pub struct ProjectionsError {
    materialization_type: &'static str,
    naughty_projections: BTreeMap<String, Vec<FieldProjection>>,
}
impl ProjectionsError {
    fn empty(materialization_type: &'static str) -> ProjectionsError {
        ProjectionsError {
            materialization_type,
            naughty_projections: BTreeMap::new(),
        }
    }

    fn is_empty(&self) -> bool {
        !self
            .naughty_projections
            .values()
            .any(|naughty| !naughty.is_empty())
    }
}

const MAX_PROJECTION_ERROR_MSGS: usize = 5;

impl fmt::Display for ProjectionsError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "There are projections that are incompatible with the materialization of type '{}':",
            self.materialization_type
        )?;
        for (reason, naughty) in self.naughty_projections.iter() {
            writeln!(f, "{}:", reason)?;

            for field in naughty.iter().take(MAX_PROJECTION_ERROR_MSGS) {
                writeln!(f, "\t{}", field)?;
            }
            if naughty.len() > MAX_PROJECTION_ERROR_MSGS {
                writeln!(
                    f,
                    "\t...and {} more projections",
                    naughty.len() - MAX_PROJECTION_ERROR_MSGS
                )?;
            }
        }
        Ok(())
    }
}
impl std::error::Error for ProjectionsError {}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn sql_ddl_is_generated_for_all_materializations() {
        let db = catalog::open(":memory:").unwrap();
        catalog::init_db_schema(&db).unwrap();

        db.execute_batch(r##"
            INSERT INTO resources (resource_id, content_type, content, is_processed) VALUES
                    (1, 'application/vnd.estuary.dev-catalog-spec+yaml', X'1234', TRUE),
                    (10, 'application/schema+yaml', CAST('true' AS BLOB), TRUE),
                    (20, 'application/vnd.estuary.dev-catalog-fixtures+yaml', CAST('[1, 2, 3]' AS BLOB), TRUE);

            INSERT INTO resource_urls (resource_id, url, is_primary) VALUES
                                (1, 'test://example/spec', TRUE),
                                (10, 'test://example/schema.json', TRUE),
                                (20, 'test://example/fixtures.json', TRUE);

            INSERT INTO collections
                (collection_id, collection_name, schema_uri, key_json, resource_id, default_projections_max_depth)
            VALUES
                (1, 'testCollection', 'test://example/schema.json', '["/a", "/b"]', 1, 3);

            INSERT INTO projections (collection_id, field, location_ptr, user_provided) VALUES
                (1, 'field_a', '/a', TRUE),
                (1, 'field_b', '/b', TRUE),
                (1, 'field_c', '/c', FALSE),
                (1, 'field_d', '/d', FALSE),
                (1, 'field_e', '/e', FALSE);

            INSERT INTO partitions (collection_id, field) VALUES
                (1, 'field_a'),
                (1, 'field_b');

            INSERT INTO inferences (collection_id, field, types_json, string_content_encoding_is_base64, string_max_length)
            VALUES
                (1, 'field_a', '["integer"]', NULL, NULL),
                (1, 'field_b', '["string"]', FALSE, 32),
                (1, 'field_c', '["null", "string"]', TRUE, NULL),
                (1, 'field_d', '["null", "object"]', NULL, NULL),
                (1, 'field_e', '["null", "number"]', NULL, NULL);
            "##).unwrap();
        let pg_config = serde_json::to_string(&MaterializationConfig::Postgres(
            SqlMaterializationConfig::postgres(),
        ))
        .unwrap();
        let sqlite_config = serde_json::to_string(&MaterializationConfig::Sqlite(
            SqlMaterializationConfig::sqlite(),
        ))
        .unwrap();
        db.execute(
            "INSERT INTO materializations (materialization_id, materialization_name, collection_id, target_type, target_uri, table_name, config_json)
            VALUES
                (1, 'pg_mat_test', 1, 'postgres', 'postgresql:foo:bar@pg.test/mydb', 'test_pg_table', ?),
                (2, 'sqlite_mat_test', 1, 'sqlite', 'file:///testsqlitedburi', 'test_sqlite_table', ?);",
            rusqlite::params![pg_config, sqlite_config]
            ).unwrap();

        let scope = Scope::empty(&db);
        let collection = &[Collection {
            id: 1,
            resource: catalog::Resource { id: 1 },
        }];
        generate_all_ddl(&scope, collection).expect("failed to generate ddl");

        let pg_ddl = db
            .query_row(
                "select ddl from materialization_ddl where materialization_id = 1",
                rusqlite::NO_PARAMS,
                |r| r.get::<usize, String>(0),
            )
            .expect("no postgres ddl was generated");
        let sqlite_ddl = db
            .query_row(
                "select ddl from materialization_ddl where materialization_id = 2",
                rusqlite::NO_PARAMS,
                |r| r.get::<usize, String>(0),
            )
            .expect("no sqlite ddl was generated");
        insta::assert_snapshot!("gen_ddl_test_postgres", pg_ddl);
        insta::assert_snapshot!("gen_ddl_test_sqlite", sqlite_ddl);
    }
}
