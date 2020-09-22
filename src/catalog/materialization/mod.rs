mod sql;

use self::sql::SqlMaterializationConfig;
use crate::catalog::{self, Collection, Scope};
use crate::specs::build as specs;
use estuary_json::schema::types;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;

pub struct Materialization {
    pub id: i64,
}

impl Materialization {
    pub fn register(
        scope: &Scope,
        materialization_name: &str,
        spec: &specs::Materialization,
    ) -> catalog::Result<Materialization> {
        let collection = scope.push_prop("collection").then(|scope| {
            Ok(Collection::get_by_name(scope, spec.collection.as_str())?)
        })?;
        let conf = MaterializationConfig::from_spec(&spec.config);
        let conf_json = serde_json::to_string(&conf)?;
        let conn = match &spec.config {
            specs::MaterializationConfig::Postgres(connection) => connection,
            specs::MaterializationConfig::Sqlite(connection) => connection,
        };
        let fields = get_field_projections(scope, collection.id)?;
        let target = MaterializationTarget {
            materialization_name,
            collection_name: spec.collection.as_str(),
            target_uri: conn.uri.as_str(),
            table_name: conn.table.as_str(),
            target_type: conf.type_name(),
            fields: fields.as_slice(),
        };
        let ddl = conf.generate_ddl(target)?;

        let mut stmt = scope.db.prepare_cached(
            "INSERT INTO materializations (
                materialization_name,
                collection_id,
                target_type,
                target_uri,
                table_name,
                config_json,
                ddl
            ) VALUES (?, ?, ?, ?, ?, ?, ?);",
        )?;
        let params = rusqlite::params![
            materialization_name,
            collection.id,
            conf.type_name(),
            conn.uri,
            conn.table,
            conf_json,
            ddl,
        ];
        stmt.execute(params)?;

        let id = scope.db.last_insert_rowid();
        Ok(Materialization { id })
    }
}

fn get_field_projections(
    scope: &Scope,
    collection_id: i64,
) -> catalog::Result<Vec<FieldProjection>> {
    let mut stmt = scope.db.prepare_cached(
        "SELECT
            field,
            location_ptr,
            user_provided,
            types_json,
            must_exist,
            string_content_type,
            string_content_encoding_is_base64,
            string_max_length,
            is_partition_key,
            is_primary_key
        FROM projected_fields
        WHERE collection_id = ?;",
    )?;
    let fields = stmt
        .query(rusqlite::params![collection_id])?
        .and_then(|row| {
            Ok(FieldProjection {
                field_name: row.get(0)?,
                location_ptr: row.get(1)?,
                user_provided: row.get(2)?,
                types: row.get::<usize, TypesWrapper>(3)?.0,
                must_exist: row.get(4)?,
                string_content_type: row.get(5)?,
                string_content_encoding_is_base64: row
                    .get::<usize, Option<bool>>(6)?
                    .unwrap_or_default(),
                string_max_length: row.get(7)?,
                is_partition_key: row.get(8)?,
                is_primary_key: row.get(9)?,
            })
        })
        .collect::<catalog::Result<Vec<_>>>()?;
    Ok(fields)
}

#[derive(Debug)]
pub struct TypesWrapper(pub types::Set);
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

    pub fn from_spec(spec: &specs::MaterializationConfig) -> MaterializationConfig {
        match spec {
            specs::MaterializationConfig::Postgres { .. } => {
                MaterializationConfig::Postgres(SqlMaterializationConfig::postgres())
            }
            specs::MaterializationConfig::Sqlite { .. } => {
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
    pub must_exist: bool,

    pub is_partition_key: bool,
    pub is_primary_key: bool,

    pub string_content_type: Option<String>,
    pub string_content_encoding_is_base64: bool,
    pub string_max_length: Option<i64>,
}

impl FieldProjection {
    pub fn is_nullable(&self) -> bool {
        (!self.must_exist) || self.types.overlaps(types::NULL)
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
    pub collection_name: &'a str,
    pub materialization_name: &'a str,
    pub target_type: &'static str,
    pub target_uri: &'a str,
    pub table_name: &'a str,
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
    use crate::catalog::DB;
    use serde_json::json;

    #[test]
    fn sql_ddl_is_generated_for_postgres_materialization() {
        let db = setup();
        let pg_materialization = serde_json::from_value(json!({
            "collection": "testCollection",
            "postgres": {
                "uri": "postgres://foo.test:5432/testdb",
                "table": "pg_test_table"
            }
        }))
        .unwrap();

        let scope = Scope::for_test(&db, 1);
        Materialization::register(&scope, "test_pg_materialization", &pg_materialization)
            .expect("failed to register materialization");

        let actual = catalog::dump_table(&db, "materializations").unwrap();
        insta::assert_yaml_snapshot!(actual);
    }

    #[test]
    fn sql_ddl_is_generated_for_sqlite_materialization() {
        let db = setup();
        let pg_materialization = serde_json::from_value(json!({
            "collection": "testCollection",
            "sqlite": {
                "uri": "file:///tmp/test/sqlite/materialization",
                "table": "sqlite_test_table"
            }
        }))
        .unwrap();

        let scope = Scope::for_test(&db, 1);
        Materialization::register(&scope, "test_pg_materialization", &pg_materialization)
            .expect("failed to register materialization");

        let actual = catalog::dump_table(&db, "materializations").unwrap();
        insta::assert_yaml_snapshot!(actual);
    }

    fn setup() -> DB {
        let db = catalog::open(":memory:").unwrap();
        catalog::init_db_schema(&db).unwrap();

        db.execute_batch(r##"
            INSERT INTO resources (resource_id, content_type, content, is_processed) VALUES
                    (1, 'application/vnd.estuary.dev-catalog-spec+yaml', X'1234', TRUE),
                    (10, 'application/schema+yaml', CAST('true' AS BLOB), TRUE);

            INSERT INTO resource_urls (resource_id, url, is_primary) VALUES
                                (1, 'test://example/spec', TRUE),
                                (10, 'test://example/schema.json', TRUE);

            INSERT INTO collections
                (collection_id, collection_name, schema_uri, key_json, resource_id)
            VALUES
                (1, 'testCollection', 'test://example/schema.json', '["/a", "/b"]', 1);

            INSERT INTO projections (collection_id, field, location_ptr, user_provided) VALUES
                (1, 'field_a', '/a', TRUE),
                (1, 'field_b', '/b', TRUE),
                (1, 'field_c', '/c', FALSE),
                (1, 'field_d', '/d', FALSE),
                (1, 'field_e', '/e', FALSE);

            INSERT INTO partitions (collection_id, field) VALUES
                (1, 'field_a'),
                (1, 'field_b');

            INSERT INTO inferences (schema_uri, location_ptr, types_json, must_exist, string_content_encoding_is_base64, string_max_length)
            VALUES
                ('test://example/schema.json', '/a', '["integer"]', TRUE, NULL, NULL),
                ('test://example/schema.json', '/b', '["string"]', FALSE, FALSE, 32),
                ('test://example/schema.json', '/c', '["null", "string"]', TRUE, TRUE, NULL),
                ('test://example/schema.json', '/d', '["null", "object"]', FALSE, NULL, NULL),
                ('test://example/schema.json', '/e', '["null", "number"]', FALSE, NULL, NULL);
        "##).unwrap();
        db
    }
}
