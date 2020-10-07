mod sql;

use self::sql::SqlMaterializationConfig;
use crate::catalog::{self, Collection, DB};
use crate::label_set;
use estuary_json::schema::types;
use estuary_protocol::consumer;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;
use std::path::Path;

#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct RuntimeConfig {
    pub collection: String,
    pub fields: Vec<RuntimeProjection>,
}

#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeProjection {
    pub field: String,
    pub location_ptr: String,
    pub primary_key: bool,
}

impl<'a> From<&'a FieldProjection> for RuntimeProjection {
    fn from(fp: &'a FieldProjection) -> RuntimeProjection {
        RuntimeProjection {
            field: fp.field_name.clone(),
            location_ptr: fp.location_ptr.clone(),
            primary_key: fp.is_primary_key,
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    InvalidProjections(#[from] ProjectionsError),

    #[error("catalog database error: {0}")]
    SQLiteErr(#[from] rusqlite::Error),

    // TODO: this is pretty ugly, but it seems better than movinng this whole materialization
    // module underneath catalog.
    #[error(transparent)]
    CatalogError(#[from] catalog::Error),

    #[error("Invalid target type '{0}' for materialization. Perhaps this catalog was created using a more recent version of flowctl?")]
    InvalidTargetType(String),
}

fn create_shard_spec(
    catalog_url: &str,
    collection_name: &str,
    target_name: &str,
    table_name: &str,
) -> consumer::ShardSpec {
    use crate::labels::{keys, values};
    let id = format!(
        "materialization/{}/{}/{}",
        collection_name, target_name, table_name
    );
    let shard_labels = label_set![
        keys::MANAGED_BY => values::FLOW,
        keys::CATALOG_URL => catalog_url,
        keys::COLLECTION => collection_name,
        keys::MATERIALIZATION_TARGET => target_name,
        keys::MATERIALIZATION_TABLE_NAME => table_name,
        keys::KEY_BEGIN => values::DEFAULT_KEY_BEGIN,
        keys::KEY_END => values::DEFAULT_KEY_END,
        keys::RCLOCK_BEGIN => values::DEFAULT_RCLOCK_BEGIN,
        keys::RCLOCK_END => values::DEFAULT_RCLOCK_END,
    ];
    consumer::ShardSpec {
        id,
        labels: Some(shard_labels),
        recovery_log_prefix: String::new(),
        hint_prefix: String::new(),
        hint_backups: 0,
        sources: Vec::new(),
        max_txn_duration: Some(prost_types::Duration {
            seconds: 1,
            nanos: 0,
        }),
        min_txn_duration: None,
        disable: false,
        hot_standbys: 0,
        disable_wait_for_ack: false,
    }
}

/// Returns the ApplyRequest to execute in order to create the Shard for this materialization.
pub fn create_shard_apply_request(
    catalog_url: &str,
    collection_name: &str,
    target_name: &str,
    table_name: &str,
) -> consumer::ApplyRequest {
    let change = consumer::apply_request::Change {
        upsert: Some(create_shard_spec(
            catalog_url,
            collection_name,
            target_name,
            table_name,
        )),
        expect_mod_revision: -1, // TODO (always update).
        delete: String::new(),
    };
    consumer::ApplyRequest {
        changes: vec![change],
        extension: Vec::new(),
    }
}

/// Takes the initializer, which has already been written to the file at `initializer_file`, and returns a
/// command that can be executed in order to apply that initializer to the target system. For
/// postgresql, this will return a `psql` invocation, and for sqlite it will use the `sqlite3` CLI.
pub fn create_apply_command(
    db: &DB,
    target: catalog::MaterializationTarget,
    table_name: &str,
    initializer_file: &Path,
) -> Result<Vec<String>, Error> {
    let config = MaterializationConfig::lookup(db, target)?;
    config.prepare_apply_command(db, target, table_name, initializer_file)
}

/// Returns the initialization string for materializing the collection to the given target system.
/// Currently systems all use SQL as the data definition language to get things setup. The term
/// "initializer" is used to encompass all types of data that may need to be applied to a target system
/// in order to prepare it to accept a materialized view. For example, for SAAS targets, this initializer
/// may be sent as an HTTP request body.
pub fn generate_target_initializer(
    db: &DB,
    target: catalog::MaterializationTarget,
    target_name: &str,
    table_name: &str,
    collection_name: &str,
    projections: &[FieldProjection],
) -> Result<String, Error> {
    let conf = MaterializationConfig::lookup(db, target)?;
    let target_uri = get_target_uri(db, target)?;
    let params = PayloadGenerationParameters {
        collection_name,
        target_name,
        target_type: conf.type_name(),
        target_uri: target_uri.as_str(),
        table_name,
        fields: projections,
        flow_document_field: FieldProjection::flow_document_column(),
    };
    let payload = conf.generate_target_initializer(params)?;
    Ok(payload)
}

fn get_target_uri(db: &DB, target: catalog::MaterializationTarget) -> Result<String, Error> {
    db.query_row(
        "SELECT target_uri FROM materialization_targets WHERE target_id = ?;",
        rusqlite::params![target.id],
        |r| r.get(0),
    )
    .map_err(Into::into)
}

fn get_target_type(db: &DB, target: catalog::MaterializationTarget) -> Result<String, Error> {
    let t: String = db.query_row(
        "select target_type from materialization_targets where target_id = ?;",
        rusqlite::params![target.id],
        |row| row.get(0),
    )?;
    Ok(t)
}

/// Returns the list of all projections for the given collection.
pub fn get_projections(db: &DB, collection: Collection) -> Result<Vec<FieldProjection>, Error> {
    let mut stmt = db.prepare_cached(
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
        .query(rusqlite::params![collection.id])?
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
struct TypesWrapper(pub types::Set);
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
enum MaterializationConfig {
    #[serde(rename = "postgres")]
    Postgres(SqlMaterializationConfig),
    #[serde(rename = "sqlite")]
    Sqlite(SqlMaterializationConfig),
}

const TARGET_TYPE_POSTGRES: &str = "postgres";
const TARGET_TYPE_SQLITE: &str = "sqlite";

impl MaterializationConfig {
    fn lookup(db: &DB, target: catalog::MaterializationTarget) -> Result<Self, Error> {
        let target_type = get_target_type(db, target)?;
        match target_type.as_str() {
            TARGET_TYPE_POSTGRES => Ok(MaterializationConfig::Postgres(
                SqlMaterializationConfig::postgres(),
            )),
            TARGET_TYPE_SQLITE => Ok(MaterializationConfig::Sqlite(
                SqlMaterializationConfig::sqlite(),
            )),
            _ => Err(Error::InvalidTargetType(target_type)),
        }
    }

    fn prepare_apply_command(
        &self,
        db: &DB,
        target: catalog::MaterializationTarget,
        _table_name: &str,
        initializer_file: &Path,
    ) -> Result<Vec<String>, Error> {
        let uri = get_target_uri(db, target)?;
        let cmd = match self {
            MaterializationConfig::Postgres(_) => vec![
                "psql".to_owned(),
                "--echo-all".to_owned(),
                format!("--file={}", initializer_file.display()),
                uri,
            ],
            MaterializationConfig::Sqlite(_) => vec![
                "sqlite3".to_owned(),
                "-echo".to_owned(),
                uri,
                format!(".read {}", initializer_file.display()),
            ],
        };
        Ok(cmd)
    }

    fn generate_target_initializer(
        &self,
        target: PayloadGenerationParameters,
    ) -> Result<String, ProjectionsError> {
        match self {
            MaterializationConfig::Postgres(sql_conf) => sql_conf.generate_ddl(target),
            MaterializationConfig::Sqlite(sql_conf) => sql_conf.generate_ddl(target),
        }
    }

    /// Returns the type of the materialization, which should match the type discriminant stored in
    /// the json itself.
    pub fn type_name(&self) -> &'static str {
        match self {
            MaterializationConfig::Postgres(_) => TARGET_TYPE_POSTGRES,
            MaterializationConfig::Sqlite(_) => TARGET_TYPE_SQLITE,
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

    // Returns the field projection for the complete Flow document. Every materialization will have
    // this column added automatically.
    pub fn flow_document_column() -> FieldProjection {
        FieldProjection {
            field_name: "flow_document".to_owned(),
            location_ptr: "/".to_owned(),
            user_provided: false,
            // TODO: actually, flow_document _could_ hold any object or array type. This is
            // theoretically OK for now, since OBJECT maps the the JSON column type for postgres,
            // which should accept any value type, but we should think about letting the sql ddl
            // generator handle a combined object|array type.
            types: types::OBJECT,
            must_exist: true,
            is_partition_key: false,
            is_primary_key: false,
            string_content_type: None,
            string_content_encoding_is_base64: false,
            string_max_length: None,
        }
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
pub struct PayloadGenerationParameters<'a> {
    pub collection_name: &'a str,
    pub target_name: &'a str,
    pub target_type: &'static str,
    pub target_uri: &'a str,
    pub table_name: &'a str,
    pub fields: &'a [FieldProjection],
    pub flow_document_field: FieldProjection,
}

impl<'a> PayloadGenerationParameters<'a> {
    fn get_runtime_config(&self) -> RuntimeConfig {
        let fields = self
            .fields
            .iter()
            .map(RuntimeProjection::from)
            .collect::<Vec<_>>();
        RuntimeConfig {
            collection: self.collection_name.to_owned(),
            fields,
        }
    }
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

