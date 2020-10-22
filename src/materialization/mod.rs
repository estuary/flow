mod error;
mod selection_ui;
mod sql;

pub use self::error::{Error, ProjectionsError};
use self::selection_ui::interactive_select_projections;
use self::sql::SqlMaterializationConfig;
use crate::catalog::{self, DB};
use crate::label_set;
use estuary_json::schema::types;
use estuary_protocol::consumer;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;
use std::path::Path;

#[derive(Debug, PartialEq, Clone)]
pub struct CollectionInfo {
    pub collection_id: i64,
    pub resource_uri: String,
    pub name: String,
    pub schema_uri: String,
    pub key: Vec<String>,
    pub all_projections: Vec<FieldProjection>,
}

impl CollectionInfo {
    /// Returns the `CollectionInfo` for the collection with the given name, or an error if such a
    /// collection does not exist.
    pub fn lookup(db: &DB, collection_name: &str) -> Result<CollectionInfo, Error> {
        let (id, resource_uri, schema_uri, key_json): (i64, String, String, String) = db
            .query_row(
                "SELECT collection_id, resource_urls.url, schema_uri, key_json
            FROM collections
            NATURAL JOIN resource_urls
            WHERE collection_name = ? AND resource_urls.is_primary;",
                rusqlite::params![collection_name],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .map_err(|err| {
                if matches!(err, rusqlite::Error::QueryReturnedNoRows) {
                    Error::NoSuchCollection(collection_name.to_owned())
                } else {
                    Error::SQLiteErr(err)
                }
            })?;

        let key = serde_json::from_str(&key_json)?;
        let all_projections = get_all_projections(db, id)?;
        Ok(CollectionInfo {
            name: collection_name.to_owned(),
            collection_id: id,
            resource_uri,
            schema_uri,
            key,
            all_projections,
        })
    }

    pub fn validate_projected_fields(&self, projections: &[FieldProjection]) -> Result<(), Error> {
        // Verify that the provided fields include all of the locations in the collections key
        let mut missing_keys = self
            .key
            .iter()
            .filter(|key| !projections.iter().any(|p| *key == &p.location_ptr))
            .cloned()
            .collect::<Vec<_>>();

        if !missing_keys.is_empty() {
            // Deduplicate these location pointers, since we got them from `all_projections`, which may
            // contain multiple entries for the same location, which could result in a pretty confusing
            // error message.
            missing_keys.sort(); // sort is needed in order for dedup to remove all duplicates
            missing_keys.dedup();
            Err(Error::MissingCollectionKeys(missing_keys))
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct RuntimeConfig {
    pub collection: String,
    pub fields: Vec<RuntimeProjection>,
}

// TODO: get rid of this struct and just serialize FieldProjection instead
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

/// How to select the collection used for a materialization.
#[derive(Debug)]
pub enum CollectionSelection {
    Named(String),
    //InteractiveSelect,
}

/// How to select the fields used for a materialization.
#[derive(Debug)]
pub enum FieldSelection {
    /// Take the default selection of "all" projections. Technically, this selectes a subset of
    /// projections _if_ there are user_provided projections with different names than the
    /// auto-generated projections for the same locations.
    DefaultAll,

    /// Take only the set of projections matching the specific named fields, in the order provided.
    Named(Vec<String>),
    /// Have the user select fields interactively from a UI.
    InteractiveSelect,
}

/// Determines which collection to use for a materialization based on the given
/// `CollectionSelection`.
pub fn resolve_collection(
    db: &DB,
    selection: CollectionSelection,
) -> Result<CollectionInfo, Error> {
    match selection {
        CollectionSelection::Named(name) => CollectionInfo::lookup(db, &name),
    }
}

/// Determines a valid subset of projections to use for a materialization of the given collection, based on the given `FieldSelection`. If `FieldSelection::InteractiveSelect` is used, then this function will temporarily take over the terminal and may block indefinitely until the user has made their selections.
pub fn resolve_projections(
    collection: CollectionInfo,
    selection: FieldSelection,
) -> Result<Vec<FieldProjection>, Error> {
    log::debug!(
        "Resolving projections for collection '{}': {:?}",
        collection.name,
        selection
    );
    let resolved = match selection {
        FieldSelection::DefaultAll => resolve_default_all_projections(collection),
        FieldSelection::Named(fields) => resolve_named_projections(collection, fields)?,
        FieldSelection::InteractiveSelect => interactive_select_projections(collection)?,
    };
    Ok(resolved)
}

// Returns a list of projections named by `fields`. This will return an error if any names from
// `fields` do not exist as projections, or if the selected projections do not include all
// components of the collection's key. The returned list will be in the same order as the given
// `fields`, so that we will respect the order in which fields were provided by the user.
fn resolve_named_projections(
    collection: CollectionInfo,
    fields: Vec<String>,
) -> Result<Vec<FieldProjection>, Error> {
    let results = fields
        .into_iter()
        .map(|field| {
            collection
                .all_projections
                .iter()
                .find(|p| &p.field_name == &field)
                .cloned()
                .ok_or_else(|| Error::NoSuchField(field))
        })
        .collect::<Result<Vec<_>, Error>>()?;

    // Validate that the projections include all components of the collection's key
    collection.validate_projected_fields(results.as_slice())?;
    Ok(results)
}

/// Filters `all_projections` so that the final list will only contain a single projection per
/// `location_ptr`. Preference is always given to user_provided projections over those that were
/// generated automatically. This makes no guarantees about which field will be selected in the case
/// that there are multiple user_provided projections for the same field.
fn resolve_default_all_projections(collection: CollectionInfo) -> Vec<FieldProjection> {
    let mut by_location: BTreeMap<String, FieldProjection> = BTreeMap::new();

    for proj in collection.all_projections {
        let should_add = proj.user_provided
            || (!by_location.contains_key(&proj.location_ptr)
                && proj.types.is_single_scalar_type());
        if should_add {
            by_location.insert(proj.location_ptr.clone(), proj);
        }
    }

    let mut results = by_location.into_iter().map(|(_, v)| v).collect::<Vec<_>>();
    // Sort the resulting projections to put the collection primary keys at the beginning.
    // This is solely to enhance readability of any resulting tables or SQL. It does not affect
    // correctness in any way.
    results.sort_by_key(|p| !p.is_primary_key);
    results
}

/// Returns the list of all projections for the given collection.
fn get_all_projections(db: &DB, collection_id: i64) -> Result<Vec<FieldProjection>, Error> {
    let mut stmt = db.prepare_cached(
        "SELECT
            field,
            location_ptr,
            user_provided,
            types_json,
            must_exist,
            title,
            description,
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
                title: row.get(5)?,
                description: row.get(6)?,
                string_content_type: row.get(7)?,
                string_content_encoding_is_base64: row
                    .get::<usize, Option<bool>>(8)?
                    .unwrap_or_default(),
                string_max_length: row.get(9)?,
                is_partition_key: row.get(10)?,
                is_primary_key: row.get(11)?,
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

/// Information about a single projection, along with information that was inferred from the
/// schema.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FieldProjection {
    pub field_name: String,
    pub location_ptr: String,
    pub user_provided: bool,
    pub types: types::Set,
    pub must_exist: bool,
    pub title: Option<String>,
    pub description: Option<String>,

    pub is_partition_key: bool,
    pub is_primary_key: bool,

    pub string_content_type: Option<String>,
    pub string_content_encoding_is_base64: bool,
    pub string_max_length: Option<i64>,
}

impl FieldProjection {
    /// Returns true if this location may be null OR undefined (must_exist == false).
    pub fn is_nullable(&self) -> bool {
        (!self.must_exist) || self.types.overlaps(types::NULL)
    }

    /// Returns the field projection for the complete Flow document. Every materialization will have
    /// this column added automatically.
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
            title: Some("Flow Document".to_owned()),
            description: Some("The complete document, with all reductions applied".to_owned()),
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

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn collection_info_is_looked_up_by_name() {
        let expected = test_collection_info();

        let db = catalog::create(":memory:").unwrap();
        db.execute_batch(
            "INSERT INTO resources (resource_id, content_type, content, is_processed)
            VALUES (1, 'application/vnd.estuary.dev-catalog-spec+yaml', X'0ABC', TRUE);",
        )
        .unwrap();
        db.execute(
            "INSERT INTO resource_urls (resource_id, url, is_primary) VALUES (1, ?, TRUE)",
            rusqlite::params![expected.resource_uri],
        )
        .unwrap();
        db.execute(
            r#"INSERT INTO collections (collection_id, collection_name, schema_uri, key_json, resource_id)
            VALUES (1, ?, ?, '["/foo", "/bar"]', TRUE)"#,
            rusqlite::params![expected.name, expected.schema_uri],
        )
        .unwrap();

        for proj in expected.all_projections.iter() {
            let mut stmt = db
                .prepare_cached(
                    "INSERT INTO projections (collection_id, field, location_ptr, user_provided) VALUES (1, ?, ?, ?);",
                )
                .unwrap();
            stmt.execute(rusqlite::params![
                proj.field_name,
                proj.location_ptr,
                proj.user_provided
            ])
            .unwrap();
            // ignore duplicate inferences, since the projections contain some duplicates
            let mut stmt = db
                .prepare_cached(
                    "INSERT OR IGNORE INTO inferences (schema_uri, location_ptr, types_json, must_exist)
                         VALUES (?, ?, ?, ?)",
                )
                .unwrap();
            stmt.execute(rusqlite::params![
                expected.schema_uri,
                proj.location_ptr,
                proj.types.to_json_array(),
                proj.must_exist
            ])
            .unwrap();
        }

        let actual =
            CollectionInfo::lookup(&db, &expected.name).expect("failed to lookup collection");
        assert_eq!(expected.name, actual.name);
        assert_eq!(expected.schema_uri, actual.schema_uri);
        assert_eq!(expected.resource_uri, actual.resource_uri);
        assert_eq!(expected.collection_id, actual.collection_id);
        // verify projections ignoring order
        assert_eq!(expected.all_projections.len(), actual.all_projections.len());
        for expected_projection in expected.all_projections {
            assert!(
                actual.all_projections.contains(&expected_projection),
                "missing expected projection: {:#?}",
                &expected_projection
            );
        }
    }

    #[test]
    fn resolve_named_projections_returns_fields_in_order_when_all_valid() {
        let inputs = test_collection_info();
        let fields = vec![
            "userfoo".to_owned(),
            "bar".to_owned(),
            "gen/string".to_owned(),
            "user_mixed".to_owned(),
        ];
        let result = resolve_named_projections(inputs, fields.clone())
            .expect("failed to resolve named projections");
        let result_fields = result.into_iter().map(|p| p.field_name).collect::<Vec<_>>();
        assert_eq!(fields, result_fields);
    }

    #[test]
    fn resolve_named_projections_returns_error_when_primary_keys_are_not_specified() {
        let inputs = test_collection_info();
        let fields = vec!["gen/string".to_owned()];
        let result = resolve_named_projections(inputs, fields).expect_err("expected an error");
        let expected = vec!["/bar".to_owned(), "/foo".to_owned()];
        match result {
            Error::MissingCollectionKeys(missing) => assert_eq!(expected, missing),
            other => panic!("expected MissingCollectionKeys error, got: {:?}", other),
        }
    }

    #[test]
    fn resolve_named_projections_returns_error_when_field_does_not_exist() {
        let inputs = test_collection_info();
        let fields = vec![
            "userfoo".to_owned(),
            "bar".to_owned(),
            "naughty_field".to_owned(),
        ];
        let result = resolve_named_projections(inputs, fields).expect_err("expected an error");
        match result {
            Error::NoSuchField(field) => assert_eq!("naughty_field", field.as_str()),
            other => panic!("expected NoSuchField error, got: {:?}", other),
        }
    }

    #[test]
    fn resolve_default_all_projections_excludes_duplicate_locations() {
        let inputs = test_collection_info();
        // just compare the field names, since it results in more readable output
        let expected_fields = vec!["bar", "userfoo", "gen/string", "user_mixed", "user_object"];
        let actual = resolve_default_all_projections(inputs);
        let actual_fields = actual
            .iter()
            .map(|p| p.field_name.as_str())
            .collect::<Vec<_>>();
        assert_eq!(expected_fields, actual_fields);
    }

    fn test_collection_info() -> CollectionInfo {
        let projections = vec![
            // included because user provided, even though it has mixed types
            projection(
                "user_mixed",
                "/user/mixed",
                types::INTEGER | types::STRING,
                true,
                false,
            ),
            // excluded because it has mixed types
            projection(
                "gen/mixed",
                "/gen/mixed",
                types::INTEGER | types::STRING,
                false,
                false,
            ),
            // excluded because not user provided and not scalar
            projection("gen_object", "/gen/object", types::OBJECT, false, false),
            // included because it's user provided
            projection("user_object", "/user/object", types::OBJECT, true, false),
            // included because it has a scalar type
            projection("gen/string", "/gen/string", types::STRING, false, false),
            // excluded because it's not a scalar type
            projection("gen/obj", "/gen/obj", types::OBJECT, false, false),
            // userfoo will take precedence
            projection("foo", "/foo", types::INTEGER, false, true),
            // included because it's user provided AND a primary key
            projection("userfoo", "/foo", types::INTEGER, true, true),
            // included because it's a primary key
            projection("bar", "/bar", types::STRING, false, true),
        ];
        CollectionInfo {
            collection_id: 1,
            name: String::from("testCollection"),
            resource_uri: String::from("test://the/test/flow.yaml"),
            schema_uri: String::from("test://test/schema.json"),
            all_projections: projections,
            key: vec![String::from("/foo"), String::from("/bar")],
        }
    }

    fn projection(
        field: &str,
        location: &str,
        types: types::Set,
        user_provided: bool,
        is_primary_key: bool,
    ) -> FieldProjection {
        FieldProjection {
            field_name: field.to_owned(),
            location_ptr: location.to_owned(),
            user_provided,
            is_primary_key,
            types,
            must_exist: is_primary_key,
            title: None,
            description: None,
            is_partition_key: false,
            string_content_type: None,
            string_content_encoding_is_base64: false,
            string_max_length: None,
        }
    }
}
