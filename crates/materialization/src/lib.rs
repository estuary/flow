mod error;
mod selection_ui;
mod sql;

pub use self::error::{Error, NaughtyProjections};
use self::selection_ui::interactive_select_projections;
use self::sql::SqlMaterializationConfig;
use json::schema::types;
use protocol::consumer;
use protocol::flow::{CollectionSpec, Inference, Projection};
use rusqlite::Connection as DB;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt::Display;
use std::path::Path;

fn validate_projected_fields(
    collection: &CollectionSpec,
    projections: &[Projection],
) -> Result<(), Error> {
    // Verify that the provided fields include all of the locations in the collections key
    let mut missing_keys = collection
        .key_ptrs
        .iter()
        .filter(|key| !projections.iter().any(|p| *key == &p.ptr))
        .cloned()
        .collect::<Vec<_>>();

    if !missing_keys.is_empty() {
        // Deduplicate these location pointers, since we got them from `projections`, which may
        // contain multiple entries for the same location, which could result in a pretty confusing
        // error message.
        missing_keys.sort(); // sort is needed in order for dedup to remove all duplicates
        missing_keys.dedup();
        Err(Error::MissingCollectionKeys(missing_keys))
    } else {
        Ok(())
    }
}

pub fn lookup_collection(db: &DB, collection_name: &str) -> Result<CollectionSpec, Error> {
    // This call provides a nicer error message when the collection isn't found, which is why we're
    // doing this with two queries rather than joining in a single query.
    let collection = catalog::Collection::get_by_name(db, collection_name)?;
    let collection_json = db.query_row(
        "SELECT spec_json FROM collections_json WHERE collection_id = ?;",
        rusqlite::params![collection.id],
        |r| r.get::<usize, String>(0),
    )?;
    let spec = serde_json::from_str(collection_json.as_str())?;
    Ok(spec)
}

fn create_shard_spec(
    catalog_url: &str,
    collection_name: &str,
    target_name: &str,
    table_name: &str,
) -> consumer::ShardSpec {
    use labels::{keys, label_set, values};
    let id = materialization_shard_id(
        collection_name,
        target_name,
        table_name,
        values::DEFAULT_KEY_BEGIN,
        values::DEFAULT_RCLOCK_BEGIN,
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

fn materialization_shard_id(
    collection_name: impl Display,
    target_name: impl Display,
    table_name: impl Display,
    key_range_begin: impl Display,
    rclock_range_begin: impl Display,
) -> String {
    format!(
        "materialization/{}/{}/{}/{}-{}",
        collection_name, target_name, table_name, key_range_begin, rclock_range_begin,
    )
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
    collection: &CollectionSpec,
) -> Result<String, Error> {
    let conf = MaterializationConfig::lookup(db, target)?;
    let target_uri = get_target_uri(db, target)?;
    let params = PayloadGenerationParameters {
        target_name,
        collection,
        target_type: conf.type_name(),
        target_uri: target_uri.as_str(),
        table_name,
        flow_document_field: flow_document_projection(),
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
) -> Result<CollectionSpec, Error> {
    match selection {
        CollectionSelection::Named(name) => lookup_collection(db, &name),
    }
}

/// Determines a valid subset of projections to use for a materialization of the given collection, based on the given `FieldSelection`. If `FieldSelection::InteractiveSelect` is used, then this function will temporarily take over the terminal and may block indefinitely until the user has made their selections.
pub fn resolve_projections(
    collection: CollectionSpec,
    selection: FieldSelection,
) -> Result<Vec<Projection>, Error> {
    log::debug!(
        "Resolving projections for collection '{}': {:?}",
        collection.name,
        selection
    );
    let resolved = match selection {
        FieldSelection::DefaultAll => resolve_default_projections(collection),
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
    collection: CollectionSpec,
    fields: Vec<String>,
) -> Result<Vec<Projection>, Error> {
    let results = fields
        .into_iter()
        .map(|field| {
            collection
                .projections
                .iter()
                .find(|p| &p.field == &field)
                .cloned()
                .ok_or_else(|| Error::NoSuchField(field))
        })
        .collect::<Result<Vec<_>, Error>>()?;

    // Validate that the projections include all components of the collection's key
    validate_projected_fields(&collection, results.as_slice())?;
    Ok(results)
}

/// Filters `projections` so that the final list will only contain a single projection per
/// `location_ptr`. Preference is always given to user_provided projections over those that were
/// generated automatically. This makes no guarantees about which field will be selected in the case
/// that there are multiple user_provided projections for the same field.
fn resolve_default_projections(collection: CollectionSpec) -> Vec<Projection> {
    let mut by_location: BTreeMap<String, Projection> = BTreeMap::new();

    for proj in collection.projections {
        if proj.user_provided
            || (!by_location.contains_key(&proj.ptr) && is_single_scalar_type(&proj))
        {
            by_location.insert(proj.ptr.clone(), proj);
        }
    }

    let mut results = by_location.into_iter().map(|(_, v)| v).collect::<Vec<_>>();
    // Sort the resulting projections to put the collection primary keys at the beginning.
    // This is solely to enhance readability of any resulting tables or SQL. It does not affect
    // correctness in any way.
    results.sort_by_key(|p| !p.is_primary_key);
    results
}

fn is_single_scalar_type(projection: &Projection) -> bool {
    projection
        .inference
        .as_ref()
        .map(|inf| {
            inf.types
                .iter()
                .collect::<types::Set>()
                .is_single_scalar_type()
        })
        .unwrap_or_default()
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
                // Without '-v ON_ERROR_STOP=1', psql will exit with 0 even when there's an error
                // in the sql. Setting this is required in order to handle any errors from psql.
                "-v".to_owned(),
                "ON_ERROR_STOP=1".to_owned(),
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
    ) -> Result<String, NaughtyProjections> {
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

pub fn flow_document_projection() -> Projection {
    Projection {
        field: "flow_document".to_owned(),
        ptr: String::new(), // empty string is the root document pointer
        user_provided: false,
        inference: Some(Inference {
            // TODO: actually, flow_document _could_ hold any object or array type. This is
            // theoretically OK for now, since OBJECT maps the the JSON column type for postgres,
            // which should accept any value type, but we should think about letting the sql ddl
            // generator handle a combined object|array type.
            types: types::OBJECT.to_vec(),
            must_exist: true,
            title: "Flow Document".to_owned(),
            description: "The complete document, with all reductions applied".to_owned(),
            string: None,
        }),
        is_partition_key: false,
        is_primary_key: false,
    }
}

#[derive(Debug)]
pub struct PayloadGenerationParameters<'a> {
    pub target_name: &'a str,
    pub target_type: &'static str,
    pub target_uri: &'a str,
    pub table_name: &'a str,
    pub collection: &'a CollectionSpec,
    pub flow_document_field: Projection,
}

#[cfg(test)]
mod test {
    use super::*;
    use protocol::flow::inference;

    #[test]
    fn collection_spec_is_looked_up_by_name() {
        let expected = test_collection_info();

        let db = catalog::create(":memory:").unwrap();
        db.execute_batch(
            "INSERT INTO resources (resource_id, content_type, content, is_processed)
            VALUES (1, 'application/vnd.estuary.dev-catalog-spec+yaml', X'0ABC', TRUE);",
        )
        .unwrap();

        db.execute(
            r#"INSERT INTO collections (collection_id, collection_name, schema_uri, key_json, resource_id)
            VALUES (1, ?, ?, '["/foo", "/bar"]', TRUE)"#,
            rusqlite::params![expected.name, expected.schema_uri],
        )
        .unwrap();

        for proj in expected.projections.iter() {
            let mut stmt = db
                .prepare_cached(
                    "INSERT INTO projections (collection_id, field, location_ptr, user_provided) VALUES (1, ?, ?, ?);",
                )
                .unwrap();
            stmt.execute(rusqlite::params![proj.field, proj.ptr, proj.user_provided])
                .unwrap();
            // ignore duplicate inferences, since the projections contain some duplicates
            let mut stmt = db
                .prepare_cached(
                    "INSERT OR IGNORE INTO inferences (schema_uri, location_ptr, types_json, must_exist)
                         VALUES (?, ?, ?, ?)",
                )
                .unwrap();
            let inference = proj.inference.as_ref().unwrap();
            let types_json = serde_json::to_string(&inference.types).unwrap();
            stmt.execute(rusqlite::params![
                expected.schema_uri,
                proj.ptr,
                types_json,
                inference.must_exist,
            ])
            .unwrap();
        }

        let actual = lookup_collection(&db, &expected.name).expect("failed to lookup collection");
        println!("actual: {:#?}", actual);
        assert_eq!(expected.name, actual.name);
        assert_eq!(expected.schema_uri, actual.schema_uri);
        // verify projections ignoring order
        assert_eq!(expected.projections.len(), actual.projections.len());
        for expected_projection in expected.projections {
            assert!(
                actual.projections.contains(&expected_projection),
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
        let result_fields = result.into_iter().map(|p| p.field).collect::<Vec<_>>();
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
    fn resolve_default_projections_excludes_duplicate_locations() {
        let inputs = test_collection_info();
        // just compare the field names, since it results in more readable output
        let expected_fields = vec!["bar", "userfoo", "gen/string", "user_mixed", "user_object"];
        let actual = resolve_default_projections(inputs);
        let actual_fields = actual.iter().map(|p| p.field.as_str()).collect::<Vec<_>>();
        assert_eq!(expected_fields, actual_fields);
    }

    fn test_collection_info() -> CollectionSpec {
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
        CollectionSpec {
            projections,
            name: String::from("testCollection"),
            schema_uri: String::from("test://test/schema.json"),
            key_ptrs: vec![String::from("/foo"), String::from("/bar")],
            uuid_ptr: String::new(),
            journal_spec: None,
            partition_fields: Vec::new(),
            ack_json_template: Vec::new(),
        }
    }

    fn projection(
        field: &str,
        location: &str,
        types: types::Set,
        user_provided: bool,
        is_primary_key: bool,
    ) -> Projection {
        let string = if types.overlaps(types::STRING) {
            Some(inference::String {
                content_type: String::new(),
                format: String::new(),
                max_length: 0,
                is_base64: false,
            })
        } else {
            None
        };
        Projection {
            field: field.to_owned(),
            ptr: location.to_owned(),
            user_provided,
            is_primary_key,
            inference: Some(Inference {
                types: types.to_vec(),
                must_exist: is_primary_key,
                title: String::new(),
                description: String::new(),
                string,
            }),
            is_partition_key: false,
        }
    }
}
