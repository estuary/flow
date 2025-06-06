#[macro_use]
mod macros;
mod behaviors;
mod dependencies;

use std::str::FromStr;

use macros::*;

// Re-exports for users of this crate.
pub use itertools::EitherOrBoth;
pub use macros::{Row, Table};

#[cfg(feature = "persist")]
pub use macros::{load_tables, persist_tables, SqlTableObj};
#[cfg(feature = "persist")]
use prost::Message;

mod built;
mod draft;
mod live;
pub mod utils;
pub use built::{BuiltRow, Validations};
pub use dependencies::Dependencies;
pub use draft::{DraftCatalog, DraftRow};
pub use live::{CatalogResolver, LiveCatalog, LiveRow};

tables!(
    table Fetches (row Fetch, sql "fetches") {
        // Import depth of this fetch.
        key depth: u32,
        // Fetched resource Url.
        key resource: url::Url,
    }

    table Resources (row Resource, sql "resources") {
        // Url of this resource.
        key resource: url::Url,
        // Content-type of this resource.
        val content_type: proto_flow::flow::ContentType,
        // Byte content of this resource.
        val content: bytes::Bytes,
        // Document content of this resource.
        val content_dom: models::RawValue,
    }

    table Imports (row Import, sql "imports") {
        // Scope is the referring resource and fragment location which caused the import.
        key scope: url::Url,
        // Resource which is imported. Never has a fragment.
        key to_resource: url::Url,
    }

    table StorageMappings (row StorageMapping, sql "storage_mappings") {
        // Catalog prefix to which this storage mapping applies.
        key catalog_prefix: models::Prefix,
        // Control-plane ID of this storage mapping.
        val control_id: models::Id,
        // Stores for journal fragments under this prefix.
        val stores: Vec<models::Store>,
    }

    table InferredSchemas (row InferredSchema, sql "inferred_schemas") {
        // Collection which this inferred schema reflects.
        key collection_name: models::Collection,
        // Inferred schema of the collection.
        val schema: models::Schema,
        // MD5 content sum of `schema`.
        val md5: String,
    }

    table DataPlanes (row #[derive(Clone)] DataPlane, sql "data_planes") {
        // Control-plane identifier for this data-plane.
        key control_id: models::Id,
        // Name of this data-plane under the catalog namespace.
        // This is used for authorization and not much else.
        val data_plane_name: String,
        // Unique and fully-qualified domain name of this data-plane.
        val data_plane_fqdn: String,
        // When true, this DataPlane is to be used for created specifications.
        val is_default: bool,
        // HMAC-256 keys for this data-plane.
        // The first is used for signing, and any key may validate.
        val hmac_keys: Vec<String>,
        // HMAC-256 keys for this data-plane in sops-encrypted yaml document format
        // The first is used for signing, and any key may validate.
        val encrypted_hmac_keys: String,
        // Name of the collection for ops logs of the data-plane.
        val ops_logs_name: models::Collection,
        // Name of the collection for ops stats of the data-plane.
        val ops_stats_name: models::Collection,
        // Address of brokers within the data-plane.
        val broker_address: String,
        // Address of reactors within the data-plane.
        val reactor_address: String,
    }

    table RoleGrants (row #[derive(serde::Deserialize, serde::Serialize)] RoleGrant, sql "role_grants") {
        // Subject of the grant, to which a capability is bestowed.
        key subject_role: models::Prefix,
        // Object of the grant, to which a capability is bestowed upon the subject.
        key object_role: models::Prefix,
        // Capability of the subject with respect to the object.
        val capability: models::Capability,
    }

    table UserGrants (row #[derive(serde::Deserialize, serde::Serialize)] UserGrant, sql "user_grants") {
        // User ID to which a capability is bestowed.
        key user_id: uuid::Uuid,
        // Object of the grant, to which a capability is bestowed upon the subject.
        key object_role: models::Prefix,
        // Capability of the subject with respect to the object.
        val capability: models::Capability,
    }

    table DraftCaptures (row #[derive(Clone)] DraftCapture, sql "draft_captures") {
        // Catalog name of this capture.
        key capture: models::Capture,
        // Scope of the draft capture.
        val scope: url::Url,
        // Expected last publication ID of this capture.
        val expect_pub_id: Option<models::Id>,
        // Model of this capture, or None if the capture is being deleted.
        val model: Option<models::CaptureDef>,
        // This draft is a "touch" which intends to refresh
        // the build of the live capture without changing it.
        // An error will be raised if `model` isn't identical to the live model.
        val is_touch: bool,
    }

    table DraftCollections (row #[derive(Clone)] DraftCollection, sql "draft_collections") {
        // Catalog name of this collection.
        key collection: models::Collection,
        // Scope of the draft collection.
        val scope: url::Url,
        // Expected last publication ID of this collection.
        val expect_pub_id: Option<models::Id>,
        // Model of this collection, or None if the collection is being deleted.
        val model: Option<models::CollectionDef>,
        // This draft is a "touch" which intends to refresh
        // the build of the live collection without changing it.
        // An error will be raised if `model` isn't identical to the live model.
        val is_touch: bool,
    }

    table DraftMaterializations (row #[derive(Clone)] DraftMaterialization, sql "draft_materializations") {
        // Catalog name of this materialization.
        key materialization: models::Materialization,
        // Scope of the draft materialization.
        val scope: url::Url,
        // Expected last publication ID of this materialization.
        val expect_pub_id: Option<models::Id>,
        // Model of this materialization, or None if the materialization is being deleted.
        val model: Option<models::MaterializationDef>,
        // This draft is a "touch" which intends to refresh
        // the build of the live materialization without changing it.
        // An error will be raised if `model` isn't identical to the live model.
        val is_touch: bool,
    }

    table DraftTests (row #[derive(Clone)] DraftTest, sql "draft_tests") {
        // Catalog name of the test.
        key test: models::Test,
        // Scope of the draft test.
        val scope: url::Url,
        // Expected last publication ID of this test.
        val expect_pub_id: Option<models::Id>,
        // Model of the test, or None if the test is being deleted.
        val model: Option<models::TestDef>,
        // This draft is a "touch" which intends to refresh
        // the build of the live materialization without changing it.
        // An error will be raised if `model` isn't identical to the live model.
        val is_touch: bool,
    }

    table LiveCaptures (row LiveCapture, sql "live_captures") {
        // Catalog name of this capture.
        key capture: models::Capture,
        // Control-plane ID of this capture.
        val control_id: models::Id,
        // Data-plane assignment for this capture.
        val data_plane_id: models::Id,
        // Most recent publication ID of this capture.
        val last_pub_id: models::Id,
        // Most recent build ID of this capture
        val last_build_id: models::Id,
        // Model of the capture as-of `last_pub_id`
        val model: models::CaptureDef,
        // Built specification of this capture as-of `last_pub_id`.
        val spec: proto_flow::flow::CaptureSpec,
        // Hash of the last_pub_ids of all the dependencies that were used to build the capture
        val dependency_hash: Option<String>,
    }

    table LiveCollections (row LiveCollection, sql "live_collections") {
        // Catalog name of this collection.
        key collection: models::Collection,
        // Control-plane ID of this collection.
        val control_id: models::Id,
        // Data-plane assignment for this collection.
        val data_plane_id: models::Id,
        // Most recent publication ID of this collection.
        val last_pub_id: models::Id,
        // Most recent build ID of this collection
        val last_build_id: models::Id,
        // Model of the collection as-of `last_pub_id`.
        val model: models::CollectionDef,
        // Built specification of this collection as-of `last_pub_id`.
        val spec: proto_flow::flow::CollectionSpec,
        // Hash of the last_pub_ids of all the dependencies that were used to build the collection
        val dependency_hash: Option<String>,
    }

    table LiveMaterializations (row LiveMaterialization, sql "live_materializations") {
        // Catalog name of this materialization.
        key materialization: models::Materialization,
        // Control-plane ID of this materialization.
        val control_id: models::Id,
        // Data-plane assignment for this materialization.
        val data_plane_id: models::Id,
        // Most recent publication ID of this materialization.
        val last_pub_id: models::Id,
        // Most recent build ID of this materialization
        val last_build_id: models::Id,
        // Model of the materialization as-of `last_pub_id`.
        val model: models::MaterializationDef,
        // Built specification of this materialization as-of `last_pub_id`.
        val spec: proto_flow::flow::MaterializationSpec,
        // Hash of the last_pub_ids of all the dependencies that were used to build the materialization
        val dependency_hash: Option<String>,
    }

    table LiveTests (row LiveTest, sql "live_tests") {
        // Catalog name of this test.
        key test: models::Test,
        // Control-plane ID of this test.
        val control_id: models::Id,
        // Most recent publication ID of this test.
        val last_pub_id: models::Id,
        // Most recent build ID of this test
        val last_build_id: models::Id,
        // Model of the test as-of `last_pub_id`.
        val model: models::TestDef,
        // Built specification of this test as-of `last_pub_id`.
        val spec: proto_flow::flow::TestSpec,
        // Hash of the last_pub_ids of all the dependencies that were used to build the test
        val dependency_hash: Option<String>,
    }

    table BuiltCaptures (row BuiltCapture, sql "built_captures") {
        // Catalog name of this capture.
        key capture: models::Capture,
        // Scope of this built capture.
        val scope: url::Url,
        // Control-plane ID of this capture, or zero if un-assigned.
        val control_id: models::Id,
        // Data-plane assignment for this capture.
        val data_plane_id: models::Id,
        // Expected last publication ID for optimistic concurrency.
        val expect_pub_id: models::Id,
        // Expected last build ID for optimistic concurrency.
        val expect_build_id: models::Id,
        // Model of this capture, or None if the capture is being deleted.
        val model: Option<models::CaptureDef>,
        // Descriptions of automated model changes made during validation.
        // If non-empty then `is_touch` is false.
        val model_fixes: Vec<String>,
        // Validated response which was used to build this spec.
        val validated: Option<proto_flow::capture::response::Validated>,
        // Built specification of this capture, or None if it's being deleted.
        val spec: Option<proto_flow::flow::CaptureSpec>,
        // Previous specification which is being modified or deleted,
        // or None if unchanged OR this is an insertion.
        val previous_spec: Option<proto_flow::flow::CaptureSpec>,
        // When true this was a "touch" which refreshed the build of the
        // unchanged live model. Its publication ID shouldn't increase.
        val is_touch: bool,
        // Hash of the last_pub_ids of all the dependencies that were used to build the capture
        val dependency_hash: Option<String>,
    }

    table BuiltCollections (row BuiltCollection, sql "built_collections") {
        // Catalog name of this collection.
        key collection: models::Collection,
        // Scope of this built collection.
        val scope: url::Url,
        // Control-plane ID of this collection, or zero if un-assigned.
        val control_id: models::Id,
        // Data-plane assignment for this collection.
        val data_plane_id: models::Id,
        // Expected last publication ID for optimistic concurrency.
        val expect_pub_id: models::Id,
        // Expected last build ID for optimistic concurrency.
        val expect_build_id: models::Id,
        // Model of this collection, or None if the collection is being deleted.
        val model: Option<models::CollectionDef>,
        // Descriptions of automated model changes made during validation.
        // If non-empty then `is_touch` is false.
        val model_fixes: Vec<String>,
        // Validated response which was used to build this spec.
        val validated: Option<proto_flow::derive::response::Validated>,
        // Built specification of this collection, or None if it's being deleted.
        val spec: Option<proto_flow::flow::CollectionSpec>,
        // Previous specification which is being modified or deleted,
        // or None if unchanged OR this is an insertion.
        val previous_spec: Option<proto_flow::flow::CollectionSpec>,
        // When true this was a "touch" which refreshed the build of the
        // unchanged live model. Its publication ID shouldn't increase.
        val is_touch: bool,
        // Hash of the last_pub_ids of all the dependencies that were used to build the collection
        val dependency_hash: Option<String>,
    }

    table BuiltMaterializations (row BuiltMaterialization, sql "built_materializations") {
        // Catalog name of this materialization.
        key materialization: models::Materialization,
        // Scope of this built materialization.
        val scope: url::Url,
        // Control-plane ID of this materialization, or zero if un-assigned.
        val control_id: models::Id,
        // Data-plane assignment for this materialization.
        val data_plane_id: models::Id,
        // Expected last publication ID for optimistic concurrency.
        val expect_pub_id: models::Id,
        // Expected last build ID for optimistic concurrency.
        val expect_build_id: models::Id,
        // Model of this materialization, or None if the materialization is being deleted.
        val model: Option<models::MaterializationDef>,
        // Descriptions of automated model changes made during validation.
        // If non-empty then `is_touch` is false.
        val model_fixes: Vec<String>,
        // Validated response which was used to build this spec.
        val validated: Option<proto_flow::materialize::response::Validated>,
        // Built specification of this materialization, or None if it's being deleted.
        val spec: Option<proto_flow::flow::MaterializationSpec>,
        // Previous specification which is being modified or deleted,
        // or None if unchanged OR this is an insertion.
        val previous_spec: Option<proto_flow::flow::MaterializationSpec>,
        // When true this was a "touch" which refreshed the build of the
        // unchanged live model. Its publication ID shouldn't increase.
        val is_touch: bool,
        // Hash of the last_pub_ids of all the dependencies that were used to build the materialization
        val dependency_hash: Option<String>,
    }

    table BuiltTests (row BuiltTest, sql "built_tests") {
        // Catalog name of this test.
        key test: models::Test,
        // Scope of this built test.
        val scope: url::Url,
        // Control-plane identifier for this test, or zero if un-assigned.
        val control_id: models::Id,
        // Expected last publication ID for optimistic concurrency.
        val expect_pub_id: models::Id,
        // Expected last build ID for optimistic concurrency.
        val expect_build_id: models::Id,
        // Model of the test, or None if the test is being deleted.
        val model: Option<models::TestDef>,
        // Descriptions of automated model changes made during validation.
        // If non-empty then `is_touch` is false.
        val model_fixes: Vec<String>,
        // Built specification of this test, or None if being deleted.
        val spec: Option<proto_flow::flow::TestSpec>,
        // Previous specification which is being modified or deleted,
        // or None if unchanged OR this is an insertion.
        val previous_spec: Option<proto_flow::flow::TestSpec>,
        // When true this was a "touch" which refreshed the build of the
        // unchanged live model. Its publication ID shouldn't increase.
        val is_touch: bool,
        // Hash of the last_pub_ids of all the dependencies that were used to build the test
        val dependency_hash: Option<String>,
    }

    table Errors (row Error, sql "errors") {
        // Scope of this error.
        val scope: url::Url,
        // Error content.
        val error: anyhow::Error,
    }

    table Meta (row Build, sql "meta") {
        val build_config: proto_flow::flow::build_api::Config,
    }
);

impl Error {
    pub fn to_draft_error(&self) -> models::draft_error::Error {
        let catalog_name = parse_synthetic_scope(&self.scope)
            .map(|(_, name)| name)
            .unwrap_or_default();
        models::draft_error::Error {
            catalog_name,
            scope: Some(self.scope.to_string()),
            // use alternate to print chained contexts
            detail: format!("{:#}", self.error),
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, serde::Serialize)]
pub struct GrantRef<'a> {
    subject_role: &'a str,
    object_role: &'a str,
    capability: models::Capability,
}

/// Attempts to parse a catalog type and name from a URL in the form of:
/// `flow://<catalog-type>/<catalog-name>`. Returns None if the URL doesn't
/// have a valid `CatalogType`, or if the scheme doesn't match.
pub fn parse_synthetic_scope(url: &url::Url) -> Option<(models::CatalogType, String)> {
    if url.scheme() != "flow" {
        return None;
    }
    let host = url.host_str()?;
    let catalog_type = models::CatalogType::from_str(host).ok()?;
    let catalog_name = url.path().trim_start_matches('/').to_string();
    Some((catalog_type, catalog_name))
}

/// Generate a synthetic scope URL for a given catalog type and name, for when a meaningful scope
/// URL is otherwise not avaialble. The `catalog_type` can be a `models::CatalogType` or a `&str`.
pub fn synthetic_scope(catalog_type: impl AsRef<str>, catalog_name: impl AsRef<str>) -> url::Url {
    let url_str = format!("flow://{}/", catalog_type.as_ref());
    let mut url = url::Url::parse(&url_str).unwrap();
    // using set_path for the catalog name ensures that the name gets properly escaped so that the URL is
    // guaranteed to be valid, even if the catalog_name is not.
    url.set_path(catalog_name.as_ref());
    url
}

// macros::TableColumn implementations for table columns.

primitive_sql_types!(
    String => "TEXT",
    url::Url => "TEXT",
    bool => "BOOLEAN",
    u32 => "INTEGER",
);

// primitive_sql_types generates SqlColumn but not Column implementations.
impl Column for String {
    fn column_fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self)
    }
}
impl Column for url::Url {
    fn column_fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}
impl Column for bool {
    fn column_fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Match SQLite encoding of booleans.
        if *self {
            f.write_str("1")
        } else {
            f.write_str("0")
        }
    }
}
impl Column for u32 {}

string_wrapper_types!(
    models::Capture,
    models::Collection,
    models::Materialization,
    models::Prefix,
    models::Test,
);

json_sql_types!(
    Vec<String>,
    Vec<models::Store>,
    models::Capability,
    models::CaptureDef,
    models::CatalogType,
    models::CollectionDef,
    models::Id,
    models::MaterializationDef,
    models::Name,
    models::RawValue,
    models::Schema,
    models::TestDef,
    proto_flow::flow::ContentType,
    uuid::Uuid,
);

proto_sql_types!(
    proto_flow::capture::response::Validated,
    proto_flow::derive::response::Validated,
    proto_flow::flow::CaptureSpec,
    proto_flow::flow::CollectionSpec,
    proto_flow::flow::MaterializationSpec,
    proto_flow::flow::TestSpec,
    proto_flow::flow::build_api::Config,
    proto_flow::materialize::response::Validated,
);

// Additional bespoke column implementations for types that require extra help.
impl Column for anyhow::Error {
    fn column_fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:#}", self)
    }
}

#[cfg(feature = "persist")]
impl SqlColumn for anyhow::Error {
    fn sql_type() -> &'static str {
        "TEXT"
    }
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        Ok(format!("{:#}", self).into())
    }
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        Ok(anyhow::anyhow!(String::column_result(value)?))
    }
}

impl Column for bytes::Bytes {
    fn column_fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        const ELIDE: &str = ".. binary ..";
        <str as std::fmt::Debug>::fmt(ELIDE, f)
    }
}

#[cfg(feature = "persist")]
impl SqlColumn for bytes::Bytes {
    fn sql_type() -> &'static str {
        "BLOB"
    }
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        Ok(self.as_ref().into())
    }
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        use rusqlite::types::FromSql;
        Ok(<Vec<u8>>::column_result(value)?.into())
    }
}

#[cfg(test)]
mod test {
    use super::macros::*;

    tables!(
        table Foos (row Foo, sql "foos") {
            val f1: u32,
        }

        table Bars (row Bar, sql "bars") {
            key b1: u32,
            val b2: u32,
        }

        table Quibs (row Quib, sql "quibs") {
            key q1: u32,
            key q2: u32,
        }
    );

    #[test]
    fn test_insert_indexing() {
        let mut tbl = Foos::new();
        tbl.insert_row(1);
        tbl.insert_row(0);
        tbl.insert_row(2);
        tbl.insert_row(1);
        tbl.insert_row(0);
        tbl.insert_row(1);

        // When key is empty, the initial ordering is preserved.
        assert_eq!(
            tbl.iter().map(|r| r.f1).collect::<Vec<_>>(),
            vec![1, 0, 2, 1, 0, 1]
        );

        // Table ordered by a single column.
        let mut tbl = Bars::new();
        tbl.insert_row(10, 90);
        tbl.insert_row(0, 78);
        tbl.insert_row(20, 56);
        tbl.insert_row(10, 34);
        tbl.insert_row(0, 12);
        tbl.insert_row(10, 90);

        // Ordered with respect to key, but not to the extra columns.
        assert_eq!(
            tbl.iter().map(|r| (r.b1, r.b2)).collect::<Vec<_>>(),
            vec![(0, 78), (0, 12), (10, 90), (10, 34), (10, 90), (20, 56)]
        );

        let joined: Vec<usize> = tbl
            .into_inner_join(
                [(0u32, 1usize), (0, 2), (10, 3), (15, 4), (20, 5), (21, 6)].into_iter(),
                |_bar, _k, v| Some(v),
            )
            .collect();

        assert_eq!(joined, vec![1, 2, 3, 5]);

        // Table ordered on a composite key.
        let mut tbl = Quibs::new();
        tbl.insert_row(10, 90);
        tbl.insert_row(0, 78);
        tbl.insert_row(20, 56);
        tbl.insert_row(10, 34);
        tbl.insert_row(0, 12);
        tbl.insert_row(10, 90);

        // Fully ordered by the composite key (both columns).
        assert_eq!(
            tbl.iter().map(|r| (r.q1, r.q2)).collect::<Vec<_>>(),
            vec![(0, 12), (0, 78), (10, 34), (10, 90), (10, 90), (20, 56)]
        );

        let joined: Vec<usize> = tbl
            .into_inner_join(
                [
                    ((0u32, 10u32), 1usize),
                    ((0, 78), 2),
                    ((0, 90), 3),
                    ((10, 34), 4),
                    ((10, 90), 5),
                    ((21, 0), 6),
                ]
                .into_iter(),
                |_quib, _k, v| Some(v),
            )
            .collect();

        assert_eq!(joined, vec![2, 4, 5]);
    }
}
