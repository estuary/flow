#[macro_use]
mod macros;
use itertools::Itertools;
use macros::*;
mod filters;
mod id;

pub use filters::{AnySpec, SpecExt};
pub use id::Id;
pub use itertools::EitherOrBoth;
pub use macros::{SpecRow, Table};

#[cfg(feature = "persist")]
pub use macros::{load_tables, persist_tables, SqlTableObj};
#[cfg(feature = "persist")]
use rusqlite::{types::FromSqlError, ToSql};

use prost::Message;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Debug,
    str::FromStr,
};

#[derive(Debug, Clone)]
pub enum Drafted<T: Debug + Serialize + DeserializeOwned> {
    Some(T),
    None,
    Deleted,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum Action {
    Update,
    Delete,
}

impl Action {
    pub fn as_str(&self) -> &'static str {
        match self {
            Action::Update => "update",
            Action::Delete => "delete",
        }
    }
}

impl FromStr for Action {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "update" => Ok(Action::Update),
            "delete" => Ok(Action::Delete),
            _ => Err(()),
        }
    }
}

impl Column for Action {}

#[cfg(feature = "persist")]
impl SqlColumn for Action {
    fn sql_type() -> &'static str {
        "TEXT"
    }

    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        self.as_str().to_sql()
    }

    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        let rusqlite::types::ValueRef::Text(text) = value else {
            return Err(FromSqlError::InvalidType);
        };
        let s = std::str::from_utf8(text)
            .map_err(|e| FromSqlError::Other(format!("invalid utf8 value: {e}").into()))?;
        Action::from_str(s)
            .map_err(|_| FromSqlError::Other(format!("invalid action value {s:?}").into()))
    }
}

tables!(
    table Fetches (row Fetch, order_by [depth resource], sql "fetches") {
        // Import depth of this fetch.
        depth: u32,
        // Fetched resource Url.
        resource: url::Url,
    }

    table Resources (row Resource, order_by [resource], sql "resources") {
        // Url of this resource.
        resource: url::Url,
        // Content-type of this resource.
        content_type: proto_flow::flow::ContentType,
        // Byte content of this resource.
        content: bytes::Bytes,
        // Document content of this resource.
        content_dom: models::RawValue,
    }

    table Imports (row Import, order_by [scope to_resource], sql "imports") {
        // Scope is the referring resource and fragment location which caused the import.
        scope: url::Url,
        // Resource which is imported. Never has a fragment.
        to_resource: url::Url,
    }

    table StorageMappings (row StorageMapping, order_by [prefix], sql "storage_mappings") {
        scope: url::Url,
        // Catalog prefix to which this storage mapping applies.
        prefix: models::Prefix,
        // Stores for journal fragments under this prefix.
        stores: Vec<models::Store>,
    }

    table InferredSchemas (row #[derive(Clone)] InferredSchema, order_by [collection_name], sql "inferred_schemas") {
        collection_name: String,
        schema: models::Schema,
        md5: String,
    }

    table Collections (row #[derive(Clone)] Collection, order_by [collection], sql "collections") {
        scope: url::Url,
        // Name of this collection.
        collection: models::Collection,

        id: Option<Id>,
        action: Option<Action>,
        expect_pub_id: Option<Id>,
        drafted: Option<models::CollectionDef>,
        live_spec: Option<models::CollectionDef>,
        last_pub_id: Option<Id>,
        inferred_schema_md5: Option<String>,
    }

    table Captures (row #[derive(Clone)] Capture, order_by [capture], sql "captures") {
        scope: url::Url,
        // Name of this capture.
        capture: models::Capture,

        id: Option<Id>,
        action: Option<Action>,
        expect_pub_id: Option<Id>,
        drafted: Option<models::CaptureDef>,
        live_spec: Option<models::CaptureDef>,
        last_pub_id: Option<Id>,
    }

    table Materializations (row #[derive(Clone)] Materialization, order_by [materialization], sql "materializations") {
        scope: url::Url,
        // Name of this materialization.
        materialization: models::Materialization,

        id: Option<Id>,
        action: Option<Action>,
        expect_pub_id: Option<Id>,
        drafted: Option<models::MaterializationDef>,
        live_spec: Option<models::MaterializationDef>,
        last_pub_id: Option<Id>,
    }

    table Tests (row #[derive(Clone)] Test, order_by [test], sql "tests") {
        scope: url::Url,
        // Name of the test.
        test: models::Test,

        id: Option<Id>,
        action: Option<Action>,
        expect_pub_id: Option<Id>,
        drafted: Option<models::TestDef>,
        live_spec: Option<models::TestDef>,
        last_pub_id: Option<Id>,
    }

    table BuiltCaptures (row BuiltCapture, order_by [capture], sql "built_captures") {
        scope: url::Url,
        // Name of this capture.
        capture: String,
        // Validated response which was used to build this spec.
        validated: proto_flow::capture::response::Validated,
        // Built specification for this capture.
        spec: proto_flow::flow::CaptureSpec,
    }

    table BuiltCollections (row BuiltCollection, order_by [collection], sql "built_collections") {
        scope: url::Url,
        // Name of this collection.
        collection: models::Collection,
        // Validated response which was used to build this spec.
        validated: Option<proto_flow::derive::response::Validated>,
        // Built specification for this collection.
        spec: proto_flow::flow::CollectionSpec,
        // The md5 sum of the inferred schema at the time that this collection
        // was built. Note that this may be present even if the collection does
        // not actually use the inferred schema. And it may also be missing,
        // even if the collection _does_ use schema inference, for "remote"
        // collections that were resolve dynamically during the build.
        inferred_schema_md5: Option<String>,
    }

    table BuiltMaterializations (row BuiltMaterialization, order_by [materialization], sql "built_materializations") {
        scope: url::Url,
        // Name of this materialization.
        materialization: String,
        // Validated response which was used to build this spec.
        validated: proto_flow::materialize::response::Validated,
        // Built specification for this materialization.
        spec: proto_flow::flow::MaterializationSpec,
    }

    table BuiltTests (row BuiltTest, order_by [test], sql "built_tests") {
        scope: url::Url,
        // Name of the test case.
        test: models::Test,
        // Built specification for this test case.
        spec: proto_flow::flow::TestSpec,
    }

    table Errors (row Error, order_by [], sql "errors") {
        scope: url::Url,
        error: anyhow::Error,
    }

    table Meta (row Build, order_by [], sql "meta") {
        build_config: proto_flow::flow::build_api::Config,
    }
);

impl NamedRow for InferredSchema {
    fn name(&self) -> &str {
        &self.collection_name
    }
}
impl<'a, T: NamedRow> NamedRow for &'a T {
    fn name(&self) -> &str {
        T::name(*self)
    }
}

spec_row! {Collection, models::CollectionDef, collection}
spec_row! {Capture, models::CaptureDef, capture}
spec_row! {Materialization, models::MaterializationDef, materialization}
spec_row! {Test, models::TestDef, test}

#[derive(Default, Debug)]
pub struct Catalog {
    pub captures: Captures,
    pub collections: Collections,
    pub materializations: Materializations,
    pub tests: Tests,
}

impl Catalog {
    pub fn is_empty(&self) -> bool {
        self.captures.is_empty()
            && self.collections.is_empty()
            && self.materializations.is_empty()
            && self.tests.is_empty()
    }

    pub fn all_spec_names(&self) -> BTreeSet<String> {
        let mut names = BTreeSet::new();
        names.extend(self.captures.iter().map(|r| r.get_name().to_string()));
        names.extend(self.collections.iter().map(|r| r.get_name().to_string()));
        names.extend(
            self.materializations
                .iter()
                .map(|r| r.get_name().to_string()),
        );
        names.extend(self.tests.iter().map(|r| r.get_name().to_string()));
        names
    }

    pub fn related_tasks(&self, collection_names: &BTreeSet<String>) -> Catalog {
        let captures = self
            .captures
            .iter()
            .filter(|r| !r.get_final_spec().writes_to().is_disjoint(collection_names))
            .cloned()
            .collect();
        let collections = self
            .collections
            .iter()
            .filter(|r| {
                !r.get_final_spec()
                    .reads_from()
                    .is_disjoint(collection_names)
            })
            .cloned()
            .collect();
        let materializations = self
            .materializations
            .iter()
            .filter(|r| {
                !r.get_final_spec()
                    .reads_from()
                    .is_disjoint(collection_names)
            })
            .cloned()
            .collect();
        let tests = self
            .tests
            .iter()
            .filter(|r| {
                !r.get_final_spec()
                    .reads_from()
                    .is_disjoint(collection_names)
                    || !r.get_final_spec().writes_to().is_disjoint(collection_names)
            })
            .cloned()
            .collect();
        Catalog {
            captures,
            collections,
            materializations,
            tests,
        }
    }

    pub fn get_named(&self, names: &BTreeSet<impl AsRef<str>>) -> Catalog {
        let captures = inner_join(self.captures.iter(), names.iter().map(|n| n.as_ref()))
            .map(|(r, _)| r.clone())
            .collect();
        let collections = inner_join(self.collections.iter(), names.iter().map(|n| n.as_ref()))
            .map(|(r, _)| r.clone())
            .collect();
        let materializations = inner_join(
            self.materializations.iter(),
            names.iter().map(|n| n.as_ref()),
        )
        .map(|(r, _)| r.clone())
        .collect();
        let tests = inner_join(self.tests.iter(), names.iter().map(|n| n.as_ref()))
            .map(|(r, _)| r.clone())
            .collect();
        Catalog {
            captures,
            collections,
            materializations,
            tests,
        }
    }

    pub fn live_to_catalog(&self) -> models::Catalog {
        let captures = self
            .captures
            .iter()
            .filter_map(|r| {
                r.live_spec
                    .clone()
                    .map(|d| (models::Capture::new(r.get_name()), d))
            })
            .collect();
        let collections = self
            .collections
            .iter()
            .filter_map(|r| {
                r.live_spec
                    .clone()
                    .map(|d| (models::Collection::new(r.get_name()), d))
            })
            .collect();
        let materializations = self
            .materializations
            .iter()
            .filter_map(|r| {
                r.live_spec
                    .clone()
                    .map(|d| (models::Materialization::new(r.get_name()), d))
            })
            .collect();
        let tests = self
            .tests
            .iter()
            .filter_map(|r| {
                r.live_spec
                    .clone()
                    .map(|d| (models::Test::new(r.get_name()), d))
            })
            .collect();

        models::Catalog {
            captures,
            collections,
            materializations,
            tests,
            ..Default::default()
        }
    }

    pub fn draft_to_catalog(&self) -> models::Catalog {
        let captures = self
            .captures
            .iter()
            .filter_map(|r| {
                r.drafted
                    .clone()
                    .map(|d| (models::Capture::new(r.get_name()), d))
            })
            .collect();
        let collections = self
            .collections
            .iter()
            .filter_map(|r| {
                r.drafted
                    .clone()
                    .map(|d| (models::Collection::new(r.get_name()), d))
            })
            .collect();
        let materializations = self
            .materializations
            .iter()
            .filter_map(|r| {
                r.drafted
                    .clone()
                    .map(|d| (models::Materialization::new(r.get_name()), d))
            })
            .collect();
        let tests = self
            .tests
            .iter()
            .filter_map(|r| {
                r.drafted
                    .clone()
                    .map(|d| (models::Test::new(r.get_name()), d))
            })
            .collect();

        models::Catalog {
            captures,
            collections,
            materializations,
            tests,
            ..Default::default()
        }
    }

    pub fn extend_draft(&mut self, draft: Catalog) {
        let Catalog {
            captures,
            collections,
            materializations,
            tests,
        } = draft;

        self.captures.upsert_all(captures, |prev, next| {
            next.id = prev.id;
            next.live_spec = prev.live_spec.clone();
            next.expect_pub_id = prev.last_pub_id;
        });
        self.collections.upsert_all(collections, |prev, next| {
            next.id = prev.id;
            next.live_spec = prev.live_spec.clone();
            next.expect_pub_id = prev.last_pub_id;
        });
        self.materializations
            .upsert_all(materializations, |prev, next| {
                next.id = prev.id;
                next.live_spec = prev.live_spec.clone();
                next.expect_pub_id = prev.last_pub_id;
            });
        self.tests.upsert_all(tests, |prev, next| {
            next.id = prev.id;
            next.live_spec = prev.live_spec.clone();
            next.expect_pub_id = prev.last_pub_id;
        });
    }

    pub fn merge(&mut self, other: Catalog) {
        self.captures.upsert_all(other.captures, |_, _| {});
        self.collections.upsert_all(other.collections, |_, _| {});
        self.materializations
            .upsert_all(other.materializations, |_, _| {});
        self.tests.upsert_all(other.tests, |_, _| {});
    }
}

fn scope_for(catalog_type: &str, catalog_name: &str) -> url::Url {
    // TODO: sanitize catalog_name to make this infallible
    url::Url::parse(&format!("flow://{catalog_type}/{catalog_name}")).unwrap()
}

impl From<models::Catalog> for Catalog {
    fn from(value: models::Catalog) -> Self {
        let models::Catalog {
            captures,
            collections,
            materializations,
            tests,
            ..
        } = value;
        let captures = captures
            .into_iter()
            .map(|(k, v)| Capture {
                scope: scope_for("capture", &k),
                capture: k,
                id: None,
                action: Some(Action::Update),
                expect_pub_id: None,
                drafted: Some(v),
                live_spec: None,
                last_pub_id: None,
            })
            .collect();
        let collections = collections
            .into_iter()
            .map(|(k, v)| Collection {
                scope: scope_for("collection", &k),
                collection: k,
                id: None,
                action: Some(Action::Update),
                expect_pub_id: None,
                drafted: Some(v),
                live_spec: None,
                last_pub_id: None,
                inferred_schema_md5: None,
            })
            .collect();
        let materializations = materializations
            .into_iter()
            .map(|(k, v)| Materialization {
                scope: scope_for("materialization", &k),
                materialization: models::Materialization::new(k),
                id: None,
                action: Some(Action::Update),
                expect_pub_id: None,
                drafted: Some(v),
                live_spec: None,
                last_pub_id: None,
            })
            .collect();
        let tests = tests
            .into_iter()
            .map(|(k, v)| Test {
                scope: scope_for("test", &k),
                test: models::Test::new(k),
                id: None,
                action: Some(Action::Update),
                expect_pub_id: None,
                drafted: Some(v),
                live_spec: None,
                last_pub_id: None,
            })
            .collect();

        Self {
            captures,
            collections,
            materializations,
            tests,
        }
    }
}

pub fn full_outer_join<'l, 'r, LT, LR, RT, RR>(
    left: LT,
    right: RT,
) -> impl Iterator<Item = EitherOrBoth<LR, RR>>
where
    'r: 'l,
    LT: IntoIterator<Item = LR>,
    LR: NamedRow + 'l,
    RT: IntoIterator<Item = RR>,
    RR: NamedRow + 'r,
{
    left.into_iter()
        .merge_join_by(right.into_iter(), |l, r| l.name().cmp(r.name()))
}

pub fn left_outer_join<'l, 'r, LT, LR, RT, RR>(
    left: LT,
    right: RT,
) -> impl Iterator<Item = (LR, Option<RR>)>
where
    'r: 'l,
    LT: IntoIterator<Item = LR>,
    LR: NamedRow + 'l,
    RT: IntoIterator<Item = RR>,
    RR: NamedRow + 'r,
{
    full_outer_join(left, right).filter_map(|eob| match eob {
        EitherOrBoth::Left(l) => Some((l, None)),
        EitherOrBoth::Both(l, r) => Some((l, Some(r))),
        EitherOrBoth::Right(_) => None,
    })
}

pub fn inner_join<'l, 'r, LT, LR, RT, RR>(left: LT, right: RT) -> impl Iterator<Item = (LR, RR)>
where
    'r: 'l,
    LT: IntoIterator<Item = LR>,
    LR: NamedRow + 'l,
    RT: IntoIterator<Item = RR>,
    RR: NamedRow + 'r,
{
    full_outer_join(left, right).filter_map(|eob| match eob {
        EitherOrBoth::Both(l, r) => Some((l, r)),
        _ => None,
    })
}

/// Sources are tables which are populated by catalog loads of the `sources` crate.
#[derive(Default, Debug)]
pub struct Sources {
    pub captures: Captures,
    pub collections: Collections,
    pub errors: Errors,
    pub fetches: Fetches,
    pub imports: Imports,
    pub materializations: Materializations,
    pub resources: Resources,
    pub storage_mappings: StorageMappings,
    pub tests: Tests,
}

/// Validations are tables populated by catalog validations of the `validation` crate.
#[derive(Default, Debug)]
pub struct Validations {
    pub built_captures: BuiltCaptures,
    pub built_collections: BuiltCollections,
    pub built_materializations: BuiltMaterializations,
    pub built_tests: BuiltTests,
    pub errors: Errors,
}

#[cfg(feature = "persist")]
impl Sources {
    pub fn into_result(mut self) -> Result<Self, Errors> {
        match std::mem::take(&mut self.errors) {
            errors if errors.is_empty() => Ok(self),
            errors => Err(errors),
        }
    }

    // Access all tables as an array of dynamic TableObj instances.
    pub fn as_tables(&self) -> Vec<&dyn SqlTableObj> {
        // This de-structure ensures we can't fail to update as tables change.
        let Self {
            captures,
            collections,
            errors,
            fetches,
            imports,
            materializations,
            resources,
            storage_mappings,
            tests,
        } = self;

        vec![
            captures,
            collections,
            errors,
            fetches,
            imports,
            materializations,
            resources,
            storage_mappings,
            tests,
        ]
    }

    // Access all tables as an array of mutable dynamic SqlTableObj instances.
    pub fn as_tables_mut(&mut self) -> Vec<&mut dyn SqlTableObj> {
        let Self {
            captures,
            collections,
            errors,
            fetches,
            imports,
            materializations,
            resources,
            storage_mappings,
            tests,
        } = self;

        vec![
            captures,
            collections,
            errors,
            fetches,
            imports,
            materializations,
            resources,
            storage_mappings,
            tests,
        ]
    }
}

#[cfg(feature = "persist")]
impl Validations {
    pub fn into_result(mut self) -> Result<Self, Errors> {
        match std::mem::take(&mut self.errors) {
            errors if errors.is_empty() => Ok(self),
            errors => Err(errors),
        }
    }

    // Access all tables as an array of dynamic TableObj instances.
    pub fn as_tables(&self) -> Vec<&dyn SqlTableObj> {
        // This de-structure ensures we can't fail to update as tables change.
        let Self {
            built_captures,
            built_collections,
            built_materializations,
            built_tests,
            errors,
        } = self;

        vec![
            built_captures,
            built_collections,
            built_materializations,
            built_tests,
            errors,
        ]
    }

    // Access all tables as an array of mutable dynamic SqlTableObj instances.
    pub fn as_tables_mut(&mut self) -> Vec<&mut dyn SqlTableObj> {
        let Self {
            built_captures,
            built_collections,
            built_materializations,
            built_tests,
            errors,
        } = self;

        vec![
            built_captures,
            built_collections,
            built_materializations,
            built_tests,
            errors,
        ]
    }
}

pub trait NamedRow {
    fn name(&self) -> &str;
}

impl<'a> NamedRow for &'a str {
    fn name(&self) -> &str {
        self
    }
}

impl NamedRow for String {
    fn name(&self) -> &str {
        self.as_str()
    }
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
    Vec<models::Store>,
    models::Schema,
    models::TestDef,
    models::CaptureDef,
    models::CollectionDef,
    models::MaterializationDef,
    models::RawValue,
    proto_flow::flow::ContentType,
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

// Modules that extend tables with additional implementations.
mod behaviors;

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
        table Foos (row Foo, order_by [], sql "foos") {
            f1: u32,
        }

        table Bars (row Bar, order_by [b1], sql "bars") {
            b1: u32,
            b2: u32,
        }

        table Quibs (row Quib, order_by [q1 q2], sql "quibs") {
            q1: u32,
            q2: u32,
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

        // When order_by is empty, the initial ordering is preserved.
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

        // Ordered with respect to order_by, but not to the extra columns.
        assert_eq!(
            tbl.iter().map(|r| (r.b1, r.b2)).collect::<Vec<_>>(),
            vec![(0, 78), (0, 12), (10, 90), (10, 34), (10, 90), (20, 56)]
        );

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
    }
}
