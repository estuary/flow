mod db;
mod regexp_sql_fn;

mod catalog;
mod collection;
mod content_type;
mod derivation;
mod error;
mod extraction;
mod lambda;
mod nodejs;
mod resource;
mod schema;

use url::Url;

pub use catalog::Catalog;
pub use collection::Collection;
pub use content_type::ContentType;
pub use derivation::Derivation;
pub use error::Error;
pub use extraction::verify_extracted_fields;
pub use lambda::Lambda;
pub use resource::Resource;
pub use rusqlite::{params as sql_params, Connection as DB};
pub use schema::Schema;

pub type Result<T> = std::result::Result<T, Error>;

/// Open a new connection to a catalog database.
pub fn open<P: AsRef<std::path::Path>>(path: P) -> Result<DB> {
    let db = DB::open(path)?;
    regexp_sql_fn::install(&db)?; // Install support for REGEXP operator.
    Ok(db)
}

pub use db::init as init_db_schema;
pub use nodejs::build_package as build_nodejs_package;

/// Holds context information about the current catalog build process. This context is used
/// whenever the we need to know which document, or which field within it, we're processing.
/// The `process_child_*` functions exist to help with the boilerplate of updating the current
/// location within a document and locating errors that occur during processing.
pub struct BuildContext<'a> {
    /// The Url of the resource that's currently being processed.
    pub resource_url: &'a Url,
    /// The database connection to use during the build process.
    pub db: &'a DB,
    current_location: estuary_json::Location<'a>,
}

/// The `LocatedProperty` has an `index` field that's used by the validator to track the order in
/// which fields were visited. This doesn't really make sense for `BuildContext` since we've
/// already deserialized into rust structs, and there's no implied ordering to struct members.
/// So we always set the `index` of `LocatedProperty` to this dummy value. This should never leak
/// out to users of `BuildContext`.
const DUMMY_FIELD_INDEX: usize = usize::MAX;

impl<'a> BuildContext<'a> {
    /// Returns a new build context, using the given database and resource url.
    pub fn new_from_root(db: &'a DB, resource_url: &'a Url) -> BuildContext<'a> {
        BuildContext {
            current_location: estuary_json::Location::Root,
            db,
            resource_url,
        }
    }

    /// Returns a new context for processing a different resource as part of the same build.
    pub fn for_new_resource(&self, resource_url: &'a Url) -> BuildContext<'a> {
        BuildContext::new_from_root(self.db, resource_url)
    }

    /// Returns a JSON pointer to the current location within the resource that's being processed.
    pub fn current_location_pointer(&self) -> String {
        self.current_location.to_pointer()
    }

    pub fn process_child_field<T, R, F>(
        &'a self,
        field_name: &'a str,
        field_value: &'a T,
        mut fun: F,
    ) -> Result<R>
    where
        F: FnMut(&BuildContext, &'a T) -> Result<R>,
    {
        let field_context = self.child_field(field_name);
        fun(&field_context, field_value).map_err(|err| self.locate_err(err))
    }

    pub fn process_child_array<T, I, F>(
        &'a self,
        field_name: &'a str,
        iter: I,
        mut fun: F,
    ) -> Result<()>
    where
        F: FnMut(&BuildContext, &'a T) -> Result<()>,
        I: Iterator<Item = &'a T>,
        T: 'a,
    {
        let field_context = self.child_field(field_name);

        for (index, value) in iter.enumerate() {
            let elem_context = field_context.child_array_element(index);
            fun(&elem_context, value).map_err(|err| self.locate_err(err))?;
        }
        Ok(())
    }

    fn locate_err(&self, err: Error) -> Error {
        match err {
            located @ Error::At { .. } => located,
            other => Error::At {
                loc: format!(
                    "resource: '{}', field: '{}'",
                    self.resource_url,
                    self.current_location.as_json_pointer()
                ),
                detail: Box::new(other),
            },
        }
    }

    fn child_array_element(&'a self, index: usize) -> BuildContext<'a> {
        BuildContext {
            current_location: self.current_location.child_array_element(index),
            db: &self.db,
            resource_url: self.resource_url,
        }
    }

    fn child_field(&'a self, field_name: &'a str) -> BuildContext<'a> {
        BuildContext {
            current_location: self
                .current_location
                .child_property(field_name, DUMMY_FIELD_INDEX),
            db: &self.db,
            resource_url: self.resource_url,
        }
    }
}

// Not public; used for testing within sub-modules.
#[cfg(test)]
use db::test::{dump_table, dump_tables};

#[cfg(test)]
mod test {
    use std::env;
    use std::path::PathBuf;
    use std::process::Command;

    #[test]
    fn run_catalog_test() {
        let mut path = PathBuf::from(&env::var("CARGO_MANIFEST_DIR").unwrap());
        path.extend(&["src", "catalog", "test_catalog.sh"]);

        let status = Command::new(path.as_os_str())
            .spawn()
            .expect("failed to start test_catalog.sh")
            .wait()
            .expect("failed to wait for command");

        assert!(status.success());
    }
}
