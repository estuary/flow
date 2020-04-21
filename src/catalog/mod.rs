pub mod db;
mod regexp_sql_fn;

mod collection;
mod derivation;
mod error;
mod lambda;
mod resource;
mod schema;
mod source;

use collection::Collection;
use derivation::Derivation;
pub use error::Error;
use lambda::Lambda;
use resource::Resource;
use schema::Schema;
use source::Source;

pub type Result<T> = std::result::Result<T, Error>;

use rusqlite::Connection as DB;
use url;

pub fn build_catalog(db: &DB, uri: url::Url) -> Result<()> {
    db::init(db)?;
    Source::register(db, uri)?;
    Ok(())
}
