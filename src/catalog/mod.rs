mod regexp_sql_fn;
pub mod db;

mod error;
mod collection;
mod derivation;
mod lambda;
mod resource;
mod schema;
mod source;

use error::Error;
use collection::Collection;
use derivation::Derivation;
use lambda::Lambda;
use resource::Resource;
use schema::Schema;
use source::Source;

type Result<T> = std::result::Result<T, Error>;

use url;
use rusqlite::Connection as DB;

pub fn build_catalog(db: &DB, uris: &[url::Url]) -> Result<()> {
    db::init(db)?;

    for uri in uris {
        Source::register(db, uri.clone())?;
    }
    Ok(())
}
