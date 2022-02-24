use sqlx::PgPool;

use crate::services::builds_root::{FetchBuilds, PutBuilds};

#[derive(Clone)]
pub struct AppContext {
    db: PgPool,
    put_builds: PutBuilds,
    fetch_builds: FetchBuilds,
}

impl AppContext {
    pub fn new(db: PgPool, put_builds: PutBuilds, fetch_builds: FetchBuilds) -> Self {
        Self {
            db,
            put_builds,
            fetch_builds,
        }
    }

    pub fn db(&self) -> &PgPool {
        &self.db
    }

    pub fn put_builds(&self) -> &PutBuilds {
        &self.put_builds
    }

    pub fn fetch_builds(&self) -> &FetchBuilds {
        &self.fetch_builds
    }
}
