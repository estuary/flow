use std::sync::Arc;

use sqlx::PgPool;

use crate::services::builds_root::{FetchBuilds, PutBuilds};
use crate::services::sessions::Token;
use crate::services::signatures::MessageVerifier;

#[derive(Clone)]
pub struct AppContext {
    db: PgPool,
    fetch_builds: FetchBuilds,
    put_builds: PutBuilds,
    session_verifier: Arc<MessageVerifier<Token>>,
}

impl AppContext {
    pub fn new(db: PgPool, put_builds: PutBuilds, fetch_builds: FetchBuilds) -> Self {
        Self {
            db,
            fetch_builds,
            put_builds,
            session_verifier: Arc::new(MessageVerifier::default()),
        }
    }

    pub fn db(&self) -> &PgPool {
        &self.db
    }

    pub fn fetch_builds(&self) -> &FetchBuilds {
        &self.fetch_builds
    }

    pub fn put_builds(&self) -> &PutBuilds {
        &self.put_builds
    }

    pub fn session_verifier(&self) -> &MessageVerifier<Token> {
        &self.session_verifier
    }
}
