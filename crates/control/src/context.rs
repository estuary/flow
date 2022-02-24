use std::sync::Arc;

use sqlx::PgPool;

use crate::services::sessions::Token;
use crate::services::signatures::MessageVerifier;

#[derive(Clone)]
pub struct AppContext {
    db: PgPool,
    session_verifier: Arc<MessageVerifier<Token>>,
}

impl AppContext {
    pub fn new(db: PgPool) -> Self {
        Self {
            db,
            session_verifier: Arc::new(MessageVerifier::default()),
        }
    }

    pub fn db(&self) -> &PgPool {
        &self.db
    }

    pub fn session_verifier(&self) -> &MessageVerifier<Token> {
        &self.session_verifier
    }
}
