use sqlx::PgPool;

#[derive(Clone)]
pub struct AppContext {
    db: PgPool,
}

impl AppContext {
    pub fn new(db: PgPool) -> Self {
        Self { db }
    }

    pub fn db(&self) -> &PgPool {
        &self.db
    }
}
