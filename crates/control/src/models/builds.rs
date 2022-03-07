use chrono::{DateTime, Utc};
use models;
use serde::{Deserialize, Serialize};
use sqlx::{types::Json, FromRow};
use std::fmt;

use crate::models::accounts;
use crate::models::id::Id;

/// Status of the Build.
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum State {
    Queued,
    Done,
    BuildFailed { code: Option<i32> },
    TestFailed { code: Option<i32> },
}

#[derive(Deserialize, FromRow, Serialize)]
pub struct Build {
    /// Account which owns this Build.
    pub account_id: Id<accounts::Account>,
    /// Root catalog built by this build, which may inline additional resources.
    /// The catalog may not be retrieved in all contexts.
    pub catalog: Option<Json<models::Catalog>>,
    /// When this record was created.
    pub created_at: DateTime<Utc>,
    /// Primary key for this record.
    pub id: Id<Build>,
    /// Connectors must be either a source or materialization.
    pub state: Json<State>,
    /// When this record was last updated.
    pub updated_at: DateTime<Utc>,
}

// TODO(johnny) impl Debug for Build is required by query_as!, but I'm not sure why.
impl fmt::Debug for Build {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Build")
            .field("account_id", &self.account_id)
            // catalog is omitted.
            .field("created_at", &self.created_at)
            .field("id", &self.id)
            .field("state", &self.state)
            .field("updated_at", &self.updated_at)
            .finish()
    }
}
