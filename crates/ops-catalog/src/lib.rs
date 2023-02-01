use serde::{Deserialize, Serialize};
use sqlx::FromRow;

pub mod generate;
pub mod monitor;
pub mod render;

#[derive(Debug, FromRow, Serialize, Deserialize)]
pub struct TenantInfo {
    // The name of the tenant, including the trailing /.
    tenant: String,
    l1_stat_rollup: i32,
}
