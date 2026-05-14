use crate::{Meta, stats};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Time grain at which a `CatalogStats` row aggregates data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Grain {
    Hourly,
    Daily,
    Monthly,
}

impl std::fmt::Display for Grain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Grain::Hourly => "hourly",
            Grain::Daily => "daily",
            Grain::Monthly => "monthly",
        })
    }
}

/// Aggregated catalog stats for one `(catalog_name, ts)` pair at a specific
/// `Grain`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CatalogStats {
    #[serde(rename = "_meta")]
    pub meta: Meta,
    pub catalog_name: String,
    pub ts: DateTime<Utc>,
    pub stats_summary: StatsSummary,
    #[serde(default)]
    pub task_stats: TaskStats,
}

/// Combined totals across every task and collection contributing to the row.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StatsSummary {
    #[serde(default)]
    pub read_by_me: stats::DocsAndBytes,
    #[serde(default)]
    pub read_from_me: stats::DocsAndBytes,
    #[serde(default)]
    pub written_by_me: stats::DocsAndBytes,
    #[serde(default)]
    pub written_to_me: stats::DocsAndBytes,
    #[serde(default)]
    pub warnings: u64,
    #[serde(default)]
    pub errors: u64,
    #[serde(default)]
    pub failures: u64,
    #[serde(default)]
    pub usage_seconds: u64,
    #[serde(default)]
    pub txn_count: u64,
}

/// Per-task-kind breakouts: maps keyed by collection name for
/// captures (target) and materializations (source), and the single
/// derivation block (a derivation has one output collection).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskStats {
    #[serde(default)]
    pub capture: BTreeMap<String, stats::CaptureBinding>,
    #[serde(default)]
    pub derive: Option<stats::Derive>,
    #[serde(default)]
    pub materialize: BTreeMap<String, stats::MaterializeBinding>,
}
