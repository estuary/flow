//! Endpoint and resource configuration: the simulated database's tables, and
//! the knobs of its backfill and replication.

use anyhow::Context;

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct EndpointConfig {
    /// Seed of every generated value. Output is a pure function of the seed
    /// and the capture's position, so runs (and resumed runs) repeat exactly.
    #[serde(default)]
    pub seed: u64,
    /// Rows of each backfill chunk, as source-postgres's `backfill_chunk_size`.
    #[serde(default = "default_backfill_chunk_size")]
    pub backfill_chunk_size: u64,
    /// Replication transactions replayed after each backfill chunk: the log
    /// the database wrote while the chunk's query ran.
    #[serde(default = "default_txns_per_chunk")]
    pub txns_per_chunk: u64,
    /// Changes of each replication transaction.
    #[serde(default = "default_changes_per_txn")]
    pub changes_per_txn: u64,
    /// Tables of the simulated database.
    #[serde(default = "canned_tables")]
    pub tables: Vec<Table>,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Table {
    pub namespace: String,
    pub stream: String,
    /// Columns, in table order. Discovery reflects them into a schema.
    pub columns: Vec<Column>,
    /// Primary-key column names, in key order.
    pub key: Vec<String>,
    /// Rows at the start of the capture, which its backfill reads.
    pub rows: u64,
    /// Relative share of replication changes which touch this table.
    #[serde(default = "default_change_weight")]
    pub change_weight: f64,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Column {
    pub name: String,
    /// A Postgres type name: `bool`, `int2`, `int4`, `int8`, `float8`,
    /// `numeric`, `text`, `varchar(n)`, `bytea`, `date`, `time`, `timestamp`,
    /// `timestamptz`, `uuid`, or `jsonb`.
    #[serde(rename = "type")]
    pub type_: String,
    #[serde(default)]
    pub nullable: bool,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceConfig {
    pub namespace: String,
    pub stream: String,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ColumnType {
    Bool,
    Int2,
    Int4,
    Int8,
    Float8,
    Numeric,
    Text,
    Varchar(u32),
    Bytea,
    Date,
    Time,
    Timestamp,
    Timestamptz,
    Uuid,
    Jsonb,
}

impl ColumnType {
    pub fn parse(type_: &str) -> anyhow::Result<Self> {
        Ok(match type_ {
            "bool" | "boolean" => Self::Bool,
            "int2" | "smallint" => Self::Int2,
            "int4" | "integer" => Self::Int4,
            "int8" | "bigint" => Self::Int8,
            "float8" => Self::Float8,
            "numeric" => Self::Numeric,
            "text" => Self::Text,
            "bytea" => Self::Bytea,
            "date" => Self::Date,
            "time" => Self::Time,
            "timestamp" => Self::Timestamp,
            "timestamptz" => Self::Timestamptz,
            "uuid" => Self::Uuid,
            "jsonb" => Self::Jsonb,
            other => {
                let n = other
                    .strip_prefix("varchar(")
                    .and_then(|s| s.strip_suffix(')'))
                    .with_context(|| format!("unsupported column type {other:?}"))?;
                Self::Varchar(n.parse().with_context(|| format!("invalid {other:?}"))?)
            }
        })
    }
}

impl EndpointConfig {
    pub fn parse(config_json: &[u8]) -> anyhow::Result<Self> {
        let config: Self =
            serde_json::from_slice(config_json).context("parsing endpoint configuration")?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.backfill_chunk_size != 0,
            "backfillChunkSize must be > 0"
        );
        anyhow::ensure!(self.changes_per_txn != 0, "changesPerTxn must be > 0");

        for table in &self.tables {
            let name = format!("{}.{}", table.namespace, table.stream);
            anyhow::ensure!(!table.key.is_empty(), "table {name} has no key");
            anyhow::ensure!(
                table.change_weight >= 0.0,
                "table {name} has a negative changeWeight"
            );
            for column in &table.columns {
                ColumnType::parse(&column.type_)
                    .with_context(|| format!("column {} of table {name}", column.name))?;
            }
            for key in &table.key {
                let column = table
                    .columns
                    .iter()
                    .find(|c| &c.name == key)
                    .with_context(|| format!("key {key} of table {name} isn't a column"))?;
                anyhow::ensure!(
                    !column.nullable,
                    "key {key} of table {name} must not be nullable"
                );
            }
        }
        Ok(())
    }

    pub fn table(&self, resource: &ResourceConfig) -> anyhow::Result<&Table> {
        self.tables
            .iter()
            .find(|t| t.namespace == resource.namespace && t.stream == resource.stream)
            .with_context(|| {
                format!(
                    "no table {}.{} is declared in the endpoint configuration",
                    resource.namespace, resource.stream
                )
            })
    }
}

fn default_backfill_chunk_size() -> u64 {
    50_000
}
fn default_txns_per_chunk() -> u64 {
    20
}
fn default_changes_per_txn() -> u64 {
    10
}
fn default_change_weight() -> f64 {
    1.0
}

/// Tables of an endpoint configuration which declares none: a wide and
/// string-heavy table, a large table with a composite key, and a narrow,
/// hot table which takes most replication changes.
pub fn canned_tables() -> Vec<Table> {
    let col = |name: &str, type_: &str, nullable: bool| Column {
        name: name.to_string(),
        type_: type_.to_string(),
        nullable,
    };
    vec![
        Table {
            namespace: "public".to_string(),
            stream: "customers".to_string(),
            columns: vec![
                col("id", "int8", false),
                col("name", "text", false),
                col("email", "varchar(128)", false),
                col("region", "varchar(16)", false),
                col("balance", "numeric", false),
                col("is_active", "bool", false),
                col("notes", "text", true),
                col("created_at", "timestamptz", false),
                col("updated_at", "timestamptz", false),
            ],
            key: vec!["id".to_string()],
            rows: 2_000_000,
            change_weight: 1.0,
        },
        Table {
            namespace: "public".to_string(),
            stream: "orders".to_string(),
            columns: vec![
                col("customer_id", "int8", false),
                col("order_id", "int4", false),
                col("status", "varchar(16)", false),
                col("sku", "text", false),
                col("quantity", "int4", false),
                col("unit_price", "float8", false),
                col("amount", "numeric", false),
                col("warehouse_id", "int2", false),
                col("tracking", "uuid", true),
                col("placed_at", "timestamptz", false),
                col("shipped_on", "date", true),
            ],
            key: vec!["customer_id".to_string(), "order_id".to_string()],
            rows: 5_000_000,
            change_weight: 2.0,
        },
        Table {
            namespace: "public".to_string(),
            stream: "events".to_string(),
            columns: vec![
                col("id", "int8", false),
                col("kind", "varchar(32)", false),
                col("session", "uuid", false),
                col("seq", "int4", false),
                col("payload", "jsonb", true),
                col("occurred_at", "timestamp", false),
            ],
            key: vec!["id".to_string()],
            rows: 200_000,
            change_weight: 6.0,
        },
    ]
}
