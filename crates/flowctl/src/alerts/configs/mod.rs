use crate::graphql::*;
use crate::output::{self, JsonCell, to_table_row};
use anyhow::{Context, anyhow};
use json::ptr::{Pointer, Token};
use std::io::Read;

#[derive(Debug, clap::Args)]
pub struct Configs {
    #[clap(subcommand)]
    cmd: Command,
}

#[derive(Debug, clap::Subcommand)]
pub enum Command {
    /// List alert configs the caller has read access to
    List(ListArgs),
    /// Create or update an alert config row, in either patch or whole-row mode
    Update(UpdateArgs),
}

#[derive(Debug, clap::Args)]
pub struct ListArgs {
    /// Filter rows whose `catalog_prefix_or_name` starts with this value.
    /// Pass an exact catalog name to inspect a single row.
    #[clap(long)]
    pub prefix: Option<String>,
}

#[derive(Debug, clap::Args)]
pub struct UpdateArgs {
    /// Catalog prefix (ending in `/`) or exact catalog name to update.
    /// Required because writes target a specific row; there is no default.
    #[clap(long)]
    pub prefix: String,

    /// Set a single field by dotted path (repeatable).
    /// Examples: `shardFailed.enabled=true`, `shardFailed.failureThreshold=10`,
    /// `taskIdle.threshold=60d`. Values are parsed as JSON if possible
    /// (so `true`, `42`, `"quoted"` work as expected); otherwise treated as
    /// a string. Mutually exclusive with `--config`.
    #[clap(long, value_name = "PATH=VALUE", conflicts_with = "config")]
    pub set: Vec<String>,

    /// Remove a single field or subtree by dotted path (repeatable).
    /// Mutually exclusive with `--config`.
    #[clap(long, value_name = "PATH", conflicts_with = "config")]
    pub unset: Vec<String>,

    /// Replace the entire row's config from a YAML or JSON file.
    /// Pass `-` to read from stdin.
    #[clap(long, value_name = "FILE")]
    pub config: Option<String>,

    /// Optional human-readable detail stored on the row.
    /// Omitting this flag preserves the existing detail.
    #[clap(long)]
    pub detail: Option<String>,
}

impl Configs {
    pub async fn run(&self, ctx: &mut crate::CliContext) -> anyhow::Result<()> {
        match &self.cmd {
            Command::List(args) => do_list(args, ctx).await,
            Command::Update(args) => do_update(args, ctx).await,
        }
    }
}

#[derive(graphql_client::GraphQLQuery)]
#[graphql(
    schema_path = "../flow-client/control-plane-api.graphql",
    query_path = "src/alerts/configs/list-query.graphql",
    response_derives = "Serialize,Clone,Debug",
    variables_derives = "Clone,Debug"
)]
struct ListAlertConfigs;

#[derive(graphql_client::GraphQLQuery)]
#[graphql(
    schema_path = "../flow-client/control-plane-api.graphql",
    query_path = "src/alerts/configs/update-mutation.graphql",
    response_derives = "Serialize,Clone,Debug",
    variables_derives = "Clone,Debug"
)]
struct UpdateAlertConfig;

const PAGE_SIZE: i64 = 50;

async fn fetch_all(
    ctx: &mut crate::CliContext,
    filter: Option<list_alert_configs::AlertConfigsFilter>,
) -> anyhow::Result<Vec<list_alert_configs::SelectAlertConfig>> {
    let mut rows = Vec::new();
    let mut after: Option<String> = None;
    loop {
        let vars = list_alert_configs::Variables {
            filter: filter.clone(),
            after: after.clone(),
            first: Some(PAGE_SIZE),
        };
        let resp = post_graphql::<ListAlertConfigs>(&ctx.client, vars)
            .await
            .context("failed to fetch alert configs")?;

        for edge in resp.alert_configs.edges {
            rows.push(edge.node);
        }

        let page = resp.alert_configs.page_info;
        if !page.has_next_page || page.end_cursor.is_none() {
            break;
        }
        after = page.end_cursor;
    }
    Ok(rows)
}

async fn do_list(args: &ListArgs, ctx: &mut crate::CliContext) -> anyhow::Result<()> {
    let filter = args
        .prefix
        .as_ref()
        .map(|p| list_alert_configs::AlertConfigsFilter {
            catalog_prefix_or_name: Some(list_alert_configs::PrefixFilter {
                starts_with: Some(p.clone()),
            }),
        });
    let rows = fetch_all(ctx, filter).await?;
    ctx.write_all(rows, ())
}

impl output::CliOutput for list_alert_configs::SelectAlertConfig {
    type TableAlt = ();
    type CellValue = JsonCell;

    fn table_headers(_: Self::TableAlt) -> Vec<&'static str> {
        vec!["Prefix or Name", "Config", "Detail", "Updated At"]
    }

    fn into_table_row(self, _: Self::TableAlt) -> Vec<Self::CellValue> {
        to_table_row(
            self,
            &["/catalogPrefixOrName", "/config", "/detail", "/updatedAt"],
        )
    }
}

async fn do_update(args: &UpdateArgs, ctx: &mut crate::CliContext) -> anyhow::Result<()> {
    if args.set.is_empty() && args.unset.is_empty() && args.config.is_none() {
        anyhow::bail!(
            "must supply at least one of --set, --unset, or --config; pass `--config -` and pipe `{{}}` to clear a row"
        );
    }

    let merged_config: serde_json::Value = if let Some(source) = args.config.as_deref() {
        load_whole_config(source)?
    } else {
        let existing = fetch_existing(ctx, &args.prefix).await?;
        let mut config = existing.unwrap_or_else(|| serde_json::Value::Object(Default::default()));
        for entry in &args.set {
            let (path, value) = parse_set_entry(entry)?;
            apply_set(&mut config, &path, value)?;
        }
        for entry in &args.unset {
            let path = parse_path(entry)?;
            apply_unset(&mut config, &path);
        }
        config
    };

    serde_json::from_value::<models::AlertConfig>(merged_config.clone()).with_context(|| {
        "merged alert config failed validation against models::AlertConfig: check field names, types, and that no unknown fields are set".to_string()
    })?;

    let vars = update_alert_config::Variables {
        catalog_prefix_or_name: args.prefix.clone(),
        config: models::RawValue::from_value(&merged_config),
        detail: args.detail.clone(),
    };
    let resp = post_graphql::<UpdateAlertConfig>(&ctx.client, vars).await?;

    let action = if resp.update_alert_config.created {
        "created"
    } else {
        "updated"
    };
    println!(
        "{action} alert config for {}",
        resp.update_alert_config.catalog_prefix_or_name
    );
    Ok(())
}

async fn fetch_existing(
    ctx: &mut crate::CliContext,
    prefix_or_name: &str,
) -> anyhow::Result<Option<serde_json::Value>> {
    let filter = Some(list_alert_configs::AlertConfigsFilter {
        catalog_prefix_or_name: Some(list_alert_configs::PrefixFilter {
            starts_with: Some(prefix_or_name.to_string()),
        }),
    });
    let rows = fetch_all(ctx, filter).await?;
    let exact = rows
        .into_iter()
        .find(|r| r.catalog_prefix_or_name == prefix_or_name);
    Ok(exact.map(|r| serde_json::from_str(r.config.get()).expect("server returned valid JSON")))
}

fn load_whole_config(source: &str) -> anyhow::Result<serde_json::Value> {
    let body = if source == "-" {
        use crossterm::tty::IsTty;
        anyhow::ensure!(
            !std::io::stdin().is_tty(),
            "stdin is a terminal; pipe a file or use `--config <path>` instead"
        );
        let mut buf = String::new();
        std::io::stdin()
            .read_to_string(&mut buf)
            .context("reading config from stdin")?;
        buf
    } else {
        std::fs::read_to_string(source).with_context(|| format!("reading config from {source}"))?
    };
    serde_yaml::from_str::<serde_json::Value>(&body)
        .with_context(|| format!("parsing config from {source} as YAML/JSON"))
}

fn parse_set_entry(entry: &str) -> anyhow::Result<(Pointer, serde_json::Value)> {
    let (path, raw) = entry
        .split_once('=')
        .ok_or_else(|| anyhow!("--set entry '{entry}' must be of form PATH=VALUE"))?;
    let path = parse_path(path)?;
    let value = serde_json::from_str::<serde_json::Value>(raw.trim())
        .unwrap_or_else(|_| serde_json::Value::String(raw.trim().to_string()));
    Ok((path, value))
}

fn parse_path(raw: &str) -> anyhow::Result<Pointer> {
    if raw.is_empty() {
        anyhow::bail!("path is empty");
    }
    let segments: Vec<&str> = raw.split('.').collect();
    if segments.iter().any(|segment| segment.is_empty()) {
        anyhow::bail!("path '{raw}' has an empty segment");
    }
    Ok(segments
        .into_iter()
        .map(|segment| Token::Property(segment.to_string()))
        .collect())
}

fn apply_set(
    target: &mut serde_json::Value,
    path: &Pointer,
    value: serde_json::Value,
) -> anyhow::Result<()> {
    if !target.is_object() {
        *target = serde_json::Value::Object(Default::default());
    }

    if let Some(target_location) = json::ptr::create_value(path, target) {
        *target_location = value;
        Ok(())
    } else {
        anyhow::bail!(
            "--set cannot descend into non-object at '{}'",
            display_dot_path(path)
        );
    }
}

fn apply_unset(target: &mut serde_json::Value, path: &Pointer) {
    let (last, intermediate) = match path.0.split_last() {
        Some(parts) => parts,
        None => return,
    };
    let mut cursor = target;
    for token in intermediate {
        let Token::Property(key) = token else {
            return;
        };
        let Some(obj) = cursor.as_object_mut() else {
            return;
        };
        let Some(next) = obj.get_mut(key) else {
            return;
        };
        cursor = next;
    }
    if let Some(obj) = cursor.as_object_mut() {
        let Token::Property(last) = last else {
            return;
        };
        obj.remove(last);
    }
}

fn display_dot_path(path: &Pointer) -> String {
    path.iter()
        .map(|token| match token {
            Token::Property(prop) => prop.clone(),
            _ => token.to_string(),
        })
        .collect::<Vec<_>>()
        .join(".")
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn parse_set_entry_coerces_json_literals() {
        let (path, value) = parse_set_entry("a.b=true").unwrap();
        assert_eq!(
            path,
            Pointer(vec![
                Token::Property("a".into()),
                Token::Property("b".into())
            ])
        );
        assert_eq!(value, serde_json::Value::Bool(true));

        let (_, value) = parse_set_entry("x=42").unwrap();
        assert_eq!(value, serde_json::json!(42));

        let (_, value) = parse_set_entry("x=\"30d\"").unwrap();
        assert_eq!(value, serde_json::Value::String("30d".to_string()));

        let (_, value) = parse_set_entry("x=30d").unwrap();
        assert_eq!(value, serde_json::Value::String("30d".to_string()));
    }

    #[test]
    fn apply_set_creates_intermediate_objects() {
        let mut target = serde_json::Value::Object(Default::default());
        apply_set(
            &mut target,
            &parse_path("shardFailed.enabled").unwrap(),
            serde_json::Value::Bool(true),
        )
        .unwrap();
        apply_set(
            &mut target,
            &parse_path("shardFailed.failureThreshold").unwrap(),
            serde_json::json!(10),
        )
        .unwrap();
        assert_eq!(
            target,
            serde_json::json!({
                "shardFailed": { "enabled": true, "failureThreshold": 10 }
            })
        );
    }

    #[test]
    fn apply_unset_removes_leaf_and_subtree() {
        let mut target = serde_json::json!({
            "shardFailed": { "enabled": true, "failureThreshold": 10 },
            "taskIdle": { "enabled": false }
        });
        apply_unset(
            &mut target,
            &parse_path("shardFailed.failureThreshold").unwrap(),
        );
        assert_eq!(
            target,
            serde_json::json!({
                "shardFailed": { "enabled": true },
                "taskIdle": { "enabled": false }
            })
        );
        apply_unset(&mut target, &parse_path("taskIdle").unwrap());
        assert_eq!(
            target,
            serde_json::json!({ "shardFailed": { "enabled": true } })
        );
    }

    #[test]
    fn apply_unset_missing_path_is_noop() {
        let mut target = serde_json::json!({ "a": { "b": 1 } });
        apply_unset(&mut target, &parse_path("x.y").unwrap());
        assert_eq!(target, serde_json::json!({ "a": { "b": 1 } }));
    }
}
