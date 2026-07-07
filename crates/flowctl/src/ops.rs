use crate::collection::read::ReadBounds;
use proto_flow::flow;
use proto_gazette::broker;
use std::collections::BTreeMap;

/// The ops `logs` and `stats` collections, bundled from
/// `ops-catalog/ops-task-template.flow.yaml`.
/// Embedded so reads of these system-owned collections don't depend on the
/// control plane returning their (privileged) built spec.
const OPS_TASK_BUNDLE: &str = include_str!("../../../ops-catalog/ops-task-template.bundle.json");

#[derive(clap::Args, Debug)]
pub struct Logs {
    #[clap(flatten)]
    pub task: TaskSelector,

    #[clap(flatten)]
    pub bounds: ReadBounds,
}

/// Selects a Flow task.
#[derive(clap::Args, Debug, Default, Clone)]
pub struct TaskSelector {
    /// The name of the task
    #[clap(long)]
    pub task: String,
}

impl Logs {
    pub async fn run(&self, ctx: &mut crate::CliContext) -> anyhow::Result<()> {
        read_task_ops(ctx, &self.task.task, OpsCollection::Logs, &self.bounds).await
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum OpsCollection {
    Logs,
    Stats,
}

/// Read a task's ops logs or stats, printing each committed document as a JSON
/// line to stdout. The read is non-blocking unless `--follow`.
pub async fn read_task_ops(
    ctx: &crate::CliContext,
    task_name: &str,
    collection: OpsCollection,
    bounds: &ReadBounds,
) -> anyhow::Result<()> {
    let (_shard_id_prefix, ops_logs_journal, ops_stats_journal, _shard_client, journal_client) =
        crate::dataplane::user_task_authorization(
            &ctx.rest,
            &ctx.user_tokens,
            &ctx.router,
            task_name,
            models::Capability::Read,
        )
        .await?;

    let ops_journal = match collection {
        OpsCollection::Logs => ops_logs_journal,
        OpsCollection::Stats => ops_stats_journal,
    };

    // Build a partition selector that scopes to this task's single ops journal:
    //  - The exact `name` label narrows the broker List RPC to this one journal.
    //  - The `estuary.dev/field/name` partition label is what the shuffle Slice's
    //    PartitionFilter matches against (it ignores `name`) when projecting
    //    causal hints.
    let include = broker::LabelSet::default();
    let include = labels::add_value(include, "name", ops_journal.as_str());
    let include =
        labels::partition::encode_field_label(include, "name", &serde_json::json!(task_name))
            .expect("value is valid");

    let task = shuffle::proto::Task {
        task: Some(shuffle::proto::task::Task::CollectionPartitions(
            shuffle::proto::CollectionPartitions {
                collection: Some(ops_collection_spec(collection, &ops_journal).await),
                partition_selector: Some(broker::LabelSelector {
                    include: Some(include),
                    exclude: None,
                }),
                not_before: bounds.not_before(),
                not_after: None,
            },
        )),
    };

    // The task-scoped broker token authorizes LIST and READ of this task's ops
    // journals. There is a single binding (the ops collection), so the factory
    // ignores its (subject, object) arguments and yields the one journal client.
    let factory: gazette::journal::ClientFactory =
        std::sync::Arc::new(move |_authz_sub: String, _authz_obj: String| journal_client.clone());

    crate::shuffle_read::read_to_stdout(ctx.registry.clone(), task, factory, bounds.follow).await
}

/// Resolve the embedded ops `logs`/`stats` CollectionSpec and point it at this
/// task's actual ops journal. The built spec carries the real schema, key, and
/// partition fields of the ops collection; only its name and partition-template
/// name (build-time placeholders) need to be rewritten to reflect the actual
/// ops collection we're reading from.
async fn ops_collection_spec(collection: OpsCollection, ops_journal: &str) -> flow::CollectionSpec {
    let key = match collection {
        OpsCollection::Logs => "logs",
        OpsCollection::Stats => "stats",
    };

    let mut specs = build_ops_specs().await;
    let mut spec = specs
        .remove(key)
        .unwrap_or_else(|| panic!("embedded ops bundle is missing the {key} collection"));

    // ops journal: "<collection>/<generation-id>/kind=<type>/name=<task>/pivot=<hex>".
    // Everything before the first `/kind=` field is the partition template
    // prefix; that minus its trailing generation-id is the collection name.
    let partition_prefix = ops_journal
        .split_once("/kind=")
        .map(|(prefix, _)| prefix)
        .unwrap_or(ops_journal);
    let collection_name = partition_prefix
        .rsplit_once('/')
        .map(|(name, _generation_id)| name)
        .unwrap_or(partition_prefix);

    // `spec.name` is used only for logging. The shuffle crate strips
    // `partition_template.name` from each listed journal name to recover the
    // partition-field suffix that its PartitionFilter matches against.
    spec.name = collection_name.to_string();
    spec.partition_template.as_mut().unwrap().name = partition_prefix.to_string();

    spec
}

/// Build the embedded ops bundle into its `logs` and `stats` CollectionSpecs,
/// keyed by trailing path segment ("logs" / "stats"). The build is offline:
/// the collections carry no connectors and `build::NoOpCatalogResolver` supplies
/// a catch-all storage mapping, so no control-plane or runtime IO occurs.
async fn build_ops_specs() -> BTreeMap<String, flow::CollectionSpec> {
    use tables::CatalogResolver;

    let catalog: models::Catalog =
        serde_json::from_str(OPS_TASK_BUNDLE).expect("embedded ops bundle must be valid JSON");
    let draft: tables::DraftCatalog = catalog.into();

    let live = build::NoOpCatalogResolver
        .resolve(draft.all_spec_names().collect())
        .await;

    // Fixed, deterministic build ids and no connector network.
    let output = build::local(
        models::Id::new([32; 8]),
        models::Id::new([1; 8]),
        "",
        ops::tracing_log_handler,
        true, // No-op captures.
        true, // No-op derivations.
        true, // No-op materializations.
        &url::Url::parse("file:///").unwrap(),
        draft,
        live,
    )
    .await;
    let output = output.into_result().unwrap_or_else(|errors| {
        for tables::Error { scope, error } in errors.iter() {
            tracing::error!(%scope, ?error);
        }
        panic!("embedded ops bundle failed to build");
    });

    output
        .built
        .built_collections
        .iter()
        .filter_map(|c| {
            let spec = c.spec.as_ref()?;
            let key = c.collection.as_str().rsplit('/').next()?;
            matches!(key, "logs" | "stats").then(|| (key.to_string(), spec.clone()))
        })
        .collect()
}

#[cfg(test)]
mod test {
    use super::*;

    // Guards that the embedded ops bundle builds offline into usable specs and
    // that the collection name and journal-name prefix are applied.
    #[tokio::test]
    async fn embedded_ops_specs_resolve() {
        for (collection, suffix) in [
            (OpsCollection::Logs, "logs"),
            (OpsCollection::Stats, "stats"),
        ] {
            let journal = format!(
                "ops/tasks/acmeCo/the-plane/{suffix}/0011/kind=capture/name=acmeCo%2Ffoo/pivot=00"
            );
            let spec = ops_collection_spec(collection, &journal).await;

            // The collection name drops the generation-id segment that the
            // journal-name prefix (partition_template.name) retains.
            let want_name = format!("ops/tasks/acmeCo/the-plane/{suffix}");
            let want_prefix = format!("ops/tasks/acmeCo/the-plane/{suffix}/0011");
            assert_eq!(spec.name, want_name);
            assert_eq!(spec.partition_template.unwrap().name, want_prefix);
            assert!(!spec.write_schema_json.is_empty(), "schema was bundled");
            assert_eq!(
                spec.key,
                [
                    "/shard/name",
                    "/shard/keyBegin",
                    "/shard/rClockBegin",
                    "/ts"
                ]
            );
            assert_eq!(spec.partition_fields, ["kind", "name"]);
        }
    }
}
