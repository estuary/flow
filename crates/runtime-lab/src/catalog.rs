//! In-memory build of the experiment's catalog. The controller is the run's
//! source of truth for built specs: it builds once, and sends each spec to its
//! shards over `Task`.

use proto_flow::flow;
use std::collections::BTreeMap;

pub enum TaskSpec {
    Capture(flow::CaptureSpec),
    Materialization(flow::MaterializationSpec),
    Derivation(flow::CollectionSpec),
}

impl TaskSpec {
    pub fn name(&self) -> &str {
        match self {
            Self::Capture(spec) => &spec.name,
            Self::Materialization(spec) => &spec.name,
            Self::Derivation(spec) => &spec.name,
        }
    }

    pub fn shard_labels(&self) -> Option<&proto_gazette::LabelSet> {
        let template = match self {
            Self::Capture(spec) => spec.shard_template.as_ref(),
            Self::Materialization(spec) => spec.shard_template.as_ref(),
            Self::Derivation(spec) => spec
                .derivation
                .as_ref()
                .and_then(|d| d.shard_template.as_ref()),
        };
        template.and_then(|template| template.labels.as_ref())
    }

    pub fn task_type(&self) -> &'static str {
        match self {
            Self::Capture(_) => labels::TASK_TYPE_CAPTURE,
            Self::Materialization(_) => labels::TASK_TYPE_MATERIALIZATION,
            Self::Derivation(_) => labels::TASK_TYPE_DERIVATION,
        }
    }

    pub fn ops_task_type(&self) -> ops::TaskType {
        match self {
            Self::Capture(_) => ops::TaskType::Capture,
            Self::Materialization(_) => ops::TaskType::Materialization,
            Self::Derivation(_) => ops::TaskType::Derivation,
        }
    }

    /// Collections which the task writes. Bindings of a capture may share one.
    pub fn written_collections(&self) -> Vec<&flow::CollectionSpec> {
        match self {
            Self::Capture(spec) => {
                let mut collections: Vec<&flow::CollectionSpec> = Vec::new();
                for collection in spec.bindings.iter().filter_map(|b| b.collection.as_deref()) {
                    if !collections.iter().any(|c| c.name == collection.name) {
                        collections.push(collection);
                    }
                }
                collections
            }
            Self::Materialization(_) => Vec::new(),
            Self::Derivation(spec) => vec![spec],
        }
    }

    /// Override the append rate of every partition the task writes.
    pub fn set_max_append_rate(&mut self, rate: i64) {
        let templates = match self {
            Self::Capture(spec) => spec
                .bindings
                .iter_mut()
                .filter_map(|b| b.collection.as_deref_mut())
                .map(|c| &mut c.partition_template)
                .collect(),
            Self::Materialization(_) => Vec::new(),
            Self::Derivation(spec) => vec![&mut spec.partition_template],
        };
        for template in templates.into_iter().flatten() {
            template.max_append_rate = rate;
        }
    }

    pub fn encode(&self) -> bytes::Bytes {
        use prost::Message;
        match self {
            Self::Capture(spec) => spec.encode_to_vec().into(),
            Self::Materialization(spec) => spec.encode_to_vec().into(),
            Self::Derivation(spec) => spec.encode_to_vec().into(),
        }
    }
}

/// Build `source` with all connectors validated, returning every built
/// capture, materialization, and derivation by name. Any build error is fatal.
///
/// Live source collections resolve through the control plane under the user's
/// token. The build and publication IDs exceed every real one, so live
/// collections always appear older than the build.
pub async fn build(
    session: &crate::auth::Session,
    source: &std::path::Path,
    connector_router: std::sync::Arc<dyn proto_grpc::connector::Router>,
) -> anyhow::Result<BTreeMap<String, TaskSpec>> {
    let source = build::arg_source_to_url(&source.to_string_lossy(), false)?;
    let project_root = build::project_root(&source);

    // We never use a file root jail when loading on a user's machine.
    let draft = build::load(&source, std::path::Path::new("/")).await;
    let draft = surface_errors(draft.into_result())?;

    let live = flowctl::local_specs::Resolver {
        pg: session.pg.clone(),
        access_token: Some(session.access_token()?),
    }
    .resolve(draft.all_catalog_names())
    .await;

    let output = build::local(
        models::Id::new([0xff; 8]),
        models::Id::new([0xff; 8]),
        connector_router,
        ops::tracing_log_handler,
        false,
        false,
        false,
        &project_root,
        draft,
        live,
    )
    .await;
    let build::Output { built, .. } = surface_errors(output.into_result())?;

    let mut specs = BTreeMap::new();
    for row in built.built_captures {
        if let Some(spec) = row.spec {
            specs.insert(spec.name.clone(), TaskSpec::Capture(spec));
        }
    }
    for row in built.built_materializations {
        if let Some(spec) = row.spec {
            specs.insert(spec.name.clone(), TaskSpec::Materialization(spec));
        }
    }
    for row in built.built_collections {
        if let Some(spec) = row.spec.filter(|spec| spec.derivation.is_some()) {
            specs.insert(spec.name.clone(), TaskSpec::Derivation(spec));
        }
    }
    Ok(specs)
}

fn surface_errors<T>(result: Result<T, tables::Errors>) -> anyhow::Result<T> {
    result.map_err(|errors| {
        let rendered = errors
            .iter()
            .map(|tables::Error { scope, error }| format!("{scope}: {error:#}"))
            .collect::<Vec<_>>()
            .join("\n");
        anyhow::anyhow!("catalog build failed:\n{rendered}")
    })
}
