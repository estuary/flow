//! Synthetic shard topology for a local run. The two shard lists (the leader's
//! `proto::join::Shard` and the shuffle service's `shuffle::proto::Shard`) carry
//! overlapping but distinct fields, both built here off the shared even key-space
//! split of [`labels::shard::even_splits`]. All shards point at the single
//! ephemeral loopback endpoint.

use proto_flow::flow;
use runtime_next::proto;

/// Build the per-shard `proto::join::Shard` list used by the materialize
/// preview driver. `reactor` and `etcd_create_revision` are synthesized; the
/// `labeling.range` matches the shuffle topology for the same index so the
/// shuffle Session sees a consistent 2D tiling.
pub fn build_materialize_join_shards(
    count: u32,
    spec: &flow::MaterializationSpec,
) -> anyhow::Result<Vec<proto::join::Shard>> {
    let labels = spec
        .shard_template
        .as_ref()
        .and_then(|template| template.labels.as_ref())
        .ok_or_else(|| anyhow::anyhow!("built materialization is missing shard labels"))?;
    build_join_shards(count, &spec.name, labels, labels::TASK_TYPE_MATERIALIZATION)
}

/// Build the per-shard `proto::join::Shard` list used by the capture preview
/// driver. Captures are leaderless and have no shuffle topology, but each
/// shard still carries its own key-range slice and identity.
pub fn build_capture_join_shards(
    count: u32,
    spec: &flow::CaptureSpec,
) -> anyhow::Result<Vec<proto::join::Shard>> {
    let labels = spec
        .shard_template
        .as_ref()
        .and_then(|template| template.labels.as_ref())
        .ok_or_else(|| anyhow::anyhow!("built capture is missing shard labels"))?;
    build_join_shards(count, &spec.name, labels, labels::TASK_TYPE_CAPTURE)
}

/// Build the per-shard `proto::join::Shard` list used by the derive preview
/// driver and by catalog tests. Like materialize, derivations have a leader and
/// a shuffle topology; the synthetic split is on key only (full r-clock range).
pub fn build_derive_join_shards(
    count: u32,
    spec: &flow::CollectionSpec,
) -> anyhow::Result<Vec<proto::join::Shard>> {
    let labels = spec
        .derivation
        .as_ref()
        .and_then(|d| d.shard_template.as_ref())
        .and_then(|template| template.labels.as_ref())
        .ok_or_else(|| anyhow::anyhow!("built derivation is missing shard labels"))?;
    build_join_shards(count, &spec.name, labels, labels::TASK_TYPE_DERIVATION)
}

fn build_join_shards(
    count: u32,
    task_name: &str,
    labels: &proto_gazette::LabelSet,
    task_type: &str,
) -> anyhow::Result<Vec<proto::join::Shard>> {
    let build = labels::expect_one(labels, labels::BUILD)
        .map_err(anyhow::Error::from)?
        .to_string();
    // A local run has no publication that assigned a generation, so shard IDs
    // take the shape they'd carry in production under generation zero.
    let id_prefix = assemble::shard_id_prefix(models::Id::zero(), task_name, task_type);

    Ok(labels::shard::even_splits(&id_prefix, count, 1)
        .into_iter()
        .enumerate()
        .map(|(i, split)| proto::join::Shard {
            id: split.id,
            labeling: Some(::ops::ShardLabeling {
                task_name: task_name.to_string(),
                range: Some(split.range),
                build: build.clone(),
                ..Default::default()
            }),
            reactor: Some(proto_gazette::broker::process_spec::Id {
                zone: "local".to_string(),
                suffix: format!("local-{i:03}"),
            }),
            etcd_create_revision: 1,
        })
        .collect())
}
