//! Synthetic shard topology for `flowctl preview`. The two shard lists
//! (the leader's `proto::join::Shard` and the shuffle service's
//! `shuffle::proto::Shard`) carry overlapping but distinct fields, both
//! built here off a shared even key-space split. All shards point at the
//! single ephemeral preview endpoint.

use proto_flow::flow;
use runtime_next::proto;

/// Compute `[key_begin, key_end]` for shard `i` of `count` over the full
/// `u32` key space.
fn key_range(i: u32, count: u32) -> (u32, u32) {
    let begin = if i == 0 {
        0
    } else {
        ((i as u64 * (u32::MAX as u64 + 1)) / count as u64) as u32
    };
    let end = if i == count - 1 {
        u32::MAX
    } else {
        (((i + 1) as u64 * (u32::MAX as u64 + 1)) / count as u64 - 1) as u32
    };
    (begin, end)
}

/// Build the per-shard `proto::join::Shard` list used in `Join` messages.
/// `id`, `reactor`, and `etcd_create_revision` are synthesized; the
/// `labeling.range` matches the shuffle topology for the same index so the
/// shuffle Session sees a consistent 2D tiling.
pub fn build_join_shards(
    count: u32,
    spec: &flow::MaterializationSpec,
) -> anyhow::Result<Vec<proto::join::Shard>> {
    let build = build_label(spec)?;

    Ok((0..count)
        .map(|i| {
            let (key_begin, key_end) = key_range(i, count);
            proto::join::Shard {
                id: format!("preview-shard-{i:03}"),
                labeling: Some(::ops::ShardLabeling {
                    task_name: spec.name.to_string(),
                    range: Some(flow::RangeSpec {
                        key_begin,
                        key_end,
                        r_clock_begin: 0,
                        r_clock_end: u32::MAX,
                    }),
                    build: build.clone(),
                    ..Default::default()
                }),
                reactor: Some(proto_gazette::broker::process_spec::Id {
                    zone: "local".to_string(),
                    suffix: format!("preview-{i:03}"),
                }),
                etcd_create_revision: 1,
            }
        })
        .collect())
}

fn build_label(spec: &flow::MaterializationSpec) -> anyhow::Result<String> {
    let labels = spec
        .shard_template
        .as_ref()
        .and_then(|template| template.labels.as_ref())
        .ok_or_else(|| anyhow::anyhow!("built materialization is missing shard labels"))?;

    Ok(labels::expect_one(labels, labels::BUILD)
        .map_err(anyhow::Error::from)?
        .to_string())
}
