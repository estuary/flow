use crate::proto;
use anyhow::Context;
use futures::stream::BoxStream;
use tokio::sync::mpsc;

/// A single filled slot: the shard's Join payload plus parked streams it owns.
pub struct JoinSlot<R: prost::Message> {
    // Join request of this shard.
    pub join: proto::Join,
    // Shard's client request stream.
    pub request_rx: BoxStream<'static, tonic::Result<R>>,
    // Shard's client response channel.
    pub response_tx: mpsc::UnboundedSender<tonic::Result<R>>,
}

/// PendingJoin holds a partially complete set of shard-ordered JoinSlots.
#[derive(Default)]
pub struct PendingJoin<R: prost::Message> {
    slots: Vec<Option<JoinSlot<R>>>,
}

/// The outcome of feeding a single Join into `PendingJoin::register`.
pub enum JoinOutcome<R: prost::Message> {
    /// Slot filled. More shard Joins still need to arrive.
    Pending { filled: usize, target: usize },
    /// Final slot filled with a consensus Join.
    /// Caller takes ownership of slots and spawns the leader actor,
    /// and must drop this join key from the service map.
    Consensus(Vec<JoinSlot<R>>),
    /// The Join disagrees with the established target, and should retry.
    /// Caller takes ownership and broadcasts Joined retry,
    /// and must drop this join key from the service map.
    Disagreement(Vec<JoinSlot<R>>),
}

impl<R: prost::Message> PendingJoin<R> {
    /// Feed a Join into this PendingJoin.
    pub fn register(
        &mut self,
        join: proto::Join,
        request_rx: BoxStream<'static, tonic::Result<R>>,
        response_tx: mpsc::UnboundedSender<tonic::Result<R>>,
    ) -> JoinOutcome<R> {
        let slot = JoinSlot {
            join,
            request_rx,
            response_tx,
        };
        let index = slot.join.shard_index as usize;

        // Is this is the first Join to arrive?
        let Some(other) = self.slots.iter().flatten().next() else {
            if slot.join.shards.len() > 1 {
                self.slots.resize_with(slot.join.shards.len(), || None);
                self.slots[index] = Some(slot);

                return JoinOutcome::Pending {
                    filled: 1,
                    target: self.slots.len(),
                };
            } else {
                // Single-shard topology is immediately complete.
                return JoinOutcome::Consensus(vec![slot]);
            }
        };

        // Is `index` already taken, or does `join` topology disagree?
        if index >= self.slots.len()
            || self.slots[index].is_some()
            || slot.join.shards != other.join.shards
        {
            let slots: Vec<JoinSlot<R>> = self
                .slots
                .iter_mut()
                .filter_map(Option::take)
                .chain(std::iter::once(slot))
                .collect();

            return JoinOutcome::Disagreement(slots);
        }

        self.slots[index] = Some(slot);
        let filled = self.slots.iter().flatten().count();

        if filled == self.slots.len() {
            let slots = self.slots.drain(..).map(Option::unwrap).collect();
            JoinOutcome::Consensus(slots)
        } else {
            JoinOutcome::Pending {
                filled,
                target: self.slots.len(),
            }
        }
    }
}

/// Validate a Join message is well-formed and internally consistent.
// This routine doesn't test for valid 2D tiling of shard ranges,
// because the `shuffle` session will do so.
pub fn validate(join: &proto::Join) -> anyhow::Result<&str> {
    anyhow::ensure!(!join.shards.is_empty());
    anyhow::ensure!((join.shard_index as usize) < join.shards.len());
    anyhow::ensure!(!join.shuffle_directory.is_empty());

    let mut task_name = "";

    for (i, shard) in join.shards.iter().enumerate() {
        let labeling = shard
            .labeling
            .as_ref()
            .with_context(|| format!("shards[{i}].labeling is missing"))?;

        anyhow::ensure!(!labeling.task_name.is_empty());
        anyhow::ensure!(!shard.id.is_empty());
        anyhow::ensure!(shard.reactor.is_some());

        if i == 0 {
            task_name = &labeling.task_name;
        } else {
            anyhow::ensure!(labeling.task_name == task_name);
        }
    }

    Ok(task_name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    fn shard(task_name: &str, id: &str) -> proto::join::Shard {
        proto::join::Shard {
            id: id.into(),
            labeling: Some(::ops::ShardLabeling {
                task_name: task_name.into(),
                ..Default::default()
            }),
            reactor: Some(proto_gazette::broker::process_spec::Id::default()),
            etcd_create_revision: 1,
        }
    }

    fn join(shards: Vec<proto::join::Shard>, shard_index: u32) -> proto::Join {
        proto::Join {
            shards,
            shard_index,
            shuffle_directory: "/tmp".into(),
            ..Default::default()
        }
    }

    fn register(
        pending: &mut PendingJoin<proto::Materialize>,
        j: proto::Join,
    ) -> (
        JoinOutcome<proto::Materialize>,
        mpsc::UnboundedReceiver<tonic::Result<proto::Materialize>>,
    ) {
        let (tx, rx) = mpsc::unbounded_channel();
        let outcome = pending.register(j, futures::stream::empty().boxed(), tx);
        (outcome, rx)
    }

    #[test]
    fn validate_cases() {
        // (mutator, expect_ok)
        let cases: &[(&str, fn(&mut proto::Join), bool)] = &[
            ("well-formed", |_| {}, true),
            ("empty shards", |j| j.shards.clear(), false),
            ("shard_index out of range", |j| j.shard_index = 9, false),
            (
                "empty shuffle_directory",
                |j| j.shuffle_directory.clear(),
                false,
            ),
            ("missing labeling", |j| j.shards[0].labeling = None, false),
            (
                "empty task_name",
                |j| j.shards[0].labeling.as_mut().unwrap().task_name.clear(),
                false,
            ),
            ("empty shard id", |j| j.shards[0].id.clear(), false),
            ("missing reactor", |j| j.shards[0].reactor = None, false),
            (
                "task_name mismatch",
                |j| j.shards[1].labeling.as_mut().unwrap().task_name = "other".into(),
                false,
            ),
        ];

        for (name, mutate, expect_ok) in cases {
            let mut j = join(vec![shard("task", "s0"), shard("task", "s1")], 0);
            mutate(&mut j);
            assert_eq!(validate(&j).is_ok(), *expect_ok, "case: {name}");
        }
        // Spot-check the success path returns the task name.
        let j = join(vec![shard("task", "s0")], 0);
        assert_eq!(validate(&j).unwrap(), "task");
    }

    #[test]
    fn single_shard_is_immediate_consensus() {
        let mut p = PendingJoin::<proto::Materialize>::default();
        let (out, _) = register(&mut p, join(vec![shard("t", "s0")], 0));
        assert!(matches!(out, JoinOutcome::Consensus(s) if s.len() == 1));
        assert!(p.slots.is_empty()); // Never resized.
    }

    #[test]
    fn multi_shard_assembles_in_order_despite_arrival_order() {
        let mut p = PendingJoin::<proto::Materialize>::default();
        let shards = vec![shard("t", "s0"), shard("t", "s1"), shard("t", "s2")];

        // Arrive 2, 0, 1.
        for (step, idx) in [2u32, 0, 1].into_iter().enumerate() {
            let (out, _) = register(&mut p, join(shards.clone(), idx));
            match (step, out) {
                (0 | 1, JoinOutcome::Pending { filled, target }) => {
                    assert_eq!((filled, target), (step + 1, 3));
                }
                (2, JoinOutcome::Consensus(slots)) => {
                    let indices: Vec<u32> = slots.iter().map(|s| s.join.shard_index).collect();
                    assert_eq!(indices, vec![0, 1, 2]);
                }
                (s, _) => panic!("unexpected outcome at step {s}"),
            }
        }
    }

    #[test]
    fn disagreement_cases() {
        let three = vec![shard("t", "s0"), shard("t", "s1"), shard("t", "s2")];
        let two = vec![shard("t", "s0"), shard("t", "s1")];
        let mut two_alt = two.clone();
        two_alt[1].id = "other".into();

        // (label, second join)
        let cases: Vec<(&str, proto::Join)> = vec![
            ("duplicate shard_index", join(three.clone(), 0)),
            (
                "index out of range vs established topology",
                join(two.clone(), 1),
            ),
            ("same-size differing shards content", join(two_alt, 1)),
        ];

        for (name, second) in cases {
            let mut p = PendingJoin::<proto::Materialize>::default();
            let (_, _) = register(&mut p, join(three.clone(), 0));
            let (out, _) = register(&mut p, second);
            let JoinOutcome::Disagreement(slots) = out else {
                panic!("{name}: expected Disagreement");
            };
            assert_eq!(slots.len(), 2, "{name}: returns parked + new");
            assert!(p.slots.iter().all(Option::is_none), "{name}: drained");
        }
    }
}
