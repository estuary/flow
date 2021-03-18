use super::invocation::Invocation;

use futures::{future::LocalBoxFuture, FutureExt};
use protocol::cgo;
use serde_json::Value;
use std::pin::Pin;
use std::task::{Context, Poll};

// Block is a set of derivation source documents which are processed as a vectorized unit.
pub struct Block {
    // Monotonic block ID (metadata only).
    pub id: usize,
    // Total number of source document bytes in this Block,
    // used as a rough estimate of the block size.
    pub num_bytes: usize,
    // For each source document, the index of its applicable derivation transform.
    pub transforms: Vec<u8>,
    // For each source document, it's packed key.
    // While tempting to turn this into an arena allocation, we must read Vec<u8>
    // from document protobuf headers already, and so just pass them through
    // without further copy. E.g., using an arena only makes sense if we also
    // avoid that header parsing allocation :shrug:.
    pub keys: Vec<Vec<u8>>,
    // Update invocations being built for each derivation transform.
    pub updates: Vec<Invocation>,
    // Publish invocations being built for each derivation transform.
    pub publishes: Vec<Invocation>,
}

impl std::fmt::Debug for Block {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Block")
            .field("id", &self.id)
            .field("num_docs", &self.keys.len())
            .field("num_bytes", &self.num_bytes)
            .finish()
    }
}

impl Block {
    pub fn new(id: usize, updates: &[Invocation], publishes: &[Invocation]) -> Block {
        Block {
            id,
            num_bytes: 0,
            updates: updates.to_owned(),
            publishes: publishes.to_owned(),
            transforms: Vec::with_capacity(32),
            keys: Vec::with_capacity(32),
        }
    }

    pub fn add_source(&mut self, transform_index: usize, packed_key: Vec<u8>, body: &[u8]) {
        self.num_bytes += body.len();
        self.transforms.push(transform_index as u8);
        self.keys.push(packed_key);
        self.updates[transform_index].add_source(body);
        self.publishes[transform_index].add_source(body);
    }

    // Invoke "update" lambdas, temporarily consuming this Block while lambdas are invoked.
    // The returned future will deliver this Block and its invocation results (or an error).
    pub fn invoke_updates(mut self, trampoline: &cgo::Trampoline) -> BlockInvoke {
        tracing::debug!(block = ?self, transforms = self.updates.len(), "invoking update lambdas");

        let it = std::mem::take(&mut self.updates)
            .into_iter()
            .enumerate()
            .map(|(tf_index, inv)| inv.invoke(tf_index, trampoline));

        BlockInvoke {
            block: Some(self),
            join: futures::future::join_all(it),
        }
    }

    // Invoke "publish" lambdas, temporarily consuming this Block while lambdas are invoked.
    // The returned future will deliver this Block and its invocation results (or an error).
    pub fn invoke_publish(mut self, trampoline: &cgo::Trampoline) -> BlockInvoke {
        tracing::debug!(block = ?self, transforms = self.publishes.len(), "invoking publish lambdas");

        let it = std::mem::take(&mut self.publishes)
            .into_iter()
            .enumerate()
            .map(|(tf_index, inv)| inv.invoke(tf_index, trampoline));

        BlockInvoke {
            block: Some(self),
            join: futures::future::join_all(it),
        }
    }
}

// BlockInvoke is a Future of a Block's concurrent invocation of transformation lambdas.
// It resolves the Block and lambda outputs, or an encountered error.
pub struct BlockInvoke {
    block: Option<Block>, // Held and returned with resolution of Future.

    // We must use JoinAll, and not TryJoinAll, because trampoline semantics require that
    // task invocation memory remain pinned until each task has returned. Otherwise the
    // first returned error would cause us to drop memory still being read by another task.
    join: futures::future::JoinAll<LocalBoxFuture<'static, Result<Vec<Vec<Value>>, anyhow::Error>>>,
}

impl std::future::Future for BlockInvoke {
    type Output = Result<(Block, Vec<Vec<Vec<Value>>>), anyhow::Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.join.poll_unpin(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => {
                let block = self.block.take().unwrap();
                let result = result.into_iter().collect::<Result<Vec<_>, _>>();

                tracing::debug!(?block, is_err = result.is_err(), "all invocations resolved");

                Poll::Ready(result.map(|v| (block, v)))
            }
        }
    }
}
