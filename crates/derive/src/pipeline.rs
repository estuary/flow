use futures::channel::oneshot;
use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

/// PendingPipeline is a pipelined value which may, in the future,
/// be received by the current process.
pub struct PendingPipeline<T: Debug + Send + 'static> {
    rx: Option<oneshot::Receiver<T>>,
    tx: Option<oneshot::Sender<T>>,
}

/// HeldPipeline is a pipelined value which is currently held,
/// and will be dispatched to the next pipelined receiver on drop().
pub struct HeldPipeline<T: Debug + Send + 'static> {
    t: Option<T>,
    tx: Option<oneshot::Sender<T>>,
}

/// PendingPipeline is a Future which receives its value and converts to a HeldPipeline.
impl<T: Debug + Send + 'static> Future for PendingPipeline<T> {
    type Output = HeldPipeline<T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let rx = self.rx.as_mut().unwrap();
        pin_utils::pin_mut!(rx);

        match rx.poll(cx) {
            Poll::Ready(t) => {
                let (_, tx) = (self.rx.take(), self.tx.take());

                Poll::Ready(HeldPipeline {
                    t: Some(t.expect("pipeline rx")),
                    tx,
                })
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<T: Debug + Send + 'static> PendingPipeline<T> {
    /// Build a new PendingPipeline around the given value.
    pub fn new(t: T) -> PendingPipeline<T> {
        let (tx_init, rx) = oneshot::channel();
        let (tx, _) = oneshot::channel();
        tx_init.send(t).unwrap();

        PendingPipeline {
            rx: Some(rx),
            tx: Some(tx),
        }
    }

    // Chain a new PendingPipeline which will receive the instance before self.
    pub fn chain_before(&mut self) -> PendingPipeline<T> {
        let (tx_next, mut rx_next) = oneshot::channel();

        // Swap |rx_next| with out own |rx|. Post-condition:
        // * This PendingPipeline will deliver to |rx_next|,
        //   which is read by the returned PendingPipeline.
        // * That PendingPipeline will deliver to our former |tx|.
        std::mem::swap(&mut rx_next, self.rx.as_mut().unwrap());

        PendingPipeline {
            rx: Some(rx_next),
            tx: Some(tx_next),
        }
    }
}

impl<T: Debug + Send + 'static> Drop for PendingPipeline<T> {
    fn drop(&mut self) {
        match (self.rx.take(), self.tx.take()) {
            (Some(rx), Some(tx)) => {
                tokio::spawn(async move {
                    let t = rx.await.expect("pipeline rx (drop)");
                    let _ = tx.send(t);
                });
            }
            (None, None) => (), // Already done.
            dbg => panic!("PendingPipeline in inconsistent state: {:?}", dbg),
        }
    }
}

impl<T: Debug + Send + 'static> HeldPipeline<T> {
    pub fn into_inner(mut self) -> T {
        self.tx.take();
        self.t.take().unwrap()
    }
}

impl<T: Debug + Send + 'static> AsRef<T> for HeldPipeline<T> {
    fn as_ref(&self) -> &T {
        self.t.as_ref().unwrap()
    }
}

impl<T: Debug + Send + 'static> AsMut<T> for HeldPipeline<T> {
    fn as_mut(&mut self) -> &mut T {
        self.t.as_mut().unwrap()
    }
}

impl<T: Debug + Send + 'static> Drop for HeldPipeline<T> {
    fn drop(&mut self) {
        match (self.t.take(), self.tx.take()) {
            (Some(t), Some(tx)) => {
                let _ = tx.send(t);
            }
            (None, None) => (), // Already done.
            dbg => panic!("HeldPipeline in inconsistent state: {:?}", dbg),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn test_pipeline_flow() {
        // Build a pipeline fixture having order D, B, C, E, A.
        let mut a = PendingPipeline::new(42);
        let mut b = a.chain_before();
        let c = a.chain_before();
        let d = b.chain_before();
        let e = a.chain_before();

        assert_eq!(42, *d.await.as_mut());
        std::mem::drop(b); // Drop without reading it.
        assert_eq!(42, *c.await.as_mut());
        std::mem::drop(e); // Drop without reading.
        assert_eq!(42, a.await.into_inner());
    }
}
