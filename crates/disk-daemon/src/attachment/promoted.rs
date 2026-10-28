use crate::device;
use crate::journal;
use crate::proto;
use anyhow::Context;
use proto::{request, response};
use tokio::sync::mpsc;

/// A promoted attachment, whose device the client uses.
pub(super) struct Promoted {
    /// Where the attachment is in its lifecycle.
    phase: Phase,
    /// The device, once it has started.
    device: Option<device::Device>,
    /// The writer this attachment's fence created.
    writer: journal::Writer,
}

enum Phase {
    /// The device start's blocking task.
    Starting(tokio::task::JoinHandle<anyhow::Result<device::Started>>),
    /// Started, awaiting the client's next `Prepare`.
    Serving,
    /// A delta was prepared and its ACK intent sent, awaiting the client's `Acknowledge`.
    Prepared {
        /// The delta's ACK intent, which the commit appends.
        ack_intent: bytes::Bytes,
        /// The recovery floor the commit installs, if the delta named one.
        floor: Option<i64>,
    },
}

impl Promoted {
    /// Begin starting a standby's `device`, which ends its rebuild of the image
    /// `content_tx` sends it, as the attachment whose fence created `writer`.
    pub fn start(
        device: device::Standby,
        content_tx: mpsc::Sender<(i64, bytes::Bytes)>,
        writer: journal::Writer,
    ) -> Self {
        let start = tokio::task::spawn_blocking(move || device.start(content_tx));

        Self {
            phase: Phase::Starting(start),
            device: None,
            writer,
        }
    }

    /// Wait for the device to start and reply `Promoted`, then serve each `Prepare`
    /// and its `Acknowledge`, until the client ends its stream.
    pub async fn serve(
        &mut self,
        requests: &mut tonic::Streaming<proto::Request>,
        responses: &mpsc::Sender<tonic::Result<proto::Response>>,
    ) -> anyhow::Result<()> {
        let Phase::Starting(start) = &mut self.phase else {
            panic!("a promoted attachment is served once");
        };
        // Awaited in place, so that if serving ends here, `stop` still finds the start
        // and tears down the device it creates.
        let started = start.await;
        self.phase = Phase::Serving;

        let device::Started {
            device,
            device_path,
            diskseq,
            mut live_rx,
        } = started??;
        self.device = Some(device);

        () = super::send(
            responses,
            response::Response::Promoted(proto::Promoted {
                device_path,
                diskseq,
            }),
        )
        .await?;

        loop {
            let recording = !matches!(self.phase, Phase::Prepared { .. });

            tokio::select! {
                request = super::recv(requests) => match request? {
                    None => return Ok(()),
                    Some(request) => () = self.on_request(request, &mut live_rx, responses).await?,
                },
                chunk = live_rx.recv(), if recording => {
                    let live = chunk.context("the disk's device failed")??;
                    () = self.writer.append(live).await?;
                }
            }
        }
    }

    async fn on_request(
        &mut self,
        request: request::Request,
        live_rx: &mut mpsc::Receiver<anyhow::Result<crate::LiveChunk>>,
        responses: &mpsc::Sender<tonic::Result<proto::Response>>,
    ) -> anyhow::Result<()> {
        match request {
            request::Request::Prepare(proto::Prepare {})
                if matches!(self.phase, Phase::Serving) =>
            {
                // A write's chunks are sent before it completes, so at most one chunk of
                // the client's flushed writes still waits here.
                loop {
                    match live_rx.try_recv() {
                        Ok(chunk) => () = self.writer.append(chunk?).await?,
                        Err(mpsc::error::TryRecvError::Empty) => break,
                        Err(mpsc::error::TryRecvError::Disconnected) => {
                            anyhow::bail!("the disk's device failed")
                        }
                    }
                }
                let (ack_intent, floor) = self.writer.prepare().await?;

                let prepared = proto::Prepared {
                    ack_intent: ack_intent.clone(),
                };
                () = super::send(responses, response::Response::Prepared(prepared)).await?;
                self.phase = Phase::Prepared { ack_intent, floor };
                Ok(())
            }
            request::Request::Acknowledge(proto::Acknowledge {}) => {
                let Phase::Prepared { ack_intent, floor } =
                    std::mem::replace(&mut self.phase, Phase::Serving)
                else {
                    return Err(crate::failed_precondition(
                        "no prepared delta awaits acknowledgement",
                    ));
                };
                () = self.writer.acknowledge(ack_intent, floor).await?;
                () = super::send(
                    responses,
                    response::Response::Acknowledged(proto::Acknowledged {}),
                )
                .await?;
                Ok(())
            }
            request::Request::Prepare(_) => Err(crate::failed_precondition(
                "a prepared delta is still awaiting its commit",
            )),
            _ => Err(crate::failed_precondition(
                "a promoted attachment serves only Prepare and Acknowledge",
            )),
        }
    }

    /// Tear down the disk: finish a device start still in flight, so the device it
    /// creates isn't left behind, then stop and delete the device. `serve` has dropped
    /// its receiver of live chunks, so the device applies the writes it still serves
    /// without waiting on the journal.
    pub async fn stop(self) -> anyhow::Result<()> {
        let Self {
            phase, mut device, ..
        } = self;

        if let Phase::Starting(start) = phase {
            match start
                .await
                .map_err(anyhow::Error::from)
                .and_then(|started| started)
            {
                Ok(started) => device = Some(started.device),
                Err(err) => tracing::error!(?err, "a disk's device failed to start"),
            }
        }
        let Some(device) = device else {
            return Ok(());
        };

        tokio::task::spawn_blocking(move || device.stop())
            .await
            .map_err(anyhow::Error::from)
            .and_then(|stopped| stopped)
    }
}
