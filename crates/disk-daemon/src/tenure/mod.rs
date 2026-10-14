mod serving;

use crate::image;
use crate::journal;
use crate::proto;
use crate::ublk;
use anyhow::Context;

/// Serves the `Disk` gRPC service.
pub struct Service {
    /// Configuration shared by all tenures.
    config: std::sync::Arc<crate::daemon::Config>,
    /// Shared kernel device control handle.
    control: std::sync::Arc<ublk::Control>,
    /// Cancelled when the daemon drains, so that no tenure outlives it. Each
    /// tenure takes a child of this token and cancels it during teardown.
    draining: tokio_util::sync::CancellationToken,
}

impl Service {
    pub fn new(
        config: std::sync::Arc<crate::daemon::Config>,
        control: std::sync::Arc<ublk::Control>,
        draining: tokio_util::sync::CancellationToken,
    ) -> Self {
        Self {
            config,
            control,
            draining,
        }
    }

    pub fn into_tonic_service(self) -> proto_grpc::disk::disk_server::DiskServer<Self> {
        proto_grpc::disk::disk_server::DiskServer::new(self)
    }
}

#[tonic::async_trait]
impl proto_grpc::disk::disk_server::Disk for Service {
    type TenureStream = tokio_stream::wrappers::ReceiverStream<tonic::Result<proto::Response>>;

    async fn tenure(
        &self,
        request: tonic::Request<tonic::Streaming<proto::Request>>,
    ) -> tonic::Result<tonic::Response<Self::TenureStream>> {
        let (responses, stream) = tokio::sync::mpsc::channel(1);

        let (config, control) = (self.config.clone(), self.control.clone());
        let ended = self.draining.child_token();

        // Use the socket's authenticated peer credentials for mount ownership,
        // so the client needs no mount privilege.
        let peer = request
            .extensions()
            .get::<tonic::transport::server::UdsConnectInfo>()
            .and_then(|info| info.peer_cred)
            .map(|cred| (cred.uid(), cred.gid()));

        // The tenure owns its disk, so it outlives this call and tears the disk
        // down as it ends. A client which drops the stream both ends `requests`
        // and closes `responses`. Either of those ends the tenure.
        let actor = TenureActor {
            config,
            control,
            peer,
            journal: String::new(),
            ended,
            state: None,
        };
        tokio::spawn(actor.run(request.into_inner(), responses));

        Ok(tonic::Response::new(
            tokio_stream::wrappers::ReceiverStream::new(stream),
        ))
    }
}

/// What runs behind one tenure stream, on a task of its own: the tenure's state
/// machine, from `Open` until the stream ends and its disk is torn down.
struct TenureActor {
    config: std::sync::Arc<crate::daemon::Config>,
    control: std::sync::Arc<ublk::Control>,
    /// User and group of the client, from the peer credential of its stream. They
    /// own the mount this tenure serves.
    peer: Option<(u32, u32)>,
    /// Journal of the disk this tenure opened, and empty before `Open`. A daemon
    /// serves many tenures at once, so a failure names the disk it ended.
    journal: String,
    /// Cancelled when this tenure is over. That happens when the daemon drains,
    /// or when the tenure's own teardown begins. Every broker call the tenure
    /// makes gives up on this token, because each one retries indefinitely.
    ended: tokio_util::sync::CancellationToken,
    /// The disk this tenure holds. It holds none before `Open`, and none while
    /// `Promote` or the teardown has taken it.
    state: Option<State>,
}

enum State {
    /// The journal is accepted and its replay runs. The disk is not served, and this
    /// tenure has not claimed the journal. `Promote` moves it on.
    ///
    /// Boxed because a `journal::Standby` carries a whole journal writer's state,
    /// which is several times the size of a `Serving`.
    Standby(Box<Standby>),
    Promoted(serving::Serving),
}

/// A tenure whose replay runs, before it is promoted.
struct Standby {
    journal: journal::Standby,
    playback: journal::Playback,
    /// Set once `Opened` has been sent, which happens exactly once.
    ///
    /// It is also what tells a `Promote` whether it must wait for the replay:
    /// `Playback::caught_up` resolves once, and afterwards pends or reports the
    /// failure which ended the playback.
    opened: bool,
}

/// What a tenure acts on next.
enum Event {
    /// A request to serve, or `None` where the client closed its half of the stream
    /// and the tenure is over.
    ///
    /// Boxed because a `Request` carries whichever of the protocol's messages is
    /// largest, beside a `CaughtUp` which carries none.
    Request(Option<Box<proto::Request>>),
    /// The replay has read the history of the journal and now follows it.
    CaughtUp,
}

impl TenureActor {
    async fn run(
        mut self,
        mut requests: tonic::Streaming<proto::Request>,
        responses: tokio::sync::mpsc::Sender<tonic::Result<proto::Response>>,
    ) {
        // As every service's stream does, a panic ends the tenure like any other
        // failure, so its disk is still torn down below.
        let outcome = proto_grpc::catch_panic(self.serve(&mut requests, &responses)).await;

        // Cancel standby playback too; otherwise it keeps tailing and holds its
        // image and uncommitted delta after the stream ends.
        () = self.ended.cancel();

        // Keep the stream open through teardown, so clients and daemon drain
        // observe completion only after cleanup.
        if let Some(State::Promoted(serving)) = self.state.take() {
            () = serving.teardown().await;
        }

        if let Err(status) = outcome {
            tracing::warn!(journal = self.journal, %status, "tenure failed");
            _ = responses.send(Err(status)).await;
        }
    }

    /// Serve requests sequentially until EOF or failure. Promotion sends any owed
    /// `Opened` before `Promoted`, then later queued requests may run.
    /// A failure ends the stream; replacement promotion repairs any saved ack.
    async fn serve(
        &mut self,
        requests: &mut tonic::Streaming<proto::Request>,
        responses: &tokio::sync::mpsc::Sender<tonic::Result<proto::Response>>,
    ) -> anyhow::Result<()> {
        let cancelled = || {
            proto_grpc::status_to_anyhow(tonic::Status::cancelled("the client dropped its tenure"))
        };

        loop {
            let replies = match self.next(requests).await? {
                Event::Request(None) => return Ok(()), // The client closed its half.
                Event::Request(Some(request)) => self.request(*request).await?,
                Event::CaughtUp => {
                    let Some(State::Standby(standby)) = &mut self.state else {
                        panic!("only a standby tenure waits on its playback");
                    };
                    standby.opened = true;

                    vec![reply(proto::response::Response::Opened(proto::Opened {}))]
                }
            };

            for reply in replies {
                responses.send(Ok(reply)).await.map_err(|_| cancelled())?;
            }
        }
    }

    /// Wait for a request, daemon drain, or playback progress/failure.
    /// In-flight handlers observe cancellation through their broker calls and
    /// playback waits; this select only runs between requests.
    async fn next(
        &mut self,
        requests: &mut tonic::Streaming<proto::Request>,
    ) -> anyhow::Result<Event> {
        let ended = self.ended.clone();

        let playback = match &mut self.state {
            Some(State::Standby(standby)) => Some(&mut standby.playback),
            _ => None,
        };
        let caught_up = async {
            match playback {
                Some(playback) => playback.caught_up().await,
                None => std::future::pending().await,
            }
        };

        tokio::select! {
            _ = ended.cancelled() => Err(proto_grpc::status_to_anyhow(
                tonic::Status::unavailable("the daemon is draining"),
            )),
            request = requests.message() => Ok(Event::Request(
                request.map_err(proto_grpc::status_to_anyhow)?.map(Box::new),
            )),
            caught_up = caught_up => {
                () = caught_up?;
                Ok(Event::CaughtUp)
            }
        }
    }

    async fn request(&mut self, request: proto::Request) -> anyhow::Result<Vec<proto::Response>> {
        let request = request
            .request
            .ok_or_else(|| crate::invalid_argument("request carries no message".to_string()))?;

        let response = match request {
            proto::request::Request::Open(open) => {
                if self.state.is_some() {
                    return Err(proto_grpc::status_to_anyhow(
                        tonic::Status::failed_precondition("a tenure opens exactly one disk"),
                    ));
                }
                let standby = self.open(open).await?;
                self.state = Some(State::Standby(Box::new(standby)));

                // `Opened` follows when the replay has read the journal's history.
                Vec::new()
            }
            proto::request::Request::Promote(proto::Promote { mut recovered_acks }) => {
                // The state is checked before it is taken. Replacing a `Serving` here
                // would drop its disk out of order, ahead of the teardown which
                // unmounts it.
                if !matches!(self.state, Some(State::Standby(_))) {
                    return Err(proto_grpc::status_to_anyhow(
                        tonic::Status::failed_precondition(
                            "a tenure promotes a disk which it opened, and exactly once",
                        ),
                    ));
                }
                // All failures here are terminal. `run` cancels playback even after
                // this state has been taken.
                let Some(State::Standby(standby)) = self.state.take() else {
                    panic!("the state was just found to be a standby");
                };
                let Standby {
                    journal,
                    mut playback,
                    opened,
                } = *standby;

                // Fence before waiting for backfill so its target stops moving.
                let claimed = journal.claim_journal().await?;

                let mut replies = Vec::new();

                if !opened {
                    // Preserve reply order. Playback observes drain cancellation itself.
                    () = playback.caught_up().await?;

                    replies.push(reply(proto::response::Response::Opened(proto::Opened {})));
                }

                // A checkpoint may carry other journals' ack intents too.
                let recovered_ack = recovered_acks.remove(&self.journal);

                let serving = serving::Serving::open(
                    &self.config,
                    &self.control,
                    self.peer,
                    claimed,
                    playback,
                    recovered_ack,
                )
                .await?;

                let promoted = proto::Promoted {
                    mount_path: serving.mount.path().display().to_string(),
                };
                self.state = Some(State::Promoted(serving));
                replies.push(reply(proto::response::Response::Promoted(promoted)));

                replies
            }
            proto::request::Request::Prepare(proto::Prepare {}) => {
                let ack = self.serving()?.prepare().await?;

                vec![reply(proto::response::Response::Prepared(
                    proto::Prepared {
                        ack: ack.unwrap_or_default(),
                    },
                ))]
            }
            proto::request::Request::Acknowledge(proto::Acknowledge { ack }) => {
                () = self.serving()?.writer.acknowledge(ack).await?;

                vec![reply(proto::response::Response::Acknowledged(
                    proto::Acknowledged {},
                ))]
            }
        };

        Ok(response)
    }

    /// Begin the replay of the disk `open` names, without claiming or mounting
    /// anything.
    ///
    /// The journal must already exist: the daemon creates none, so an `Open` which
    /// names one nothing created is refused here. Its live specification is checked
    /// before a device exists, because a specification a disk could not be recovered
    /// from is a disk which can never prepare a delta.
    async fn open(&mut self, open: proto::Open) -> anyhow::Result<Standby> {
        let proto::Open {
            journal,
            device_size,
        } = open;

        if journal.is_empty() {
            return Err(crate::invalid_argument(
                "the tenure named no journal".to_string(),
            ));
        }

        let blocks = blocks(device_size)?;
        self.journal = journal.clone();

        let standby =
            journal::Standby::new(&self.config.brokers, journal, self.ended.clone()).await?;

        let image = image::Image::create(&self.config.image_dir, blocks)
            .with_context(|| format!("creating an image in {:?}", self.config.image_dir))?;

        let uncommitted =
            journal::UncommittedDelta::create(&self.config.image_dir).with_context(|| {
                format!(
                    "creating an uncommitted delta in {:?}",
                    self.config.image_dir
                )
            })?;
        let playback = standby.play(image, uncommitted);

        Ok(Standby {
            journal: standby,
            playback,
            opened: false,
        })
    }

    fn serving(&mut self) -> anyhow::Result<&mut serving::Serving> {
        let refused = match &mut self.state {
            Some(State::Promoted(serving)) => return Ok(serving),
            Some(State::Standby(_)) => {
                "this tenure has not promoted the disk it opened, so it is not its writer"
            }
            None => "the first request of a tenure must be Open",
        };
        Err(proto_grpc::status_to_anyhow(
            tonic::Status::failed_precondition(refused),
        ))
    }
}

/// One response of the tenure stream.
fn reply(response: proto::response::Response) -> proto::Response {
    proto::Response {
        response: Some(response),
    }
}

/// Block count of a device. `device_size` is the one durable geometry a tenure
/// supplies, because the block size is [`crate::BLOCK_SIZE`] for every disk.
fn blocks(device_size: u64) -> anyhow::Result<u32> {
    if device_size == 0 || !device_size.is_multiple_of(crate::BLOCK_SIZE as u64) {
        return Err(crate::invalid_argument(format!(
            "device size {device_size} must be a non-zero multiple of the {} byte block size",
            crate::BLOCK_SIZE,
        )));
    }
    let blocks = device_size / crate::BLOCK_SIZE as u64;

    if blocks > u32::MAX as u64 {
        return Err(crate::invalid_argument(format!(
            "a device of {blocks} blocks exceeds the 2^32 which a chunk indexes",
        )));
    }
    Ok(blocks as u32)
}

#[cfg(test)]
mod test {
    use super::blocks;

    #[test]
    fn test_a_devices_geometry_is_checked_before_it_exists() {
        let mut out = Vec::new();

        for device_size in [1 << 30, 0, 4097, 1 << 47] {
            let outcome = match blocks(device_size) {
                Ok(count) => format!("{count} blocks"),
                Err(err) => format!("{err:#}"),
            };
            out.push(format!("{device_size:<16}{outcome}"));
        }
        insta::assert_snapshot!(out.join("\n"), @"
        1073741824      262144 blocks
        0               device size 0 must be a non-zero multiple of the 4096 byte block size (InvalidArgument)
        4097            device size 4097 must be a non-zero multiple of the 4096 byte block size (InvalidArgument)
        140737488355328 a device of 34359738368 blocks exceeds the 2^32 which a chunk indexes (InvalidArgument)
        ");
    }
}
