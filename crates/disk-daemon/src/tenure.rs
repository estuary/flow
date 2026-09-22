//! The tenure service. One bidirectional stream serves exactly one disk.
//!
//! A tenure is a state machine over its stream. It begins with `Open`, which
//! creates the image and begins rebuilding it from the journal. `Promote` claims the
//! journal, finishes the replay, and creates the device, the filesystem, and the
//! mount over it. It then serves `Prepare` and `Acknowledge` pairs, which move the
//! disk's durable state forward atomically with the client's own commit. It ends
//! when the stream ends, for any reason at all, by unmounting and destroying
//! everything it made.
//!
//! One request is served at a time, and its replies are sent before the next one
//! is read. A client may pipeline, so a request queued behind `Promote` is served
//! once the disk is promoted.
//!
//! Every error is terminal. A device or broker failure is terminal because the
//! disk's contents can no longer be trusted to reach its journal. A protocol
//! violation is terminal because the client has lost track of which delta it owes
//! an acknowledgement. What differs is the code the stream ends with. That code is
//! the only part of a failure a client can act on. See [`crate::failure`].

use crate::failure;
use crate::image::Image;
use crate::journal;
use crate::proto;
use crate::serving::Serving;
use crate::ublk::Control;
use anyhow::Context;

/// Serves the `Disk` gRPC service.
pub struct Service {
    daemon: std::sync::Arc<crate::daemon::Config>,
    control: std::sync::Arc<Control>,
    /// Cancelled when the daemon drains, so that no tenure outlives it. Each
    /// tenure takes a child of this token and cancels it during teardown.
    draining: tokio_util::sync::CancellationToken,
}

impl Service {
    pub fn new(
        daemon: std::sync::Arc<crate::daemon::Config>,
        control: std::sync::Arc<Control>,
        draining: tokio_util::sync::CancellationToken,
    ) -> Self {
        Self {
            daemon,
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

        let (daemon, control) = (self.daemon.clone(), self.control.clone());
        let ended = self.draining.child_token();

        // The mount this tenure returns belongs to the client rather than to this
        // daemon, so that a client needs no privilege of its own. A Unix socket
        // carries the peer's credential, which is the one identity a client cannot
        // claim falsely.
        let owner = request
            .extensions()
            .get::<tonic::transport::server::UdsConnectInfo>()
            .and_then(|info| info.peer_cred)
            .map(|cred| (cred.uid(), cred.gid()));

        // The tenure owns its disk, so it outlives this call and tears the disk
        // down as it ends. A client which drops the stream both ends `requests`
        // and closes `responses`. Either of those ends the tenure.
        let tenure = Tenure {
            daemon,
            control,
            owner,
            journal: String::new(),
            ended,
            state: State::Fresh,
        };
        tokio::spawn(tenure.run(request.into_inner(), responses));

        Ok(tonic::Response::new(
            tokio_stream::wrappers::ReceiverStream::new(stream),
        ))
    }
}

struct Tenure {
    daemon: std::sync::Arc<crate::daemon::Config>,
    control: std::sync::Arc<Control>,
    /// User and group which own the mount this tenure serves, taken from the
    /// credential of its stream.
    owner: Option<(u32, u32)>,
    /// Journal of the disk this tenure opened, and empty before `Open`. A daemon
    /// serves many tenures at once, so a failure names the disk it ended.
    journal: String,
    /// Cancelled when this tenure is over. That happens when the daemon drains,
    /// or when the tenure's own teardown begins. Every broker call the tenure
    /// makes gives up on this token, because each one retries indefinitely.
    ended: tokio_util::sync::CancellationToken,
    state: State,
}

enum State {
    /// Before `Open`, which must be a tenure's first request.
    Fresh,
    /// The journal is accepted and its replay runs. The disk is not served, and this
    /// tenure has not claimed the journal. `Promote` moves it on.
    ///
    /// Boxed because an `Opening` carries a whole journal writer's state, which is
    /// several times the size of a `Serving`.
    Standing(Box<Standing>),
    Serving(Serving),
}

/// A tenure whose replay runs, before it is promoted.
struct Standing {
    opening: journal::Opening,
    playback: journal::playback::Playback,
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

impl Tenure {
    async fn run(
        mut self,
        mut requests: tonic::Streaming<proto::Request>,
        responses: tokio::sync::mpsc::Sender<tonic::Result<proto::Response>>,
    ) {
        let outcome = self.serve(&mut requests, &responses).await;

        // "The tenure is over" now means one thing, whatever state it ended in. A
        // tenure which was still standing by has nothing to tear down, and its
        // playback would otherwise go on tailing the journal — holding the image and
        // the delta buffer open — until the daemon itself exits. A serving tenure's
        // teardown cancels this token anyway, through `Writer::abandon`.
        () = self.ended.cancel();

        // The disk is torn down before the failure which ended the tenure is
        // reported, so that a client which sees its tenure end sees a disk
        // which is already gone. It is also how a draining daemon waits for its
        // tenures. This stream stays open until its disk is destroyed.
        if let State::Serving(serving) = std::mem::replace(&mut self.state, State::Fresh) {
            () = serving.teardown().await;
        }

        if let Err(status) = outcome {
            tracing::warn!(journal = self.journal, %status, "tenure failed");
            _ = responses.send(Err(status)).await;
        }
    }

    /// Serve requests until the client stops sending them, or something fails.
    ///
    /// One request is served at a time, and its replies are sent before the next is
    /// read. A client may still pipeline: the requests queue in the stream, and the
    /// replies come back in the order the requests were sent. A request queued behind
    /// a `Promote` is therefore served once the disk is promoted, because that handler
    /// waits for the replay itself and answers `Opened` and `Promoted` together.
    ///
    /// A failure ends the stream with its status rather than a reply. A client of a
    /// failed tenure has to open another one and repair the acknowledgement it holds
    /// either way, and Gazette de-duplicates a repair of one which had in fact landed.
    async fn serve(
        &mut self,
        requests: &mut tonic::Streaming<proto::Request>,
        responses: &tokio::sync::mpsc::Sender<tonic::Result<proto::Response>>,
    ) -> tonic::Result<()> {
        let cancelled = || tonic::Status::cancelled("the client dropped its tenure");

        loop {
            let replies = match self.next(requests).await? {
                Event::Request(None) => return Ok(()), // The client closed its half.
                Event::Request(Some(request)) => self.request(*request).await?,
                Event::CaughtUp => {
                    let State::Standing(standing) = &mut self.state else {
                        panic!("only a standing tenure waits on its playback");
                    };
                    standing.opened = true;

                    vec![reply(proto::response::Response::Opened(proto::Opened {}))]
                }
            };

            for reply in replies {
                responses.send(Ok(reply)).await.map_err(|_| cancelled())?;
            }
        }
    }

    /// Wait for whatever this tenure must act on next.
    ///
    /// A standing tenure waits on its playback as well as on its client. That is
    /// what lets `Opened` arrive without a client having asked for it, and it is also
    /// how a tenure which stands by learns that its playback died.
    ///
    /// This does not race a request which is already being served. What that request
    /// waits on gives up instead: its broker calls, and the replay a `Promote` waits
    /// to become current. Those are the only waits a tenure has which no timeout
    /// bounds.
    async fn next(
        &mut self,
        requests: &mut tonic::Streaming<proto::Request>,
    ) -> tonic::Result<Event> {
        let ended = self.ended.clone();

        let playback = match &mut self.state {
            State::Standing(standing) => Some(&mut standing.playback),
            _ => None,
        };
        let caught_up = async {
            match playback {
                Some(playback) => playback.caught_up().await,
                None => std::future::pending().await,
            }
        };

        tokio::select! {
            _ = ended.cancelled() => Err(tonic::Status::unavailable("the daemon is draining")),
            request = requests.message() => Ok(Event::Request(request?.map(Box::new))),
            caught_up = caught_up => {
                () = caught_up.map_err(failure::status)?;
                Ok(Event::CaughtUp)
            }
        }
    }

    async fn request(&mut self, request: proto::Request) -> tonic::Result<Vec<proto::Response>> {
        use proto::request::Request;
        use proto::response::Response;

        let request = request
            .request
            .ok_or_else(|| tonic::Status::invalid_argument("request carries no message"))?;

        let response = match request {
            Request::Open(open) => {
                if !matches!(self.state, State::Fresh) {
                    return Err(tonic::Status::failed_precondition(
                        "a tenure opens exactly one disk",
                    ));
                }
                let standing = self.open(open).await.map_err(failure::status)?;
                self.state = State::Standing(Box::new(standing));

                // `Opened` follows when the replay has read the journal's history.
                Vec::new()
            }
            Request::Promote(proto::Promote { recovered_acks }) => {
                // The state is checked before it is taken. Replacing a `Serving` here
                // would drop its disk out of order, ahead of the teardown which
                // unmounts it.
                if !matches!(self.state, State::Standing(_)) {
                    return Err(tonic::Status::failed_precondition(
                        "a tenure promotes a disk which it opened, and exactly once",
                    ));
                }
                // `Fresh` stands in for as long as this handler runs. Every failure
                // within it is terminal, no request is read meanwhile, and `run`
                // cancels `ended` afterwards — so a playback dropped here stops
                // tailing the journal rather than outliving the tenure.
                let State::Standing(standing) = std::mem::replace(&mut self.state, State::Fresh)
                else {
                    panic!("the state was just found standing");
                };
                let Standing {
                    opening,
                    mut playback,
                    opened,
                } = *standing;

                // The claim comes now, and not when the replay is current. A client
                // which pipelines this behind its `Open` is asking for the disk at
                // once, and the claim bounds what is left to read, per
                // `Opening::claim_journal`.
                let claimed = opening.claim_journal().await.map_err(failure::status)?;

                let mut replies = Vec::new();

                if !opened {
                    // `Opened` is owed first, so this promotion waits for it here
                    // rather than resuming from `next`. A drain reaches this wait
                    // through the playback, which reports `Ended` as its failure, so
                    // there is nothing for the tenure's own token to race.
                    () = playback.caught_up().await.map_err(failure::status)?;

                    replies.push(reply(Response::Opened(proto::Opened {})));
                }

                let serving = Serving::open(
                    &self.daemon,
                    &self.control,
                    self.owner,
                    claimed,
                    playback,
                    recovered_acks,
                )
                .await
                .map_err(failure::status)?;

                let promoted = proto::Promoted {
                    mount_path: serving.mount.path().display().to_string(),
                };
                self.state = State::Serving(serving);
                replies.push(reply(Response::Promoted(promoted)));

                replies
            }
            Request::Prepare(proto::Prepare {}) => {
                let ack = self.serving()?.prepare().await.map_err(failure::status)?;

                vec![reply(Response::Prepared(proto::Prepared {
                    ack: ack.unwrap_or_default(),
                }))]
            }
            Request::Acknowledge(proto::Acknowledge { ack }) => {
                () = self
                    .serving()?
                    .writer
                    .acknowledge(ack)
                    .await
                    .map_err(failure::status)?;

                vec![reply(Response::Acknowledged(proto::Acknowledged {}))]
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
    async fn open(&mut self, open: proto::Open) -> anyhow::Result<Standing> {
        let proto::Open {
            journal,
            device_size,
        } = open;

        failure::ensure_valid!(!journal.is_empty(), "the tenure named no journal");

        let blocks = blocks(device_size)?;
        self.journal = journal.clone();

        let mut opening =
            journal::Opening::new(&self.daemon.client, journal, self.ended.clone()).await?;

        let image = Image::create(&self.daemon.image_dir, blocks)
            .with_context(|| format!("creating an image in {:?}", self.daemon.image_dir))?;

        let buffer = journal::buffer::Buffer::create(&self.daemon.image_dir)
            .with_context(|| format!("creating a delta buffer in {:?}", self.daemon.image_dir,))?;
        let playback = opening.play(image, buffer);

        Ok(Standing {
            opening,
            playback,
            opened: false,
        })
    }

    fn serving(&mut self) -> tonic::Result<&mut Serving> {
        match &mut self.state {
            State::Serving(serving) => Ok(serving),
            State::Standing(_) => Err(tonic::Status::failed_precondition(
                "this tenure has not promoted the disk it opened, so it is not its writer",
            )),
            State::Fresh => Err(tonic::Status::failed_precondition(
                "the first request of a tenure must be Open",
            )),
        }
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
    failure::ensure_valid!(
        device_size != 0 && device_size.is_multiple_of(crate::BLOCK_SIZE as u64),
        "device size {device_size} must be a non-zero multiple of the {} byte block size",
        crate::BLOCK_SIZE,
    );
    let blocks = device_size / crate::BLOCK_SIZE as u64;

    failure::ensure_valid!(
        blocks <= u32::MAX as u64,
        "a device of {blocks} blocks exceeds the 2^32 which a chunk indexes",
    );
    Ok(blocks as u32)
}

#[cfg(test)]
mod test {
    use super::blocks;

    #[test]
    fn test_a_devices_geometry_is_checked_before_it_exists() {
        assert_eq!(blocks(1 << 30).unwrap(), 262144);

        for (device_size, expect) in [
            (0, "non-zero multiple"),
            (4097, "non-zero multiple"),
            (1 << 47, "exceeds the 2^32"),
        ] {
            let err = blocks(device_size).unwrap_err();
            assert!(format!("{err}").contains(expect), "{err}");
        }
    }
}
