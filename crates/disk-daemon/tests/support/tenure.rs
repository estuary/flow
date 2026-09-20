//! One raw tenure stream of the daemon's gRPC.
//!
//! `disk_daemon::client` is what an ordinary case uses, and what a client should be
//! written on. This is for the cases which cannot be written on it: a request out of
//! turn, an acknowledgement of bytes nothing prepared, or two requests sent before
//! either reply is read. The client holds no protocol state of its own, so it cannot
//! express any of those, which is the point of it.

use disk_daemon::proto;

pub struct Tenure {
    requests: tokio::sync::mpsc::Sender<proto::Request>,
    responses: tonic::Streaming<proto::Response>,
}

impl Tenure {
    pub async fn open(channel: tonic::transport::Channel) -> Self {
        let mut client = proto_grpc::disk::disk_client::DiskClient::new(channel);

        let (requests, receiver) = tokio::sync::mpsc::channel(1);
        let responses = client
            .tenure(tokio_stream::wrappers::ReceiverStream::new(receiver))
            .await
            .expect("opening a tenure")
            .into_inner();

        Self {
            requests,
            responses,
        }
    }

    /// Open a disk, promote it at once, and return its mount path.
    ///
    /// The `Promote` is pipelined behind the `Open`, which is what a caller who wants a
    /// disk now does: the daemon fences as soon as it reads it, and answers `Opened`
    /// and then `Promoted`.
    pub async fn serve(&mut self, open: proto::Open) -> tonic::Result<std::path::PathBuf> {
        () = self.send(proto::request::Request::Open(open)).await;

        self.promote(Vec::new()).await
    }

    /// Open a disk and wait for its replay to become current, without promoting it.
    /// That is a hot standby.
    pub async fn stand_by(&mut self, open: proto::Open) -> tonic::Result<()> {
        match self.request(proto::request::Request::Open(open)).await? {
            proto::response::Response::Opened(proto::Opened {}) => Ok(()),
            response => panic!("expected Opened, got {response:?}"),
        }
    }

    /// Fence the journal, finish the replay, and serve the disk.
    ///
    /// This consumes the `Opened` which precedes `Promoted` whenever it has not been
    /// awaited already.
    pub async fn promote(
        &mut self,
        recovered_acks: Vec<bytes::Bytes>,
    ) -> tonic::Result<std::path::PathBuf> {
        match self
            .request(proto::request::Request::Promote(proto::Promote {
                recovered_acks,
            }))
            .await?
        {
            proto::response::Response::Promoted(promoted) => {
                return Ok(promoted.mount_path.into());
            }
            proto::response::Response::Opened(proto::Opened {}) => (),
            response => panic!("expected Opened or Promoted, got {response:?}"),
        }
        match self.reply().await? {
            proto::response::Response::Promoted(promoted) => Ok(promoted.mount_path.into()),
            response => panic!("expected Promoted, got {response:?}"),
        }
    }

    /// Cut a delta and return its acknowledgement. It is empty when the disk did not
    /// change.
    pub async fn prepare(&mut self) -> tonic::Result<bytes::Bytes> {
        () = self.send_prepare().await;

        self.prepared().await
    }

    pub async fn acknowledge(&mut self, ack: bytes::Bytes) -> tonic::Result<()> {
        () = self.send_acknowledge(ack).await;

        self.acknowledged().await
    }

    /// Send a `Prepare` without reading its reply, so that a case can put another
    /// request behind it.
    pub async fn send_prepare(&mut self) {
        self.send(proto::request::Request::Prepare(proto::Prepare {}))
            .await
    }

    /// Send an `Acknowledge` without reading its reply, as a client which pipelines
    /// the next transaction's `Prepare` behind it does.
    pub async fn send_acknowledge(&mut self, ack: bytes::Bytes) {
        self.send(proto::request::Request::Acknowledge(proto::Acknowledge {
            ack,
        }))
        .await
    }

    /// Read the reply owed to a `Prepare`, and require it to be that reply.
    pub async fn prepared(&mut self) -> tonic::Result<bytes::Bytes> {
        match self.reply().await? {
            proto::response::Response::Prepared(prepared) => Ok(prepared.ack),
            response => panic!("expected Prepared, got {response:?}"),
        }
    }

    /// Read the reply owed to an `Acknowledge`, and require it to be that reply.
    pub async fn acknowledged(&mut self) -> tonic::Result<()> {
        match self.reply().await? {
            proto::response::Response::Acknowledged(proto::Acknowledged {}) => Ok(()),
            response => panic!("expected Acknowledged, got {response:?}"),
        }
    }

    /// End the tenure as a client does, and wait for the daemon to finish tearing its
    /// disk down.
    pub async fn close(mut self) {
        drop(self.requests);
        assert_eq!(self.responses.message().await.expect("a clean close"), None);
    }

    /// Wait for a failed tenure to end. The daemon ends it only once the disk is
    /// destroyed.
    pub async fn ended(mut self) {
        drop(self.requests);
        assert!(matches!(self.responses.message().await, Ok(None) | Err(_)));
    }

    async fn request(
        &mut self,
        request: proto::request::Request,
    ) -> tonic::Result<proto::response::Response> {
        () = self.send(request).await;

        self.reply().await
    }

    /// Read whatever the daemon replies next, or the status which ended the tenure.
    pub async fn reply(&mut self) -> tonic::Result<proto::response::Response> {
        match self.responses.message().await? {
            Some(proto::Response { response }) => Ok(response.expect("a reply carries a message")),
            None => Err(tonic::Status::unknown("the tenure ended without a reply")),
        }
    }

    pub async fn send(&self, request: proto::request::Request) {
        () = self
            .send_message(proto::Request {
                request: Some(request),
            })
            .await;
    }

    /// Send a message exactly as given, including one which carries no request at
    /// all. Only a case about malformed messages needs this.
    pub async fn send_message(&self, request: proto::Request) {
        self.requests
            .send(request)
            .await
            .expect("the tenure is open");
    }
}
