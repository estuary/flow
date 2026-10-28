use disk_daemon::proto;

pub struct Attachment {
    requests: tokio::sync::mpsc::Sender<proto::Request>,
    responses: tonic::Streaming<proto::Response>,
}

impl Attachment {
    pub async fn open(channel: tonic::transport::Channel) -> Self {
        let mut client = proto_grpc::disk::disk_client::DiskClient::new(channel);

        let (requests, receiver) = tokio::sync::mpsc::channel(1);
        let responses = client
            .attach(tokio_stream::wrappers::ReceiverStream::new(receiver))
            .await
            .expect("opening an attachment")
            .into_inner();

        Self {
            requests,
            responses,
        }
    }

    /// Pipeline `requests`, then read a reply to each.
    pub async fn exchange(
        &mut self,
        requests: Vec<proto::request::Request>,
    ) -> tonic::Result<Vec<proto::response::Response>> {
        let count = requests.len();

        for request in requests {
            () = self.send(request).await;
        }
        let mut replies = Vec::with_capacity(count);

        for _ in 0..count {
            replies.push(self.reply().await?);
        }
        Ok(replies)
    }

    /// Close the attachment, and wait for the daemon to tear its disk down.
    pub async fn close(mut self) {
        drop(self.requests);
        assert_eq!(self.responses.message().await.expect("a clean close"), None);
    }

    /// Wait for the daemon to close a failed attachment.
    pub async fn closed(mut self) {
        drop(self.requests);
        assert!(matches!(self.responses.message().await, Ok(None) | Err(_)));
    }

    /// Read the next reply, or the status which failed the attachment.
    pub async fn reply(&mut self) -> tonic::Result<proto::response::Response> {
        match self.responses.message().await? {
            Some(proto::Response { response }) => Ok(response.expect("a reply carries a message")),
            None => Err(tonic::Status::unknown(
                "the attachment closed without a reply",
            )),
        }
    }

    pub async fn send(&self, request: proto::request::Request) {
        () = self
            .send_message(proto::Request {
                request: Some(request),
            })
            .await;
    }

    /// Send a message as is, including one with no request.
    pub async fn send_message(&self, request: proto::Request) {
        self.requests
            .send(request)
            .await
            .expect("the attachment is open");
    }
}
