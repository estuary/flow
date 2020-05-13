use super::protocol::{journal_client::JournalClient, ReadRequest, ReadResponse};
use tonic::transport::Channel;

pub async fn foo_the_bar() -> Result<(), Box<dyn std::error::Error>> {
    let mut client: JournalClient<Channel> = JournalClient::connect("http://[::1]:50051").await?;

    let resp: tonic::Response<tonic::Streaming<ReadResponse>> = client
        .read(ReadRequest {
            journal: "foo/bar".to_owned(),
            offset: -1,
            block: false,
            ..ReadRequest::default()
        })
        .await?;
    let mut resp = resp.into_inner();

    while let Some(msg) = resp.message().await? {
        println!("read_response: {:?}", msg);
    }

    Ok(())
}
