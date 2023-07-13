use crate::Client;
use proto_gazette::broker;


#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("grpc error: {0}")]
    GRPC(#[from] tonic::Status),

    #[error("append response not OK: {0:?}")]
    NotOk(broker::Status),
}

pub async fn append_once(
    client: &mut Client,
    journal: String,
    content: Vec<u8>,
) -> Result<(), Error> {
    let req = broker::AppendRequest {
        journal,
        content,
        ..Default::default()
    };
    let resp = client.append(futures::stream::once(async { req })).await?;
    let status = broker::Status::from_i32(resp.into_inner().status).unwrap();

    if status != broker::Status::Ok {
        Err(Error::NotOk(status))
    } else {
        Ok(())
    }
}
