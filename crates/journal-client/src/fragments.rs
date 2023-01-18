use crate::Client;
use futures::TryStream;
use proto_gazette::broker;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("grpc error: {0}")]
    GRPC(#[from] tonic::Status),
}

/// Returns journal fragments one at a time based on the given request, automatically fetching
/// subsequent pages if necessary.
pub struct FragmentIter {
    req: broker::FragmentsRequest,
    client: Option<Client>,
    current: std::vec::IntoIter<broker::fragments_response::Fragment>,
}

impl FragmentIter {
    /// Returns a new `FragmentIter`, which will lazily request additional fragments.
    pub fn new(client: Client, req: broker::FragmentsRequest) -> FragmentIter {
        FragmentIter {
            req,
            client: Some(client),
            current: Vec::new().into_iter(),
        }
    }

    pub async fn next(&mut self) -> Option<Result<broker::fragments_response::Fragment, Error>> {
        let FragmentIter {
            req,
            client,
            current,
        } = self;
        let mut next = current.next();
        if next.is_none() {
            if let Some(mut c) = client.take() {
                match c.list_fragments(req.clone()).await {
                    Ok(response) => {
                        if response.get_ref().next_page_token > 0 {
                            req.next_page_token = response.get_ref().next_page_token;
                            *client = Some(c); // don't put back client if there's not a next page
                        }
                        *current = response.into_inner().fragments.into_iter();
                        next = current.next();
                    }
                    Err(err) => return Some(Err(Error::GRPC(err))),
                }
            }
        }
        next.map(|r| Ok(r))
    }

    pub fn into_stream(
        self,
    ) -> impl TryStream<Ok = broker::fragments_response::Fragment, Error = Error> + 'static {
        futures::stream::try_unfold(self, |mut iter| async {
            let val = iter.next().await;
            if let Some(maybe_res) = val {
                match maybe_res {
                    Ok(res) => Ok(Some((res, iter))),
                    Err(err) => Err(err),
                }
            } else {
                Ok(None)
            }
        })
    }
}
