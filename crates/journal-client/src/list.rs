use crate::Client;
use proto_gazette::broker;

pub async fn list_journals(
    client: &mut Client,
    selector: &broker::LabelSelector,
) -> Result<Vec<broker::JournalSpec>, tonic::Status> {
    let req = broker::ListRequest {
        selector: Some(selector.clone()),
    };
    let resp = client.list(req).await?;
    let specs = resp
        .into_inner()
        .journals
        .into_iter()
        .flat_map(|j| j.spec.into_iter())
        .collect();
    Ok(specs)
}
