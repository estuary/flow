use serde::de::DeserializeOwned;

#[derive(serde::Serialize, Default, Debug)]
pub struct SearchParams {
    pub query: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub page: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expand: Option<Vec<String>>,
}

pub async fn stripe_search<R: DeserializeOwned + Send + 'static>(
    client: &stripe::Client,
    resource: &str,
    mut params: SearchParams,
) -> anyhow::Result<Vec<R>> {
    let mut all_data = Vec::new();
    let mut page = None;
    loop {
        if let Some(p) = page {
            params.page = Some(p);
        }
        let resp: stripe::SearchList<R> = client
            .get_query(&format!("/{resource}/search"), &params)
            .await?;
        let count = resp.data.len();
        all_data.extend(resp.data);
        if count == 0 || !resp.has_more {
            break;
        }
        page = resp.next_page;
    }
    Ok(all_data)
}
