use reqwest::{Client, Response};
use serde::Deserialize;
use serde_json::json;

use crate::interface::{Auth, Pagination, SourceDefinition, Stream};

pub async fn run_stream<
    A: Auth,
    P: Pagination,
    C: for<'de> Deserialize<'de>,
    S: Stream<Config = C>,
>(
    config: serde_json::Value,
    source_definition: SourceDefinition<A, P>,
    mut stream: S,
) -> Result<(), anyhow::Error> {
    // Prepare authentication
    let auth_config = parse_config_key(&config, "authentication")?;

    let mut auth = source_definition.auth;
    auth.prepare(auth_config)?;

    // Prepare pagination
    let pagination_config = parse_config_key(&config, "pagination")?;

    let mut pagination = source_definition.pagination;
    pagination.prepare(pagination_config)?;

    // Prepare stream
    let stream_config = parse_config_key(&config, &stream.key())?;
    stream.prepare(stream_config)?;

    // Request loop
    let client = Client::new();
    let mut last_response: Option<Response> = None;
    loop {
        // Create a new request
        let mut req = stream.create_request();

        // If we had a last response, call pagination logic to see if there are more pages for us to process
        if last_response.is_some() {
            let has_more_pages = pagination
                .paginate(&mut req, last_response.unwrap())
                .await?;

            if !has_more_pages {
                break;
            }
        }

        // Authenticate
        auth.authenticate(&mut req).await?;

        // Execute the request and store the response for next iteration of loop
        let response = client.execute(req).await?;
        last_response = Some(response);

        // TODO: this should be handled by a ratelimiting logic
        std::thread::sleep(std::time::Duration::from_millis(500));
    }
    Ok(())
}

fn parse_config_key<T: for<'de> Deserialize<'de>>(
    config: &serde_json::Value,
    key: &str,
) -> serde_json::Result<T> {
    serde_json::from_value::<T>(config.get(key).unwrap_or(&json!(())).clone())
}
