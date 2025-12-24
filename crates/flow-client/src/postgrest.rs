use std::collections::VecDeque;

pub fn new_client(pg_url: &url::Url, pg_api_token: &str) -> postgrest::Postgrest {
    postgrest::Postgrest::new(pg_url.as_str()).insert_header("apikey", pg_api_token)
}

pub async fn exec<T>(
    builder: postgrest::Builder,
    access_token: Option<&str>,
) -> Result<T, tonic::Status>
where
    for<'de> T: serde::Deserialize<'de>,
{
    // This is returning a reqwest::RequestBuilder, but it's a version of reqwest
    // which is pinned by the `postgrest` crate and differs from the `reqwest`
    // available at this crate's root.
    //
    // Similarly, as we cannot name the reqwest::Error type returned by a postgrest::Builder,
    // we're unable to destructure it and fall back to mapping any error to code Unknown.
    let mut builder = builder.build();

    if let Some(token) = access_token {
        builder = builder.bearer_auth(token);
    }

    let map_err = |err| tonic::Status::unknown(format!("{err}"));

    let response = builder.send().await.map_err(map_err)?;
    let status = response.status();

    if status.is_success() {
        let bytes = response.bytes().await.map_err(map_err)?;
        serde_json::from_slice(&bytes).map_err(|err| {
            tonic::Status::invalid_argument(format!(
                "failed to deserialize response body ({err})\n\tbody:{}",
                String::from_utf8_lossy(&bytes[..(bytes.len().min(500))])
            ))
        })
    } else {
        Err(tonic::Status::unknown(
            response.text().await.map_err(map_err)?,
        ))
    }
}

pub async fn exec_paginated<T>(
    builder: postgrest::Builder,
    access_token: Option<&str>,
) -> impl futures::stream::Stream<Item = Result<T, tonic::Status>>
where
    for<'de> T: serde::Deserialize<'de>,
{
    const PAGE_SIZE: usize = 500;
    let access_token = access_token.map(str::to_string);

    futures::stream::unfold(
        (0usize, builder, access_token, VecDeque::new(), false),
        |(offset, builder, access_token, mut rest, done)| async move {
            if let Some(item) = rest.pop_front() {
                return Some((Ok(item), (offset, builder, access_token, rest, done)));
            } else if done {
                return None;
            }

            // Subtract one because PostgREST ranges are inclusive.
            let cur = builder.clone().range(offset, offset + PAGE_SIZE - 1);

            let next = exec::<VecDeque<T>>(cur, access_token.as_ref().map(String::as_str)).await;

            match next {
                Err(err) => Some((Err(err), (offset, builder, access_token, rest, done))),
                Ok(next) if next.is_empty() => None,
                Ok(mut next) => {
                    let next_len = next.len();
                    let item = next.pop_front().unwrap();

                    Some((
                        Ok(item),
                        (
                            offset + next_len,
                            builder,
                            access_token,
                            next,
                            next_len < PAGE_SIZE,
                        ),
                    ))
                }
            }
        },
    )
}
