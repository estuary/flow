use super::floor;
use proto_gazette::broker;

/// A standby's read of its disk's journal, from the recovery floor `open`
/// resolved. It yields each response with the offset the read has reached.
pub type Read = futures::stream::BoxStream<'static, anyhow::Result<(broker::ReadResponse, i64)>>;

/// Open `journal`: list it, resolve its recovery floor, confirm its head, and
/// begin a blocking read from that floor. Returns the read, and the span from
/// its floor to the head.
pub async fn open(
    client: &gazette::journal::Client,
    journal: &str,
) -> anyhow::Result<(Read, std::ops::Range<i64>)> {
    let request = broker::ListRequest {
        selector: Some(broker::LabelSelector {
            include: Some(labels::build_set([("name", journal)])),
            exclude: None,
        }),
        watch: true,
        ..Default::default()
    };
    let mut watch = std::pin::pin!(client.clone().list_watch(request));

    let listing = loop {
        match futures::StreamExt::next(&mut watch).await {
            Some(Ok(listing)) => break listing,
            Some(Err(gazette::RetryError { attempt, inner })) if inner.is_transient() => {
                tracing::warn!(journal, attempt, %inner, "listing failed (will retry)");
            }
            // A broker fails a request which doesn't validate with Unknown. The journal
            // name is the listing's only input from the client.
            Some(Err(gazette::RetryError {
                inner: gazette::Error::Grpc(status),
                ..
            })) if status.code() == tonic::Code::Unknown => {
                return Err(crate::invalid_argument(format!(
                    "listing {journal}: {}",
                    status.message()
                )));
            }
            Some(Err(gazette::RetryError { inner, .. })) => {
                return Err(anyhow::Error::new(inner).context(format!("listing {journal}")));
            }
            None => unreachable!("a listing watch does not end"),
        }
    };
    let Some(listing) = listing.journals.into_iter().next() else {
        return Err(crate::invalid_argument(format!(
            "journal {journal} does not exist or is not a disk journal",
        )));
    };

    let floor = floor::parse(journal, &listing)?;
    // A fully suspended journal has no fragments to read, so its head is
    // set to the floor.
    let head = confirm_head(client, journal).await?.unwrap_or(floor);

    let request = broker::ReadRequest {
        journal: journal.to_string(),
        offset: floor,
        block: true,
        ..Default::default()
    };
    let read = read(journal.to_string(), floor, client.clone().read(request));

    Ok((futures::StreamExt::boxed(read), floor..head))
}

/// The `responses` of a blocking read of `journal` from `floor`.
fn read(
    journal: String,
    floor: i64,
    responses: impl futures::Stream<Item = gazette::RetryResult<broker::ReadResponse>> + Send + 'static,
) -> impl futures::Stream<Item = anyhow::Result<(broker::ReadResponse, i64)>> + Send + 'static {
    coroutines::try_coroutine(move |mut co| async move {
        let mut responses = std::pin::pin!(responses);
        let mut offset = floor;

        loop {
            match futures::StreamExt::next(&mut responses).await {
                Some(Ok(response)) => {
                    // Only a read at its floor may skip ahead, past content pruned below
                    // the real floor, which the label trails. Any later skip is a gap in
                    // content the read needs.
                    anyhow::ensure!(
                        response.offset == offset || offset == floor,
                        "reading {journal}: content this read needed was deleted from the \
                         store, from offset {offset} to {}",
                        response.offset,
                    );
                    offset = response.offset + response.content.len() as i64;
                    () = co.yield_((response, offset)).await;
                }
                Some(Err(gazette::RetryError { attempt, inner })) if inner.is_transient() => {
                    tracing::warn!(journal, attempt, %inner, "journal read failed (will retry)");
                }
                Some(Err(gazette::RetryError {
                    inner: gazette::Error::BrokerStatus(broker::Status::Suspended),
                    ..
                })) => {}
                Some(Err(gazette::RetryError { inner, .. })) => {
                    return Err(anyhow::Error::new(inner).context(format!("reading {journal}")));
                }
                None => unreachable!("a blocking read does not end"),
            }
        }
    })
}

async fn confirm_head(
    client: &gazette::journal::Client,
    journal: &str,
) -> anyhow::Result<Option<i64>> {
    let request = broker::AppendRequest {
        journal: journal.to_string(),
        suspend: broker::append_request::Suspend::Keep as i32,
        ..Default::default()
    };
    let mut stream = std::pin::pin!(client.append(request, futures::stream::empty));

    loop {
        match futures::StreamExt::next(&mut stream).await {
            Some(Ok(response)) => {
                let commit = response
                    .commit
                    .expect("an append which succeeded reports its commit");
                return Ok(Some(commit.end));
            }
            Some(Err(gazette::RetryError {
                inner: gazette::Error::BrokerStatus(broker::Status::Suspended),
                ..
            })) => return Ok(None),
            Some(Err(gazette::RetryError { attempt, inner })) if inner.is_transient() => {
                tracing::warn!(journal, attempt, %inner, "confirming the head failed (will retry)");
            }
            Some(Err(gazette::RetryError { inner, .. })) => {
                return Err(
                    anyhow::Error::new(inner).context(format!("confirming the head of {journal}"))
                );
            }
            None => unreachable!("an append stream does not end without a response"),
        }
    }
}

#[cfg(test)]
mod test {
    use proto_gazette::broker;

    #[tokio::test]
    async fn test_a_read_may_skip_ahead_only_from_its_floor() {
        // (name, responses as (offset, content length), offset read through, or the gap
        // a read refuses)
        //
        // Each read begins from a floor of 40.
        let cases = [
            ("from the floor", vec![(40, 34), (74, 10)], Ok(84)),
            // The store no longer holds content at the floor, so the read lands past
            // it, even with an empty response.
            ("skips ahead", vec![(100, 0), (100, 34), (134, 10)], Ok(144)),
            // Once a read has left its floor, a skip is a gap.
            ("gap", vec![(40, 34), (500, 10)], Err((74, 500))),
            (
                "gap after skipping",
                vec![(100, 34), (500, 10)],
                Err((134, 500)),
            ),
        ];

        for (name, responses, expect) in cases {
            let count = responses.len();
            let stream = responses.into_iter().map(|(offset, len)| {
                Ok(broker::ReadResponse {
                    offset,
                    content: vec![0; len].into(),
                    ..Default::default()
                })
            });
            let read = super::read(
                "acmeCo/disk/one".to_string(),
                40,
                futures::stream::iter(stream),
            );

            let outcome = async {
                let mut read = std::pin::pin!(read);
                let mut offset = 40;
                for _ in 0..count {
                    (_, offset) = futures::StreamExt::next(&mut read).await.unwrap()?;
                }
                anyhow::Ok(offset)
            }
            .await
            .map_err(|err| format!("{err:#}"));

            let expect = expect.map_err(|(from, to)| {
                format!(
                    "reading acmeCo/disk/one: content this read needed was deleted from the \
                     store, from offset {from} to {to}"
                )
            });
            assert_eq!(outcome, expect, "{name}");
        }
    }
}
