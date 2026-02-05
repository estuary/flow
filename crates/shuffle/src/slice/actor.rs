use anyhow::Context;
use futures::{StreamExt, stream};
use proto_flow::shuffle;
use proto_gazette::broker;
use tokio::sync::mpsc;

pub struct SliceActor {
    pub cancel: tokens::CancellationToken,
    pub service: crate::Service,
    pub session_id: u64,
    pub members: Vec<shuffle::Member>,
    pub slice_member_index: u32,

    pub task_name: models::Name,
    pub bindings: Vec<crate::Binding>,
    pub clients: Vec<Option<gazette::journal::Client>>,

    pub queue_request_tx: Vec<mpsc::Sender<shuffle::QueueRequest>>,
    pub slice_response_tx: mpsc::Sender<tonic::Result<shuffle::SliceResponse>>,

    pub pending_reads: stream::FuturesUnordered<stream::StreamFuture<super::BoxedRead>>,
    pub parser: simd_doc::SimdParser,
}

impl SliceActor {
    #[tracing::instrument(
        level = "debug",
        err(level = "warn"),
        skip_all,
        fields(
            session = self.session_id,
            member = self.slice_member_index,
        )
    )]
    pub async fn rx_loop<R>(
        mut self,
        mut slice_request_rx: R,
        queue_response_rx: Vec<stream::BoxStream<'static, tonic::Result<shuffle::QueueResponse>>>,
    ) -> anyhow::Result<()>
    where
        R: futures::Stream<Item = tonic::Result<shuffle::SliceRequest>> + Send + Unpin + 'static,
    {
        let _ = &self.queue_request_tx; // TODO

        let _drop_guard = self.cancel.clone().drop_guard();

        // Await Start from the Session RPC.
        let verify = crate::verify("SliceRequest", "Start", &self.members[0].endpoint, 0);
        match verify.not_eof(slice_request_rx.next().await)? {
            shuffle::SliceRequest {
                start: Some(shuffle::slice_request::Start {}),
                ..
            } => (),
            request => return Err(verify.fail(request)),
        };

        // Start tasks that watch journal listings of assigned bindings.
        let mut listing_tasks: stream::FuturesUnordered<
            tokio::task::JoinHandle<Option<anyhow::Error>>,
        > = self
            .bindings
            .iter()
            .filter(|binding| {
                binding.index % self.members.len() == self.slice_member_index as usize
            })
            .map(|binding| {
                super::listing::spawn_listing(
                    binding,
                    journal_client(&self.service, &mut self.clients, binding, &self.task_name),
                    self.slice_response_tx.clone(),
                    self.cancel.clone(),
                )
            })
            .collect();

        // Build a Stream over receive Futures for every Queue RPC.
        let mut queue_response_rx: stream::FuturesUnordered<_> = queue_response_rx
            .into_iter()
            .enumerate()
            .map(next_queue_rx)
            .collect();

        // REMEMBER: we cannot send SliceResponse from this loop without risking deadlock.
        // We also cannot send QueueRequest from this loop.

        loop {
            tokio::select! {
                slice_request = slice_request_rx.next() => {
                    match slice_request {
                        Some(result) => self.on_slice_request(result).await?,
                        None => break Ok(()), // Clean EOF: shutdown.
                    }
                }
                Some((member_index, queue_response, rx)) = queue_response_rx.next() => {
                    self.on_queue_response(member_index, queue_response).await?;
                    queue_response_rx.push(next_queue_rx((member_index, rx)));
                }
                Some(listing_task) = listing_tasks.next() => {
                    self.on_listing_task(listing_task)?;
                }
                Some((result, read)) = self.pending_reads.next() => {
                    self.on_read_result(result, read).await?;

                    // Heap the read, drain some others, and then -- how do we start the next read?
                    todo!()
                }
            }
        }
    }

    async fn on_slice_request(
        &mut self,
        slice_request: tonic::Result<shuffle::SliceRequest>,
    ) -> anyhow::Result<()> {
        let verify = crate::verify("SliceRequest", "StartRead", &self.members[0].endpoint, 0);

        match verify.ok(slice_request)? {
            shuffle::SliceRequest {
                start_read: Some(start_read),
                ..
            } => {
                let read = self.on_start_read(start_read).await?;
                self.pending_reads.push(read.into_future());
                Ok(())
            }

            request => Err(verify.fail(request)),
        }
    }

    async fn on_queue_response(
        &mut self,
        member_index: usize,
        queue_response: Option<tonic::Result<shuffle::QueueResponse>>,
    ) -> anyhow::Result<()> {
        let verify = crate::verify(
            "QueueResponse",
            "TODO",
            &self.members[member_index].endpoint,
            member_index,
        );
        let queue_response = verify.not_eof(queue_response)?;

        match queue_response {
            response => Err(verify.fail(response)),
        }
    }

    fn on_listing_task(
        &mut self,
        listing_task: Result<Option<anyhow::Error>, tokio::task::JoinError>,
    ) -> anyhow::Result<()> {
        match listing_task {
            Err(err) => Err(anyhow::Error::new(err).context("listing task panicked")),
            Ok(None) => anyhow::bail!("listing task canceled before SliceActor::rx_loop exited"),
            Ok(Some(err)) => Err(err),
        }
    }

    pub async fn on_start_read(
        &mut self,
        start_read: shuffle::slice_request::StartRead,
    ) -> anyhow::Result<super::BoxedRead> {
        let shuffle::slice_request::StartRead {
            binding,
            spec,
            create_revision: _,
            mod_revision: _,
            route,
            checkpoint,
        } = start_read;

        let binding = self
            .bindings
            .get(binding as usize)
            .context("StartRead invalid binding")?;

        let spec = spec.context("StartRead missing spec")?;
        let client = journal_client(&self.service, &mut self.clients, binding, &self.task_name);
        let offset = checkpoint.iter().map(|p| p.offset).min().unwrap_or(0);

        let request = broker::ReadRequest {
            begin_mod_time: binding.not_before.to_unix().0 as i64,

            // Add `journal_read_suffix` as a metadata component to the journal name.
            // This helps identify the sources of reads from the perspective of a gazette broker.
            journal: format!("{};{}", spec.name, binding.journal_read_suffix),

            block: true,
            do_not_proxy: true,
            offset,
            end_offset: 0, // No end offset.
            metadata_only: false,
            header: route.map(|r| broker::Header {
                route: Some(r),
                ..Default::default()
            }),
        };

        Ok(super::read::new(client.read(request), binding.index as u32))
    }

    pub async fn on_read_result(
        &mut self,
        result: Option<anyhow::Result<gazette::journal::read::LinesBatch>>,
        mut read: super::BoxedRead,
    ) -> anyhow::Result<()> {
        let Some(result) = result else {
            tracing::info!(
                binding = read.binding(),
                journal = %read.fragment().journal,
                "read stream ended unexpectedly"
            );
            return Ok(());
        };
        let mut batch = result?;

        let result = simd_doc::transcode_many(
            &mut self.parser,
            &mut batch.content,
            &mut batch.offset,
            Default::default(),
        );

        let _transcoded = match result {
            Err((err, range)) => {
                anyhow::bail!(
                    "failed to parse JSON at offset {}-{} of journal {} (binding {}): {err}",
                    range.start,
                    range.end,
                    read.fragment().journal,
                    read.binding(),
                );
            }
            Ok(transcoded) => transcoded,
        };

        if !batch.content.is_empty() {
            read.as_mut().put_back(batch.content.into());
        }

        Ok(())
    }
}

fn journal_client(
    service: &crate::Service,
    clients: &mut Vec<Option<gazette::journal::Client>>,
    binding: &crate::Binding,
    task_name: &models::Name,
) -> gazette::journal::Client {
    let cell = &mut clients[binding.index];

    match cell {
        Some(client) => client.clone(),
        None => {
            let client = (service.gazette_factory)(binding.collection.clone(), task_name.clone());

            *cell = Some(client.clone());
            client
        }
    }
}

// Helper which builds a future that yields the next response from a member's Slice RPC.
async fn next_queue_rx(
    (member_index, mut rx): (
        usize,
        stream::BoxStream<'static, tonic::Result<shuffle::QueueResponse>>,
    ),
) -> (
    usize,                                                             // Member index.
    Option<tonic::Result<shuffle::QueueResponse>>,                     // Response.
    stream::BoxStream<'static, tonic::Result<shuffle::QueueResponse>>, // Stream.
) {
    (member_index, rx.next().await, rx)
}
