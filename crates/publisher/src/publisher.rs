use bytes::BufMut;
use futures::StreamExt;
use proto_gazette::{broker, uuid};

/// Publisher is responsible for transactional publishing of documents to
/// journal partitions, creating partitions on-demand and as needed.
pub struct Publisher {
    // Re-useable Appenders for binding journals.
    appenders: super::AppenderGroup,
    // Subject used to scope journal authorizations.
    authz_subject: String,
    // Bindings of this Publisher.
    bindings: Vec<super::Binding>,
    // Lazily-initialized journal Client (and, for Mapped bindings, partitions
    // watch) for each `bindings` entry.
    binding_clients: Vec<super::LazyBindingClient>,
    // Factory for building journal Clients on demand.
    client_factory: gazette::journal::ClientFactory,
    // Clock used to stamp published document UUIDs.
    clock: uuid::Clock,
    // Re-useable buffer into which packed keys are extracted.
    packed_key_buf: bytes::BytesMut,
    // Re-useable buffer into which journal names are built.
    prefix_buf: String,
    // Producer used to stamp published document UUIDs.
    producer: uuid::Producer,
}

impl Publisher {
    /// Create a new Publisher for the given bindings, producer identity, and clock.
    ///
    /// `client_factory` is used to build lazy per-binding journal clients
    /// (one per entry of `bindings`), and to build ephemeral clients inside
    /// `write_intents` for ACK intents that do not match any current binding.
    /// `authz_subject` is passed through to this factory without modification,
    /// and a binding's `authz_object()` is the AuthZ object.
    ///
    /// The `producer` identifies this Publisher as a distinct writer and is
    /// embedded in every UUID it generates. The `clock` provides a monotonic
    /// timestamp for ordering documents within this producer's stream.
    pub fn new(
        authz_subject: String,
        bindings: Vec<super::Binding>,
        client_factory: gazette::journal::ClientFactory,
        producer: uuid::Producer,
        clock: uuid::Clock,
    ) -> Self {
        let binding_clients = bindings
            .iter()
            .map(|b| {
                let factory = client_factory.clone();
                let authz_subject = authz_subject.clone();
                let authz_object = b.authz_object().to_string();

                match b {
                    super::Binding::Mapped(_) => {
                        let init: crate::MappedClientInit = Box::new(move || {
                            let client = factory(authz_subject, authz_object.clone());
                            let partitions =
                                crate::watch::watch_partitions(client.clone(), &authz_object);
                            (client, partitions)
                        });
                        super::LazyBindingClient::Mapped(std::sync::LazyLock::new(init))
                    }
                    super::Binding::Fixed(_) => {
                        let init: crate::FixedClientInit =
                            Box::new(move || factory(authz_subject, authz_object));
                        super::LazyBindingClient::Fixed(std::sync::LazyLock::new(init))
                    }
                }
            })
            .collect();

        Self {
            appenders: super::AppenderGroup::new(),
            authz_subject,
            binding_clients,
            bindings,
            client_factory,
            clock,
            packed_key_buf: bytes::BytesMut::new(),
            prefix_buf: String::new(),
            producer,
        }
    }

    /// Advance the clock to reflect the current wall-clock time.
    ///
    /// Should be called at the start of each transaction to ensure UUIDs
    /// embed a reasonably current timestamp. The clock is monotonic and
    /// will not regress if called multiple times in quick succession.
    pub fn update_clock(&mut self) {
        let now = tokens::now();
        self.clock.update(uuid::Clock::from_unix(
            now.timestamp() as u64,
            now.timestamp_subsec_nanos(),
        ));
    }

    /// Enqueue a document for publication to the appropriate journal partition.
    ///
    /// Assigns a UUID with the given `flags` and passes it to `doc`, which
    /// returns `(binding_index, document)`. For Mapped bindings the document
    /// is mapped to a physical partition (creating one if needed, which may
    /// issue an Apply RPC). For Fixed bindings the binding's journal is used
    /// directly, with no key extraction or partition mapping. The document is
    /// serialized as newline-delimited JSON into the partition's Appender
    /// buffer, and checkpoint'd. The checkpoint may start a background Append
    /// RPC if the buffer exceeds the flush threshold.
    pub async fn enqueue<N: json::AsNode>(
        &mut self,
        doc: impl FnOnce(uuid::Uuid) -> tonic::Result<(usize, N)>,
        flags: uuid::Flags,
    ) -> tonic::Result<&crate::Appender> {
        // Invariant: buffers are always empty (but may have capacity).
        let prefix = std::mem::take(&mut self.prefix_buf);
        let packed_key = std::mem::take(&mut self.packed_key_buf);

        // Sequence the document.
        let uuid = proto_gazette::uuid::build(self.producer, self.clock.tick(), flags);
        let (binding_idx, doc) = doc(uuid)?;

        let (mut journal, mut packed_key) = match &self.bindings[binding_idx] {
            super::Binding::Mapped(mapped) => {
                let super::LazyBindingClient::Mapped(lazy) = &self.binding_clients[binding_idx]
                else {
                    unreachable!("Mapped binding has Mapped lazy client");
                };
                super::mapping::map_partition(mapped, lazy, &doc, prefix, packed_key).await?
            }
            super::Binding::Fixed(fixed) => {
                let mut prefix = prefix;
                prefix.push_str(&fixed.journal);
                (prefix, packed_key)
            }
        };

        let client = self.binding_clients[binding_idx].client();
        let appender = self.appenders.activate(&journal, client);

        // Enqueue the serialization to the Appender's buffer, then checkpoint.
        let mut writer = std::mem::take(&mut appender.buffer).writer();
        serde_json::to_writer(&mut writer, &doc::SerPolicy::noop().on(&doc))
            .expect("serialization of json::AsNode cannot fail");
        appender.buffer = writer.into_inner();
        appender.buffer.put_u8(b'\n');
        appender.checkpoint().await?;

        // Clear and reclaim buffers for reuse.
        journal.clear();
        self.prefix_buf = journal;
        packed_key.clear();
        self.packed_key_buf = packed_key;

        Ok(appender)
    }

    /// Enqueue an owned document for publication to the appropriate journal partition.
    ///
    /// This is the owned-document counterpart of [`Self::enqueue`]. It takes a
    /// `doc::OwnedNode` so bump-backed heap documents can be moved through the
    /// async partition-mapping path without requiring shared references to be
    /// held across await points.
    pub async fn enqueue_owned(
        &mut self,
        doc: impl FnOnce(uuid::Uuid) -> tonic::Result<(usize, doc::OwnedNode)>,
        flags: uuid::Flags,
    ) -> tonic::Result<(&crate::Appender, usize)> {
        // Invariant: buffers are always empty (but may have capacity).
        let prefix = std::mem::take(&mut self.prefix_buf);
        let packed_key = std::mem::take(&mut self.packed_key_buf);

        // Sequence the document.
        let uuid = proto_gazette::uuid::build(self.producer, self.clock.tick(), flags);
        let (binding_idx, doc) = doc(uuid)?;

        let (doc, mut journal, mut packed_key) = match &self.bindings[binding_idx] {
            super::Binding::Mapped(mapped) => {
                let super::LazyBindingClient::Mapped(lazy) = &self.binding_clients[binding_idx]
                else {
                    unreachable!("Mapped binding has Mapped lazy client");
                };
                super::mapping::map_partition_owned(mapped, lazy, doc, prefix, packed_key).await?
            }
            super::Binding::Fixed(fixed) => {
                let mut prefix = prefix;
                prefix.push_str(&fixed.journal);
                (doc, prefix, packed_key)
            }
        };

        let client = self.binding_clients[binding_idx].client();
        let appender = self.appenders.activate(&journal, client);

        // Enqueue the serialization to the Appender's buffer, then checkpoint.
        let buffer_len = appender.buffer.len();
        let mut writer = std::mem::take(&mut appender.buffer).writer();
        serde_json::to_writer(&mut writer, &doc::SerPolicy::noop().on_owned(&doc))
            .expect("serialization of doc::OwnedNode cannot fail");
        appender.buffer = writer.into_inner();
        let bytes_written = appender.buffer.len() - buffer_len;
        appender.buffer.put_u8(b'\n');
        appender.checkpoint().await?;

        // Clear and reclaim buffers for reuse.
        journal.clear();
        self.prefix_buf = journal;
        packed_key.clear();
        self.packed_key_buf = packed_key;

        Ok((appender, bytes_written))
    }

    /// Flush all active Appenders, ensuring every buffered byte is durably appended.
    ///
    /// Starts concurrent background Append RPCs for any Appenders with remaining
    /// data, then awaits their completion.
    pub async fn flush(&mut self) -> tonic::Result<()> {
        self.appenders.flush().await
    }

    /// Snapshot this producer's contribution to the current transaction.
    ///
    /// Ticks the clock to obtain a commit timestamp and returns it alongside
    /// the producer identity and the names of all journals that were actively
    /// appended to. The caller aggregates these across all producers in the
    /// transaction and passes them to `intents::build_transaction_intents()`.
    pub fn commit_intents(&mut self) -> (uuid::Producer, uuid::Clock, Vec<String>) {
        let clock = self.clock.tick();

        let journal_names = self
            .appenders
            .active_set()
            .map(|(name, _)| name.to_string())
            .collect();

        (self.producer, clock, journal_names)
    }

    /// Snapshot a backfill marker broadcast: every partition journal of a Mapped
    /// collection binding, plus a ticked commit clock and producer identity. Unlike
    /// [`Self::commit_intents`] (only appended journals), a marker must reach *every*
    /// partition so a reader observes it regardless of its selector.
    pub async fn marker_commit(
        &mut self,
        binding_idx: usize,
    ) -> tonic::Result<(uuid::Producer, uuid::Clock, Vec<String>)> {
        // Snapshot the journals to broadcast to, releasing the partitions watch
        // borrow before returning.
        let journals: Vec<String> = match &self.bindings[binding_idx] {
            super::Binding::Mapped(_) => {
                let super::LazyBindingClient::Mapped(lazy) = &self.binding_clients[binding_idx]
                else {
                    unreachable!("Mapped binding has Mapped lazy client");
                };
                let (_client, partitions) = &(**lazy);
                let partitions = partitions.ready().await;
                let refresh = partitions.token();
                refresh
                    .result()?
                    .iter()
                    .map(|split| split.name.to_string())
                    .collect()
            }
            super::Binding::Fixed(_) => {
                unreachable!("backfill markers are only broadcast to Mapped collection bindings")
            }
        };

        let clock = self.clock.tick();
        Ok((self.producer, clock, journals))
    }

    /// Write pre-serialized ACK intent documents to their journals.
    ///
    /// Takes the output of `intents::build_transaction_intents()` — per-journal
    /// NDJSON `Bytes` — and appends each to its journal in parallel. For each
    /// journal, this uses a hybrid client strategy:
    /// - If the journal matches a binding, reuse that binding's client.
    ///   For Mapped bindings the match is a prefix-match on the binding's
    ///   partitions prefix; for Fixed bindings it's an exact name match.
    /// - Otherwise, build an ephemeral client. This supports recovered ACK
    ///   intents that may reference journals no longer bound to the current
    ///   task (e.g. from a prior published task). For this class of journals,
    ///   a `NotFound` status is tolerated and handled by discarding the ACK
    ///   (it was intentionally deleted, for example due to a data reset)
    ///
    /// On Ok return, all ACKs have been durably appended (no need to flush).
    pub async fn write_intents<I>(&mut self, journal_intents: I) -> tonic::Result<()>
    where
        I: IntoIterator<Item = (String, bytes::Bytes)>,
    {
        let mut ephemeral = super::AppenderGroup::new();

        for (journal, intents) in journal_intents {
            super::validate_ndjson(&journal, &intents)?;

            // Attempt to find a binding that covers `journal`.
            // TODO(johnny): We're walking bindings for each ACK intent journal.
            // This is probably fine but _could_ be faster.
            // We can do this by building a sorted index of partition template name => binding.
            let binding_client = self
                .bindings
                .iter()
                .position(|b| match b {
                    super::Binding::Mapped(m) => journal.starts_with(&m.partitions_prefix),
                    super::Binding::Fixed(f) => journal == f.journal,
                })
                .map(|i| self.binding_clients[i].client());

            let appender = if let Some(client) = binding_client {
                self.appenders.activate(&journal, client)
            } else {
                let client = (self.client_factory)(self.authz_subject.clone(), journal.clone());
                ephemeral.activate(&journal, &client)
            };

            appender.buffer.extend_from_slice(&intents);
        }

        // Require that binding appenders flush nominally: journals must exist.
        self.appenders.flush().await?;
        self.appenders.sweep();

        let ephemeral_flushes = ephemeral
            .active_set()
            .map(|(journal, appender)| async move {
                match appender.flush().await {
                    Ok(()) => Ok(()),
                    Err(status) if status.code() == tonic::Code::NotFound => {
                        tracing::warn!(%journal, "discarding journal ACK-intent (not found)");
                        Ok(())
                    }
                    Err(status) => Err(status),
                }
            });
        _ = futures::future::try_join_all(ephemeral_flushes).await?;

        Ok(())
    }

    /// Take accumulated per-journal append-throttle samples since the last call.
    pub fn take_throttle_samples(&mut self) -> Vec<super::ThrottleSample<'_>> {
        self.appenders.take_throttle_samples()
    }

    /// Build a detached future which attempts to split partition `journal` at
    /// its key-range midpoint (see [`super::mapping::split_partition`]).
    ///
    /// Returns None when `journal` is not a partition of any Mapped binding
    /// (e.g. the fixed ops-stats journal) — such journals can never be split.
    ///
    /// The future owns cloned journal-client and partitions-watch handles, so
    /// the caller may park and poll it while this Publisher continues to
    /// publish. A stale watch read is benign: the split's CAS fails as a
    /// `Lost` outcome rather than acting on a layout that was never evaluated.
    pub fn split_partition(
        &self,
        journal: &str,
    ) -> Option<futures::future::BoxFuture<'static, tonic::Result<super::SplitOutcome>>> {
        use futures::FutureExt;

        let index = self.bindings.iter().position(|b| match b {
            super::Binding::Mapped(m) => journal.starts_with(&m.partitions_prefix),
            super::Binding::Fixed(_) => false,
        })?;
        let super::Binding::Mapped(binding) = &self.bindings[index] else {
            unreachable!("position matched a Mapped binding");
        };
        let super::LazyBindingClient::Mapped(lazy) = &self.binding_clients[index] else {
            unreachable!("Mapped binding has a Mapped lazy client");
        };

        // Force the lazy client + watch (warm in practice — `journal` was
        // appended to through this binding) and clone owned handles into the
        // detached future, along with the only part of the binding a split
        // reads: its partition template.
        let partitions_template = binding.partitions_template.clone();
        let (client, partitions) = &**lazy;
        let (client, partitions) = (client.clone(), partitions.clone());
        let journal = journal.to_string();

        Some(
            async move {
                super::mapping::split_partition(
                    &partitions_template,
                    &client,
                    &partitions,
                    &journal,
                )
                .await
            }
            .boxed(),
        )
    }

    /// Apply the `estuary.dev/truncated-at` journal label to the partitions of
    /// each active backfill. Journals already at the target value are skipped.
    pub async fn apply_truncated_at_labels(
        &mut self,
        active_backfills: &std::collections::BTreeMap<usize, u64>,
    ) -> tonic::Result<()> {
        for (&index, &clock) in active_backfills {
            let target = labels::truncated_at_value(clock);

            let super::Binding::Mapped(binding) = &self.bindings[index] else {
                return Err(tonic::Status::internal(format!(
                    "binding {index} has an active backfill but is not a Mapped collection binding"
                )));
            };
            let client = self.binding_clients[index].client();

            // Watch the partition listing: the watch handles transient-error
            // backoff and restates the journals after every change, so a lost CAS
            // race (`false`) is retried on the snapshot that the racing writer's
            // own change delivers.
            let watch = client.clone().list_watch(broker::ListRequest {
                selector: Some(broker::LabelSelector {
                    include: Some(labels::build_set([(
                        "name:prefix",
                        binding.partitions_prefix.as_str(),
                    )])),
                    exclude: None,
                }),
                watch: true,
                ..Default::default()
            });
            let mut watch = std::pin::pin!(watch);

            loop {
                match watch.next().await {
                    Some(Ok(listing)) => {
                        if advance_truncated_at_labels(client, listing, &target).await? {
                            break;
                        }
                    }
                    // Transient — retried on the next poll.
                    Some(Err(gazette::RetryError { inner, .. })) if inner.is_transient() => {}
                    Some(Err(gazette::RetryError { inner, .. })) => {
                        return Err(status_from_gazette(inner));
                    }
                    None => break,
                }
            }
        }
        Ok(())
    }

    /// Access the lazy Client and partitions watch for the Mapped binding at
    /// `index`. Panics if the binding is Fixed. Primarily used by tests.
    pub fn mapped_binding_client(
        &self,
        index: usize,
    ) -> &(
        gazette::journal::Client,
        tokens::PendingWatch<Vec<super::watch::PartitionSplit>>,
    ) {
        match &self.binding_clients[index] {
            super::LazyBindingClient::Mapped(lazy) => &**lazy,
            super::LazyBindingClient::Fixed(_) => {
                panic!("binding {index} is Fixed, not Mapped")
            }
        }
    }
}

async fn advance_truncated_at_labels(
    client: &gazette::journal::Client,
    listing: broker::ListResponse,
    target: &str,
) -> tonic::Result<bool> {
    for journal in listing.journals {
        let Some(change) = truncated_at_label_change(journal, target)? else {
            continue;
        };

        match retry_transient("apply truncated-at label", || {
            client.apply(broker::ApplyRequest {
                changes: vec![change.clone()],
            })
        })
        .await
        {
            Ok(_) => {}
            Err(gazette::Error::BrokerStatus(broker::Status::EtcdTransactionFailed)) => {
                return Ok(false);
            }
            Err(err) => return Err(status_from_gazette(err)),
        }
    }
    Ok(true)
}

/// Convert a gazette client Error into a tonic::Status, preserving a gRPC status
/// when present and otherwise wrapping the error as Internal.
fn status_from_gazette(err: gazette::Error) -> tonic::Status {
    match err {
        gazette::Error::Grpc(status) => status,
        other => tonic::Status::internal(other.to_string()),
    }
}

async fn retry_transient<T, F, Fut>(what: &'static str, mut op: F) -> gazette::Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = gazette::Result<T>>,
{
    let mut attempt: u32 = 0;
    loop {
        let err = match op().await {
            ok @ Ok(_) => return ok,
            Err(err) => err,
        };
        attempt += 1;

        if !err.is_transient() || attempt == 8 {
            return Err(err);
        }

        // Exponential backoff from a 100ms base, capped at 10 seconds.
        let backoff = std::time::Duration::from_millis(100)
            .saturating_mul(2u32.saturating_pow(attempt - 1))
            .min(std::time::Duration::from_secs(10));
        tracing::warn!(what, attempt, %err, "gazette RPC failed; retrying after backoff");
        tokio::time::sleep(backoff).await;
    }
}

/// Build the apply Change that advances `journal`'s `estuary.dev/truncated-at`
/// label to `target`, or `None` if the journal is already at or beyond it. The
/// fixed-width hex encoding sorts lexically by clock, so the skip covers both an
/// equal label (idempotent) and a newer one (a later backfill already applied,
/// or clock skew across a restart) -- the label only ever advances.
fn truncated_at_label_change(
    journal: broker::list_response::Journal,
    target: &str,
) -> tonic::Result<Option<broker::apply_request::Change>> {
    let Some(mut spec) = journal.spec else {
        return Err(tonic::Status::internal(
            "list response journal is missing its spec",
        ));
    };
    let current = spec
        .labels
        .as_ref()
        .and_then(|set| labels::maybe_one(set, labels::TRUNCATED_AT).ok())
        .unwrap_or("");

    if current >= target {
        return Ok(None);
    }

    spec.labels = Some(labels::set_value(
        spec.labels.take().unwrap_or_default(),
        labels::TRUNCATED_AT,
        target,
    ));

    Ok(Some(broker::apply_request::Change {
        expect_mod_revision: journal.mod_revision,
        upsert: Some(spec),
        delete: String::new(),
    }))
}

#[cfg(test)]
mod test {
    use super::*;

    // A list-response journal carrying `truncated_at` (when Some) plus an
    // unrelated label, used to check the advance decision and that other labels
    // survive the upsert.
    fn journal(mod_revision: i64, truncated_at: Option<&str>) -> broker::list_response::Journal {
        let mut set = labels::build_set([("estuary.dev/collection", "the/collection")]);
        if let Some(value) = truncated_at {
            set = labels::set_value(set, labels::TRUNCATED_AT, value);
        }
        broker::list_response::Journal {
            spec: Some(broker::JournalSpec {
                name: "the/collection/pivot=00".to_string(),
                labels: Some(set),
                ..Default::default()
            }),
            mod_revision,
            ..Default::default()
        }
    }

    #[test]
    fn test_truncated_at_label_change_advances_only() {
        let target = labels::truncated_at_value(0x20);

        // No label yet, an older label, and an equal-then-newer label: the
        // decision is "advance only" -- skip unless strictly older than target.
        let absent = truncated_at_label_change(journal(1, None), &target).unwrap();
        assert!(absent.is_some(), "absent label advances");

        let older = labels::truncated_at_value(0x10);
        let older = truncated_at_label_change(journal(1, Some(&older)), &target).unwrap();
        assert!(older.is_some(), "older label advances");

        let equal = truncated_at_label_change(journal(1, Some(&target)), &target).unwrap();
        assert!(equal.is_none(), "equal label is skipped (idempotent)");

        let newer = labels::truncated_at_value(0x30);
        let newer = truncated_at_label_change(journal(1, Some(&newer)), &target).unwrap();
        assert!(newer.is_none(), "newer label is never regressed");
    }

    #[test]
    fn test_truncated_at_label_change_builds_upsert() {
        let target = labels::truncated_at_value(0x20);
        let prior = labels::truncated_at_value(0x10);

        let change = truncated_at_label_change(journal(42, Some(&prior)), &target)
            .unwrap()
            .expect("older label advances");

        // The CAS guard carries the listed revision, and the upsert sets the
        // target label while preserving the journal's unrelated labels.
        assert_eq!(change.expect_mod_revision, 42);
        let set = change.upsert.unwrap().labels.unwrap();
        assert_eq!(
            labels::maybe_one(&set, labels::TRUNCATED_AT).unwrap(),
            target
        );
        assert_eq!(
            labels::maybe_one(&set, "estuary.dev/collection").unwrap(),
            "the/collection"
        );
    }

    #[test]
    fn test_truncated_at_label_change_requires_spec() {
        let target = labels::truncated_at_value(0x20);
        let no_spec = broker::list_response::Journal {
            spec: None,
            mod_revision: 1,
            ..Default::default()
        };
        assert!(truncated_at_label_change(no_spec, &target).is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn test_retry_transient_budget_and_passthrough() {
        use std::sync::atomic::{AtomicU32, Ordering};

        struct Case {
            name: &'static str,
            fail_times: u32,
            transient: bool,
            expect_ok: bool,
            expect_calls: u32,
        }
        let cases = [
            Case {
                name: "immediate success",
                fail_times: 0,
                transient: true,
                expect_ok: true,
                expect_calls: 1,
            },
            Case {
                name: "transient then success",
                fail_times: 3,
                transient: true,
                expect_ok: true,
                expect_calls: 4,
            },
            Case {
                name: "terminal surfaces at once",
                fail_times: 99,
                transient: false,
                expect_ok: false,
                expect_calls: 1,
            },
            Case {
                name: "transient exhausts budget",
                fail_times: 99,
                transient: true,
                expect_ok: false,
                expect_calls: 8, // mirrors retry_transient's attempt budget
            },
        ];

        for case in cases {
            let calls = AtomicU32::new(0);
            let result = retry_transient("test", || {
                let n = calls.fetch_add(1, Ordering::SeqCst);
                let out: gazette::Result<u32> = if n < case.fail_times {
                    Err(if case.transient {
                        gazette::Error::Grpc(tonic::Status::unavailable("transient"))
                    } else {
                        gazette::Error::BrokerStatus(broker::Status::EtcdTransactionFailed)
                    })
                } else {
                    Ok(n)
                };
                std::future::ready(out)
            })
            .await;

            assert_eq!(result.is_ok(), case.expect_ok, "{}", case.name);
            assert_eq!(
                calls.load(Ordering::SeqCst),
                case.expect_calls,
                "{}",
                case.name
            );
        }
    }
}
