use bytes::BufMut;
use proto_gazette::uuid;

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
        doc: impl FnOnce(uuid::Uuid) -> (usize, N),
        flags: uuid::Flags,
    ) -> tonic::Result<&crate::Appender> {
        // Invariant: buffers are always empty (but may have capacity).
        let prefix = std::mem::take(&mut self.prefix_buf);
        let packed_key = std::mem::take(&mut self.packed_key_buf);

        // Sequence the document.
        let uuid = proto_gazette::uuid::build(self.producer, self.clock.tick(), flags);
        let (binding_idx, doc) = doc(uuid);

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
