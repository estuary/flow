use bytes::BufMut;
use proto_gazette::uuid;

pub struct Publisher {
    bindings: Vec<super::Binding>,
    producer: uuid::Producer,
    clock: uuid::Clock,
    appenders: super::AppenderGroup,
    prefix_buf: String,
    packed_key_buf: bytes::BytesMut,
}

impl Publisher {
    /// Create a new Publisher for the given bindings, producer identity, and clock.
    ///
    /// The `producer` identifies this Publisher as a distinct writer and is
    /// embedded in every UUID it generates. The `clock` provides a monotonic
    /// timestamp for ordering documents within this producer's stream.
    pub fn new(
        bindings: Vec<super::Binding>,
        producer: uuid::Producer,
        clock: uuid::Clock,
    ) -> Self {
        Self {
            bindings,
            producer,
            clock,
            appenders: super::AppenderGroup::new(),
            prefix_buf: String::new(),
            packed_key_buf: bytes::BytesMut::new(),
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
    /// returns `(binding_index, document)`. The document is mapped to a physical
    /// partition (creating one if needed, which may issue an Apply RPC), serialized
    /// as newline-delimited JSON into the partition's Appender buffer, and
    /// checkpoint'd. The checkpoint may start a background Append RPC if the
    /// buffer exceeds the flush threshold.
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
        let (binding, doc) = doc(uuid);

        let (mut journal, mut packed_key) =
            super::mapping::map_partition(&self.bindings[binding], &doc, prefix, packed_key)
                .await?;

        let (client, _partitions) = &(*self.bindings[binding].client);
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

    /// Write ACK intent documents to journals, flush, and sweep active to idle.
    ///
    /// Takes the output of `intents::build_transaction_intents()` and writes
    /// each journal's ACK documents as newline-delimited JSON. For each journal,
    /// finds the binding whose `partitions_template.name` is a prefix of the
    /// journal name, activates an appender using that binding's client, and
    /// writes the ACK documents. After all ACKs are written, flushes and sweeps
    /// all appenders to idle.
    pub async fn write_intents(
        &mut self,
        journal_acks: &[(String, Vec<serde_json::Value>)],
    ) -> tonic::Result<()> {
        for (journal, acks) in journal_acks {
            // TODO(johnny): We're walking bindings for each ACK intent journal.
            // This is probably fine but _could_ be faster.
            // We can do this by building a sorted index of partition template name => binding.
            let binding = self
                .bindings
                .iter()
                .find(|b| journal.starts_with(&b.partitions_template.name))
                .ok_or_else(|| {
                    tonic::Status::internal(format!(
                        "cannot find a binding to write ACK of journal {journal}"
                    ))
                })?;

            let (client, _partitions) = &(*binding.client);
            let appender = self.appenders.activate(journal, client);

            for ack in acks {
                let mut writer = std::mem::take(&mut appender.buffer).writer();
                serde_json::to_writer(&mut writer, &ack)
                    .expect("serialization of Value cannot fail");
                appender.buffer = writer.into_inner();
                appender.buffer.put_u8(b'\n');
            }
        }

        self.appenders.flush().await?;
        self.appenders.sweep();
        Ok(())
    }
}
