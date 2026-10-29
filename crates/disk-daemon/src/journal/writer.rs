use super::floor;
use crate::proto;
use anyhow::Context;
use proto_gazette::{broker, fixed_framing, uuid};

/// The journal's writer, through which a promoted attachment appends the
/// records of its deltas.
pub struct Writer {
    /// Name of the disk's journal.
    pub name: String,
    /// Appends this writer's records, each checking the registers name its author.
    appender: publisher::Appender,
    /// Lists the journal and updates its replay-from label.
    client: gazette::journal::Client,
    /// Producer of this writer's records, which its fence made the journal's author.
    author: uuid::Producer,
    /// Clock of the last record: ticked per record, raised to wall time per delta.
    clock: uuid::Clock,
    /// The current delta has a record. An empty one appends no acknowledgement.
    delta_has_records: bool,
    /// Offset of the last floor candidate this writer appended.
    candidate: Option<i64>,
    /// The recovery floor which the current delta installs once it commits.
    floor: Option<i64>,
    /// Write head the broker last confirmed.
    head: i64,
}

impl Writer {
    /// Fence the previous primary of `journal` by appending a fence record,
    /// which installs a new author in the journal's registers.
    pub async fn fence(
        client: gazette::journal::Client,
        journal: String,
        recovered_ack_intent: Option<&bytes::Bytes>,
    ) -> anyhow::Result<(Self, i64)> {
        let author = random_producer();

        // A fully suspended journal reports no registers, and the fence resumes
        // it anyway.
        let (observed, registers) =
            super::confirm_head(&client, &journal, broker::append_request::Suspend::Resume)
                .await?
                .expect("an append which resumes the journal is not refused as suspended");

        if observed == 0 && recovered_ack_intent.is_some() {
            return Err(crate::invalid_argument(format!(
                "journal {journal} has no content, so it holds no committed data for a recovered ACK intent",
            )));
        }

        let fence = proto::DiskRecord {
            uuid: bytes::Bytes::copy_from_slice(
                uuid::build(author, uuid::Clock::zero(), uuid::Flags::OUTSIDE_TXN).as_bytes(),
            ),
            ..Default::default()
        };
        let mut content = bytes::BytesMut::new();
        fixed_framing::encode(&fence, &mut content);
        if let Some(ack_intent) = recovered_ack_intent {
            content.extend_from_slice(ack_intent);
        }
        let content = content.freeze();

        let (check_registers, offset) = fence_check(&registers, observed);
        let request = broker::AppendRequest {
            journal: journal.clone(),
            check_registers: Some(check_registers),
            union_registers: Some(author_registers(author)),
            offset,
            ..Default::default()
        };
        let source = || futures::stream::iter([Ok(content.clone())]);

        let response = {
            let mut stream = std::pin::pin!(client.append(request, source));
            loop {
                match futures::StreamExt::next(&mut stream).await {
                    Some(Ok(response)) => break response,
                    Some(Err(gazette::RetryError { attempt, inner })) if inner.is_transient() => {
                        tracing::warn!(journal, attempt, %inner, "fencing failed (will retry)");
                    }
                    // This promotion lost a race to another fence, or the
                    // journal lost its registers.
                    Some(Err(gazette::RetryError {
                        inner:
                            inner @ gazette::Error::BrokerStatus(
                                broker::Status::RegisterMismatch
                                | broker::Status::WrongAppendOffset,
                            ),
                        ..
                    })) => {
                        return Err(proto_grpc::status_to_anyhow(tonic::Status::aborted(
                            format!(
                                "fencing {journal}: its author or head changed after an \
                                 append confirmed its head at {observed}: {inner}"
                            ),
                        )));
                    }
                    Some(Err(gazette::RetryError { inner, .. })) => {
                        return Err(anyhow::Error::new(inner).context(format!("fencing {journal}")));
                    }
                    None => unreachable!("an append stream does not end without a response"),
                }
            }
        };
        let head = response
            .commit
            .expect("a fence which succeeded reports its commit")
            .end;

        let appender = publisher::Appender::new(client.clone(), journal.clone())
            .with_check_registers(broker::LabelSelector {
                include: Some(author_registers(author)),
                exclude: None,
            });

        let writer = Self {
            name: journal,
            appender,
            client,
            author,
            clock: uuid::Clock::from_time(std::time::SystemTime::now()),
            delta_has_records: false,
            candidate: None,
            floor: None,
            head,
        };

        Ok((writer, head))
    }

    /// Append the device's live chunk `live` to the current delta, without
    /// waiting for the broker to confirm it.
    pub async fn append(&mut self, live: crate::LiveChunk) -> anyhow::Result<()> {
        let crate::LiveChunk {
            chunk,
            floor_candidate,
            floor,
        } = live;

        if floor_candidate {
            // Flush all outstanding appends to get the exact offset for the
            // candidate's record.
            () = self.flush().await?;
            self.candidate = Some(self.head);
        }
        match floor {
            Some(crate::Floor::LastCandidate) => self.floor = self.candidate,
            Some(crate::Floor::Replayed(offset)) => self.floor = Some(offset),
            None => {}
        }

        let uuid = uuid::build(self.author, self.clock.tick(), uuid::Flags::CONTINUE_TXN);
        let record = proto::DiskRecord {
            uuid: bytes::Bytes::copy_from_slice(uuid.as_bytes()),
            chunk: Some(chunk),
            opens_horizon: floor_candidate,
        };
        fixed_framing::encode(&record, &mut self.appender.buffer);
        self.delta_has_records = true;

        self.appender
            .checkpoint()
            .await
            .map_err(proto_grpc::status_to_anyhow)
    }

    /// Prepare the current delta. Returns its ACK intent, and any
    /// recovery floor this commit installs.
    pub async fn prepare(&mut self) -> anyhow::Result<(bytes::Bytes, Option<i64>)> {
        self.clock
            .update(uuid::Clock::from_time(std::time::SystemTime::now()));

        if !self.delta_has_records {
            return Ok((bytes::Bytes::new(), None));
        }
        let uuid = uuid::build(self.author, self.clock.tick(), uuid::Flags::ACK_TXN);
        let record = proto::DiskRecord {
            uuid: bytes::Bytes::copy_from_slice(uuid.as_bytes()),
            ..Default::default()
        };
        let mut ack_intent = bytes::BytesMut::new();
        fixed_framing::encode(&record, &mut ack_intent);
        self.delta_has_records = false;

        () = self.flush().await?;

        Ok((ack_intent.freeze(), self.floor.take()))
    }

    /// Commit the delta `prepare` prepared: append its `ack_intent`, and wait for the
    /// broker to confirm it. A commit which installs a recovery `floor` stores it
    /// on the journal.
    pub async fn acknowledge(
        &mut self,
        ack_intent: bytes::Bytes,
        floor: Option<i64>,
    ) -> anyhow::Result<()> {
        self.appender.buffer.extend_from_slice(&ack_intent);
        () = self.flush().await?;

        let Some(floor) = floor else {
            return Ok(());
        };
        tracing::info!(
            journal = self.name,
            floor,
            head = self.head,
            "storing the recovery floor",
        );

        floor::store(&self.client, &self.name, floor)
            .await
            .with_context(|| format!("storing the recovery floor of {}", self.name))
    }

    async fn flush(&mut self) -> anyhow::Result<()> {
        let response = self
            .appender
            .flush()
            .await
            .map_err(proto_grpc::status_to_anyhow)?;

        // An appender might flush nothing if it hasn't appended anything since
        // it was created, like right after a fence. `response.commit` is None
        // in that case.
        if let Some(commit) = response.commit {
            self.head = commit.end;
        }
        Ok(())
    }
}

/// The selector a fence's append checks, and the offset it pins, given the
/// `registers` and `head` which a zero-byte append confirmed just before it.
fn fence_check(registers: &broker::LabelSet, head: i64) -> (broker::LabelSelector, i64) {
    let authors = labels::values(registers, "author");

    if !authors.is_empty() {
        let include = broker::LabelSet {
            labels: authors.to_vec(),
        };
        let selector = broker::LabelSelector {
            include: Some(include),
            exclude: None,
        };
        return (selector, 0);
    }
    // A selector's empty value matches every value of its label.
    let selector = broker::LabelSelector {
        include: None,
        exclude: Some(labels::build_set([("author", "")])),
    };
    (selector, head)
}

/// A producer for this writer's records, which its fence installs as the
/// journal's author.
fn random_producer() -> uuid::Producer {
    let mut producer: [u8; 6] = rand::random();
    producer[0] |= 0x01; // Set multicast bit (mark as not a real MAC address).
    uuid::Producer::from_bytes(producer)
}

/// Registers which name `author` as a journal's author, as its producer in hex.
fn author_registers(author: uuid::Producer) -> broker::LabelSet {
    let value: String = author
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();

    labels::build_set([("author", value.as_str())])
}

#[cfg(test)]
mod test {
    use proto_gazette::broker;

    #[test]
    fn test_fence_check() {
        let first = labels::build_set([("author", "0123456789ab")]);
        let second = labels::build_set([("author", "ba9876543210")]);
        let none = broker::LabelSet::default();

        // (name, registers and head a zero-byte append confirmed, registers once
        // the fence is late, offset the fence pins)
        //
        // A fence lands on the registers it was derived from, and is refused on
        // the later registers.
        let cases = [
            // The primary's appends move the head, but keep its author.
            ("another fence", &first, 1024, &second, 0),
            ("lost registers", &first, 1024, &none, 0),
            // Only a fence moves the head of a journal without an author.
            (
                "another fence without an author",
                &none,
                1024,
                &second,
                1024,
            ),
            // Gazette checks no offset of zero.
            ("another fence without content", &none, 0, &second, 0),
        ];

        for (name, registers, head, late, offset) in cases {
            let (selector, pinned) = super::fence_check(registers, head);
            let outcome = (
                labels::matches(&selector, registers).unwrap(),
                labels::matches(&selector, late).unwrap(),
                pinned,
            );
            assert_eq!(outcome, (true, false, offset), "{name}");
        }
    }
}
