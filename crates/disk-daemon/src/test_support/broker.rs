use crate::proto;
use proto_gazette::{broker, fixed_framing, uuid};

/// Blocks of the images these scenarios replay into. A journal scenario asserts which
/// blocks a replay left allocated, so a small disk keeps that report readable.
pub const BLOCKS: u32 = 64;

/// The scenario's end of a writer's recording channel, where a device would be.
pub type Recorder = tokio::sync::mpsc::Sender<crate::device::Mutation>;

/// A journal's parsed UUID: who wrote a record, when, and what it is.
pub type UuidParts = (uuid::Producer, uuid::Clock, uuid::Flags);

/// `chunks` as one recorded mutation, which opens no horizon.
pub fn mutation(chunks: Vec<proto::Chunk>) -> crate::device::Mutation {
    crate::device::Mutation {
        chunks,
        opens_horizon: false,
    }
}

/// One block of `fill`, as a device records a write of it.
pub fn write(block: u32, fill: u8) -> crate::device::Mutation {
    mutation(crate::chunk::encode_write(
        block,
        &bytes::Bytes::from(vec![fill; crate::BLOCK_SIZE as usize]),
    ))
}

/// Render `records` one per line: its kind, who appended it, and what it carries.
/// Authors are random, so they are numbered in order of first appearance.
pub fn render(records: &[(UuidParts, proto::DiskRecord)]) -> String {
    let mut authors: Vec<uuid::Producer> = Vec::new();

    for ((producer, _clock, _flags), _record) in records {
        if !authors.contains(producer) {
            authors.push(*producer);
        }
    }

    records
        .iter()
        .map(|((producer, _clock, flags), record)| {
            let kind = if flags.is_outside() {
                "fence"
            } else if flags.is_ack() {
                "ack"
            } else {
                "continue"
            };
            let mut carries: Vec<String> = record.chunks.iter().map(super::render_chunk).collect();

            if record.opens_horizon {
                carries.push("opens a horizon".to_string());
            }
            let author = authors
                .iter()
                .position(|author| author == producer)
                .unwrap()
                + 1;
            let author = format!("author {author}");

            format!("{kind:<10}{author:<10}{}", carries.join(", "))
                .trim_end()
                .to_string()
        })
        .collect::<Vec<_>>()
        .join("\n")
}

pub struct Fixture {
    /// Brokers as the writer under test reaches them, signed exactly as the daemon
    /// signs.
    pub daemon_brokers: gazette::journal::Client,
    /// Client of the test itself, which probes, reads, and applies specs.
    pub client: gazette::journal::Client,
    /// Owns the brokers and etcd used by these scenarios.
    data_plane: e2e_support::DataPlane,
}

impl Fixture {
    pub async fn start() -> Self {
        let data_plane =
            e2e_support::DataPlane::start(e2e_support::DataPlaneArgs { broker_count: 1 })
                .await
                .expect("DataPlane start");

        // The daemon's own constructor, so that a scenario exercises the credential
        // the daemon signs rather than one of the test's making.
        let daemon_brokers = crate::daemon::broker_client(
            &data_plane.gazette.brokers[0].endpoint,
            "local",
            "disk-daemon-test",
            &data_plane.gazette.auth_keys,
        )
        .expect("building the daemon's broker client");

        Self {
            daemon_brokers,
            client: data_plane.journal_client.clone(),
            data_plane,
        }
    }

    pub async fn stop(self) {
        self.data_plane
            .graceful_stop()
            .await
            .expect("DataPlane graceful_stop");
    }

    /// Open, fence, and promote `journal` as an attachment does, and serve a writer
    /// from it over a recording channel the scenario sends to.
    pub async fn open(&self, journal: &str) -> anyhow::Result<(Recorder, crate::journal::Writer)> {
        let (recorder, writer, _blocks) = self.recover(journal, None).await?;

        Ok((recorder, writer))
    }

    /// [`Fixture::open`], reporting the fill byte of every block the replay left
    /// allocated.
    pub async fn recover(
        &self,
        journal: &str,
        ack: Option<bytes::Bytes>,
    ) -> anyhow::Result<(Recorder, crate::journal::Writer, Vec<(u32, u8)>)> {
        let (promoted, blocks) = self.promote(journal, ack).await?;
        let (recorder, recorded) = tokio::sync::mpsc::channel(64);

        Ok((recorder, promoted.serve(recorded), blocks))
    }

    /// Open, fence, and promote `journal` as an attachment does, repairing `ack`, and
    /// report the fill byte of every block the replay left allocated.
    pub async fn promote(
        &self,
        journal: &str,
        ack: Option<bytes::Bytes>,
    ) -> anyhow::Result<(crate::journal::Promoted, Vec<(u32, u8)>)> {
        let standby = self.standby(journal).await?;
        let (promoted, recovered) = standby.promote(ack).await?;

        Ok((promoted, super::allocated(&recovered.image)))
    }

    /// Open `journal` and play it back to its head, having created it from
    /// [`Fixture::spec`] as a deployment's activation would.
    pub async fn standby(&self, journal: &str) -> anyhow::Result<crate::journal::Standby> {
        () = self.create_journal(self.spec(journal)).await?;

        self.standby_uncreated(journal).await
    }

    /// [`Fixture::standby`], creating nothing, for a scenario which staged a journal
    /// of its own — or which stages none at all.
    pub async fn standby_uncreated(
        &self,
        journal: &str,
    ) -> anyhow::Result<crate::journal::Standby> {
        // The image outlives its directory, having no directory entry of its own.
        let dir = tempfile::tempdir()?;
        let image = crate::image::Image::create(dir.path(), BLOCKS)?;
        let uncommitted = crate::journal::UncommittedDelta::create(dir.path())?;

        crate::journal::Standby::new(
            &self.daemon_brokers,
            journal.to_string(),
            tokio_util::sync::CancellationToken::new(),
            image,
            uncommitted,
        )
        .await
    }

    /// Use uncompressed fragments for scenarios inspecting raw records.
    pub fn spec(&self, journal: &str) -> broker::JournalSpec {
        broker::JournalSpec {
            name: journal.to_string(),
            replication: 1,
            labels: Some(labels::build_set([(
                labels::CONTENT_TYPE,
                crate::CONTENT_TYPE_DISK,
            )])),
            fragment: Some(broker::journal_spec::Fragment {
                length: 1 << 20,
                compression_codec: broker::CompressionCodec::None as i32,
                stores: vec!["file:///".to_string()],
                refresh_interval: Some(std::time::Duration::from_secs(300).into()),
                flush_interval: Some(std::time::Duration::from_secs(48 * 3600).into()),
                retention: None,
                path_postfix_template: String::new(),
            }),
            flags: broker::journal_spec::Flag::ORdwr as u32,
            max_append_rate: 1 << 22,
            suspend: None,
        }
    }

    /// Create `journal` unless it exists.
    ///
    /// Every open needs one, and a scenario calls this directly to stage a journal
    /// whose spec differs from [`Fixture::spec`].
    pub async fn create_journal(&self, spec: broker::JournalSpec) -> anyhow::Result<()> {
        let request = broker::ApplyRequest {
            changes: vec![broker::apply_request::Change {
                expect_mod_revision: 0,
                upsert: Some(spec),
                delete: String::new(),
            }],
        };

        match self.client.apply(request).await {
            Ok(_) | Err(gazette::Error::BrokerStatus(broker::Status::EtcdTransactionFailed)) => {
                Ok(())
            }
            Err(err) => Err(err.into()),
        }
    }

    /// Every record of `journal`, paired with its parsed UUID.
    pub async fn read(&self, journal: &str) -> Vec<(UuidParts, proto::DiskRecord)> {
        self.read_from(journal, 0).await
    }

    /// [`Fixture::read`], from `offset`. It must be a record boundary, as a
    /// recovery floor is.
    pub async fn read_from(
        &self,
        journal: &str,
        offset: i64,
    ) -> Vec<(UuidParts, proto::DiskRecord)> {
        // A broker-confirmed head bounds the read, as a recovering attachment does.
        // Without a bound, the broker reports the offset beyond the last record as not
        // yet available, and a reader treats that as an error.
        let head = self.head(journal).await;

        let mut content = bytes::BytesMut::new();
        let stream = self.client.clone().read(broker::ReadRequest {
            journal: journal.to_string(),
            offset,
            end_offset: head,
            block: false,
            ..Default::default()
        });
        futures::pin_mut!(stream);

        while let Some(response) = futures::StreamExt::next(&mut stream).await {
            content.extend_from_slice(&response.expect("reading a journal").content);
        }

        let mut records = Vec::new();
        let mut rest = bytes::BytesMut::from(&content[..]);

        while !rest.is_empty() {
            match fixed_framing::unpack::<proto::DiskRecord>(&mut rest).expect("a record decodes") {
                fixed_framing::Frame::Record { message, .. } => {
                    let uuid = uuid::Uuid::from_slice(&message.uuid).unwrap();
                    records.push((uuid::parse(uuid).unwrap(), message));
                }
                frame => panic!("expected a record of {journal}, got {frame:?}"),
            }
        }
        records
    }

    /// Broker-confirmed write head. A journal which was created and never appended to
    /// holds zero.
    pub async fn head(&self, journal: &str) -> i64 {
        crate::journal::confirm_head(&self.client, journal)
            .await
            .expect("confirming a journal's head")
    }

    /// Value of the journal's recovery-floor label, which is absent until a delta of
    /// the disk first commits.
    pub async fn floor_label(&self, journal: &str) -> Option<String> {
        let listing = self
            .client
            .get_journal(journal)
            .await
            .expect("listing a journal")
            .expect("the journal exists");

        listing.spec.and_then(|spec| spec.labels).and_then(|set| {
            labels::maybe_one(&set, labels::DISK_RECOVERY_FLOOR)
                .ok()
                .filter(|value| !value.is_empty())
                .map(str::to_string)
        })
    }

    /// Wait for `journal`'s head to move past `head`: for an append a writer starts
    /// on its own, which no flush of the scenario's awaits, to land.
    pub async fn advances(&self, journal: &str, head: i64) {
        for _ in 0..1000 {
            if self.head(journal).await > head {
                return;
            }
            () = tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("{journal} did not advance past {head}");
    }
}
