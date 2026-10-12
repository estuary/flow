use crate::device::recording;
use crate::proto;
use proto_gazette::{broker, fixed_framing, uuid};

/// Blocks of the images these scenarios replay into. A journal scenario asserts which
/// blocks a replay left allocated, so a small disk keeps that report readable.
pub const BLOCKS: u32 = 64;

/// A journal's parsed UUID: who wrote a record, when, and what it is.
pub type UuidParts = (uuid::Producer, uuid::Clock, uuid::Flags);

/// One block of `fill`, as a device write of it encodes.
pub fn write(block: u32, fill: u8) -> Vec<proto::Chunk> {
    crate::chunk::encode_write(
        block,
        &bytes::Bytes::from(vec![fill; crate::BLOCK_SIZE as usize]),
    )
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

    /// Open, claim, and promote `journal` as a tenure does, and serve a writer from
    /// it which compacts nothing.
    pub async fn open(
        &self,
        journal: &str,
    ) -> anyhow::Result<(recording::Recorder, crate::journal::Writer)> {
        let (recorder, writer, _blocks) = self.recover(journal, None).await?;

        Ok((recorder, writer))
    }

    /// [`Fixture::open`], reporting the fill byte of every block the replay left
    /// allocated.
    pub async fn recover(
        &self,
        journal: &str,
        ack: Option<bytes::Bytes>,
    ) -> anyhow::Result<(recording::Recorder, crate::journal::Writer, Vec<(u32, u8)>)> {
        let (promoted, blocks) = self.promote(journal, ack).await?;
        let (recorder, recorded) = recording::channel(64, std::task::Waker::noop().clone());

        Ok((recorder, promoted.serve_uncompacted(recorded), blocks))
    }

    /// Open, claim, and promote `journal` as a tenure does, repairing `ack`, and
    /// report the fill byte of every block the replay left allocated.
    pub async fn promote(
        &self,
        journal: &str,
        ack: Option<bytes::Bytes>,
    ) -> anyhow::Result<(crate::journal::Promoted, Vec<(u32, u8)>)> {
        let standby = self.standby(journal).await?;

        // The image outlives its directory, having no directory entry of its own.
        let dir = tempfile::tempdir()?;
        let image = crate::image::Image::create(dir.path(), BLOCKS)?;
        let uncommitted = crate::journal::UncommittedDelta::create(dir.path())?;

        let playback = standby.play(image, uncommitted);

        let claimed = standby.claim_journal().await?;
        let (promoted, recovered) = claimed.promote(playback, ack).await?;

        Ok((promoted, super::allocated(&recovered.image)))
    }

    /// Open `journal`, having created it from [`Fixture::spec`] as a deployment's
    /// activation would.
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
        crate::journal::Standby::new(
            &self.daemon_brokers,
            journal.to_string(),
            tokio_util::sync::CancellationToken::new(),
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
        e2e_support::journals::create(&self.client, spec).await
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
        // A broker-confirmed head bounds the read, as a recovering tenure does.
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

    /// Value of the journal's `author` register, which only a fence installs.
    ///
    /// The daemon itself never reads registers, so a scenario reads what the daemon
    /// wrote through `e2e_support` rather than through the code which wrote it.
    pub async fn author(&self, journal: &str) -> Option<String> {
        e2e_support::journals::register(&self.client, journal, "author")
            .await
            .expect("probing a journal")
    }

    /// Append `fence` to `journal` under the request a claim of `author` writes its
    /// fence under, outside of any tenure, and return the head just past it.
    pub async fn append_fence(
        &self,
        journal: &str,
        author: uuid::Producer,
        fence: &proto::DiskRecord,
    ) -> i64 {
        let mut appender = publisher::Appender::new(self.client.clone(), journal.to_string());

        () = appender
            .set_request(broker::AppendRequest {
                journal: journal.to_string(),
                union_registers: Some(crate::journal::fence(author)),
                ..Default::default()
            })
            .await
            .expect("setting a fence's request");

        fixed_framing::encode(fence, &mut appender.buffer);
        let barrier = appender.barrier();

        () = appender.flush().await.expect("appending a fence");
        barrier
            .await
            .expect("a fence's append")
            .commit
            .expect("an append which succeeded reports its commit")
            .end
    }

    /// Broker-confirmed write head. A journal which was created and never appended to
    /// holds zero.
    pub async fn head(&self, journal: &str) -> i64 {
        e2e_support::journals::head(&self.client, journal)
            .await
            .expect("probing a journal")
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
            labels::maybe_one(&set, crate::DISK_RECOVERY_FLOOR)
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
