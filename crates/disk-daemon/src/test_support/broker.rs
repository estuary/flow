//! Opening a journal against a real Gazette broker from a test.
//!
//! A data plane is expensive to start, so one case starts one and works a journal per
//! scenario. The daemon creates no journal, so [`Fixture::standby`] creates one from
//! [`Fixture::spec`] as a deployment's activation would. A scenario which stages a
//! journal of its own, or which is about one that does not exist, opens with
//! [`Fixture::standby_uncreated`].
//!
//! The operations which are about journals rather than about disks — creating one,
//! probing it, reading a register of it — are `e2e_support::journals`'.

use crate::image::Image;
use crate::journal::uncommitted;
use crate::journal::{Promoted, Standby, Writer};
use crate::proto;
use crate::recording::{self, Recorder};
use proto_gazette::{broker, fixed_framing, uuid};

/// Blocks of the images these scenarios replay into. A journal scenario asserts which
/// blocks a replay left allocated, so a small disk keeps that report readable.
pub const BLOCKS: u32 = 64;

/// A journal's parsed UUID: who wrote a record, when, and what it is.
pub type UuidParts = (uuid::Producer, uuid::Clock, uuid::Flags);

pub struct Fixture {
    /// Client of the writer under test, signed exactly as the daemon signs.
    pub daemon_client: gazette::journal::Client,
    /// Client of the test itself, which probes, reads, and applies specs.
    pub client: gazette::journal::Client,
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
        let daemon_client = crate::daemon::client(
            &data_plane.gazette.brokers[0].endpoint,
            "local",
            "disk-daemon-test",
            &data_plane.gazette.auth_keys,
        )
        .expect("building the daemon's journal client");

        Self {
            daemon_client,
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
    pub async fn open(&self, journal: &str) -> anyhow::Result<(Recorder, Writer)> {
        let (recorder, writer, _blocks) = self.recover(journal, Vec::new()).await?;

        Ok((recorder, writer))
    }

    /// [`Fixture::open`], reporting the fill byte of every block the replay left
    /// allocated.
    pub async fn recover(
        &self,
        journal: &str,
        acks: Vec<bytes::Bytes>,
    ) -> anyhow::Result<(Recorder, Writer, Vec<(u32, u8)>)> {
        let (promoted, blocks) = self.promote(journal, acks).await?;
        let (recorder, recorded) = recording::channel(64, std::task::Waker::noop().clone());

        Ok((recorder, promoted.serve_uncompacted(recorded), blocks))
    }

    /// Open, claim, and promote `journal` as a tenure does, repairing `acks`, and
    /// report the fill byte of every block the replay left allocated.
    pub async fn promote(
        &self,
        journal: &str,
        acks: Vec<bytes::Bytes>,
    ) -> anyhow::Result<(Promoted, Vec<(u32, u8)>)> {
        let standby = self.standby(journal).await?;

        // The image outlives its directory, having no directory entry of its own.
        let dir = tempfile::tempdir()?;
        let image = Image::create(dir.path(), BLOCKS)?;
        let uncommitted = uncommitted::UncommittedDelta::create(dir.path())?;

        let playback = standby.play(image, uncommitted);

        // A tenure claims when it reads `Promote`, and finishes the promotion after.
        let claimed = standby.claim_journal().await?;
        let (promoted, recovered) = claimed.promote(playback, acks).await?;

        Ok((promoted, super::allocated(&recovered.image)))
    }

    /// Open `journal`, having created it from [`Fixture::spec`] as a deployment's
    /// activation would.
    pub async fn standby(&self, journal: &str) -> anyhow::Result<Standby> {
        () = self.create_journal(self.spec(journal)).await?;

        self.standby_uncreated(journal).await
    }

    /// [`Fixture::standby`], creating nothing, for a scenario which staged a journal
    /// of its own — or which stages none at all.
    pub async fn standby_uncreated(&self, journal: &str) -> anyhow::Result<Standby> {
        Standby::new(
            &self.daemon_client,
            journal.to_string(),
            tokio_util::sync::CancellationToken::new(),
        )
        .await
    }

    /// Spec a scenario's journal is created from.
    ///
    /// The codec is NONE, because a scenario reads these records back through a client
    /// of its own rather than through a broker which would decompress them.
    pub fn spec(&self, journal: &str) -> broker::JournalSpec {
        broker::JournalSpec {
            name: journal.to_string(),
            replication: 1,
            // A disk's journal declares that it holds one, as a recovery log declares
            // that it holds a recovery log.
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

    /// Append `fence` to `journal` under the request a claim of `epoch` writes its
    /// fence under, outside of any tenure, and return the head just past it.
    pub async fn append_fence(
        &self,
        journal: &str,
        epoch: uuid::Producer,
        fence: &proto::DiskRecord,
    ) -> i64 {
        let mut appender = publisher::Appender::new(self.client.clone(), journal.to_string());

        () = appender
            .set_request(broker::AppendRequest {
                journal: journal.to_string(),
                union_registers: Some(crate::journal::author(epoch)),
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
