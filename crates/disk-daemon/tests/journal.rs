//! Journal writer tests against a real broker.
//!
//! A data-plane is expensive to start, so every case works a journal of its own
//! and they share one. Each holds its [`Capture`] for the length of the case:
//! dropping it closes the capture channel, which is how a session ends.

use disk_daemon::capture::{self, Capture};
use disk_daemon::chunk::{covered_blocks, encode_punch, encode_write};
use disk_daemon::journal::{self, Writer, fence};
use disk_daemon::proto;
use disk_daemon::wake::Waker;
use gazette::journal::framing;
use proto_gazette::{broker, uuid};

const BLOCK_SIZE: u32 = 4096;

#[tokio::test]
async fn journal_writer_tests() {
    let data_plane = e2e_support::DataPlane::start(e2e_support::DataPlaneArgs { broker_count: 1 })
        .await
        .expect("DataPlane start");

    let fixture = Fixture {
        endpoint: data_plane.gazette.brokers[0].endpoint.clone(),
        credential: credential(&data_plane.gazette),
        client: data_plane.journal_client.clone(),
    };

    first_use_claims_the_journal(&fixture).await;
    a_replacement_writer_fences_the_first(&fixture).await;
    an_ambiguous_claim_finds_its_own_epoch(&fixture).await;
    a_session_which_never_publishes_creates_no_journal(&fixture).await;
    a_committed_delta_reads_back_as_its_chunks(&fixture).await;
    a_large_delta_carries_one_record_per_mutation(&fixture).await;
    a_journal_without_a_store_never_opens(&fixture).await;

    data_plane
        .graceful_stop()
        .await
        .expect("DataPlane graceful_stop");
}

/// First use creates the journal, claims its author register with a fence
/// record, and appends its delta under that claim.
async fn first_use_claims_the_journal(fixture: &Fixture) {
    let journal = "acmeCo/disk/first-use";
    let (capture, writer) = fixture.open(journal, false).await.unwrap();

    capture.offer(vec![encode_punch(3, 2)]).unwrap();
    let ack = writer
        .publish()
        .await
        .unwrap()
        .expect("the delta is not empty");
    () = writer.commit(ack).await.unwrap();

    let records = fixture.read(journal).await;
    assert_eq!(records.len(), 3);

    let (producer, _clock, flags) = records[0].0;
    assert!(flags.is_outside(), "a fence is outside a transaction");
    assert_ne!(
        producer,
        writer.epoch(),
        "a fence has a producer of its own"
    );
    assert_eq!(records[0].1.installs_epoch, writer.epoch().as_bytes()[..]);
    assert!(records[0].1.chunks.is_empty());

    let (producer, _clock, flags) = records[1].0;
    assert!(flags.is_continue());
    assert_eq!(producer, writer.epoch());
    assert_eq!(records[1].1.chunks, vec![encode_punch(3, 2)]);

    let (producer, _clock, flags) = records[2].0;
    assert!(flags.is_ack());
    assert_eq!(producer, writer.epoch());
    assert!(records[2].1.chunks.is_empty());

    assert_eq!(
        fixture.author(journal).await.as_deref(),
        Some(fence::value(writer.epoch()).as_str()),
    );
}

/// A replacement session takes the author register, and the first session then
/// cannot append.
async fn a_replacement_writer_fences_the_first(fixture: &Fixture) {
    let journal = "acmeCo/disk/contended";
    let (first_capture, first) = fixture.open(journal, false).await.unwrap();

    first_capture.offer(vec![encode_punch(0, 1)]).unwrap();
    let ack = first.publish().await.unwrap().unwrap();
    () = first.commit(ack).await.unwrap();

    // The journal now holds a committed delta, so a replacement claims at open.
    let (_second_capture, second) = fixture.open(journal, true).await.unwrap();
    assert_ne!(first.epoch(), second.epoch());

    first_capture.offer(vec![encode_punch(1, 1)]).unwrap();
    let err = first.publish().await.unwrap_err();

    assert!(
        format!("{err:#}").contains("RegisterMismatch"),
        "expected a fenced-out append, got: {err:#}",
    );
    // Every later request reports the failure which ended the session.
    let err = first.publish().await.unwrap_err();
    assert!(format!("{err:#}").contains("session has failed"), "{err:#}");
}

/// A claim whose append landed even though its RPC did not report success is
/// resolved by finding the epoch already installed, not by choosing a new one.
async fn an_ambiguous_claim_finds_its_own_epoch(fixture: &Fixture) {
    let journal = "acmeCo/disk/ambiguous";
    let (_capture, writer) = fixture.open(journal, false).await.unwrap();

    let epoch = writer.epoch();
    let fence_record = fence::record(epoch);

    () = journal::spec::create(&fixture.client, fixture.spec(journal))
        .await
        .unwrap();
    () = fence::claim(&fixture.client, journal, None, epoch, fence_record.clone())
        .await
        .unwrap();

    // Repeating the claim is what a retry of an ambiguous append does: it
    // re-appends identical content under a check which no longer matches.
    () = fence::claim(&fixture.client, journal, None, epoch, fence_record)
        .await
        .unwrap();

    assert_eq!(
        fixture.author(journal).await.as_deref(),
        Some(fence::value(epoch).as_str()),
    );
}

/// A session which publishes nothing leaves no journal behind at all.
async fn a_session_which_never_publishes_creates_no_journal(fixture: &Fixture) {
    let journal = "acmeCo/disk/untouched";
    let (capture, writer) = fixture.open(journal, false).await.unwrap();

    assert_eq!(writer.publish().await.unwrap(), None);
    drop((capture, writer));

    assert!(!fixture.exists(journal).await);
}

/// A committed delta reads back as exactly the chunks which were captured.
async fn a_committed_delta_reads_back_as_its_chunks(fixture: &Fixture) {
    let journal = "acmeCo/disk/delta";
    let (capture, writer) = fixture.open(journal, false).await.unwrap();

    let mutations = vec![
        encode_write(0, &bytes::Bytes::from(vec![0x11; 8192]), BLOCK_SIZE),
        encode_write(2, &bytes::Bytes::from(vec![0; 4096]), BLOCK_SIZE),
        vec![encode_punch(3, 4)],
    ];
    for mutation in &mutations {
        capture.offer(mutation.clone()).unwrap();
    }

    let ack = writer.publish().await.unwrap().unwrap();
    () = writer.commit(ack.clone()).await.unwrap();

    let chunks: Vec<_> = fixture
        .read(journal)
        .await
        .into_iter()
        .flat_map(|(_uuid, record)| record.chunks)
        .collect();

    assert_eq!(chunks, mutations.concat());

    // Committing again is a protocol violation, because the delta it
    // acknowledged is already committed.
    let err = writer.commit(ack).await.unwrap_err();
    assert!(format!("{err:#}").contains("no published delta"), "{err:#}");
}

/// A delta of many mutations carries exactly one record each, covering exactly
/// the blocks those mutations wrote.
async fn a_large_delta_carries_one_record_per_mutation(fixture: &Fixture) {
    let journal = "acmeCo/disk/bounded";
    const WRITES: usize = 8;

    let (capture, writer) = fixture.open(journal, false).await.unwrap();
    let write = encode_write(0, &bytes::Bytes::from(vec![0x22; 128 * 1024]), BLOCK_SIZE);

    for _ in 0..WRITES {
        capture.offer(write.clone()).unwrap();
    }

    let ack = writer.publish().await.unwrap().unwrap();
    () = writer.commit(ack).await.unwrap();

    let records = fixture.read(journal).await;
    let mut blocks = Vec::new();
    let mut carrying = 0;

    for (_uuid, decoded) in &records {
        carrying += usize::from(!decoded.chunks.is_empty());
        blocks.extend(
            decoded
                .chunks
                .iter()
                .flat_map(|chunk| covered_blocks(chunk, BLOCK_SIZE)),
        );
    }

    // A mutation is never split, so each write is exactly one record.
    assert_eq!(carrying, WRITES);
    assert_eq!(
        blocks,
        std::iter::repeat_with(|| 0u32..32)
            .take(WRITES)
            .flatten()
            .collect::<Vec<_>>(),
    );
}

/// A journal which resolves to no fragment store never opens.
async fn a_journal_without_a_store_never_opens(fixture: &Fixture) {
    let journal = proto::JournalConfig {
        journal: "acmeCo/disk/storeless".to_string(),
        ..Default::default()
    };
    let Err(err) = fixture.writer(journal, false).await else {
        panic!("a journal with no store must not open");
    };
    assert!(format!("{err:#}").contains("no fragment store"), "{err:#}");
}

struct Fixture {
    endpoint: String,
    credential: String,
    /// Client of the test itself, which probes, reads, and applies specs.
    client: gazette::journal::Client,
}

impl Fixture {
    /// Journal stored in the test broker's file root. Every field is supplied,
    /// because the daemon has no defaults to fall back on.
    fn journal_config(&self, journal: &str) -> proto::JournalConfig {
        proto::JournalConfig {
            journal: journal.to_string(),
            fragment_stores: vec!["file:///".to_string()],
            replication: 1,
            labels: Vec::new(),
            fragment_length: 1 << 18,
            flush_interval_seconds: Some(48 * 3600),
            refresh_interval_seconds: 5 * 60,
            max_append_rate: Some(1 << 22),
            compression_codec: broker::CompressionCodec::None as i32,
        }
    }

    fn spec(&self, journal: &str) -> broker::JournalSpec {
        journal::spec::build(&self.journal_config(journal)).unwrap()
    }

    async fn open(
        &self,
        journal: &str,
        committed_state: bool,
    ) -> anyhow::Result<(Capture, Writer)> {
        self.writer(self.journal_config(journal), committed_state)
            .await
    }

    async fn writer(
        &self,
        journal: proto::JournalConfig,
        committed_state: bool,
    ) -> anyhow::Result<(Capture, Writer)> {
        let (capture, captured) = capture::channel(64, Waker::new().unwrap());
        let (client, _router) = journal::shared_client();

        let writer = Writer::open(
            &client,
            journal::Open {
                journal,
                broker: proto::Broker {
                    endpoint: self.endpoint.clone(),
                    credential: self.credential.clone(),
                },
                committed_state,
                retained: Vec::new(),
            },
            captured,
        )
        .await?;

        Ok((capture, writer))
    }

    /// Every record of `journal`, paired with its parsed UUID.
    async fn read(&self, journal: &str) -> Vec<(UuidParts, proto::DiskRecord)> {
        // A broker-confirmed head bounds the read, which is what a recovering
        // session does: without a bound the broker reports the offset beyond
        // the last record as not yet available, which is an error to a reader.
        let head = fence::probe(&self.client, journal).await.unwrap().head;

        let mut content = bytes::BytesMut::new();
        let stream = self.client.clone().read(broker::ReadRequest {
            journal: journal.to_string(),
            offset: 0,
            end_offset: head,
            block: false,
            ..Default::default()
        });
        futures::pin_mut!(stream);

        while let Some(response) = futures::StreamExt::next(&mut stream).await {
            content.extend_from_slice(&response.expect("reading a journal").content);
        }

        let mut records = Vec::new();
        let mut rest = &content[..];

        while !rest.is_empty() {
            match framing::decode::<proto::DiskRecord>(rest).expect("a record decodes") {
                framing::Frame::Record { message, consumed } => {
                    let uuid = uuid::Uuid::from_slice(&message.uuid).unwrap();
                    records.push((uuid::parse(uuid).unwrap(), message));
                    rest = &rest[consumed..];
                }
                frame => panic!("expected a record of {journal}, got {frame:?}"),
            }
        }
        records
    }

    /// Value of the journal's `author` register.
    async fn author(&self, journal: &str) -> Option<String> {
        fence::probe(&self.client, journal).await.unwrap().author
    }

    async fn exists(&self, journal: &str) -> bool {
        fence::probe(&self.client, journal).await.unwrap().exists
    }
}

type UuidParts = (uuid::Producer, uuid::Clock, uuid::Flags);

/// Sign a token carrying the capabilities a disk journal writer needs.
fn credential(cluster: &e2e_support::GazetteCluster) -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let claims = proto_gazette::Claims {
        cap: proto_gazette::capability::LIST
            | proto_gazette::capability::APPLY
            | proto_gazette::capability::READ
            | proto_gazette::capability::APPEND,
        exp: now + 3600,
        iat: now,
        iss: "disk-daemon-test".to_string(),
        sel: broker::LabelSelector::default(),
        sub: "disk-daemon-test".to_string(),
    };

    tokens::jwt::sign(claims, &cluster.encode_key).unwrap()
}
