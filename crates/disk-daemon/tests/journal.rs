//! Journal writer tests against a real broker.
//!
//! A data-plane is expensive to start, so the cases share one. Each case works a
//! journal of its own, and holds its [`Capture`] for the length of the case.
//! Dropping that `Capture` closes the capture channel, which is how a session ends.

mod common;

use disk_daemon::BLOCK_SIZE;
use disk_daemon::capture::{self, Capture};
use disk_daemon::chunk::{covered_blocks, encode_punch, encode_write};
use disk_daemon::image::Image;
use disk_daemon::journal::{self, Opening, Writer, fence};
use disk_daemon::proto;
use disk_daemon::wake::Waker;
use gazette::journal::framing;
use proto_gazette::{broker, uuid};

const BLOCKS: u32 = 64;

/// No case reports a floor, so every replay seeks from zero and reads the first
/// fragment the store still holds.
const NO_FLOOR: u64 = 0;

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
    a_session_which_never_publishes_appends_nothing(&fixture).await;
    a_committed_delta_reads_back_as_its_chunks(&fixture).await;
    a_large_delta_carries_one_record_per_mutation(&fixture).await;
    a_journal_which_does_not_exist_never_opens(&fixture).await;
    an_unrecoverable_journal_never_opens(&fixture).await;
    recovery_applies_only_committed_deltas(&fixture).await;
    a_recovered_acknowledgement_is_repaired(&fixture).await;
    an_orphaned_journal_recovers_nothing(&fixture).await;

    data_plane
        .graceful_stop()
        .await
        .expect("DataPlane graceful_stop");
}

/// First use claims the journal its caller created. It installs the author
/// register with a fence record, then appends its delta under that claim.
async fn first_use_claims_the_journal(fixture: &Fixture) {
    let journal = "acmeCo/disk/first-use";
    let (capture, writer) = fixture.open(journal).await.unwrap();

    capture.offer(vec![encode_punch(3, 2)]).unwrap();
    let ack = writer
        .publish()
        .await
        .unwrap()
        .expect("the delta is not empty");
    _ = writer.commit(ack).await.unwrap();

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

/// A replacement session takes the author register. The first session then cannot
/// append.
async fn a_replacement_writer_fences_the_first(fixture: &Fixture) {
    let journal = "acmeCo/disk/contended";
    let (first_capture, first) = fixture.open(journal).await.unwrap();

    first_capture.offer(vec![encode_punch(0, 1)]).unwrap();
    let ack = first.publish().await.unwrap().unwrap();
    _ = first.commit(ack).await.unwrap();

    // The journal now holds a committed delta, so a replacement claims it as that
    // replacement recovers.
    let (_second_capture, second, _blocks) = fixture.recover(journal, Vec::new()).await.unwrap();
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

/// A claim whose append landed, even though its RPC reported no success, is
/// resolved by finding the epoch already installed. It does not choose a new epoch.
async fn an_ambiguous_claim_finds_its_own_epoch(fixture: &Fixture) {
    let journal = "acmeCo/disk/ambiguous";
    let (_capture, writer) = fixture.open(journal).await.unwrap();

    let epoch = writer.epoch();
    let fence_record = fence::record(epoch);

    () = fence::claim(&fixture.client, journal, None, epoch, fence_record.clone())
        .await
        .unwrap();

    // A retry of an ambiguous append repeats the claim. It re-appends identical
    // content under a check which no longer matches.
    () = fence::claim(&fixture.client, journal, None, epoch, fence_record)
        .await
        .unwrap();

    assert_eq!(
        fixture.author(journal).await.as_deref(),
        Some(fence::value(epoch).as_str()),
    );
}

/// A session which publishes nothing appends nothing. Its journal is left as its
/// caller created it, with no fence and no content.
async fn a_session_which_never_publishes_appends_nothing(fixture: &Fixture) {
    let journal = "acmeCo/disk/untouched";
    let (capture, writer) = fixture.open(journal).await.unwrap();

    assert_eq!(writer.publish().await.unwrap(), None);
    drop((capture, writer));

    assert_eq!(fixture.head(journal).await, 0);
    assert_eq!(fixture.author(journal).await, None);
}

/// A committed delta reads back as exactly the chunks which were captured.
async fn a_committed_delta_reads_back_as_its_chunks(fixture: &Fixture) {
    let journal = "acmeCo/disk/delta";
    let (capture, writer) = fixture.open(journal).await.unwrap();

    let mutations = vec![
        encode_write(0, &bytes::Bytes::from(vec![0x11; 8192])),
        encode_write(2, &bytes::Bytes::from(vec![0; 4096])),
        vec![encode_punch(3, 4)],
    ];
    for mutation in &mutations {
        capture.offer(mutation.clone()).unwrap();
    }

    let ack = writer.publish().await.unwrap().unwrap();
    _ = writer.commit(ack.clone()).await.unwrap();

    let chunks: Vec<_> = fixture
        .read(journal)
        .await
        .into_iter()
        .flat_map(|(_uuid, record)| record.chunks)
        .collect();

    assert_eq!(chunks, mutations.concat());

    // A second commit is a protocol violation. The delta it acknowledged is
    // already committed.
    let err = writer.commit(ack).await.unwrap_err();
    assert!(format!("{err:#}").contains("no published delta"), "{err:#}");
}

/// A delta of many mutations carries exactly one record per mutation. Those records
/// cover exactly the blocks the mutations wrote.
async fn a_large_delta_carries_one_record_per_mutation(fixture: &Fixture) {
    let journal = "acmeCo/disk/bounded";
    const WRITES: usize = 8;

    let (capture, writer) = fixture.open(journal).await.unwrap();
    let write = encode_write(0, &bytes::Bytes::from(vec![0x22; 128 * 1024]));

    for _ in 0..WRITES {
        capture.offer(write.clone()).unwrap();
    }

    let ack = writer.publish().await.unwrap().unwrap();
    _ = writer.commit(ack).await.unwrap();

    let records = fixture.read(journal).await;
    let mut blocks = Vec::new();
    let mut carrying = 0;

    for (_uuid, decoded) in &records {
        carrying += usize::from(!decoded.chunks.is_empty());
        blocks.extend(decoded.chunks.iter().flat_map(covered_blocks));
    }

    // Nothing splits a mutation, so each write is exactly one record.
    assert_eq!(carrying, WRITES);
    assert_eq!(
        blocks,
        std::iter::repeat_with(|| 0u32..32)
            .take(WRITES)
            .flatten()
            .collect::<Vec<_>>(),
    );
}

/// A journal the caller never created never opens. The daemon does not create
/// one, so there is nothing for the disk to be.
async fn a_journal_which_does_not_exist_never_opens(fixture: &Fixture) {
    let Err(err) = fixture.opening("acmeCo/disk/absent").await else {
        panic!("a journal which does not exist must not open");
    };
    assert!(format!("{err:#}").contains("does not exist"), "{err:#}");
}

/// A journal whose spec a disk could not be recovered from never opens. The
/// daemon validates that spec rather than converging it, because the spec
/// belongs to the caller which applied it.
async fn an_unrecoverable_journal_never_opens(fixture: &Fixture) {
    let journal = "acmeCo/disk/unrecoverable";

    // The daemon both appends to a disk journal and replays it.
    let mut spec = common::journal_spec(journal, broker::CompressionCodec::None, 5 * 60);
    spec.flags = broker::journal_spec::Flag::ORdonly as u32;

    () = common::create_journal(&fixture.client, spec).await.unwrap();

    let Err(err) = fixture.opening(journal).await else {
        panic!("a journal this daemon cannot append to must not open");
    };
    assert!(format!("{err:#}").contains("must be read-write"), "{err:#}");
}

/// Recovery rebuilds the deltas which committed. It discards a delta whose
/// acknowledgement never reached the journal.
async fn recovery_applies_only_committed_deltas(fixture: &Fixture) {
    let journal = "acmeCo/disk/recovered";
    let (capture, writer) = fixture.open(journal).await.unwrap();

    for (block, fill) in [(1, 0xaa), (2, 0xbb)] {
        capture.offer(write(block, fill)).unwrap();
    }
    let ack = writer.publish().await.unwrap().unwrap();
    _ = writer.commit(ack).await.unwrap();

    // A second delta, published but never committed. A session which crashed
    // between the two leaves this behind.
    capture.offer(write(2, 0xcc)).unwrap();
    capture.offer(write(3, 0xdd)).unwrap();
    _ = writer.publish().await.unwrap().unwrap();
    drop((capture, writer));

    let (_capture, _writer, blocks) = fixture.recover(journal, Vec::new()).await.unwrap();
    assert_eq!(blocks, vec![(1, 0xaa), (2, 0xbb)]);
}

/// The client made an acknowledgement durable, but it never reached the journal.
/// Recovery appends it verbatim, which commits the delta it acknowledges.
async fn a_recovered_acknowledgement_is_repaired(fixture: &Fixture) {
    let journal = "acmeCo/disk/repaired";
    let (capture, writer) = fixture.open(journal).await.unwrap();

    capture.offer(write(4, 0x11)).unwrap();
    let ack = writer.publish().await.unwrap().unwrap();
    drop((capture, writer));

    let (_capture, _writer, blocks) = fixture.recover(journal, vec![ack.clone()]).await.unwrap();

    assert_eq!(blocks, vec![(4, 0x11)]);

    // A second repair re-appends the same bytes, and Gazette de-duplicates those by
    // UUID. A session which repeats a repair therefore recovers the same disk.
    let (_capture, _writer, blocks) = fixture.recover(journal, vec![ack]).await.unwrap();
    assert_eq!(blocks, vec![(4, 0x11)]);
}

/// A journal which a failed first use left content in holds no committed state,
/// so its disk is fresh.
async fn an_orphaned_journal_recovers_nothing(fixture: &Fixture) {
    let journal = "acmeCo/disk/orphaned";
    let (capture, writer) = fixture.open(journal).await.unwrap();

    capture.offer(write(5, 0x22)).unwrap();
    _ = writer.publish().await.unwrap().unwrap();
    drop((capture, writer));

    assert!(
        fixture.head(journal).await > 0,
        "the delta reached the journal"
    );

    let (_capture, _writer, blocks) = fixture.recover(journal, Vec::new()).await.unwrap();
    assert!(blocks.is_empty(), "{blocks:?}");
}

/// One block of `fill`, as a device write of it encodes.
fn write(block: u32, fill: u8) -> Vec<proto::Chunk> {
    encode_write(block, &bytes::Bytes::from(vec![fill; BLOCK_SIZE as usize]))
}

struct Fixture {
    endpoint: String,
    credential: String,
    /// Client of the test itself, which probes, reads, and applies specs.
    client: gazette::journal::Client,
}

impl Fixture {
    /// Create `journal` as a disk's caller does, then open it as its session
    /// does. The daemon creates no journal, so every case which opens one must
    /// create it first.
    async fn open(&self, journal: &str) -> anyhow::Result<(Capture, Writer)> {
        () = common::create_journal(
            &self.client,
            common::journal_spec(journal, broker::CompressionCodec::None, 5 * 60),
        )
        .await?;

        let opening = self.opening(journal).await?;
        let (capture, captured) = capture::channel(64, Waker::new().unwrap());

        Ok((capture, opening.serve(captured, None, None)))
    }

    /// Open `journal` as a session with committed state does, and report the
    /// fill byte of every block the replay left allocated.
    async fn recover(
        &self,
        journal: &str,
        acks: Vec<bytes::Bytes>,
    ) -> anyhow::Result<(Capture, Writer, Vec<(u32, u8)>)> {
        let mut opening = self.opening(journal).await?;

        // The image outlives its directory, having no directory entry of its own.
        let dir = tempfile::tempdir()?;
        let mut image = Image::create(dir.path(), BLOCKS)?;

        _ = opening.recover(&mut image, acks, NO_FLOOR).await?;

        let mut block = vec![0u8; BLOCK_SIZE as usize];
        let blocks = image
            .allocated()
            .iter()
            .map(|index| {
                image.read_at(index, &mut block).unwrap();
                (index, block[0])
            })
            .collect();

        let (capture, captured) = capture::channel(64, Waker::new().unwrap());

        Ok((capture, opening.serve(captured, None, None), blocks))
    }

    async fn opening(&self, journal: &str) -> anyhow::Result<Opening> {
        let (client, _router) = journal::shared_client();

        Opening::new(
            &client,
            journal::Open {
                journal: journal.to_string(),
                broker: proto::Broker {
                    endpoint: self.endpoint.clone(),
                    credential: self.credential.clone(),
                },
            },
            tokio_util::sync::CancellationToken::new(),
        )
        .await
    }

    /// Every record of `journal`, paired with its parsed UUID.
    async fn read(&self, journal: &str) -> Vec<(UuidParts, proto::DiskRecord)> {
        // A broker-confirmed head bounds the read, as a recovering session does.
        // Without a bound, the broker reports the offset beyond the last record as
        // not yet available, and a reader treats that as an error.
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

    /// Broker-confirmed write head. A journal which was created and never
    /// appended to holds zero.
    async fn head(&self, journal: &str) -> i64 {
        fence::probe(&self.client, journal).await.unwrap().head
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
