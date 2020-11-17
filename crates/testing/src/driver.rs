use catalog::specs::{TestStep, TestStepIngest, TestStepVerify};
use itertools::{
    EitherOrBoth::{Both, Left, Right},
    Itertools,
};
use prost::Message;
use protocol::consumer;
use protocol::flow;
use protocol::protocol::header;
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};

/// Offsets are a bag of journals and their byte offset.
pub type Offsets = BTreeMap<String, i64>;

/// Transform is a minimal description of a flow transform as used by Driver.
#[derive(Deserialize)]
pub struct Transform {
    source_name: String,
    transform_name: String,
    derivation_name: String,
    read_delay_seconds: u64,
    has_publish: bool,
}

impl Transform {
    /// Load all Transforms from a catalog database.
    pub fn load_all(db: &rusqlite::Connection) -> Result<Vec<Transform>, rusqlite::Error> {
        db.prepare(
            "SELECT
        source_name,
        transform_name,
        derivation_name,
        IFNULL(read_delay_seconds, 0),
        publish_id IS NOT NULL
        FROM transform_details;",
        )?
        .query_map(rusqlite::NO_PARAMS, |r| {
            Ok(Transform {
                source_name: r.get(0)?,
                transform_name: r.get(1)?,
                derivation_name: r.get(2)?,
                read_delay_seconds: r.get::<_, i64>(3)? as u64,
                has_publish: r.get(4)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()
    }
}

/// PendingStat describes a derivation's read of an upstream source which
/// may not have happened yet. PendingStats are tracked within a BTreeSet, keyed
/// and ordered on (read_at_seconds, derivation). PendingStats are reducible,
/// and multiple test operations may produce PendingStats which are folded
/// into a single tracked stat.
#[derive(Debug, Default)]
pub struct PendingStat {
    pub ready_at_seconds: u64,
    pub derivation: String,

    pub journal_etcd: header::Etcd,
    pub offsets: Offsets,
    pub may_publish: bool,
}

impl Ord for PendingStat {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let other = (other.ready_at_seconds, &other.derivation);
        (self.ready_at_seconds, &self.derivation).cmp(&other)
    }
}

impl PartialOrd for PendingStat {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for PendingStat {}

impl PartialEq for PendingStat {
    fn eq(&self, other: &Self) -> bool {
        self.derivation == other.derivation && self.ready_at_seconds == other.ready_at_seconds
    }
}

impl PendingStat {
    /// Build the shard StatRequest implied by this PendingRead.
    pub fn stat_request(&self, shard: String) -> consumer::StatRequest {
        let mut extension = Vec::new();
        self.journal_etcd.encode(&mut extension).unwrap();

        consumer::StatRequest {
            header: None,
            shard,
            read_through: self.offsets.iter().map(|(j, o)| (j.clone(), *o)).collect(),
            extension,
        }
    }

    // Merge an Iterator of PendingStats into a BTreeSet<PendingStat>.
    fn merge_into(set: &mut BTreeSet<PendingStat>, it: impl Iterator<Item = PendingStat>) {
        for next in it {
            let merged = match set.take(&next) {
                Some(mut prior) => {
                    // Fold |next| into |prior| by deeply merging to take the
                    // maximum offset observed for a journal.
                    prior.offsets = prior
                        .offsets
                        .into_iter()
                        .merge_join_by(next.offsets.into_iter(), |(l, _), (r, _)| l.cmp(r))
                        .map(|either| match either {
                            Both((j, lhs), (_, rhs)) => (j, lhs.max(rhs)),
                            Left((j, o)) | Right((j, o)) => (j, o),
                        })
                        .collect();

                    if prior.journal_etcd.revision < next.journal_etcd.revision {
                        prior.journal_etcd = next.journal_etcd;
                    }
                    prior.may_publish |= next.may_publish;
                    prior
                }
                None => next,
            };
            set.insert(merged);
        }
    }
}

/// Driver coordinates the execution of a catalog test case.
pub struct Driver<'a> {
    // All catalog transforms. Ingests and shard stats are projected through
    // transforms to identify implied reads which should occur.
    transforms: &'a [Transform],
    // Transitive collection dependencies, ordered as (derived-collection, source-collection),
    // and inclusive of a base-case entry where a collection depends on itself.
    collection_dependencies: &'a [(String, String)],
    // Remaining steps of the test. Dequeued from the list head, as the test executes.
    steps: &'a [TestStep],
    // Current test time.
    at_seconds: u64,
    // Maximum observed offset for each journal.
    at_heads: Offsets,
    // Pending reads which remain to be stat-ed.
    pending: BTreeSet<PendingStat>,
}

/// Action atom returned by Driver to progress the test.
#[derive(Debug)]
pub enum Action<'a> {
    /// Stat one or more PendingReads which can now be expected to have completed.
    Stat(Vec<PendingStat>),
    /// Ingest documents into a collection.
    Ingest(&'a TestStepIngest),
    /// Verify derived documents of a collection.
    Verify(&'a TestStepVerify),
    /// Advance test time by the given number of seconds.
    Advance(u64),
}

impl<'a> Iterator for Driver<'a> {
    type Item = Action<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        // Split off PendingStats having ready_at_seconds == self.at_seconds into |ready|,
        // vs everything else which remains in |self.pending|.
        let mut ready = self.pending.split_off(&PendingStat {
            ready_at_seconds: self.at_seconds + 1,
            ..Default::default()
        });
        std::mem::swap(&mut self.pending, &mut ready);

        // If there are ready PendingStats, return them.
        if !ready.is_empty() {
            return Some(Action::Stat(ready.into_iter().collect()));
        }

        // Determine the smallest time interval we could advance to unblock a pending stat.
        let min_advance = self
            .pending
            .iter()
            .next()
            .map(|p| p.ready_at_seconds - self.at_seconds);

        match (min_advance, self.steps.split_first()) {
            (None, None) => {
                // Test is complete.
                None
            }
            (Some(advance), None) => {
                self.at_seconds += advance;
                Some(Action::Advance(advance))
            }
            (None, Some((TestStep::Verify(verify), tail))) => {
                self.steps = tail;
                Some(Action::Verify(verify))
            }
            (Some(advance), Some((TestStep::Verify(verify), tail))) => {
                // Is there a pending stat of |verify.collection|, or one of it's dependencies?
                let predicate = |read: &PendingStat| {
                    self.collection_dependencies
                        .binary_search_by_key(
                            &(verify.collection.as_ref(), &read.derivation),
                            |(der, src)| (&der, &src),
                        )
                        .is_ok()
                };
                if self.pending.iter().any(predicate) {
                    self.at_seconds += advance;
                    Some(Action::Advance(advance))
                } else {
                    self.steps = tail;
                    Some(Action::Verify(verify))
                }
            }
            (_, Some((TestStep::Ingest(spec), tail))) => {
                // Dequeue and return a next Ingest.
                self.steps = tail;
                Some(Action::Ingest(spec))
            }
        }
    }
}

impl<'a> Driver<'a> {
    /// Construct a new Driver of the given test case |steps|, and |transforms|.
    pub fn new(
        transforms: &'a [Transform],
        collection_dependencies: &'a [(String, String)],
        steps: &'a [TestStep],
    ) -> Driver<'a> {
        Driver {
            transforms,
            collection_dependencies,
            steps,
            at_seconds: 0,
            at_heads: Offsets::new(),
            pending: BTreeSet::new(),
        }
    }

    /// Tell the driver of a completed ingestion.
    pub fn completed_ingest(&mut self, spec: &TestStepIngest, response: flow::IngestResponse) {
        log::info!("completed_ingest: {:?}", response);
        self.merge_max_heads(&response.journal_write_heads);

        PendingStat::merge_into(
            &mut self.pending,
            project_publish(
                self.at_seconds,
                self.transforms,
                spec.collection.as_ref(),
                &response.journal_etcd.unwrap_or_default(),
                &response.journal_write_heads,
            ),
        );
    }

    /// Tell the Driver of a completed shard stat.
    pub fn completed_stat(&mut self, read: &PendingStat, response: consumer::StatResponse) {
        log::info!("completed_stat: {:?}", response);
        self.merge_max_heads(&response.publish_at);

        if read.may_publish {
            PendingStat::merge_into(
                &mut self.pending,
                project_publish(
                    self.at_seconds,
                    self.transforms,
                    &read.derivation,
                    &header::Etcd::decode(response.extension.as_ref()).unwrap(),
                    &response.publish_at,
                ),
            );
        }
    }

    /// Journal "write heads" (maximum observed written offsets for each journal)
    /// as tracked by this Driver.
    pub fn journal_heads(&self) -> &Offsets {
        &self.at_heads
    }

    fn merge_max_heads<'d, O>(&mut self, offsets: O)
    where
        O: IntoIterator<Item = (&'d String, &'d i64)> + Copy,
    {
        for (j, next) in offsets.into_iter() {
            let (j, o) = match self.at_heads.remove_entry(j) {
                Some((j, prior)) => (j, prior.max(*next)),
                None => (j.clone(), *next),
            };
            self.at_heads.insert(j, o);
        }
    }
}

// Project a publish into a collection, with the given offsets and journal revision,
// through catalog transforms to arrive at implied future reads which will occur.
fn project_publish<'a, O>(
    at_seconds: u64,
    transforms: &'a [Transform],
    collection: &'a str,
    journal_etcd: &'a header::Etcd,
    offsets: O,
) -> impl Iterator<Item = PendingStat> + 'a
where
    O: IntoIterator<Item = (&'a String, &'a i64)> + Copy + 'a,
{
    transforms
        .iter()
        .filter(move |t| t.source_name == collection)
        .map(move |transform| PendingStat {
            derivation: transform.derivation_name.clone(),
            ready_at_seconds: at_seconds + transform.read_delay_seconds,
            journal_etcd: journal_etcd.clone(),
            offsets: offsets
                .into_iter()
                .map(|(j, o)| {
                    (
                        format!(
                            "{};transform/{}/{}",
                            j, transform.derivation_name, transform.transform_name
                        ),
                        *o,
                    )
                })
                .collect(),
            may_publish: transform.has_publish,
        })
}

#[cfg(test)]
mod test {
    use super::*;
    use serde_json::json;
    use std::collections::HashMap;

    #[test]
    fn test_evaluation_of_fixture_test_case() {
        let transforms: Vec<Transform> = serde_json::from_value(json!([
            {
                "source_name": "A",
                "transform_name": "a-update",
                "derivation_name": "B",
                "read_delay_seconds": 0,
                "has_publish": false,
            },
            {
                "source_name": "A",
                "transform_name": "a-to-b-1",
                "derivation_name": "B",
                "read_delay_seconds": 1,
                "has_publish": true,
            },
            {
                "source_name": "A",
                "transform_name": "a-to-b-2",
                "derivation_name": "B",
                "read_delay_seconds": 3,
                "has_publish": true,
            },
            {
                "source_name": "B",
                "transform_name": "b-to-c",
                "derivation_name": "C",
                "read_delay_seconds": 2,
                "has_publish": true,
            },
            {
                "source_name": "Y",
                "transform_name": "y-to-z",
                "derivation_name": "Z",
                "read_delay_seconds": 3600,
                "has_publish": true,
            },
        ]))
        .unwrap();

        let collection_dependencies: Vec<(String, String)> = serde_json::from_value(json!([
            ["B", "A"],
            ["B", "B"],
            ["C", "A"],
            ["C", "B"],
            ["C", "C"],
            ["Z", "Y"],
            ["Z", "Z"],
        ]))
        .unwrap();

        let steps: Vec<TestStep> = serde_json::from_value(json!([
            {"ingest": {"collection": "A", "documents": [true]}},
            {"ingest": {"collection": "Y", "documents": [true]}},
            {"verify": {"collection": "C", "documents": [true]}},
        ]))
        .unwrap();

        let mut driver = Driver::new(&transforms, &collection_dependencies, &steps);

        // Test begins with ingestion to A.
        match driver.next() {
            Some(Action::Ingest(spec)) => driver.completed_ingest(
                spec,
                flow::IngestResponse {
                    journal_etcd: Some(header::Etcd {
                        revision: 867,
                        ..Default::default()
                    }),
                    journal_write_heads: vec![("A/part=0".to_owned(), 111)].into_iter().collect(),
                },
            ),
            v @ _ => panic!("{:?}", v),
        };

        // a-update is ready to stat immediately, but does not publish.
        match driver.next() {
            Some(Action::Stat(reads)) => {
                assert_eq!(reads.len(), 1);
                let a_update = &reads[0];

                assert_eq!(a_update.derivation, "B");
                assert_eq!(
                    a_update.offsets,
                    vec![("A/part=0;transform/B/a-update".to_owned(), 111)]
                        .into_iter()
                        .collect()
                );
                assert_eq!(a_update.journal_etcd.revision, 867);

                driver.completed_stat(
                    a_update,
                    consumer::StatResponse {
                        status: 0,
                        header: None,
                        read_through: HashMap::new(), // Ignored by CaseDriver.
                        // publish_at is ignored for this stat, as update-only
                        // transforms don't publish to the derived collection.
                        publish_at: vec![("B/part=0".to_owned(), 2)].into_iter().collect(),
                        extension: Vec::new(),
                    },
                );
            }
            v @ _ => panic!("{:?}", v),
        };

        // We next ingest into Y.
        match driver.next() {
            Some(Action::Ingest(spec)) => driver.completed_ingest(
                spec,
                flow::IngestResponse {
                    journal_etcd: None,
                    journal_write_heads: vec![("Y/part=0".to_owned(), 999)].into_iter().collect(),
                },
            ),
            v @ _ => panic!("{:?}", v),
        };

        // Wait for a-to-b-1, then stat it.
        match driver.next() {
            Some(Action::Advance(1)) => (),
            v @ _ => panic!("{:?}", v),
        };
        match driver.next() {
            Some(Action::Stat(reads)) => {
                assert_eq!(reads.len(), 1);
                let a_to_b1 = &reads[0];

                assert_eq!(a_to_b1.derivation, "B");
                assert_eq!(
                    a_to_b1.offsets,
                    vec![("A/part=0;transform/B/a-to-b-1".to_owned(), 111)]
                        .into_iter()
                        .collect()
                );
                assert_eq!(a_to_b1.journal_etcd.revision, 867);

                driver.completed_stat(
                    a_to_b1,
                    consumer::StatResponse {
                        status: 0,
                        header: None,
                        read_through: HashMap::new(),
                        publish_at: vec![("B/part=0".to_owned(), 200)].into_iter().collect(),
                        extension: Vec::new(),
                    },
                );

                // A Stat action can map to multiple stat-ed shards, which can produce
                // slightly different offsets (depending on their append ordering).
                // We fold StatResponses to track the max offset across journals.
                driver.completed_stat(
                    a_to_b1,
                    consumer::StatResponse {
                        status: 0,
                        header: None,
                        read_through: HashMap::new(),
                        publish_at: vec![("B/part=0".to_owned(), 222)].into_iter().collect(),
                        // Stat returns a journal Etcd header which advances the current revision.
                        extension: {
                            let mut b = Vec::new();
                            header::Etcd {
                                revision: 912,
                                ..Default::default()
                            }
                            .encode(&mut b)
                            .unwrap();
                            b
                        },
                    },
                );
            }
            v @ _ => panic!("{:?}", v),
        };

        // Wait for a-to-b-2 and b-to-c, and stat them.
        match driver.next() {
            Some(Action::Advance(2)) => (),
            v @ _ => panic!("{:?}", v),
        };
        match driver.next() {
            Some(Action::Stat(reads)) => {
                // Expect that both stats unblocked at the same time.
                assert_eq!(reads.len(), 2);
                let a_to_b2 = &reads[0];
                let b_to_c = &reads[1];

                assert_eq!(a_to_b2.derivation, "B");
                assert_eq!(
                    a_to_b2.offsets,
                    vec![("A/part=0;transform/B/a-to-b-2".to_owned(), 111)]
                        .into_iter()
                        .collect()
                );
                // Must read through journal revision of the original ingest.
                assert_eq!(a_to_b2.journal_etcd.revision, 867);

                assert_eq!(b_to_c.derivation, "C");
                assert_eq!(
                    b_to_c.offsets,
                    vec![("B/part=0;transform/C/b-to-c".to_owned(), 222)]
                        .into_iter()
                        .collect()
                );
                // Must read through revision returned by 'a-to-b1' stat.
                assert_eq!(b_to_c.journal_etcd.revision, 912);

                driver.completed_stat(
                    a_to_b2,
                    consumer::StatResponse {
                        status: 0,
                        header: None,
                        read_through: HashMap::new(),
                        publish_at: vec![("B/part=0".to_owned(), 333)].into_iter().collect(),
                        extension: Vec::new(),
                    },
                );
                driver.completed_stat(
                    b_to_c,
                    consumer::StatResponse {
                        status: 0,
                        header: None,
                        read_through: HashMap::new(),
                        publish_at: vec![("C/part=0".to_owned(), 444)].into_iter().collect(),
                        extension: Vec::new(),
                    },
                );
            }
            v @ _ => panic!("{:?}", v),
        };

        // Wait for b-to-c a final time, and stat.
        match driver.next() {
            Some(Action::Advance(2)) => (),
            v @ _ => panic!("{:?}", v),
        };
        match driver.next() {
            Some(Action::Stat(reads)) => {
                // Expect that both stats unblocked at the same time.
                assert_eq!(reads.len(), 1);
                let b_to_c = &reads[0];

                assert_eq!(b_to_c.derivation, "C");
                assert_eq!(
                    b_to_c.offsets,
                    vec![("B/part=0;transform/C/b-to-c".to_owned(), 333)]
                        .into_iter()
                        .collect()
                );

                driver.completed_stat(
                    b_to_c,
                    consumer::StatResponse {
                        status: 0,
                        header: None,
                        read_through: HashMap::new(),
                        publish_at: vec![("C/part=0".to_owned(), 555)].into_iter().collect(),
                        extension: Vec::new(),
                    },
                );
            }
            v @ _ => panic!("{:?}", v),
        };

        // Verify "C".
        match driver.next() {
            Some(Action::Verify(spec)) => {
                assert_eq!(spec.collection.as_ref(), "C");
            }
            v @ _ => panic!("{:?}", v),
        };

        // All test steps have been consumed, but there's still a pending stat
        // of "Z" we must await.
        match driver.next() {
            Some(Action::Advance(3595)) => (),
            v @ _ => panic!("{:?}", v),
        };
        match driver.next() {
            Some(Action::Stat(reads)) => {
                assert_eq!(reads.len(), 1);
                let y_to_z = &reads[0];

                assert_eq!(y_to_z.derivation, "Z");
                assert_eq!(
                    y_to_z.offsets,
                    vec![("Y/part=0;transform/Z/y-to-z".to_owned(), 999)]
                        .into_iter()
                        .collect()
                );

                driver.completed_stat(
                    y_to_z,
                    consumer::StatResponse {
                        status: 0,
                        header: None,
                        read_through: HashMap::new(),
                        publish_at: vec![("Z/part=0".to_owned(), 1000)].into_iter().collect(),
                        extension: Vec::new(),
                    },
                );
            }
            v @ _ => panic!("{:?}", v),
        };

        // *Now* the test case is complete.
        match driver.next() {
            None => (),
            v @ _ => panic!("{:?}", v),
        }

        assert_eq!(driver.at_seconds, 3600);
        assert_eq!(
            driver.at_heads,
            vec![
                ("A/part=0".to_owned(), 111),
                ("B/part=0".to_owned(), 333),
                ("C/part=0".to_owned(), 555),
                ("Y/part=0".to_owned(), 999),
                ("Z/part=0".to_owned(), 1000),
            ]
            .into_iter()
            .collect()
        );
    }
}
