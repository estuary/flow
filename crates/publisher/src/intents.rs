use base64::Engine;
use itertools::Itertools;
use proto_gazette::uuid;
use std::collections::BTreeMap;

/// Build per-journal ACK intent documents for a committed transaction.
///
/// Each element of `transaction` is a (producer, clock, journals) tuple
/// representing one producer's contribution. The output is a map of
/// journal_name => ndjson_bytes, where each value is the concatenated
/// newline-delimited JSON encoding of that journal's ACK documents (with a
/// trailing newline). The first ACK document for each journal carries causal
/// hints referencing all other journal+producer pairs in the transaction;
/// subsequent ACK documents for the same journal (from other producers) omit
/// hints as they'd be redundant.
pub fn build_transaction_intents(
    transaction: &[(uuid::Producer, uuid::Clock, Vec<String>)],
) -> BTreeMap<String, bytes::Bytes> {
    // Flatten and index on journal, then producer.
    let mut flattened: Vec<(&str, uuid::Producer, uuid::Clock)> = transaction
        .iter()
        .flat_map(|(producer, clock, journals)| {
            journals
                .iter()
                .map(move |journal| (journal.as_ref(), *producer, *clock))
        })
        .collect();
    flattened.sort();

    // Group by journal: we'll generate an ACK intent payload for each one.
    let group_by = flattened
        .iter()
        .chunk_by(|(journal, _producer, _clock)| *journal);

    let mut journal_acks = BTreeMap::new();

    for (this_journal, mut these_producers) in group_by.into_iter() {
        // We'll generate an ACK document for each of `these_producers`, but we
        // attach causal hints only to the first one -- others would be redundant.
        let (_this_journal, this_producer, this_commit) = these_producers.next().unwrap();

        // Walk all *other* journals+producers and generate causal hints for each.
        let group_by = flattened
            .iter()
            .filter(|(hinted_journal, hinted_producer, _clock)| {
                *hinted_journal != this_journal || hinted_producer != this_producer
            })
            .chunk_by(|(journal, _producer, _clock)| *journal);

        let mut prev_journal = this_journal;

        let hinted_journals = group_by
            .into_iter()
            .map(|(hinted_journal, hinted_producers)| {
                let (truncate_delta, suffix) = gazette::delta::encode(prev_journal, hinted_journal);
                prev_journal = hinted_journal;

                let hinted_producers = hinted_producers
                    .map(|(_hinted_journal, hinted_producer, hinted_commit)| {
                        if hinted_producer == this_producer {
                            // Decoder interprets as `this_producer` & `this_commit`
                            // without consuming space in the journal.
                            serde_json::json!({})
                        } else {
                            let p = base64::engine::general_purpose::STANDARD
                                .encode(hinted_producer.as_bytes());

                            serde_json::json!({
                                "p": p,
                                "c": format!("{:016x}", hinted_commit.as_u64()),
                            })
                        }
                    })
                    .collect::<Vec<_>>();

                serde_json::json!({
                    "j": [truncate_delta, suffix],
                    "p": hinted_producers,
                })
            })
            .collect::<Vec<_>>();

        let this_uuid = uuid::build(*this_producer, *this_commit, uuid::Flags::ACK_TXN);
        let mut buf = Vec::new();
        write_ndjson(
            &mut buf,
            &serde_json::json!({
                "_meta": { "uuid": this_uuid },
                "is_ack": true,
                "hints": hinted_journals,
            }),
        );

        // Remainder of `these_producers` also need ACK documents.
        // They were already hinted by the first ACK of this journal, and don't carry hints themselves.
        for (_this_journal, this_producer, this_commit) in these_producers {
            let this_uuid = uuid::build(*this_producer, *this_commit, uuid::Flags::ACK_TXN);
            write_ndjson(
                &mut buf,
                &serde_json::json!({
                    "_meta": { "uuid": this_uuid },
                    "is_ack": true,
                }),
            );
        }

        journal_acks.insert(this_journal.to_string(), bytes::Bytes::from(buf));
    }

    journal_acks
}

fn write_ndjson(buf: &mut Vec<u8>, doc: &serde_json::Value) {
    serde_json::to_writer(&mut *buf, doc).expect("serialization of Value cannot fail");
    buf.push(b'\n');
}

/// Decode causal hints embedded in an ACK document.
///
/// Returns a `HintIter` that yields `(hinted_journal, hinted_producer, hinted_clock)`
/// tuples with minimal allocation. The iterator reuses a single journal name buffer
/// across delta-encoded hint entries.
///
/// `this_journal` is the journal the ACK was read from. `this_producer` and `this_clock`
/// are extracted from the ACK document's UUID. `doc` is the ACK document itself.
///
/// ACK documents without a `hints` field (e.g., secondary ACKs for the same journal)
/// produce an empty iterator.
pub fn decode_transaction_hints<'a, N: json::AsNode>(
    this_journal: &str,
    this_producer: uuid::Producer,
    this_clock: uuid::Clock,
    doc: &'a N,
) -> HintIter<'a, N> {
    let hints = match doc.as_node() {
        json::Node::Object(fields) => match <N::Fields as json::Fields<N>>::get(fields, "hints") {
            Some(field) => match json::Field::value(&field).as_node() {
                json::Node::Array(arr) => arr,
                _ => &[],
            },
            None => &[],
        },
        _ => &[],
    };

    HintIter {
        this_producer,
        this_clock,
        hints,
        journal_buf: this_journal.to_string(),
        hint_idx: 0,
        producers: &[],
        producer_idx: 0,
    }
}

/// Lending iterator over decoded causal hints from an ACK document.
///
/// Each call to `next()` yields `(hinted_journal, hinted_producer, hinted_clock)`.
/// The journal reference borrows from the iterator's internal buffer and is
/// valid until the next call to `next()`.
pub struct HintIter<'a, N: json::AsNode> {
    this_producer: uuid::Producer,
    this_clock: uuid::Clock,
    hints: &'a [N],
    journal_buf: String,
    hint_idx: usize,
    producers: &'a [N],
    producer_idx: usize,
}

impl<'a, N: json::AsNode> HintIter<'a, N> {
    pub fn next(&mut self) -> Option<Result<(&str, uuid::Producer, uuid::Clock), &'static str>> {
        loop {
            if self.producer_idx < self.producers.len() {
                let entry = &self.producers[self.producer_idx];
                self.producer_idx += 1;

                return Some(
                    decode_producer_entry(entry, self.this_producer, self.this_clock)
                        .map(|(p, c)| (self.journal_buf.as_str(), p, c)),
                );
            }

            if self.hint_idx >= self.hints.len() {
                return None;
            }

            let hint = &self.hints[self.hint_idx];
            self.hint_idx += 1;

            match decode_hint_journal(hint, &mut self.journal_buf) {
                Ok(producers) => {
                    self.producers = producers;
                    self.producer_idx = 0;
                }
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

fn decode_hint_journal<'a, N: json::AsNode>(
    hint: &'a N,
    journal_buf: &mut String,
) -> Result<&'a [N], &'static str> {
    let fields = match hint.as_node() {
        json::Node::Object(f) => f,
        _ => return Err("hint entry is not an object"),
    };

    // Decode journal delta: "j": [truncate, suffix]
    let j_val = json::Field::value(
        &<N::Fields as json::Fields<N>>::get(fields, "j").ok_or("hint missing 'j' field")?,
    );
    let j_arr = match j_val.as_node() {
        json::Node::Array(arr) if arr.len() == 2 => arr,
        _ => return Err("hint 'j' is not a 2-element array"),
    };

    let truncate = match j_arr[0].as_node() {
        json::Node::PosInt(v) => v as i32,
        json::Node::NegInt(v) => v as i32,
        _ => return Err("hint truncate is not an integer"),
    };
    let suffix = match j_arr[1].as_node() {
        json::Node::String(s) => s,
        _ => return Err("hint suffix is not a string"),
    };

    gazette::delta::decode(journal_buf, truncate, suffix);

    // Extract producers array: "p": [...]
    let p_val = json::Field::value(
        &<N::Fields as json::Fields<N>>::get(fields, "p").ok_or("hint missing 'p' field")?,
    );
    match p_val.as_node() {
        json::Node::Array(arr) => Ok(arr),
        _ => Err("hint 'p' is not an array"),
    }
}

fn decode_producer_entry<N: json::AsNode>(
    entry: &N,
    this_producer: uuid::Producer,
    this_clock: uuid::Clock,
) -> Result<(uuid::Producer, uuid::Clock), &'static str> {
    let fields = match entry.as_node() {
        json::Node::Object(f) => f,
        _ => return Err("producer entry is not an object"),
    };

    // Empty object means same producer and clock as this ACK's UUID.
    if <N::Fields as json::Fields<N>>::len(fields) == 0 {
        return Ok((this_producer, this_clock));
    }

    // Decode producer from base64.
    let p_str = match json::Field::value(
        &<N::Fields as json::Fields<N>>::get(fields, "p")
            .ok_or("producer entry missing 'p' field")?,
    )
    .as_node()
    {
        json::Node::String(s) => s,
        _ => return Err("producer 'p' is not a string"),
    };

    let mut buf = [0u8; 8];
    let n = base64::engine::general_purpose::STANDARD
        .decode_slice(p_str, &mut buf)
        .map_err(|_| "invalid base64 in producer 'p'")?;
    if n != 6 {
        return Err("producer 'p' decoded to wrong length (expected 6 bytes)");
    }
    let producer = uuid::Producer(buf[..6].try_into().unwrap());

    // Decode clock from hex string.
    let c_str = match json::Field::value(
        &<N::Fields as json::Fields<N>>::get(fields, "c")
            .ok_or("producer entry missing 'c' field")?,
    )
    .as_node()
    {
        json::Node::String(s) => s,
        _ => return Err("producer 'c' is not a string"),
    };
    let clock = uuid::Clock::from_u64(
        u64::from_str_radix(c_str, 16).map_err(|_| "invalid hex in producer 'c'")?,
    );

    Ok((producer, clock))
}

#[cfg(test)]
mod test {
    use super::*;

    // Deterministic test producers (multicast bit set per RFC 4122).
    const P1: uuid::Producer = uuid::Producer([0x01, 0x00, 0x00, 0x00, 0x00, 0x01]);
    const P2: uuid::Producer = uuid::Producer([0x03, 0x00, 0x00, 0x00, 0x00, 0x02]);
    const P3: uuid::Producer = uuid::Producer([0x05, 0x00, 0x00, 0x00, 0x00, 0x03]);

    fn clock(v: u64) -> uuid::Clock {
        uuid::Clock::from_u64(v)
    }

    fn js(journals: &[&str]) -> Vec<String> {
        journals.iter().map(|j| j.to_string()).collect()
    }

    /// Parse the NDJSON output of `build_transaction_intents` back into the
    /// structured `Vec<serde_json::Value>` shape these tests assert against.
    fn parse_intents(
        journal_acks: BTreeMap<String, bytes::Bytes>,
    ) -> Vec<(String, Vec<serde_json::Value>)> {
        journal_acks
            .into_iter()
            .map(|(journal, bytes)| {
                assert_eq!(bytes.last(), Some(&b'\n'));
                let docs = bytes[..bytes.len() - 1]
                    .split(|b| *b == b'\n')
                    .map(|line| serde_json::from_slice(line).unwrap())
                    .collect();
                (journal, docs)
            })
            .collect()
    }

    #[test]
    fn test_empty_transaction() {
        let result = parse_intents(build_transaction_intents(&[]));
        insta::assert_json_snapshot!(result);
    }

    #[test]
    fn test_single_producer_single_journal() {
        let txn = vec![(P1, clock(100), js(&["acmeCo/anvils/part=a/pivot=00"]))];
        insta::assert_json_snapshot!(parse_intents(build_transaction_intents(&txn)));
    }

    #[test]
    fn test_single_producer_multiple_journals() {
        let txn = vec![(
            P1,
            clock(100),
            js(&[
                "acmeCo/anvils/part=a/pivot=00",
                "acmeCo/anvils/part=b/pivot=00",
            ]),
        )];
        insta::assert_json_snapshot!(parse_intents(build_transaction_intents(&txn)));
    }

    #[test]
    fn test_multiple_producers_single_journal() {
        let txn = vec![
            (P1, clock(100), js(&["acmeCo/anvils/part=a/pivot=00"])),
            (P2, clock(200), js(&["acmeCo/anvils/part=a/pivot=00"])),
        ];
        insta::assert_json_snapshot!(parse_intents(build_transaction_intents(&txn)));
    }

    // Three producers across four journals with overlapping membership.
    // Exercises delta encoding across consecutive hints with varying
    // shared-prefix lengths and multiple producers per hinted journal.
    #[test]
    fn test_three_producers_four_journals() {
        let txn = vec![
            (
                P1,
                clock(0x1122334455667788),
                js(&[
                    "acmeCo/anvils/part=x/pivot=00",
                    "acmeCo/hammers/pivot=00",
                    "acmeCo/anvils/part=y/pivot=00",
                ]),
            ),
            (
                P2,
                clock(0x5040302010203040),
                js(&[
                    "acmeCo/anvils/part=x/pivot=00",
                    "acmeCo/anvils/part=z/pivot=00",
                ]),
            ),
            (
                P3,
                clock(0x9181716151413121),
                js(&[
                    "acmeCo/hammers/pivot=00",
                    "acmeCo/anvils/part=y/pivot=00",
                    "acmeCo/anvils/part=z/pivot=00",
                ]),
            ),
        ];
        insta::assert_json_snapshot!(parse_intents(build_transaction_intents(&txn)));
    }

    // --- decode_transaction_hints tests ---

    /// Build the complete flattened set of (journal, producer, clock) from a transaction.
    fn flatten_transaction(
        txn: &[(uuid::Producer, uuid::Clock, Vec<String>)],
    ) -> std::collections::BTreeSet<(String, [u8; 6], u64)> {
        txn.iter()
            .flat_map(|(p, c, journals)| journals.iter().map(move |j| (j.clone(), p.0, c.as_u64())))
            .collect()
    }

    /// Verify the round-trip property: for each journal's first ACK, decoding
    /// its hints and adding back (this_journal, this_producer, this_clock)
    /// recovers the full flattened transaction set. Secondary ACKs must have
    /// no hints.
    fn assert_round_trip(txn: &[(uuid::Producer, uuid::Clock, Vec<String>)]) {
        let expected = flatten_transaction(txn);
        let journal_acks = parse_intents(build_transaction_intents(txn));

        for (journal, acks) in &journal_acks {
            // First ACK carries hints.
            let ack = &acks[0];
            let uuid_str = ack["_meta"]["uuid"].as_str().unwrap();
            let (producer, commit_clock, flags) = uuid::parse_str(uuid_str).unwrap();
            assert!(flags.is_ack());

            let mut decoded: std::collections::BTreeSet<(String, [u8; 6], u64)> =
                std::collections::BTreeSet::new();

            // The ACK's own identity is excluded from hints; add it back.
            decoded.insert((journal.clone(), producer.0, commit_clock.as_u64()));

            let mut iter = decode_transaction_hints(journal, producer, commit_clock, ack);
            while let Some(result) = iter.next() {
                let (hinted_journal, hinted_producer, hinted_clock) = result.unwrap();
                decoded.insert((
                    hinted_journal.to_string(),
                    hinted_producer.0,
                    hinted_clock.as_u64(),
                ));
            }

            assert_eq!(
                decoded, expected,
                "round-trip mismatch for journal {journal}"
            );

            // Secondary ACKs carry no hints.
            for ack in &acks[1..] {
                let uuid_str = ack["_meta"]["uuid"].as_str().unwrap();
                let (producer, clock, _) = uuid::parse_str(uuid_str).unwrap();
                let mut iter = decode_transaction_hints(journal, producer, clock, ack);
                assert!(
                    iter.next().is_none(),
                    "secondary ACK for {journal} should have no hints"
                );
            }
        }
    }

    #[test]
    fn test_decode_empty_transaction() {
        assert_round_trip(&[]);
    }

    #[test]
    fn test_decode_single_producer_single_journal() {
        let txn = vec![(P1, clock(100), js(&["acmeCo/anvils/part=a/pivot=00"]))];
        assert_round_trip(&txn);
    }

    #[test]
    fn test_decode_single_producer_multiple_journals() {
        // Exercises the `{}` self-referencing producer entry.
        let txn = vec![(
            P1,
            clock(100),
            js(&[
                "acmeCo/anvils/part=a/pivot=00",
                "acmeCo/anvils/part=b/pivot=00",
            ]),
        )];
        assert_round_trip(&txn);
    }

    #[test]
    fn test_decode_multiple_producers_single_journal() {
        let txn = vec![
            (P1, clock(100), js(&["acmeCo/anvils/part=a/pivot=00"])),
            (P2, clock(200), js(&["acmeCo/anvils/part=a/pivot=00"])),
        ];
        assert_round_trip(&txn);
    }

    #[test]
    fn test_decode_three_producers_four_journals() {
        // Exercises delta encoding, self-referencing producers, and
        // multiple producers per hinted journal across varying prefix lengths.
        let txn = vec![
            (
                P1,
                clock(0x1122334455667788),
                js(&[
                    "acmeCo/anvils/part=x/pivot=00",
                    "acmeCo/hammers/pivot=00",
                    "acmeCo/anvils/part=y/pivot=00",
                ]),
            ),
            (
                P2,
                clock(0x5040302010203040),
                js(&[
                    "acmeCo/anvils/part=x/pivot=00",
                    "acmeCo/anvils/part=z/pivot=00",
                ]),
            ),
            (
                P3,
                clock(0x9181716151413121),
                js(&[
                    "acmeCo/hammers/pivot=00",
                    "acmeCo/anvils/part=y/pivot=00",
                    "acmeCo/anvils/part=z/pivot=00",
                ]),
            ),
        ];
        assert_round_trip(&txn);
    }

    #[test]
    fn test_decode_no_hints_field() {
        // A document without "hints" produces an empty iterator.
        let doc = serde_json::json!({"_meta": {"uuid": "00000000-0000-0000-0000-000000000000"}, "is_ack": true});
        let mut iter = decode_transaction_hints("some/journal", P1, clock(1), &doc);
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_decode_empty_hints_array() {
        // An explicit empty "hints" array produces an empty iterator.
        let doc = serde_json::json!({"hints": [], "is_ack": true});
        let mut iter = decode_transaction_hints("some/journal", P1, clock(1), &doc);
        assert!(iter.next().is_none());
    }
}
