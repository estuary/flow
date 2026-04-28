//! On-disk codec for the RocksDB keys that back a runtime-next task.
//!
//! [`encode_persist`] turns a [`proto::Persist`] message into an ordered
//! sequence of [`KeyOp`] effects that a runtime crate stages into a real
//! `rocksdb::WriteBatch`. [`State::decode_key_value`] is called once per
//! `(key, value)` pair from a full `rocksdb::DB` scan on session startup,
//! folding the entry into a [`State`] snapshot. Neither entry point
//! touches `rocksdb` types.
//!
//! `{state_key}` below is the binding-stable `state_key` field of
//! `flow::MaterializationSpec.Binding` — distinct from `journal_read_suffix`,
//! which is `materialize/{materialization}/{state_key}`. Both encode and
//! decode use a caller-supplied `binding_state_keys` mapping to translate
//! between the binding indices used in the leader protocol and the
//! `state_key` strings used in RocksDB keys.
//!
//! | Prefix       | Key tail                                 | Value                            |
//! |--------------|------------------------------------------|----------------------------------|
//! | `FH:`        | `{journal}\0{state_key}\0{producer[6]}`  | proto `shuffle.ProducerFrontier` |
//! | `FC:`        | `{journal}\0{state_key}\0{producer[6]}`  | proto `shuffle.ProducerFrontier` |
//! | `AI:`        | `{journal}`                              | raw ACK intent bytes             |
//! | `MK-v2:`     | `{state_key}`                            | `tuple::pack` packed key         |
//! | (singleton)  | `connector-state`                        | reduced JSON merge-patch         |
//! | (singleton)  | `trigger-params`                         | JSON `TriggerVariables`          |
//! | (singleton)  | `last-applied`                           | proto task spec                  |

use crate::proto;
use bytes::{BufMut, Bytes, BytesMut};
use prost::Message;
use proto_gazette::uuid;
use std::collections::BTreeMap;

/// Key prefix for hinted Frontier entries:
/// `FH:{journal}\0{state_key}\0{producer}`.
pub const PREFIX_HINTED_FRONTIER: &[u8] = b"FH:";
/// Exclusive upper bound used for `DeleteRange` over `PREFIX_HINTED_FRONTIER`.
pub const PREFIX_HINTED_FRONTIER_END: &[u8] = b"FH;";
/// Key prefix for committed Frontier entries:
/// `FC:{journal}\0{state_key}\0{producer}`.
pub const PREFIX_COMMITTED_FRONTIER: &[u8] = b"FC:";
/// Key prefix for per-journal ACK intent entries: `AI:{journal}`.
pub const PREFIX_ACK_INTENT: &[u8] = b"AI:";
/// Exclusive upper bound used for `DeleteRange` over `PREFIX_ACK_INTENT`.
pub const PREFIX_ACK_INTENT_END: &[u8] = b"AI;";
/// Key prefix for per-binding max-key entries: `MK-v2:{state_key}`.
pub const PREFIX_MAX_KEY: &[u8] = b"MK-v2:";
/// Reduced connector state (opaque JSON).
pub const KEY_CONNECTOR_STATE: &[u8] = b"connector-state";
/// Trigger variables (JSON `models::TriggerVariables`).
pub const KEY_TRIGGER_PARAMS: &[u8] = b"trigger-params";
/// Last-applied task spec (protobuf bytes).
pub const KEY_LAST_APPLIED: &[u8] = b"last-applied";

/// A single write effect contributed by a `Persist`. Values are carried as
/// [`Bytes`] so shared allocations (e.g. a proto-decoded
/// `connector_patches_json` buffer) can be split without copies.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyOp {
    Put {
        key: Bytes,
        value: Bytes,
    },
    Merge {
        key: Bytes,
        value: Bytes,
    },
    Delete {
        key: Bytes,
    },
    /// Delete keys in the half-open range `[from, to)`.
    DeleteRange {
        from: Bytes,
        to: Bytes,
    },
}

/// Errors produced by [`encode_persist`].
#[derive(Debug, thiserror::Error)]
pub enum EncodeError {
    #[error(
        "FrontierChunk journal has binding index {binding}, but only {num_bindings} \
         binding state_keys were supplied"
    )]
    UnknownBinding { binding: u32, num_bindings: usize },
    #[error("connector_patches_json is framed (starts with `[`) but is malformed: {reason}")]
    MalformedStatePatches { reason: &'static str },
}

/// Errors produced by [`decode_state`].
#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    #[error("duplicate {kind} singleton key")]
    DuplicateSingleton { kind: &'static str },
    #[error("FH:/FC: key is malformed: {reason}")]
    MalformedFrontierKey { reason: &'static str },
    #[error("FH:/FC: value failed to decode as ProducerFrontier")]
    FrontierValueDecode(#[source] prost::DecodeError),
    #[error("key component is not valid UTF-8")]
    InvalidUtf8(#[source] std::str::Utf8Error),
    #[error(transparent)]
    RocksDB(#[from] rocksdb::Error),
}

/// Encode a [`proto::Persist`] as an ordered list of [`KeyOp`] effects.
///
/// `binding_state_keys[i]` is the stable `state_key` for binding index `i`
/// in the Persist's `FrontierChunk`s — written as the key tail of
/// `FH:/FC:{journal}\0{state_key}\0{producer}`. This matches the existing
/// runtime's `MK-v2:{state_key}` keying.
///
/// Each populated field maps independently to one effect — this function
/// does not validate semantic combinations (e.g., ACK intents without a
/// committed frontier). Callers compose their own invariants across the
/// stream of `Persist` messages that accumulate into a single WriteBatch.
pub fn encode_persist<S: AsRef<str>>(
    persist: &proto::Persist,
    binding_state_keys: &[S],
) -> Result<Vec<KeyOp>, EncodeError> {
    let proto::Persist {
        nonce: _,
        delete_hinted_frontier,
        hinted_frontier,
        committed_frontier,
        connector_patches_json,
        max_keys,
        delete_ack_intents,
        ack_intents,
        delete_trigger_params,
        trigger_params_json,
        last_applied,
    } = persist;

    let mut buf = BytesMut::new();
    let mut ops = Vec::new();

    if *delete_hinted_frontier {
        ops.push(KeyOp::DeleteRange {
            from: Bytes::from_static(PREFIX_HINTED_FRONTIER),
            to: Bytes::from_static(PREFIX_HINTED_FRONTIER_END),
        });
    }
    if let Some(chunk) = hinted_frontier {
        encode_frontier_chunk(
            PREFIX_HINTED_FRONTIER,
            chunk,
            binding_state_keys,
            &mut ops,
            &mut buf,
        )?;
    }

    if let Some(chunk) = committed_frontier {
        encode_frontier_chunk(
            PREFIX_COMMITTED_FRONTIER,
            chunk,
            binding_state_keys,
            &mut ops,
            &mut buf,
        )?;
    }

    for patch in split_state_patches(connector_patches_json)? {
        ops.push(KeyOp::Merge {
            key: Bytes::from_static(KEY_CONNECTOR_STATE),
            value: patch,
        });
    }

    for (binding, packed_key) in max_keys {
        let state_key = binding_state_keys
            .get(*binding as usize)
            .ok_or(EncodeError::UnknownBinding {
                binding: *binding,
                num_bindings: binding_state_keys.len(),
            })?
            .as_ref();

        buf.extend_from_slice(PREFIX_MAX_KEY);
        buf.extend_from_slice(state_key.as_bytes());
        ops.push(KeyOp::Put {
            key: buf.split().freeze(),
            value: packed_key.clone(),
        });
    }

    if *delete_ack_intents {
        ops.push(KeyOp::DeleteRange {
            from: Bytes::from_static(PREFIX_ACK_INTENT),
            to: Bytes::from_static(PREFIX_ACK_INTENT_END),
        });
    }
    for (journal, intent) in ack_intents {
        buf.extend_from_slice(PREFIX_ACK_INTENT);
        buf.extend_from_slice(journal.as_bytes());
        ops.push(KeyOp::Put {
            key: buf.split().freeze(),
            value: intent.clone(),
        });
    }

    if *delete_trigger_params {
        ops.push(KeyOp::Delete {
            key: Bytes::from_static(KEY_TRIGGER_PARAMS),
        });
    }
    if !trigger_params_json.is_empty() {
        ops.push(KeyOp::Put {
            key: Bytes::from_static(KEY_TRIGGER_PARAMS),
            value: trigger_params_json.clone(),
        });
    }

    if !last_applied.is_empty() {
        ops.push(KeyOp::Put {
            key: Bytes::from_static(KEY_LAST_APPLIED),
            value: last_applied.clone(),
        });
    }

    Ok(ops)
}

fn encode_frontier_chunk<S: AsRef<str>>(
    prefix: &'static [u8],
    chunk: &shuffle::proto::FrontierChunk,
    binding_state_keys: &[S],
    ops: &mut Vec<KeyOp>,
    buf: &mut BytesMut,
) -> Result<(), EncodeError> {
    // FrontierChunk.journals is delta-encoded against a running journal name.
    let mut journal = String::new();

    for jf in &chunk.journals {
        let new_len = journal
            .len()
            .saturating_sub(jf.journal_name_truncate_delta.max(0) as usize);
        journal.truncate(new_len);
        journal.push_str(&jf.journal_name_suffix);

        let state_key = binding_state_keys
            .get(jf.binding as usize)
            .ok_or(EncodeError::UnknownBinding {
                binding: jf.binding,
                num_bindings: binding_state_keys.len(),
            })?
            .as_ref();

        for producer in &jf.producers {
            let producer_id = uuid::Producer::from_i64(producer.producer);
            append_frontier_key(buf, prefix, &journal, state_key, producer_id.as_bytes());
            let key = buf.split().freeze();

            // The producer id is captured in the key tail; clear it from the
            // value so the field has exactly one source of truth.
            let value = shuffle::proto::ProducerFrontier {
                producer: 0,
                ..*producer
            };
            value.encode(buf).expect("BytesMut never errors on encode");
            ops.push(KeyOp::Put {
                key,
                value: buf.split().freeze(),
            });
        }
    }

    Ok(())
}

fn append_frontier_key(
    out: &mut BytesMut,
    prefix: &[u8],
    journal: &str,
    state_key: &str,
    producer: &[u8; 6],
) {
    out.extend_from_slice(prefix);
    out.extend_from_slice(journal.as_bytes());
    out.put_u8(0);
    out.extend_from_slice(state_key.as_bytes());
    out.put_u8(0);
    out.extend_from_slice(producer);
}

/// Encode JSON patches as a State-Update-Wire-Format payload.
///
/// The wire form is always a JSON array with one patch per line:
/// zero patches is empty bytes, one or more patches are framed with `[` / `,`
/// prefixes and `\n` terminators, closed by `]`.
pub fn encode_state_patches(patches: &[Bytes]) -> Bytes {
    if patches.is_empty() {
        return Bytes::new();
    }
    let body_len: usize = patches.iter().map(|p| p.len()).sum();
    let mut out = Vec::with_capacity(body_len + 2 * patches.len() + 1);
    for (i, patch) in patches.iter().enumerate() {
        out.push(if i == 0 { b'[' } else { b',' });
        out.extend_from_slice(patch);
        out.push(b'\n');
    }
    out.push(b']');
    Bytes::from(out)
}

/// Split a State-Update-Wire-Format payload into its individual JSON patches.
///
/// The wire form is always framed — a JSON array with each patch on its own
/// line, prefixed by `[` (first) or `,` (subsequent) and terminated by `\n`,
/// with a closing `]`. Empty bytes is interpreted as "zero patches".
pub fn split_state_patches(payload: &Bytes) -> Result<Vec<Bytes>, EncodeError> {
    if payload.is_empty() {
        return Ok(Vec::new());
    }
    if payload.first() != Some(&b'[') {
        return Err(EncodeError::MalformedStatePatches {
            reason: "expected leading `[`",
        });
    }

    let mut out = Vec::new();
    let mut cursor = 0usize;
    loop {
        match payload.get(cursor) {
            Some(b'[') | Some(b',') => cursor += 1,
            Some(b']') => {
                let tail = payload.len() - cursor - 1;
                if tail == 0 || (tail == 1 && payload[cursor + 1] == b'\n') {
                    return Ok(out);
                }
                return Err(EncodeError::MalformedStatePatches {
                    reason: "trailing bytes after closing `]`",
                });
            }
            _ => {
                return Err(EncodeError::MalformedStatePatches {
                    reason: "expected framing `[`, `,`, or `]`",
                });
            }
        }

        // Handle `[]` (empty array) and guard against a stray trailing comma.
        if payload.get(cursor) == Some(&b']') {
            if out.is_empty() {
                let tail = payload.len() - cursor - 1;
                if tail == 0 || (tail == 1 && payload[cursor + 1] == b'\n') {
                    return Ok(out);
                }
                return Err(EncodeError::MalformedStatePatches {
                    reason: "trailing bytes after closing `]`",
                });
            }
            return Err(EncodeError::MalformedStatePatches {
                reason: "trailing comma before `]`",
            });
        }

        let newline = payload[cursor..].iter().position(|b| *b == b'\n').ok_or(
            EncodeError::MalformedStatePatches {
                reason: "missing trailing newline",
            },
        )?;
        let end = cursor + newline;
        out.push(payload.slice(cursor..end));
        cursor = end + 1;
    }
}

// ---------------------------------------------------------------------------
// Decoder
// ---------------------------------------------------------------------------

/// Plain-old-data snapshot of the recovery-log state RocksDB holds for a
/// task, accumulated by repeated calls to [`State::decode_key_value`].
///
/// `hinted` and `committed` are append-only Vecs of `JournalFrontier`
/// entries pushed in RocksDB byte-sort order on the persisted
/// `(journal, state_key, producer)` key tail. These must be sorted
/// on `(journal, binding)` prior to being fed to shuffle::Frontier::new().
#[derive(Debug, Default, Clone)]
pub struct State {
    /// `FH:` entries.
    pub hinted: Vec<shuffle::JournalFrontier>,
    /// `FC:` entries.
    pub committed: Vec<shuffle::JournalFrontier>,
    /// `AI:` range, keyed by journal name.
    pub ack_intents: BTreeMap<String, Bytes>,
    /// `MK-v2:` range, keyed by binding index.
    pub max_keys: BTreeMap<u32, Bytes>,
    pub connector_state: Option<Bytes>,
    pub trigger_params: Option<Bytes>,
    pub last_applied: Option<Bytes>,
    /// Keys the decoder could not classify, captured for diagnostics.
    pub unknown: Vec<(Bytes, Bytes)>,
}

impl State {
    /// Decode one `(key, value)` pair into `self`.
    ///
    /// `binding_state_keys` is a slice of `(state_key, binding_index)`
    /// tuples sorted on `state_key`, used to translate persisted
    /// `state_key`s in `FH:`/`FC:`/`MK-v2:` keys into their current
    /// binding indices. Entries whose `state_key` does not appear in
    /// the slice are silently dropped — they belong to bindings that
    /// have been removed or backfilled.
    pub fn decode_key_value(
        &mut self,
        key: &[u8],
        value: &[u8],
        binding_state_keys: &[(String, u32)],
    ) -> Result<(), DecodeError> {
        if let Some(rest) = key.strip_prefix(PREFIX_HINTED_FRONTIER) {
            decode_frontier_entry(&mut self.hinted, rest, value, binding_state_keys)
        } else if let Some(rest) = key.strip_prefix(PREFIX_COMMITTED_FRONTIER) {
            decode_frontier_entry(&mut self.committed, rest, value, binding_state_keys)
        } else if let Some(rest) = key.strip_prefix(PREFIX_ACK_INTENT) {
            let journal = std::str::from_utf8(rest).map_err(DecodeError::InvalidUtf8)?;
            self.ack_intents
                .insert(journal.to_owned(), Bytes::copy_from_slice(value));
            Ok(())
        } else if let Some(rest) = key.strip_prefix(PREFIX_MAX_KEY) {
            let state_key = std::str::from_utf8(rest).map_err(DecodeError::InvalidUtf8)?;
            if let Some(binding) = lookup_binding(binding_state_keys, state_key) {
                self.max_keys.insert(binding, Bytes::copy_from_slice(value));
            }
            Ok(())
        } else if key == KEY_CONNECTOR_STATE {
            set_singleton(&mut self.connector_state, value, "connector-state")
        } else if key == KEY_TRIGGER_PARAMS {
            set_singleton(&mut self.trigger_params, value, "trigger-params")
        } else if key == KEY_LAST_APPLIED {
            set_singleton(&mut self.last_applied, value, "last-applied")
        } else {
            self.unknown
                .push((Bytes::copy_from_slice(key), Bytes::copy_from_slice(value)));
            Ok(())
        }
    }
}

fn set_singleton(
    slot: &mut Option<Bytes>,
    value: &[u8],
    kind: &'static str,
) -> Result<(), DecodeError> {
    if slot.is_some() {
        Err(DecodeError::DuplicateSingleton { kind })
    } else {
        *slot = Some(Bytes::copy_from_slice(value));
        Ok(())
    }
}

/// Binary-search `binding_state_keys` for `state_key`, returning the
/// associated binding index if present.
fn lookup_binding(binding_state_keys: &[(String, u32)], state_key: &str) -> Option<u32> {
    binding_state_keys
        .binary_search_by(|probe| probe.0.as_str().cmp(state_key))
        .ok()
        .map(|i| binding_state_keys[i].1)
}

fn decode_frontier_entry(
    target: &mut Vec<shuffle::JournalFrontier>,
    rest: &[u8],
    value: &[u8],
    binding_state_keys: &[(String, u32)],
) -> Result<(), DecodeError> {
    // Layout: journal \0 state_key \0 producer[6]
    if rest.len() < 6 + 2 {
        return Err(DecodeError::MalformedFrontierKey {
            reason: "too short",
        });
    }
    let (head, producer_bytes) = rest.split_at(rest.len() - 6);
    // `head` should end with the NUL that separates state_key from the
    // producer id. Strip it so the remaining bytes are `journal \0 state_key`.
    let head = head
        .strip_suffix(b"\0")
        .ok_or(DecodeError::MalformedFrontierKey {
            reason: "missing NUL before producer id",
        })?;

    let sep = head
        .iter()
        .position(|b| *b == 0)
        .ok_or(DecodeError::MalformedFrontierKey {
            reason: "missing state_key separator",
        })?;
    let (journal, after) = head.split_at(sep);
    let state_key = &after[1..];
    if state_key.contains(&0) {
        return Err(DecodeError::MalformedFrontierKey {
            reason: "stray NUL in state_key",
        });
    }
    let journal = std::str::from_utf8(journal).map_err(DecodeError::InvalidUtf8)?;
    let state_key = std::str::from_utf8(state_key).map_err(DecodeError::InvalidUtf8)?;

    let Some(binding) = lookup_binding(binding_state_keys, state_key) else {
        return Ok(()); // Backfilled or removed binding — discard.
    };
    let binding = binding as u16;

    let proto = shuffle::proto::ProducerFrontier::decode(value)
        .map_err(DecodeError::FrontierValueDecode)?;
    // The encoder stores the producer id only in the key; rehydrate it here.
    let key_id: [u8; 6] = producer_bytes.try_into().unwrap();

    let producer = shuffle::ProducerFrontier {
        producer: uuid::Producer::from_bytes(key_id),
        last_commit: uuid::Clock::from_u64(proto.last_commit),
        hinted_commit: uuid::Clock::from_u64(proto.hinted_commit),
        offset: proto.offset,
    };

    // RocksDB iterates in byte-sort order, so consecutive FH/FC entries for
    // the same (journal, state_key) — and therefore (journal, binding) —
    // arrive together.
    if let Some(last) = target.last_mut()
        && last.journal.as_ref() == journal
        && last.binding == binding
    {
        last.producers.push(producer);
    } else {
        target.push(shuffle::JournalFrontier {
            journal: journal.into(),
            binding,
            producers: vec![producer],
            bytes_read_delta: 0,
            bytes_behind_delta: 0,
        });
    }

    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;

    fn producer_id(tag: u8) -> [u8; 6] {
        [0x01, tag, 0, 0, 0, 0]
    }

    fn producer_frontier(
        tag: u8,
        last_commit: u64,
        offset: i64,
    ) -> shuffle::proto::ProducerFrontier {
        shuffle::proto::ProducerFrontier {
            producer: uuid::Producer::from_bytes(producer_id(tag)).as_i64(),
            last_commit,
            hinted_commit: 0,
            offset,
        }
    }

    // Two journals, two bindings, with the second journal name delta-encoded
    // against the first.
    fn chunk_fixture() -> shuffle::proto::FrontierChunk {
        shuffle::proto::FrontierChunk {
            journals: vec![
                shuffle::proto::JournalFrontier {
                    journal_name_suffix: "acme/events/000".into(),
                    binding: 0,
                    producers: vec![
                        producer_frontier(0xaa, 100, 250),
                        producer_frontier(0xbb, 90, -300),
                    ],
                    ..Default::default()
                },
                shuffle::proto::JournalFrontier {
                    journal_name_truncate_delta: 3,
                    journal_name_suffix: "001".into(),
                    binding: 1,
                    producers: vec![producer_frontier(0xcc, 50, -50)],
                    ..Default::default()
                },
            ],
            ..Default::default()
        }
    }

    fn frontier_key(prefix: &[u8], journal: &[u8], state_key: &[u8], producer: [u8; 6]) -> Bytes {
        let mut k = Vec::from(prefix);
        k.extend_from_slice(journal);
        k.push(0);
        k.extend_from_slice(state_key);
        k.push(0);
        k.extend_from_slice(&producer);
        Bytes::from(k)
    }

    fn prefixed(prefix: &[u8], tail: &[u8]) -> Bytes {
        let mut k = Vec::from(prefix);
        k.extend_from_slice(tail);
        Bytes::from(k)
    }

    fn max_keys_map(entries: &[(u32, &'static [u8])]) -> std::collections::BTreeMap<u32, Bytes> {
        entries
            .iter()
            .map(|(k, v)| (*k, Bytes::from_static(v)))
            .collect()
    }

    /// Sorted `Vec<(state_key, binding_index)>` mapping for
    /// `State::decode_key_value`.
    fn state_key_index(entries: &[(&str, u32)]) -> Vec<(String, u32)> {
        let mut v: Vec<(String, u32)> = entries
            .iter()
            .map(|(sk, idx)| ((*sk).to_string(), *idx))
            .collect();
        v.sort_by(|a, b| a.0.cmp(&b.0));
        v
    }

    /// Drive a (key, value) iterable through `State::decode_key_value`.
    fn decode_pairs<I>(pairs: I, binding_state_keys: &[(String, u32)]) -> Result<State, DecodeError>
    where
        I: IntoIterator<Item = (Bytes, Bytes)>,
    {
        let mut state = State::default();
        for (k, v) in pairs {
            state.decode_key_value(&k, &v, binding_state_keys)?;
        }
        Ok(state)
    }

    fn ack_intents_map(
        entries: &[(&str, &'static [u8])],
    ) -> std::collections::BTreeMap<String, Bytes> {
        entries
            .iter()
            .map(|(j, v)| (j.to_string(), Bytes::from_static(v)))
            .collect()
    }

    #[test]
    fn encode_persist_snapshots() {
        // Fixture strings stand in for `state_key`s resolved by binding index.
        let binding_state_keys: &[&str] = &["materialize/mat/t1", "materialize/mat/t2"];
        let cases: Vec<(&str, proto::Persist)> = vec![
            ("empty", proto::Persist::default()),
            // Maximal one-shot commit pins op ordering across every populated field.
            (
                "one_shot_commit",
                proto::Persist {
                    delete_hinted_frontier: true,
                    hinted_frontier: Some(chunk_fixture()),
                    committed_frontier: Some(chunk_fixture()),
                    connector_patches_json: Bytes::from_static(b"[{\"cursor\":\"abc\"}\n]"),
                    max_keys: max_keys_map(&[(0, b"packed-1"), (1, b"packed-2")]),
                    delete_ack_intents: true,
                    ack_intents: ack_intents_map(&[("acme/events/000", b"ack-bytes-A")]),
                    trigger_params_json: Bytes::from_static(b"{\"run_id\":\"r1\"}"),
                    last_applied: Bytes::from_static(b"spec-proto-bytes"),
                    ..Default::default()
                },
            ),
            // committed_frontier without the AI: prelude: the new proto
            // decouples delete_ack_intents from committed_frontier.
            (
                "committed_no_acks",
                proto::Persist {
                    committed_frontier: Some(chunk_fixture()),
                    ..Default::default()
                },
            ),
            // Standalone ack clear: DeleteRange alone, no Put follow-up.
            (
                "delete_ack_alone",
                proto::Persist {
                    delete_ack_intents: true,
                    ..Default::default()
                },
            ),
            (
                "standalone_trigger_delete",
                proto::Persist {
                    connector_patches_json: Bytes::from_static(b"[{\"idle\":true}\n]"),
                    delete_trigger_params: true,
                    ..Default::default()
                },
            ),
            // delete_trigger_params combined with a fresh Put: the codec emits
            // Delete then Put so the final state is the new trigger value.
            (
                "trigger_delete_then_put",
                proto::Persist {
                    delete_trigger_params: true,
                    trigger_params_json: Bytes::from_static(b"{\"run_id\":\"r2\"}"),
                    ..Default::default()
                },
            ),
        ];

        let snapshot: Vec<(&str, Vec<KeyOp>)> = cases
            .into_iter()
            .map(|(name, p)| (name, encode_persist(&p, binding_state_keys).unwrap()))
            .collect();
        insta::assert_debug_snapshot!(snapshot);
    }

    #[test]
    fn encode_persist_hinted_then_committed_roundtrip() {
        // Encode a hinted batch followed by a committed batch, replay both
        // through an in-memory store, and round-trip the result through the
        // decoder.
        let persist1 = proto::Persist {
            delete_hinted_frontier: true,
            hinted_frontier: Some(chunk_fixture()),
            max_keys: max_keys_map(&[(0, b"mk-v1")]),
            ..Default::default()
        };
        let persist2 = proto::Persist {
            committed_frontier: Some(chunk_fixture()),
            connector_patches_json: Bytes::from_static(
                b"[{\"a\":1}\n,{\"b\":null}\n,{\"c\":[1,2]}\n]",
            ),
            delete_ack_intents: true,
            ack_intents: ack_intents_map(&[
                ("acme/events/000", b"ack-000"),
                ("acme/events/001", b"ack-001"),
            ]),
            delete_trigger_params: true,
            ..Default::default()
        };

        let binding_state_keys = &["materialize/mat/t1", "materialize/mat/t2"];
        let ops1 = encode_persist(&persist1, binding_state_keys).unwrap();
        let ops2 = encode_persist(&persist2, binding_state_keys).unwrap();

        let mut store: Vec<(Bytes, Bytes)> = Vec::new();
        for op in ops1.into_iter().chain(ops2.into_iter()) {
            apply_op(&mut store, op);
        }
        store.sort_by(|a, b| a.0.cmp(&b.0));

        let mapping = state_key_index(&[("materialize/mat/t1", 0), ("materialize/mat/t2", 1)]);
        insta::assert_debug_snapshot!(decode_pairs(store, &mapping).unwrap());
    }

    #[test]
    fn encode_persist_errors() {
        let cases: Vec<(&str, proto::Persist, &[&str], &str)> = vec![(
            "unknown_binding",
            proto::Persist {
                committed_frontier: Some(chunk_fixture()),
                ..Default::default()
            },
            &["only-one-state-key"],
            "UnknownBinding",
        )];

        for (label, persist, state_keys, want) in cases {
            let err = encode_persist(&persist, state_keys).unwrap_err();
            assert!(
                format!("{err:?}").contains(want),
                "{label}: expected {want}, got {err:?}"
            );
        }
    }

    #[test]
    fn split_state_patches_cases() {
        let ok_cases: &[(&[u8], &[&[u8]])] = &[
            // Empty bytes tolerated (proto default) as "zero patches".
            (b"", &[]),
            // Canonical zero-patches wire form.
            (b"[]", &[]),
            (b"[]\n", &[]),
            (b"[{\"a\":1}\n]", &[b"{\"a\":1}"]),
            (
                b"[{\"a\":1}\n,{\"b\":2}\n,{\"c\":3}\n]",
                &[b"{\"a\":1}", b"{\"b\":2}", b"{\"c\":3}"],
            ),
            // Trailing newline after `]` is permitted.
            (b"[{\"a\":1}\n]\n", &[b"{\"a\":1}"]),
        ];
        for (input, want) in ok_cases {
            let got = split_state_patches(&Bytes::copy_from_slice(input)).unwrap();
            let got: Vec<&[u8]> = got.iter().map(|b| b.as_ref()).collect();
            assert_eq!(got, *want, "input {:?}", String::from_utf8_lossy(input));
        }

        let err_cases: &[&[u8]] = &[
            b"{\"a\":1}",                // bare single-patch form is no longer valid
            b"[{\"a\":1}]",              // missing trailing newline
            b"[{\"a\":1}\n] extra",      // junk after closing
            b"[{\"a\":1}\n{\"b\":2}\n]", // missing inter-entry comma
            b"[{\"a\":1}\n,]",           // trailing comma before `]`
        ];
        for input in err_cases {
            let err = split_state_patches(&Bytes::copy_from_slice(input)).unwrap_err();
            assert!(
                matches!(err, EncodeError::MalformedStatePatches { .. }),
                "input {:?}: got {err:?}",
                String::from_utf8_lossy(input),
            );
        }
    }

    #[test]
    fn encode_state_patches_roundtrip() {
        let cases: &[&[&[u8]]] = &[
            &[],
            &[b"{\"a\":1}"],
            &[b"{\"a\":1}", b"{\"b\":null}", b"{\"c\":[1,2]}"],
        ];
        for patches in cases {
            let patches: Vec<Bytes> = patches.iter().map(|p| Bytes::copy_from_slice(p)).collect();
            let encoded = encode_state_patches(&patches);
            let decoded = split_state_patches(&encoded).unwrap();
            assert_eq!(decoded, patches);
        }

        // Zero patches encode to empty bytes (State Update Wire Format).
        let empty = encode_state_patches(&[]);
        assert!(empty.is_empty());

        // One-patch form is framed identically to N-patch form.
        let one = encode_state_patches(&[Bytes::from_static(b"{\"a\":1}")]);
        assert_eq!(one.as_ref(), b"[{\"a\":1}\n]");

        // Multi form uses the wire framing consumed by split_state_patches.
        let multi = encode_state_patches(&[
            Bytes::from_static(b"{\"a\":1}"),
            Bytes::from_static(b"{\"b\":2}"),
        ]);
        assert_eq!(multi.as_ref(), b"[{\"a\":1}\n,{\"b\":2}\n]");
    }

    #[test]
    fn decode_state_classifies_ranges() {
        let fh_value = producer_frontier(0xaa, 777, 12345).encode_to_vec();
        let fc_value = producer_frontier(0xbb, 999, 4242).encode_to_vec();

        let pairs = vec![
            (
                frontier_key(
                    PREFIX_HINTED_FRONTIER,
                    b"journal/0",
                    b"derive/d/binding",
                    producer_id(0xaa),
                ),
                Bytes::from(fh_value),
            ),
            (
                frontier_key(
                    PREFIX_COMMITTED_FRONTIER,
                    b"journal/0",
                    b"derive/d/binding",
                    producer_id(0xbb),
                ),
                Bytes::from(fc_value),
            ),
            (
                Bytes::from_static(KEY_CONNECTOR_STATE),
                Bytes::from_static(b"{\"reduced\":true}"),
            ),
            (
                Bytes::from_static(KEY_TRIGGER_PARAMS),
                Bytes::from_static(b"{\"run_id\":\"r\"}"),
            ),
            (
                Bytes::from_static(KEY_LAST_APPLIED),
                Bytes::from_static(b"proto-bytes"),
            ),
            (
                prefixed(PREFIX_MAX_KEY, b"derive/d/binding"),
                Bytes::from_static(b"pk"),
            ),
            (
                prefixed(PREFIX_ACK_INTENT, b"j1"),
                Bytes::from_static(b"ack"),
            ),
            (Bytes::from_static(b"unknown-key"), Bytes::from_static(b"!")),
        ];

        let mapping = state_key_index(&[("derive/d/binding", 0)]);
        insta::assert_debug_snapshot!(decode_pairs(pairs, &mapping).unwrap());
    }

    #[test]
    fn decode_drops_unknown_state_keys() {
        // FH:/FC: and MK-v2: entries whose state_key is not in the
        // current binding mapping are silently discarded — they belong
        // to backfilled or removed bindings.
        let fh = producer_frontier(0xaa, 1, 0).encode_to_vec();
        let pairs = vec![
            (
                frontier_key(
                    PREFIX_HINTED_FRONTIER,
                    b"j",
                    b"removed-binding",
                    producer_id(0xaa),
                ),
                Bytes::from(fh),
            ),
            (
                prefixed(PREFIX_MAX_KEY, b"removed-binding"),
                Bytes::from_static(b"pk"),
            ),
        ];
        let state = decode_pairs(pairs, &state_key_index(&[("kept-binding", 0)])).unwrap();
        assert!(state.hinted.is_empty());
        assert!(state.max_keys.is_empty());
        assert!(state.unknown.is_empty());
    }

    #[test]
    fn decode_state_errors() {
        let valid_value = Bytes::from(producer_frontier(0xaa, 1, 0).encode_to_vec());

        // FH:/FC: layout: rest = journal \0 state_key \0 producer[6].
        #[allow(clippy::type_complexity)]
        let cases: Vec<(&str, Vec<(Bytes, Bytes)>, &str)> = vec![
            (
                "fh_too_short",
                vec![(
                    prefixed(PREFIX_HINTED_FRONTIER, b"abc"),
                    valid_value.clone(),
                )],
                "too short",
            ),
            (
                "fh_missing_nul_before_producer",
                // 9-byte rest: head = "abc" (no trailing \0), producer = 6 bytes.
                vec![(
                    prefixed(PREFIX_HINTED_FRONTIER, b"abc\xaa\xaa\xaa\xaa\xaa\xaa"),
                    valid_value.clone(),
                )],
                "missing NUL before producer id",
            ),
            (
                "fh_missing_state_key_separator",
                // head = "abc\0", strip → "abc", no inner \0.
                vec![(
                    prefixed(PREFIX_HINTED_FRONTIER, b"abc\0\xaa\xaa\xaa\xaa\xaa\xaa"),
                    valid_value.clone(),
                )],
                "missing state_key separator",
            ),
            (
                "fh_stray_nul_in_state_key",
                // head = "j\0sk\0extra\0" → strip → "j\0sk\0extra"; state_key contains \0.
                vec![(
                    prefixed(
                        PREFIX_HINTED_FRONTIER,
                        b"j\0sk\0extra\0\xaa\xaa\xaa\xaa\xaa\xaa",
                    ),
                    valid_value.clone(),
                )],
                "stray NUL in state_key",
            ),
            (
                "fh_value_decode_failure",
                // Single byte 0x80 is an incomplete varint and fails prost decoding.
                vec![(
                    frontier_key(PREFIX_HINTED_FRONTIER, b"j", b"sk", producer_id(0xaa)),
                    Bytes::from_static(b"\x80"),
                )],
                "FrontierValueDecode",
            ),
            (
                "ai_invalid_utf8",
                vec![(prefixed(PREFIX_ACK_INTENT, b"\xff\xfe"), Bytes::new())],
                "InvalidUtf8",
            ),
            (
                "duplicate_connector_state",
                vec![
                    (
                        Bytes::from_static(KEY_CONNECTOR_STATE),
                        Bytes::from_static(b"{}"),
                    ),
                    (
                        Bytes::from_static(KEY_CONNECTOR_STATE),
                        Bytes::from_static(b"{}"),
                    ),
                ],
                "connector-state",
            ),
        ];

        // Mapping must include the state_keys exercised by the FH/FC fixtures
        // (else those errors are masked by silent drop).
        let mapping = state_key_index(&[("sk", 0)]);
        for (label, pairs, want) in cases {
            let err = decode_pairs(pairs, &mapping).unwrap_err();
            assert!(
                format!("{err:?}").contains(want),
                "{label}: expected {want}, got {err:?}"
            );
        }
    }

    // Apply a KeyOp to an in-memory sorted store, respecting DeleteRange.
    // Merge is treated as append-with-newline so the round-trip snapshot sees
    // the framed accumulation; real RocksDB would reduce via the merge operator.
    fn apply_op(store: &mut Vec<(Bytes, Bytes)>, op: KeyOp) {
        match op {
            KeyOp::Put { key, value } => {
                store.retain(|(k, _)| k != &key);
                store.push((key, value));
            }
            KeyOp::Merge { key, value } => {
                if let Some(existing) = store.iter_mut().find(|(k, _)| k == &key) {
                    let mut merged = Vec::with_capacity(existing.1.len() + 1 + value.len());
                    merged.extend_from_slice(&existing.1);
                    merged.push(b'\n');
                    merged.extend_from_slice(&value);
                    existing.1 = Bytes::from(merged);
                } else {
                    store.push((key, value));
                }
            }
            KeyOp::Delete { key } => {
                store.retain(|(k, _)| k != &key);
            }
            KeyOp::DeleteRange { from, to } => {
                store.retain(|(k, _)| !(k.as_ref() >= from.as_ref() && k.as_ref() < to.as_ref()));
            }
        }
    }
}
