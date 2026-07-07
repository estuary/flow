//! Wire format helpers for connector state patches.
//!
//! Connector state updates move through runtime-next as a framed sequence of
//! JSON merge patches. The empty byte string means "no patches"; otherwise the
//! payload is a JSON array whose elements are tab-delimited.
//!
//! The delimiter is a tab rather than a newline because image connectors read
//! requests as newline-delimited JSON (`derive-typescript`'s `readLines`): a
//! raw newline inside the embedded `statePatches` array would split one Flush
//! request across lines and corrupt it. A tab is safe because compact JSON
//! never contains a raw tab — control characters are escaped inside strings and
//! there is no insignificant whitespace — yet it is still valid JSON whitespace
//! between array elements, so a connector can `JSON.parse` the payload directly.

use anyhow::Context;
use bytes::Bytes;

#[derive(Debug, thiserror::Error)]
#[error("{reason}")]
pub struct MalformedStatePatches {
    pub reason: &'static str,
}

/// Apply a State-Update-Wire-Format payload of JSON merge patches to a
/// JSON-encoded connector state document, returning the updated document.
///
/// Empty `state_json` is interpreted as an empty object.
pub fn apply_state_patches(state_json: &Bytes, patches_json: &Bytes) -> anyhow::Result<Bytes> {
    let mut doc = if state_json.is_empty() {
        serde_json::Value::Object(Default::default())
    } else {
        serde_json::from_slice(state_json).context("parsing connector state JSON")?
    };

    for patch in split_state_patches(patches_json)? {
        let patch = serde_json::from_slice(&patch).context("parsing connector state patch")?;
        json_patch::merge(&mut doc, &patch);
    }

    Ok(Bytes::from(serde_json::to_vec(&doc)?))
}

/// Encode a connector-supplied [`proto_flow::flow::ConnectorState`] into a
/// State-Update-Wire-Format payload accepted by [`extend_state_patches`] /
/// [`split_state_patches`].
///
/// `None` and the proto default both yield empty bytes ("zero patches"). When
/// `merge_patch` is false the connector wants its prior state replaced — we
/// encode that as a leading `null` patch (state reset) followed by the
/// connector's `updated_json`.
///
/// The wire format delimits patches with `\t`, so a raw tab byte in the
/// connector's `updated_json` would corrupt framing — [`split_state_patches`]
/// would truncate the patch at the first embedded tab. An RFC-8259-compliant
/// encoder escapes in-string tabs as `\t` (two bytes), so a raw tab can only
/// appear as structural whitespace from a pretty-printer (e.g.
/// `json.MarshalIndent(_, "", "\t")`). We defensively replace any such tab with
/// a space, which is semantically inert between JSON tokens; the `memchr` scan
/// is cheap and the replacement loop runs only in this vanishingly rare case.
pub fn encode_connector_state(state: Option<proto_flow::flow::ConnectorState>) -> Bytes {
    let Some(proto_flow::flow::ConnectorState {
        merge_patch,
        updated_json,
    }) = state
    else {
        return Bytes::new();
    };

    if updated_json.is_empty() {
        return Bytes::new();
    }

    let mut b = Vec::with_capacity(updated_json.len() + 12);
    b.push(b'[');

    if !merge_patch {
        b.extend_from_slice(b"null\t,");
    }

    let json_start = b.len();
    b.extend_from_slice(&updated_json);
    if memchr::memchr(b'\t', &b[json_start..]).is_some() {
        for byte in &mut b[json_start..] {
            if *byte == b'\t' {
                *byte = b' ';
            }
        }
    }

    b.extend_from_slice(b"\t]");

    b.into()
}

/// A State-Update-Wire-Format payload that resets persisted connector state to
/// an empty object. Used by the derive Reset flow to durably clear connector
/// state after a `Request.Reset`, so RocksDB agrees the reset connector's state
/// is empty. Encoded as a full replacement (leading `null`) followed by `{}`.
pub fn reset_connector_state_patch() -> Bytes {
    encode_connector_state(Some(proto_flow::flow::ConnectorState {
        updated_json: Bytes::from_static(b"{}"),
        merge_patch: false,
    }))
}

/// Extend a State-Update-Wire-Format payload with another payload.
///
/// Both `out` and `src` use the framed JSON-array wire form accepted by
/// [`split_state_patches`]. Empty bytes is interpreted as "zero patches".
pub fn extend_state_patches(out: &mut Vec<u8>, src: &[u8]) {
    if out.is_empty() {
        out.extend_from_slice(src);
    } else if !src.is_empty() {
        out.truncate(out.len() - 1); // Remove trailing ']'.
        let src = &src[1..]; // Remove leading '['.

        out.push(b','); // Add separator.
        out.extend_from_slice(src);
    }
}

/// Split a State-Update-Wire-Format payload into its individual JSON patches.
///
/// The wire form is always framed — a JSON array with each patch terminated by
/// a tab and prefixed by `[` (first) or `,` (subsequent), with a closing `]`.
/// Empty bytes is interpreted as "zero patches".
pub fn split_state_patches(payload: &Bytes) -> Result<Vec<Bytes>, MalformedStatePatches> {
    if payload.is_empty() {
        return Ok(Vec::new());
    }
    if payload.first() != Some(&b'[') {
        return Err(MalformedStatePatches {
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
                if tail == 0 || (tail == 1 && payload[cursor + 1] == b'\t') {
                    return Ok(out);
                }
                return Err(MalformedStatePatches {
                    reason: "trailing bytes after closing `]`",
                });
            }
            _ => {
                return Err(MalformedStatePatches {
                    reason: "expected framing `[`, `,`, or `]`",
                });
            }
        }

        // Handle `[]` (empty array) and guard against a stray trailing comma.
        if payload.get(cursor) == Some(&b']') {
            if out.is_empty() {
                let tail = payload.len() - cursor - 1;
                if tail == 0 || (tail == 1 && payload[cursor + 1] == b'\t') {
                    return Ok(out);
                }
                return Err(MalformedStatePatches {
                    reason: "trailing bytes after closing `]`",
                });
            }
            return Err(MalformedStatePatches {
                reason: "trailing comma before `]`",
            });
        }

        let delim =
            payload[cursor..]
                .iter()
                .position(|b| *b == b'\t')
                .ok_or(MalformedStatePatches {
                    reason: "missing trailing tab",
                })?;
        let end = cursor + delim;
        out.push(payload.slice(cursor..end));
        cursor = end + 1;
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use proto_flow::flow;

    #[test]
    fn split_state_patches_cases() {
        let ok_cases: &[(&[u8], &[&[u8]])] = &[
            // Empty bytes tolerated (proto default) as "zero patches".
            (b"", &[]),
            // Canonical zero-patches wire form.
            (b"[]", &[]),
            (b"[]\t", &[]),
            (b"[{\"a\":1}\t]", &[b"{\"a\":1}"]),
            (
                b"[{\"a\":1}\t,{\"b\":2}\t,{\"c\":3}\t]",
                &[b"{\"a\":1}", b"{\"b\":2}", b"{\"c\":3}"],
            ),
            // Trailing tab after `]` is permitted.
            (b"[{\"a\":1}\t]\t", &[b"{\"a\":1}"]),
        ];
        for (input, want) in ok_cases {
            let got = split_state_patches(&Bytes::copy_from_slice(input)).unwrap();
            let got: Vec<&[u8]> = got.iter().map(|b| b.as_ref()).collect();
            assert_eq!(got, *want, "input {:?}", String::from_utf8_lossy(input));
        }

        let err_cases: &[&[u8]] = &[
            b"{\"a\":1}",                // bare single-patch form is no longer valid
            b"[{\"a\":1}]",              // missing trailing tab
            b"[{\"a\":1}\t] extra",      // junk after closing
            b"[{\"a\":1}\t{\"b\":2}\t]", // missing inter-entry comma
            b"[{\"a\":1}\t,]",           // trailing comma before `]`
        ];
        for input in err_cases {
            split_state_patches(&Bytes::copy_from_slice(input)).unwrap_err();
        }
    }

    #[test]
    fn extend_state_patches_cases() {
        let mut out = Vec::new();
        extend_state_patches(&mut out, b"");
        assert!(out.is_empty());

        extend_state_patches(&mut out, b"[{\"a\":1}\t]");
        assert_eq!(out.as_slice(), b"[{\"a\":1}\t]");

        extend_state_patches(&mut out, b"");
        assert_eq!(out.as_slice(), b"[{\"a\":1}\t]");

        extend_state_patches(&mut out, b"[{\"b\":2}\t,{\"c\":null}\t]");
        assert_eq!(out.as_slice(), b"[{\"a\":1}\t,{\"b\":2}\t,{\"c\":null}\t]");

        let decoded = split_state_patches(&Bytes::from(out)).unwrap();
        let decoded: Vec<&[u8]> = decoded.iter().map(|b| b.as_ref()).collect();
        assert_eq!(
            decoded,
            vec![
                b"{\"a\":1}".as_slice(),
                b"{\"b\":2}".as_slice(),
                b"{\"c\":null}".as_slice()
            ]
        );
    }

    #[test]
    fn encode_connector_state_cases() {
        let cases = [
            (None, b"".as_slice(), Vec::<&[u8]>::new()),
            (
                Some(flow::ConnectorState::default()),
                b"",
                Vec::<&[u8]>::new(),
            ),
            (
                Some(flow::ConnectorState {
                    updated_json: Bytes::from_static(br#"{"a":1}"#),
                    merge_patch: true,
                }),
                b"[{\"a\":1}\t]",
                vec![br#"{"a":1}"#.as_slice()],
            ),
            (
                Some(flow::ConnectorState {
                    updated_json: Bytes::from_static(br#"{"a":1}"#),
                    merge_patch: false,
                }),
                b"[null\t,{\"a\":1}\t]",
                vec![b"null".as_slice(), br#"{"a":1}"#.as_slice()],
            ),
            // Raw tabs in pretty-printed connector JSON are replaced with spaces
            // so they don't corrupt the tab-delimited framing.
            (
                Some(flow::ConnectorState {
                    updated_json: Bytes::from_static(b"{\n\t\"a\": 1\n}"),
                    merge_patch: true,
                }),
                b"[{\n \"a\": 1\n}\t]",
                vec![b"{\n \"a\": 1\n}".as_slice()],
            ),
        ];

        for (state, want_encoded, want_split) in cases {
            let encoded = encode_connector_state(state);
            assert_eq!(encoded.as_ref(), want_encoded);

            let split = split_state_patches(&encoded).unwrap();
            let split: Vec<&[u8]> = split.iter().map(|b| b.as_ref()).collect();
            assert_eq!(split, want_split);
        }
    }

    #[test]
    fn replacement_state_resets_prior_state() {
        let patches = encode_connector_state(Some(flow::ConnectorState {
            updated_json: Bytes::from_static(br#"{"kept":1}"#),
            merge_patch: false,
        }));
        let updated =
            apply_state_patches(&Bytes::from_static(br#"{"dropped":true}"#), &patches).unwrap();

        let updated: serde_json::Value = serde_json::from_slice(&updated).unwrap();
        assert_eq!(updated, serde_json::json!({"kept": 1}));
    }
}
