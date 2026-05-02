//! Wire format helpers for connector state patches.
//!
//! Connector state updates move through runtime-next as a framed sequence of
//! JSON merge patches. The empty byte string means "no patches"; otherwise the
//! payload is a JSON array with one patch per line.

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
pub fn encode_connector_state(state: Option<proto_flow::flow::ConnectorState>) -> Bytes {
    let Some(proto_flow::flow::ConnectorState {
        merge_patch,
        updated_json,
    }) = state
    else {
        return Bytes::new();
    };

    let mut b = Vec::with_capacity(updated_json.len() + 12);
    b.push(b'[');

    if !merge_patch {
        b.extend_from_slice(b"null,\n,");
    }
    b.extend_from_slice(&updated_json);
    b.extend_from_slice(b"\n]");

    b.into()
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
/// The wire form is always framed — a JSON array with each patch on its own
/// line, prefixed by `[` (first) or `,` (subsequent) and terminated by `\n`,
/// with a closing `]`. Empty bytes is interpreted as "zero patches".
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
                if tail == 0 || (tail == 1 && payload[cursor + 1] == b'\n') {
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
                if tail == 0 || (tail == 1 && payload[cursor + 1] == b'\n') {
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

        let newline =
            payload[cursor..]
                .iter()
                .position(|b| *b == b'\n')
                .ok_or(MalformedStatePatches {
                    reason: "missing trailing newline",
                })?;
        let end = cursor + newline;
        out.push(payload.slice(cursor..end));
        cursor = end + 1;
    }
}

#[cfg(test)]
mod test {
    use super::*;

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
            split_state_patches(&Bytes::copy_from_slice(input)).unwrap_err();
        }
    }

    #[test]
    fn extend_state_patches_cases() {
        let mut out = Vec::new();
        extend_state_patches(&mut out, b"");
        assert!(out.is_empty());

        extend_state_patches(&mut out, b"[{\"a\":1}\n]");
        assert_eq!(out.as_slice(), b"[{\"a\":1}\n]");

        extend_state_patches(&mut out, b"");
        assert_eq!(out.as_slice(), b"[{\"a\":1}\n]");

        extend_state_patches(&mut out, b"[{\"b\":2}\n,{\"c\":null}\n]");
        assert_eq!(out.as_slice(), b"[{\"a\":1}\n,{\"b\":2}\n,{\"c\":null}\n]");

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
}
