use anyhow::Context;
use bytes::{Buf, BufMut};

/// Version of the protocol which this crate speaks, as carried by `Header`.
pub const PROTOCOL_VERSION: u32 = 1;

/// Maximum payload length of a single frame.
/// Decoders reject a frame declaring a longer payload before buffering it.
pub const MAX_PAYLOAD: usize = 1 << 20;

// Tag byte, then a u32 big-endian payload length.
const PREFIX_LEN: usize = 5;

/// Frames sent by the client to the wrapper.
#[derive(Debug, Clone, PartialEq)]
pub enum Input {
    /// First frame of the stream, which describes the command to run.
    Header(Header),
    /// Bytes for the command's stdin.
    Stdin(bytes::Bytes),
    /// Close the command's stdin. A raw EOF of the input stream instead
    /// cancels the command.
    StdinEof,
}

/// Frames sent by the wrapper to the client.
#[derive(Debug, Clone, PartialEq)]
pub enum Output {
    Stdout(bytes::Bytes),
    Stderr(bytes::Bytes),
    /// The command exited. Last frame of a successful run.
    Exit {
        code: Option<i32>,
        signal: Option<i32>,
    },
    /// The wrapper could not run the command. Last frame of the stream.
    Error {
        message: String,
    },
}

#[derive(Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Header {
    pub version: u32,
    /// Name of the workspace directory, under the wrapper's workspace root,
    /// which is the command's working directory and HOME.
    pub workspace: String,
    /// Command line, run as `bash -lc <command>`.
    pub command: String,
    /// Environment variables added to the command's environment.
    /// Values may be secrets: Header's Debug implementation shows only keys.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub env: std::collections::BTreeMap<String, String>,
}

impl std::fmt::Debug for Header {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Header")
            .field("version", &self.version)
            .field("workspace", &self.workspace)
            .field("command", &self.command)
            .field("env_keys", &self.env.keys().collect::<Vec<_>>())
            .finish()
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct ExitJson {
    code: Option<i32>,
    signal: Option<i32>,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct ErrorJson {
    message: String,
}

impl Input {
    /// Append the encoded frame to `buf`.
    /// Fails only if the payload exceeds MAX_PAYLOAD.
    pub fn encode(&self, buf: &mut bytes::BytesMut) -> anyhow::Result<()> {
        match self {
            Input::Header(header) => {
                let payload = serde_json::to_vec(header).expect("Header serializes");
                put_frame(buf, 1, &payload)
            }
            Input::Stdin(data) => put_frame(buf, 2, data),
            Input::StdinEof => put_frame(buf, 3, &[]),
        }
    }

    /// Remove and return the next complete frame of `buf`,
    /// or None if `buf` doesn't yet hold one.
    pub fn decode(buf: &mut bytes::BytesMut) -> anyhow::Result<Option<Self>> {
        let Some((tag, payload)) = take_frame(buf)? else {
            return Ok(None);
        };
        let frame = match tag {
            1 => Input::Header(
                serde_json::from_slice(&payload).context("failed to decode Header frame")?,
            ),
            2 => Input::Stdin(payload),
            3 => {
                expect_empty(&payload, "StdinEof")?;
                Input::StdinEof
            }
            _ => anyhow::bail!("unknown input frame tag {tag}"),
        };
        Ok(Some(frame))
    }
}

impl Output {
    /// Append the encoded frame to `buf`.
    /// Fails only if the payload exceeds MAX_PAYLOAD.
    pub fn encode(&self, buf: &mut bytes::BytesMut) -> anyhow::Result<()> {
        match self {
            Output::Stdout(data) => put_frame(buf, 1, data),
            Output::Stderr(data) => put_frame(buf, 2, data),
            Output::Exit { code, signal } => {
                let payload = serde_json::to_vec(&ExitJson {
                    code: *code,
                    signal: *signal,
                })
                .expect("Exit serializes");
                put_frame(buf, 3, &payload)
            }
            Output::Error { message } => {
                let payload = serde_json::to_vec(&ErrorJson {
                    message: message.clone(),
                })
                .expect("Error serializes");
                put_frame(buf, 4, &payload)
            }
        }
    }

    /// Remove and return the next complete frame of `buf`,
    /// or None if `buf` doesn't yet hold one.
    pub fn decode(buf: &mut bytes::BytesMut) -> anyhow::Result<Option<Self>> {
        let Some((tag, payload)) = take_frame(buf)? else {
            return Ok(None);
        };
        let frame = match tag {
            1 => Output::Stdout(payload),
            2 => Output::Stderr(payload),
            3 => {
                let ExitJson { code, signal } =
                    serde_json::from_slice(&payload).context("failed to decode Exit frame")?;
                Output::Exit { code, signal }
            }
            4 => {
                let ErrorJson { message } =
                    serde_json::from_slice(&payload).context("failed to decode Error frame")?;
                Output::Error { message }
            }
            _ => anyhow::bail!("unknown output frame tag {tag}"),
        };
        Ok(Some(frame))
    }
}

fn put_frame(buf: &mut bytes::BytesMut, tag: u8, payload: &[u8]) -> anyhow::Result<()> {
    if payload.len() > MAX_PAYLOAD {
        anyhow::bail!(
            "frame payload of {} bytes exceeds the maximum of {MAX_PAYLOAD}",
            payload.len()
        );
    }
    buf.reserve(PREFIX_LEN + payload.len());
    buf.put_u8(tag);
    buf.put_u32(payload.len() as u32);
    buf.put_slice(payload);
    Ok(())
}

fn take_frame(buf: &mut bytes::BytesMut) -> anyhow::Result<Option<(u8, bytes::Bytes)>> {
    if buf.len() < PREFIX_LEN {
        return Ok(None);
    }
    let tag = buf[0];
    let len = u32::from_be_bytes(buf[1..PREFIX_LEN].try_into().unwrap()) as usize;

    if len > MAX_PAYLOAD {
        anyhow::bail!("frame payload of {len} bytes exceeds the maximum of {MAX_PAYLOAD}");
    }
    if buf.len() < PREFIX_LEN + len {
        // Size `buf` for the rest of the frame, so it's read without regrowth.
        buf.reserve(PREFIX_LEN + len - buf.len());
        return Ok(None);
    }
    buf.advance(PREFIX_LEN);
    Ok(Some((tag, buf.split_to(len).freeze())))
}

fn expect_empty(payload: &[u8], name: &str) -> anyhow::Result<()> {
    if !payload.is_empty() {
        anyhow::bail!("{name} frame has a non-empty payload");
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_round_trip() {
        let inputs = vec![
            Input::Header(Header {
                version: PROTOCOL_VERSION,
                workspace: "ws".to_string(),
                command: "echo hi".to_string(),
                env: [("KEY".to_string(), "value".to_string())].into(),
            }),
            Input::Stdin(bytes::Bytes::from_static(b"some input")),
            Input::Stdin(bytes::Bytes::new()),
            Input::StdinEof,
        ];
        let outputs = vec![
            Output::Stdout(bytes::Bytes::from_static(b"out")),
            Output::Stderr(bytes::Bytes::from_static(b"err")),
            Output::Exit {
                code: Some(3),
                signal: None,
            },
            Output::Exit {
                code: None,
                signal: Some(9),
            },
            Output::Error {
                message: "it broke".to_string(),
            },
        ];

        let mut buf = bytes::BytesMut::new();
        for frame in &inputs {
            frame.encode(&mut buf).unwrap();
        }
        let encoded = buf.clone();

        // Decode one byte at a time, so every partial-frame boundary is exercised.
        let mut decoded = Vec::new();
        let mut partial = bytes::BytesMut::new();
        for byte in encoded.iter() {
            partial.put_u8(*byte);
            while let Some(frame) = Input::decode(&mut partial).unwrap() {
                decoded.push(frame);
            }
        }
        assert!(partial.is_empty());
        assert_eq!(inputs, decoded);

        let mut buf = bytes::BytesMut::new();
        for frame in &outputs {
            frame.encode(&mut buf).unwrap();
        }
        let mut decoded = Vec::new();
        while let Some(frame) = Output::decode(&mut buf).unwrap() {
            decoded.push(frame);
        }
        assert!(buf.is_empty());
        assert_eq!(outputs, decoded);

        insta::assert_debug_snapshot!(encoded, @r#"b"\x01\0\0\0H{\"version\":1,\"workspace\":\"ws\",\"command\":\"echo hi\",\"env\":{\"KEY\":\"value\"}}\x02\0\0\0\nsome input\x02\0\0\0\0\x03\0\0\0\0""#);
    }

    #[test]
    fn test_header_without_env() {
        let mut buf = bytes::BytesMut::new();
        put_frame(
            &mut buf,
            1,
            br#"{"version":1,"workspace":"ws","command":"true"}"#,
        )
        .unwrap();
        insta::assert_debug_snapshot!(Input::decode(&mut buf).unwrap(), @r#"
        Some(
            Header(
                Header {
                    version: 1,
                    workspace: "ws",
                    command: "true",
                    env_keys: [],
                },
            ),
        )
        "#);
    }

    #[test]
    fn test_header_debug_omits_env_values() {
        let header = Header {
            version: 1,
            workspace: "ws".to_string(),
            command: "true".to_string(),
            env: [("API_TOKEN".to_string(), "sekret".to_string())].into(),
        };
        let debug = format!("{header:?}");
        assert!(debug.contains("API_TOKEN"));
        assert!(!debug.contains("sekret"));
    }

    #[test]
    fn test_decode_errors() {
        let decode = |raw: &[u8]| {
            let mut buf = bytes::BytesMut::from(raw);
            Input::decode(&mut buf).map_err(|err| format!("{err:#}"))
        };
        let oversize = (MAX_PAYLOAD as u32 + 1).to_be_bytes();

        insta::assert_debug_snapshot!([
            // Rejected from the length prefix alone, before the payload arrives.
            decode(&[2, oversize[0], oversize[1], oversize[2], oversize[3]]),
            decode(&[9, 0, 0, 0, 0]),
            decode(&[3, 0, 0, 0, 1, b'x']),
            decode(&[1, 0, 0, 0, 1, b'{']),
            // Incomplete frames are not (yet) errors.
            decode(&[2, 0, 0, 0, 3, b'x']),
            decode(&[2, 0, 0]),
        ], @r#"
        [
            Err(
                "frame payload of 1048577 bytes exceeds the maximum of 1048576",
            ),
            Err(
                "unknown input frame tag 9",
            ),
            Err(
                "StdinEof frame has a non-empty payload",
            ),
            Err(
                "failed to decode Header frame: EOF while parsing an object at line 1 column 1",
            ),
            Ok(
                None,
            ),
            Ok(
                None,
            ),
        ]
        "#);

        let mut buf = bytes::BytesMut::new();
        let err = Output::Stdout(vec![0; MAX_PAYLOAD + 1].into())
            .encode(&mut buf)
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "frame payload of 1048577 bytes exceeds the maximum of 1048576"
        );
        assert!(buf.is_empty());
    }
}
