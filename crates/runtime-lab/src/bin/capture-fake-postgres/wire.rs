//! The connector's output, written as source-boilerplate writes it: framed
//! protobuf through a 64 KiB buffer, flushed after every message other than
//! a document, into a stdout pipe enlarged to 1 MiB.

use prost::Message;
use proto_flow::capture::{Response, response};
use std::io::Write;

/// Buffer of stdout, as source-boilerplate's `bufio.Writer`.
const WRITE_BUFFER: usize = 64 * 1024;
/// Size of the stdout pipe, as source-boilerplate sets it. A database
/// connector uses the slack to overlap its next query's round trip with the
/// runtime's draining of the last; this one has no round trip.
const PIPE_SIZE: i32 = 1 << 20;

pub struct Writer {
    out: std::io::BufWriter<std::fs::File>,
    /// Document being built, which `captured` frames and writes.
    pub doc: Vec<u8>,
    header: Vec<u8>,
}

impl Writer {
    /// A writer of stdout, which this process owns from here on. Stdout is
    /// otherwise line-buffered, which would flush within documents.
    pub fn stdout() -> Self {
        use std::os::fd::FromRawFd;

        // Not every stdout is a pipe (as in tests), so failure is ignored.
        unsafe { libc::fcntl(1, libc::F_SETPIPE_SZ, PIPE_SIZE) };
        let file = unsafe { std::fs::File::from_raw_fd(1) };
        Self::new(file)
    }

    pub fn new(file: std::fs::File) -> Self {
        Self {
            out: std::io::BufWriter::with_capacity(WRITE_BUFFER, file),
            doc: Vec::with_capacity(4096),
            header: Vec::with_capacity(32),
        }
    }

    /// Write `self.doc` as a `Captured` of `binding`, and clear it. It's
    /// framed by hand, rather than through a `Response`, which would copy the
    /// document into its own allocation.
    pub fn captured(&mut self, binding: u32) -> std::io::Result<()> {
        use prost::encoding::{encode_varint, encoded_len_varint};

        let doc_len = self.doc.len() as u64;
        let binding_len = if binding == 0 {
            0
        } else {
            1 + encoded_len_varint(binding as u64)
        };
        let inner_len = binding_len as u64 + 1 + encoded_len_varint(doc_len) as u64 + doc_len;
        let message_len = 1 + encoded_len_varint(inner_len) as u64 + inner_len;

        let header = &mut self.header;
        header.clear();
        header.extend_from_slice(&(message_len as u32).to_le_bytes());
        header.push(0x32); // Response.captured: field 6, length-delimited.
        encode_varint(inner_len, header);
        if binding != 0 {
            header.push(0x08); // Captured.binding: field 1, varint.
            encode_varint(binding as u64, header);
        }
        header.push(0x12); // Captured.doc_json: field 2, length-delimited.
        encode_varint(doc_len, header);

        self.out.write_all(&self.header)?;
        self.out.write_all(&self.doc)?;
        self.doc.clear();
        Ok(())
    }

    /// Write a message other than a document, and flush.
    pub fn message(&mut self, kind: response::Kind) -> std::io::Result<()> {
        let response = Response {
            kind: Some(kind),
            ..Default::default()
        };
        let len = response.encoded_len();
        self.header.clear();
        self.header.extend_from_slice(&(len as u32).to_le_bytes());
        response.encode(&mut self.header).expect("Vec grows");
        self.out.write_all(&self.header)?;
        self.out.flush()
    }

    pub fn checkpoint(&mut self, patch: Option<serde_json::Value>) -> std::io::Result<()> {
        self.message(response::Kind::Checkpoint(response::Checkpoint {
            state: patch.map(|patch| proto_flow::flow::ConnectorState {
                updated_json: patch.to_string().into(),
                merge_patch: true,
            }),
        }))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn captured_frames_match_prost() {
        for binding in [0, 1, 300] {
            let tmp = tempfile::NamedTempFile::new().unwrap();
            let mut writer = Writer::new(tmp.reopen().unwrap());
            writer.doc.extend_from_slice(br#"{"id":1}"#);
            writer.captured(binding).unwrap();
            writer.out.flush().unwrap();
            let actual = std::fs::read(tmp.path()).unwrap();

            let mut expect = Vec::new();
            connector_init::Codec::Proto.encode(
                &Response {
                    kind: Some(response::Kind::Captured(response::Captured {
                        binding,
                        doc_json: br#"{"id":1}"#.to_vec().into(),
                    })),
                    ..Default::default()
                },
                &mut expect,
            );
            assert_eq!(actual, expect, "binding {binding}");
        }
    }
}
