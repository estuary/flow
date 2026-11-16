//! Async decoding of Gazette's framed Snappy fragments.
//!
//! `snap` supplies raw block decompression; its framing decoder requires
//! synchronous `Read`, and `async-compression` has no Snappy backend.

use std::ops::Range;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

const CHECKSUM_SIZE: usize = 4;
const CHUNK_HEADER_SIZE: usize = 4;
const MAGIC_BODY: &[u8] = b"sNaPpY";

/// Largest block a chunk decompresses to.
const MAX_BLOCK_SIZE: usize = 65536;
/// Largest encoding of a `MAX_BLOCK_SIZE` block.
const MAX_ENCODED_LEN_OF_MAX_BLOCK_SIZE: usize = 76490;
/// Largest chunk body: a whole block encoded, and its checksum.
const MAX_CHUNK_LEN: usize = MAX_ENCODED_LEN_OF_MAX_BLOCK_SIZE + CHECKSUM_SIZE;

const CHUNK_TYPE_COMPRESSED_DATA: u8 = 0x00;
const CHUNK_TYPE_UNCOMPRESSED_DATA: u8 = 0x01;
const CHUNK_TYPE_STREAM_IDENTIFIER: u8 = 0xff;

/// Decompress `inner`, which carries one Snappy framed stream.
pub fn decode<R: futures::io::AsyncBufRead + Unpin>(inner: R) -> impl futures::io::AsyncRead {
    Reader {
        inner,
        state: State {
            decoder: snap::raw::Decoder::new(),
            chunk: vec![0; MAX_CHUNK_LEN].into_boxed_slice(),
            decoded: vec![0; MAX_BLOCK_SIZE].into_boxed_slice(),
            pending: 0..0,
            step: Step::StreamStart,
            filled: 0,
        },
    }
}

fn corrupt() -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, "snappy: corrupt input")
}

fn unsupported() -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, "snappy: unsupported input")
}

struct Reader<R> {
    inner: R,
    state: State,
}

struct State {
    decoder: snap::raw::Decoder,
    chunk: Box<[u8]>,
    decoded: Box<[u8]>,
    pending: Range<usize>,
    step: Step,
    filled: usize,
}

/// The read which decoding is waiting on.
#[derive(Clone, Copy)]
enum Step {
    StreamStart,
    Header,
    CompressedData { chunk_len: usize },
    UncompressedChecksum { n: usize },
    UncompressedData { n: usize, checksum: u32 },
    StreamIdentifier,
    Skip { chunk_len: usize },
}

enum Filled {
    End,
    Decoded,
    Direct(usize),
}

// `read_fragment_url_body` consumes a fragment as an `AsyncRead`, as it does the
// Gzip and Zstd decoders.
impl<R: futures::io::AsyncBufRead + Unpin> futures::io::AsyncRead for Reader<R> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        out: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        let Reader { inner, state } = self.get_mut();

        match ready!(state.poll_fill(cx, Pin::new(inner), out))? {
            Filled::End => Poll::Ready(Ok(0)),
            Filled::Direct(n) => Poll::Ready(Ok(n)),
            Filled::Decoded => {
                let n = out.len().min(state.pending.len());
                out[..n].copy_from_slice(&state.decoded[state.pending.start..][..n]);
                state.pending.start += n;

                Poll::Ready(Ok(n))
            }
        }
    }
}

impl State {
    /// Decode chunks until one has been decoded into either `out` or `decoded`, or the
    /// stream ends on a chunk boundary.
    fn poll_fill<R: futures::io::AsyncBufRead>(
        &mut self,
        cx: &mut Context<'_>,
        mut inner: Pin<&mut R>,
        out: &mut [u8],
    ) -> Poll<std::io::Result<Filled>> {
        while self.pending.is_empty() {
            match self.step {
                Step::StreamStart | Step::Header => {
                    let header = &mut self.chunk[..CHUNK_HEADER_SIZE];
                    if !ready!(read_full(
                        cx,
                        inner.as_mut(),
                        header,
                        &mut self.filled,
                        true
                    ))? {
                        return Poll::Ready(Ok(Filled::End));
                    }
                    let header = self.chunk[..CHUNK_HEADER_SIZE].try_into().unwrap();
                    self.step = next_step(header, matches!(self.step, Step::StreamStart))?;
                }
                Step::CompressedData { chunk_len } => {
                    // A chunk which straddles `inner`'s buffers is copied into `chunk`.
                    let mut buffered = None;
                    if self.filled == 0 {
                        let available =
                            ready!(futures::io::AsyncBufRead::poll_fill_buf(inner.as_mut(), cx))?;
                        if available.len() >= chunk_len {
                            buffered = Some(decompress(
                                &mut self.decoder,
                                &available[..chunk_len],
                                out,
                                &mut self.decoded,
                            )?);
                        }
                    }
                    let decompressed = match buffered {
                        Some(decompressed) => {
                            futures::io::AsyncBufRead::consume(inner.as_mut(), chunk_len);
                            decompressed
                        }
                        None => {
                            let chunk = &mut self.chunk[..chunk_len];
                            ready!(read_full(
                                cx,
                                inner.as_mut(),
                                chunk,
                                &mut self.filled,
                                false
                            ))?;
                            decompress(
                                &mut self.decoder,
                                &self.chunk[..chunk_len],
                                out,
                                &mut self.decoded,
                            )?
                        }
                    };
                    self.step = Step::Header;

                    match decompressed {
                        Decompressed::Direct(n) => return Poll::Ready(Ok(Filled::Direct(n))),
                        Decompressed::Staged(n) => self.pending = 0..n,
                    }
                }
                Step::UncompressedChecksum { n } => {
                    let checksum = &mut self.chunk[..CHECKSUM_SIZE];
                    ready!(read_full(
                        cx,
                        inner.as_mut(),
                        checksum,
                        &mut self.filled,
                        false
                    ))?;

                    let checksum =
                        u32::from_le_bytes(self.chunk[..CHECKSUM_SIZE].try_into().unwrap());
                    // The data needs no decompressing, so it skips `chunk`.
                    self.step = Step::UncompressedData { n, checksum };
                }
                Step::UncompressedData { n, checksum } => {
                    // Bytes read into `out` are lost if this returns pending, because
                    // the next poll may pass another buffer. So what was read moves
                    // to `decoded`, and the read resumes there.
                    if self.filled == 0 && n != 0 && n <= out.len() {
                        let mut filled = 0;
                        match read_full(cx, inner.as_mut(), &mut out[..n], &mut filled, false) {
                            Poll::Ready(result) => {
                                result?;
                                verify(&out[..n], checksum)?;
                                self.step = Step::Header;
                                return Poll::Ready(Ok(Filled::Direct(n)));
                            }
                            Poll::Pending => {
                                self.decoded[..filled].copy_from_slice(&out[..filled]);
                                self.filled = filled;
                                return Poll::Pending;
                            }
                        }
                    }
                    let decoded = &mut self.decoded[..n];
                    ready!(read_full(
                        cx,
                        inner.as_mut(),
                        decoded,
                        &mut self.filled,
                        false
                    ))?;

                    verify(&self.decoded[..n], checksum)?;
                    self.step = Step::Header;
                    self.pending = 0..n;
                }
                Step::StreamIdentifier => {
                    let body = &mut self.chunk[..MAGIC_BODY.len()];
                    ready!(read_full(cx, inner.as_mut(), body, &mut self.filled, false))?;

                    if &self.chunk[..MAGIC_BODY.len()] != MAGIC_BODY {
                        Err(corrupt())?;
                    }
                    self.step = Step::Header;
                }
                Step::Skip { chunk_len } => {
                    let body = &mut self.chunk[..chunk_len];
                    ready!(read_full(cx, inner.as_mut(), body, &mut self.filled, false))?;

                    self.step = Step::Header;
                }
            }
        }
        Poll::Ready(Ok(Filled::Decoded))
    }
}

enum Decompressed {
    /// Into the caller's buffer.
    Direct(usize),
    /// Into `decoded`.
    Staged(usize),
}

/// Decompress a compressed chunk's `body`: into `out` where it holds the whole block,
/// and otherwise into `decoded`.
fn decompress(
    decoder: &mut snap::raw::Decoder,
    body: &[u8],
    out: &mut [u8],
    decoded: &mut [u8],
) -> std::io::Result<Decompressed> {
    let (checksum, block) = body.split_at(CHECKSUM_SIZE);
    let checksum = u32::from_le_bytes(checksum.try_into().unwrap());

    let n = snap::raw::decompress_len(block).map_err(|_| corrupt())?;
    if n > MAX_BLOCK_SIZE {
        return Err(corrupt());
    }
    // An empty block decodes to nothing, which `out` would report as the end of the
    // stream. It goes through `decoded`, which moves past it.
    let direct = n != 0 && n <= out.len();
    let dst = if direct {
        &mut out[..n]
    } else {
        &mut decoded[..n]
    };

    decoder.decompress(block, dst).map_err(|_| corrupt())?;
    verify(dst, checksum)?;

    Ok(if direct {
        Decompressed::Direct(n)
    } else {
        Decompressed::Staged(n)
    })
}

/// The step which reads the chunk that `header` introduces. The chunk types are
/// specified at https://github.com/google/snappy/blob/master/framing_format.txt
fn next_step(header: [u8; CHUNK_HEADER_SIZE], first: bool) -> std::io::Result<Step> {
    let [chunk_type, len @ ..] = header;
    let chunk_len = u32::from_le_bytes([len[0], len[1], len[2], 0]) as usize;

    if first && chunk_type != CHUNK_TYPE_STREAM_IDENTIFIER {
        return Err(corrupt());
    }
    if chunk_len > MAX_CHUNK_LEN {
        return Err(unsupported());
    }
    match chunk_type {
        CHUNK_TYPE_COMPRESSED_DATA if chunk_len < CHECKSUM_SIZE => Err(corrupt()),
        CHUNK_TYPE_COMPRESSED_DATA => Ok(Step::CompressedData { chunk_len }),
        CHUNK_TYPE_UNCOMPRESSED_DATA
            if !(CHECKSUM_SIZE..=CHECKSUM_SIZE + MAX_BLOCK_SIZE).contains(&chunk_len) =>
        {
            Err(corrupt())
        }
        CHUNK_TYPE_UNCOMPRESSED_DATA => Ok(Step::UncompressedChecksum {
            n: chunk_len - CHECKSUM_SIZE,
        }),
        CHUNK_TYPE_STREAM_IDENTIFIER if chunk_len != MAGIC_BODY.len() => Err(corrupt()),
        CHUNK_TYPE_STREAM_IDENTIFIER => Ok(Step::StreamIdentifier),
        // Reserved unskippable chunks (chunk types 0x02-0x7f).
        0x02..=0x7f => Err(unsupported()),
        // Padding (chunk type 0xfe), and reserved skippable chunks (chunk types
        // 0x80-0xfd).
        _ => Ok(Step::Skip { chunk_len }),
    }
}

/// Fill `buf` from `inner`, resuming from `filled` bytes. An end of stream before
/// `buf` is full is corrupt input, unless `allow_eof` and nothing of `buf` was read,
/// which reports `false`.
fn read_full<R: futures::io::AsyncRead>(
    cx: &mut Context<'_>,
    mut inner: Pin<&mut R>,
    buf: &mut [u8],
    filled: &mut usize,
    allow_eof: bool,
) -> Poll<std::io::Result<bool>> {
    while *filled != buf.len() {
        match ready!(futures::io::AsyncRead::poll_read(
            inner.as_mut(),
            cx,
            &mut buf[*filled..]
        ))? {
            0 if *filled == 0 && allow_eof => return Poll::Ready(Ok(false)),
            0 => return Poll::Ready(Err(corrupt())),
            n => *filled += n,
        }
    }
    *filled = 0;
    Poll::Ready(Ok(true))
}

fn verify(data: &[u8], checksum: u32) -> std::io::Result<()> {
    if crc(data) == checksum {
        Ok(())
    } else {
        Err(corrupt())
    }
}

/// The checksum specified in section 3 of
/// https://github.com/google/snappy/blob/master/framing_format.txt
fn crc(data: &[u8]) -> u32 {
    (crc_fast::checksum(crc_fast::CrcAlgorithm::Crc32Iscsi, data) as u32)
        .rotate_right(15)
        .wrapping_add(0xa282ead8)
}

#[cfg(test)]
mod test {
    use super::{CHUNK_HEADER_SIZE, MAX_BLOCK_SIZE, MAX_CHUNK_LEN, crc, decode};

    #[test]
    fn test_the_hard_coded_encoded_length_is_snappys() {
        assert_eq!(
            super::MAX_ENCODED_LEN_OF_MAX_BLOCK_SIZE,
            snap::raw::max_compress_len(MAX_BLOCK_SIZE)
        );
    }

    #[tokio::test]
    async fn test_decode() {
        let identifier = [[0xff, 6, 0, 0].as_slice(), b"sNaPpY"].concat();
        let empty_checksum = crc(b"").to_le_bytes();
        let small = content(100);
        // A compressed block, an uncompressed one, and a partial block.
        let blocks = content(2 * MAX_BLOCK_SIZE + 7);

        // `stream`, with `chunks` after its stream identifier.
        let spliced = |stream: &[u8], chunks: &[&[u8]]| {
            [
                &stream[..identifier.len()],
                &chunks.concat(),
                &stream[identifier.len()..],
            ]
            .concat()
        };
        // `stream`, with the checksum of its data chunk at `index` corrupted.
        let corrupted = |mut stream: Vec<u8>, index: usize| {
            let mut at = identifier.len();
            for _ in 0..index {
                let len = u32::from_le_bytes([stream[at + 1], stream[at + 2], stream[at + 3], 0]);
                at += CHUNK_HEADER_SIZE + len as usize;
            }
            stream[at + CHUNK_HEADER_SIZE] ^= 0xff;
            stream
        };
        let oversized = (MAX_CHUNK_LEN as u32 + 1).to_le_bytes();

        // (name, stream, what it decodes to, or the kind of input decoding refuses)
        let cases = [
            ("empty", framed(b""), Ok(Vec::new())),
            ("a byte", framed(&content(1)), Ok(content(1))),
            (
                "a block",
                framed(&content(MAX_BLOCK_SIZE)),
                Ok(content(MAX_BLOCK_SIZE)),
            ),
            ("blocks", framed(&blocks), Ok(blocks.clone())),
            // A stream identifier may recur, where streams were concatenated.
            (
                "concatenated",
                [framed(&small), framed(&small)].concat(),
                Ok([small.clone(), small.clone()].concat()),
            ),
            // An empty uncompressed chunk, and a compressed chunk of an empty block.
            // Neither is the end of the stream.
            (
                "empty chunks",
                spliced(
                    &framed(&small),
                    &[
                        &[0x01, 4, 0, 0],
                        &empty_checksum,
                        &[0x00, 5, 0, 0],
                        &empty_checksum,
                        &[0x00],
                    ],
                ),
                Ok(small.clone()),
            ),
            // Padding, then a reserved skippable chunk.
            (
                "skippable chunks",
                spliced(
                    &framed(&small),
                    &[&[0xfe, 3, 0, 0, 0, 0, 0], &[0x80, 2, 0, 0, 0xaa, 0xbb]],
                ),
                Ok(small.clone()),
            ),
            (
                "truncated header",
                framed(&small)[..1].to_vec(),
                Err("corrupt"),
            ),
            (
                "truncated identifier",
                framed(&small)[..CHUNK_HEADER_SIZE].to_vec(),
                Err("corrupt"),
            ),
            (
                "truncated chunk",
                framed(&blocks)[..framed(&blocks).len() - 1].to_vec(),
                Err("corrupt"),
            ),
            (
                "not snappy",
                b"not a snappy stream at all".to_vec(),
                Err("corrupt"),
            ),
            // A well-formed uncompressed chunk, which is not the stream identifier a
            // stream must begin with.
            (
                "no identifier",
                vec![0x01, 6, 0, 0, 0, 0, 0, 0, b'h', b'i'],
                Err("corrupt"),
            ),
            (
                "wrong identifier",
                [[0xff, 6, 0, 0].as_slice(), b"sNaPpX"].concat(),
                Err("corrupt"),
            ),
            (
                "compressed checksum",
                corrupted(framed(&blocks), 0),
                Err("corrupt"),
            ),
            (
                "uncompressed checksum",
                corrupted(framed(&blocks), 1),
                Err("corrupt"),
            ),
            // A compressed chunk shorter than its checksum.
            (
                "short chunk",
                [identifier.as_slice(), &[0x00, 3, 0, 0, 0, 0, 0]].concat(),
                Err("corrupt"),
            ),
            (
                "reserved chunk",
                [identifier.as_slice(), &[0x02, 0, 0, 0]].concat(),
                Err("unsupported"),
            ),
            // One byte longer than the largest a block encodes to.
            (
                "oversized chunk",
                [identifier.as_slice(), &[0x00], &oversized[..3]].concat(),
                Err("unsupported"),
            ),
        ];

        for (name, stream, expect) in cases {
            let expect = expect.map_err(|kind| format!("snappy: {kind} input"));

            // Reads of a byte stage every block in `decoded`, and reads of a block take
            // each straight into the caller's buffer.
            for size in [1, MAX_BLOCK_SIZE] {
                let whole = futures::io::Cursor::new(stream.clone());

                // Pending before each byte, as a reader awaiting the network is, so that
                // every read resumes partway.
                let bytes = stream
                    .iter()
                    .map(|byte| Ok::<_, std::io::Error>(bytes::Bytes::from(vec![*byte])));
                let pending = futures::io::BufReader::new(PendingBetween {
                    r: futures::TryStreamExt::into_async_read(futures::stream::iter(bytes)),
                    ready: false,
                });

                for (reader, outcome) in [
                    ("whole", read_all(whole, size).await),
                    ("a byte then pending", read_all(pending, size).await),
                ] {
                    let outcome = outcome.map_err(|err| err.to_string());
                    assert!(
                        outcome == expect,
                        "{name}, {reader}, reads of {size}: {:?}",
                        outcome.map(|decoded| decoded.len())
                    );
                }
            }
        }
    }

    /// Compress `content` as `SNAPPY` fragments are written, in blocks of
    /// `MAX_BLOCK_SIZE`.
    fn framed(content: &[u8]) -> Vec<u8> {
        let mut encoder = snap::write::FrameEncoder::new(Vec::new());
        std::io::Write::write_all(&mut encoder, content).unwrap();

        encoder.into_inner().unwrap()
    }

    /// `len` bytes, whose blocks alternate between a repeated pattern, which compresses,
    /// and pseudo-random bytes, which do not. Both chunk types then occur.
    fn content(len: usize) -> Vec<u8> {
        let mut state: u64 = 0x9e37_79b9_7f4a_7c15;

        (0..len)
            .map(|index| {
                if (index / MAX_BLOCK_SIZE).is_multiple_of(2) {
                    return (index % 7) as u8;
                }
                // Xorshift.
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state as u8
            })
            .collect()
    }

    /// Decode `reader` in reads of up to `size` bytes, as a caller with a buffer of
    /// that size does.
    async fn read_all(
        reader: impl futures::io::AsyncBufRead + Unpin,
        size: usize,
    ) -> std::io::Result<Vec<u8>> {
        let reader = decode(reader);
        futures::pin_mut!(reader);
        let (mut decoded, mut buf) = (Vec::new(), vec![0; size]);

        loop {
            match futures::AsyncReadExt::read(&mut reader, &mut buf).await? {
                0 => return Ok(decoded),
                n => decoded.extend_from_slice(&buf[..n]),
            }
        }
    }

    /// Pending before each read it passes on, as a reader awaiting the network is.
    struct PendingBetween<R> {
        r: R,
        ready: bool,
    }

    impl<R: futures::io::AsyncRead + Unpin> futures::io::AsyncRead for PendingBetween<R> {
        fn poll_read(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &mut [u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            if !std::mem::replace(&mut self.ready, false) {
                self.ready = true;
                cx.waker().wake_by_ref();
                return std::task::Poll::Pending;
            }
            futures::io::AsyncRead::poll_read(std::pin::Pin::new(&mut self.r), cx, buf)
        }
    }
}
