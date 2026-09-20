//! Decompression of fragments written with `SNAPPY`. That codec is Snappy's framing
//! format, and not its raw block format.
//!
//! This is a port of golang/snappy's `Reader`, which Gazette's brokers decode
//! fragments with. It keeps that reader's two buffers, its `fill` loop, and its
//! errors. `snap` has a framing decoder, but only over a synchronous `Read`, so it
//! provides the block decompression here and not the framing. `async-compression`,
//! which decodes the gzip and zstd fragments beside this module, has no snappy
//! backend.
//!
//! It departs from Go in two respects, each to skip a copy: decoding is bound by
//! memory bandwidth, so copies are a large share of its cost. Where the caller's
//! buffer holds a whole block, a chunk is decoded straight into it rather than
//! staged through `decoded`. And it reads an `AsyncBufRead`, as the gzip and zstd
//! decoders do, so a compressed chunk which the input already holds whole is
//! decompressed from the input's buffer rather than copied into `chunk`.

use futures::io::{AsyncBufRead, AsyncRead};
use std::ops::Range;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

const CHECKSUM_SIZE: usize = 4;
const CHUNK_HEADER_SIZE: usize = 4;
const MAGIC_BODY: &[u8] = b"sNaPpY";

/// Largest block a chunk decompresses to.
const MAX_BLOCK_SIZE: usize = 65536;
/// Largest encoding of a `MAX_BLOCK_SIZE` block, which Go hard-codes as well.
const MAX_ENCODED_LEN_OF_MAX_BLOCK_SIZE: usize = 76490;
/// Largest chunk body: a whole block encoded, and its checksum.
const MAX_CHUNK_LEN: usize = MAX_ENCODED_LEN_OF_MAX_BLOCK_SIZE + CHECKSUM_SIZE;

const CHUNK_TYPE_COMPRESSED_DATA: u8 = 0x00;
const CHUNK_TYPE_UNCOMPRESSED_DATA: u8 = 0x01;
const CHUNK_TYPE_STREAM_IDENTIFIER: u8 = 0xff;

/// Decompress `inner`, which carries one Snappy framed stream.
pub fn decode<R: AsyncBufRead>(inner: R) -> impl AsyncRead {
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

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("snappy: corrupt input")]
    Corrupt,
    #[error("snappy: unsupported input")]
    Unsupported,
}

impl From<Error> for std::io::Error {
    fn from(err: Error) -> Self {
        std::io::Error::new(std::io::ErrorKind::InvalidData, err)
    }
}

pin_project_lite::pin_project! {
    struct Reader<R> {
        #[pin]
        inner: R,
        state: State,
    }
}

struct State {
    decoder: snap::raw::Decoder,
    /// Where chunk headers and bodies are read, except a compressed body which
    /// `inner`'s buffer holds whole.
    chunk: Box<[u8]>,
    /// Decoded bytes, of which `pending` have not yet been read.
    decoded: Box<[u8]>,
    pending: Range<usize>,
    step: Step,
    /// Bytes read so far by the `read_full` of `step`.
    filled: usize,
}

/// The read which decoding is waiting on.
#[derive(Clone, Copy)]
enum Step {
    /// The first chunk header, which must introduce a stream identifier.
    StreamStart,
    Header,
    CompressedData {
        chunk_len: usize,
    },
    UncompressedChecksum {
        n: usize,
    },
    UncompressedData {
        n: usize,
        checksum: u32,
    },
    StreamIdentifier,
    Skip {
        chunk_len: usize,
    },
}

enum Filled {
    /// The stream ended on a chunk boundary.
    End,
    /// `decoded[pending]` holds bytes.
    Decoded,
    /// This many bytes were decoded into the caller's buffer.
    Direct(usize),
}

impl<R: AsyncBufRead> AsyncRead for Reader<R> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        out: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.project();
        let state = this.state;

        match ready!(state.poll_fill(cx, this.inner, out))? {
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
    fn poll_fill<R: AsyncBufRead>(
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
                        let available = ready!(inner.as_mut().poll_fill_buf(cx))?;
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
                            inner.as_mut().consume(chunk_len);
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
                        Err(Error::Corrupt)?;
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
) -> Result<Decompressed, Error> {
    let (checksum, block) = body.split_at(CHECKSUM_SIZE);
    let checksum = u32::from_le_bytes(checksum.try_into().unwrap());

    let n = snap::raw::decompress_len(block).map_err(|_| Error::Corrupt)?;
    if n > MAX_BLOCK_SIZE {
        return Err(Error::Corrupt);
    }
    // An empty block decodes to nothing, which `out` would report as the end of the
    // stream. It goes through `decoded`, which moves past it.
    let direct = n != 0 && n <= out.len();
    let dst = if direct {
        &mut out[..n]
    } else {
        &mut decoded[..n]
    };

    decoder.decompress(block, dst).map_err(|_| Error::Corrupt)?;
    verify(dst, checksum)?;

    Ok(if direct {
        Decompressed::Direct(n)
    } else {
        Decompressed::Staged(n)
    })
}

/// The step which reads the chunk that `header` introduces. The chunk types are
/// specified at https://github.com/google/snappy/blob/master/framing_format.txt
fn next_step(header: [u8; CHUNK_HEADER_SIZE], first: bool) -> Result<Step, Error> {
    let [chunk_type, len @ ..] = header;
    let chunk_len = u32::from_le_bytes([len[0], len[1], len[2], 0]) as usize;

    if first && chunk_type != CHUNK_TYPE_STREAM_IDENTIFIER {
        return Err(Error::Corrupt);
    }
    if chunk_len > MAX_CHUNK_LEN {
        return Err(Error::Unsupported);
    }
    match chunk_type {
        CHUNK_TYPE_COMPRESSED_DATA if chunk_len < CHECKSUM_SIZE => Err(Error::Corrupt),
        CHUNK_TYPE_COMPRESSED_DATA => Ok(Step::CompressedData { chunk_len }),
        CHUNK_TYPE_UNCOMPRESSED_DATA
            if !(CHECKSUM_SIZE..=CHECKSUM_SIZE + MAX_BLOCK_SIZE).contains(&chunk_len) =>
        {
            Err(Error::Corrupt)
        }
        CHUNK_TYPE_UNCOMPRESSED_DATA => Ok(Step::UncompressedChecksum {
            n: chunk_len - CHECKSUM_SIZE,
        }),
        CHUNK_TYPE_STREAM_IDENTIFIER if chunk_len != MAGIC_BODY.len() => Err(Error::Corrupt),
        CHUNK_TYPE_STREAM_IDENTIFIER => Ok(Step::StreamIdentifier),
        // Reserved unskippable chunks (chunk types 0x02-0x7f).
        0x02..=0x7f => Err(Error::Unsupported),
        // Padding (chunk type 0xfe), and reserved skippable chunks (chunk types
        // 0x80-0xfd).
        _ => Ok(Step::Skip { chunk_len }),
    }
}

/// Fill `buf` from `inner`, resuming from `filled` bytes, as Go's `readFull`. An end
/// of stream before `buf` is full is corrupt input, unless `allow_eof` and nothing of
/// `buf` was read, which reports `false`.
fn read_full<R: AsyncRead>(
    cx: &mut Context<'_>,
    mut inner: Pin<&mut R>,
    buf: &mut [u8],
    filled: &mut usize,
    allow_eof: bool,
) -> Poll<std::io::Result<bool>> {
    while *filled != buf.len() {
        match ready!(inner.as_mut().poll_read(cx, &mut buf[*filled..]))? {
            0 if *filled == 0 && allow_eof => return Poll::Ready(Ok(false)),
            0 => return Poll::Ready(Err(Error::Corrupt.into())),
            n => *filled += n,
        }
    }
    *filled = 0;
    Poll::Ready(Ok(true))
}

fn verify(data: &[u8], checksum: u32) -> Result<(), Error> {
    if crc(data) == checksum {
        Ok(())
    } else {
        Err(Error::Corrupt)
    }
}

/// The checksum specified in section 3 of
/// https://github.com/google/snappy/blob/master/framing_format.txt
fn crc(data: &[u8]) -> u32 {
    crc32c::crc32c(data)
        .rotate_right(15)
        .wrapping_add(0xa282ead8)
}

#[cfg(test)]
mod test {
    use super::decode;

    /// Compress `content` as `SNAPPY` fragments are written, in blocks of
    /// `MAX_BLOCK_SIZE`.
    fn framed(content: &[u8]) -> Vec<u8> {
        let mut encoder = snap::write::FrameEncoder::new(Vec::new());
        std::io::Write::write_all(&mut encoder, content).unwrap();

        encoder.into_inner().unwrap()
    }

    /// Content which compresses, in the repeated run, and content which does not, in
    /// the pseudo-random tail. Both chunk types then occur.
    fn content(len: usize) -> Vec<u8> {
        (0..len)
            .map(|index| match index % 3 {
                0 => 0xab,
                _ => (index.wrapping_mul(2654435761) >> 13) as u8,
            })
            .collect()
    }

    async fn decoded(compressed: &[u8]) -> std::io::Result<Vec<u8>> {
        decoded_from(futures::io::Cursor::new(compressed.to_vec())).await
    }

    async fn decoded_from(
        reader: impl futures::io::AsyncBufRead + Unpin,
    ) -> std::io::Result<Vec<u8>> {
        let mut out = Vec::new();
        let reader = decode(reader);
        futures::pin_mut!(reader);

        futures::AsyncReadExt::read_to_end(&mut reader, &mut out).await?;
        Ok(out)
    }

    #[test]
    fn test_the_hard_coded_encoded_length_is_snappys() {
        assert_eq!(
            super::MAX_ENCODED_LEN_OF_MAX_BLOCK_SIZE,
            snap::raw::max_compress_len(super::MAX_BLOCK_SIZE)
        );
    }

    #[tokio::test]
    async fn test_round_trip_across_chunk_boundaries() {
        // These are empty, part of a block, exactly a block, and several blocks
        // with a partial one at the end.
        for len in [
            0,
            1,
            100,
            super::MAX_BLOCK_SIZE,
            5 * super::MAX_BLOCK_SIZE + 7,
        ] {
            let expect = content(len);
            assert_eq!(decoded(&framed(&expect)).await.unwrap(), expect, "{len}");
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
            std::pin::Pin::new(&mut self.r).poll_read(cx, buf)
        }
    }

    #[tokio::test]
    async fn test_a_reader_which_returns_one_byte_then_pending() {
        let expect = content(3 * super::MAX_BLOCK_SIZE + 7);
        let compressed = framed(&expect);

        let bytes = compressed
            .into_iter()
            .map(|byte| Ok::<_, std::io::Error>(bytes::Bytes::from(vec![byte])));
        let reader = futures::io::BufReader::new(PendingBetween {
            r: futures::TryStreamExt::into_async_read(futures::stream::iter(bytes)),
            ready: false,
        });

        assert_eq!(decoded_from(reader).await.unwrap(), expect);
    }

    #[tokio::test]
    async fn test_empty_chunks_are_not_the_end_of_the_stream() {
        let expect = content(100);
        let compressed = framed(&expect);
        let empty = super::crc(b"").to_le_bytes();

        // After the stream identifier: an empty uncompressed chunk, and a compressed
        // chunk of an empty block.
        let mut spliced = compressed[..10].to_vec();
        spliced.extend_from_slice(&[0x01, 0x04, 0x00, 0x00]);
        spliced.extend_from_slice(&empty);
        spliced.extend_from_slice(&[0x00, 0x05, 0x00, 0x00]);
        spliced.extend_from_slice(&empty);
        spliced.push(0x00);
        spliced.extend_from_slice(&compressed[10..]);

        assert_eq!(decoded(&spliced).await.unwrap(), expect);
    }

    #[tokio::test]
    async fn test_padding_and_skippable_chunks_are_skipped() {
        let expect = content(super::MAX_BLOCK_SIZE + 7);
        let compressed = framed(&expect);

        // After the ten-byte stream identifier: padding, then a reserved skippable.
        let mut spliced = compressed[..10].to_vec();
        spliced.extend_from_slice(&[0xfe, 0x03, 0x00, 0x00, 0, 0, 0]);
        spliced.extend_from_slice(&[0x80, 0x02, 0x00, 0x00, 0xaa, 0xbb]);
        spliced.extend_from_slice(&compressed[10..]);

        assert_eq!(decoded(&spliced).await.unwrap(), expect);
    }

    #[tokio::test]
    async fn test_a_truncated_stream_is_corrupt() {
        let compressed = framed(&content(3 * super::MAX_BLOCK_SIZE));

        for len in [1, super::CHUNK_HEADER_SIZE, compressed.len() - 1] {
            let err = decoded(&compressed[..len]).await.unwrap_err();
            assert_eq!(err.to_string(), "snappy: corrupt input", "{len}");
        }
    }

    #[tokio::test]
    async fn test_a_stream_which_is_not_snappy_is_corrupt() {
        let err = decoded(b"not a snappy stream at all").await.unwrap_err();
        assert_eq!(err.to_string(), "snappy: corrupt input");

        // A well-formed uncompressed chunk. It is still not the stream identifier
        // every stream must begin with.
        let err = decoded(&[0x01, 0x06, 0x00, 0x00, 0, 0, 0, 0, b'h', b'i'])
            .await
            .unwrap_err();
        assert_eq!(err.to_string(), "snappy: corrupt input");
    }

    #[tokio::test]
    async fn test_a_corrupted_block_fails_its_checksum() {
        let mut compressed = framed(&content(1000));
        *compressed.last_mut().unwrap() ^= 0xff;

        let err = decoded(&compressed).await.unwrap_err();
        assert_eq!(err.to_string(), "snappy: corrupt input");
    }

    #[tokio::test]
    async fn test_reserved_chunks_and_oversized_chunks_are_unsupported() {
        let identifier = &framed(b"x")[..10];

        // A reserved unskippable chunk type.
        let mut stream = identifier.to_vec();
        stream.extend_from_slice(&[0x02, 0x00, 0x00, 0x00]);
        let err = decoded(&stream).await.unwrap_err();
        assert_eq!(err.to_string(), "snappy: unsupported input");

        // A chunk one byte longer than the largest a block encodes to.
        let len = (super::MAX_ENCODED_LEN_OF_MAX_BLOCK_SIZE + super::CHECKSUM_SIZE + 1) as u32;
        let mut stream = identifier.to_vec();
        stream.push(0x00);
        stream.extend_from_slice(&len.to_le_bytes()[..3]);
        let err = decoded(&stream).await.unwrap_err();
        assert_eq!(err.to_string(), "snappy: unsupported input");
    }
}
