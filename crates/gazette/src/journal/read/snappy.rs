//! Decompression of fragments written with `SNAPPY`. That codec is Snappy's framing
//! format, and not its raw block format.
//!
//! `snap` decodes that format from a `std::io::Read`, but a fragment arrives
//! asynchronously. This therefore reads each chunk's four-byte header to learn how
//! much to await, then hands the whole chunk to a decoder. The decoder only ever
//! sees a complete chunk. It is also long-lived, so it validates the stream
//! identifier and the chunk ordering across the whole fragment.

/// Header of every chunk: a type byte and a 24-bit little-endian body length.
const HEADER_LEN: usize = 4;

/// Largest block the framing format permits a chunk to decompress to.
const MAX_BLOCK_LEN: usize = 65536;

/// Decompress `reader`, which carries one Snappy framed stream.
pub fn decode<R>(reader: R) -> impl futures::io::AsyncRead
where
    R: futures::io::AsyncRead,
{
    let chunks = coroutines::try_coroutine(move |mut co| async move {
        futures::pin_mut!(reader);

        let mut decoder = snap::read::FrameDecoder::new(std::io::Cursor::new(Vec::new()));
        let mut block = vec![0u8; MAX_BLOCK_LEN];

        loop {
            let chunk = decoder.get_mut().get_mut();
            chunk.clear();
            chunk.resize(HEADER_LEN, 0);

            match read_full(&mut reader, chunk).await? {
                0 => return Ok(()),
                HEADER_LEN => (),
                len => return Err(truncated(len, HEADER_LEN)),
            }
            let body = u32::from_le_bytes([chunk[1], chunk[2], chunk[3], 0]) as usize;

            // The header is three bytes wide, so an arbitrary stream can ask for
            // sixteen megabytes. This format permits nothing that large. Refusing
            // here keeps the buffer bounded.
            if body > max_chunk_len() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("snappy chunk of {body} bytes exceeds the format's maximum"),
                ));
            }
            chunk.resize(HEADER_LEN + body, 0);

            let read = read_full(&mut reader, &mut chunk[HEADER_LEN..]).await?;
            if read != body {
                return Err(truncated(read, body));
            }
            decoder.get_mut().set_position(0);

            loop {
                match std::io::Read::read(&mut decoder, &mut block)? {
                    0 => break,
                    len => {
                        co.yield_(bytes::Bytes::copy_from_slice(&block[..len]))
                            .await
                    }
                }
            }
        }
    });

    futures::TryStreamExt::into_async_read(Box::pin(chunks))
}

/// Fill `buf` from `reader`, returning the bytes read. A short return is the end of
/// the stream. That is a truncation unless it falls on a chunk boundary.
async fn read_full<R>(reader: &mut R, buf: &mut [u8]) -> std::io::Result<usize>
where
    R: futures::io::AsyncRead + Unpin,
{
    let mut filled = 0;

    while filled != buf.len() {
        match futures::AsyncReadExt::read(reader, &mut buf[filled..]).await? {
            0 => break,
            read => filled += read,
        }
    }
    Ok(filled)
}

/// Largest chunk body the format permits. That is a whole block compressed, plus its
/// checksum.
fn max_chunk_len() -> usize {
    snap::raw::max_compress_len(MAX_BLOCK_LEN) + 4
}

fn truncated(read: usize, expected: usize) -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::UnexpectedEof,
        format!("snappy stream ends within a chunk, having {read} of {expected} bytes"),
    )
}

#[cfg(test)]
mod test {
    use super::decode;

    /// Compress `content` as `SNAPPY` fragments are written, in blocks of
    /// `MAX_BLOCK_LEN`.
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
        let mut out = Vec::new();
        let reader = decode(futures::io::Cursor::new(compressed.to_vec()));
        futures::pin_mut!(reader);

        futures::AsyncReadExt::read_to_end(&mut reader, &mut out).await?;
        Ok(out)
    }

    #[tokio::test]
    async fn test_round_trip_across_chunk_boundaries() {
        // These are empty, part of a block, exactly a block, and several blocks
        // with a partial one at the end.
        for len in [
            0,
            1,
            100,
            super::MAX_BLOCK_LEN,
            5 * super::MAX_BLOCK_LEN + 7,
        ] {
            let expect = content(len);
            assert_eq!(decoded(&framed(&expect)).await.unwrap(), expect, "{len}");
        }
    }

    #[tokio::test]
    async fn test_a_truncated_stream_is_an_error() {
        let compressed = framed(&content(3 * super::MAX_BLOCK_LEN));

        for len in [1, super::HEADER_LEN, compressed.len() - 1] {
            let err = decoded(&compressed[..len]).await.unwrap_err();
            assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof, "{len}");
        }
    }

    #[tokio::test]
    async fn test_a_stream_which_is_not_snappy_is_an_error() {
        let err = decoded(b"not a snappy stream at all").await.unwrap_err();
        assert!(format!("{err}").contains("exceeds the format"), "{err}");

        // A well-formed uncompressed chunk. It is still not the stream identifier
        // every stream must begin with.
        let err = decoded(&[0x01, 0x06, 0x00, 0x00, 0, 0, 0, 0, b'h', b'i'])
            .await
            .unwrap_err();

        assert!(format!("{err}").contains("stream header"), "{err}");
    }

    #[tokio::test]
    async fn test_a_corrupted_block_fails_its_checksum() {
        let mut compressed = framed(&content(1000));
        *compressed.last_mut().unwrap() ^= 0xff;

        let err = decoded(&compressed).await.unwrap_err();
        assert!(format!("{err}").contains("checksum"), "{err}");
    }
}
