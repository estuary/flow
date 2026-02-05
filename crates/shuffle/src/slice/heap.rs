/*
use super::Read;

use futures::stream;

struct ReadyRead {
    head: doc::OwnedArchivedNode,
    tail: simd_doc::transcoded::OwnedIterOut,
    read: Read,
}

async fn foobar(
    pending: &mut stream::FuturesUnordered<stream::StreamFuture<Read>>,
    parser: &mut simd_doc::SimdParser,
    mut offset: i64,
    mut content: Vec<u8>,
    read: Read,
) -> anyhow::Result<()> {
    let mut begin = offset;

    let transcoded =
        match simd_doc::transcode_many(parser, &mut content, &mut offset, Default::default()) {
            Ok(t) => t,
            Err((err, offsets)) => {
                anyhow::bail!(
                    "failed to parse document from {} at offset {offsets:?}: {err}",
                    read.journal,
                );
            }
        };

    let mut it = transcoded.into_iter();
    let Some((doc, _end_offset)) = it.next() else {
        pending.push(read.into_future());
        return Ok(());
    };

    let ready = ReadyRead {
        doc,
        queue: it,
        read,
    };

    //offset = offset.min(response.offset);
    //write_head = write_head.max(response.write_head);

    //buffer.extend(&response);

    todo!()
}
*/
