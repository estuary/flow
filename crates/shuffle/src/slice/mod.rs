use proto_gazette::broker;

mod actor;
mod handler;
mod heap;
mod listing;
mod read;

pub(crate) use handler::serve_slice;

pub trait Read:
    futures::Stream<Item = anyhow::Result<gazette::journal::read::LinesBatch>>
    + futures::stream::FusedStream
    + Send
{
    fn binding(&self) -> u32;

    #[allow(dead_code)]
    fn fragment(&self) -> &broker::Fragment;

    #[allow(dead_code)]
    fn write_head(&self) -> i64;

    fn put_back(self: std::pin::Pin<&mut Self>, content: bytes::Bytes);
}

pub type BoxedRead = std::pin::Pin<Box<dyn Read>>;
