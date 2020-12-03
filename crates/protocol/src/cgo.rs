/// Service is a trait implemented by Rust services which may be called from Go.
pub trait Service {
    /// Error type returned by Service invocations.
    type Error: std::error::Error;

    /// Create a new instance of the Service.
    fn create() -> Self;

    /// Invoke the Service with the given code & data payload.
    ///
    /// Both codes & payloads are used defined -- a service and its callers must
    /// establish a shared protocol for interaction, in terms of codes and/or
    /// []byte payloads.
    ///
    /// Invoke may return []byte data to the caller by writing it at the end of
    /// |arena|. Existing content of the arena must not be modified.
    ///
    /// Invoke may also return Out frames to the caller by appending into |out|.
    /// Out frames can return []byte data by writing to |arena| first, and then
    /// appending an Out frame which references the written offsets.
    /// As with |arena|, existing |out| frames must not be modified.
    fn invoke(
        &mut self,
        code: u32,
        data: &[u8],
        arena: &mut Vec<u8>,
        out: &mut Vec<Out>,
    ) -> Result<(), Self::Error>;
}

/// Output frame produced by a Service.
#[repr(C)]
pub struct Out {
    /// Service-defined response code.
    pub code: u32,
    /// Begin data offset within the arena.
    pub begin: u32,
    /// End data offset within the arena.
    pub end: u32,
}
