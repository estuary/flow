use prost::Message;

/// Service is a trait implemented by Rust services which may be called from Go.
pub trait Service {
    /// Error type returned by Service invocations.
    type Error: std::error::Error + Send + Sync + 'static;

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

    /// Invoke the Service with the given code and Message,
    /// which is marshalled to an allocated buffer.
    /// This routine is intended for testing Services.
    fn invoke_message<M: Message>(
        &mut self,
        code: u32,
        msg: M,
        arena: &mut Vec<u8>,
        out: &mut Vec<Out>,
    ) -> Result<(), Self::Error> {
        let mut data = Vec::new();
        msg.encode_raw(&mut data);
        self.invoke(code, &data, arena, out)
    }
}

/// Send a protobuf message to the caller, by marshalling directly into
/// the |arena| and pushing an Out frame that references its offsets.
pub fn send_message<M: Message>(code: u32, msg: &M, arena: &mut Vec<u8>, out: &mut Vec<Out>) {
    let begin = arena.len() as u32;
    msg.encode_raw(arena);

    out.push(Out {
        code,
        begin,
        end: arena.len() as u32,
    });
}

/// Send bytes to the caller, where bytes have already been appended into
/// |arena| beginning at arena offset |begin|. An Out frame that references
/// the arena span (through the current arena length) is pushed.
pub fn send_bytes(code: u32, begin: usize, arena: &mut Vec<u8>, out: &mut Vec<Out>) {
    out.push(Out {
        code,
        begin: begin as u32,
        end: arena.len() as u32,
    });
}

/// Output frame produced by a Service.
#[repr(C)]
#[derive(Debug)]
pub struct Out {
    /// Service-defined response code.
    pub code: u32,
    /// Begin data offset within the arena.
    pub begin: u32,
    /// End data offset within the arena.
    pub end: u32,
}
