use bytes::{Buf, BufMut};
use prost::Message;
use std::{cell::RefCell, collections::HashMap};

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

/// Send a code to the caller without any data.
pub fn send_code(code: u32, out: &mut Vec<Out>) {
    out.push(Out {
        code,
        begin: 0,
        end: 0,
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

/// Trampoline manages tasks which are "bounced" to Go for execution
/// over a CGO bridge, with resolved results eventually sent back.
pub struct Trampoline {
    // Queue of tasks to be dispatched across the CGO bridge.
    dispatch_queue: RefCell<(u64, Vec<TrampolineTask>)>,
    // Callback handlers for tasks we've dispatched, and are awaiting responses for.
    awaiting: RefCell<HashMap<u64, Box<dyn FnOnce(Result<&[u8], anyhow::Error>)>>>,
}

impl Trampoline {
    pub fn new() -> Trampoline {
        Trampoline {
            dispatch_queue: Default::default(),
            awaiting: Default::default(),
        }
    }

    // True if there no queued or awaiting tasks remaining.
    pub fn is_empty(&self) -> bool {
        self.dispatch_queue.borrow().1.is_empty() && self.awaiting.borrow().is_empty()
    }

    // Start a new task which is queued for dispatch and execution over the bridge.
    pub fn start_task<D, C>(&self, code: u32, dispatch: D, callback: C)
    where
        D: for<'a> FnOnce(&'a mut Vec<u8>) + 'static,
        C: for<'a> FnOnce(Result<&'a [u8], anyhow::Error>) + 'static,
    {
        let mut queue = self.dispatch_queue.borrow_mut();

        // Assign monotonic sequence number.
        let id = queue.0;
        queue.0 += 1;

        queue.1.push({
            TrampolineTask {
                id,
                code,
                dispatch: Box::new(dispatch),
                callback: Box::new(callback),
            }
        });

        tracing::debug!(?id, ?code, queued = ?queue.1.len(), awaiting = ?self.awaiting.borrow().len(), "queued trampoline task");
    }

    // Dispatch all queued tasks over the bridge.
    pub fn dispatch_tasks(&self, code: u32, arena: &mut Vec<u8>, out: &mut Vec<Out>) {
        let mut awaiting = self.awaiting.borrow_mut();

        for task in self.dispatch_queue.borrow_mut().1.drain(..) {
            let begin = arena.len();

            arena.put_u64_le(task.id);
            arena.put_u32_le(task.code);
            (task.dispatch)(arena);

            send_bytes(code, begin, arena, out);
            awaiting.insert(task.id, task.callback);
        }
    }

    // Resolve a task for which we've received the response |data|.
    pub fn resolve_task(&self, mut data: &[u8]) {
        let id = data.get_u64_le();
        let ok = data.get_u8();

        let result = if ok != 0 {
            Ok(data)
        } else {
            Err(anyhow::anyhow!("{}", String::from_utf8_lossy(data)))
        };

        let callback = self
            .awaiting
            .borrow_mut()
            .remove(&id)
            .expect("unknown task ID");
        (callback)(result);

        tracing::debug!(?id, remaining = ?self.awaiting.borrow().len(), "resolved trampoline task");
    }
}

struct TrampolineTask {
    id: u64,
    code: u32,
    dispatch: Box<dyn FnOnce(&mut Vec<u8>)>,
    callback: Box<dyn FnOnce(Result<&[u8], anyhow::Error>)>,
}
