use bytes::{Buf, BufMut};
use futures::future::LocalBoxFuture;
use futures::{channel::oneshot, FutureExt};
use models::tables;
use prost::Message;
use protocol::{
    cgo, flow,
    flow::build_api::{self, Code},
    materialize,
};
use std::rc::Rc;
use std::task::Poll;
use std::{cell::RefCell, collections::HashMap};
use url::Url;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("protocol error (invalid state or invocation)")]
    InvalidState,
    #[error("Protobuf decoding error")]
    ProtoDecode(#[from] prost::DecodeError),
    #[error(transparent)]
    UTF8Error(#[from] std::str::Utf8Error),
    #[error(transparent)]
    Anyhow(#[from] anyhow::Error),
}

/// Trampoline manages tasks which are "bounced" to Go for execution
/// over a CGO bridge, with resolved results eventually sent back.
// NOTE(johnny): This will move to the `cgo` module, but I'm letting it bake here.
struct Trampoline {
    // Queue of tasks to be dispatched across the CGO bridge.
    dispatch_queue: RefCell<(u64, Vec<TrampolineTask>)>,
    // Callback handlers for tasks we've dispatched, and are awaiting responses for.
    awaiting: RefCell<HashMap<u64, Box<dyn FnOnce(Result<&[u8], anyhow::Error>)>>>,
}

impl Trampoline {
    fn new() -> Trampoline {
        Trampoline {
            dispatch_queue: Default::default(),
            awaiting: Default::default(),
        }
    }

    // True if there no queued or awaiting tasks remain.
    fn is_empty(&self) -> bool {
        self.dispatch_queue.borrow().1.is_empty() && self.awaiting.borrow().is_empty()
    }

    // Start a new task which is queued for dispatch and execution over the bridge.
    fn start_task<D, C>(&self, code: u32, dispatch: D, callback: C)
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

        tracing::debug!(?id, ?code, "starting task");
    }

    // Dispatch all queued tasks over the bridge.
    fn dispatch_tasks(&self, code: u32, arena: &mut Vec<u8>, out: &mut Vec<cgo::Out>) {
        let mut awaiting = self.awaiting.borrow_mut();

        for task in self.dispatch_queue.borrow_mut().1.drain(..) {
            let begin = arena.len();

            arena.put_u64_le(task.id);
            arena.put_u32_le(task.code);
            (task.dispatch)(arena);

            cgo::send_bytes(code, begin, arena, out);
            awaiting.insert(task.id, task.callback);
        }
    }

    // Resolve a task for which we've received the response |data|.
    fn resolve_task(&self, mut data: &[u8]) {
        let id = data.get_u64_le();
        let ok = data.get_u8();

        let result = if ok != 0 {
            Ok(data)
        } else {
            Err(anyhow::anyhow!("{}", String::from_utf8_lossy(data)))
        };

        let n_remain = self.awaiting.borrow().len();
        tracing::debug!(?id, ?n_remain, "resolving task");

        let callback = self
            .awaiting
            .borrow_mut()
            .remove(&id)
            .expect("unknown task ID");
        (callback)(result);
    }
}

struct TrampolineTask {
    id: u64,
    code: u32,
    dispatch: Box<dyn FnOnce(&mut Vec<u8>)>,
    callback: Box<dyn FnOnce(Result<&[u8], anyhow::Error>)>,
}

// Fetcher implements sources::Fetcher, and delegates to Go via Trampoline.
struct Fetcher(Rc<Trampoline>);

impl sources::Fetcher for Fetcher {
    fn fetch<'a>(
        &self,
        resource: &'a Url,
        content_type: &'a flow::ContentType,
    ) -> LocalBoxFuture<'a, Result<bytes::Bytes, anyhow::Error>> {
        let request = build_api::Fetch {
            resource_url: resource.to_string(),
            content_type: *content_type as i32,
        };
        let (tx, rx) = oneshot::channel();

        self.0.start_task(
            build_api::Code::TrampolineFetch as u32,
            move |arena: &mut Vec<u8>| request.encode_raw(arena),
            move |result: Result<&[u8], anyhow::Error>| {
                let result = result.map(|data| bytes::Bytes::copy_from_slice(data));
                tx.send(result).unwrap();
            },
        );
        rx.map(|r| r.unwrap()).boxed_local()
    }
}

// Drivers implements validation::Drivers, and delegates to Go via Trampoline.
struct Drivers(Rc<Trampoline>);

impl validation::Drivers for Drivers {
    fn validate_materialization<'a>(
        &'a self,
        mut request: materialize::ValidateRequest,
        endpoint_config: serde_json::Value,
    ) -> LocalBoxFuture<'a, Result<materialize::ValidateResponse, anyhow::Error>> {
        // To actually perform a validation, the caller first creates a session (using
        // |endpoint_config|) to obtain a handle, and *then* validates the |request|.
        // We must pass the (request, endpoint_config) tuple to the Go side to do this workflow.
        //
        // We _could_ represent this as a new protobuf message, but we already have a ValidateRequest
        // that, by construction, has an empty and unused |handle| field. As an implementation detail
        // of the bridge API binding, we thus simply pack |endpoint_config| into |handle|.
        request.handle = endpoint_config.to_string().into();

        let (tx, rx) = oneshot::channel();

        self.0.start_task(
            build_api::Code::TrampolineValidateMaterialization as u32,
            move |arena: &mut Vec<u8>| request.encode_raw(arena),
            move |result: Result<&[u8], anyhow::Error>| {
                let result = result.and_then(|data| {
                    materialize::ValidateResponse::decode(data).map_err(Into::into)
                });
                tx.send(result).unwrap();
            },
        );
        rx.map(|r| r.unwrap()).boxed_local()
    }
}

// BuildFuture is a polled future which builds a catalog.
struct BuildFuture {
    boxed: LocalBoxFuture<'static, Result<tables::All, anyhow::Error>>,
    trampoline: Rc<Trampoline>,
}

impl BuildFuture {
    fn new(config: build_api::Config) -> Result<Self, Error> {
        let trampoline = Rc::new(Trampoline::new());
        let fetcher = Fetcher(trampoline.clone());
        let drivers = Drivers(trampoline.clone());
        let future = crate::configured_build(config, fetcher, drivers);

        Ok(BuildFuture {
            boxed: future.boxed_local(),
            trampoline,
        })
    }

    // Dispatch all queued work to the Go side of the CGO bridge.
    fn dispatch_work(&mut self, arena: &mut Vec<u8>, out: &mut Vec<cgo::Out>) {
        self.trampoline
            .dispatch_tasks(build_api::Code::Trampoline as u32, arena, out);
    }
}

/// API implements the CGO bridge service for the build API.
pub struct API {
    state: State,
}

// State is the private inner state machine of the API.
enum State {
    Init,
    // We're ready to be immediately polled.
    PollReady { future: BuildFuture },
    // We've polled to Pending and have dispatched work, but it must
    // resolve before we may continue.
    PollIdle { future: BuildFuture },
    // Build is completed.
    Done,
}

impl cgo::Service for API {
    type Error = Error;

    fn create() -> Self {
        Self { state: State::Init }
    }

    fn invoke(
        &mut self,
        code: u32,
        data: &[u8],
        arena: &mut Vec<u8>,
        out: &mut Vec<cgo::Out>,
    ) -> Result<(), Self::Error> {
        tracing::trace!(?code, "invoke");

        let code = match Code::from_i32(code as i32) {
            Some(c) => c,
            None => return Err(Error::InvalidState),
        };
        let state = std::mem::replace(&mut self.state, State::Init);

        match (code, state) {
            // Begin build.
            (Code::Begin, State::Init) => {
                let config = build_api::Config::decode(data)?;

                self.state = State::PollReady {
                    future: BuildFuture::new(config)?,
                };
                Ok(())
            }
            // Poll future.
            (Code::Poll, State::PollReady { mut future }) => {
                let waker = futures::task::noop_waker();
                let mut ctx = std::task::Context::from_waker(&waker);

                match future.boxed.poll_unpin(&mut ctx) {
                    Poll::Ready(result) => {
                        let tables = result?;

                        // We must have drained all outstanding fetches.
                        assert!(future.trampoline.is_empty());

                        if tables.errors.is_empty() {
                            cgo::send_code(Code::Done as u32, out);
                        } else {
                            cgo::send_code(Code::DoneWithErrors as u32, out);
                        }

                        self.state = State::Done;
                        Ok(())
                    }
                    Poll::Pending => {
                        future.dispatch_work(arena, out);

                        self.state = State::PollIdle { future };
                        Ok(())
                    }
                }
            }
            // Trampoline task has resolved.
            (Code::Trampoline, State::PollIdle { future })
            | (Code::Trampoline, State::PollReady { future }) => {
                future.trampoline.resolve_task(data);

                self.state = State::PollReady { future };
                Ok(())
            }
            // Return source catalog JSON schema.
            (Code::CatalogSchema, State::Init) => {
                let settings = schemars::gen::SchemaSettings::draft07();
                let generator = schemars::gen::SchemaGenerator::new(settings);
                let schema = generator.into_root_schema_for::<sources::Catalog>();

                let begin = arena.len();
                let w: &mut Vec<u8> = &mut *arena;
                serde_json::to_writer_pretty(w, &schema).expect("encoding cannot fail");
                cgo::send_bytes(Code::CatalogSchema as u32, begin, arena, out);

                self.state = State::Done;
                Ok(())
            }
            _ => Err(Error::InvalidState),
        }
    }
}
