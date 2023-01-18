use futures::future::LocalBoxFuture;
use futures::{channel::oneshot, FutureExt};
use prost::Message;
use protocol::{
    capture, flow,
    flow::build_api::{self, Code},
    materialize,
};
use std::rc::Rc;
use std::task::Poll;
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

// Fetcher implements sources::Fetcher, and delegates to Go via Trampoline.
struct Fetcher(Rc<cgo::Trampoline>);

impl sources::Fetcher for Fetcher {
    fn fetch<'a>(
        &self,
        resource: &'a Url,
        content_type: flow::ContentType,
    ) -> LocalBoxFuture<'a, Result<bytes::Bytes, anyhow::Error>> {
        let request = build_api::Fetch {
            resource_url: resource.to_string(),
            content_type: content_type as i32,
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
struct Drivers(Rc<cgo::Trampoline>);

impl validation::Drivers for Drivers {
    fn validate_materialization<'a>(
        &'a self,
        request: materialize::ValidateRequest,
    ) -> LocalBoxFuture<'a, Result<materialize::ValidateResponse, anyhow::Error>> {
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

    fn validate_capture<'a>(
        &'a self,
        request: protocol::capture::ValidateRequest,
    ) -> LocalBoxFuture<'a, Result<protocol::capture::ValidateResponse, anyhow::Error>> {
        let (tx, rx) = oneshot::channel();

        self.0.start_task(
            build_api::Code::TrampolineValidateCapture as u32,
            move |arena: &mut Vec<u8>| request.encode_raw(arena),
            move |result: Result<&[u8], anyhow::Error>| {
                let result = result
                    .and_then(|data| capture::ValidateResponse::decode(data).map_err(Into::into));
                tx.send(result).unwrap();
            },
        );
        rx.map(|r| r.unwrap()).boxed_local()
    }
}

// BuildFuture is a polled future which builds a catalog.
struct BuildFuture {
    boxed: LocalBoxFuture<'static, Result<tables::All, anyhow::Error>>,
    trampoline: Rc<cgo::Trampoline>,
}

impl BuildFuture {
    fn new(config: build_api::Config) -> Result<Self, Error> {
        let trampoline = Rc::new(cgo::Trampoline::new());
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
        let code = match Code::from_i32(code as i32) {
            Some(c) => c,
            None => return Err(Error::InvalidState),
        };
        tracing::trace!(?code, "invoke");

        match (code, std::mem::replace(&mut self.state, State::Init)) {
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
                let schema = models::Catalog::root_json_schema();

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
