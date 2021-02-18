use futures::future::LocalBoxFuture;
use futures::{channel::oneshot, FutureExt};
use models::tables;
use prost::Message;
use protocol::{
    cgo, flow,
    flow::build_api::{self, Code},
    materialize,
};
use std::cell::RefCell;
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

// Fetcher implements sources::Fetcher, delegated to Go via the CGO bridge.
struct Fetcher(Rc<RefCell<Vec<Fetch>>>);

// Fetch represents an outstanding fetch() of Fetcher.
struct Fetch {
    request: build_api::Fetch,
    response: oneshot::Sender<Result<bytes::Bytes, anyhow::Error>>,
}

impl sources::Fetcher for Fetcher {
    fn fetch<'a>(
        &self,
        resource: &'a Url,
        content_type: &'a flow::ContentType,
    ) -> LocalBoxFuture<'a, Result<bytes::Bytes, anyhow::Error>> {
        let (tx, rx) = oneshot::channel();

        self.0.borrow_mut().push(Fetch {
            request: build_api::Fetch {
                resource_url: resource.to_string(),
                content_type: *content_type as i32,
            },
            response: tx,
        });

        rx.map(|r| r.unwrap()).boxed_local()
    }
}

// Drivers implements validation::Drivers, delegated to Go via the CGO bridge.
#[derive(Default)]
pub struct Drivers {}

impl validation::Drivers for Drivers {
    fn validate_materialization<'a>(
        &'a self,
        _endpoint_type: flow::EndpointType,
        _endpoint_config: serde_json::Value,
        _request: materialize::ValidateRequest,
    ) -> LocalBoxFuture<'a, Result<materialize::ValidateResponse, anyhow::Error>> {
        async { anyhow::bail!("not implemented yet") }.boxed_local()
    }
}

// BuildFuture is a polled future which builds a catalog.
struct BuildFuture {
    boxed: LocalBoxFuture<'static, Result<tables::All, anyhow::Error>>,
    fetch_dispatch: Rc<RefCell<Vec<Fetch>>>,
    fetch_awaiting: Vec<Fetch>,
}

impl BuildFuture {
    fn new(config: build_api::Config) -> Result<Self, Error> {
        let fetch_dispatch = Rc::new(RefCell::new(Vec::new()));
        let fetcher = Fetcher(fetch_dispatch.clone());
        let drivers = Drivers::default(); // TODO.

        let future = crate::configured_build(config, fetcher, drivers);

        Ok(BuildFuture {
            boxed: future.boxed_local(),
            fetch_dispatch,
            fetch_awaiting: Vec::new(),
        })
    }

    // Dispatch all queued work to the Go side of the CGO bridge.
    fn dispatch_work(&mut self, arena: &mut Vec<u8>, out: &mut Vec<cgo::Out>) {
        for fetch in self.fetch_dispatch.borrow_mut().drain(..) {
            cgo::send_message(Code::FetchRequest as u32, &fetch.request, arena, out);
            self.fetch_awaiting.push(fetch);
        }
    }

    // Resolve an awaiting fetch of the given resource, with the given result.
    fn resolve_fetch(&mut self, resource_url: String, result: Result<bytes::Bytes, anyhow::Error>) {
        let index = self
            .fetch_awaiting
            .iter()
            .enumerate()
            .find_map(|(index, fetch)| {
                if fetch.request.resource_url == resource_url {
                    Some(index)
                } else {
                    None
                }
            })
            .expect("resource_url must be an awaiting fetch");

        self.fetch_awaiting
            .swap_remove(index)
            .response
            .send(result)
            .unwrap();
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
    PollReady {
        future: BuildFuture,
    },
    // We've polled to Pending and have dispatched work, but it must
    // resolve before we may continue.
    PollIdle {
        future: BuildFuture,
    },
    // We're loading catalog sources, and have been notified that a
    // fetched resource is about to resolve.
    ResolvingFetch {
        future: BuildFuture,
        resource_url: String,
    },
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
                        assert!(future.fetch_dispatch.borrow().is_empty());
                        assert!(future.fetch_awaiting.is_empty());

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
            // Fetch is resolving.
            (Code::FetchRequest, State::PollIdle { future })
            | (Code::FetchRequest, State::PollReady { future }) => {
                let resource_url = std::str::from_utf8(data)?.to_string();

                self.state = State::ResolvingFetch {
                    future,
                    resource_url,
                };
                Ok(())
            }
            // Fetch has resolved successfully.
            (
                Code::FetchSuccess,
                State::ResolvingFetch {
                    mut future,
                    resource_url,
                },
            ) => {
                future.resolve_fetch(resource_url, Ok(bytes::Bytes::copy_from_slice(data)));

                self.state = State::PollReady { future };
                Ok(())
            }
            // Fetch has resolved with an error.
            (
                Code::FetchFailed,
                State::ResolvingFetch {
                    mut future,
                    resource_url,
                },
            ) => {
                future.resolve_fetch(
                    resource_url,
                    Err(anyhow::anyhow!("{}", String::from_utf8_lossy(data))),
                );

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
