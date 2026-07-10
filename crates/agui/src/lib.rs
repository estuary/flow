//! AG-UI protocol server: HTTP POST of a `RunAgentInput` -> SSE of typed AG-UI
//! events, proxying a single inference call to an LLM provider.
//!
//! This crate is pure: no database, no authentication, no policy. It owns the
//! AG-UI wire contract ([`types`], [`events`]), the provider abstraction
//! ([`provider`]) with an [`anthropic`] backend and a deterministic [`mock`],
//! the SSE framing in both directions ([`sse`]), and the [`run`] state machine
//! that ties them together. Authentication, authorization, quota, and routing
//! live in the embedding service (`control-plane-api`), which calls [`run`] and
//! [`sse_response`] after its policy checks pass.
//!
//! See `DESIGN.md` for the full design and the AG-UI wire contract.

pub mod anthropic;
pub mod events;
pub mod mock;
pub mod provider;
pub mod run;
pub mod sse;
pub mod types;

pub use anthropic::AnthropicProvider;
pub use events::Event;
pub use mock::MockProvider;
pub use provider::{Provider, ProviderEvent, ProviderRequest};
pub use run::run;
pub use sse::sse_response;
pub use types::RunAgentInput;
