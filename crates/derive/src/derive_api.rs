use super::pipeline::{self, Pipeline};
use super::registers;

use bytes::Buf;
use prost::Message;
use proto_flow::flow::derive_api::{self, Code};

#[derive(thiserror::Error, Debug, serde::Serialize)]
pub enum Error {
    #[error("RocksDB error: {0}")]
    #[serde(serialize_with = "crate::serialize_as_display")]
    Rocks(#[from] rocksdb::Error),
    #[error("register database error")]
    RegisterErr(#[from] registers::Error),
    #[error(transparent)]
    PipelineErr(#[from] pipeline::Error),
    #[error("Protobuf decoding error")]
    #[serde(serialize_with = "crate::serialize_as_display")]
    ProtoDecode(#[from] prost::DecodeError),
    #[error("protocol error (invalid state or invocation)")]
    InvalidState,
}

/// API provides a derivation capability as a cgo::Service.
pub struct API {
    state: State,
}

// State is the private inner state machine of the API.
enum State {
    Init,
    Opened(registers::Registers),
    Idle(Pipeline),
    Running(Pipeline),
    DocHeader(Pipeline, derive_api::DocHeader),
    Flushing(Pipeline),
    Draining(Pipeline),
    Prepare(Pipeline),
}

impl cgo::Service for API {
    type Error = Error;

    fn create() -> Self {
        Self { state: State::Init }
    }

    fn invoke(
        &mut self,
        code: u32,
        mut data: &[u8],
        arena: &mut Vec<u8>,
        out: &mut Vec<cgo::Out>,
    ) -> Result<(), Self::Error> {
        let code = match Code::from_i32(code as i32) {
            Some(c) => c,
            None => return Err(Error::InvalidState),
        };
        tracing::trace!(code = ?code, "derive_invoke");

        match (code, std::mem::replace(&mut self.state, State::Init)) {
            (Code::Open, State::Init) => {
                let derive_api::Open {
                    local_dir,
                    rocksdb_env_memptr,
                } = derive_api::Open::decode(data)?;

                tracing::debug!(
                    ?local_dir,
                    ?rocksdb_env_memptr,
                    "opening registers database"
                );

                // Re-hydrate the provided memory address into rocksdb::Env wrapping
                // an owned *mut librocksdb_sys::rocksdb_env_t.
                let env = unsafe {
                    rocksdb::Env::from_raw(rocksdb_env_memptr as *mut librocksdb_sys::rocksdb_env_t)
                };

                let mut opts = rocksdb::Options::default();
                opts.set_env(&env);
                let registers = registers::Registers::new(opts, &local_dir)?;

                self.state = State::Opened(registers);
            }
            (Code::Configure, State::Opened(registers)) => {
                let config = derive_api::Config::decode(data)?;
                let pipeline = pipeline::Pipeline::from_config_and_parts(config, registers, 1)?;
                self.state = State::Idle(pipeline);
            }
            (Code::Configure, State::Idle(pipeline)) => {
                let config = derive_api::Config::decode(data)?;
                let (registers, next_id) = pipeline.into_inner();
                let pipeline =
                    pipeline::Pipeline::from_config_and_parts(config, registers, next_id)?;
                self.state = State::Idle(pipeline);
            }
            (Code::RestoreCheckpoint, State::Idle(pipeline)) => {
                let checkpoint = pipeline.last_checkpoint()?;
                cgo::send_message(Code::RestoreCheckpoint as u32, &checkpoint, arena, out);
                self.state = State::Idle(pipeline);
            }
            (Code::ClearRegisters, State::Idle(mut pipeline)) => {
                pipeline.clear_registers()?;
                self.state = State::Idle(pipeline);
            }
            (Code::BeginTransaction, State::Idle(pipeline)) => {
                self.state = State::Running(pipeline);
            }
            (Code::NextDocumentHeader, State::Running(pipeline)) => {
                let header = derive_api::DocHeader::decode(data)?;
                self.state = State::DocHeader(pipeline, header);
            }
            (Code::NextDocumentBody, State::DocHeader(mut pipeline, header)) => {
                if pipeline.add_source_document(header, data)? {
                    pipeline.poll_and_trampoline(arena, out)?;
                }
                self.state = State::Running(pipeline);
            }
            (Code::FlushTransaction, State::Running(mut pipeline)) => {
                pipeline.flush();

                // If we poll to idle then flushing completes synchronously and we
                // can transition to Draining. Otherwise outstanding trampoline tasks
                // must complete before we may transition.
                if pipeline.poll_and_trampoline(arena, out)? {
                    cgo::send_code(Code::FlushedTransaction as u32, out);
                    self.state = State::Draining(pipeline);
                } else {
                    self.state = State::Flushing(pipeline);
                }
            }
            (Code::Trampoline, State::Running(mut pipeline)) => {
                pipeline.resolve_task(data);
                pipeline.poll_and_trampoline(arena, out)?;
                self.state = State::Running(pipeline);
            }
            (Code::Trampoline, State::Flushing(mut pipeline)) => {
                pipeline.resolve_task(data);

                // Once we poll to idle, flushing is complete and we being Draining.
                // Otherwise we're still flushing due to outstanding trampoline tasks.
                if pipeline.poll_and_trampoline(arena, out)? {
                    cgo::send_code(Code::FlushedTransaction as u32, out);
                    self.state = State::Draining(pipeline);
                } else {
                    self.state = State::Flushing(pipeline);
                }
            }
            (Code::DrainChunk, State::Draining(pipeline)) if data.len() == 4 => {
                let (pipeline, more) = pipeline.drain_chunk(data.get_u32() as usize, arena, out)?;
                if more {
                    self.state = State::Draining(pipeline);
                } else {
                    self.state = State::Prepare(pipeline);
                }
            }
            (Code::PrepareToCommit, State::Prepare(mut pipeline)) => {
                let derive_api::Prepare { checkpoint } = derive_api::Prepare::decode(data)?;
                pipeline.prepare(checkpoint.expect("checkpoint cannot be None"))?;

                self.state = State::Idle(pipeline);
            }
            _ => return Err(Error::InvalidState),
        }

        Ok(())
    }
}
