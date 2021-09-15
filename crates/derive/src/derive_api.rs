use super::pipeline::{self, Pipeline};
use super::registers;
use allocator::ThreadStatsReader;

use prost::Message;
use protocol::{
    cgo, flow,
    flow::derive_api::{self, Code},
};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("RocksDB error: {0}")]
    Rocks(#[from] rocksdb::Error),
    #[error("register database error")]
    RegisterErr(#[from] registers::Error),
    #[error(transparent)]
    PipelineErr(#[from] pipeline::Error),
    #[error("Protobuf decoding error")]
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
    DocHeader(Pipeline, flow::derive_api::DocHeader),
    Flushing(Pipeline),
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
        data: &[u8],
        arena: &mut Vec<u8>,
        out: &mut Vec<cgo::Out>,
    ) -> Result<(), Self::Error> {
        let code = match Code::from_i32(code as i32) {
            Some(c) => c,
            None => return Err(Error::InvalidState),
        };
        let span = tracing::span!(tracing::Level::TRACE, "derive_invoke", code = ?code);
        let _guard = span.enter();
        let mem_stats = ThreadStatsReader::new();
        let initial = mem_stats.current();

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

                // If we poll to idle, drain the combiner and transition to Prepare.
                // Otherwise begin to flush.
                if pipeline.poll_and_trampoline(arena, out)? {
                    pipeline.drain(arena, out);
                    self.state = State::Prepare(pipeline);
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

                // If we poll to idle, drain the combiner and transition to Prepare.
                // Otherwise we're still flushing.
                if pipeline.poll_and_trampoline(arena, out)? {
                    pipeline.drain(arena, out);
                    self.state = State::Prepare(pipeline);
                } else {
                    self.state = State::Flushing(pipeline);
                }
            }
            (Code::PrepareToCommit, State::Prepare(mut pipeline)) => {
                let derive_api::Prepare { checkpoint } = derive_api::Prepare::decode(data)?;
                pipeline.prepare(checkpoint.expect("checkpoint cannot be None"))?;

                self.state = State::Idle(pipeline);
            }
            _ => return Err(Error::InvalidState),
        }
        let diff = mem_stats.current() - initial;
        tracing::trace!(mem_initial = ?initial, mem_changes = ?diff, "mem changes");
        Ok(())
    }
}
