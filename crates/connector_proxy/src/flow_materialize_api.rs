use crate::errors::Error;
use protocol::materialize::{
    ApplyRequest, ApplyResponse, SpecRequest, SpecResponse, TransactionRequest,
    TransactionResponse, ValidateRequest, ValidateResponse,
};

#[allow(unused_variables)]
#[rustfmt::skip]
pub trait FlowMaterializePlugin: Send + Sync {
    fn on_spec_request(&self, response: &mut SpecRequest) -> Result<(), Error> { Ok(()) }
    fn on_spec_response(&self, response: &mut SpecResponse) -> Result<(), Error> { Ok(()) }

    fn on_validate_request(&self, request: &mut ValidateRequest) -> Result<(), Error> { Ok(()) }
    fn on_validate_response(&self, response: &mut ValidateResponse) -> Result<(), Error> { Ok(()) }

    fn on_apply_upsert_request(&self, request: &mut ApplyRequest) -> Result<(), Error> { Ok(()) }
    fn on_apply_upsert_response(&self, response: &mut ApplyResponse) -> Result<(), Error> { Ok(()) }

    fn on_apply_delete_request(&self, request: &mut ApplyRequest) -> Result<(), Error> { Ok(()) }
    fn on_apply_delete_response(&self, response: &mut ApplyResponse) -> Result<(), Error> { Ok(()) }

    fn on_transactions_request( &self, request: &mut TransactionRequest) -> Result<(), Error> { Ok(()) }
    fn on_transactions_response( &self, response: &mut TransactionResponse) -> Result<(), Error> { Ok(()) }
}

#[rustfmt::skip]
pub trait FlowMaterialize {
    fn do_spec( &self, entrypoint: Vec<String>, plugins: Vec<Box<dyn FlowMaterializePlugin>>) -> Result<(), Error>;
    fn do_validate( &self, entrypoint: Vec<String>, plugins: Vec<Box<dyn FlowMaterializePlugin>>) -> Result<(), Error>;
    fn do_apply_upsert( &self, entrypoint: Vec<String>, plugins: Vec<Box<dyn FlowMaterializePlugin>>) -> Result<(), Error>;
    fn do_apply_delete( &self, entrypoint: Vec<String>, plugins: Vec<Box<dyn FlowMaterializePlugin>>) -> Result<(), Error>;
    fn do_transactions( &self, entrypoint: Vec<String>, plugins: Vec<Box<dyn FlowMaterializePlugin>>) -> Result<(), Error>;
}
