use crate::errors::Error;
use protocol::capture::{
    ApplyRequest, ApplyResponse, DiscoverRequest, DiscoverResponse, PullRequest, PullResponse,
    SpecRequest, SpecResponse, ValidateRequest, ValidateResponse,
};

// The optional APIs that a capture plugin implement to intercept and handle the requests/responses of
// any operations of the FlowCapture protocol.
#[allow(unused_variables)]
#[rustfmt::skip]
pub trait FlowCapturePlugin: Send + Sync {
    fn on_spec_request(&self, request: &mut SpecRequest) -> Result<(), Error> { Ok(()) }
    fn on_spec_response(&self, response: &mut SpecResponse) -> Result<(), Error> { Ok(()) }

    fn on_discover_request(&self, request: &mut DiscoverRequest) -> Result<(), Error> { Ok(()) }
    fn on_discover_response(&self, response: &mut DiscoverResponse) -> Result<(), Error> { Ok(()) }

    fn on_validate_request(&self, request: &mut ValidateRequest) -> Result<(), Error> { Ok(()) }
    fn on_validate_response(&self, response: &mut ValidateResponse) -> Result<(), Error> { Ok(()) }

    fn on_apply_upsert_request(&self, request: &mut ApplyRequest) -> Result<(), Error> { Ok(()) }
    fn on_apply_upsert_response(&self, response: &mut ApplyResponse) -> Result<(), Error> { Ok(()) }

    fn on_apply_delete_request(&self, request: &mut ApplyRequest) -> Result<(), Error> { Ok(()) }
    fn on_apply_delete_response(&self, response: &mut ApplyResponse) -> Result<(), Error> { Ok(()) }

    fn on_pull_request(&self, request: &mut PullRequest) -> Result<(), Error> { Ok(()) }
    fn on_pull_response(&self, response: &mut PullResponse) -> Result<(), Error> { Ok(()) }
}

// The APIs that a capture connector runner are required to implement for adapting the FlowCapture protocol to the
// native protocol that the connector supports.
#[rustfmt::skip]
pub trait FlowCapture {
    fn do_spec( &self, entrypoint: Vec<String>, plugins: Vec<Box<dyn FlowCapturePlugin>>) -> Result<(), Error>;
    fn do_discover( &self, entrypoint: Vec<String>, plugins: Vec<Box<dyn FlowCapturePlugin>>) -> Result<(), Error>;
    fn do_validate( &self, entrypoint: Vec<String>, plugins: Vec<Box<dyn FlowCapturePlugin>>) -> Result<(), Error>;
    fn do_apply_upsert( &self, entrypoint: Vec<String>, plugins: Vec<Box<dyn FlowCapturePlugin>>) -> Result<(), Error>;
    fn do_apply_delete( &self, entrypoint: Vec<String>, plugins: Vec<Box<dyn FlowCapturePlugin>>) -> Result<(), Error>;
    fn do_pull( &self, entrypoint: Vec<String>, plugins: Vec<Box<dyn FlowCapturePlugin>>) -> Result<(), Error>;
}
