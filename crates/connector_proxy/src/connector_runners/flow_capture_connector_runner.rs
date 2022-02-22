use crate::connector_runners::commandutils::{
    invoke_and_handle_request, invoke_and_handle_response,
};
use crate::errors::Error;
use crate::flow_capture_api::{FlowCapture, FlowCapturePlugin};
use crate::plugin_handlers;

use protocol::capture::{
    ApplyRequest, DiscoverRequest, PullRequest, SpecResponse, ValidateRequest,
};
pub struct FlowCaptureConnectorRunner {}

impl FlowCapture for FlowCaptureConnectorRunner {
    fn do_spec(
        &self,
        entrypoint: Vec<String>,
        plugins: Vec<Box<dyn FlowCapturePlugin>>,
    ) -> Result<(), Error> {
        let handlers = plugin_handlers!(plugins, SpecResponse, FlowCapturePlugin::on_spec_response);
        invoke_and_handle_response(entrypoint, "spec", &handlers)
    }

    fn do_discover(
        &self,
        entrypoint: Vec<String>,
        plugins: Vec<Box<dyn FlowCapturePlugin>>,
    ) -> Result<(), Error> {
        let handlers = plugin_handlers!(
            plugins,
            DiscoverRequest,
            FlowCapturePlugin::on_discover_request
        );

        invoke_and_handle_request(entrypoint, "discover", &handlers)
    }

    fn do_validate(
        &self,
        entrypoint: Vec<String>,
        plugins: Vec<Box<dyn FlowCapturePlugin>>,
    ) -> Result<(), Error> {
        let handlers = plugin_handlers!(
            plugins,
            ValidateRequest,
            FlowCapturePlugin::on_validate_request
        );

        invoke_and_handle_request(entrypoint, "validate", &handlers)
    }

    fn do_apply_upsert(
        &self,
        entrypoint: Vec<String>,
        plugins: Vec<Box<dyn FlowCapturePlugin>>,
    ) -> Result<(), Error> {
        let handlers = plugin_handlers!(
            plugins,
            ApplyRequest,
            FlowCapturePlugin::on_apply_upsert_request
        );

        invoke_and_handle_request(entrypoint, "apply-upsert", &handlers)
    }

    fn do_apply_delete(
        &self,
        entrypoint: Vec<String>,
        plugins: Vec<Box<dyn FlowCapturePlugin>>,
    ) -> Result<(), Error> {
        let handlers = plugin_handlers!(
            plugins,
            ApplyRequest,
            FlowCapturePlugin::on_apply_delete_request
        );

        invoke_and_handle_request(entrypoint, "apply-delete", &handlers)
    }

    fn do_pull(
        &self,
        entrypoint: Vec<String>,
        plugins: Vec<Box<dyn FlowCapturePlugin>>,
    ) -> Result<(), Error> {
        let handlers = plugin_handlers!(plugins, PullRequest, FlowCapturePlugin::on_pull_request);

        invoke_and_handle_request(entrypoint, "pull", &handlers)
    }
}
