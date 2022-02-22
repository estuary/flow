use crate::connector_runners::commandutils::{
    invoke_and_handle_request, invoke_and_handle_response,
};
use crate::errors::Error;
use crate::flow_materialize_api::{FlowMaterialize, FlowMaterializePlugin};
use crate::plugin_handlers;

use protocol::materialize::{ApplyRequest, SpecResponse, TransactionRequest, ValidateRequest};

// FlowMaterializeConnectorRunner runs materialize connectors in Flow materialize protocol.
// https://github.com/estuary/flow/blob/master/go/protocols/materialize/materialize.proto
pub struct FlowMaterializeConnectorRunner {}

impl FlowMaterialize for FlowMaterializeConnectorRunner {
    fn do_spec(
        &self,
        entrypoint: Vec<String>,
        plugins: Vec<Box<dyn FlowMaterializePlugin>>,
    ) -> Result<(), Error> {
        let handlers = plugin_handlers!(
            plugins,
            SpecResponse,
            FlowMaterializePlugin::on_spec_response
        );
        invoke_and_handle_response(entrypoint, "spec", &handlers)
    }

    fn do_validate(
        &self,
        entrypoint: Vec<String>,
        plugins: Vec<Box<dyn FlowMaterializePlugin>>,
    ) -> Result<(), Error> {
        let handlers = plugin_handlers!(
            plugins,
            ValidateRequest,
            FlowMaterializePlugin::on_validate_request
        );
        invoke_and_handle_request(entrypoint, "validate", &handlers)
    }

    fn do_apply_upsert(
        &self,
        entrypoint: Vec<String>,
        plugins: Vec<Box<dyn FlowMaterializePlugin>>,
    ) -> Result<(), Error> {
        let handlers = plugin_handlers!(
            plugins,
            ApplyRequest,
            FlowMaterializePlugin::on_apply_upsert_request
        );
        invoke_and_handle_request(entrypoint, "apply-upsert", &handlers)
    }

    fn do_apply_delete(
        &self,
        entrypoint: Vec<String>,
        plugins: Vec<Box<dyn FlowMaterializePlugin>>,
    ) -> Result<(), Error> {
        let handlers = plugin_handlers!(
            plugins,
            ApplyRequest,
            FlowMaterializePlugin::on_apply_delete_request
        );
        invoke_and_handle_request(entrypoint, "apply-delete", &handlers)
    }

    fn do_transactions(
        &self,
        entrypoint: Vec<String>,
        plugins: Vec<Box<dyn FlowMaterializePlugin>>,
    ) -> Result<(), Error> {
        let handlers = plugin_handlers!(
            plugins,
            TransactionRequest,
            FlowMaterializePlugin::on_transactions_request
        );
        invoke_and_handle_request(entrypoint, "transactions", &handlers)
    }
}
