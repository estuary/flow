use crate::connector_runners::commandutils::{check_exit_status, invoke_connector};
use crate::errors::Error;
use crate::flow_capture_api::{FlowCapture, FlowCapturePlugin};

use std::process::Stdio;

pub struct AirbyteSourceConnectorRunner {}

impl FlowCapture for AirbyteSourceConnectorRunner {
    fn do_spec(
        &self,
        entrypoint: Vec<String>,
        _plugins: Vec<Box<dyn FlowCapturePlugin>>,
    ) -> Result<(), Error> {
        let status =
            invoke_connector(entrypoint, Stdio::inherit(), Stdio::inherit(), "spec")?.wait();
        check_exit_status(status)
    }

    fn do_discover(
        &self,
        entrypoint: Vec<String>,
        _plugins: Vec<Box<dyn FlowCapturePlugin>>,
    ) -> Result<(), Error> {
        let status =
            invoke_connector(entrypoint, Stdio::inherit(), Stdio::inherit(), "discover")?.wait();
        check_exit_status(status)
    }

    fn do_validate(
        &self,
        entrypoint: Vec<String>,
        _plugins: Vec<Box<dyn FlowCapturePlugin>>,
    ) -> Result<(), Error> {
        let status =
            invoke_connector(entrypoint, Stdio::inherit(), Stdio::inherit(), "validate")?.wait();
        check_exit_status(status)
    }

    fn do_apply_upsert(
        &self,
        _entrypoint: Vec<String>,
        _plugins: Vec<Box<dyn FlowCapturePlugin>>,
    ) -> Result<(), Error> {
        Ok(())
    }

    fn do_apply_delete(
        &self,
        _entrypoint: Vec<String>,
        _plugins: Vec<Box<dyn FlowCapturePlugin>>,
    ) -> Result<(), Error> {
        Ok(())
    }

    fn do_pull(
        &self,
        entrypoint: Vec<String>,
        _plugins: Vec<Box<dyn FlowCapturePlugin>>,
    ) -> Result<(), Error> {
        invoke_connector(entrypoint, Stdio::inherit(), Stdio::inherit(), "pull")?.wait()?;
        Ok(())
    }
}
