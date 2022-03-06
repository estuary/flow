use crate::apis::{FlowCaptureOperation, FlowMaterializeOperation, Interceptor, InterceptorStream};
use crate::errors::Error;
use crate::libs::command::resume_process;

pub struct DefaultFlowCaptureInterceptor {}
impl Interceptor<FlowCaptureOperation> for DefaultFlowCaptureInterceptor {
    fn convert_command_args(
        &self,
        op: &FlowCaptureOperation,
        args: Vec<String>,
    ) -> Result<Vec<String>, Error> {
        let mut new_args = vec![op.to_string()];
        new_args.extend_from_slice(&args);
        Ok(new_args)
    }

    fn convert_request(
        &self,
        pid: Option<u32>,
        _op: &FlowCaptureOperation,
        stream: InterceptorStream,
    ) -> Result<InterceptorStream, Error> {
        match pid {
            None => Err(Error::MissingPid),
            Some(pid) => {
                resume_process(pid)?;
                Ok(stream)
            }
        }
    }
}

pub struct DefaultFlowMaterializeInterceptor {}
impl Interceptor<FlowMaterializeOperation> for DefaultFlowMaterializeInterceptor {
    fn convert_command_args(
        &self,
        op: &FlowMaterializeOperation,
        args: Vec<String>,
    ) -> Result<Vec<String>, Error> {
        let mut new_args = vec![op.to_string()];
        new_args.extend_from_slice(&args);
        Ok(new_args)
    }

    fn convert_request(
        &self,
        pid: Option<u32>,
        _op: &FlowMaterializeOperation,
        stream: InterceptorStream,
    ) -> Result<InterceptorStream, Error> {
        match pid {
            None => Err(Error::MissingPid),
            Some(pid) => {
                resume_process(pid)?;
                Ok(stream)
            }
        }
    }
}
