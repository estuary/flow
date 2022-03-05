use crate::apis::{FlowCaptureOperation, Interceptor, InterceptorStream};
use crate::errors::Error;
use crate::libs::command::resume_process;

pub struct AirbyteCaptureInterceptor {}

impl AirbyteCaptureInterceptor {
    fn convert_spec_request(pid: u32, stream: InterceptorStream) -> InterceptorStream {
        resume_process(pid);
        stream
    }

    fn convert_request(
        pid: u32,
        operation: &FlowCaptureOperation,
        stream: InterceptorStream,
    ) -> Result<InterceptorStream, Error> {
        Ok(match operation {
            FlowCaptureOperation::Spec => Self::convert_spec_request(pid, stream),

            //FlowCaptureOperation::Discover => Self::convert_discover_request(stream),
            //FlowCaptureOperation::Validate => Self::convert_validate_request(stream),
            //FlowCaptureOperation::ApplyUpsert | FlowCaptureOperation::ApplyDelete => {
            //    Self::convert_apply_request(stream)
            //}
            //FlowCaptureOperation::Pull => Self::convert_pull_request(stream),
            _ => stream,
        })
    }

    fn convert_response(
        _operation: &FlowCaptureOperation,
        _in_stream: InterceptorStream,
    ) -> Result<InterceptorStream, Error> {
        panic!("TBD AirbyteCaptureInterceptor")
    }
}

impl Interceptor<FlowCaptureOperation> for AirbyteCaptureInterceptor {
    fn convert_command_args(
        &self,
        op: &FlowCaptureOperation,
        args: Vec<String>,
    ) -> Result<Vec<String>, Error> {
        //let new_args = vec![self.operation.to_string()];
        //new_args.extend_from_slice(&args);
        //Ok(new_args)
        Ok(args)
    }

    fn convert_request(
        &self,
        pid: Option<u32>,
        op: &FlowCaptureOperation,
        stream: InterceptorStream,
    ) -> Result<InterceptorStream, Error> {
        match pid {
            None => Err(Error::MissingPid),
            Some(pid) => Self::convert_request(pid, op, stream),
        }
    }
}
