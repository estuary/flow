use crate::apis::{
    FlowCaptureOperation, FlowMaterializeOperation, FlowOperation, Interceptor, InterceptorStream,
};
use crate::errors::Error;
use crate::libs::command::resume_process;
use std::marker::PhantomData;

pub struct DefaultInterceptor<T: FlowOperation + std::fmt::Display> {
    pub _type: PhantomData<T>,
}

impl<T: FlowOperation + std::fmt::Display> Interceptor<T> for DefaultInterceptor<T> {
    fn convert_command_args(&mut self, op: &T, args: Vec<String>) -> Result<Vec<String>, Error> {
        let mut new_args = vec![op.to_string()];
        new_args.extend_from_slice(&args);
        Ok(new_args)
    }

    fn convert_request(
        &mut self,
        pid: Option<u32>,
        _op: &T,
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

pub type DefaultFlowMaterializeInterceptor = DefaultInterceptor<FlowMaterializeOperation>;
pub type DefaultFlowCaptureInterceptor = DefaultInterceptor<FlowCaptureOperation>;
