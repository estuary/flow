use crate::apis::{FlowCaptureOperation, FlowMaterializeOperation, Interceptor};
pub struct DefaultFlowCaptureInterceptor {}
impl Interceptor<FlowCaptureOperation> for DefaultFlowCaptureInterceptor {}

pub struct DefaultFlowMaterializeInterceptor {}
impl Interceptor<FlowMaterializeOperation> for DefaultFlowMaterializeInterceptor {}
