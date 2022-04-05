pub mod capture;
pub mod flow;
pub mod materialize;

use proto_gazette::consumer;

mod serde_capture {
    use crate::capture::*;
    include!(concat!(env!("OUT_DIR"), "/capture.serde.rs"));
}
mod serde_flow {
    use crate::flow::*;
    include!(concat!(env!("OUT_DIR"), "/flow.serde.rs"));
}
mod serde_materialize {
    use crate::materialize::*;
    include!(concat!(env!("OUT_DIR"), "/materialize.serde.rs"));
}
