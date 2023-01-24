use super::{FromMessage, IntoMessages};
use proto_flow::flow;

/// TestSpec is a stub of the flow TestSpec protobuf type,
/// which implements IntoMessages and FromMessage.
/// It's exposed only to facilitate higher-level testing of protocol conversions.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct TestSpec {
    pub test: String,
}

impl IntoMessages for flow::TestSpec {
    type Message = TestSpec;

    fn into_messages(self) -> Vec<Self::Message> {
        let Self { test, steps: _ } = self;

        vec![TestSpec { test }]
    }
}

impl FromMessage for flow::TestSpec {
    type Message = TestSpec;

    fn from_message(TestSpec { test }: TestSpec, out: &mut Vec<Self>) -> anyhow::Result<()> {
        Ok(out.push(Self {
            test,
            steps: Vec::new(),
        }))
    }
}
