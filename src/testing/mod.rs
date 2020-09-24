use crate::catalog::specs;
use crate::runtime::{self, cluster};

use std::collections::BTreeMap;

type _Offsets = BTreeMap<String, u64>;

pub async fn run_test_case(
    cluster: &runtime::Cluster,
    steps: Vec<specs::TestStep>,
) -> Result<(), cluster::Error> {
    for step in steps {
        match step {
            specs::TestStep::Ingest(ingest) => {
                let body = serde_json::json!({
                    ingest.collection:  ingest.documents,
                });

                let offsets = cluster.ingest(body).await?;
                log::info!("ingest returned offsets {:?}", offsets);
            }
            _ => log::info!("skipping test step {:?}", step),
        }
    }
    Ok(())
}
