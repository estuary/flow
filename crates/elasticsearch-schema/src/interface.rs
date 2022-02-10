use super::elastic_search_data_types::ESTypeOverride;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct Input {
    pub schema_json_base64: String,
    pub overrides: Vec<ESTypeOverride>,
}
