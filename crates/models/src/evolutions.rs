use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, JsonSchema)]
pub struct EvolvedCollection {
    /// Original name of the collection
    pub old_name: String,
    /// The new name of the collection, which may be the same as the original name if only materialization bindings were updated
    pub new_name: String,
    /// The names of any materializations that were updated as a result of evolving this collection
    pub updated_materializations: Vec<String>,
    /// The names of any captures that were updated as a result of evolving this collection
    pub updated_captures: Vec<String>,
}
