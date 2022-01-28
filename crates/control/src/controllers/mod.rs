pub mod connectors;
pub mod health_check;

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum Payload<D> {
    Data(D),
    Error(String),
}
