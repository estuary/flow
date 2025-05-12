use models::status::StatusResponse;
use serde::Serialize;

use crate::output::{to_table_row, JsonCell};

#[derive(Debug, clap::Args)]
pub struct Status {
    /// Names of the live specs to fetch the status of
    #[clap(required(true))]
    pub catalog_names: Vec<String>,
}

pub async fn do_controller_status(
    ctx: &mut crate::CliContext,
    Status { catalog_names }: &Status,
) -> anyhow::Result<()> {
    let query = catalog_names
        .iter()
        .map(|name| ("name".to_string(), name.clone()))
        .collect::<Vec<_>>();
    let resp: Vec<StatusResponse> = ctx.client.api_get("/api/v1/catalog/status", &query).await?;
    ctx.write_all::<_, StatusOutput>(resp.into_iter().map(StatusOutput), ())?;
    Ok(())
}

#[derive(Debug, Serialize)]
#[serde(transparent)]
pub struct StatusOutput(StatusResponse);

impl crate::output::CliOutput for StatusOutput {
    type TableAlt = ();
    type CellValue = JsonCell;

    fn table_headers(_alt: Self::TableAlt) -> Vec<&'static str> {
        vec![
            "Name",
            "Type",
            "Controller Updated At",
            "Live Spec Updated At",
            "Controller Error",
            "Failures",
            "Activation Complete",
        ]
    }

    fn into_table_row(self, _alt: Self::TableAlt) -> Vec<Self::CellValue> {
        let mut row = to_table_row(
            &self.0,
            &[
                "/catalog_name",
                "/spec_type",
                "/controller_updated_at",
                "/live_spec_updated_at",
                "/controller_error",
                "/failures",
            ],
        );
        // Activation Complete is a computed column so we need to add it manually.
        let activation_complete = self
            .0
            .controller_status
            .as_ref()
            .and_then(|s| s.activation_status())
            .map(|activation| {
                serde_json::Value::Bool(activation.last_activated == self.0.last_build_id)
            });
        row.push(JsonCell(activation_complete));
        row
    }
}
