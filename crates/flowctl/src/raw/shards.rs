use crate::{ops::TaskSelector, CliContext};

pub async fn do_list_shards(ctx: &mut CliContext, selector: &TaskSelector) -> anyhow::Result<()> {
    let models::authorizations::UserTaskAuthorization {
        reactor_address,
        reactor_token,
        shard_id_prefix,
        ..
    } = flow_client::fetch_user_task_authorization(
        &ctx.client,
        models::authorizations::UserTaskAuthorizationRequest {
            task: selector.task.clone(),
            capability: models::Capability::Read,
            started_unix: 0,
        },
    )
    .await?;

    let shard_client = gazette::journal::Client::new(
        reactor_address,
        gazette::Metadata::new().with_bearer_token(&reactor_token)?,
        ctx.router.clone(),
    );

    let req = proto_gazette::consumer::ListRequest {
        selector: Some(proto_gazette::LabelSelector {
            include: Some(proto_gazette::LabelSet {
                labels: vec![proto_gazette::Label {
                    name: "id".to_string(),
                    value: shard_id_prefix.clone(),
                    prefix: true,
                }],
            }),
            ..Default::default()
        }),
        ..Default::default()
    };
    let resp = shard_client.list(req).await?;
    if resp.status != (proto_gazette::consumer::Status::Ok as i32) {
        tracing::warn!(response = ?resp, "failed to list shards");
        return Err(anyhow::anyhow!(
            "shard lising response returned error code: {} ({})",
            resp.status,
            resp.status().as_str_name()
        ));
    }
    let wrapped = resp.shards.into_iter().map(ShardWrapper);
    ctx.write_all(wrapped, ())?;

    Ok(())
}

#[derive(Debug, serde::Serialize)]
#[serde(transparent)]
struct ShardWrapper(proto_gazette::consumer::list_response::Shard);

impl crate::output::CliOutput for ShardWrapper {
    type TableAlt = ();
    type CellValue = String;

    fn table_headers(_alt: Self::TableAlt) -> Vec<&'static str> {
        vec!["ID", "Status", "Primary", "Error"]
    }

    fn into_table_row(self, _alt: Self::TableAlt) -> Vec<Self::CellValue> {
        let id = self
            .0
            .spec
            .as_ref()
            .map(|s| s.id.clone())
            .unwrap_or_default();
        vec![
            id,
            extract_status(&self.0),
            extract_primary(&self.0),
            extract_error(&self.0),
        ]
    }
}

fn extract_primary(shard: &proto_gazette::consumer::list_response::Shard) -> String {
    let Some(route) = shard.route.as_ref() else {
        return "No routes".to_string();
    };
    if route.primary == -1 {
        return "No primary".to_string();
    }
    route
        .members
        .get(route.primary as usize)
        .map(|m| m.suffix.clone())
        .unwrap_or_default()
}

fn extract_status(shard: &proto_gazette::consumer::list_response::Shard) -> String {
    let Some(route) = shard.route.as_ref() else {
        return "No routes".to_string();
    };
    if route.primary == -1 {
        return "No primary".to_string();
    }
    let Some(primary_status) = shard.status.get(route.primary as usize) else {
        return "Missing status for primary route".to_string();
    };
    proto_gazette::consumer::replica_status::Code::try_from(primary_status.code)
        .map(|c| c.as_str_name().to_owned())
        .unwrap_or_else(|_| format!("Unknown status code: {}", primary_status.code))
}

fn extract_error(shard: &proto_gazette::consumer::list_response::Shard) -> String {
    use std::fmt::Write;
    let Some(route) = shard.route.as_ref() else {
        return "No routes".to_string();
    };
    if route.primary == -1 {
        return "No primary".to_string();
    }
    let Some(primary_status) = shard.status.get(route.primary as usize) else {
        return "Missing status for primary route".to_string();
    };

    let mut err = primary_status.errors.get(0).cloned().unwrap_or_default();
    if primary_status.errors.len() > 1 {
        write!(&mut err, " (+{} more)", primary_status.errors.len() - 1).unwrap();
    }
    err
}
