//! Connector routing used specifically while running publication catalog tests.

use tokio::sync::mpsc;

/// Routes enabled built derivations to their data planes.
pub(super) struct CatalogTestConnectorRouter {
    data_planes: Vec<(models::Id, proto_grpc::connector::EndpointRouter)>,
    catalog_names: Vec<(String, models::Id)>,
}

impl CatalogTestConnectorRouter {
    pub(super) fn new(output: &build::Output) -> anyhow::Result<Self> {
        // Extract all enabled derivations and each one's data-plane ID.
        let mut catalog_names = output
            .built
            .built_collections
            .iter()
            .filter_map(|row| {
                let derivation = row.spec.as_ref()?.derivation.as_ref()?;
                let enabled = !derivation
                    .shard_template
                    .as_ref()
                    .is_some_and(|shard| shard.disable);
                enabled.then(|| (row.collection.to_string(), row.data_plane_id))
            })
            .collect::<Vec<_>>();
        catalog_names.sort_by(|(lhs, _), (rhs, _)| lhs.cmp(rhs));

        let mut data_plane_ids = catalog_names
            .iter()
            .map(|(_, data_plane_id)| *data_plane_id)
            .collect::<Vec<_>>();
        data_plane_ids.sort();
        data_plane_ids.dedup();

        // Build an EndpointRouter for each distinct data-plane.
        let data_planes = data_plane_ids
            .into_iter()
            .map(|data_plane_id| {
                let data_plane = output
                    .live
                    .data_planes
                    .get_key(&data_plane_id)
                    .ok_or_else(|| {
                        let (collection, _) = catalog_names
                            .iter()
                            .find(|(_, id)| *id == data_plane_id)
                            .expect("every ID was taken from a derivation");

                        anyhow::anyhow!(
                            "derivation {collection} is assigned to data-plane {data_plane_id}, which isn't in the build",
                        )
                    })?;

                Ok((
                    data_plane_id,
                    crate::data_plane::build_connector_route(data_plane)?,
                ))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;

        Ok(Self {
            data_planes,
            catalog_names,
        })
    }

    fn route_for(
        &self,
        task_type: ops::TaskType,
        task_name: &str,
    ) -> tonic::Result<&proto_grpc::connector::EndpointRouter> {
        if task_type != ops::TaskType::Derivation {
            return Err(tonic::Status::invalid_argument(format!(
                "catalog tests route derivations only, not {} {task_name}",
                task_type.as_str_name(),
            )));
        }
        let name_index = self
            .catalog_names
            .binary_search_by(|(name, _)| name.as_str().cmp(task_name))
            .map_err(|_| {
                tonic::Status::not_found(format!(
                    "no data plane route for enabled derivation {task_name}"
                ))
            })?;
        let data_plane_id = self.catalog_names[name_index].1;
        let plane_index = self
            .data_planes
            .binary_search_by_key(&data_plane_id, |(id, _)| *id)
            .expect("catalog-name routes reference known data planes");

        Ok(&self.data_planes[plane_index].1)
    }
}

impl proto_grpc::connector::Router for CatalogTestConnectorRouter {
    fn open(
        &self,
        task_type: ops::TaskType,
        task_name: &str,
        request_rx: mpsc::Receiver<proto_flow::connector::Request>,
    ) -> mpsc::Receiver<tonic::Result<proto_flow::connector::Response>> {
        match self.route_for(task_type, task_name) {
            Ok(router) => {
                proto_grpc::connector::Router::open(router, task_type, task_name, request_rx)
            }
            Err(status) => {
                let (response_tx, response_rx) = mpsc::channel(proto_grpc::CHANNEL_BUFFER);
                response_tx
                    .try_send(Err(status))
                    .expect("new response channel is open and empty");
                response_rx
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn insert_derivation(
        built: &mut tables::BuiltCollections,
        name: &str,
        data_plane_id: models::Id,
        disabled: bool,
    ) {
        let mut derivation = proto_flow::flow::collection_spec::Derivation::default();
        derivation.shard_template = Some(proto_gazette::consumer::ShardSpec {
            disable: disabled,
            ..Default::default()
        });
        built.insert_row(
            models::Collection::new(name),
            url::Url::parse("flow://test").unwrap(),
            models::Id::zero(),
            data_plane_id,
            models::Id::zero(),
            models::Id::zero(),
            None::<models::CollectionDef>,
            Vec::<String>::new(),
            None::<proto_flow::derive::response::Validated>,
            Some(proto_flow::flow::CollectionSpec {
                name: name.to_string(),
                derivation: Some(Box::new(derivation)),
                ..Default::default()
            }),
            None::<proto_flow::flow::CollectionSpec>,
            false,
            None::<String>,
        );
    }

    fn insert_data_plane(output: &mut build::Output, control_id: models::Id, suffix: &str) {
        use base64::Engine;

        output.live.data_planes.insert_row(
            control_id,
            format!("ops/dp/public/{suffix}"),
            format!("{suffix}.dp.estuary-data.com"),
            false, // closed
            vec![base64::engine::general_purpose::STANDARD.encode(b"an HMAC key")],
            models::RawValue::from_string("{}".to_string()).unwrap(),
            models::Collection::new("ops/logs"),
            models::Collection::new("ops/stats"),
            format!("broker.{suffix}:8080"),
            format!("https://reactor.{suffix}:8080"),
            None,
            None,
        );
    }

    #[test]
    fn routes_enabled_derivations_only() {
        let mut output = build::Output::default();
        insert_data_plane(&mut output, models::Id::new([1; 8]), "acme");
        insert_derivation(
            &mut output.built.built_collections,
            "acmeCo/enabled",
            models::Id::new([1; 8]),
            false,
        );
        insert_derivation(
            &mut output.built.built_collections,
            "acmeCo/disabled",
            models::Id::new([9; 8]), // Absent from `live`, and never routed.
            true,
        );

        let router = CatalogTestConnectorRouter::new(&output).unwrap();
        assert_eq!(router.data_planes.len(), 1);
        assert_eq!(router.catalog_names.len(), 1);
        assert_eq!(
            router
                .route_for(ops::TaskType::Derivation, "acmeCo/enabled")
                .unwrap()
                .endpoint(),
            "https://reactor.acme:8080"
        );
        let status = match router.route_for(ops::TaskType::Derivation, "acmeCo/disabled") {
            Ok(_) => panic!("disabled derivation unexpectedly has a route"),
            Err(status) => status,
        };
        assert_eq!(status.code(), tonic::Code::NotFound);
    }

    /// An enabled derivation whose plane isn't in the build fails `new`, naming
    /// the derivation — before any reactor is contacted.
    #[test]
    fn a_missing_data_plane_fails_the_router() {
        let mut output = build::Output::default();
        insert_derivation(
            &mut output.built.built_collections,
            "acmeCo/derivation",
            models::Id::new([9; 8]), // Absent from `live`.
            false,
        );

        // `Self` isn't `Debug`, so `unwrap_err` is unavailable.
        let err = match CatalogTestConnectorRouter::new(&output) {
            Ok(_) => panic!("expected a missing data plane to fail the router"),
            Err(err) => format!("{err:#}"),
        };
        assert!(
            err.contains("acmeCo/derivation") && err.contains("isn't in the build"),
            "got: {err}",
        );
    }

    /// `open` has no way to refuse a session other than the response channel,
    /// so a routing failure must arrive there as a `Status` — not a panic, and
    /// not a silently empty stream which would read as a clean connector EOF.
    #[test]
    fn open_surfaces_routing_failures_on_the_response_channel() {
        use proto_grpc::connector::Router;

        let router = CatalogTestConnectorRouter::new(&build::Output::default()).unwrap();

        for (task_type, task_name, expect) in [
            (
                ops::TaskType::Derivation,
                "acmeCo/not-in-the-build",
                tonic::Code::NotFound,
            ),
            (
                ops::TaskType::Capture,
                "acmeCo/capture",
                tonic::Code::InvalidArgument,
            ),
        ] {
            let (_request_tx, request_rx) = mpsc::channel(proto_grpc::CHANNEL_BUFFER);
            let mut response_rx = router.open(task_type, task_name, request_rx);

            let status = match response_rx.try_recv() {
                Ok(Ok(response)) => panic!("expected a Status, got {response:?}"),
                Ok(Err(status)) => status,
                Err(err) => panic!("expected a Status, got {err:?}"),
            };
            assert_eq!(status.code(), expect, "for {task_name}");
            assert!(
                status.message().contains(task_name),
                "the Status should name the task, got: {}",
                status.message(),
            );
        }
    }
}
