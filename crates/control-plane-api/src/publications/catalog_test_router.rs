//! Connector routing used specifically while running publication catalog tests.

use tokio::sync::mpsc;

/// Routes enabled built derivations to their data planes.
pub(super) struct CatalogTestConnectorRouter {
    data_planes: Vec<(models::Id, proto_grpc::connector::EndpointRouter)>,
    catalog_names: Vec<(String, models::Id)>,
}

impl CatalogTestConnectorRouter {
    pub(super) fn new(
        snapshot: &crate::Snapshot,
        built_collections: &tables::BuiltCollections,
    ) -> anyhow::Result<Self> {
        let mut catalog_names = built_collections
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

        let data_planes = data_plane_ids
            .into_iter()
            .map(|data_plane_id| {
                snapshot
                    .data_plane_connector_route(data_plane_id)
                    .map(|route| (data_plane_id, route))
                    .map_err(anyhow::Error::from)
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

    #[test]
    fn routes_enabled_derivations_only() {
        let snapshot = crate::Snapshot::build_fixture(None);
        let mut built = tables::BuiltCollections::new();
        insert_derivation(&mut built, "acmeCo/enabled", models::Id::new([1; 8]), false);
        insert_derivation(&mut built, "acmeCo/disabled", models::Id::new([9; 8]), true);

        let router = CatalogTestConnectorRouter::new(&snapshot, &built).unwrap();
        assert_eq!(router.data_planes.len(), 1);
        assert_eq!(router.catalog_names.len(), 1);
        assert_eq!(
            router
                .route_for(ops::TaskType::Derivation, "acmeCo/enabled")
                .unwrap()
                .endpoint(),
            "reactor.1"
        );
        let status = match router.route_for(ops::TaskType::Derivation, "acmeCo/disabled") {
            Ok(_) => panic!("disabled derivation unexpectedly has a route"),
            Err(status) => status,
        };
        assert_eq!(status.code(), tonic::Code::NotFound);
    }
}
