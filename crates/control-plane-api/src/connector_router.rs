//! Routing the control plane's own connector RPCs to the data plane which owns
//! each task, over the reactor's `connector.Connector` proxy.

use anyhow::Context;
use futures::StreamExt;

/// Build an [`proto_grpc::connector::EndpointRouter`] from a [`tables::DataPlane`].
pub fn data_plane_route(
    data_plane: &tables::DataPlane,
) -> anyhow::Result<proto_grpc::connector::EndpointRouter> {
    let (encode_key, _decode) = tokens::jwt::parse_base64_hmac_keys(
        data_plane.hmac_keys.iter().take(1),
    )
    .with_context(|| {
        format!(
            "data-plane {} has no usable HMAC key",
            data_plane.data_plane_name
        )
    })?;

    Ok(proto_grpc::connector::EndpointRouter::new(
        data_plane.reactor_address.clone(),
        proto_grpc::Signer::new(data_plane.data_plane_fqdn.clone(), encode_key),
    ))
}

/// Routes each built derivation to the data plane which owns it.
pub struct DataPlaneRouter {
    /// One route for every data plane in the build, ordered by control ID.
    data_planes: Vec<(models::Id, proto_grpc::connector::EndpointRouter)>,
    /// Built derivation names mapped to their data-plane IDs, ordered by name.
    catalog_names: Vec<(String, models::Id)>,
}

impl DataPlaneRouter {
    /// Resolve a route for every built derivation, including disabled ones.
    pub fn new(output: &build::Output) -> anyhow::Result<Self> {
        // `tables::DataPlanes` is ordered by control ID, so this remains sorted
        // without a second indexing structure and builds exactly one router per
        // row. The parallel catalog-name multimap below refers into it by ID.
        let data_planes = output
            .live
            .data_planes
            .iter()
            .map(|data_plane| Ok((data_plane.control_id, data_plane_route(data_plane)?)))
            .collect::<anyhow::Result<Vec<_>>>()?;
        let mut catalog_names = Vec::new();

        for row in output.built.built_collections.iter() {
            let Some(_derivation) = row.spec.as_ref().and_then(|spec| spec.derivation.as_ref())
            else {
                continue;
            };
            let plane_index = data_planes
                .binary_search_by_key(&row.data_plane_id, |(control_id, _route)| *control_id)
                .map_err(|_| {
                    anyhow::anyhow!(
                        "derivation {} is assigned to data-plane {}, which isn't in the build",
                        row.collection,
                        row.data_plane_id,
                    )
                })?;

            let (data_plane_id, route) = &data_planes[plane_index];
            let data_plane = output
                .live
                .data_planes
                .get_key(data_plane_id)
                .expect("a route was built from this data plane");
            tracing::debug!(
                derivation = %row.collection,
                data_plane = %data_plane.data_plane_name,
                reactor_address = %route.endpoint(),
                "routing a derivation's connector to its data plane",
            );
            catalog_names.push((row.collection.to_string(), *data_plane_id));
        }

        // BuiltCollections is already ordered, but keep the representation's
        // invariant local so another catalog-name source can be merged later.
        catalog_names.sort_by(|(lhs, _), (rhs, _)| lhs.cmp(rhs));

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
            .binary_search_by(|(name, _data_plane_id)| name.as_str().cmp(task_name))
            .map_err(|_| {
                tonic::Status::not_found(format!("no data plane route for derivation {task_name}"))
            })?;
        let data_plane_id = self.catalog_names[name_index].1;
        let plane_index = self
            .data_planes
            .binary_search_by_key(&data_plane_id, |(control_id, _route)| *control_id)
            .expect("catalog-name routes reference known data planes");

        Ok(&self.data_planes[plane_index].1)
    }
}

impl proto_grpc::connector::Router for DataPlaneRouter {
    fn open(
        &self,
        task_type: ops::TaskType,
        task_name: &str,
        request_rx: tokio::sync::mpsc::Receiver<proto_flow::connector::Request>,
    ) -> futures::stream::BoxStream<'static, tonic::Result<proto_flow::connector::Response>> {
        match self.route_for(task_type, task_name) {
            Ok(router) => {
                proto_grpc::connector::Router::open(router, task_type, task_name, request_rx)
            }
            Err(status) => futures::stream::once(async { Err(status) }).boxed(),
        }
    }
}

#[cfg(test)]
mod tests {
    /// Some router results are not `Debug`, so `unwrap_err` is unavailable.
    fn err_of<T>(result: Result<T, impl Into<anyhow::Error>>) -> String {
        match result {
            Ok(_) => panic!("expected an error"),
            Err(err) => format!("{:#}", err.into()),
        }
    }

    fn status_of<T>(result: tonic::Result<T>) -> tonic::Code {
        match result {
            Ok(_) => panic!("expected a Status"),
            Err(status) => status.code(),
        }
    }

    const KEY: &[u8] = b"a data-plane HMAC key";
    const FQDN: &str = "acme.dp.estuary-data.com";

    fn data_plane() -> tables::DataPlane {
        use base64::Engine;

        let mut planes = tables::DataPlanes::default();
        planes.insert_row(
            models::Id::zero(),
            "ops/dp/public/acme".to_string(),
            FQDN.to_string(),
            false, // closed
            vec![base64::engine::general_purpose::STANDARD.encode(KEY)],
            models::RawValue::from_string("{}".to_string()).unwrap(),
            models::Collection::new("ops/logs"),
            models::Collection::new("ops/stats"),
            "broker.acme:8080".to_string(),
            "https://reactor.acme:8080".to_string(),
            None,
            None,
        );
        planes.into_iter().next().unwrap()
    }

    fn insert_derivation(output: &mut build::Output, name: &str, data_plane_id: models::Id) {
        output.built.built_collections.insert_row(
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
                derivation: Some(Box::new(Default::default())),
                ..Default::default()
            }),
            None::<proto_flow::flow::CollectionSpec>,
            false,
            None::<String>,
        );
    }

    /// A plane's route dials its reactor address.
    #[test]
    fn a_data_plane_route_uses_its_reactor_address() {
        let route = super::data_plane_route(&data_plane()).unwrap();

        assert_eq!(route.endpoint(), "https://reactor.acme:8080");
    }

    /// A plane with no parseable key fails while building the route, naming it.
    #[test]
    fn a_plane_without_a_usable_key_is_an_error() {
        let mut plane = data_plane();
        plane.hmac_keys = vec!["not base64!".to_string()];

        let err = err_of(super::data_plane_route(&plane));
        assert!(
            err.contains("ops/dp/public/acme") && err.contains("no usable HMAC key"),
            "got: {err}",
        );
    }

    /// The router answers only for derivations it was built with.
    #[test]
    fn unknown_names_and_task_types_are_refused() {
        let router = super::DataPlaneRouter {
            data_planes: vec![(
                models::Id::zero(),
                super::data_plane_route(&data_plane()).unwrap(),
            )],
            catalog_names: vec![("acmeCo/derivation".to_string(), models::Id::zero())],
        };

        router
            .route_for(ops::TaskType::Derivation, "acmeCo/derivation")
            .unwrap();

        assert_eq!(
            status_of(router.route_for(ops::TaskType::Derivation, "acmeCo/other")),
            tonic::Code::NotFound,
        );
        assert_eq!(
            status_of(router.route_for(ops::TaskType::Capture, "acmeCo/derivation")),
            tonic::Code::InvalidArgument,
        );
    }

    /// A built derivation whose plane isn't in the build fails `new`, naming
    /// the collection — before any reactor is contacted.
    #[test]
    fn a_missing_data_plane_fails_the_router() {
        let mut output = build::Output::default();
        insert_derivation(
            &mut output,
            "acmeCo/derivation",
            models::Id::new([9; 8]), // Absent from `live`.
        );

        let err = err_of(super::DataPlaneRouter::new(&output));
        assert!(
            err.contains("acmeCo/derivation") && err.contains("isn't in the build"),
            "got: {err}",
        );
    }

    /// Each plane owns one EndpointRouter, while the independently sorted name
    /// map selects the correct plane for derivations in a unified namespace.
    #[test]
    fn catalog_names_route_across_multiple_data_planes() {
        use base64::Engine;

        const OTHER_KEY: &[u8] = b"another data-plane HMAC key";
        const OTHER_FQDN: &str = "other.dp.estuary-data.com";
        let other_id = models::Id::new([2; 8]);

        let mut output = build::Output::default();
        output.live.data_planes.insert(data_plane());

        let mut other = data_plane();
        other.control_id = other_id;
        other.data_plane_name = "ops/dp/public/other".to_string();
        other.data_plane_fqdn = OTHER_FQDN.to_string();
        other.reactor_address = "https://reactor.other:8080".to_string();
        other.hmac_keys = vec![base64::engine::general_purpose::STANDARD.encode(OTHER_KEY)];
        output.live.data_planes.insert(other);

        // Insert opposite to lexical order to exercise the name-map invariant.
        insert_derivation(&mut output, "acmeCo/z-on-first", models::Id::zero());
        insert_derivation(&mut output, "acmeCo/a-on-other", other_id);

        let router = super::DataPlaneRouter::new(&output).unwrap();
        assert_eq!(router.data_planes.len(), 2);
        assert_eq!(
            router.catalog_names,
            vec![
                ("acmeCo/a-on-other".to_string(), other_id),
                ("acmeCo/z-on-first".to_string(), models::Id::zero()),
            ]
        );

        for (name, endpoint) in [
            ("acmeCo/a-on-other", "https://reactor.other:8080"),
            ("acmeCo/z-on-first", "https://reactor.acme:8080"),
        ] {
            let route = router.route_for(ops::TaskType::Derivation, name).unwrap();
            assert_eq!(route.endpoint(), endpoint);
        }
    }
}
