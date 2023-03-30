use super::{Connectors, ControlPlane};
use futures::future::{FutureExt, LocalBoxFuture};
use proto_flow::{capture, derive, materialize};
use std::collections::BTreeMap;

/// NoOpDrivers are permissive placeholders for interaction with connectors,
/// that do not fail and return the right shape of response.
pub struct NoOpDrivers;

impl Connectors for NoOpDrivers {
    fn validate_capture<'a>(
        &'a self,
        request: capture::request::Validate,
    ) -> LocalBoxFuture<'a, Result<capture::response::Validated, anyhow::Error>> {
        use capture::response::{validated::Binding, Validated};

        Box::pin(async move {
            let bindings = request
                .bindings
                .into_iter()
                .enumerate()
                .map(|(i, _)| Binding {
                    resource_path: vec![format!("binding-{}", i)],
                })
                .collect::<Vec<_>>();
            Ok(Validated { bindings })
        })
    }

    fn validate_derivation<'a>(
        &'a self,
        request: derive::request::Validate,
    ) -> LocalBoxFuture<'a, Result<derive::response::Validated, anyhow::Error>> {
        use derive::response::{validated::Transform, Validated};

        Box::pin(async move {
            let transforms = request
                .transforms
                .into_iter()
                .map(|_| Transform { read_only: false })
                .collect::<Vec<_>>();
            Ok(Validated {
                transforms,
                generated_files: BTreeMap::new(),
            })
        })
    }

    fn validate_materialization<'a>(
        &'a self,
        request: materialize::request::Validate,
    ) -> LocalBoxFuture<'a, Result<materialize::response::Validated, anyhow::Error>> {
        use materialize::response::{
            validated::{constraint::Type, Binding, Constraint},
            Validated,
        };

        Box::pin(async move {
            let response_bindings = request
                .bindings
                .into_iter()
                .enumerate()
                .map(|(i, b)| {
                    let resource_path = vec![format!("binding-{}", i)];
                    let constraints = b
                        .collection
                        .expect("collection must exist")
                        .projections
                        .into_iter()
                        .map(|proj| {
                            (
                                proj.field,
                                Constraint {
                                    r#type: Type::FieldOptional as i32,
                                    reason: "no-op validator allows everything".to_string(),
                                },
                            )
                        })
                        .collect::<BTreeMap<_, _>>();
                    Binding {
                        constraints,
                        resource_path,
                        delta_updates: true,
                    }
                })
                .collect::<Vec<_>>();
            Ok(Validated {
                bindings: response_bindings,
            })
        })
    }

    fn inspect_image<'a>(
        &'a self,
        _image: String,
    ) -> LocalBoxFuture<'a, Result<Vec<u8>, anyhow::Error>> {
        // Just return a constant value that matches the basic shape of the `docker inspect` output.
        // The `source` property is just to make it obvious where this came from if looking at a build db.
        Box::pin(async move {
            Ok(r#"[{"Config": {},"source": "flow no-op driver"}]"#.as_bytes().to_vec())
        })
    }
}

pub struct NoOpControlPlane;

impl ControlPlane for NoOpControlPlane {
    fn resolve_collections<'a, 'b: 'a>(
        &'a self,
        _collections: Vec<models::Collection>,
        _temp_build_config: &'b proto_flow::flow::build_api::Config,
        _temp_storage_mappings: &'b [tables::StorageMapping],
    ) -> LocalBoxFuture<'a, anyhow::Result<Vec<proto_flow::flow::CollectionSpec>>> {
        async move { Ok(vec![]) }.boxed()
    }
}
