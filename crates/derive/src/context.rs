use super::lambda;
use super::nodejs;

use protocol::flow;
use std::sync::Arc;
use url::Url;

pub struct Transform {
    // TODO: remove after moving validation to shuffle API.
    pub source_schema: Url,
    // Prepared lambdas for invocation.
    pub update: lambda::Lambda,
    pub publish: lambda::Lambda,
    // Index of this Transform within its owning array.
    // This makes it easy to map back to an index from a &Transform.
    pub index: usize,
}

pub struct Context {
    pub derivation_key: Arc<[doc::Pointer]>,
    pub derivation_partitions: Vec<doc::Pointer>,
    pub derivation_schema: Url,
    pub register_initial: serde_json::Value,
    pub register_schema: Url,
    pub schema_index: &'static doc::SchemaIndex<'static>,
    pub transforms: Vec<Transform>,
    pub uuid_placeholder_ptr: String,
}

impl Context {
    pub fn build_from_spec(
        derivation: flow::DerivationSpec,
        node: &nodejs::NodeRuntime,
        schema_index: &'static doc::SchemaIndex<'static>,
    ) -> Result<Context, anyhow::Error> {
        let flow::DerivationSpec {
            collection,
            transforms: transform_specs,
            register_initial_json,
            register_schema_uri,
        } = derivation;

        let flow::CollectionSpec {
            projections,
            partition_fields,
            key_ptrs,
            schema_uri,
            uuid_ptr: uuid_placeholder_ptr,
            ..
        } = collection.unwrap_or_default();

        let prepare_lambda = |lambda: &Option<flow::LambdaSpec>| {
            Ok(match lambda {
                None => lambda::Lambda::Noop,
                Some(l) if !l.typescript.is_empty() => node.new_lambda(&l.typescript),
                Some(l) if !l.remote.is_empty() => {
                    lambda::Lambda::new_web_json(Url::parse(&l.remote)?)
                }
                _ => anyhow::bail!("lambda {:?} doesn't have a supported runtime", lambda),
            })
        };

        // Take TransformSpecs from the DerivationSpec, and collect as prepared Transforms.
        let mut transforms = Vec::new();
        for (
            index,
            flow::TransformSpec {
                update_lambda,
                publish_lambda,
                shuffle,
                ..
            },
        ) in transform_specs.into_iter().enumerate()
        {
            let flow::Shuffle {
                source_schema_uri, ..
            } = shuffle.unwrap_or_default();

            let update = prepare_lambda(&update_lambda)?;
            let publish = prepare_lambda(&publish_lambda)?;

            transforms.push(Transform {
                index,
                publish,
                source_schema: Url::parse(&source_schema_uri)?,
                update,
            });
        }

        let mut derivation_partitions = Vec::new();
        for field in partition_fields {
            let ptr = projections.iter().find_map(|proj| {
                if proj.field == field {
                    Some(proj.ptr.as_str())
                } else {
                    None
                }
            });
            anyhow::ensure!(
                ptr.is_some(),
                "malformed spec: partition projection not found"
            );
            derivation_partitions.push(doc::Pointer::from_str(ptr.unwrap()));
        }

        Ok(Context {
            derivation_key: key_ptrs
                .iter()
                .map(|k| doc::Pointer::from_str(k))
                .collect::<Vec<_>>()
                .into(),
            derivation_partitions,
            uuid_placeholder_ptr,
            derivation_schema: Url::parse(&schema_uri)?,
            register_initial: serde_json::from_str(&register_initial_json)?,
            register_schema: Url::parse(&register_schema_uri)?,
            schema_index,
            transforms,
        })
    }
}
