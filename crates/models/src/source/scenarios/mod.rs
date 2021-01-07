use super::{
    specs, CaptureName, CollectionName, ContentType, EndpointName, EndpointType, FetchResult,
    JsonPointer, LoadError, Loader, MaterializationName, Scope, ShuffleHash, TestName,
    TransformName, Visitor,
};

use futures::channel::oneshot;
use futures::future::FutureExt;
use serde_json::{json, Value};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::task::Poll;
use url::Url;

macro_rules! file_tests {
    ($($name:ident,)*) => {
    $(
        #[test]
        fn $name() {
			let fixture = include_bytes!(concat!(stringify!($name), ".yaml"));
			let output = evaluate_fixtures(VecVisitor(Vec::new()), fixture);
			insta::assert_yaml_snapshot!(output.unwrap().0);
        }
    )*
    }
}

file_tests! {
    test_catalog_import_cycles,
    test_collections,
    test_derivations,
    test_schema_with_inline,
    test_schema_with_nested_ids,
    test_schema_with_references,
    test_simple_catalog,
    test_test_case,
    test_endpoints_captures_materializations,
}

pub fn evaluate_fixtures<V: Visitor>(visitor: V, fixtures: &[u8]) -> Result<V, V::Error> {
    let fixtures: serde_json::Map<String, Value> =
        serde_yaml::from_slice(fixtures).expect("fixtures must be a valid YAML object");

    // Fetches holds started fetches since the last future poll.
    // Use an ordered map so that we signal one-shots in a stable order,
    // making snapshots reliable.
    let fetches: RefCell<BTreeMap<String, oneshot::Sender<FetchResult>>> =
        RefCell::new(BTreeMap::new());

    // Fetch function which queues oneshot futures for started fetches.
    let fetch = |url: &Url| {
        let (tx, rx) = oneshot::channel();
        if let Some(_) = fetches.borrow_mut().insert(url.to_string(), tx) {
            panic!("url {} has already been fetched", url);
        }
        rx.map(|r| r.unwrap())
    };

    let loader = Loader::new(visitor, fetch);

    let root = Url::parse("test://root").unwrap();
    let mut fut = loader
        .load_resource(Scope::new(&root), &root, ContentType::CatalogSpec)
        .boxed_local();

    let waker = futures::task::noop_waker();
    let mut ctx = std::task::Context::from_waker(&waker);

    loop {
        match fut.poll_unpin(&mut ctx) {
            Poll::Ready(Ok(())) => {
                std::mem::forget(fut);
                return Ok(loader.into_visitor());
            }
            Poll::Ready(Err(err)) => return Err(err),
            Poll::Pending if fetches.borrow().is_empty() => {
                panic!("future is pending, but started no fetches")
            }
            Poll::Pending => {
                for (url, tx) in fetches.borrow_mut().split_off("") {
                    match fixtures.get(&url) {
                        Some(value) => tx.send(Ok(value.to_string().as_bytes().into())),
                        None => tx.send(Err("fixture not found".into())),
                    }
                    .unwrap();
                }
            }
        }
    }
}

pub struct VecVisitor(Vec<Value>);

impl Visitor for VecVisitor {
    type Error = std::convert::Infallible;

    fn visit_fetch<'a>(&mut self, scope: Scope<'a>, resource: &Url) -> Result<(), Self::Error> {
        self.0.push(json!({
            "fetch": {
                "scope": scope.flatten().as_str(),
                "resource": resource.as_str(),
            }
        }));
        Ok(())
    }

    fn visit_catalog<'a>(&mut self, scope: Scope<'a>) -> Result<(), Self::Error> {
        self.0.push(json!({
            "catalog": {
                "scope": scope.flatten().as_str(),
            }
        }));
        Ok(())
    }

    fn visit_schema_document<'a>(
        &mut self,
        scope: Scope<'a>,
        _dom: &serde_json::Value,
    ) -> Result<(), Self::Error> {
        self.0.push(json!({
            "schema": {
                "scope": scope.flatten().as_str(),
            }
        }));
        Ok(())
    }

    fn visit_import<'a>(
        &mut self,
        scope: Scope<'a>,
        parent_uri: &Url,
        child_uri: &Url,
    ) -> Result<(), Self::Error> {
        self.0.push(json!({
            "import": {
                "scope": scope.flatten().as_str(),
                "parent_uri": parent_uri.as_str(),
                "child_uri": child_uri.as_str(),
            }
        }));
        Ok(())
    }

    fn visit_resource<'a>(
        &mut self,
        scope: Scope<'a>,
        resource: &Url,
        content_type: ContentType,
        _content: &[u8],
    ) -> Result<(), Self::Error> {
        self.0.push(json!({
            "resource": {
                "scope": scope.flatten().as_str(),
                "resource": resource.as_str(),
                "content_type": content_type.as_str(),
            }
        }));
        Ok(())
    }

    fn visit_nodejs_dependency<'a>(
        &mut self,
        scope: Scope<'a>,
        package: &str,
        version: &str,
    ) -> Result<(), Self::Error> {
        self.0.push(json!({
            "nodejs_dependency": {
                "scope": scope.flatten().as_str(),
                "package": package,
                "version": version,
            }
        }));
        Ok(())
    }

    fn visit_collection<'a>(
        &mut self,
        scope: Scope<'a>,
        name: &CollectionName,
        schema: &Url,
        key: &specs::CompositeKey,
        store: &EndpointName,
        patch_config: &serde_json::Value,
    ) -> Result<(), Self::Error> {
        self.0.push(json!({
            "collection": {
                "scope": scope.flatten().as_str(),
                "name": name,
                "schema": schema.as_str(),
                "key": key,
                "store": store,
                "patch_config": patch_config,
            }
        }));
        Ok(())
    }

    fn visit_projection<'a>(
        &mut self,
        scope: Scope<'a>,
        collection: &CollectionName,
        field: &str,
        location: &JsonPointer,
        partition: bool,
        user_provided: bool,
    ) -> Result<(), Self::Error> {
        self.0.push(json!({
            "projection": {
                "scope": scope.flatten().as_str(),
                "collection": collection,
                "field": field,
                "location": location,
                "partition": partition,
                "user_provided": user_provided,
            }
        }));
        Ok(())
    }

    fn visit_derivation<'a>(
        &mut self,
        scope: Scope<'a>,
        derivation: &CollectionName,
        register_schema: &Url,
        register_initial: &serde_json::Value,
    ) -> Result<(), Self::Error> {
        self.0.push(json!({
            "derivation": {
                "scope": scope.flatten().as_str(),
                "derivation": derivation,
                "register_schema": register_schema.as_str(),
                "register_initial": register_initial,
            }
        }));
        Ok(())
    }

    fn visit_transform<'a>(
        &mut self,
        scope: Scope<'a>,
        transform: &TransformName,
        derivation: &CollectionName,
        source: &CollectionName,
        source_partitions: Option<&specs::PartitionSelector>,
        source_schema: Option<&Url>,
        shuffle_key: Option<&specs::CompositeKey>,
        shuffle_lambda: Option<&specs::Lambda>,
        shuffle_hash: ShuffleHash,
        read_delay: Option<std::time::Duration>,
        update: Option<&specs::Lambda>,
        publish: Option<&specs::Lambda>,
    ) -> Result<(), Self::Error> {
        self.0.push(json!({
            "transform": {
                "scope": scope.flatten().as_str(),
                "transform": transform,
                "derivation": derivation,
                "source": source,
                "source_partitions": source_partitions,
                "source_schema": source_schema.map(|u| u.as_str()),
                "shuffle_key": shuffle_key,
                "shuffle_lambda": shuffle_lambda,
                "shuffle_hash": shuffle_hash as i32,
                "read_delay": read_delay.map(|d| d.as_secs()),
                "update": update,
                "publish": publish,
            }
        }));
        Ok(())
    }

    fn visit_endpoint<'a>(
        &mut self,
        scope: Scope<'a>,
        endpoint: &EndpointName,
        endpoint_type: EndpointType,
        base_config: &serde_json::Value,
    ) -> Result<(), Self::Error> {
        self.0.push(json!({
            "endpoint": {
                "scope": scope.flatten().as_str(),
                "endpoint": endpoint,
                "endpoint_type": endpoint_type.as_str(),
                "base_config": base_config,
            }
        }));
        Ok(())
    }

    fn visit_materialization<'a>(
        &mut self,
        scope: Scope<'a>,
        materialization: &MaterializationName,
        source: &CollectionName,
        source_schema: Option<&Url>,
        endpoint: &EndpointName,
        patch_config: &serde_json::Value,
        fields: &specs::FieldSelector,
    ) -> Result<(), Self::Error> {
        self.0.push(json!({
            "materialization": {
                "scope": scope.flatten().as_str(),
                "materialization": materialization,
                "source": source,
                "source_schema": source_schema.map(|u| u.as_str()),
                "endpoint": endpoint,
                "patch_config": patch_config,
                "fields": fields,
            }
        }));
        Ok(())
    }

    fn visit_capture<'a>(
        &mut self,
        scope: Scope<'a>,
        capture: &CaptureName,
        target: &CollectionName,
        allow_push: bool,
        endpoint: Option<&EndpointName>,
        patch_config: Option<&serde_json::Value>,
    ) -> Result<(), Self::Error> {
        self.0.push(json!({
            "capture": {
                "scope": scope.flatten().as_str(),
                "capture": capture,
                "target": target,
                "allow_push": allow_push,
                "endpoint": endpoint,
                "patch_config": patch_config,
            }
        }));
        Ok(())
    }

    fn visit_test_step_ingest<'a>(
        &mut self,
        scope: Scope<'a>,
        test: &TestName,
        step_index: usize,
        collection: &CollectionName,
        documents: &[serde_json::Value],
    ) -> Result<(), Self::Error> {
        self.0.push(json!({
            "test_step_ingest": {
                "scope": scope.flatten().as_str(),
                "test": test,
                "step_index": step_index,
                "collection": collection,
                "documents": documents,
            }
        }));
        Ok(())
    }

    fn visit_test_step_verify<'a>(
        &mut self,
        scope: Scope<'a>,
        test: &TestName,
        step_index: usize,
        collection: &CollectionName,
        documents: &[serde_json::Value],
        partitions: Option<&specs::PartitionSelector>,
    ) -> Result<(), Self::Error> {
        self.0.push(json!({
            "test_step_verify": {
                "scope": scope.flatten().as_str(),
                "test": test,
                "step_index": step_index,
                "collection": collection,
                "documents": documents,
                "partitions": partitions,
            }
        }));
        Ok(())
    }

    fn visit_test<'a>(
        &mut self,
        scope: Scope<'a>,
        test: &TestName,
        total_steps: usize,
    ) -> Result<(), Self::Error> {
        self.0.push(json!({
            "test": {
                "scope": scope.flatten().as_str(),
                "test": test,
                "total_steps": total_steps,
            }
        }));
        Ok(())
    }

    fn visit_error<'a>(&mut self, scope: Scope<'a>, err: LoadError) -> Result<(), Self::Error> {
        self.0.push(json!({
            "error": {
                "scope": scope.flatten().as_str(),
                "message": format!("{:?}", err),
            }
        }));
        Ok(())
    }
}
