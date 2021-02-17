use crate::{Fetcher, Loader, Scope, Tables};
use futures::channel::oneshot;
use futures::future::{FutureExt, LocalBoxFuture};
use protocol::flow;
use serde_json::Value;
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
            let fixture: Value = serde_yaml::from_slice(fixture).unwrap();
			let tables = evaluate_fixtures(Default::default(), &fixture);
			insta::assert_debug_snapshot!(tables);
        }
    )*
    }
}

file_tests! {
    test_catalog_import_cycles,
    test_collections,
    test_derivations,
    test_endpoints_captures_materializations,
    test_journal_rules,
    test_schema_with_anchors,
    test_schema_with_inline,
    test_schema_with_nested_ids,
    test_schema_with_references,
    test_simple_catalog,
    test_test_case,
}

// MockFetcher queues and returns oneshot futures for started fetches.
struct MockFetcher<'f> {
    fetches: &'f RefCell<BTreeMap<String, oneshot::Sender<Result<bytes::Bytes, anyhow::Error>>>>,
}

impl<'f> Fetcher for MockFetcher<'f> {
    fn fetch<'a>(
        &self,
        resource: &'a Url,
        _content_type: &'a flow::ContentType,
    ) -> LocalBoxFuture<'a, Result<bytes::Bytes, anyhow::Error>> {
        let (tx, rx) = oneshot::channel();

        if let Some(_) = self.fetches.borrow_mut().insert(resource.to_string(), tx) {
            panic!("resource {} has already been fetched", resource);
        }
        rx.map(|r| r.unwrap()).boxed_local()
    }
}

pub fn evaluate_fixtures(catalog: Tables, fixture: &Value) -> Tables {
    let fixtures = match fixture {
        Value::Object(m) => m,
        _ => panic!("fixtures must be an object having resource properties"),
    };

    // Fetches holds started fetches since the last future poll.
    // Use an ordered map so that we signal one-shots in a stable order,
    // making snapshots reliable.
    let fetches = RefCell::new(BTreeMap::new());

    let loader = Loader::new(catalog, MockFetcher { fetches: &fetches });
    let root = Url::parse("test://example/catalog.yaml").unwrap();

    // What's going on here? Glad you asked.
    //
    // loader.load_resource() is returning a Future, which under the covers
    // is a big, synchronously-invoked, and deterministic state machine that's
    // built with the compiler's assistance.
    //
    // By construction, we know that there's only one real "await" point of this
    // future which will cause it to return Poll::Pending: when it's called into the
    // |fetch| closure we gave it above, and it has no work remaining to do until
    // at least one of the Futures returned by |fetch| resolves.
    //
    // Loader is walking sources concurrently. It processes fetches as separate
    // internal (still synchronous & deterministic) tasks, so we *do* expect to
    // see multiple |fetch| calls made in between Poll::Pending poll results of
    // the future.
    //
    // So, the strategy is this: we _synchronously_ poll the future forward in a
    // loop (e.x., we're not using a runtime *at all*). Every time it returns
    // Poll::Pending, we assert that it's queued new calls to |fetch| during this
    // iteration, and we resolve each of those response futures. Eventually, it
    // returns Poll::Ready when it's fully walked the source fixture, and at that
    // point we unwrap and return the loaded Tables.
    //
    // Note that the use of BTreeMap above is significant: it means that we resolve
    // those call futures in a stable (sorted) order, and that's the order with which
    // the future will process resolved responses on it's next poll(). It makes the
    // whole mess fully deterministic.

    let mut fut = loader
        .load_resource(Scope::new(&root), &root, flow::ContentType::CatalogSpec)
        .boxed_local();

    let waker = futures::task::noop_waker();
    let mut ctx = std::task::Context::from_waker(&waker);

    loop {
        match fut.poll_unpin(&mut ctx) {
            Poll::Ready(()) => {
                std::mem::drop(fut);
                return loader.into_tables();
            }
            Poll::Pending if fetches.borrow().is_empty() => {
                // Note the future can return Pending *only because* it's blocked
                // waiting for one or more |fetch| fixtures above to resolve.
                panic!("future is pending, but started no fetches")
            }
            Poll::Pending => {
                for (url, tx) in fetches.borrow_mut().split_off("") {
                    match fixtures.get(&url) {
                        Some(value) => tx.send(Ok(serde_json::to_vec(&value).unwrap().into())),
                        None => tx.send(Err(anyhow::anyhow!("fixture not found"))),
                    }
                    .unwrap();
                }
            }
        }
    }
}
