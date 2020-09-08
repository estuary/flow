use crate::derive::state::DocStore;
use crate::specs::store::Document;
use headers::{ContentType, HeaderMapExt};
use log::info;
use std::sync::{Arc, Mutex};
use warp::{filters::BoxedFilter, Filter, Reply};

// GET / -> "state service ready"
fn rt_hello() -> BoxedFilter<(impl Reply,)> {
    warp::get()
        .and(warp::path::end())
        .map(|| "state service ready")
        .boxed()
}

// GET /docs/path/to/doc -> Returns zero or more previously POSTed documents
// having the given prefix, as a JSON array.
fn rt_get_doc(store: Arc<Mutex<Box<impl DocStore + 'static>>>) -> BoxedFilter<(impl Reply,)> {
    warp::get()
        .and(warp::path("docs"))
        .and(warp::path::tail())
        .map(move |prefix: warp::path::Tail| {
            info!("GET {:?}", prefix);

            let mut b = Vec::with_capacity(1024);
            b.push(b'[');

            let store = store.lock().unwrap();
            for (ind, doc) in store.iter_prefix(&prefix.as_str()).enumerate() {
                if ind != 0 {
                    b.push(b',');
                }
                serde_json::to_writer(&mut b, &doc).unwrap();
            }
            b.push(b']');

            let mut resp = warp::reply::Response::new(b.into());
            resp.headers_mut().typed_insert(ContentType::json());
            resp
        })
        .boxed()
}

// POST /docs -> Sets a document (in POST body) within the store.
fn rt_post_doc(store: Arc<Mutex<Box<impl DocStore + 'static>>>) -> BoxedFilter<(impl Reply,)> {
    warp::post()
        .and(warp::path!("docs"))
        .and(warp::body::bytes())
        .map(move |body: bytes::Bytes| {
            let doc = serde_json::from_slice::<Document>(body.as_ref());
            let resp = match doc {
                Err(err) => {
                    info!("PUT decode failed: {}", err);

                    let mut resp = warp::reply::Response::new(err.to_string().into());
                    *resp.status_mut() = warp::http::StatusCode::from_u16(400).unwrap();
                    resp.headers_mut().typed_insert(ContentType::text_utf8());
                    resp
                }
                Ok(doc) => {
                    info!("PUT document doc {:?}", doc);

                    store.lock().unwrap().put(&doc);
                    warp::reply::reply().into_response()
                }
            };
            resp
        })
        .boxed()
}

pub fn build(
    store: Arc<Mutex<Box<impl DocStore + 'static>>>,
) -> BoxedFilter<(impl Reply + 'static,)> {
    rt_hello()
        .or(rt_post_doc(store.clone()))
        .or(rt_get_doc(store.clone()))
        .boxed()
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::derive::state::MemoryStore;
    use serde_json::{
        json,
        value::{RawValue, Value},
    };
    use warp::test::request as test_req;

    #[tokio::test]
    async fn test_request_cases() {
        let store = Arc::new(Mutex::new(Box::new(MemoryStore::new())));
        let svc = build(store);

        // GET of / => ready message.
        let res = test_req().path("/").reply(&svc).await;
        assert_eq!(res.status(), 200);
        assert_eq!(res.body(), "state service ready");

        // GET of /docs/not/found => empty JSON array.
        let res = test_req().path("/docs/not/found").reply(&svc).await;
        assert_eq!(res.status(), 200);
        assert_eq!(res.headers().typed_get(), Some(ContentType::json()));
        assert_eq!(res.body(), "[]");

        // POST of /docs with invalid JSON => error.
        let res = test_req()
            .method("POST")
            .path("/docs")
            .body("\"bad")
            .reply(&svc)
            .await;
        assert_eq!(res.status(), 400);
        assert_eq!(res.headers().typed_get(), Some(ContentType::text_utf8()));
        assert_eq!(res.body(), "EOF while parsing a string at line 1 column 4");

        // POST of /docs with wrong JSON shape => error.
        let res = test_req()
            .method("POST")
            .path("/docs")
            .body("{}")
            .reply(&svc)
            .await;
        assert_eq!(res.status(), 400);
        assert_eq!(res.headers().typed_get(), Some(ContentType::text_utf8()));
        assert_eq!(res.body(), "missing field `key` at line 1 column 2");

        for (key, val) in vec![
            ("foo", json!("base")),
            ("foo/1", json!(false)),
            ("foo/2", json!({"bar": 2})),
            ("foo/3", json!({"baz": 3})),
            ("foo/1", json!(true)), // Replace
            ("fo0", json!(42)),
            ("other", json!([52, 62])),
        ]
        .into_iter()
        {
            let doc = Document {
                key: key.into(),
                value: &serde_json::from_value::<Box<RawValue>>(val).unwrap(),
                expire_at: None,
            };
            let res = test_req()
                .method("POST")
                .path("/docs")
                .json(&doc)
                .reply(&svc)
                .await;
            assert_eq!(res.status(), 200);
            assert_eq!(res.body().len(), 0);
        }

        // GET of /docs/foo/ => retrieves expected documents. Note "foo/" doesn't prefix "foo".
        let res = test_req().path("/docs/foo/").reply(&svc).await;
        let res: Value = serde_json::from_slice(res.body()).unwrap();
        assert_eq!(
            res,
            json!([
                {"key": "foo/1", "value": true}, // Expect replaced value.
                {"key": "foo/2", "value": {"bar": 2}},
                {"key": "foo/3", "value": {"baz": 3}},
            ])
        );

        // GET of /docs/other => retrieves single document.
        let res = test_req().path("/docs/other").reply(&svc).await;
        let res: Value = serde_json::from_slice(res.body()).unwrap();
        assert_eq!(
            res,
            json!([
                {"key": "other", "value": [52, 62]},
            ])
        );
    }
}
