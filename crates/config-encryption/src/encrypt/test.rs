use crate::encrypt::handler::{router, EncryptReq, EncryptedConfig, Error};
use axum::{
    body::Body,
    http::{self, Request, StatusCode},
};
use serde_json::Value;
use tower::ServiceExt;

fn sops_args() -> crate::SopsArgs {
    crate::SopsArgs {
        gcp_kms: String::from("projects/helpful-kingdom-273219/locations/us-central1/keyRings/dev/cryptoKeys/CI-estuary-flow"),
        encrypted_suffix: String::from("_sops"),
    }
}

const SCHEMA: &str = r#"{
    "type": "object",
    "properties": {
        "not_secret": {"type": "string"},
        "supa_secret": {"type": "string", "secret": true},
        "also_secret": {"type": "string", "airbyte_secret": true}
    }
}"#;

fn body_with_config(config: &str) -> Body {
    Body::from(format!(
        r#"{{ "schema": {}, "config": {} }}"#,
        SCHEMA, config
    ))
}

#[tokio::test]
async fn json_config_is_encrypted() {
    let _ = tracing_subscriber::fmt().try_init();
    let app = router(sops_args());
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .header(http::header::CONTENT_TYPE, "application/json")
                .uri("/v1/encrypt-config")
                .body(body_with_config(
                    r#"{"supa_secret": "foo", "also_secret": "bar", "not_secret": "clear"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    let status = resp.status();
    assert_eq!(StatusCode::OK, status);
    let json = body_json(resp).await;
    assert_json_loc_eq(&json, "/not_secret", "clear");

    assert_not_present(&json, "/supa_secret");
    assert_not_present(&json, "/also_secret");
    assert_encrypted(&json, "/supa_secret_sops");
    assert_encrypted(&json, "/also_secret_sops");
}

#[tokio::test]
async fn validation_error_is_returned() {
    let _ = tracing_subscriber::fmt().try_init();
    let app = router(sops_args());
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .header(http::header::CONTENT_TYPE, "application/json")
                .uri("/v1/encrypt-config")
                .body(body_with_config(r#"{"supa_secret": 123}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    let status = resp.status();
    let json = body_json(resp).await;
    assert_json_loc_eq(&json, "/error", "FailedValidation");
    assert_eq!(StatusCode::BAD_REQUEST, status);
}

fn assert_encrypted(json: &Value, pointer: &str) {
    match json.pointer(pointer) {
        Some(value) => {
            let str_value = value.as_str().expect("value must be a string");
            assert!(str_value.starts_with("ENC["));
        }
        None => panic!("missing value at '{}' in obj: {}", pointer, json),
    }
}

fn assert_not_present(json: &Value, pointer: &str) {
    assert!(json.pointer(pointer).is_none());
}

fn assert_json_loc_eq(json: &Value, pointer: &str, expected: impl Into<Value>) {
    let expected = expected.into();
    assert_eq!(
        Some(&expected),
        json.pointer(pointer),
        "unexpected value at '{}', body: {}",
        pointer,
        json,
    );
}

async fn body_json<B>(resp: axum::response::Response<B>) -> Value
where
    B: axum::body::HttpBody,
    B::Error: std::fmt::Debug,
{
    let body = hyper::body::to_bytes(resp.into_body()).await.unwrap();
    println!("body: {}", String::from_utf8_lossy(body.as_ref()));
    serde_json::from_slice(&body).unwrap()
}
