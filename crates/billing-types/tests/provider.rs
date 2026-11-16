use billing_types::{BillingProvider, StripeBillingProvider};
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
};

#[derive(Clone)]
struct StubState {
    responses: Arc<Mutex<VecDeque<(axum::http::StatusCode, serde_json::Value)>>>,
    requests: Arc<Mutex<Vec<Request>>>,
}
#[derive(Debug)]
struct Request {
    path: String,
    headers: axum::http::HeaderMap,
    form: HashMap<String, String>,
}
async fn create_request(
    axum::extract::State(state): axum::extract::State<StubState>,
    headers: axum::http::HeaderMap,
    axum::Form(form): axum::Form<HashMap<String, String>>,
) -> (axum::http::StatusCode, axum::Json<serde_json::Value>) {
    record(state, "/v1/customers".to_string(), headers, form)
}
async fn other_request(
    axum::extract::State(state): axum::extract::State<StubState>,
    uri: axum::http::Uri,
    headers: axum::http::HeaderMap,
    _body: axum::body::Bytes,
) -> (axum::http::StatusCode, axum::Json<serde_json::Value>) {
    record(state, uri.path().to_string(), headers, HashMap::new())
}
fn record(
    state: StubState,
    path: String,
    headers: axum::http::HeaderMap,
    form: HashMap<String, String>,
) -> (axum::http::StatusCode, axum::Json<serde_json::Value>) {
    state.requests.lock().unwrap().push(Request {
        path,
        headers,
        form,
    });
    let (status, body) = state.responses.lock().unwrap().pop_front().unwrap_or_else(|| (
        axum::http::StatusCode::BAD_REQUEST,
        serde_json::json!({"error": {"type": "invalid_request_error", "message": "unexpected request"}}),
    ));
    (status, axum::Json(body))
}
async fn stripe_provider(
    responses: Vec<(axum::http::StatusCode, serde_json::Value)>,
) -> (StripeBillingProvider, StubState) {
    let state = StubState {
        responses: Arc::new(Mutex::new(responses.into_iter().collect())),
        requests: Arc::new(Mutex::new(Vec::new())),
    };
    let app = axum::Router::new()
        .route("/v1/customers", axum::routing::post(create_request))
        .fallback(other_request)
        .with_state(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/", listener.local_addr().unwrap());
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (
        StripeBillingProvider::from_client(stripe::Client::from_url(url.as_str(), "test")),
        state,
    )
}
fn customer(id: &str, metadata: serde_json::Value) -> serde_json::Value {
    serde_json::json!({"id": id, "object": "customer", "created": 0, "livemode": false, "metadata": metadata})
}
#[tokio::test]
async fn create_customer_is_idempotent_per_tenant_and_tagged_with_it() {
    let tenant = "acmeCo/";
    let (provider, state) = stripe_provider(vec![(
        axum::http::StatusCode::OK,
        customer("cus_test", serde_json::json!({})),
    )])
    .await;
    let metadata = HashMap::from([
        (
            "estuary.dev/tenant_name".to_string(),
            "wrong-tenant".to_string(),
        ),
        ("custom-field".to_string(), "preserved".to_string()),
    ]);
    provider
        .create_customer(tenant, None, None, metadata)
        .await
        .unwrap();
    let requests = state.requests.lock().unwrap();
    let request = &requests[0];
    assert_eq!(
        request.headers.get("idempotency-key").unwrap(),
        "flow-customer-create:acmeCo/"
    );
    assert_eq!(request.form["name"], tenant);
    assert_eq!(request.form["metadata[estuary.dev/tenant_name]"], tenant);
    assert_eq!(request.form["metadata[custom-field]"], "preserved");
    assert!(!request.form.contains_key("email"));
}

#[tokio::test]
async fn retrieve_invoice_maps_only_not_found_to_none() {
    let (provider, state) = stripe_provider(vec![
        (
            axum::http::StatusCode::NOT_FOUND,
            serde_json::json!({"error": {"type": "invalid_request_error", "message": "No such invoice"}}),
        ),
        (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            serde_json::json!({"error": {"type": "api_error", "message": "unavailable"}}),
        ),
    ])
    .await;
    let id: stripe::InvoiceId = "in_missing".parse().unwrap();
    assert!(provider.retrieve_invoice(&id).await.unwrap().is_none());
    let error = provider.retrieve_invoice(&id).await.unwrap_err();
    assert!(format!("{error:#}").contains("500") || format!("{error:#}").contains("unavailable"));
    let requests = state.requests.lock().unwrap();
    assert_eq!(requests[0].path, "/v1/invoices/in_missing");
    assert_eq!(requests[1].path, "/v1/invoices/in_missing");
}
