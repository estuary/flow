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
    method: axum::http::Method,
    path: String,
    headers: axum::http::HeaderMap,
    form: HashMap<String, String>,
}
async fn create_request(
    axum::extract::State(state): axum::extract::State<StubState>,
    headers: axum::http::HeaderMap,
    axum::Form(form): axum::Form<HashMap<String, String>>,
) -> (axum::http::StatusCode, axum::Json<serde_json::Value>) {
    record(
        state,
        axum::http::Method::POST,
        "/v1/customers".to_string(),
        headers,
        form,
    )
}
async fn other_request(
    axum::extract::State(state): axum::extract::State<StubState>,
    method: axum::http::Method,
    uri: axum::http::Uri,
    headers: axum::http::HeaderMap,
    _body: axum::body::Bytes,
) -> (axum::http::StatusCode, axum::Json<serde_json::Value>) {
    record(
        state,
        method,
        uri.path().to_string(),
        headers,
        HashMap::new(),
    )
}
fn record(
    state: StubState,
    method: axum::http::Method,
    path: String,
    headers: axum::http::HeaderMap,
    form: HashMap<String, String>,
) -> (axum::http::StatusCode, axum::Json<serde_json::Value>) {
    state.requests.lock().unwrap().push(Request {
        method,
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
async fn create_customer_serializes_human_billing_input_and_idempotency_key() {
    let tenant = "acmeCo/";
    let (provider, state) = stripe_provider(vec![(
        axum::http::StatusCode::OK,
        customer("cus_test", serde_json::json!({})),
    )])
    .await;
    let metadata = HashMap::from([
        (
            "created_by_user_email".to_string(),
            "billing@example.com".to_string(),
        ),
        ("created_by_user_name".to_string(), "A. Admin".to_string()),
        (
            "estuary.dev/customer_name".to_string(),
            "Acme Corporation".to_string(),
        ),
        (
            "estuary.dev/tenant_name".to_string(),
            "wrong-tenant".to_string(),
        ),
        ("custom-field".to_string(), "preserved".to_string()),
    ]);
    let address = serde_json::from_value(serde_json::json!({
        "city": "New York", "country": "US", "line1": "1 Main St",
        "postal_code": "10001", "state": "NY"
    }))
    .unwrap();
    provider
        .create_customer(tenant, Some("billing@example.com"), Some(address), metadata)
        .await
        .unwrap();
    let requests = state.requests.lock().unwrap();
    let request = &requests[0];
    assert_eq!(request.method, axum::http::Method::POST);
    assert_eq!(request.path, "/v1/customers");
    assert_eq!(
        request.headers.get("idempotency-key").unwrap(),
        "flow-customer-create:acmeCo/"
    );
    assert_eq!(request.form["name"], tenant);
    assert_eq!(request.form["email"], "billing@example.com");
    assert_eq!(
        request.form["description"],
        "Represents the billing entity for Flow tenant 'acmeCo/'"
    );
    assert_eq!(request.form["address[city]"], "New York");
    assert_eq!(request.form["address[country]"], "US");
    assert_eq!(request.form["address[line1]"], "1 Main St");
    assert_eq!(request.form["address[postal_code]"], "10001");
    assert_eq!(request.form["address[state]"], "NY");
    assert_eq!(request.form["metadata[estuary.dev/tenant_name]"], tenant);
    assert_eq!(
        request.form["metadata[created_by_user_email]"],
        "billing@example.com"
    );
    assert_eq!(request.form["metadata[created_by_user_name]"], "A. Admin");
    assert_eq!(
        request.form["metadata[estuary.dev/customer_name]"],
        "Acme Corporation"
    );
    assert_eq!(request.form["metadata[custom-field]"], "preserved");
}

#[tokio::test]
async fn create_customer_serializes_automation_input_without_human_name_or_email() {
    let tenant = "automatedTenant/";
    let (provider, state) = stripe_provider(vec![(
        axum::http::StatusCode::OK,
        customer("cus_auto", serde_json::json!({})),
    )])
    .await;
    let metadata = HashMap::from([(
        "estuary.dev/created_by_automation".to_string(),
        "true".to_string(),
    )]);
    provider
        .create_customer(tenant, None, None, metadata)
        .await
        .unwrap();
    let requests = state.requests.lock().unwrap();
    let request = &requests[0];
    assert_eq!(
        request.headers.get("idempotency-key").unwrap(),
        "flow-customer-create:automatedTenant/"
    );
    assert_eq!(request.form["name"], tenant);
    assert_eq!(
        request.form["description"],
        "Represents the billing entity for Flow tenant 'automatedTenant/'"
    );
    assert!(!request.form.contains_key("email"));
    assert_eq!(
        request.form["metadata[estuary.dev/created_by_automation]"],
        "true"
    );
    assert_eq!(request.form["metadata[estuary.dev/tenant_name]"], tenant);
    assert!(
        !request
            .form
            .contains_key("metadata[estuary.dev/customer_name]")
    );
    assert!(!request.form.keys().any(|key| key.starts_with("address[")));
}

#[tokio::test]
async fn retrieve_customer_uses_recorded_id_and_propagates_errors() {
    let (provider, state) = stripe_provider(vec![
        (
            axum::http::StatusCode::OK,
            serde_json::json!({"id": "cus_deleted", "object": "customer", "deleted": true}),
        ),
        (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            serde_json::json!({"error": {"type": "api_error", "message": "unavailable"}}),
        ),
    ])
    .await;
    let id: stripe::CustomerId = "cus_deleted".parse().unwrap();
    assert!(provider.retrieve_customer(&id).await.unwrap().deleted);
    let id: stripe::CustomerId = "cus_error".parse().unwrap();
    assert!(provider.retrieve_customer(&id).await.is_err());
    let requests = state.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].path, "/v1/customers/cus_deleted");
    assert_eq!(requests[1].path, "/v1/customers/cus_error");
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
