//! A stand-in for the Stripe API in tests: serves canned responses in order
//! and records the method and URI of each request it receives.

pub async fn stripe_stub(
    responses: Vec<(axum::http::StatusCode, serde_json::Value)>,
) -> (
    stripe::Client,
    std::sync::Arc<std::sync::Mutex<Vec<String>>>,
) {
    let requests = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let recorded = requests.clone();
    let responses = std::sync::Arc::new(std::sync::Mutex::new(responses.into_iter()));
    let app = axum::Router::new().fallback(move |method: axum::http::Method, uri: axum::http::Uri| {
        let recorded = recorded.clone();
        let responses = responses.clone();
        async move {
            recorded.lock().unwrap().push(format!("{method} {}", uri.to_string().trim_end_matches("?")));
            let (status, body) = responses.lock().unwrap().next().unwrap_or((
                axum::http::StatusCode::BAD_REQUEST,
                serde_json::json!({"error": {"type": "invalid_request_error", "message": "unexpected request"}}),
            ));
            (status, axum::Json(body))
        }
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/", listener.local_addr().unwrap());
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (stripe::Client::from_url(url.as_str(), "test"), requests)
}
