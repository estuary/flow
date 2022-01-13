use std::net::TcpListener;

pub async fn spawn_app() -> anyhow::Result<String> {
    // Binding to port 0 will automatically assign a free random port.
    let listener = TcpListener::bind("127.0.0.1:0").expect("No random port available");
    let addr = listener.local_addr()?.to_string();
    let server = control::run(listener)?;
    // Tokio runs an executor for each test, so this server will shut down at the end of the test.
    let _ = tokio::spawn(server);
    Ok(addr)
}
