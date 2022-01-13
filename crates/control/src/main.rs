use std::net::TcpListener;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let listener = TcpListener::bind("0.0.0.0:3000")?;
    Ok(control::run(listener)?.await?)
}
