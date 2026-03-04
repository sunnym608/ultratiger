use std::net::SocketAddr;

use tokio::net::TcpListener;
use tracing::info;
use ultra_core::{app, config::AppConfig};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "ultra_core=info,tower_http=info".into()),
        )
        .init();

    let config = AppConfig::default();
    let app = app::router(config);

    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    let listener = TcpListener::bind(addr)
        .await
        .expect("failed to bind TCP listener");

    info!("Ultra Core listening on {}", addr);
    axum::serve(listener, app)
        .await
        .expect("server terminated unexpectedly");
}
