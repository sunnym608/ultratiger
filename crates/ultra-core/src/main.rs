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
    let (app, state) = app::router(config);

    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    let listener = TcpListener::bind(addr)
        .await
        .expect("failed to bind TCP listener");

    info!("Ultra Core listening on {}", addr);
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal(state))
        .await
        .expect("server terminated unexpectedly");
}

async fn shutdown_signal(state: app::AppState) {
    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {},
        _ = terminate => {},
    }

    state.begin_shutdown();
    info!("shutdown signal received");
}
