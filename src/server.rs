// ---------------------------------------------------------------------------
// HTTP + WebSocket server — Axum-based, single listener.
//
// Routes:
//   GET  /ws                    → WebSocket upgrade (Otto connections)
//   GET  /health                → Health check
//   GET  /ready                 → Readiness check (DB + optional Docker)
//   POST /api/webhooks/gitlab   → GitLab webhook receiver
//   GET  /.well-known/botto     → Discovery endpoint
//   GET  /admin                 → Admin settings page
//   GET  /api/admin/config      → Get current config (secrets redacted)
//   PUT  /api/admin/config      → Update config (hot-swap + persist)
//   GET  /api/admin/status      → Live server status
//
// Graceful shutdown on SIGTERM/SIGINT — drains active connections before exit.
// ---------------------------------------------------------------------------

use crate::api;
use crate::types::state::AppState;
use anyhow::Result;
use axum::routing::{any, get, post};
use axum::Router;
use tokio::net::TcpListener;
use tokio::signal;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use tracing::info;

pub async fn run(state: AppState) -> Result<()> {
    let app = Router::new()
        .route("/ws", any(api::ws::handler))
        .route("/health", get(api::health::health))
        .route("/ready", get(api::health::ready))
        .route("/api/webhooks/gitlab", post(api::webhooks::gitlab_webhook))
        .route("/.well-known/botto", get(api::discovery::well_known))
        .route("/admin", get(api::admin::page))
        .route("/api/admin/config", get(api::admin::get_config).put(api::admin::update_config))
        .route("/api/admin/status", get(api::admin::get_status))
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(state.clone());

    let addr = format!("{}:{}", state.config().server.host, state.config().server.port);
    let listener = TcpListener::bind(&addr).await?;
    info!("listening on {}", addr);

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    info!("server shut down gracefully");
    Ok(())
}

/// Wait for SIGTERM or SIGINT (Ctrl+C).
async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => info!("received SIGINT, shutting down..."),
        _ = terminate => info!("received SIGTERM, shutting down..."),
    }
}
