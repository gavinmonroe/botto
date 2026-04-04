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
        .route("/admin/workflows", get(api::admin::workflows_page))
        .route("/admin/directives", get(api::admin::directives_page))
        .route("/api/admin/config", get(api::admin::get_config).put(api::admin::update_config))
        .route("/api/admin/status", get(api::admin::get_status))
        .route("/api/admin/repo-configs", get(api::admin::list_repo_configs))
        .route("/api/admin/repo-configs/{*project_path}", axum::routing::delete(api::admin::delete_repo_config))
        // Workflow API
        .route("/api/workflows", get(api::workflows::list_workflows).post(api::workflows::create_workflow))
        .route("/api/workflows/{id}", get(api::workflows::get_workflow).put(api::workflows::update_workflow).delete(api::workflows::delete_workflow))
        .route("/api/workflows/{id}/enable", post(api::workflows::enable_workflow))
        .route("/api/workflows/{id}/disable", post(api::workflows::disable_workflow))
        .route("/api/workflows/{id}/trigger", post(api::workflows::trigger_workflow))
        .route("/api/workflows/{id}/runs", get(api::workflows::list_runs))
        .route("/api/workflow-runs/active", get(api::workflows::list_active_runs))
        .route("/api/workflow-runs/{run_id}", get(api::workflows::get_run))
        .route("/api/workflow-runs/{run_id}/log", get(api::workflows::get_run_log))
        // Session API (v2 orchestrator) — /waiting, /active, /recent must precede /{id} to avoid capture
        .route("/api/workflows/sessions/waiting", get(api::workflows::waiting_sessions))
        .route("/api/workflows/sessions/active", get(api::workflows::active_sessions))
        .route("/api/workflows/sessions/recent", get(api::workflows::recent_sessions))
        .route("/api/workflows/sessions/{id}", get(api::workflows::get_session))
        .route("/api/workflows/sessions/{id}/respond", post(api::workflows::respond_to_session))
        .route("/api/workflows/sessions/{id}/messages", get(api::workflows::session_messages))
        .route("/api/workflows/sessions/{id}/trace", get(api::workflows::session_trace))
        .route("/api/mentor/query", post(api::workflows::mentor_query))
        // Directive API
        .route("/api/directives", get(api::directives::list_directives).post(api::directives::create_directive))
        .route("/api/directives/{id}", get(api::directives::get_directive).put(api::directives::update_directive).delete(api::directives::delete_directive))
        .route("/api/directives/{id}/items", get(api::directives::list_work_items))
        // Slack channel adapter
        .route("/api/webhooks/slack/events", post(crate::services::channels::slack_input::slack_events_handler))
        .route("/api/webhooks/slack/interactions", post(crate::services::channels::slack_input::slack_interactions_handler))
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
