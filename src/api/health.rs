// ---------------------------------------------------------------------------
// Health + readiness endpoints.
// ---------------------------------------------------------------------------

use crate::types::state::AppState;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::Json;
use serde_json::{json, Value};

/// Liveness probe — always returns 200 if the process is running.
pub async fn health() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}

/// Readiness probe — checks DB, AI, GitLab, queue, and sandbox status.
/// External checks (AI, GitLab) run concurrently with a short timeout
/// so the endpoint stays fast even when services are slow.
pub async fn ready(State(state): State<AppState>) -> Result<Json<Value>, StatusCode> {
    // Check database — this is the hard gate for readiness
    let db_ok = sqlx::query("SELECT 1")
        .execute(state.pool())
        .await
        .is_ok();

    if !db_ok {
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }

    let cfg = state.config();
    let connected_count = state.connections().len();

    // Run AI and GitLab checks concurrently with a 3s timeout each
    let (ai_status, gitlab_status) = tokio::join!(
        check_ai_status(&cfg.ai.base_url),
        check_gitlab_status(&cfg.gitlab.url, &cfg.gitlab.bot_token),
    );

    // Queue manager status
    let queue_status = match state.queue_manager() {
        Some(_) => "running",
        None => "not_started",
    };

    // Warm pool status
    let (sandbox_status, warm_containers) = match state.warm_pool() {
        Some(pool) => ("enabled", pool.count()),
        None => ("disabled", 0),
    };

    // In-flight reviews
    let in_flight_count = state.in_flight().len();

    Ok(Json(json!({
        "status": "ready",
        "database": "ok",
        "ai": ai_status,
        "gitlab": gitlab_status,
        "queue": queue_status,
        "sandbox": {
            "status": sandbox_status,
            "warm_containers": warm_containers,
        },
        "connected_ottos": connected_count,
        "in_flight_reviews": in_flight_count,
    })))
}

async fn check_ai_status(base_url: &str) -> &'static str {
    if base_url.is_empty() {
        return "not_configured";
    }
    // Lightweight HEAD with short timeout — we just need reachability
    let check = async {
        static CLIENT: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();
        let client = CLIENT.get_or_init(|| {
            reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(3))
                .build()
                .unwrap_or_default()
        });
        client.head(base_url).send().await
    };
    match tokio::time::timeout(std::time::Duration::from_secs(3), check).await {
        Ok(Ok(_)) => "ok",
        _ => "unreachable",
    }
}

async fn check_gitlab_status(base_url: &str, token: &str) -> &'static str {
    if token.is_empty() {
        return "not_configured";
    }
    let gl_cfg = crate::services::gitlab::client::GitLabConfig {
        base_url: base_url.to_string(),
        token: token.to_string(),
    };
    match tokio::time::timeout(
        std::time::Duration::from_secs(3),
        crate::services::gitlab::client::test_connection(&gl_cfg),
    )
    .await
    {
        Ok(Ok(_)) => "ok",
        _ => "unreachable",
    }
}
