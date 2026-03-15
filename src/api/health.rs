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

/// Readiness probe — checks DB connectivity and optional Docker.
pub async fn ready(State(state): State<AppState>) -> Result<Json<Value>, StatusCode> {
    // Check database
    let db_ok = sqlx::query("SELECT 1")
        .execute(state.pool())
        .await
        .is_ok();

    if !db_ok {
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }

    let connected_count = state.connections().len();

    Ok(Json(json!({
        "status": "ready",
        "database": "ok",
        "sandbox_enabled": state.config().sandbox.enabled,
        "connected_ottos": connected_count,
    })))
}
