// ---------------------------------------------------------------------------
// Admin settings API — protected by the same API key as WebSocket auth.
//
// Endpoints:
//   GET  /admin              — serve the embedded settings HTML page
//   GET  /api/admin/config   — return current config (secrets redacted)
//   PUT  /api/admin/config   — update config, hot-swap, persist to TOML
//   GET  /api/admin/status   — live server status (connections, reviews, etc.)
//
// Auth: Bearer token in Authorization header, or ?key= query param for
// the initial page load. Empty API key = dev mode (all access allowed),
// consistent with WebSocket auth behavior.
// ---------------------------------------------------------------------------

use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{Html, IntoResponse, Json};
use serde::{Deserialize, Serialize};

use crate::config::{self, ConfigResponse, ConfigUpdate};
use crate::types::state::AppState;

// ---------------------------------------------------------------------------
// Auth
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct AdminQuery {
    key: Option<String>,
}

/// Validate the API key from either Authorization header or query param.
/// Returns Ok(()) if authorized, Err(response) if not.
fn check_auth(state: &AppState, headers: &HeaderMap, query: &AdminQuery) -> Result<(), (StatusCode, Json<serde_json::Value>)> {
    let cfg = state.config();
    let expected = &cfg.auth.api_key;

    // Dev mode: empty key allows all access (consistent with WS auth)
    if expected.is_empty() {
        return Ok(());
    }

    // Check Authorization: Bearer <key>
    if let Some(auth_header) = headers.get("authorization") {
        if let Ok(value) = auth_header.to_str() {
            if let Some(token) = value.strip_prefix("Bearer ") {
                if token == expected {
                    return Ok(());
                }
            }
        }
    }

    // Check ?key= query param
    if let Some(ref key) = query.key {
        if key == expected {
            return Ok(());
        }
    }

    Err((
        StatusCode::UNAUTHORIZED,
        Json(serde_json::json!({ "error": "unauthorized" })),
    ))
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// Serve the embedded admin settings page.
pub async fn page(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<AdminQuery>,
) -> impl IntoResponse {
    if let Err(e) = check_auth(&state, &headers, &query) {
        return e.into_response();
    }
    Html(include_str!("admin_page.html")).into_response()
}

/// Return the current config with secrets redacted.
pub async fn get_config(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<AdminQuery>,
) -> impl IntoResponse {
    if let Err(e) = check_auth(&state, &headers, &query) {
        return e.into_response();
    }
    let cfg = state.config();
    Json(ConfigResponse::from_config(&cfg)).into_response()
}

/// Update config, hot-swap in memory, persist to disk.
pub async fn update_config(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<AdminQuery>,
    Json(update): Json<ConfigUpdate>,
) -> impl IntoResponse {
    if let Err(e) = check_auth(&state, &headers, &query) {
        return e.into_response();
    }

    let current = state.config();
    let (new_config, restart_fields) = config::apply_update(&current, update);

    // Persist to disk first — if this fails, don't swap in memory
    if let Err(e) = config::save_to_file(&new_config).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "error": format!("failed to save config: {}", e),
            })),
        ).into_response();
    }

    // Hot-swap in memory
    state.swap_config(new_config.clone());

    let restart_required = !restart_fields.is_empty();

    Json(UpdateResponse {
        saved: true,
        restart_required,
        restart_fields,
        config: ConfigResponse::from_config(&new_config),
    }).into_response()
}

#[derive(Serialize)]
struct UpdateResponse {
    saved: bool,
    restart_required: bool,
    restart_fields: Vec<String>,
    config: ConfigResponse,
}

/// Live server status — connections, in-flight reviews, etc.
pub async fn get_status(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<AdminQuery>,
) -> impl IntoResponse {
    if let Err(e) = check_auth(&state, &headers, &query) {
        return e.into_response();
    }

    let connections = state.connections();
    let total_connections = connections.len();
    let authenticated = connections.iter().filter(|e| e.value().authenticated).count();
    let viewing_mrs: Vec<String> = connections
        .iter()
        .filter_map(|e| e.value().viewing_mr.as_ref().map(|mr| mr.key()))
        .collect();

    let in_flight = state.in_flight();
    let active_reviews: Vec<String> = in_flight
        .iter()
        .filter(|e| !e.value().is_complete())
        .map(|e| e.key().clone())
        .collect();

    let cfg = state.config();

    Json(serde_json::json!({
        "connections": {
            "total": total_connections,
            "authenticated": authenticated,
            "viewing_mrs": viewing_mrs,
        },
        "reviews": {
            "active": active_reviews.len(),
            "active_mrs": active_reviews,
            "max_concurrent": cfg.server.max_concurrent_reviews,
        },
        "sandbox": {
            "enabled": cfg.sandbox.enabled,
            "docker_available": cfg.sandbox.docker_available,
            "max_concurrent": cfg.sandbox.max_concurrent,
        },
        "version": env!("CARGO_PKG_VERSION"),
    })).into_response()
}
