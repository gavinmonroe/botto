// ---------------------------------------------------------------------------
// Directive REST API — CRUD for directives and work item feeds.
//
// Endpoints:
//   POST   /api/directives              — create directive from NL description
//   GET    /api/directives              — list directives
//   GET    /api/directives/:id          — get directive with stats
//   PUT    /api/directives/:id          — update directive
//   DELETE /api/directives/:id          — retire directive
//   GET    /api/directives/:id/items    — work item feed
// ---------------------------------------------------------------------------

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Json};
use serde::Deserialize;

use crate::services::directive::{crud, parser};
use crate::types::state::AppState;

// ---------------------------------------------------------------------------
// Auth (reuse pattern from workflows.rs)
// ---------------------------------------------------------------------------

fn check_auth(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<(), (StatusCode, Json<serde_json::Value>)> {
    let cfg = state.config();
    let expected = &cfg.auth.api_key;

    if expected.is_empty() {
        return Ok(());
    }

    if let Some(auth_header) = headers.get("authorization") {
        if let Ok(value) = auth_header.to_str() {
            if let Some(token) = value.strip_prefix("Bearer ") {
                if token == expected {
                    return Ok(());
                }
            }
        }
    }

    Err((
        StatusCode::UNAUTHORIZED,
        Json(serde_json::json!({ "error": "unauthorized" })),
    ))
}

// ---------------------------------------------------------------------------
// Request/query types
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct CreateDirectiveBody {
    pub description: String,
    pub created_by: Option<String>,
}

#[derive(Deserialize)]
pub struct ListItemsQuery {
    pub status: Option<String>,
    pub limit: Option<u32>,
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// POST /api/directives — create a directive from natural language.
pub async fn create_directive(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<CreateDirectiveBody>,
) -> impl IntoResponse {
    if let Err(e) = check_auth(&state, &headers) {
        return e.into_response();
    }

    let cfg = state.config();
    let ai_config = crate::services::ai::client::AiClientConfig {
        base_url: cfg.ai.base_url.clone(),
        api_key: cfg.ai.api_key.clone(),
    };
    let model = cfg.ai.models.workflow_decompose.clone();
    let created_by = body.created_by.as_deref().unwrap_or("api");

    let directive = match parser::parse_directive(&ai_config, &model, &body.description, Some(created_by)).await {
        Ok(d) => d,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "ok": false, "error": e.to_string() })),
            )
                .into_response();
        }
    };

    if let Err(e) = crud::create_directive(state.pool(), &directive).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "ok": false, "error": e.to_string() })),
        )
            .into_response();
    }

    (
        StatusCode::CREATED,
        Json(serde_json::json!({
            "ok": true,
            "directive": directive,
        })),
    )
        .into_response()
}

/// GET /api/directives — list all non-retired directives.
pub async fn list_directives(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(e) = check_auth(&state, &headers) {
        return e.into_response();
    }

    match crud::list_directives(state.pool()).await {
        Ok(directives) => Json(serde_json::json!({
            "ok": true,
            "directives": directives,
        }))
        .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "ok": false, "error": e.to_string() })),
        )
            .into_response(),
    }
}

/// GET /api/directives/:id — get a directive with stats.
pub async fn get_directive(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> impl IntoResponse {
    if let Err(e) = check_auth(&state, &headers) {
        return e.into_response();
    }

    let pool = state.pool();

    let directive = match crud::load_directive(pool, &id).await {
        Ok(Some(d)) => d,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({ "ok": false, "error": "directive not found" })),
            )
                .into_response();
        }
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "ok": false, "error": e.to_string() })),
            )
                .into_response();
        }
    };

    // Gather stats.
    let active_sessions = crud::count_active_sessions_for_directive(pool, &id)
        .await
        .unwrap_or(0);
    let failed_sessions = crud::count_failed_sessions(pool, &id)
        .await
        .unwrap_or(0);

    Json(serde_json::json!({
        "ok": true,
        "directive": directive,
        "stats": {
            "activeSessions": active_sessions,
            "failedSessions": failed_sessions,
        },
    }))
    .into_response()
}

/// PUT /api/directives/:id — update a directive.
pub async fn update_directive(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(mut directive): Json<crate::services::directive::types::Directive>,
) -> impl IntoResponse {
    if let Err(e) = check_auth(&state, &headers) {
        return e.into_response();
    }

    if directive.id != id {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "ok": false, "error": "directive ID mismatch" })),
        )
            .into_response();
    }

    directive.updated_at = crate::services::workflow::crud::epoch_secs();

    match crud::update_directive(state.pool(), &directive).await {
        Ok(true) => Json(serde_json::json!({ "ok": true })).into_response(),
        Ok(false) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "ok": false, "error": "directive not found" })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "ok": false, "error": e.to_string() })),
        )
            .into_response(),
    }
}

/// DELETE /api/directives/:id — retire a directive (soft delete).
pub async fn delete_directive(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> impl IntoResponse {
    if let Err(e) = check_auth(&state, &headers) {
        return e.into_response();
    }

    match crud::retire_directive(state.pool(), &id).await {
        Ok(true) => Json(serde_json::json!({ "ok": true })).into_response(),
        Ok(false) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "ok": false, "error": "directive not found" })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "ok": false, "error": e.to_string() })),
        )
            .into_response(),
    }
}

/// GET /api/directives/:id/items — work item feed for a directive.
pub async fn list_work_items(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(query): Query<ListItemsQuery>,
) -> impl IntoResponse {
    if let Err(e) = check_auth(&state, &headers) {
        return e.into_response();
    }

    let limit = query.limit.unwrap_or(50).min(200);

    match crud::list_work_items(state.pool(), &id, query.status.as_deref(), limit).await {
        Ok(items) => Json(serde_json::json!({
            "ok": true,
            "items": items,
        }))
        .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "ok": false, "error": e.to_string() })),
        )
            .into_response(),
    }
}
