// ---------------------------------------------------------------------------
// Workflow REST API — CRUD for workflows, manual triggers, run status,
// and mentor queries.
//
// Endpoints:
//   GET    /api/workflows                    — list workflows (optional ?project_id=)
//   POST   /api/workflows                    — create workflow (from NL description)
//   GET    /api/workflows/:id                — get workflow definition
//   PUT    /api/workflows/:id                — update workflow definition
//   DELETE /api/workflows/:id                — delete workflow + runs
//   POST   /api/workflows/:id/enable         — enable workflow
//   POST   /api/workflows/:id/disable        — disable workflow
//   POST   /api/workflows/:id/trigger        — manually trigger a run
//   GET    /api/workflows/:id/runs            — list runs for a workflow
//   GET    /api/workflow-runs/:run_id         — get run details
//   GET    /api/workflow-runs/:run_id/log     — get run log
//   GET    /api/workflow-runs/active          — list active runs
//   POST   /api/mentor/query                  — query the mentor knowledge store
//
// Auth: same Bearer token as admin API.
// ---------------------------------------------------------------------------

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Json};
use serde::Deserialize;
use crate::services::mentor::client::MentorClient;
use crate::services::workflow::{crud, decomposer, escalation};
use crate::services::workflow::session::{SessionManager, SessionManagerConfig};
use crate::types::state::AppState;
use crate::types::workflow::TriggerSource;

// ---------------------------------------------------------------------------
// Auth (reuse pattern from admin.rs)
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
// Query params
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct ListWorkflowsQuery {
    pub project_id: Option<i64>,
    pub all: Option<bool>,
}

#[derive(Deserialize)]
pub struct ListRunsQuery {
    pub limit: Option<u32>,
}

#[derive(Deserialize)]
pub struct RecentSessionsQuery {
    pub limit: Option<u32>,
}

#[derive(Deserialize)]
pub struct CreateWorkflowBody {
    pub description: String,
    #[serde(default)]
    pub project_id: i64,
    pub created_by: Option<String>,
}

#[derive(Deserialize)]
pub struct MentorQueryBody {
    pub question: String,
    pub repo: Option<String>,
    pub limit: Option<u32>,
}

// ---------------------------------------------------------------------------
// Workflow CRUD handlers
// ---------------------------------------------------------------------------

/// GET /api/workflows
pub async fn list_workflows(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ListWorkflowsQuery>,
) -> impl IntoResponse {
    if let Err(e) = check_auth(&state, &headers) {
        return e.into_response();
    }

    let result = if let Some(project_id) = query.project_id {
        crud::list_workflows_for_project(state.pool(), project_id).await
    } else if query.all.unwrap_or(false) {
        crud::list_all_workflows(state.pool()).await
    } else {
        crud::list_enabled_workflows(state.pool()).await
    };

    match result {
        Ok(workflows) => Json(serde_json::json!({
            "ok": true,
            "workflows": workflows,
        }))
        .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "ok": false, "error": e.to_string() })),
        )
            .into_response(),
    }
}

/// POST /api/workflows
pub async fn create_workflow(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<CreateWorkflowBody>,
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

    let decomposer = decomposer::NlDecomposer::new(ai_config, model);
    let created_by = body.created_by.as_deref().unwrap_or("api");

    match decomposer
        .decompose(&body.description, body.project_id, created_by)
        .await
    {
        Ok(definition) => {
            if let Err(e) = crud::create_workflow(state.pool(), &definition).await {
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
                    "workflow": definition,
                })),
            )
                .into_response()
        }
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "ok": false, "error": e.to_string() })),
        )
            .into_response(),
    }
}

/// GET /api/workflows/:id
pub async fn get_workflow(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> impl IntoResponse {
    if let Err(e) = check_auth(&state, &headers) {
        return e.into_response();
    }

    match crud::get_workflow(state.pool(), &id).await {
        Ok(Some(wf)) => Json(serde_json::json!({ "ok": true, "workflow": wf })).into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "ok": false, "error": "workflow not found" })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "ok": false, "error": e.to_string() })),
        )
            .into_response(),
    }
}

/// PUT /api/workflows/:id
pub async fn update_workflow(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(workflow): Json<crate::types::workflow::WorkflowDefinition>,
) -> impl IntoResponse {
    if let Err(e) = check_auth(&state, &headers) {
        return e.into_response();
    }

    if workflow.id.to_string() != id {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "ok": false, "error": "workflow ID mismatch" })),
        )
            .into_response();
    }

    match crud::update_workflow(state.pool(), &workflow).await {
        Ok(true) => Json(serde_json::json!({ "ok": true })).into_response(),
        Ok(false) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "ok": false, "error": "workflow not found" })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "ok": false, "error": e.to_string() })),
        )
            .into_response(),
    }
}

/// DELETE /api/workflows/:id
pub async fn delete_workflow(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> impl IntoResponse {
    if let Err(e) = check_auth(&state, &headers) {
        return e.into_response();
    }

    match crud::delete_workflow(state.pool(), &id).await {
        Ok(true) => Json(serde_json::json!({ "ok": true })).into_response(),
        Ok(false) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "ok": false, "error": "workflow not found" })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "ok": false, "error": e.to_string() })),
        )
            .into_response(),
    }
}

/// POST /api/workflows/:id/enable
pub async fn enable_workflow(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> impl IntoResponse {
    if let Err(e) = check_auth(&state, &headers) {
        return e.into_response();
    }

    match crud::set_workflow_enabled(state.pool(), &id, true).await {
        Ok(true) => Json(serde_json::json!({ "ok": true })).into_response(),
        Ok(false) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "ok": false, "error": "workflow not found" })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "ok": false, "error": e.to_string() })),
        )
            .into_response(),
    }
}

/// POST /api/workflows/:id/disable
pub async fn disable_workflow(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> impl IntoResponse {
    if let Err(e) = check_auth(&state, &headers) {
        return e.into_response();
    }

    match crud::set_workflow_enabled(state.pool(), &id, false).await {
        Ok(true) => Json(serde_json::json!({ "ok": true })).into_response(),
        Ok(false) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "ok": false, "error": "workflow not found" })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "ok": false, "error": e.to_string() })),
        )
            .into_response(),
    }
}

/// POST /api/workflows/:id/trigger
pub async fn trigger_workflow(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> impl IntoResponse {
    if let Err(e) = check_auth(&state, &headers) {
        return e.into_response();
    }

    let workflow = match crud::get_workflow(state.pool(), &id).await {
        Ok(Some(wf)) => wf,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({ "ok": false, "error": "workflow not found" })),
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

    // Fix #5: reject disabled workflows on manual trigger.
    if !workflow.enabled {
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({
                "ok": false,
                "error": "workflow is disabled — enable it first or use PUT to update",
            })),
        )
            .into_response();
    }

    let cfg = state.config();
    let pool = state.pool().clone();
    let mentor = MentorClient::new(pool.clone(), "global".into());

    let agent_config = crate::services::workflow::factory::AgentFactoryConfig {
        gitlab: Some(crate::services::gitlab::client::GitLabConfig {
            base_url: cfg.gitlab.url.clone(),
            token: cfg.gitlab.bot_token.clone(),
        }),
        ai: Some(crate::services::ai::client::AiClientConfig {
            base_url: cfg.ai.base_url.clone(),
            api_key: cfg.ai.api_key.clone(),
        }),
        ai_default_model: cfg.ai.models.workflow_orchestrate.clone(),
        sandbox_max_memory_mb: cfg.sandbox.max_memory_mb,
        pool: pool.clone(),
        botto_config: Some((*cfg).clone()),
        event_bus: Some(state.event_bus().clone()),
    };

    let timeout = cfg.workflows.default_step_timeout_secs;

    // Fix #2: use the shared workflow semaphore for concurrency control.
    let semaphore = state.workflow_semaphore().clone();
    let permit = match semaphore.try_acquire_owned() {
        Ok(p) => p,
        Err(_) => {
            tracing::warn!(workflow_id = %id, "trigger_workflow: max concurrent runs reached");
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({
                    "ok": false,
                    "error": "max concurrent workflow runs reached, try again later",
                })),
            )
                .into_response();
        }
    };

    // Fix #4: create the run ID up front and return it to the caller.
    let run_id = uuid::Uuid::new_v4();

    let wf_name = workflow.name.clone();
    let event_bus = state.event_bus().clone();
    let ai_config_for_session = crate::services::ai::client::AiClientConfig {
        base_url: cfg.ai.base_url.clone(),
        api_key: cfg.ai.api_key.clone(),
    };
    let ai_model = cfg.ai.models.workflow_orchestrate.clone();

    match workflow.mode {
        crate::types::workflow::WorkflowMode::Autonomous => {
            // v2 path: SessionManager with Planner/Generator/Evaluator
            let sm_config = SessionManagerConfig {
                ai_model: ai_model.clone(),
                ..Default::default()
            };
            let manager = SessionManager::new(
                pool.clone(),
                ai_config_for_session,
                agent_config,
                mentor,
                event_bus,
                sm_config,
            );

            let trigger_data = serde_json::json!({
                "trigger": "manual",
                "user": "api",
            });

            tokio::spawn(async move {
                match crate::services::workflow::session::create_session(
                    &pool, workflow.id, "manual", Some(trigger_data),
                ).await {
                    Ok(mut session) => {
                        tracing::info!(workflow = %wf_name, session_id = %session.id, "autonomous session started");
                        if let Err(e) = manager.drive(&mut session, &wf_name).await {
                            tracing::error!(workflow = %wf_name, session_id = %session.id, error = %e, "autonomous session failed");
                        } else {
                            tracing::info!(workflow = %wf_name, session_id = %session.id, status = %session.status, "autonomous session finished");
                        }
                    }
                    Err(e) => {
                        tracing::error!(workflow = %wf_name, error = %e, "failed to create session");
                    }
                }
                drop(permit);
            });
        }
        crate::types::workflow::WorkflowMode::Simple => {
            // v1 path: DAG orchestrator
            let orchestrator =
                crate::services::workflow::orchestrator::Orchestrator::new(pool, mentor, agent_config, timeout);
            let trigger = TriggerSource::Manual {
                user: "api".into(),
            };

            tokio::spawn(async move {
                let _run = orchestrator.execute_with_id(run_id, &workflow, trigger).await;
                drop(permit);
                tracing::debug!(workflow = %wf_name, %run_id, "simple workflow run finished");
            });
        }
    }

    (
        StatusCode::ACCEPTED,
        Json(serde_json::json!({
            "ok": true,
            "message": "workflow triggered",
            "workflow_id": id,
            "run_id": run_id.to_string(),
        })),
    )
        .into_response()
}

// ---------------------------------------------------------------------------
// Run handlers
// ---------------------------------------------------------------------------

/// GET /api/workflows/:id/runs
pub async fn list_runs(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(query): Query<ListRunsQuery>,
) -> impl IntoResponse {
    if let Err(e) = check_auth(&state, &headers) {
        return e.into_response();
    }

    // Fix #6: cap limit to prevent unbounded queries.
    let limit = query.limit.unwrap_or(20).min(100);
    match crud::list_runs_for_workflow(state.pool(), &id, limit).await {
        Ok(runs) => Json(serde_json::json!({ "ok": true, "runs": runs })).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "ok": false, "error": e.to_string() })),
        )
            .into_response(),
    }
}

/// GET /api/workflow-runs/active
pub async fn list_active_runs(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(e) = check_auth(&state, &headers) {
        return e.into_response();
    }

    match crud::list_active_runs(state.pool()).await {
        Ok(runs) => Json(serde_json::json!({ "ok": true, "runs": runs })).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "ok": false, "error": e.to_string() })),
        )
            .into_response(),
    }
}

/// GET /api/workflow-runs/:run_id
pub async fn get_run(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(run_id): Path<String>,
) -> impl IntoResponse {
    if let Err(e) = check_auth(&state, &headers) {
        return e.into_response();
    }

    match crud::get_run(state.pool(), &run_id).await {
        Ok(Some(run)) => Json(serde_json::json!({ "ok": true, "run": run })).into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "ok": false, "error": "run not found" })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "ok": false, "error": e.to_string() })),
        )
            .into_response(),
    }
}

/// GET /api/workflow-runs/:run_id/log
pub async fn get_run_log(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(run_id): Path<String>,
) -> impl IntoResponse {
    if let Err(e) = check_auth(&state, &headers) {
        return e.into_response();
    }

    match crud::get_run_log(state.pool(), &run_id).await {
        Ok(entries) => {
            let log: Vec<serde_json::Value> = entries
                .into_iter()
                .map(|(step_id, event_type, data, created_at)| {
                    serde_json::json!({
                        "step_id": step_id,
                        "event_type": event_type,
                        "data": data,
                        "created_at": created_at,
                    })
                })
                .collect();
            Json(serde_json::json!({ "ok": true, "log": log })).into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "ok": false, "error": e.to_string() })),
        )
            .into_response(),
    }
}

// ---------------------------------------------------------------------------
// Mentor query handler
// ---------------------------------------------------------------------------

/// POST /api/mentor/query
pub async fn mentor_query(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<MentorQueryBody>,
) -> impl IntoResponse {
    if let Err(e) = check_auth(&state, &headers) {
        return e.into_response();
    }

    let repo = body.repo.unwrap_or_else(|| "global".into());
    let limit = body.limit.unwrap_or(10);
    let client = MentorClient::new(state.pool().clone(), repo);

    match client.query(&body.question, limit).await {
        Ok(results) => {
            let entries: Vec<serde_json::Value> = results
                .iter()
                .map(|r| {
                    serde_json::json!({
                        "id": r.id,
                        "content": r.content,
                        "scope": r.scope,
                        "scope_type": r.scope_type,
                        "category": r.category,
                        "confidence": r.confidence,
                        "hit_count": r.hit_count,
                    })
                })
                .collect();
            Json(serde_json::json!({ "ok": true, "results": entries })).into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "ok": false, "error": e.to_string() })),
        )
            .into_response(),
    }
}

// ---------------------------------------------------------------------------
// Session handlers (v2 orchestrator)
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct RespondToSessionBody {
    pub content: String,
    pub option: Option<String>,
}

/// POST /api/workflows/sessions/{id}/respond
pub async fn respond_to_session(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(body): Json<RespondToSessionBody>,
) -> impl IntoResponse {
    if let Err(e) = check_auth(&state, &headers) {
        return e.into_response();
    }

    let pool = state.pool().clone();
    let event_bus = state.event_bus().clone();

    // Load the session.
    let mut session = match crud::load_session(&pool, &id).await {
        Ok(Some(s)) => s,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({ "ok": false, "error": "session not found" })),
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

    // Validate it's waiting for human input or clarification.
    if session.status != crate::types::workflow::SessionStatus::WaitingForHuman
        && session.status != crate::types::workflow::SessionStatus::Clarifying
    {
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({
                "ok": false,
                "error": format!("session is in '{}' state, not 'waiting_for_human' or 'clarifying'", session.status),
            })),
        )
            .into_response();
    }

    // Handle the response.
    let new_status = match escalation::handle_response(
        &pool,
        &mut session,
        &event_bus,
        &body.content,
        body.option.as_deref(),
    )
    .await
    {
        Ok(status) => status,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "ok": false, "error": e.to_string() })),
            )
                .into_response();
        }
    };

    // If the session is not terminal, spawn it to continue running.
    if !new_status.is_terminal() {
        let cfg = state.config();
        let ai_config = crate::services::ai::client::AiClientConfig {
            base_url: cfg.ai.base_url.clone(),
            api_key: cfg.ai.api_key.clone(),
        };
        let agent_config = crate::services::workflow::factory::AgentFactoryConfig {
            gitlab: Some(crate::services::gitlab::client::GitLabConfig {
                base_url: cfg.gitlab.url.clone(),
                token: cfg.gitlab.bot_token.clone(),
            }),
            ai: Some(crate::services::ai::client::AiClientConfig {
                base_url: cfg.ai.base_url.clone(),
                api_key: cfg.ai.api_key.clone(),
            }),
            ai_default_model: cfg.ai.models.workflow_orchestrate.clone(),
            sandbox_max_memory_mb: cfg.sandbox.max_memory_mb,
            pool: pool.clone(),
            botto_config: Some((*cfg).clone()),
            event_bus: Some(state.event_bus().clone()),
        };
        let mentor = MentorClient::new(pool.clone(), "global".into());
        let sm_config = SessionManagerConfig {
            ai_model: cfg.ai.models.workflow_orchestrate.clone(),
            ..Default::default()
        };
        let manager = SessionManager::new(
            pool.clone(),
            ai_config,
            agent_config,
            mentor,
            event_bus,
            sm_config,
        );

        let wf_name = crud::get_workflow(&pool, &session.workflow_id.to_string())
            .await
            .ok()
            .flatten()
            .map(|w| w.name)
            .unwrap_or_else(|| "unknown".into());

        tokio::spawn(async move {
            if let Err(e) = manager.drive(&mut session, &wf_name).await {
                tracing::warn!(
                    session_id = %session.id,
                    error = %e,
                    "respond_to_session: session drive failed"
                );
            }
        });
    }

    Json(serde_json::json!({
        "ok": true,
        "new_status": new_status.as_str(),
    }))
    .into_response()
}

/// GET /api/workflows/sessions/{id}/messages
pub async fn session_messages(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> impl IntoResponse {
    if let Err(e) = check_auth(&state, &headers) {
        return e.into_response();
    }

    match crud::load_session_messages(state.pool(), &id, 200).await {
        Ok(messages) => Json(serde_json::json!({
            "ok": true,
            "messages": messages,
        }))
        .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "ok": false, "error": e.to_string() })),
        )
            .into_response(),
    }
}

/// GET /api/workflows/sessions/waiting
pub async fn waiting_sessions(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(e) = check_auth(&state, &headers) {
        return e.into_response();
    }

    match crud::load_waiting_sessions(state.pool()).await {
        Ok(sessions) => Json(serde_json::json!({
            "ok": true,
            "sessions": sessions,
        }))
        .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "ok": false, "error": e.to_string() })),
        )
            .into_response(),
    }
}

/// GET /api/workflows/sessions/{id}
pub async fn get_session(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> impl IntoResponse {
    if let Err(e) = check_auth(&state, &headers) {
        return e.into_response();
    }

    match crud::load_session(state.pool(), &id).await {
        Ok(Some(session)) => Json(serde_json::json!({
            "ok": true,
            "session": session,
        }))
        .into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "ok": false, "error": "session not found" })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "ok": false, "error": e.to_string() })),
        )
            .into_response(),
    }
}

/// GET /api/workflows/sessions/active
pub async fn active_sessions(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(e) = check_auth(&state, &headers) {
        return e.into_response();
    }

    match crud::load_active_sessions(state.pool()).await {
        Ok(sessions) => Json(serde_json::json!({
            "ok": true,
            "sessions": sessions,
        }))
        .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "ok": false, "error": e.to_string() })),
        )
            .into_response(),
    }
}

/// GET /api/workflows/sessions/{id}/trace
pub async fn session_trace(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> impl IntoResponse {
    if let Err(e) = check_auth(&state, &headers) {
        return e.into_response();
    }

    match crud::load_trace(state.pool(), &id).await {
        Ok(events) => Json(serde_json::json!({
            "ok": true,
            "trace": events,
        }))
        .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "ok": false, "error": e.to_string() })),
        )
            .into_response(),
    }
}

/// GET /api/workflows/sessions/recent
pub async fn recent_sessions(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<RecentSessionsQuery>,
) -> impl IntoResponse {
    if let Err(e) = check_auth(&state, &headers) {
        return e.into_response();
    }

    let limit = query.limit.unwrap_or(20).min(100);
    match crud::load_recent_sessions(state.pool(), limit).await {
        Ok(sessions) => Json(serde_json::json!({
            "ok": true,
            "sessions": sessions,
        }))
        .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "ok": false, "error": e.to_string() })),
        )
            .into_response(),
    }
}
