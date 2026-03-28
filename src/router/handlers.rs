// ---------------------------------------------------------------------------
// Request handlers — one function per Otto message type.
//
// Every handler takes &AppState + the request payload, returns a JSON Value
// in the Result<T> shape: { ok: true, data: ... } or { ok: false, error: ... }
// ---------------------------------------------------------------------------

#![allow(unused_variables, unused_imports)]

use crate::api::ws::WsOutbound;
use crate::config;
use crate::db;
use crate::types::state::AppState;
use serde_json::{json, Value};
use tokio::sync::{broadcast, watch};
use tracing::{info, warn};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn ok(data: Value) -> Value {
    json!({ "ok": true, "data": data })
}

fn err(msg: &str) -> Value {
    json!({ "ok": false, "error": msg })
}

/// Extract a string field, trying snake_case first then camelCase.
/// This handles both Botto-native messages (snake_case) and Otto-routed
/// messages (camelCase) transparently.
fn extract_str<'a>(payload: &'a Value, key: &str) -> Option<&'a str> {
    payload
        .get(key)
        .and_then(|v| v.as_str())
        .or_else(|| payload.get(&to_camel(key)).and_then(|v| v.as_str()))
}

fn extract_i64(payload: &Value, key: &str) -> Option<i64> {
    payload
        .get(key)
        .and_then(|v| v.as_i64())
        .or_else(|| payload.get(&to_camel(key)).and_then(|v| v.as_i64()))
}

fn extract_u64(payload: &Value, key: &str) -> Option<u64> {
    payload
        .get(key)
        .and_then(|v| v.as_u64())
        .or_else(|| payload.get(&to_camel(key)).and_then(|v| v.as_u64()))
}

/// Validate a project_path looks reasonable (non-empty, contains a slash, no control chars).
fn validate_project_path(path: &str) -> bool {
    !path.is_empty()
        && path.contains('/')
        && path.len() <= 500
        && path.chars().all(|c| !c.is_control())
}

/// Convert snake_case to camelCase: "project_path" → "projectPath"
fn to_camel(snake: &str) -> String {
    let mut result = String::new();
    let mut capitalize_next = false;
    for ch in snake.chars() {
        if ch == '_' {
            capitalize_next = true;
        } else if capitalize_next {
            result.push(ch.to_ascii_uppercase());
            capitalize_next = false;
        } else {
            result.push(ch);
        }
    }
    result
}

// ---------------------------------------------------------------------------
// Settings
// ---------------------------------------------------------------------------

pub async fn get_settings(state: &AppState) -> Value {
    let cfg = state.config();
    ok(json!({
        "sandbox_enabled": cfg.sandbox.enabled,
        "shared_triage_available": true,
        "gitlab_url": cfg.gitlab.url,
        "ai_configured": !cfg.ai.base_url.is_empty(),
        "conflict_radar_enabled": cfg.conflict.enabled,
        "cluster_enabled": cfg.cluster.enabled,
        "auto_review_on_push": cfg.review.auto_review_on_push,
        "warm_containers": cfg.sandbox.warm_containers,
        "version": env!("CARGO_PKG_VERSION"),
    }))
}

// ---------------------------------------------------------------------------
// GitLab operations (delegated to Botto's bot credentials)
// ---------------------------------------------------------------------------

pub async fn test_gitlab_connection(state: &AppState, _payload: &Value) -> Value {
    let cfg = state.config();
    if cfg.gitlab.bot_token.is_empty() {
        return err("GitLab bot token not configured");
    }
    let gl_cfg = crate::services::gitlab::client::GitLabConfig {
        base_url: cfg.gitlab.url.clone(),
        token: cfg.gitlab.bot_token.clone(),
    };
    match crate::services::gitlab::client::test_connection(&gl_cfg).await {
        Ok(user) => ok(json!({
            "username": user.username,
            "url": cfg.gitlab.url,
        })),
        Err(e) => err(&format!("GitLab connection failed: {}", e)),
    }
}

pub async fn fetch_project(state: &AppState, payload: &Value) -> Value {
    let project_path = match extract_str(payload, "project_path") {
        Some(p) if validate_project_path(p) => p,
        Some(_) => return err("invalid project_path format"),
        None => return err("missing project_path"),
    };
    let cfg = state.config();
    let gl_cfg = crate::services::gitlab::client::GitLabConfig {
        base_url: cfg.gitlab.url.clone(),
        token: cfg.gitlab.bot_token.clone(),
    };
    match crate::services::gitlab::client::fetch_project(&gl_cfg, project_path).await {
        Ok(project) => ok(serde_json::to_value(&project).unwrap_or_default()),
        Err(e) => err(&format!("failed to fetch project: {}", e)),
    }
}

pub async fn fetch_mr_metadata(state: &AppState, payload: &Value) -> Value {
    let project_id = match extract_i64(payload, "project_id") {
        Some(i) => i,
        None => return err("missing project_id"),
    };
    let mr_iid = match extract_u64(payload, "mr_iid") {
        Some(i) => i,
        None => return err("missing mr_iid"),
    };
    let cfg = state.config();
    let gl_cfg = crate::services::gitlab::client::GitLabConfig {
        base_url: cfg.gitlab.url.clone(),
        token: cfg.gitlab.bot_token.clone(),
    };
    match crate::services::gitlab::client::fetch_merge_request(&gl_cfg, project_id, mr_iid).await {
        Ok(mr) => ok(serde_json::to_value(&mr).unwrap_or_default()),
        Err(e) => err(&format!("failed to fetch MR metadata: {}", e)),
    }
}

pub async fn fetch_mr_changes(state: &AppState, payload: &Value) -> Value {
    let mr_iid = match extract_u64(payload, "mr_iid") {
        Some(i) => i,
        None => return err("missing mr_iid"),
    };
    let cfg = state.config();
    let gl_cfg = crate::services::gitlab::client::GitLabConfig {
        base_url: cfg.gitlab.url.clone(),
        token: cfg.gitlab.bot_token.clone(),
    };

    // Otto sends projectId (number), Botto-native may send project_path (string).
    // Accept either: resolve project_path → id if needed.
    let project_id = if let Some(id) = extract_i64(payload, "project_id") {
        id
    } else if let Some(path) = extract_str(payload, "project_path") {
        match crate::services::gitlab::client::fetch_project(&gl_cfg, path).await {
            Ok(project) => project.id,
            Err(e) => return err(&format!("failed to resolve project: {}", e)),
        }
    } else {
        return err("missing project_id or project_path");
    };

    match crate::services::gitlab::client::fetch_mr_changes(&gl_cfg, project_id, mr_iid).await {
        Ok(changes) => ok(serde_json::to_value(&changes).unwrap_or_default()),
        Err(e) => err(&format!("failed to fetch MR changes: {}", e)),
    }
}

pub async fn fetch_file_content(state: &AppState, payload: &Value) -> Value {
    let project_id = match extract_i64(payload, "project_id") {
        Some(i) => i,
        None => return err("missing project_id"),
    };
    let file_path = match extract_str(payload, "file_path") {
        Some(p) => p,
        None => return err("missing file_path"),
    };
    let ref_name = extract_str(payload, "ref").unwrap_or("main");
    let cfg = state.config();
    let gl_cfg = crate::services::gitlab::client::GitLabConfig {
        base_url: cfg.gitlab.url.clone(),
        token: cfg.gitlab.bot_token.clone(),
    };
    match crate::services::gitlab::client::fetch_file_content(&gl_cfg, project_id, file_path, ref_name).await {
        Ok(content) => ok(json!({ "content": content })),
        Err(e) => err(&format!("failed to fetch file: {}", e)),
    }
}

pub async fn fetch_file_tree(state: &AppState, payload: &Value) -> Value {
    let project_id = match extract_i64(payload, "project_id") {
        Some(i) => i,
        None => return err("missing project_id"),
    };
    let path = extract_str(payload, "path").unwrap_or("");
    let ref_name = extract_str(payload, "ref").unwrap_or("main");
    let recursive = payload.get("recursive").and_then(|v| v.as_bool()).unwrap_or(false);
    let cfg = state.config();
    let gl_cfg = crate::services::gitlab::client::GitLabConfig {
        base_url: cfg.gitlab.url.clone(),
        token: cfg.gitlab.bot_token.clone(),
    };
    match crate::services::gitlab::client::fetch_file_tree(&gl_cfg, project_id, path, ref_name, recursive).await {
        Ok(tree) => ok(serde_json::to_value(&tree).unwrap_or_default()),
        Err(e) => err(&format!("failed to fetch file tree: {}", e)),
    }
}

pub async fn fetch_mr_discussions(state: &AppState, payload: &Value) -> Value {
    let project_id = match extract_i64(payload, "project_id") {
        Some(i) => i,
        None => return err("missing project_id"),
    };
    let mr_iid = match extract_u64(payload, "mr_iid") {
        Some(i) => i,
        None => return err("missing mr_iid"),
    };
    let cfg = state.config();
    let gl_cfg = crate::services::gitlab::client::GitLabConfig {
        base_url: cfg.gitlab.url.clone(),
        token: cfg.gitlab.bot_token.clone(),
    };
    match crate::services::gitlab::client::fetch_mr_discussions(&gl_cfg, project_id, mr_iid).await {
        Ok(discussions) => ok(serde_json::to_value(&discussions).unwrap_or_default()),
        Err(e) => err(&format!("failed to fetch discussions: {}", e)),
    }
}

pub async fn fetch_ticket(state: &AppState, payload: &Value) -> Value {
    let ticket_key = match extract_str(payload, "ticket_key") {
        Some(k) => k,
        None => return err("missing ticket_key"),
    };

    // Jira config can come from the request payload (Otto sends provider info)
    // or from server-side config in the future.
    let base_url = match extract_str(payload, "base_url") {
        Some(u) => u,
        None => return err("missing base_url for ticket provider"),
    };
    let email = match extract_str(payload, "email") {
        Some(e) => e,
        None => return err("missing email for ticket provider"),
    };
    let api_token = match extract_str(payload, "api_token") {
        Some(t) => t,
        None => return err("missing api_token for ticket provider"),
    };

    let jira_cfg = crate::services::ticket::jira::JiraConfig {
        base_url: base_url.to_string(),
        email: email.to_string(),
        api_token: api_token.to_string(),
    };

    match crate::services::ticket::jira::fetch_ticket(&jira_cfg, ticket_key).await {
        Ok(ticket) => ok(serde_json::to_value(&ticket).unwrap_or_default()),
        Err(e) => err(&format!("failed to fetch ticket: {}", e)),
    }
}

pub async fn fetch_ticket_batch(state: &AppState, payload: &Value) -> Value {
    let ticket_keys = match payload.get("ticket_keys").and_then(|v| v.as_array()) {
        Some(keys) => keys.iter().filter_map(|k| k.as_str()).collect::<Vec<_>>(),
        None => return err("missing ticket_keys"),
    };

    let base_url = match extract_str(payload, "base_url") {
        Some(u) => u,
        None => return err("missing base_url for ticket provider"),
    };
    let email = match extract_str(payload, "email") {
        Some(e) => e,
        None => return err("missing email for ticket provider"),
    };
    let api_token = match extract_str(payload, "api_token") {
        Some(t) => t,
        None => return err("missing api_token for ticket provider"),
    };

    let jira_cfg = crate::services::ticket::jira::JiraConfig {
        base_url: base_url.to_string(),
        email: email.to_string(),
        api_token: api_token.to_string(),
    };

    let mut results = serde_json::Map::new();
    for key in ticket_keys {
        match crate::services::ticket::jira::fetch_ticket(&jira_cfg, key).await {
            Ok(ticket) => {
                results.insert(
                    key.to_string(),
                    serde_json::to_value(&ticket).unwrap_or_default(),
                );
            }
            Err(e) => {
                warn!("failed to fetch ticket {}: {}", key, e);
            }
        }
    }
    ok(Value::Object(results))
}

// ---------------------------------------------------------------------------
// Review cache
// ---------------------------------------------------------------------------

pub async fn get_cached_review(state: &AppState, payload: &Value) -> Value {
    let project_path = match extract_str(payload, "project_path") {
        Some(p) => p,
        None => return err("missing project_path"),
    };
    let mr_iid = match extract_i64(payload, "mr_iid") {
        Some(i) => i,
        None => return err("missing mr_iid"),
    };
    let diff_hash = match extract_str(payload, "diff_hash") {
        Some(h) => h,
        None => return err("missing diff_hash"),
    };

    match db::queries::get_cached_review(state.pool(), project_path, mr_iid, diff_hash).await {
        Ok(Some(row)) => {
            let data: Vec<u8> = row.0;
            let file_hashes: String = row.1;
            let review_json = crate::router::decompress_or_raw(&data);
            match serde_json::from_slice::<Value>(&review_json) {
                Ok(review) => ok(json!({
                    "review": review,
                    "file_diff_hashes": file_hashes,
                })),
                Err(e) => err(&format!("corrupt cached review: {}", e)),
            }
        }
        Ok(None) => ok(Value::Null),
        Err(e) => err(&format!("cache read error: {}", e)),
    }
}

// ---------------------------------------------------------------------------
// Comment actions
// ---------------------------------------------------------------------------

pub async fn get_comment_actions(state: &AppState, payload: &Value) -> Value {
    let project_path = match extract_str(payload, "project_path") {
        Some(p) => p,
        None => return err("missing project_path"),
    };
    let mr_iid = match extract_i64(payload, "mr_iid") {
        Some(i) => i,
        None => return err("missing mr_iid"),
    };

    match db::queries::get_comment_actions(state.pool(), project_path, mr_iid).await {
        Ok(rows) => {
            let actions_json: Vec<Value> = rows
                .into_iter()
                .map(|row: (String, String, String, Option<String>, i64, Option<String>, Option<String>)| {
                    json!({
                        "comment_id": row.0,
                        "user_id": row.1,
                        "action": row.2,
                        "edited_body": row.3,
                        "created_at": row.4,
                        "category": row.5,
                        "severity": row.6,
                    })
                })
                .collect();
            ok(json!(actions_json))
        }
        Err(e) => err(&format!("failed to get comment actions: {}", e)),
    }
}

// ---------------------------------------------------------------------------
// Team settings
// ---------------------------------------------------------------------------

pub async fn get_team_settings(state: &AppState, payload: &Value) -> Value {
    let project_path = match extract_str(payload, "project_path") {
        Some(p) => p,
        None => return err("missing project_path"),
    };

    match db::queries::get_shared_triage(state.pool(), project_path).await {
        Ok(shared) => ok(json!({ "shared_triage": shared })),
        Err(e) => err(&format!("failed to get team settings: {}", e)),
    }
}

pub async fn set_team_settings(state: &AppState, payload: &Value) -> Value {
    let project_path = match extract_str(payload, "project_path") {
        Some(p) => p,
        None => return err("missing project_path"),
    };
    let shared_triage = payload
        .get("shared_triage")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    match db::queries::set_shared_triage(state.pool(), project_path, shared_triage).await {
        Ok(()) => ok(json!({ "shared_triage": shared_triage })),
        Err(e) => err(&format!("failed to set team settings: {}", e)),
    }
}

// ---------------------------------------------------------------------------
// Review queue
// ---------------------------------------------------------------------------

pub async fn get_queue_status(state: &AppState, payload: &Value) -> Value {
    let project_path = match extract_str(payload, "project_path") {
        Some(p) => p,
        None => return err("missing project_path"),
    };

    match db::queries::get_queue_items(state.pool(), project_path).await {
        Ok(rows) => {
            let items: Vec<Value> = rows
                .iter()
                .map(|r| {
                    json!({
                        "id": r.0,
                        "project_path": r.1,
                        "mr_iid": r.2,
                        "priority_score": r.3,
                        "status": r.4,
                        "error": r.5,
                        "enqueued_at": r.6,
                    })
                })
                .collect();
            let active = rows.iter().find(|r| r.4 == "running").map(|r| {
                json!({ "project_path": r.1, "mr_iid": r.2 })
            });
            ok(json!({ "items": items, "active": active }))
        }
        Err(e) => err(&format!("failed to query queue: {}", e)),
    }
}

pub async fn enqueue_review(state: &AppState, payload: &Value) -> Value {
    let project_path = match extract_str(payload, "project_path") {
        Some(p) => p,
        None => return err("missing project_path"),
    };
    let mr_iid = match extract_i64(payload, "mr_iid") {
        Some(i) => i,
        None => return err("missing mr_iid"),
    };
    let priority_score = payload
        .get("priority_score")
        .and_then(|v| v.as_f64())
        .unwrap_or(50.0);

    let mr_context_json = payload
        .get("mr_context")
        .map(|v| serde_json::to_vec(v).unwrap_or_default())
        .unwrap_or_default();

    match db::queries::enqueue_review(state.pool(), project_path, mr_iid, priority_score, &mr_context_json).await {
        Ok(()) => ok(json!({
            "project_path": project_path,
            "mr_iid": mr_iid,
            "priority_score": priority_score,
            "status": "queued",
        })),
        Err(e) => err(&format!("failed to enqueue: {}", e)),
    }
}

pub async fn pause_review(state: &AppState, payload: &Value) -> Value {
    let project_path = match extract_str(payload, "project_path") {
        Some(p) => p,
        None => return err("missing project_path"),
    };
    let mr_iid = match extract_i64(payload, "mr_iid") {
        Some(i) => i,
        None => return err("missing mr_iid"),
    };

    match db::queries::update_queue_status(state.pool(), project_path, mr_iid, &["queued", "running"], "paused").await {
        Ok(affected) => ok(json!(affected > 0)),
        Err(e) => err(&format!("failed to pause review: {}", e)),
    }
}

pub async fn resume_review(state: &AppState, payload: &Value) -> Value {
    let project_path = match extract_str(payload, "project_path") {
        Some(p) => p,
        None => return err("missing project_path"),
    };
    let mr_iid = match extract_i64(payload, "mr_iid") {
        Some(i) => i,
        None => return err("missing mr_iid"),
    };

    match db::queries::update_queue_status(state.pool(), project_path, mr_iid, &["paused"], "queued").await {
        Ok(affected) => ok(json!(affected > 0)),
        Err(e) => err(&format!("failed to resume review: {}", e)),
    }
}

pub async fn cancel_review(state: &AppState, payload: &Value) -> Value {
    let project_path = match extract_str(payload, "project_path") {
        Some(p) => p,
        None => return err("missing project_path"),
    };
    let mr_iid = match extract_i64(payload, "mr_iid") {
        Some(i) => i,
        None => return err("missing mr_iid"),
    };

    match db::queries::delete_queue_item(state.pool(), project_path, mr_iid).await {
        Ok(affected) => ok(json!(affected > 0)),
        Err(e) => err(&format!("failed to cancel review: {}", e)),
    }
}

// ---------------------------------------------------------------------------
// Sandbox
// ---------------------------------------------------------------------------

pub async fn get_sandbox_job(state: &AppState, payload: &Value) -> Value {
    let job_id = match extract_str(payload, "job_id") {
        Some(j) => j,
        None => return err("missing job_id"),
    };

    match db::queries::get_sandbox_job(state.pool(), job_id).await {
        Ok(Some(r)) => ok(json!({
            "id": r.0,
            "project_path": r.1,
            "mr_iid": r.2,
            "comment_id": r.3,
            "status": r.4,
            "strategy": r.5,
            "container_id": r.6,
            "fix_diff": r.7,
            "test_output": r.8,
            "commit_sha": r.9,
            "error": r.10,
            "created_at": r.11,
            "updated_at": r.12,
        })),
        Ok(None) => ok(Value::Null),
        Err(e) => err(&format!("failed to query sandbox job: {}", e)),
    }
}

// ---------------------------------------------------------------------------
// Streaming handlers
// ---------------------------------------------------------------------------

pub async fn stream_review(
    state: &AppState,
    conn_id: &str,
    user_id: &str,
    stream_id: &str,
    payload: &Value,
    tx: &broadcast::Sender<String>,
    cancel_rx: watch::Receiver<bool>,
) {
    use crate::services::review::orchestrator;
    use crate::types::review::MrContext;
    use crate::types::state::InFlightReview;

    // Parse MrContext from payload
    let mr_context: MrContext = match payload.get("mrContext") {
        Some(ctx) => match serde_json::from_value(ctx.clone()) {
            Ok(mr) => mr,
            Err(e) => {
                let msg = WsOutbound::StreamChunk {
                    stream_id: stream_id.to_string(),
                    chunk: json!({ "type": "STREAM_TASK_ERROR", "payload": { "task": "init", "error": format!("invalid mrContext: {}", e) } }),
                };
                let _ = tx.send(serde_json::to_string(&msg).unwrap_or_default());
                let _ = tx.send(serde_json::to_string(&WsOutbound::StreamEnd { stream_id: stream_id.to_string() }).unwrap_or_default());
                return;
            }
        },
        None => {
            let msg = WsOutbound::StreamChunk {
                stream_id: stream_id.to_string(),
                chunk: json!({ "type": "STREAM_TASK_ERROR", "payload": { "task": "init", "error": "missing mrContext" } }),
            };
            let _ = tx.send(serde_json::to_string(&msg).unwrap_or_default());
            let _ = tx.send(serde_json::to_string(&WsOutbound::StreamEnd { stream_id: stream_id.to_string() }).unwrap_or_default());
            return;
        }
    };

    let mr_ref = crate::types::state::MrRef {
        project_path: mr_context.project_path.clone(),
        mr_iid: mr_context.mr_iid,
    };
    let mr_key = mr_ref.key();

    // Check if this is a forced regeneration (skip cache)
    let skip_cache = payload
        .get("skipCache")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    // If regenerating, clear the in-flight entry so we don't dedup against a stale review
    if skip_cache {
        state.in_flight().remove(&mr_key);
    }

    // --- In-flight deduplication (atomic check-and-insert) ---
    // Use DashMap::entry() to avoid TOCTOU race where two simultaneous
    // requests both see get() return None and both start reviews.
    let existing_in_flight = {
        use dashmap::mapref::entry::Entry;
        match state.in_flight().entry(mr_key.clone()) {
            Entry::Occupied(entry) => Some(entry.get().clone()),
            Entry::Vacant(entry) => {
                // Atomically insert the new in-flight review while holding the lock
                let in_flight = InFlightReview::new();
                entry.insert(in_flight);
                None
            }
        }
    };

    if let Some(in_flight) = existing_in_flight {
        // Late-join: another Otto already triggered a review for this MR.
        // Replay buffered chunks, then subscribe to live stream.

        tracing::info!(
            "late-join review: {}:!{} for conn={}",
            mr_context.project_path, mr_context.mr_iid, conn_id
        );

        // 1. Replay all chunks emitted so far
        let replay = in_flight.replay_buffer();
        for chunk in &replay {
            let msg = WsOutbound::StreamChunk {
                stream_id: stream_id.to_string(),
                chunk: chunk.clone(),
            };
            let _ = tx.send(serde_json::to_string(&msg).unwrap_or_default());
        }

        // 2. If already complete, we're done
        if in_flight.is_complete() {
            let _ = tx.send(
                serde_json::to_string(&WsOutbound::StreamEnd {
                    stream_id: stream_id.to_string(),
                })
                .unwrap_or_default(),
            );
            return;
        }

        // 3. Subscribe to live chunks until completion
        let mut live_rx = in_flight.subscribe_live();
        let mut completed_rx = in_flight.completed.subscribe();
        let stream_id_owned = stream_id.to_string();
        let tx_clone = tx.clone();

        loop {
            tokio::select! {
                chunk = live_rx.recv() => {
                    match chunk {
                        Ok(c) => {
                            let msg = WsOutbound::StreamChunk {
                                stream_id: stream_id_owned.clone(),
                                chunk: c,
                            };
                            let _ = tx_clone.send(serde_json::to_string(&msg).unwrap_or_default());
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            tracing::warn!("late-join lagged by {} chunks", n);
                        }
                        Err(_) => break,
                    }
                }
                _ = completed_rx.changed() => {
                    if *completed_rx.borrow() {
                        break;
                    }
                }
            }
        }

        let _ = tx.send(
            serde_json::to_string(&WsOutbound::StreamEnd {
                stream_id: stream_id.to_string(),
            })
            .unwrap_or_default(),
        );
        return;
    }

    // --- No in-flight review: we already inserted via entry() above ---
    // Safe to unwrap: we just inserted in the Vacant arm above and no code
    // path removes it before this point. Use expect for clarity.
    let in_flight = state.in_flight().get(&mr_key)
        .expect("in-flight entry was just inserted")
        .clone();

    // Acquire review concurrency permit (limits how many MR reviews run at once).
    // Late-joiners (above) skip this — they piggyback on the existing review.
    let _review_permit = {
        let sem = state.review_semaphore().clone();
        if sem.available_permits() == 0 {
            let _ = tx.send(serde_json::to_string(&WsOutbound::StreamChunk {
                stream_id: stream_id.to_string(),
                chunk: json!({ "type": "STREAM_PROGRESS", "payload": { "message": "waiting for review slot..." } }),
            }).unwrap_or_default());
        }
        sem.acquire_owned().await.expect("review semaphore closed")
    };

    // Publish review started event
    state.event_bus().publish(crate::services::events::Event {
        event_type: crate::services::events::EventType::ReviewStarted,
        project_path: mr_context.project_path.clone(),
        mr_iid: Some(mr_context.mr_iid),
        user_id: Some(user_id.to_string()),
        payload: None,
    });

    // Create a channel for orchestrator → forwarding
    let (chunk_tx, mut chunk_rx) = tokio::sync::mpsc::channel::<Value>(128);

    // Bridge cancellation: watch::Receiver → CancellationToken
    let cancel_token = tokio_util::sync::CancellationToken::new();
    let cancel_token_clone = cancel_token.clone();
    let mut cancel_rx = cancel_rx;
    tokio::spawn(async move {
        while cancel_rx.changed().await.is_ok() {
            if *cancel_rx.borrow() {
                cancel_token_clone.cancel();
                break;
            }
        }
    });

    // Forward chunks: orchestrator → in_flight (replay + live) → all viewers
    let tx_clone = tx.clone();
    let stream_id_owned = stream_id.to_string();
    let in_flight_clone = in_flight.clone();

    let forwarder = tokio::spawn(async move {
        while let Some(chunk) = chunk_rx.recv().await {
            // Record in the in-flight buffer (for late-joiners via dedup)
            in_flight_clone.emit(chunk.clone());

            // Send to the requesting Otto
            let msg = WsOutbound::StreamChunk {
                stream_id: stream_id_owned.clone(),
                chunk,
            };
            let _ = tx_clone.send(serde_json::to_string(&msg).unwrap_or_default());

            // NOTE: We do NOT broadcast to other MR viewers here.
            // Other Ottos that request the same review will late-join via
            // the in-flight dedup mechanism, which replays buffered chunks
            // with their own stream_id. Broadcasting raw STREAM_CHUNKs with
            // the originator's stream_id would be dropped by other clients
            // (they have no matching stream handler for that ID).
        }
    });

    // Run the orchestrator
    let tasks = orchestrator::all_tasks();
    let ai_sem = state.ai_semaphore().clone();
    let _result = orchestrator::execute_review(
        &state.config(),
        state.pool(),
        &mr_context,
        &tasks,
        chunk_tx,
        cancel_token,
        skip_cache,
        Some(ai_sem),
    )
    .await;

    // Wait for forwarder to drain
    let _ = forwarder.await;

    // Mark in-flight as complete and remove
    in_flight.finish();
    state.in_flight().remove(&mr_key);

    // Publish completion event
    state.event_bus().publish(crate::services::events::Event {
        event_type: crate::services::events::EventType::ReviewComplete,
        project_path: mr_context.project_path.clone(),
        mr_iid: Some(mr_context.mr_iid),
        user_id: Some(user_id.to_string()),
        payload: None,
    });

    // Send stream end
    let _ = tx.send(
        serde_json::to_string(&WsOutbound::StreamEnd {
            stream_id: stream_id.to_string(),
        })
        .unwrap_or_default(),
    );
}

pub async fn stream_chat(
    state: &AppState,
    stream_id: &str,
    payload: &Value,
    tx: &broadcast::Sender<String>,
    cancel_rx: watch::Receiver<bool>,
) {
    use crate::services::ai::client::ChatMessage;
    use crate::services::ai::service;

    let send_chunk = |chunk: Value| {
        let msg = WsOutbound::StreamChunk {
            stream_id: stream_id.to_string(),
            chunk,
        };
        let _ = tx.send(serde_json::to_string(&msg).unwrap_or_default());
    };

    // Check AI is configured
    if state.config().ai.base_url.is_empty() {
        send_chunk(json!({
            "type": "STREAM_CHAT_ERROR",
            "payload": { "error": "AI provider not configured on this Botto server" },
        }));
        let _ = tx.send(serde_json::to_string(&WsOutbound::StreamEnd { stream_id: stream_id.to_string() }).unwrap_or_default());
        return;
    }

    // Parse chat payload
    let question = payload.get("question").and_then(|v| v.as_str()).unwrap_or("");

    // Build structured review context from Otto's ChatReviewContext JSON object.
    // Otto sends reviewContext as { mrContext, summary, fileReviews, edgeCases, relatedFiles }.
    // We parse it into a structured context string matching Otto's buildContextMessage().
    let review_context = crate::services::ai::prompts::chat::build_context_from_payload(payload);

    // Extract project_path from the review context to fetch cached repo config.
    // The project_path lives inside reviewContext.mrContext.projectPath.
    let project_path = payload
        .get("reviewContext")
        .and_then(|rc| rc.get("mrContext"))
        .and_then(|mr| mr.get("projectPath"))
        .and_then(|v| v.as_str())
        .unwrap_or("");

    // Fetch cached repo config (non-blocking — returns None if not cached).
    // We don't trigger a GitLab API fetch here because chat is latency-sensitive.
    // The config will already be cached if a review was run on this project.
    let repo_config_text = if !project_path.is_empty() {
        crate::services::repo_config::get_cached_formatted(state.pool(), project_path).await
    } else {
        None
    };

    // Build message history
    let mut messages = vec![
        crate::services::ai::prompts::chat::build_system(&review_context, state.config().ai.custom_prompts.get("chat"), repo_config_text.as_deref()),
    ];

    // Add conversation history if provided
    if let Some(history) = payload.get("history").and_then(|v| v.as_array()) {
        for msg in history {
            let role = msg.get("role").and_then(|v| v.as_str()).unwrap_or("user");
            let content = msg.get("content").and_then(|v| v.as_str()).unwrap_or("");
            messages.push(ChatMessage {
                role: role.to_string(),
                content: Some(content.to_string()),
                tool_calls: None,
                tool_call_id: None,
            });
        }
    }

    // Add the current question
    messages.push(ChatMessage {
        role: "user".to_string(),
        content: Some(question.to_string()),
        tool_calls: None,
        tool_call_id: None,
    });

    // Bridge cancellation
    let cancel_token = tokio_util::sync::CancellationToken::new();
    let cancel_token_clone = cancel_token.clone();
    let mut cancel_rx = cancel_rx;
    tokio::spawn(async move {
        while cancel_rx.changed().await.is_ok() {
            if *cancel_rx.borrow() {
                cancel_token_clone.cancel();
                break;
            }
        }
    });

    // Stream the response
    let (delta_tx, mut delta_rx) = tokio::sync::mpsc::channel::<String>(64);
    let tx_clone = tx.clone();
    let stream_id_owned = stream_id.to_string();

    // Forward deltas to WebSocket — match Otto's StreamChunk shape
    let forwarder = tokio::spawn(async move {
        while let Some(delta) = delta_rx.recv().await {
            let msg = WsOutbound::StreamChunk {
                stream_id: stream_id_owned.clone(),
                chunk: json!({ "type": "STREAM_CHAT_DELTA", "payload": { "content": delta } }),
            };
            let _ = tx_clone.send(serde_json::to_string(&msg).unwrap_or_default());
        }
    });

    match service::generate_chat_response(&state.config(), messages, &delta_tx, cancel_token).await {
        Ok(full_response) => {
            drop(delta_tx);
            let _ = forwarder.await;
            send_chunk(json!({
                "type": "STREAM_CHAT_COMPLETE",
                "payload": { "content": full_response, "suggestedQuestions": [] },
            }));
        }
        Err(e) => {
            drop(delta_tx);
            let _ = forwarder.await;
            send_chunk(json!({
                "type": "STREAM_CHAT_ERROR",
                "payload": { "error": e.to_string() },
            }));
        }
    }

    let _ = tx.send(serde_json::to_string(&WsOutbound::StreamEnd { stream_id: stream_id.to_string() }).unwrap_or_default());
}

pub async fn stream_inquiry(
    state: &AppState,
    stream_id: &str,
    payload: &Value,
    tx: &broadcast::Sender<String>,
    cancel_rx: watch::Receiver<bool>,
) {
    use crate::services::ai::client::ChatMessage;
    use crate::services::ai::service;

    let send_chunk = |chunk: Value| {
        let msg = WsOutbound::StreamChunk {
            stream_id: stream_id.to_string(),
            chunk,
        };
        let _ = tx.send(serde_json::to_string(&msg).unwrap_or_default());
    };

    // Check AI is configured
    if state.config().ai.base_url.is_empty() {
        send_chunk(json!({
            "type": "STREAM_INQUIRY_ERROR",
            "payload": { "error": "AI provider not configured on this Botto server" },
        }));
        let _ = tx.send(serde_json::to_string(&WsOutbound::StreamEnd { stream_id: stream_id.to_string() }).unwrap_or_default());
        return;
    }

    // Parse inquiry payload
    let question = payload.get("question").and_then(|v| v.as_str()).unwrap_or("");

    // Build context from the inquiryContext object
    let context_text = crate::services::ai::prompts::inquiry::build_context_from_payload(payload);

    // Build message history: system + context + previous slides + current question
    let mut messages = vec![
        crate::services::ai::prompts::inquiry::build_system(state.config().ai.custom_prompts.get("inquiry")),
        ChatMessage {
            role: "user".into(),
            content: Some(context_text),
            tool_calls: None,
            tool_call_id: None,
        },
        ChatMessage {
            role: "assistant".into(),
            content: Some("I see the selected code. What would you like to know?".into()),
            tool_calls: None,
            tool_call_id: None,
        },
    ];

    // Add previous slides as conversation history (for follow-ups)
    if let Some(prev_slides) = payload
        .get("inquiryContext")
        .and_then(|c| c.get("previousSlides"))
        .and_then(|v| v.as_array())
    {
        for slide in prev_slides {
            let q = slide.get("question").and_then(|v| v.as_str()).unwrap_or("");
            let a = slide.get("answer").and_then(|v| v.as_str()).unwrap_or("");
            messages.push(ChatMessage {
                role: "user".into(),
                content: Some(q.to_string()),
                tool_calls: None,
                tool_call_id: None,
            });
            if !a.is_empty() {
                messages.push(ChatMessage {
                    role: "assistant".into(),
                    content: Some(a.to_string()),
                    tool_calls: None,
                    tool_call_id: None,
                });
            }
        }
    }

    // Add the current question
    messages.push(ChatMessage {
        role: "user".into(),
        content: Some(question.to_string()),
        tool_calls: None,
        tool_call_id: None,
    });

    // Bridge cancellation
    let cancel_token = tokio_util::sync::CancellationToken::new();
    let cancel_token_clone = cancel_token.clone();
    let mut cancel_rx = cancel_rx;
    tokio::spawn(async move {
        while cancel_rx.changed().await.is_ok() {
            if *cancel_rx.borrow() {
                cancel_token_clone.cancel();
                break;
            }
        }
    });

    // Stream the response
    let (delta_tx, mut delta_rx) = tokio::sync::mpsc::channel::<String>(64);
    let tx_clone = tx.clone();
    let stream_id_owned = stream_id.to_string();

    // Forward deltas to WebSocket
    let forwarder = tokio::spawn(async move {
        while let Some(delta) = delta_rx.recv().await {
            let msg = WsOutbound::StreamChunk {
                stream_id: stream_id_owned.clone(),
                chunk: json!({ "type": "STREAM_INQUIRY_DELTA", "payload": { "content": delta } }),
            };
            let _ = tx_clone.send(serde_json::to_string(&msg).unwrap_or_default());
        }
    });

    match service::generate_inquiry_response(&state.config(), messages, &delta_tx, cancel_token).await {
        Ok(full_response) => {
            drop(delta_tx);
            let _ = forwarder.await;
            send_chunk(json!({
                "type": "STREAM_INQUIRY_COMPLETE",
                "payload": { "content": full_response },
            }));
        }
        Err(e) => {
            drop(delta_tx);
            let _ = forwarder.await;
            send_chunk(json!({
                "type": "STREAM_INQUIRY_ERROR",
                "payload": { "error": e.to_string() },
            }));
        }
    }

    let _ = tx.send(serde_json::to_string(&WsOutbound::StreamEnd { stream_id: stream_id.to_string() }).unwrap_or_default());
}

// ---------------------------------------------------------------------------
// Team digest
// ---------------------------------------------------------------------------

pub async fn get_team_digest(state: &AppState, payload: &Value) -> Value {
    let project_path = match extract_str(payload, "project_path") {
        Some(p) => p,
        None => return err("missing project_path"),
    };

    let period_str = extract_str(payload, "period").unwrap_or("weekly");
    let period = match period_str {
        "daily" => crate::services::digest::DigestPeriod::Daily,
        "weekly" => crate::services::digest::DigestPeriod::Weekly,
        _ => return err("invalid period — must be 'daily' or 'weekly'"),
    };

    match crate::services::digest::get_team_digest(state, project_path, period).await {
        Ok(digest) => ok(serde_json::to_value(digest).unwrap_or_default()),
        Err(e) => {
            warn!("digest computation failed for {}: {}", project_path, e);
            err(&format!("failed to compute digest: {}", e))
        }
    }
}

// ---------------------------------------------------------------------------
// Repo config
// ---------------------------------------------------------------------------

pub async fn get_repo_config(state: &AppState, payload: &Value) -> Value {
    let project_path = match extract_str(payload, "project_path") {
        Some(p) => p,
        None => return err("missing project_path"),
    };

    // Try cache first. If not cached, attempt a fetch using project_id + ref.
    let project_id = extract_i64(payload, "project_id");
    let ref_name = extract_str(payload, "ref").unwrap_or("main");

    let cfg = state.config();
    let gl_cfg = crate::services::gitlab::client::GitLabConfig {
        base_url: cfg.gitlab.url.clone(),
        token: cfg.gitlab.bot_token.clone(),
    };

    // If project_id is provided, do a full get_or_fetch (cache + API fallback).
    // Otherwise, only return what's already cached.
    let config = if let Some(pid) = project_id {
        crate::services::repo_config::get_or_fetch(
            state.pool(), &gl_cfg, project_path, pid, ref_name,
        ).await
    } else {
        // No project_id — check cache only, don't hit GitLab API
        match crate::services::repo_config::get_cached_formatted(state.pool(), project_path).await {
            Some(formatted) => {
                // Return just the formatted text — caller doesn't need the full struct
                return ok(json!({ "formatted": formatted }));
            }
            None => None,
        }
    };

    match config {
        Some(rc) => ok(json!({
            "config": serde_json::to_value(&rc).unwrap_or_default(),
            "formatted": crate::services::repo_config::format_for_prompt(&rc),
        })),
        None => ok(json!({ "config": null, "formatted": null })),
    }
}

pub async fn invalidate_repo_config(state: &AppState, payload: &Value) -> Value {
    let project_path = match extract_str(payload, "project_path") {
        Some(p) => p,
        None => return err("missing project_path"),
    };

    crate::services::repo_config::invalidate(state.pool(), project_path).await;
    ok(json!({ "invalidated": true }))
}

// ---------------------------------------------------------------------------
// Conflict Radar
// ---------------------------------------------------------------------------

pub async fn get_conflicts(state: &AppState, payload: &Value) -> Value {
    let mr_iid = match extract_u64(payload, "mr_iid") {
        Some(iid) => iid,
        None => return err("missing mr_iid"),
    };

    let cfg = state.config();
    if !cfg.conflict.enabled {
        return ok(json!({ "mrIid": mr_iid, "conflicts": [] }));
    }

    let gl_cfg = crate::services::gitlab::client::GitLabConfig {
        base_url: cfg.gitlab.url.clone(),
        token: cfg.gitlab.bot_token.clone(),
    };

    // Accept either project_id (numeric) or project_path (string).
    // Uses the cached project_id resolver to avoid redundant API calls.
    let project_id = match extract_i64(payload, "project_id") {
        Some(id) => id,
        None => {
            let project_path = match extract_str(payload, "project_path") {
                Some(p) => p,
                None => return err("missing project_id or project_path"),
            };
            match state.resolve_project_id(project_path).await {
                Some(id) => id,
                None => return err("failed to resolve project"),
            }
        }
    };

    // Ensure the file index is populated for this MR and other open MRs in
    // the project. Without this, conflict detection returns empty results when
    // no webhook has fired (cold start, no webhook configured, Botto restart).
    if let Err(e) = crate::services::file_index::ensure_populated(
        state.pool(), &gl_cfg, project_id, mr_iid,
    ).await {
        tracing::warn!("file index ensure_populated failed for !{}: {}", mr_iid, e);
    }
    // Populate other open MRs so there's something to conflict against.
    if let Err(e) = crate::services::file_index::ensure_project_populated(
        state.pool(), &gl_cfg, project_id,
    ).await {
        tracing::warn!("file index ensure_project_populated failed for project {}: {}", project_id, e);
    }

    match crate::services::conflict::detector::detect_conflicts(
        state.pool(),
        &gl_cfg,
        project_id,
        mr_iid,
    )
    .await
    {
        Ok(report) => ok(serde_json::to_value(&report).unwrap_or_default()),
        Err(e) => err(&format!("conflict detection failed: {}", e)),
    }
}

// ---------------------------------------------------------------------------
// Cross-MR Clusters
// ---------------------------------------------------------------------------

pub async fn get_cluster(state: &AppState, payload: &Value) -> Value {
    let mr_iid = match extract_u64(payload, "mr_iid") {
        Some(iid) => iid,
        None => return err("missing mr_iid"),
    };

    let cfg = state.config();
    if !cfg.cluster.enabled {
        return ok(json!({ "clusters": [] }));
    }

    // Accept either project_id or project_path — same pattern as get_conflicts.
    // Uses the cached project_id resolver to avoid redundant API calls.
    let project_id = match extract_i64(payload, "project_id") {
        Some(id) => id,
        None => {
            let project_path = match extract_str(payload, "project_path") {
                Some(p) => p,
                None => return err("missing project_id or project_path"),
            };
            match state.resolve_project_id(project_path).await {
                Some(id) => id,
                None => return err("failed to resolve project"),
            }
        }
    };

    let gl_cfg = crate::services::gitlab::client::GitLabConfig {
        base_url: cfg.gitlab.url.clone(),
        token: cfg.gitlab.bot_token.clone(),
    };

    // Ensure the file index is populated for this MR and the project.
    // If any MRs were freshly populated, run cluster detection so we have
    // results to return (clusters are only created by detect_clusters, not
    // by file index population alone).
    let mut needs_detection = false;
    if let Ok(true) = crate::services::file_index::ensure_populated(
        state.pool(), &gl_cfg, project_id, mr_iid,
    ).await {
        needs_detection = true;
    }
    if let Ok(count) = crate::services::file_index::ensure_project_populated(
        state.pool(), &gl_cfg, project_id,
    ).await {
        if count > 0 {
            needs_detection = true;
        }
    }

    // Run cluster detection if we just populated the index — otherwise
    // there are no cluster rows to query.
    if needs_detection {
        let ticket_strategy =
            crate::services::cluster::strategies::ticket::TicketClusterStrategy;
        let file_strategy =
            crate::services::cluster::strategies::file_overlap::FileOverlapStrategy {
                jaccard_threshold: cfg.cluster.file_overlap_threshold,
                max_cluster_size: cfg.cluster.max_cluster_size,
            };
        let strategies: Vec<&dyn crate::services::cluster::strategies::ClusterStrategy> =
            vec![&ticket_strategy, &file_strategy];

        if let Err(e) = crate::services::cluster::detector::detect_clusters(
            state.pool(), &gl_cfg, project_id, mr_iid, &strategies,
        ).await {
            tracing::warn!("on-demand cluster detection failed for !{}: {}", mr_iid, e);
        }
    }

    match db::queries::get_clusters_for_mr(state.pool(), project_id, mr_iid as i64).await {
        Ok(rows) => {
            let clusters: Vec<Value> = rows
                .into_iter()
                .filter_map(|(id, proj_id, ticket_key, member_mrs_json, signals_json, relevance, summary_blob, summary_hash, order_blob, _updated)| {
                    let member_mrs: Vec<crate::types::cluster::ClusterMember> =
                        serde_json::from_str(&member_mrs_json).ok()?;
                    let signals: Vec<crate::types::cluster::ClusterSignal> =
                        serde_json::from_str(&signals_json).ok()?;

                    // Decompress summary if present
                    let summary: Option<crate::types::cluster::ClusterSummaryData> =
                        summary_blob.and_then(|blob| {
                            let json_str = decompress_string(&blob)?;
                            serde_json::from_str(&json_str).ok()
                        });

                    // Decompress review order if present
                    let review_order: Option<crate::types::cluster::ClusterReviewOrder> =
                        order_blob.and_then(|blob| {
                            let json_str = decompress_string(&blob)?;
                            serde_json::from_str(&json_str).ok()
                        });

                    let cluster = crate::types::cluster::MrCluster {
                        id,
                        project_id: proj_id,
                        ticket_key,
                        member_mrs,
                        relevance_score: relevance,
                        signals,
                        summary,
                        review_order,
                    };

                    serde_json::to_value(&cluster).ok()
                })
                .collect();

            ok(json!({ "clusters": clusters }))
        }
        Err(e) => err(&format!("cluster lookup failed: {}", e)),
    }
}

// ---------------------------------------------------------------------------
// Server config (read/write via WebSocket — mirrors the HTTP admin API)
// ---------------------------------------------------------------------------

/// Return the current server config with secrets redacted.
pub async fn get_server_config(state: &AppState) -> Value {
    let cfg = state.config();
    let response = config::ConfigResponse::from_config(&cfg);
    ok(serde_json::to_value(&response).unwrap_or_default())
}

/// Apply a partial config update, persist to disk, and hot-swap in memory.
pub async fn update_server_config(state: &AppState, payload: &Value) -> Value {
    let update: config::ConfigUpdate = match serde_json::from_value(payload.clone()) {
        Ok(u) => u,
        Err(e) => return err(&format!("invalid config update: {}", e)),
    };

    let current = state.config();
    let (new_config, restart_fields) = config::apply_update(&current, update);

    // Persist to disk first — if this fails, don't swap in memory
    if let Err(e) = config::save_to_file(&new_config).await {
        return err(&format!("failed to save config: {}", e));
    }

    // Hot-swap in memory
    state.swap_config(new_config.clone());

    let restart_required = !restart_fields.is_empty();
    let response = config::ConfigResponse::from_config(&new_config);

    info!(
        "config updated via WebSocket (restart_required={}, fields={:?})",
        restart_required, restart_fields
    );

    ok(json!({
        "saved": true,
        "restart_required": restart_required,
        "restart_fields": restart_fields,
        "config": serde_json::to_value(&response).unwrap_or_default(),
    }))
}

// ---------------------------------------------------------------------------
// Server status (lightweight — no admin auth required)
// ---------------------------------------------------------------------------

pub async fn get_server_status(state: &AppState) -> Value {
    let connected = state.connections().len();
    let in_flight = state.in_flight().len();
    let warm = state.warm_pool().map(|p| p.count()).unwrap_or(0);
    let cfg = state.config();

    ok(json!({
        "connected_ottos": connected,
        "in_flight_reviews": in_flight,
        "sandbox_enabled": cfg.sandbox.enabled,
        "warm_containers": warm,
        "ai_configured": !cfg.ai.base_url.is_empty(),
        "conflict_radar": cfg.conflict.enabled,
        "cluster_enabled": cfg.cluster.enabled,
        "version": env!("CARGO_PKG_VERSION"),
    }))
}

// ---------------------------------------------------------------------------
// Presence (on-demand query)
// ---------------------------------------------------------------------------

pub async fn get_presence(state: &AppState, payload: &Value) -> Value {
    let project_path = match extract_str(payload, "project_path") {
        Some(p) => p,
        None => return err("missing project_path"),
    };
    let mr_iid = match extract_u64(payload, "mr_iid") {
        Some(i) => i,
        None => return err("missing mr_iid"),
    };

    let mr = crate::types::state::MrRef {
        project_path: project_path.to_string(),
        mr_iid,
    };

    let presence = state.get_mr_presence(&mr, None);
    let viewer_count = state.viewers_of(&mr).len();

    ok(json!({
        "project_path": project_path,
        "mr_iid": mr_iid,
        "viewer_count": viewer_count,
        "viewers": presence,
    }))
}

pub async fn batch_presence(state: &AppState, payload: &Value) -> Value {
    let project_path = match extract_str(payload, "project_path") {
        Some(p) => p,
        None => return err("missing project_path"),
    };
    let mr_iids = match payload.get("mr_iids").and_then(|v| v.as_array()) {
        Some(arr) => arr.iter().filter_map(|v| v.as_u64()).collect::<Vec<_>>(),
        None => return err("missing mr_iids array"),
    };

    // Cap to 50 MRs per request
    let mr_iids = if mr_iids.len() > 50 { &mr_iids[..50] } else { &mr_iids };

    let results: Vec<Value> = mr_iids
        .iter()
        .map(|&iid| {
            let mr = crate::types::state::MrRef {
                project_path: project_path.to_string(),
                mr_iid: iid,
            };
            let count = state.viewers_of(&mr).len();
            json!({ "mr_iid": iid, "viewer_count": count })
        })
        .collect();

    ok(json!({ "project_path": project_path, "mrs": results }))
}

// ---------------------------------------------------------------------------
// Active reviews
// ---------------------------------------------------------------------------

pub async fn get_active_reviews(state: &AppState) -> Value {
    let in_flight = state.in_flight();
    let reviews: Vec<Value> = in_flight
        .iter()
        .map(|entry| {
            let key = entry.key().clone();
            let parts: Vec<&str> = key.splitn(2, ':').collect();
            let (project_path, mr_iid) = if parts.len() == 2 {
                (parts[0].to_string(), parts[1].parse::<u64>().unwrap_or(0))
            } else {
                (key.clone(), 0)
            };
            json!({
                "key": key,
                "project_path": project_path,
                "mr_iid": mr_iid,
                "complete": entry.value().is_complete(),
            })
        })
        .collect();

    ok(json!({
        "count": reviews.len(),
        "reviews": reviews,
    }))
}

// ---------------------------------------------------------------------------
// Connected users
// ---------------------------------------------------------------------------

pub async fn get_connected_users(state: &AppState) -> Value {
    let connections = state.connections();
    let users: Vec<Value> = connections
        .iter()
        .filter_map(|entry| {
            let conn = entry.value();
            let user_id = conn.user_id.as_ref()?;
            Some(json!({
                "user_id": user_id,
                "display_name": conn.display_name,
                "avatar_url": conn.avatar_url,
                "viewing_mr": conn.viewing_mr.as_ref().map(|mr| json!({
                    "project_path": mr.project_path,
                    "mr_iid": mr.mr_iid,
                })),
            }))
        })
        .collect();

    ok(json!({
        "count": users.len(),
        "users": users,
    }))
}

// ---------------------------------------------------------------------------
// Sandbox jobs list (per MR)
// ---------------------------------------------------------------------------

pub async fn get_sandbox_jobs(state: &AppState, payload: &Value) -> Value {
    let project_path = match extract_str(payload, "project_path") {
        Some(p) => p,
        None => return err("missing project_path"),
    };
    let mr_iid = match extract_i64(payload, "mr_iid") {
        Some(i) => i,
        None => return err("missing mr_iid"),
    };

    match db::queries::get_sandbox_jobs_for_mr(state.pool(), project_path, mr_iid).await {
        Ok(rows) => {
            let jobs: Vec<Value> = rows
                .into_iter()
                .map(|(id, status, comment_id, strategy, commit_sha, error, created_at, updated_at)| {
                    json!({
                        "id": id,
                        "status": status,
                        "comment_id": comment_id,
                        "strategy": strategy,
                        "commit_sha": commit_sha,
                        "error": error,
                        "created_at": created_at,
                        "updated_at": updated_at,
                    })
                })
                .collect();
            ok(json!({ "jobs": jobs }))
        }
        Err(e) => err(&format!("failed to query sandbox jobs: {}", e)),
    }
}

// ---------------------------------------------------------------------------
// Review history
// ---------------------------------------------------------------------------

pub async fn get_review_history(state: &AppState, payload: &Value) -> Value {
    let project_path = match extract_str(payload, "project_path") {
        Some(p) => p,
        None => return err("missing project_path"),
    };

    match db::queries::list_cached_reviews(state.pool(), project_path).await {
        Ok(rows) => {
            let reviews: Vec<Value> = rows
                .into_iter()
                .map(|(mr_iid, diff_hash, created_at, expires_at)| {
                    json!({
                        "mr_iid": mr_iid,
                        "diff_hash": diff_hash,
                        "created_at": created_at,
                        "expires_at": expires_at,
                    })
                })
                .collect();
            ok(json!({ "reviews": reviews }))
        }
        Err(e) => err(&format!("failed to query review history: {}", e)),
    }
}

// ---------------------------------------------------------------------------
// File index status
// ---------------------------------------------------------------------------

pub async fn get_file_index_status(state: &AppState, payload: &Value) -> Value {
    let project_path = match extract_str(payload, "project_path") {
        Some(p) => p,
        None => return err("missing project_path"),
    };

    // Resolve project_id from project_path
    let project_id = match state.resolve_project_id(project_path).await {
        Some(id) => id,
        None => return err("failed to resolve project"),
    };

    match db::queries::get_file_index_stats(state.pool(), project_id).await {
        Ok((mr_count, file_count)) => ok(json!({
            "project_path": project_path,
            "project_id": project_id,
            "indexed_mrs": mr_count,
            "indexed_files": file_count,
        })),
        Err(e) => err(&format!("failed to query file index: {}", e)),
    }
}

// ---------------------------------------------------------------------------
// Cache invalidation
// ---------------------------------------------------------------------------

pub async fn invalidate_review_cache(state: &AppState, payload: &Value) -> Value {
    let project_path = match extract_str(payload, "project_path") {
        Some(p) => p,
        None => return err("missing project_path"),
    };
    let mr_iid = match extract_i64(payload, "mr_iid") {
        Some(i) => i,
        None => return err("missing mr_iid"),
    };

    match db::queries::invalidate_mr_review_cache(state.pool(), project_path, mr_iid).await {
        Ok(deleted) => ok(json!({
            "project_path": project_path,
            "mr_iid": mr_iid,
            "deleted": deleted,
        })),
        Err(e) => err(&format!("failed to invalidate cache: {}", e)),
    }
}

// ---------------------------------------------------------------------------
// Ping
// ---------------------------------------------------------------------------

pub async fn ping() -> Value {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    ok(json!({ "ts": now }))
}

// ---------------------------------------------------------------------------
// Reviewer preferences
// ---------------------------------------------------------------------------

pub async fn get_reviewer_prefs(state: &AppState, payload: &Value) -> Value {
    let project_path = match extract_str(payload, "project_path") {
        Some(p) => p,
        None => return err("missing project_path"),
    };

    match db::queries::get_reviewer_prefs(state.pool(), project_path).await {
        Ok(Some((text, updated_at))) => ok(json!({
            "project_path": project_path,
            "prefs": text,
            "updated_at": updated_at,
        })),
        Ok(None) => ok(json!({
            "project_path": project_path,
            "prefs": null,
            "updated_at": null,
        })),
        Err(e) => err(&format!("failed to get reviewer prefs: {}", e)),
    }
}

// ---------------------------------------------------------------------------
// Latest cached review (without requiring diff_hash)
// ---------------------------------------------------------------------------

pub async fn get_latest_cached_review(state: &AppState, payload: &Value) -> Value {
    let project_path = match extract_str(payload, "project_path") {
        Some(p) => p,
        None => return err("missing project_path"),
    };
    let mr_iid = match extract_i64(payload, "mr_iid") {
        Some(i) => i,
        None => return err("missing mr_iid"),
    };

    match db::queries::get_latest_cached_review(state.pool(), project_path, mr_iid).await {
        Ok(Some(row)) => {
            let data: Vec<u8> = row.0;
            let file_hashes: String = row.1;
            let diff_hash: String = row.2;
            let review_json = crate::router::decompress_or_raw(&data);
            match serde_json::from_slice::<Value>(&review_json) {
                Ok(review) => ok(json!({
                    "review": review,
                    "diff_hash": diff_hash,
                    "file_diff_hashes": file_hashes,
                })),
                Err(e) => err(&format!("corrupt cached review: {}", e)),
            }
        }
        Ok(None) => ok(Value::Null),
        Err(e) => err(&format!("cache read error: {}", e)),
    }
}

// ---------------------------------------------------------------------------
// Warm pool status
// ---------------------------------------------------------------------------

pub async fn get_warm_pool_status(state: &AppState) -> Value {
    match state.warm_pool() {
        Some(pool) => {
            let containers: Vec<Value> = pool.list_status()
                .into_iter()
                .map(|(mr_key, image, age_secs, idle_secs)| {
                    json!({
                        "mr_key": mr_key,
                        "image": image,
                        "age_secs": age_secs,
                        "idle_secs": idle_secs,
                    })
                })
                .collect();
            ok(json!({
                "enabled": true,
                "count": containers.len(),
                "containers": containers,
            }))
        }
        None => ok(json!({
            "enabled": false,
            "count": 0,
            "containers": [],
        })),
    }
}

// ---------------------------------------------------------------------------
// Events
// ---------------------------------------------------------------------------

pub async fn get_events(state: &AppState, payload: &Value) -> Value {
    let project_path = match extract_str(payload, "project_path") {
        Some(p) => p,
        None => return err("missing project_path"),
    };
    let mr_iid = extract_i64(payload, "mr_iid");
    let limit = extract_i64(payload, "limit").unwrap_or(50).min(200);

    match db::queries::get_recent_events(state.pool(), project_path, mr_iid, limit).await {
        Ok(rows) => {
            let events: Vec<Value> = rows
                .into_iter()
                .map(|(id, event_type, user_id, payload_bytes, created_at)| {
                    let payload_json = payload_bytes
                        .and_then(|b| serde_json::from_slice::<Value>(&b).ok());
                    json!({
                        "id": id,
                        "event_type": event_type,
                        "user_id": user_id,
                        "payload": payload_json,
                        "created_at": created_at,
                    })
                })
                .collect();
            ok(json!({ "events": events }))
        }
        Err(e) => err(&format!("failed to query events: {}", e)),
    }
}

// ---------------------------------------------------------------------------
// Workflow runs
// ---------------------------------------------------------------------------

pub async fn get_workflow_runs(state: &AppState, payload: &Value) -> Value {
    let limit = extract_i64(payload, "limit").unwrap_or(20).min(100);

    match db::queries::list_workflow_runs(state.pool(), limit).await {
        Ok(rows) => {
            let runs: Vec<Value> = rows
                .into_iter()
                .map(|(id, workflow_id, trigger_type, trigger_data, status, step_states, verification, started_at, completed_at)| {
                    json!({
                        "id": id,
                        "workflow_id": workflow_id,
                        "trigger_type": trigger_type,
                        "trigger_data": trigger_data.and_then(|d| serde_json::from_str::<Value>(&d).ok()),
                        "status": status,
                        "step_states": serde_json::from_str::<Value>(&step_states).unwrap_or_default(),
                        "final_verification": verification.and_then(|v| serde_json::from_str::<Value>(&v).ok()),
                        "started_at": started_at,
                        "completed_at": completed_at,
                    })
                })
                .collect();
            ok(json!({ "runs": runs }))
        }
        Err(e) => err(&format!("failed to query workflow runs: {}", e)),
    }
}

// ---------------------------------------------------------------------------
// Workflows list
// ---------------------------------------------------------------------------

pub async fn get_workflows(state: &AppState, payload: &Value) -> Value {
    let enabled_only = payload.get("enabled_only").and_then(|v| v.as_bool()).unwrap_or(true);

    match db::queries::list_workflows(state.pool(), enabled_only).await {
        Ok(rows) => {
            let workflows: Vec<Value> = rows
                .into_iter()
                .map(|(id, name, description, project_id, definition, enabled, created_by, created_at, updated_at)| {
                    json!({
                        "id": id,
                        "name": name,
                        "description": description,
                        "project_id": project_id,
                        "definition": serde_json::from_str::<Value>(&definition).unwrap_or_default(),
                        "enabled": enabled,
                        "created_by": created_by,
                        "created_at": created_at,
                        "updated_at": updated_at,
                    })
                })
                .collect();
            ok(json!({ "workflows": workflows }))
        }
        Err(e) => err(&format!("failed to query workflows: {}", e)),
    }
}

/// Decompress gzip data to a string. Returns None on failure.
fn decompress_string(data: &[u8]) -> Option<String> {
    use flate2::read::GzDecoder;
    use std::io::Read;
    let mut decoder = GzDecoder::new(data);
    let mut result = String::new();
    decoder.read_to_string(&mut result).ok()?;
    Some(result)
}
