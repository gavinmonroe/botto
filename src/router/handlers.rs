// ---------------------------------------------------------------------------
// Request handlers — one function per Otto message type.
//
// Every handler takes &AppState + the request payload, returns a JSON Value
// in the Result<T> shape: { ok: true, data: ... } or { ok: false, error: ... }
// ---------------------------------------------------------------------------

#![allow(unused_variables, unused_imports)]

use crate::api::ws::WsOutbound;
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
        Some(p) => p,
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
                .map(|row: (String, String, String, Option<String>, i64)| {
                    json!({
                        "comment_id": row.0,
                        "user_id": row.1,
                        "action": row.2,
                        "edited_body": row.3,
                        "created_at": row.4,
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
                let _ = tx.send(serde_json::to_string(&msg).unwrap());
                let _ = tx.send(serde_json::to_string(&WsOutbound::StreamEnd { stream_id: stream_id.to_string() }).unwrap());
                return;
            }
        },
        None => {
            let msg = WsOutbound::StreamChunk {
                stream_id: stream_id.to_string(),
                chunk: json!({ "type": "STREAM_TASK_ERROR", "payload": { "task": "init", "error": "missing mrContext" } }),
            };
            let _ = tx.send(serde_json::to_string(&msg).unwrap());
            let _ = tx.send(serde_json::to_string(&WsOutbound::StreamEnd { stream_id: stream_id.to_string() }).unwrap());
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
            let _ = tx.send(serde_json::to_string(&msg).unwrap());
        }

        // 2. If already complete, we're done
        if in_flight.is_complete() {
            let _ = tx.send(
                serde_json::to_string(&WsOutbound::StreamEnd {
                    stream_id: stream_id.to_string(),
                })
                .unwrap(),
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
                            let _ = tx_clone.send(serde_json::to_string(&msg).unwrap());
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
            .unwrap(),
        );
        return;
    }

    // --- No in-flight review: we already inserted via entry() above ---
    let in_flight = state.in_flight().get(&mr_key).unwrap().clone();

    // Acquire review concurrency permit (limits how many MR reviews run at once).
    // Late-joiners (above) skip this — they piggyback on the existing review.
    let _review_permit = {
        let sem = state.review_semaphore().clone();
        if sem.available_permits() == 0 {
            let _ = tx.send(serde_json::to_string(&WsOutbound::StreamChunk {
                stream_id: stream_id.to_string(),
                chunk: json!({ "type": "STREAM_PROGRESS", "payload": { "message": "waiting for review slot..." } }),
            }).unwrap());
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
            let _ = tx_clone.send(serde_json::to_string(&msg).unwrap());

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
        .unwrap(),
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
        let _ = tx.send(serde_json::to_string(&msg).unwrap());
    };

    // Check AI is configured
    if state.config().ai.base_url.is_empty() {
        send_chunk(json!({
            "type": "STREAM_CHAT_ERROR",
            "payload": { "error": "AI provider not configured on this Botto server" },
        }));
        let _ = tx.send(serde_json::to_string(&WsOutbound::StreamEnd { stream_id: stream_id.to_string() }).unwrap());
        return;
    }

    // Parse chat payload
    let question = payload.get("question").and_then(|v| v.as_str()).unwrap_or("");
    let review_context = payload.get("reviewContext").and_then(|v| v.as_str()).unwrap_or("");

    // Build message history
    let mut messages = vec![
        crate::services::ai::prompts::chat::build_system(review_context, None),
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
            let _ = tx_clone.send(serde_json::to_string(&msg).unwrap());
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

    let _ = tx.send(serde_json::to_string(&WsOutbound::StreamEnd { stream_id: stream_id.to_string() }).unwrap());
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
