// ---------------------------------------------------------------------------
// GitLab webhook receiver — handles MR, push, and note events.
//
// Validates the webhook secret token, parses the event, and dispatches
// to the event bus for cache invalidation and notification broadcasting.
// ---------------------------------------------------------------------------

use crate::types::state::AppState;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use tracing::{info, warn};

pub async fn gitlab_webhook(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> StatusCode {
    // Validate webhook secret if configured
    if let Some(ref expected_secret) = state.config().gitlab.webhook_secret {
        let token = headers
            .get("X-Gitlab-Token")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        if token != expected_secret {
            warn!("webhook rejected: invalid secret token");
            return StatusCode::UNAUTHORIZED;
        }
    }

    let event_type = headers
        .get("X-Gitlab-Event")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("unknown");

    let payload: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            warn!("webhook rejected: invalid JSON: {}", e);
            return StatusCode::BAD_REQUEST;
        }
    };

    info!("webhook received: {}", event_type);

    match event_type {
        "Merge Request Hook" => handle_mr_event(&state, &payload).await,
        "Push Hook" => handle_push_event(&state, &payload).await,
        "Note Hook" => handle_note_event(&state, &payload).await,
        _ => {
            // Ignore events we don't care about
        }
    }

    StatusCode::OK
}

async fn handle_mr_event(state: &AppState, payload: &serde_json::Value) {
    let project_path = payload["project"]["path_with_namespace"]
        .as_str()
        .unwrap_or("");
    let mr_iid = payload["object_attributes"]["iid"].as_u64();
    let action = payload["object_attributes"]["action"]
        .as_str()
        .unwrap_or("");

    if project_path.is_empty() || mr_iid.is_none() {
        return;
    }

    let mr_iid = mr_iid.unwrap();
    info!(
        "MR event: {} !{} action={}",
        project_path, mr_iid, action
    );

    // Evict warm container on merge or close — the MR is done.
    match action {
        "merge" | "close" => {
            if let Some(pool) = state.warm_pool() {
                let mr_key = format!("{}:{}", project_path, mr_iid);
                if pool.remove(&mr_key) {
                    info!("warm pool: evicted container for {} (MR {})", mr_key, action);
                }
            }
        }
        _ => {}
    }

    // Publish event for cache invalidation and notification
    state.event_bus().publish(crate::services::events::Event {
        event_type: crate::services::events::EventType::MrUpdated,
        project_path: project_path.to_string(),
        mr_iid: Some(mr_iid),
        user_id: None,
        payload: Some(serde_json::json!({ "action": action })),
    });
}

async fn handle_push_event(state: &AppState, payload: &serde_json::Value) {
    let project_path = payload["project"]["path_with_namespace"]
        .as_str()
        .unwrap_or("");
    let branch = payload["ref"]
        .as_str()
        .unwrap_or("")
        .strip_prefix("refs/heads/")
        .unwrap_or("");

    if project_path.is_empty() || branch.is_empty() {
        return;
    }

    let after_sha = payload["after"].as_str().unwrap_or("");

    info!("push event: {} branch={}", project_path, branch);

    // Check if .otto.json was added, modified, or removed in this push.
    // Invalidate the cached repo config so the next review/fix re-fetches it.
    // We invalidate on ANY branch push that touches .otto.json — the cache is
    // project-level, and the next get_or_fetch() will re-fetch from whatever
    // branch the caller needs.
    if let Some(commits) = payload["commits"].as_array() {
        let touches_otto_json = commits.iter().any(|c| {
            ["added", "modified", "removed"].iter().any(|field| {
                c[field]
                    .as_array()
                    .map(|files| files.iter().any(|f| f.as_str() == Some(".otto.json")))
                    .unwrap_or(false)
            })
        });
        if touches_otto_json {
            info!("push event: .otto.json changed in {}, invalidating cached config", project_path);
            crate::services::repo_config::invalidate(state.pool(), project_path).await;
        }
    }

    // Warm container eviction on push.
    // If Botto pushed this commit (bot push), keep the container warm.
    // If the author pushed, the checkout is stale — evict.
    if let Some(pool) = state.warm_pool() {
        let mr_keys = pool.find_by_branch(project_path, branch);
        for mr_key in mr_keys {
            if !after_sha.is_empty() && pool.is_bot_push(&mr_key, after_sha) {
                info!("warm pool: keeping {} (Botto's own push {})", mr_key, &after_sha[..8.min(after_sha.len())]);
            } else {
                if pool.remove(&mr_key) {
                    info!("warm pool: evicted {} (author push to {})", mr_key, branch);
                }
            }
        }
    }

    // Invalidate any cached reviews for MRs with this source branch.
    // The actual cache invalidation happens in the event handler that
    // subscribes to MrUpdated events.
    state.event_bus().publish(crate::services::events::Event {
        event_type: crate::services::events::EventType::MrUpdated,
        project_path: project_path.to_string(),
        mr_iid: None,
        user_id: None,
        payload: Some(serde_json::json!({ "branch": branch, "trigger": "push" })),
    });
}

async fn handle_note_event(state: &AppState, payload: &serde_json::Value) {
    let project_path = payload["project"]["path_with_namespace"]
        .as_str()
        .unwrap_or("");
    let mr_iid = payload["merge_request"]["iid"].as_u64();

    if project_path.is_empty() || mr_iid.is_none() {
        return;
    }

    let mr_iid = mr_iid.unwrap();
    info!("note event: {} !{}", project_path, mr_iid);

    // Notify connected Ottos that a new comment was posted
    let mr_ref = crate::types::state::MrRef {
        project_path: project_path.to_string(),
        mr_iid,
    };

    let msg = serde_json::json!({
        "type": "EVENT_NOTIFICATION",
        "event_type": "note_added",
        "project_path": project_path,
        "mr_iid": mr_iid,
    });

    state.broadcast_to_mr(&mr_ref, &msg.to_string());
}
