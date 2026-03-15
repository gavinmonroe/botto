// ---------------------------------------------------------------------------
// Message router — dispatches inbound messages to handler functions.
//
// Two categories:
//   1. Request/Response (one-shot) — Otto sends a request, gets a JSON response.
//      Maps directly to Otto's `sendMessage` pattern.
//   2. Streaming — Otto starts a stream, receives chunks until completion.
//      Maps to Otto's `openStream`/port pattern, multiplexed over the WS.
//
// The router is intentionally flat — one match arm per message type, no
// abstraction layers. Each handler is self-contained and reads what it needs.
// ---------------------------------------------------------------------------

pub mod handlers;

use crate::api::ws::WsOutbound;
use crate::db;
use crate::types::state::{AppState, MrRef};
use serde_json::Value;
use tokio::sync::{broadcast, watch};
use tracing::{info, warn};

// ---------------------------------------------------------------------------
// One-shot request handler
// ---------------------------------------------------------------------------

pub async fn handle_request(state: &AppState, payload: &Value) -> Value {
    let msg_type = payload
        .get("type")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");

    // Otto-routed messages nest the actual data inside a "payload" field:
    //   { type: "FETCH_MR_CHANGES", payload: { hostId: "abc", projectId: 123, mrIid: 42 } }
    // Botto-native messages (team settings, comment actions, etc.) put fields at the top level:
    //   { type: "GET_TEAM_SETTINGS", project_path: "team/repo" }
    // Unwrap the inner payload if it exists, otherwise use the message itself.
    let effective_payload = payload
        .get("payload")
        .filter(|v| v.is_object())
        .unwrap_or(payload);

    match msg_type {
        "GET_SETTINGS" => handlers::get_settings(state).await,
        "TEST_GITLAB_CONNECTION" => handlers::test_gitlab_connection(state, effective_payload).await,
        "FETCH_PROJECT" => handlers::fetch_project(state, effective_payload).await,
        "FETCH_MR_METADATA" => handlers::fetch_mr_metadata(state, effective_payload).await,
        "FETCH_MR_CHANGES" => handlers::fetch_mr_changes(state, effective_payload).await,
        "FETCH_FILE_CONTENT" => handlers::fetch_file_content(state, effective_payload).await,
        "FETCH_FILE_TREE" => handlers::fetch_file_tree(state, effective_payload).await,
        "FETCH_MR_DISCUSSIONS" => handlers::fetch_mr_discussions(state, effective_payload).await,
        "FETCH_TICKET" => handlers::fetch_ticket(state, effective_payload).await,
        "FETCH_TICKET_BATCH" => handlers::fetch_ticket_batch(state, effective_payload).await,
        "GET_CACHED_REVIEW" => handlers::get_cached_review(state, effective_payload).await,
        "GET_COMMENT_ACTIONS" => handlers::get_comment_actions(state, effective_payload).await,
        "GET_TEAM_SETTINGS" => handlers::get_team_settings(state, effective_payload).await,
        "SET_TEAM_SETTINGS" => handlers::set_team_settings(state, effective_payload).await,
        "GET_QUEUE_STATUS" => handlers::get_queue_status(state, effective_payload).await,
        "ENQUEUE_REVIEW" => handlers::enqueue_review(state, effective_payload).await,
        "PAUSE_REVIEW" => handlers::pause_review(state, effective_payload).await,
        "RESUME_REVIEW" => handlers::resume_review(state, effective_payload).await,
        "CANCEL_REVIEW" => handlers::cancel_review(state, effective_payload).await,
        "GET_SANDBOX_JOB" => handlers::get_sandbox_job(state, effective_payload).await,
        _ => {
            warn!("unknown request type: {}", msg_type);
            serde_json::json!({
                "ok": false,
                "error": format!("unknown request type: {}", msg_type)
            })
        }
    }
}

// ---------------------------------------------------------------------------
// Stream handler
// ---------------------------------------------------------------------------

pub async fn handle_stream(
    state: &AppState,
    conn_id: &str,
    user_id: &str,
    stream_id: &str,
    payload: &Value,
    tx: &broadcast::Sender<String>,
    cancel_rx: watch::Receiver<bool>,
) {
    let stream_type = payload
        .get("type")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");

    let send_chunk = |chunk: Value| {
        let msg = WsOutbound::StreamChunk {
            stream_id: stream_id.to_string(),
            chunk,
        };
        let _ = tx.send(serde_json::to_string(&msg).unwrap());
    };

    let send_end = || {
        let msg = WsOutbound::StreamEnd {
            stream_id: stream_id.to_string(),
        };
        let _ = tx.send(serde_json::to_string(&msg).unwrap());
    };

    match stream_type {
        "STREAM_REVIEW" => {
            info!(
                "stream review started: conn={} stream={} user={}",
                conn_id, stream_id, user_id
            );
            handlers::stream_review(state, conn_id, user_id, stream_id, payload, tx, cancel_rx)
                .await;
        }
        "STREAM_CHAT" => {
            info!(
                "stream chat started: conn={} stream={}",
                conn_id, stream_id
            );
            handlers::stream_chat(state, stream_id, payload, tx, cancel_rx).await;
        }
        _ => {
            warn!("unknown stream type: {}", stream_type);
            send_chunk(serde_json::json!({
                "type": "STREAM_TASK_ERROR",
                "payload": { "task": "unknown", "error": format!("unknown stream type: {}", stream_type) }
            }));
            send_end();
        }
    }
}

// ---------------------------------------------------------------------------
// Presence: handle VIEWING_MR
// ---------------------------------------------------------------------------

pub async fn handle_viewing_mr(
    state: &AppState,
    conn_id: &str,
    user_id: &str,
    mr: &MrRef,
    tx: &broadcast::Sender<String>,
) -> anyhow::Result<()> {
    // Check for cached review
    if let Ok(Some(row)) =
        db::queries::get_latest_cached_review(state.pool(), &mr.project_path, mr.mr_iid as i64)
            .await
    {
        let data: Vec<u8> = row.0;
        let file_hashes: String = row.1;
        let diff_hash: String = row.2;

        let review_json = decompress_or_raw(&data);
        if let Ok(review) = serde_json::from_slice::<Value>(&review_json) {
            let msg = crate::api::ws::WsOutbound::CachedReview {
                project_path: mr.project_path.clone(),
                mr_iid: mr.mr_iid,
                diff_hash,
                review,
                file_diff_hashes: file_hashes,
            };
            let _ = tx.send(serde_json::to_string(&msg).unwrap());
        }
    }

    // Send comment actions
    if let Ok(actions) =
        db::queries::get_comment_actions(state.pool(), &mr.project_path, mr.mr_iid as i64).await
    {
        let rows: Vec<(String, String, String, Option<String>, i64)> = actions;
        if !rows.is_empty() {
            let actions_json: Vec<Value> = rows
                .into_iter()
                .map(|row| {
                    serde_json::json!({
                        "comment_id": row.0,
                        "user_id": row.1,
                        "action": row.2,
                        "edited_body": row.3,
                        "created_at": row.4,
                    })
                })
                .collect();

            let msg = crate::api::ws::WsOutbound::CommentActionsSync {
                project_path: mr.project_path.clone(),
                mr_iid: mr.mr_iid,
                actions: actions_json,
            };
            let _ = tx.send(serde_json::to_string(&msg).unwrap());
        }
    }

    // Notify other viewers that someone joined
    let viewers = state.viewers_of(mr);
    if viewers.len() > 1 {
        let msg = crate::api::ws::WsOutbound::EventNotification {
            event_type: "user_joined_mr".to_string(),
            project_path: mr.project_path.clone(),
            mr_iid: Some(mr.mr_iid),
            payload: Some(serde_json::json!({ "user_id": user_id, "viewer_count": viewers.len() })),
        };
        state.broadcast_to_mr_except(mr, &serde_json::to_string(&msg).unwrap(), conn_id);
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Comment action handler
// ---------------------------------------------------------------------------

pub async fn handle_comment_action(
    state: &AppState,
    conn_id: &str,
    user_id: &str,
    project_path: &str,
    mr_iid: u64,
    comment_id: &str,
    action: &str,
    edited_body: Option<&str>,
) {
    // Persist
    let _ = db::queries::upsert_comment_action(
        state.pool(),
        project_path,
        mr_iid as i64,
        comment_id,
        user_id,
        action,
        edited_body,
    )
    .await;

    // Check if shared triage is enabled for this project
    let shared: bool = db::queries::get_shared_triage(state.pool(), project_path)
        .await
        .unwrap_or(false);

    if shared {
        let mr = MrRef {
            project_path: project_path.to_string(),
            mr_iid,
        };
        let msg = WsOutbound::CommentActionBroadcast {
            project_path: project_path.to_string(),
            mr_iid,
            comment_id: comment_id.to_string(),
            user_id: user_id.to_string(),
            action: action.to_string(),
            edited_body: edited_body.map(|s| s.to_string()),
        };
        state.broadcast_to_mr_except(&mr, &serde_json::to_string(&msg).unwrap(), conn_id);
    }

    // Publish event
    state.event_bus().publish(crate::services::events::Event {
        event_type: crate::services::events::EventType::CommentAction,
        project_path: project_path.to_string(),
        mr_iid: Some(mr_iid),
        user_id: Some(user_id.to_string()),
        payload: Some(serde_json::json!({
            "comment_id": comment_id,
            "action": action,
        })),
    });
}

// ---------------------------------------------------------------------------
// Fix request handler
// ---------------------------------------------------------------------------

pub async fn handle_fix_request(
    state: &AppState,
    _conn_id: &str,
    user_id: &str,
    project_path: &str,
    mr_iid: u64,
    comment_id: &str,
    suggestion: &str,
    file_path: &str,
    original_code: &str,
    source_branch: &str,
    comment_body: Option<&str>,
    comment_title: Option<&str>,
    severity: Option<&str>,
    target_branch: Option<&str>,
    start_line: Option<u32>,
    end_line: Option<u32>,
    tx: &broadcast::Sender<String>,
) {
    if !state.config().sandbox.enabled {
        let msg = WsOutbound::FixComplete {
            job_id: String::new(),
            comment_id: comment_id.to_string(),
            commit_sha: None,
            error: Some("sandbox is not enabled on this server".into()),
        };
        let _ = tx.send(serde_json::to_string(&msg).unwrap());
        return;
    }

    let job_id = uuid::Uuid::new_v4().to_string();

    let _ = db::queries::insert_sandbox_job(
        state.pool(),
        &job_id,
        project_path,
        mr_iid as i64,
        Some(comment_id),
        "auto",
    )
    .await;

    let msg = WsOutbound::FixProgress {
        job_id: job_id.clone(),
        comment_id: comment_id.to_string(),
        status: "pending".into(),
        detail: "sandbox job queued".into(),
    };
    let _ = tx.send(serde_json::to_string(&msg).unwrap());

    let mr_ref = MrRef {
        project_path: project_path.to_string(),
        mr_iid,
    };

    state.event_bus().publish(crate::services::events::Event {
        event_type: crate::services::events::EventType::FixStarted,
        project_path: project_path.to_string(),
        mr_iid: Some(mr_iid),
        user_id: Some(user_id.to_string()),
        payload: Some(serde_json::json!({
            "job_id": &job_id,
            "comment_id": comment_id,
            "file_path": file_path,
        })),
    });

    // Build the sandbox manager and run the fix
    let broadcaster: std::sync::Arc<dyn Fn(&MrRef, &str) + Send + Sync> = {
        let s = state.clone();
        std::sync::Arc::new(move |mr, msg| s.broadcast_to_mr(mr, msg))
    };

    let sandbox_mgr = crate::services::sandbox::manager::SandboxManager::new(
        state.config().clone(),
        state.pool().clone(),
        state.event_bus().clone(),
        broadcaster,
    );

    match sandbox_mgr {
        Some(mgr) => {
            // Fetch rich context from GitLab for the AI agent.
            // These are best-effort — the fix can still proceed without them.
            let gl_cfg = crate::services::gitlab::client::GitLabConfig {
                base_url: state.config().gitlab.url.clone(),
                token: state.config().gitlab.bot_token.clone(),
            };

            let project_id = crate::services::gitlab::client::fetch_project(&gl_cfg, project_path)
                .await
                .map(|p| p.id)
                .ok();

            // Fetch MR metadata (title, description) and file content in parallel
            let (mr_meta, file_content, mr_changes) = {
                let gl1 = gl_cfg.clone();
                let gl2 = gl_cfg.clone();
                let gl3 = gl_cfg.clone();
                let pid = project_id;
                let fp = file_path.to_string();
                let sb = source_branch.to_string();

                let mr_fut = async {
                    if let Some(pid) = pid {
                        crate::services::gitlab::client::fetch_merge_request(&gl1, pid, mr_iid).await.ok()
                    } else { None }
                };
                let file_fut = async {
                    if let Some(pid) = pid {
                        crate::services::gitlab::client::fetch_file_content(&gl2, pid, &fp, &sb).await.ok()
                    } else { None }
                };
                let changes_fut = async {
                    if let Some(pid) = pid {
                        crate::services::gitlab::client::fetch_mr_changes(&gl3, pid, mr_iid).await.ok()
                    } else { None }
                };

                tokio::join!(mr_fut, file_fut, changes_fut)
            };

            // Extract the diff for the specific file being fixed
            let file_diff = mr_changes.as_ref().and_then(|changes| {
                changes.changes.iter()
                    .find(|c| c.new_path == file_path || c.old_path == file_path)
                    .map(|c| c.diff.clone())
            });

            // Detect fork-based MRs: if source_project_id != target_project_id,
            // the branch lives on the fork, not the upstream repo.
            let source_project_path = if let Some(mr) = &mr_meta {
                match (mr.source_project_id, mr.target_project_id) {
                    (Some(src), Some(tgt)) if src != tgt => {
                        // Resolve the fork's project path
                        crate::services::gitlab::client::fetch_project_by_id(&gl_cfg, src)
                            .await
                            .map(|p| p.path_with_namespace)
                            .ok()
                    }
                    _ => None, // same project, no fork
                }
            } else {
                None
            };

            let req = crate::services::sandbox::manager::FixRequest {
                job_id: job_id.clone(),
                project_path: project_path.to_string(),
                mr_iid,
                source_branch: source_branch.to_string(),
                comment_id: comment_id.to_string(),
                file_path: file_path.to_string(),
                original_code: original_code.to_string(),
                suggestion: suggestion.to_string(),
                comment_body: comment_body.map(|s| s.to_string()),
                comment_title: comment_title.map(|s| s.to_string()),
                severity: severity.map(|s| s.to_string()),
                target_branch: target_branch
                    .map(|s| s.to_string())
                    .or_else(|| mr_meta.as_ref().map(|m| m.target_branch.clone())),
                start_line,
                end_line,
                file_content,
                mr_title: mr_meta.as_ref().map(|m| m.title.clone()),
                mr_description: mr_meta.as_ref().and_then(|m| m.description.clone()),
                file_diff,
                source_project_path,
            };

            let result = mgr.run_fix(req).await;

            // Post a GitLab MR comment on successful fix with commit link.
            // This gives visibility to all MR participants, not just Otto users.
            if result.success {
                if let Some(ref sha) = result.commit_sha {
                    let comment_body = format!(
                        "🔧 **Botto applied a fix** for this review comment.\n\n\
                         Commit: {}\n\n\
                         The fix was applied and tests passed in a sandboxed Docker container.",
                        sha,
                    );
                    if let Some(pid) = project_id {
                        if let Err(e) = crate::services::gitlab::client::post_mr_note(
                            &gl_cfg, pid, mr_iid, &comment_body,
                        ).await {
                            tracing::warn!("failed to post fix comment on MR: {}", e);
                        }
                    }
                }
            }

            let msg = WsOutbound::FixComplete {
                job_id: result.job_id,
                comment_id: comment_id.to_string(),
                commit_sha: result.commit_sha,
                error: result.error,
            };
            // Broadcast to all MR viewers (includes the requester).
            // Don't also send via tx — that would double-deliver to the requester.
            let complete_msg = serde_json::to_string(&msg).unwrap();
            state.broadcast_to_mr(&mr_ref, &complete_msg);
        }
        None => {
            let msg = WsOutbound::FixComplete {
                job_id,
                comment_id: comment_id.to_string(),
                commit_sha: None,
                error: Some("failed to initialize sandbox (Docker not available?)".into()),
            };
            let _ = tx.send(serde_json::to_string(&msg).unwrap());
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Try to decompress gzip data, fall back to raw bytes.
pub(crate) fn decompress_or_raw(data: &[u8]) -> Vec<u8> {
    use flate2::read::GzDecoder;
    use std::io::Read;

    let mut decoder = GzDecoder::new(data);
    let mut decompressed = Vec::new();
    match decoder.read_to_end(&mut decompressed) {
        Ok(_) => decompressed,
        Err(_) => data.to_vec(),
    }
}
