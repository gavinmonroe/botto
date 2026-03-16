// ---------------------------------------------------------------------------
// WebSocket gateway — the primary communication channel between Otto and Botto.
//
// Lifecycle:
//   1. Otto opens WS connection to /ws
//   2. First message must be AUTH with the shared API key + user identity
//   3. After auth, all Otto message types are routed to handlers
//   4. Streaming responses (reviews, chat) are multiplexed over the same WS
//      using a stream_id field so the client can demux
//   5. On disconnect, presence is cleaned up and in-flight streams are aborted
//
// Design decisions:
//   - One WS connection per Otto instance (not per-stream). Multiplexing is
//     cheaper than multiple connections and avoids Chrome's 6-connection limit.
//   - Auth is first-message, not query param, to avoid API keys in server logs.
//   - Each connection gets a broadcast::Sender for targeted sends from any task.
//   - The read loop is the only place that receives from the WS. The write loop
//     merges outbound messages from the broadcast channel + direct sends.
// ---------------------------------------------------------------------------

use crate::router;
use crate::types::state::{AppState, Connection, MrRef, ViewingFile};
use axum::extract::ws::{Message, WebSocket};
use axum::extract::{State, WebSocketUpgrade};
use axum::response::IntoResponse;
use futures::stream::SplitSink;
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{broadcast, Mutex};
use tracing::{debug, info, warn};

/// Max message size: 16MB (large diffs can be big).
const MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;

/// How long to wait for the AUTH message before closing.
const AUTH_TIMEOUT_SECS: u64 = 10;

/// Minimum interval between VIEWING_FILES updates from a single connection.
/// Server-side safety net behind the client's 3s throttle.
const VIEWING_FILES_MIN_INTERVAL: std::time::Duration = std::time::Duration::from_secs(2);

pub async fn handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> impl IntoResponse {
    ws.max_message_size(MAX_MESSAGE_SIZE)
        .on_upgrade(move |socket| handle_connection(socket, state))
}

// ---------------------------------------------------------------------------
// Connection handling
// ---------------------------------------------------------------------------

async fn handle_connection(socket: WebSocket, state: AppState) {
    let conn_id = uuid::Uuid::new_v4().to_string();
    let (ws_sink, mut ws_stream) = socket.split();
    let ws_sink = Arc::new(Mutex::new(ws_sink));

    // Create a broadcast channel for this connection (for targeted sends from other tasks).
    // Capacity 256 — if the client can't keep up, messages are dropped (lagged).
    let (tx, mut rx) = broadcast::channel::<String>(256);

    info!("ws connected: {}", conn_id);

    // --- Auth handshake ---
    // First message must be AUTH. Wait up to AUTH_TIMEOUT_SECS.
    let auth_result = tokio::time::timeout(
        std::time::Duration::from_secs(AUTH_TIMEOUT_SECS),
        wait_for_auth(&mut ws_stream, &state),
    )
    .await;

    let user_id = match auth_result {
        Ok(Some(uid)) => uid,
        Ok(None) => {
            let _ = send_json(&ws_sink, &WsOutbound::AuthError {
                error: "authentication failed".into(),
            })
            .await;
            info!("ws auth failed: {}", conn_id);
            return;
        }
        Err(_) => {
            let _ = send_json(&ws_sink, &WsOutbound::AuthError {
                error: "auth timeout".into(),
            })
            .await;
            info!("ws auth timeout: {}", conn_id);
            return;
        }
    };

    // Register connection — resolve GitLab profile for display name + avatar
    let (display_name, avatar_url) = {
        let cfg = state.config();
        if !cfg.gitlab.bot_token.is_empty() && !cfg.gitlab.url.is_empty() {
            let gl_cfg = crate::services::gitlab::client::GitLabConfig {
                base_url: cfg.gitlab.url.clone(),
                token: cfg.gitlab.bot_token.clone(),
            };
            match crate::services::gitlab::client::fetch_user_by_username(&gl_cfg, &user_id).await {
                Ok(Some(gl_user)) => (Some(gl_user.name), gl_user.avatar_url),
                _ => (None, None),
            }
        } else {
            (None, None)
        }
    };

    let conn = Connection {
        id: conn_id.clone(),
        user_id: Some(user_id.clone()),
        display_name,
        avatar_url,
        authenticated: true,
        viewing_mr: None,
        viewing_files: Vec::new(),
        files_updated_at: None,
        tx: tx.clone(),
    };
    state.connections().insert(conn_id.clone(), conn);

    // Persist to DB (best-effort)
    let _ = crate::db::queries::upsert_connection(
        state.pool(),
        &conn_id,
        Some(&user_id),
        None,
    )
    .await;

    // Send auth success with server capabilities
    let cfg = state.config();
    let _ = send_json(&ws_sink, &WsOutbound::AuthOk {
        capabilities: ServerCapabilities {
            sandbox_enabled: cfg.sandbox.enabled,
            max_concurrent_reviews: cfg.sandbox.max_concurrent,
            shared_triage_available: true,
            version: env!("CARGO_PKG_VERSION").to_string(),
        },
    })
    .await;

    info!("ws authenticated: {} user={}", conn_id, user_id);

    // Track active streams for this connection (for abort on disconnect).
    let active_streams: Arc<Mutex<HashMap<String, tokio::sync::watch::Sender<bool>>>> =
        Arc::new(Mutex::new(HashMap::new()));

    // --- Write loop: forward broadcast messages to the WebSocket ---
    // Also sends periodic pings to keep the connection alive through
    // proxies/load balancers that kill idle connections.
    let write_sink = ws_sink.clone();
    let write_conn_id = conn_id.clone();
    let write_handle = tokio::spawn(async move {
        let mut ping_interval = tokio::time::interval(std::time::Duration::from_secs(30));
        ping_interval.tick().await; // skip first immediate tick

        loop {
            tokio::select! {
                msg = rx.recv() => {
                    match msg {
                        Ok(text) => {
                            if send_text(&write_sink, &text).await.is_err() {
                                debug!("ws write failed (client gone): {}", write_conn_id);
                                break;
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            warn!("ws broadcast lagged by {} messages, disconnecting slow client: {}", n, write_conn_id);
                            break;
                        }
                        Err(_) => break,
                    }
                }
                _ = ping_interval.tick() => {
                    let ping_data = b"botto".to_vec();
                    if write_sink.lock().await.send(Message::Ping(ping_data.into())).await.is_err() {
                        break;
                    }
                }
            }
        }
    });

    // --- Read loop: receive messages from Otto, route to handlers ---
    let read_state = state.clone();
    let read_conn_id = conn_id.clone();
    let read_streams = active_streams.clone();
    let read_sink = ws_sink.clone();

    loop {
        match ws_stream.next().await {
            Some(Ok(Message::Text(text))) => {
                handle_message(
                    &read_state,
                    &read_conn_id,
                    &user_id,
                    &text,
                    &read_sink,
                    &tx,
                    &read_streams,
                )
                .await;
            }
            Some(Ok(Message::Close(_))) | None => {
                debug!("ws closed: {}", read_conn_id);
                break;
            }
            Some(Ok(Message::Ping(data))) => {
                let _ = ws_sink.lock().await.send(Message::Pong(data)).await;
            }
            Some(Err(e)) => {
                debug!("ws error: {} — {}", read_conn_id, e);
                break;
            }
            _ => {}
        }
    }

    // --- Cleanup ---
    write_handle.abort();

    // Abort all active streams
    let streams = active_streams.lock().await;
    for (stream_id, cancel_tx) in streams.iter() {
        debug!("aborting stream {} for {}", stream_id, conn_id);
        let _ = cancel_tx.send(true);
    }
    drop(streams);

    // Publish leave event if viewing an MR, and broadcast presence removal
    if let Some(entry) = state.connections().get(&conn_id) {
        if let Some(ref mr) = entry.viewing_mr {
            state.event_bus().publish(crate::services::events::Event {
                event_type: crate::services::events::EventType::UserLeftMr,
                project_path: mr.project_path.clone(),
                mr_iid: Some(mr.mr_iid),
                user_id: Some(user_id.clone()),
                payload: None,
            });
            // Broadcast empty presence so other viewers remove this user's avatars
            let msg = WsOutbound::PresenceUpdate {
                user_id: user_id.clone(),
                display_name: entry.display_name.clone(),
                avatar_url: entry.avatar_url.clone(),
                files: vec![],
            };
            state.broadcast_to_mr_except(mr, &serde_json::to_string(&msg).unwrap(), &conn_id);
        }
    }

    // Remove from connections + secondary index
    state.remove_connection(&conn_id);
    let _ = crate::db::queries::remove_connection(state.pool(), &conn_id).await;

    info!("ws disconnected: {} user={}", conn_id, user_id);
}

// ---------------------------------------------------------------------------
// Auth handshake
// ---------------------------------------------------------------------------

async fn wait_for_auth(
    ws_stream: &mut futures::stream::SplitStream<WebSocket>,
    state: &AppState,
) -> Option<String> {
    while let Some(Ok(msg)) = ws_stream.next().await {
        if let Message::Text(text) = msg {
            let parsed: Result<WsInbound, _> = serde_json::from_str(&text);
            match parsed {
                Ok(WsInbound::Auth { api_key, user_id }) => {
                    let expected = &state.config().auth.api_key;
                    // If no API key is configured, allow all connections (dev mode).
                    if expected.is_empty() || api_key == *expected {
                        return Some(user_id);
                    }
                    return None;
                }
                _ => {
                    // First message must be AUTH
                    return None;
                }
            }
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Message dispatch
// ---------------------------------------------------------------------------

async fn handle_message(
    state: &AppState,
    conn_id: &str,
    user_id: &str,
    text: &str,
    sink: &Arc<Mutex<SplitSink<WebSocket, Message>>>,
    tx: &broadcast::Sender<String>,
    active_streams: &Arc<Mutex<HashMap<String, tokio::sync::watch::Sender<bool>>>>,
) {
    let parsed: Result<WsInbound, _> = serde_json::from_str(text);
    let msg = match parsed {
        Ok(m) => m,
        Err(e) => {
            warn!("invalid message from {}: {}", conn_id, e);
            let _ = send_json(sink, &WsOutbound::Error {
                error: format!("invalid message: {}", e),
                request_id: None,
            })
            .await;
            return;
        }
    };

    match msg {
        WsInbound::Auth { .. } => {
            // Already authenticated, ignore duplicate auth
        }

        WsInbound::ViewingMr {
            project_path,
            mr_iid,
        } => {
            let mr_ref = MrRef {
                project_path: project_path.clone(),
                mr_iid,
            };

            // Update connection state + secondary index (returns old MR if switching)
            let old_mr = state.track_viewer(conn_id, &mr_ref);

            // Publish leave for previous MR if switching
            if let Some(ref old) = old_mr {
                state.event_bus().publish(crate::services::events::Event {
                    event_type: crate::services::events::EventType::UserLeftMr,
                    project_path: old.project_path.clone(),
                    mr_iid: Some(old.mr_iid),
                    user_id: Some(user_id.to_string()),
                    payload: None,
                });
            }

            let _ = crate::db::queries::update_viewing_mr(
                state.pool(),
                conn_id,
                Some(&mr_ref.key()),
            )
            .await;

            // Publish join event
            state.event_bus().publish(crate::services::events::Event {
                event_type: crate::services::events::EventType::UserJoinedMr,
                project_path: project_path.clone(),
                mr_iid: Some(mr_iid),
                user_id: Some(user_id.to_string()),
                payload: None,
            });

            // Send back any existing cached review + comment actions + presence snapshot
            let _ = router::handle_viewing_mr(state, conn_id, user_id, &mr_ref, tx).await;
        }

        WsInbound::LeftMr => {
            // Grab display info before untracking (which clears viewing_mr)
            let (display_name, avatar_url) = state.connections().get(conn_id)
                .map(|c| (c.display_name.clone(), c.avatar_url.clone()))
                .unwrap_or((None, None));

            let old_mr = state.untrack_viewer(conn_id);
            if let Some(ref mr) = old_mr {
                state.event_bus().publish(crate::services::events::Event {
                    event_type: crate::services::events::EventType::UserLeftMr,
                    project_path: mr.project_path.clone(),
                    mr_iid: Some(mr.mr_iid),
                    user_id: Some(user_id.to_string()),
                    payload: None,
                });
                // Broadcast presence removal to remaining viewers
                let msg = WsOutbound::PresenceUpdate {
                    user_id: user_id.to_string(),
                    display_name,
                    avatar_url,
                    files: vec![],
                };
                state.broadcast_to_mr_except(mr, &serde_json::to_string(&msg).unwrap(), conn_id);
            }
            let _ =
                crate::db::queries::update_viewing_mr(state.pool(), conn_id, None).await;
        }

        WsInbound::ViewingFiles { files } => {
            // Rate limit: ignore if last update was less than 2s ago
            let now = tokio::time::Instant::now();
            let should_process = if let Some(conn) = state.connections().get(conn_id) {
                match conn.files_updated_at {
                    Some(last) => now.duration_since(last) >= VIEWING_FILES_MIN_INTERVAL,
                    None => true,
                }
            } else {
                false
            };

            if !should_process {
                // Silently drop — client will send again in a few seconds
                return;
            }

            // Get the MR this connection is viewing (needed for broadcast targeting)
            let mr_ref = state.connections().get(conn_id)
                .and_then(|c| c.viewing_mr.clone());

            if let Some(ref mr) = mr_ref {
                // Update connection state and grab display info
                let (display_name, avatar_url) = if let Some(mut conn) = state.connections().get_mut(conn_id) {
                    conn.viewing_files = files.clone();
                    conn.files_updated_at = Some(now);
                    (conn.display_name.clone(), conn.avatar_url.clone())
                } else {
                    (None, None)
                };

                // Broadcast delta to other viewers of the same MR
                let msg = WsOutbound::PresenceUpdate {
                    user_id: user_id.to_string(),
                    display_name,
                    avatar_url,
                    files,
                };
                state.broadcast_to_mr_except(mr, &serde_json::to_string(&msg).unwrap(), conn_id);
            }
        }

        WsInbound::Request { request_id, payload } => {
            // One-shot request/response — route to handler, send response back
            let state = state.clone();
            let sink = sink.clone();
            let request_id_clone = request_id.clone();
            tokio::spawn(async move {
                let response = router::handle_request(&state, &payload).await;
                let outbound = WsOutbound::Response {
                    request_id: request_id_clone,
                    payload: response,
                };
                let _ = send_json(&sink, &outbound).await;
            });
        }

        WsInbound::StreamStart {
            stream_id,
            payload,
        } => {
            // Start a streaming operation (review, chat).
            // Create a cancellation channel for this stream.
            let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
            active_streams
                .lock()
                .await
                .insert(stream_id.clone(), cancel_tx);

            let state = state.clone();
            let tx = tx.clone();
            let stream_id_clone = stream_id.clone();
            let conn_id = conn_id.to_string();
            let user_id = user_id.to_string();
            let streams = active_streams.clone();

            tokio::spawn(async move {
                router::handle_stream(
                    &state,
                    &conn_id,
                    &user_id,
                    &stream_id_clone,
                    &payload,
                    &tx,
                    cancel_rx,
                )
                .await;

                // Remove from active streams when done
                streams.lock().await.remove(&stream_id_clone);
            });
        }

        WsInbound::StreamCancel { stream_id } => {
            if let Some(cancel_tx) = active_streams.lock().await.get(&stream_id) {
                let _ = cancel_tx.send(true);
            }
        }

        WsInbound::CommentAction {
            project_path,
            mr_iid,
            comment_id,
            action,
            edited_body,
        } => {
            let state = state.clone();
            let conn_id = conn_id.to_string();
            let user_id = user_id.to_string();
            tokio::spawn(async move {
                router::handle_comment_action(
                    &state,
                    &conn_id,
                    &user_id,
                    &project_path,
                    mr_iid,
                    &comment_id,
                    &action,
                    edited_body.as_deref(),
                )
                .await;
            });
        }

        WsInbound::RequestFix {
            project_path,
            mr_iid,
            comment_id,
            suggestion,
            file_path,
            original_code,
            source_branch,
            comment_body,
            comment_title,
            severity,
            target_branch,
            start_line,
            end_line,
            gitlab_note_id,
        } => {
            let state = state.clone();
            let conn_id = conn_id.to_string();
            let user_id = user_id.to_string();
            let tx = tx.clone();
            tokio::spawn(async move {
                router::handle_fix_request(
                    &state,
                    &conn_id,
                    &user_id,
                    &project_path,
                    mr_iid,
                    &comment_id,
                    &suggestion,
                    &file_path,
                    &original_code,
                    source_branch.as_deref().unwrap_or("main"),
                    comment_body.as_deref(),
                    comment_title.as_deref(),
                    severity.as_deref(),
                    target_branch.as_deref(),
                    start_line,
                    end_line,
                    gitlab_note_id,
                    &tx,
                )
                .await;
            });
        }
    }
}

// ---------------------------------------------------------------------------
// Send helpers
// ---------------------------------------------------------------------------

async fn send_json<T: Serialize>(
    sink: &Arc<Mutex<SplitSink<WebSocket, Message>>>,
    msg: &T,
) -> Result<(), axum::Error> {
    let text = serde_json::to_string(msg).unwrap();
    send_text(sink, &text).await
}

async fn send_text(
    sink: &Arc<Mutex<SplitSink<WebSocket, Message>>>,
    text: &str,
) -> Result<(), axum::Error> {
    sink.lock()
        .await
        .send(Message::Text(text.into()))
        .await
        .map_err(|e| axum::Error::new(e))
}

// ---------------------------------------------------------------------------
// Wire protocol types — what goes over the WebSocket.
//
// Inbound = Otto → Botto. Outbound = Botto → Otto.
// These wrap the Otto message protocol with connection-level framing
// (auth, stream multiplexing, presence).
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
pub enum WsInbound {
    #[serde(rename = "AUTH")]
    Auth { api_key: String, user_id: String },

    #[serde(rename = "VIEWING_MR")]
    ViewingMr {
        project_path: String,
        mr_iid: u64,
    },

    #[serde(rename = "LEFT_MR")]
    LeftMr,

    /// File-level presence: which files are visible in the user's viewport.
    /// Rate-limited server-side to max once per 2s per connection.
    #[serde(rename = "VIEWING_FILES")]
    ViewingFiles {
        files: Vec<ViewingFile>,
    },

    /// One-shot request/response (maps to Otto's sendMessage pattern).
    #[serde(rename = "REQUEST")]
    Request {
        request_id: String,
        payload: serde_json::Value,
    },

    /// Start a streaming operation (maps to Otto's openStream/port pattern).
    #[serde(rename = "STREAM_START")]
    StreamStart {
        stream_id: String,
        payload: serde_json::Value,
    },

    /// Cancel an in-flight stream.
    #[serde(rename = "STREAM_CANCEL")]
    StreamCancel { stream_id: String },

    /// Comment action (accept/dismiss/edit).
    #[serde(rename = "COMMENT_ACTION")]
    CommentAction {
        project_path: String,
        mr_iid: u64,
        comment_id: String,
        action: String,
        edited_body: Option<String>,
    },

    /// Request a sandbox fix.
    #[serde(rename = "REQUEST_FIX")]
    RequestFix {
        project_path: String,
        mr_iid: u64,
        comment_id: String,
        suggestion: String,
        file_path: String,
        original_code: String,
        source_branch: Option<String>,
        comment_body: Option<String>,
        comment_title: Option<String>,
        severity: Option<String>,
        target_branch: Option<String>,
        start_line: Option<u32>,
        end_line: Option<u32>,
        /// GitLab note ID for reply threading. When a fix originates from a
        /// follow-up comment analysis (not a Botto review comment), the
        /// `comment_id` is an Otto-internal tracking key. This field carries
        /// the real GitLab note ID so the post-fix comment replies to the
        /// correct discussion thread.
        gitlab_note_id: Option<i64>,
    },
}

#[derive(Debug, Serialize)]
#[serde(tag = "type")]
pub enum WsOutbound {
    #[serde(rename = "AUTH_OK")]
    AuthOk { capabilities: ServerCapabilities },

    #[serde(rename = "AUTH_ERROR")]
    AuthError { error: String },

    /// Response to a one-shot REQUEST.
    #[serde(rename = "RESPONSE")]
    Response {
        request_id: String,
        payload: serde_json::Value,
    },

    /// A chunk in a multiplexed stream.
    #[serde(rename = "STREAM_CHUNK")]
    StreamChunk {
        stream_id: String,
        chunk: serde_json::Value,
    },

    /// Stream completed.
    #[serde(rename = "STREAM_END")]
    StreamEnd {
        stream_id: String,
    },

    /// Error (not tied to a specific request).
    #[serde(rename = "ERROR")]
    Error {
        error: String,
        request_id: Option<String>,
    },

    /// Broadcast: another user's comment action.
    #[serde(rename = "COMMENT_ACTION_BROADCAST")]
    CommentActionBroadcast {
        project_path: String,
        mr_iid: u64,
        comment_id: String,
        user_id: String,
        action: String,
        edited_body: Option<String>,
    },

    /// Sandbox fix progress.
    #[serde(rename = "FIX_PROGRESS")]
    FixProgress {
        job_id: String,
        comment_id: String,
        status: String,
        detail: String,
    },

    /// Sandbox fix completed.
    #[serde(rename = "FIX_COMPLETE")]
    FixComplete {
        job_id: String,
        comment_id: String,
        commit_sha: Option<String>,
        error: Option<String>,
        /// URL of the auto-created MR when fix_branch_mode is "new_branch".
        #[serde(skip_serializing_if = "Option::is_none")]
        fix_mr_url: Option<String>,
    },

    /// Cached review delivered on MR join.
    #[serde(rename = "CACHED_REVIEW")]
    CachedReview {
        project_path: String,
        mr_iid: u64,
        diff_hash: String,
        review: serde_json::Value,
        file_diff_hashes: String,
    },

    /// Sync all comment actions for an MR on join.
    #[serde(rename = "COMMENT_ACTIONS_SYNC")]
    CommentActionsSync {
        project_path: String,
        mr_iid: u64,
        actions: Vec<serde_json::Value>,
    },

    /// Generic event notification.
    #[serde(rename = "EVENT_NOTIFICATION")]
    EventNotification {
        event_type: String,
        project_path: String,
        mr_iid: Option<u64>,
        payload: Option<serde_json::Value>,
    },

    /// File-level presence delta: one user's current viewport files changed.
    /// Sent to all other viewers of the same MR. Empty `files` means the user
    /// left the MR or disconnected — remove their presence indicators.
    #[serde(rename = "PRESENCE_UPDATE")]
    PresenceUpdate {
        user_id: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        display_name: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        avatar_url: Option<String>,
        files: Vec<ViewingFile>,
    },

    /// Full presence snapshot: all viewers' file positions for an MR.
    /// Sent once to a user when they join an MR (same pattern as COMMENT_ACTIONS_SYNC).
    #[serde(rename = "PRESENCE_SNAPSHOT")]
    PresenceSnapshot {
        viewers: Vec<serde_json::Value>,
    },
}

#[derive(Debug, Clone, Serialize)]
pub struct ServerCapabilities {
    pub sandbox_enabled: bool,
    pub max_concurrent_reviews: u32,
    pub shared_triage_available: bool,
    pub version: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // REQUEST_FIX deserialization — verify gitlab_note_id handling
    // -----------------------------------------------------------------------

    #[test]
    fn request_fix_without_gitlab_note_id() {
        let json = r#"{
            "type": "REQUEST_FIX",
            "project_path": "mygroup/myproject",
            "mr_iid": 42,
            "comment_id": "abc-123",
            "suggestion": "let x = 1;",
            "file_path": "src/main.rs",
            "original_code": "let x = 0;"
        }"#;
        let msg: WsInbound = serde_json::from_str(json).unwrap();
        match msg {
            WsInbound::RequestFix {
                project_path,
                mr_iid,
                comment_id,
                suggestion,
                file_path,
                original_code,
                gitlab_note_id,
                ..
            } => {
                assert_eq!(project_path, "mygroup/myproject");
                assert_eq!(mr_iid, 42);
                assert_eq!(comment_id, "abc-123");
                assert_eq!(suggestion, "let x = 1;");
                assert_eq!(file_path, "src/main.rs");
                assert_eq!(original_code, "let x = 0;");
                assert_eq!(gitlab_note_id, None);
            }
            other => panic!("expected RequestFix, got {:?}", other),
        }
    }

    #[test]
    fn request_fix_with_gitlab_note_id() {
        let json = r#"{
            "type": "REQUEST_FIX",
            "project_path": "mygroup/myproject",
            "mr_iid": 42,
            "comment_id": "followup-99887766-0",
            "suggestion": "let x = 1;",
            "file_path": "src/main.rs",
            "original_code": "let x = 0;",
            "gitlab_note_id": 99887766
        }"#;
        let msg: WsInbound = serde_json::from_str(json).unwrap();
        match msg {
            WsInbound::RequestFix {
                comment_id,
                gitlab_note_id,
                ..
            } => {
                assert_eq!(comment_id, "followup-99887766-0");
                assert_eq!(gitlab_note_id, Some(99887766));
            }
            other => panic!("expected RequestFix, got {:?}", other),
        }
    }

    #[test]
    fn request_fix_with_null_gitlab_note_id() {
        let json = r#"{
            "type": "REQUEST_FIX",
            "project_path": "mygroup/myproject",
            "mr_iid": 42,
            "comment_id": "abc-123",
            "suggestion": "let x = 1;",
            "file_path": "src/main.rs",
            "original_code": "let x = 0;",
            "gitlab_note_id": null
        }"#;
        let msg: WsInbound = serde_json::from_str(json).unwrap();
        match msg {
            WsInbound::RequestFix { gitlab_note_id, .. } => {
                assert_eq!(gitlab_note_id, None);
            }
            other => panic!("expected RequestFix, got {:?}", other),
        }
    }

    #[test]
    fn request_fix_with_all_optional_fields() {
        let json = r#"{
            "type": "REQUEST_FIX",
            "project_path": "org/repo",
            "mr_iid": 7,
            "comment_id": "followup-12345-2",
            "suggestion": "fn fixed() {}",
            "file_path": "lib.rs",
            "original_code": "fn broken() {}",
            "source_branch": "feature-branch",
            "comment_body": "Follow-up fix explanation",
            "comment_title": "Follow-up fix: lib.rs",
            "severity": "warning",
            "target_branch": "main",
            "start_line": 10,
            "end_line": 15,
            "gitlab_note_id": 12345
        }"#;
        let msg: WsInbound = serde_json::from_str(json).unwrap();
        match msg {
            WsInbound::RequestFix {
                project_path,
                mr_iid,
                comment_id,
                suggestion,
                file_path,
                original_code,
                source_branch,
                comment_body,
                comment_title,
                severity,
                target_branch,
                start_line,
                end_line,
                gitlab_note_id,
            } => {
                assert_eq!(project_path, "org/repo");
                assert_eq!(mr_iid, 7);
                assert_eq!(comment_id, "followup-12345-2");
                assert_eq!(suggestion, "fn fixed() {}");
                assert_eq!(file_path, "lib.rs");
                assert_eq!(original_code, "fn broken() {}");
                assert_eq!(source_branch, Some("feature-branch".into()));
                assert_eq!(comment_body, Some("Follow-up fix explanation".into()));
                assert_eq!(comment_title, Some("Follow-up fix: lib.rs".into()));
                assert_eq!(severity, Some("warning".into()));
                assert_eq!(target_branch, Some("main".into()));
                assert_eq!(start_line, Some(10));
                assert_eq!(end_line, Some(15));
                assert_eq!(gitlab_note_id, Some(12345));
            }
            other => panic!("expected RequestFix, got {:?}", other),
        }
    }

    // -----------------------------------------------------------------------
    // Backward compatibility — existing review comment fix (no gitlab_note_id)
    // still works with numeric comment_id parsed as note ID in the router.
    // -----------------------------------------------------------------------

    #[test]
    fn request_fix_review_comment_backward_compat() {
        // This is what Otto sends today for a Botto review comment fix.
        // comment_id is the numeric GitLab note ID as a string.
        // gitlab_note_id is absent — the router falls back to parsing comment_id.
        let json = r#"{
            "type": "REQUEST_FIX",
            "project_path": "mygroup/myproject",
            "mr_iid": 42,
            "comment_id": "55443322",
            "suggestion": "let x = 1;",
            "file_path": "src/main.rs",
            "original_code": "let x = 0;",
            "source_branch": "fix-branch"
        }"#;
        let msg: WsInbound = serde_json::from_str(json).unwrap();
        match msg {
            WsInbound::RequestFix {
                comment_id,
                gitlab_note_id,
                ..
            } => {
                assert_eq!(comment_id, "55443322");
                assert_eq!(gitlab_note_id, None);
                // Router will do: gitlab_note_id.or_else(|| comment_id.parse().ok())
                // => Some(55443322) — backward compatible
                let resolved: Option<i64> = gitlab_note_id.or_else(|| comment_id.parse().ok());
                assert_eq!(resolved, Some(55443322));
            }
            other => panic!("expected RequestFix, got {:?}", other),
        }
    }

    #[test]
    fn request_fix_followup_note_id_resolution() {
        // Follow-up fix: comment_id is a tracking key, gitlab_note_id is the real note.
        let json = r#"{
            "type": "REQUEST_FIX",
            "project_path": "mygroup/myproject",
            "mr_iid": 42,
            "comment_id": "followup-99887766-0",
            "suggestion": "let x = 1;",
            "file_path": "src/main.rs",
            "original_code": "let x = 0;",
            "gitlab_note_id": 99887766
        }"#;
        let msg: WsInbound = serde_json::from_str(json).unwrap();
        match msg {
            WsInbound::RequestFix {
                comment_id,
                gitlab_note_id,
                ..
            } => {
                // comment_id is NOT a valid i64 — it's a tracking key
                assert!(comment_id.parse::<i64>().is_err());
                // gitlab_note_id carries the real note ID
                assert_eq!(gitlab_note_id, Some(99887766));
                // Router will do: gitlab_note_id.or_else(|| comment_id.parse().ok())
                // => Some(99887766) — uses the explicit field
                let resolved: Option<i64> = gitlab_note_id.or_else(|| comment_id.parse().ok());
                assert_eq!(resolved, Some(99887766));
            }
            other => panic!("expected RequestFix, got {:?}", other),
        }
    }
}
