// ---------------------------------------------------------------------------
// Integration smoke test — starts the server, connects via WebSocket,
// authenticates, and verifies the basic request/response flow.
// ---------------------------------------------------------------------------

use botto::config::*;
use botto::db;
use botto::server;
use botto::types::state::AppState;
use futures::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::path::PathBuf;
use tokio::net::TcpListener;
use tokio_tungstenite::{connect_async, tungstenite::Message};

/// Build a test config with an ephemeral SQLite database.
fn test_config(port: u16) -> BottoConfig {
    BottoConfig {
        server: ServerConfig {
            host: "127.0.0.1".into(),
            port,
            max_concurrent_reviews: 3,
            max_concurrent_ai_calls: 6,
        },
        auth: AuthConfig {
            api_key: "test-key".into(),
        },
        gitlab: GitLabConfig {
            url: "https://gitlab.example.com".into(),
            bot_token: "".into(),
            webhook_secret: None,
        },
        ai: AiConfig {
            base_url: "".into(),
            api_key: "".into(),
            models: AiModelConfig::default(),
        },
        sandbox: SandboxConfig {
            enabled: false,
            docker_available: false,
            max_concurrent: 1,
            timeout_seconds: 60,
            max_memory_mb: 512,
            max_disk_mb: 1024,
            fix_branch_mode: botto::config::FixBranchMode::SameBranch,
            warm_containers: false,
            warm_idle_timeout_secs: 600,
            warm_max_lifetime_secs: 3600,
            live_output: false,
            output_redaction: true,
        },
        cache: CacheConfig {
            review_ttl_days: 7,
            max_cached_reviews: 100,
        },
        harness: HarnessConfig {
            enabled: false,
            max_rounds: 1,
            variants_per_round: 2,
            concurrency: 1,
            test_cases: 1,
            gitlab_seed_orgs: vec![],
            memory_dir: PathBuf::from("/tmp/harness"),
            judge_model: "claude-opus-4-6".into(),
        },
        data_dir: PathBuf::from("/tmp"),
    }
}

/// Find a free port for the test server.
async fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap().port()
}

#[tokio::test]
async fn test_ws_auth_and_health() {
    let port = free_port().await;
    let cfg = test_config(port);

    // In-memory SQLite
    let pool = db::init(std::path::Path::new(":memory:")).await.unwrap();
    let state = AppState::new(cfg, pool);

    // Start server in background
    let state_clone = state.clone();
    let server_handle = tokio::spawn(async move {
        server::run(state_clone).await.unwrap();
    });

    // Give server a moment to bind
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Test health endpoint
    let health_resp = reqwest::get(format!("http://127.0.0.1:{}/health", port))
        .await
        .unwrap();
    assert_eq!(health_resp.status(), 200);
    let health_body: Value = health_resp.json().await.unwrap();
    assert_eq!(health_body["status"], "ok");

    // Test readiness endpoint
    let ready_resp = reqwest::get(format!("http://127.0.0.1:{}/ready", port))
        .await
        .unwrap();
    assert_eq!(ready_resp.status(), 200);

    // Test discovery endpoint
    let discovery_resp = reqwest::get(format!("http://127.0.0.1:{}/.well-known/botto", port))
        .await
        .unwrap();
    assert_eq!(discovery_resp.status(), 200);
    let discovery_body: Value = discovery_resp.json().await.unwrap();
    assert_eq!(discovery_body["name"], "botto");

    // Connect via WebSocket
    let (mut ws, _) = connect_async(format!("ws://127.0.0.1:{}/ws", port))
        .await
        .unwrap();

    // Send AUTH
    ws.send(Message::Text(
        json!({
            "type": "AUTH",
            "api_key": "test-key",
            "user_id": "testuser"
        })
        .to_string()
        .into(),
    ))
    .await
    .unwrap();

    // Receive AUTH_OK
    let msg = ws.next().await.unwrap().unwrap();
    let auth_resp: Value = serde_json::from_str(msg.to_text().unwrap()).unwrap();
    assert_eq!(auth_resp["type"], "AUTH_OK");
    assert!(auth_resp["capabilities"]["shared_triage_available"]
        .as_bool()
        .unwrap());

    // Send a one-shot request (GET_SETTINGS)
    ws.send(Message::Text(
        json!({
            "type": "REQUEST",
            "request_id": "req_1",
            "payload": { "type": "GET_SETTINGS" }
        })
        .to_string()
        .into(),
    ))
    .await
    .unwrap();

    // Receive response
    let msg = ws.next().await.unwrap().unwrap();
    let resp: Value = serde_json::from_str(msg.to_text().unwrap()).unwrap();
    assert_eq!(resp["type"], "RESPONSE");
    assert_eq!(resp["request_id"], "req_1");
    assert_eq!(resp["payload"]["ok"], true);

    // Test bad auth
    let (mut ws2, _) = connect_async(format!("ws://127.0.0.1:{}/ws", port))
        .await
        .unwrap();

    ws2.send(Message::Text(
        json!({
            "type": "AUTH",
            "api_key": "wrong-key",
            "user_id": "baduser"
        })
        .to_string()
        .into(),
    ))
    .await
    .unwrap();

    let msg2 = ws2.next().await.unwrap().unwrap();
    let auth_err: Value = serde_json::from_str(msg2.to_text().unwrap()).unwrap();
    assert_eq!(auth_err["type"], "AUTH_ERROR");

    // Clean up
    ws.close(None).await.ok();
    server_handle.abort();
}

#[tokio::test]
async fn test_comment_actions_persistence() {
    let port = free_port().await;
    let cfg = test_config(port);
    let pool = db::init(std::path::Path::new(":memory:")).await.unwrap();
    let state = AppState::new(cfg, pool);

    let state_clone = state.clone();
    let server_handle = tokio::spawn(async move {
        server::run(state_clone).await.unwrap();
    });

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Connect and auth
    let (mut ws, _) = connect_async(format!("ws://127.0.0.1:{}/ws", port))
        .await
        .unwrap();

    ws.send(Message::Text(
        json!({ "type": "AUTH", "api_key": "test-key", "user_id": "alice" })
            .to_string()
            .into(),
    ))
    .await
    .unwrap();

    // Consume AUTH_OK
    ws.next().await.unwrap().unwrap();

    // Send a comment action
    ws.send(Message::Text(
        json!({
            "type": "COMMENT_ACTION",
            "project_path": "team/repo",
            "mr_iid": 42,
            "comment_id": "c1",
            "action": "accepted",
            "edited_body": null
        })
        .to_string()
        .into(),
    ))
    .await
    .unwrap();

    // Small delay for async processing
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Query comment actions via request
    ws.send(Message::Text(
        json!({
            "type": "REQUEST",
            "request_id": "req_2",
            "payload": {
                "type": "GET_COMMENT_ACTIONS",
                "project_path": "team/repo",
                "mr_iid": 42
            }
        })
        .to_string()
        .into(),
    ))
    .await
    .unwrap();

    let msg = ws.next().await.unwrap().unwrap();
    let resp: Value = serde_json::from_str(msg.to_text().unwrap()).unwrap();
    assert_eq!(resp["type"], "RESPONSE");
    assert_eq!(resp["request_id"], "req_2");
    assert_eq!(resp["payload"]["ok"], true);

    let actions = resp["payload"]["data"].as_array().unwrap();
    assert_eq!(actions.len(), 1);
    assert_eq!(actions[0]["comment_id"], "c1");
    assert_eq!(actions[0]["user_id"], "alice");
    assert_eq!(actions[0]["action"], "accepted");

    ws.close(None).await.ok();
    server_handle.abort();
}

#[tokio::test]
async fn test_team_settings() {
    let port = free_port().await;
    let cfg = test_config(port);
    let pool = db::init(std::path::Path::new(":memory:")).await.unwrap();
    let state = AppState::new(cfg, pool);

    let state_clone = state.clone();
    let server_handle = tokio::spawn(async move {
        server::run(state_clone).await.unwrap();
    });

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let (mut ws, _) = connect_async(format!("ws://127.0.0.1:{}/ws", port))
        .await
        .unwrap();

    ws.send(Message::Text(
        json!({ "type": "AUTH", "api_key": "test-key", "user_id": "bob" })
            .to_string()
            .into(),
    ))
    .await
    .unwrap();
    ws.next().await.unwrap().unwrap(); // AUTH_OK

    // Set shared triage
    ws.send(Message::Text(
        json!({
            "type": "REQUEST",
            "request_id": "req_3",
            "payload": {
                "type": "SET_TEAM_SETTINGS",
                "project_path": "team/repo",
                "shared_triage": true
            }
        })
        .to_string()
        .into(),
    ))
    .await
    .unwrap();

    let msg = ws.next().await.unwrap().unwrap();
    let resp: Value = serde_json::from_str(msg.to_text().unwrap()).unwrap();
    assert_eq!(resp["payload"]["ok"], true);
    assert_eq!(resp["payload"]["data"]["shared_triage"], true);

    // Read it back
    ws.send(Message::Text(
        json!({
            "type": "REQUEST",
            "request_id": "req_4",
            "payload": {
                "type": "GET_TEAM_SETTINGS",
                "project_path": "team/repo"
            }
        })
        .to_string()
        .into(),
    ))
    .await
    .unwrap();

    let msg = ws.next().await.unwrap().unwrap();
    let resp: Value = serde_json::from_str(msg.to_text().unwrap()).unwrap();
    assert_eq!(resp["payload"]["data"]["shared_triage"], true);

    ws.close(None).await.ok();
    server_handle.abort();
}

// ---------------------------------------------------------------------------
// Helper: connect and authenticate a WebSocket client
// ---------------------------------------------------------------------------

async fn connect_and_auth(port: u16, user_id: &str) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>> {
    let (mut ws, _) = connect_async(format!("ws://127.0.0.1:{}/ws", port))
        .await
        .unwrap();

    ws.send(Message::Text(
        json!({ "type": "AUTH", "api_key": "test-key", "user_id": user_id })
            .to_string()
            .into(),
    ))
    .await
    .unwrap();

    // Consume AUTH_OK
    let msg = ws.next().await.unwrap().unwrap();
    let auth: Value = serde_json::from_str(msg.to_text().unwrap()).unwrap();
    assert_eq!(auth["type"], "AUTH_OK");

    ws
}

/// Helper: send a REQUEST and get the RESPONSE payload
async fn send_request(
    ws: &mut tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    request_id: &str,
    payload: Value,
) -> Value {
    ws.send(Message::Text(
        json!({
            "type": "REQUEST",
            "request_id": request_id,
            "payload": payload,
        })
        .to_string()
        .into(),
    ))
    .await
    .unwrap();

    let msg = ws.next().await.unwrap().unwrap();
    let resp: Value = serde_json::from_str(msg.to_text().unwrap()).unwrap();
    assert_eq!(resp["type"], "RESPONSE");
    assert_eq!(resp["request_id"], request_id);
    resp["payload"].clone()
}

// ---------------------------------------------------------------------------
// Presence tracking
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_presence_viewing_mr() {
    let port = free_port().await;
    let cfg = test_config(port);
    let pool = db::init(std::path::Path::new(":memory:")).await.unwrap();
    let state = AppState::new(cfg, pool);

    let state_clone = state.clone();
    let server_handle = tokio::spawn(async move {
        server::run(state_clone).await.unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let mut ws = connect_and_auth(port, "alice").await;

    // Send VIEWING_MR
    ws.send(Message::Text(
        json!({
            "type": "VIEWING_MR",
            "project_path": "team/repo",
            "mr_iid": 42
        })
        .to_string()
        .into(),
    ))
    .await
    .unwrap();

    // Small delay for async processing
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Verify the connection state tracks the MR
    let viewers = state.viewers_of(&botto::types::state::MrRef {
        project_path: "team/repo".to_string(),
        mr_iid: 42,
    });
    assert_eq!(viewers.len(), 1);

    // Send LEFT_MR
    ws.send(Message::Text(
        json!({ "type": "LEFT_MR" }).to_string().into(),
    ))
    .await
    .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let viewers = state.viewers_of(&botto::types::state::MrRef {
        project_path: "team/repo".to_string(),
        mr_iid: 42,
    });
    assert_eq!(viewers.len(), 0);

    ws.close(None).await.ok();
    server_handle.abort();
}

// ---------------------------------------------------------------------------
// Queue operations (enqueue, pause, resume, cancel)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_queue_operations() {
    let port = free_port().await;
    let cfg = test_config(port);
    let pool = db::init(std::path::Path::new(":memory:")).await.unwrap();
    let state = AppState::new(cfg, pool);

    let state_clone = state.clone();
    let server_handle = tokio::spawn(async move {
        server::run(state_clone).await.unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let mut ws = connect_and_auth(port, "bob").await;

    // Enqueue a review
    let resp = send_request(&mut ws, "q1", json!({
        "type": "ENQUEUE_REVIEW",
        "project_path": "team/repo",
        "mr_iid": 10,
        "priority_score": 75.0,
    })).await;
    assert_eq!(resp["ok"], true);
    assert_eq!(resp["data"]["status"], "queued");

    // Check queue status
    let resp = send_request(&mut ws, "q2", json!({
        "type": "GET_QUEUE_STATUS",
        "project_path": "team/repo",
    })).await;
    assert_eq!(resp["ok"], true);
    let items = resp["data"]["items"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["mr_iid"], 10);
    assert_eq!(items[0]["status"], "queued");

    // Pause the review
    let resp = send_request(&mut ws, "q3", json!({
        "type": "PAUSE_REVIEW",
        "project_path": "team/repo",
        "mr_iid": 10,
    })).await;
    assert_eq!(resp["ok"], true);
    assert_eq!(resp["data"], true);

    // Verify paused
    let resp = send_request(&mut ws, "q4", json!({
        "type": "GET_QUEUE_STATUS",
        "project_path": "team/repo",
    })).await;
    let items = resp["data"]["items"].as_array().unwrap();
    assert_eq!(items[0]["status"], "paused");

    // Resume the review
    let resp = send_request(&mut ws, "q5", json!({
        "type": "RESUME_REVIEW",
        "project_path": "team/repo",
        "mr_iid": 10,
    })).await;
    assert_eq!(resp["ok"], true);
    assert_eq!(resp["data"], true);

    // Cancel the review
    let resp = send_request(&mut ws, "q6", json!({
        "type": "CANCEL_REVIEW",
        "project_path": "team/repo",
        "mr_iid": 10,
    })).await;
    assert_eq!(resp["ok"], true);
    assert_eq!(resp["data"], true);

    // Verify empty queue
    let resp = send_request(&mut ws, "q7", json!({
        "type": "GET_QUEUE_STATUS",
        "project_path": "team/repo",
    })).await;
    let items = resp["data"]["items"].as_array().unwrap();
    assert_eq!(items.len(), 0);

    ws.close(None).await.ok();
    server_handle.abort();
}

// ---------------------------------------------------------------------------
// Unknown message type returns error
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_unknown_request_type() {
    let port = free_port().await;
    let cfg = test_config(port);
    let pool = db::init(std::path::Path::new(":memory:")).await.unwrap();
    let state = AppState::new(cfg, pool);

    let state_clone = state.clone();
    let server_handle = tokio::spawn(async move {
        server::run(state_clone).await.unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let mut ws = connect_and_auth(port, "charlie").await;

    let resp = send_request(&mut ws, "u1", json!({
        "type": "TOTALLY_FAKE_MESSAGE",
    })).await;
    assert_eq!(resp["ok"], false);
    assert!(resp["error"].as_str().unwrap().contains("unknown request type"));

    ws.close(None).await.ok();
    server_handle.abort();
}

// ---------------------------------------------------------------------------
// Sandbox job query (not found returns null)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_sandbox_job_not_found() {
    let port = free_port().await;
    let cfg = test_config(port);
    let pool = db::init(std::path::Path::new(":memory:")).await.unwrap();
    let state = AppState::new(cfg, pool);

    let state_clone = state.clone();
    let server_handle = tokio::spawn(async move {
        server::run(state_clone).await.unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let mut ws = connect_and_auth(port, "dave").await;

    let resp = send_request(&mut ws, "s1", json!({
        "type": "GET_SANDBOX_JOB",
        "job_id": "nonexistent-job-id",
    })).await;
    assert_eq!(resp["ok"], true);
    assert_eq!(resp["data"], Value::Null);

    ws.close(None).await.ok();
    server_handle.abort();
}

// ---------------------------------------------------------------------------
// CRITICAL: Payload nesting + camelCase handling
//
// Otto sends messages in the format:
//   { type: "FETCH_MR_DISCUSSIONS", payload: { hostId: "h1", projectId: 42, mrIid: 7 } }
//
// Botto handlers historically expected flat snake_case:
//   { type: "FETCH_MR_DISCUSSIONS", project_id: 42, mr_iid: 7 }
//
// These tests verify that Botto correctly:
//   1. Unwraps the inner "payload" object
//   2. Accepts camelCase field names (projectId, mrIid, projectPath, etc.)
//   3. Still works with flat snake_case (Botto-native messages)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_otto_camelcase_nested_payload() {
    let port = free_port().await;
    let cfg = test_config(port);
    let pool = db::init(std::path::Path::new(":memory:")).await.unwrap();
    let state = AppState::new(cfg, pool);

    let state_clone = state.clone();
    let server_handle = tokio::spawn(async move {
        server::run(state_clone).await.unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let mut ws = connect_and_auth(port, "eve").await;

    // Test 1: GET_QUEUE_STATUS with Otto's nested camelCase format.
    // Otto sends: { type: "GET_QUEUE_STATUS", payload: { projectPath: "team/repo" } }
    let resp = send_request(&mut ws, "cc1", json!({
        "type": "GET_QUEUE_STATUS",
        "payload": { "projectPath": "team/repo" }
    })).await;
    assert_eq!(resp["ok"], true, "GET_QUEUE_STATUS with nested camelCase failed: {:?}", resp);
    assert!(resp["data"]["items"].is_array(), "expected items array");

    // Test 2: Same message with flat snake_case (Botto-native format).
    // This must also still work.
    let resp = send_request(&mut ws, "cc2", json!({
        "type": "GET_QUEUE_STATUS",
        "project_path": "team/repo"
    })).await;
    assert_eq!(resp["ok"], true, "GET_QUEUE_STATUS with flat snake_case failed: {:?}", resp);

    // Test 3: ENQUEUE_REVIEW with Otto's nested camelCase format.
    let resp = send_request(&mut ws, "cc3", json!({
        "type": "ENQUEUE_REVIEW",
        "payload": {
            "projectPath": "team/repo",
            "mrIid": 99,
            "priorityScore": 80.0
        }
    })).await;
    assert_eq!(resp["ok"], true, "ENQUEUE_REVIEW with nested camelCase failed: {:?}", resp);
    assert_eq!(resp["data"]["status"], "queued");

    // Test 4: Verify the enqueued item shows up (proves the camelCase fields were parsed)
    let resp = send_request(&mut ws, "cc4", json!({
        "type": "GET_QUEUE_STATUS",
        "payload": { "projectPath": "team/repo" }
    })).await;
    let items = resp["data"]["items"].as_array().unwrap();
    assert_eq!(items.len(), 1, "expected 1 queued item");
    assert_eq!(items[0]["mr_iid"], 99, "mr_iid should be 99");

    // Test 5: PAUSE_REVIEW with nested camelCase
    let resp = send_request(&mut ws, "cc5", json!({
        "type": "PAUSE_REVIEW",
        "payload": { "projectPath": "team/repo", "mrIid": 99 }
    })).await;
    assert_eq!(resp["ok"], true, "PAUSE_REVIEW with nested camelCase failed: {:?}", resp);
    assert_eq!(resp["data"], true, "should have paused 1 row");

    // Test 6: CANCEL_REVIEW with nested camelCase
    let resp = send_request(&mut ws, "cc6", json!({
        "type": "CANCEL_REVIEW",
        "payload": { "projectPath": "team/repo", "mrIid": 99 }
    })).await;
    assert_eq!(resp["ok"], true, "CANCEL_REVIEW with nested camelCase failed: {:?}", resp);

    // Test 7: GET_SANDBOX_JOB with nested camelCase
    let resp = send_request(&mut ws, "cc7", json!({
        "type": "GET_SANDBOX_JOB",
        "payload": { "jobId": "nonexistent" }
    })).await;
    assert_eq!(resp["ok"], true, "GET_SANDBOX_JOB with nested camelCase failed: {:?}", resp);
    assert_eq!(resp["data"], Value::Null);

    // Test 8: GET_COMMENT_ACTIONS with nested camelCase
    let resp = send_request(&mut ws, "cc8", json!({
        "type": "GET_COMMENT_ACTIONS",
        "payload": { "projectPath": "team/repo", "mrIid": 42 }
    })).await;
    assert_eq!(resp["ok"], true, "GET_COMMENT_ACTIONS with nested camelCase failed: {:?}", resp);

    ws.close(None).await.ok();
    server_handle.abort();
}

/// Regression test: flat snake_case messages must still work after the
/// camelCase/nesting fix. This catches accidental breakage of Botto-native
/// messages (team settings, comment actions, etc.).
#[tokio::test]
async fn test_flat_snake_case_still_works() {
    let port = free_port().await;
    let cfg = test_config(port);
    let pool = db::init(std::path::Path::new(":memory:")).await.unwrap();
    let state = AppState::new(cfg, pool);

    let state_clone = state.clone();
    let server_handle = tokio::spawn(async move {
        server::run(state_clone).await.unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let mut ws = connect_and_auth(port, "frank").await;

    // SET_TEAM_SETTINGS with flat snake_case (Botto-native)
    let resp = send_request(&mut ws, "fs1", json!({
        "type": "SET_TEAM_SETTINGS",
        "project_path": "team/repo",
        "shared_triage": true
    })).await;
    assert_eq!(resp["ok"], true, "SET_TEAM_SETTINGS flat snake_case failed: {:?}", resp);

    // GET_TEAM_SETTINGS with flat snake_case
    let resp = send_request(&mut ws, "fs2", json!({
        "type": "GET_TEAM_SETTINGS",
        "project_path": "team/repo"
    })).await;
    assert_eq!(resp["ok"], true);
    assert_eq!(resp["data"]["shared_triage"], true);

    // ENQUEUE + GET_QUEUE_STATUS with flat snake_case
    let resp = send_request(&mut ws, "fs3", json!({
        "type": "ENQUEUE_REVIEW",
        "project_path": "team/repo",
        "mr_iid": 55,
        "priority_score": 60.0
    })).await;
    assert_eq!(resp["ok"], true, "ENQUEUE_REVIEW flat snake_case failed: {:?}", resp);

    let resp = send_request(&mut ws, "fs4", json!({
        "type": "GET_QUEUE_STATUS",
        "project_path": "team/repo"
    })).await;
    let items = resp["data"]["items"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["mr_iid"], 55);

    ws.close(None).await.ok();
    server_handle.abort();
}
