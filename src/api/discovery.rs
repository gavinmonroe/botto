// ---------------------------------------------------------------------------
// Discovery endpoint — allows Otto extensions to auto-discover Botto.
//
// GET /.well-known/botto returns server info and capabilities.
// Otto checks this URL when a user adds a GitLab host.
// ---------------------------------------------------------------------------

use crate::types::state::AppState;
use axum::extract::State;
use axum::response::Json;
use serde_json::{json, Value};

pub async fn well_known(State(state): State<AppState>) -> Json<Value> {
    let cfg = state.config();
    Json(json!({
        "name": "botto",
        "version": env!("CARGO_PKG_VERSION"),
        "ws": format!("ws://{}:{}/ws", cfg.server.host, cfg.server.port),
        "capabilities": {
            "sandbox": cfg.sandbox.enabled,
            "shared_triage": true,
            "review_queue": true,
            "webhooks": cfg.gitlab.webhook_secret.is_some(),
        }
    }))
}
