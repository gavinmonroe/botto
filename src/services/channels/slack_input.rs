// ---------------------------------------------------------------------------
// Slack input adapter — Axum handlers for Slack Events API and interactive
// components (buttons from escalation messages).
//
// Handles:
//   - URL verification challenge (Slack sends this on endpoint registration)
//   - event_callback with app_mention and message events
//   - Interactive component payloads (block_actions from escalation buttons)
//   - Request signature verification via X-Slack-Signature
// ---------------------------------------------------------------------------

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::Json;
use tracing::{debug, info, warn};

use super::bus::MessageBus;
use super::types::{
    ChannelType, InboundAction, InboundMessage, MessageContext, ReplyTarget, ThreadType,
};
use crate::services::workflow::crud::epoch_secs;
use crate::types::state::AppState;

// ---------------------------------------------------------------------------
// Events API handler
// ---------------------------------------------------------------------------

pub async fn slack_events_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let cfg = state.config();
    if !cfg.channels.enabled || !cfg.channels.slack.enabled {
        return Err(StatusCode::NOT_FOUND);
    }

    // Verify Slack signature
    if !verify_slack_signature(&headers, &body, &cfg.channels.slack.signing_secret) {
        warn!("Slack webhook rejected: invalid signature");
        return Err(StatusCode::UNAUTHORIZED);
    }

    let payload: serde_json::Value = serde_json::from_slice(&body).map_err(|e| {
        warn!("Slack webhook rejected: invalid JSON: {}", e);
        StatusCode::BAD_REQUEST
    })?;

    // Handle URL verification challenge
    if payload["type"].as_str() == Some("url_verification") {
        let challenge = payload["challenge"]
            .as_str()
            .unwrap_or("")
            .to_string();
        debug!("Slack URL verification challenge received");
        return Ok(Json(serde_json::json!({ "challenge": challenge })));
    }

    // Handle event callbacks
    if payload["type"].as_str() == Some("event_callback") {
        let event = &payload["event"];
        let event_type = event["type"].as_str().unwrap_or("");

        match event_type {
            "app_mention" | "message" => {
                // Skip bot messages to prevent echo loops — when the bot posts
                // a reply, Slack sends it back as a message event.
                if event["bot_id"].is_string()
                    || event["subtype"].as_str() == Some("bot_message")
                {
                    debug!("ignoring bot message to prevent echo loop");
                    return Ok(Json(serde_json::json!({ "ok": true })));
                }

                // Only process messages that mention the bot or are DMs
                if event_type == "message" && event["channel_type"].as_str() != Some("im") {
                    // Skip non-DM messages that aren't app_mentions
                    return Ok(Json(serde_json::json!({ "ok": true })));
                }

                if let Some(bus) = state.message_bus() {
                    parse_slack_event(bus, event, &payload);
                }
            }
            _ => {
                debug!(event_type = %event_type, "ignoring Slack event type");
            }
        }
    }

    Ok(Json(serde_json::json!({ "ok": true })))
}

// ---------------------------------------------------------------------------
// Interactive components handler (buttons, menus, etc.)
// ---------------------------------------------------------------------------

pub async fn slack_interactions_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let cfg = state.config();
    if !cfg.channels.enabled || !cfg.channels.slack.enabled {
        return Err(StatusCode::NOT_FOUND);
    }

    // Slack sends interactive payloads as form-encoded with a "payload" field
    let body_str = String::from_utf8_lossy(&body);
    let payload_str = if body_str.starts_with("payload=") {
        urlencoding::decode(&body_str[8..])
            .unwrap_or_default()
            .to_string()
    } else {
        body_str.to_string()
    };

    // Verify signature against the raw body
    if !verify_slack_signature(&headers, &body, &cfg.channels.slack.signing_secret) {
        warn!("Slack interaction rejected: invalid signature");
        return Err(StatusCode::UNAUTHORIZED);
    }

    let payload: serde_json::Value = serde_json::from_str(&payload_str).map_err(|e| {
        warn!("Slack interaction rejected: invalid JSON: {}", e);
        StatusCode::BAD_REQUEST
    })?;

    let interaction_type = payload["type"].as_str().unwrap_or("");
    debug!(interaction_type = %interaction_type, "Slack interaction received");

    if interaction_type == "block_actions" {
        if let Some(bus) = state.message_bus() {
            parse_slack_interaction(bus, &payload);
        }
    }

    Ok(Json(serde_json::json!({ "ok": true })))
}

// ---------------------------------------------------------------------------
// Parsing helpers
// ---------------------------------------------------------------------------

fn parse_slack_event(bus: &MessageBus, event: &serde_json::Value, full_payload: &serde_json::Value) {
    let text = event["text"].as_str().unwrap_or("").to_string();
    let user_id = event["user"].as_str().unwrap_or("unknown").to_string();
    let channel_id = event["channel"].as_str().unwrap_or("").to_string();
    let thread_ts = event["thread_ts"]
        .as_str()
        .or_else(|| event["ts"].as_str())
        .map(|s| s.to_string());
    let ts = event["ts"].as_str().unwrap_or("").to_string();

    let is_dm = event["channel_type"].as_str() == Some("im");
    let thread_type = if is_dm {
        Some(ThreadType::DirectMessage)
    } else {
        Some(ThreadType::SlackThread)
    };

    // Strip bot mention from text (Slack includes <@BOTID> in app_mention events)
    let clean_text = strip_slack_mention(&text);
    let (action, content) = parse_slack_command(&clean_text);

    let now = epoch_secs();
    let reply_target = ReplyTarget {
        channel: ChannelType::Slack,
        target_id: channel_id.clone(),
        thread_id: thread_ts.clone(),
    };

    let message = InboundMessage {
        id: uuid::Uuid::new_v4().to_string(),
        context: MessageContext {
            channel: ChannelType::Slack,
            channel_id: channel_id.clone(),
            user_id: user_id.clone(),
            user_name: user_id.clone(), // Slack user ID; display name resolved later
            user_email: None,
            thread_id: thread_ts,
            thread_type,
            parent_message_id: Some(ts),
            project_path: None,
            project_id: None,
            reply_to: Some(reply_target),
            received_at: now,
            raw_payload: Some(full_payload.clone()),
        },
        action,
        raw_content: content,
        parsed_at: now,
    };

    info!(
        user = %user_id,
        channel = %channel_id,
        action = %message.action.action_name(),
        "parsed Slack event"
    );

    bus.publish_inbound(message);
}

fn parse_slack_interaction(bus: &MessageBus, payload: &serde_json::Value) {
    let user_id = payload["user"]["id"]
        .as_str()
        .unwrap_or("unknown")
        .to_string();
    let user_name = payload["user"]["username"]
        .as_str()
        .unwrap_or(&user_id)
        .to_string();
    let channel_id = payload["channel"]["id"]
        .as_str()
        .unwrap_or("")
        .to_string();
    let message_ts = payload["message"]["ts"]
        .as_str()
        .map(|s| s.to_string());

    // Extract the action value from block_actions
    let actions = payload["actions"].as_array();
    let action_value = actions
        .and_then(|a| a.first())
        .and_then(|a| a["value"].as_str())
        .unwrap_or("")
        .to_string();

    let now = epoch_secs();
    let reply_target = ReplyTarget {
        channel: ChannelType::Slack,
        target_id: channel_id.clone(),
        thread_id: message_ts.clone(),
    };

    let message = InboundMessage {
        id: uuid::Uuid::new_v4().to_string(),
        context: MessageContext {
            channel: ChannelType::Slack,
            channel_id: channel_id.clone(),
            user_id: user_id.clone(),
            user_name,
            user_email: None,
            thread_id: message_ts,
            thread_type: Some(ThreadType::SlackThread),
            parent_message_id: None,
            project_path: None,
            project_id: None,
            reply_to: Some(reply_target),
            received_at: now,
            raw_payload: Some(payload.clone()),
        },
        action: InboundAction::RespondToEscalation,
        raw_content: action_value,
        parsed_at: now,
    };

    info!(
        user = %user_id,
        channel = %channel_id,
        "parsed Slack interaction (escalation response)"
    );

    bus.publish_inbound(message);
}

/// Strip Slack user mention tags like `<@U12345>` from text.
fn strip_slack_mention(text: &str) -> String {
    let mut result = text.to_string();
    while let Some(start) = result.find("<@") {
        if let Some(end) = result[start..].find('>') {
            result = format!("{}{}", &result[..start], &result[start + end + 1..]);
        } else {
            break;
        }
    }
    result.trim().to_string()
}

/// Parse a Slack message into an action + content, similar to GitLab command parsing.
fn parse_slack_command(text: &str) -> (InboundAction, String) {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return (InboundAction::Help, String::new());
    }

    // Check for explicit commands
    let lower = trimmed.to_lowercase();
    if let Some(rest) = lower.strip_prefix("directive ").or_else(|| lower.strip_prefix("create directive ")) {
        return (InboundAction::CreateDirective, trimmed[trimmed.len() - rest.len()..].to_string());
    }
    if let Some(rest) = lower.strip_prefix("workflow ").or_else(|| lower.strip_prefix("trigger ")) {
        return (InboundAction::TriggerWorkflow, trimmed[trimmed.len() - rest.len()..].to_string());
    }
    if lower.starts_with("review") {
        let rest = trimmed.get(6..).unwrap_or("").trim().to_string();
        return (InboundAction::RequestReview, rest);
    }
    if lower.starts_with("fix") {
        let rest = trimmed.get(3..).unwrap_or("").trim().to_string();
        return (InboundAction::RequestFix, rest);
    }
    if lower.starts_with("status") {
        return (InboundAction::QueryStatus, String::new());
    }
    if lower == "help" || lower == "?" {
        return (InboundAction::Help, String::new());
    }

    // Default: natural language
    (InboundAction::NaturalLanguage, trimmed.to_string())
}

// ---------------------------------------------------------------------------
// Signature verification
// ---------------------------------------------------------------------------

/// Verify the Slack request signature using HMAC-SHA256.
/// Returns true if the signature is valid, or if no signing secret is configured
/// (development mode).
fn verify_slack_signature(headers: &HeaderMap, body: &[u8], signing_secret: &str) -> bool {
    if signing_secret.is_empty() {
        // No secret configured — skip verification (dev mode)
        return true;
    }

    let timestamp = match headers.get("X-Slack-Request-Timestamp").and_then(|v| v.to_str().ok()) {
        Some(ts) => ts,
        None => {
            warn!("missing X-Slack-Request-Timestamp header");
            return false;
        }
    };

    let signature = match headers.get("X-Slack-Signature").and_then(|v| v.to_str().ok()) {
        Some(sig) => sig,
        None => {
            warn!("missing X-Slack-Signature header");
            return false;
        }
    };

    // Check timestamp freshness (within 5 minutes)
    if let Ok(ts) = timestamp.parse::<i64>() {
        let now = epoch_secs();
        if (now - ts).unsigned_abs() > 300 {
            warn!("Slack request timestamp too old");
            return false;
        }
    }

    // Compute expected signature: v0=HMAC-SHA256(signing_secret, "v0:{timestamp}:{body}")
    use std::io::Write;
    let sig_basestring = format!("v0:{}:", timestamp);
    let mut mac_input = Vec::with_capacity(sig_basestring.len() + body.len());
    mac_input.write_all(sig_basestring.as_bytes()).ok();
    mac_input.write_all(body).ok();

    // Use a simple constant-time comparison approach
    // We compute HMAC manually using two rounds of SHA-256 (HMAC construction)
    // For production, you'd use the `hmac` crate, but we avoid adding deps.
    // Instead, we do a basic hash comparison that's sufficient for webhook verification.
    let expected = format!("v0={}", hex_hmac_sha256(signing_secret.as_bytes(), &mac_input));

    // Constant-time comparison
    if expected.len() != signature.len() {
        return false;
    }
    let mut diff = 0u8;
    for (a, b) in expected.bytes().zip(signature.bytes()) {
        diff |= a ^ b;
    }
    diff == 0
}

/// Compute HMAC-SHA256 and return the hex-encoded digest.
fn hex_hmac_sha256(key: &[u8], message: &[u8]) -> String {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;

    type HmacSha256 = Hmac<Sha256>;

    let mut mac = HmacSha256::new_from_slice(key)
        .expect("HMAC accepts any key length");
    mac.update(message);
    let result = mac.finalize();
    let bytes = result.into_bytes();

    // Hex-encode the digest
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strip_mention() {
        assert_eq!(strip_slack_mention("<@U12345> hello"), "hello");
        assert_eq!(strip_slack_mention("hi <@U999> there"), "hi  there");
        assert_eq!(strip_slack_mention("no mention"), "no mention");
    }

    #[test]
    fn parse_commands() {
        let (action, _) = parse_slack_command("help");
        assert_eq!(action, InboundAction::Help);

        let (action, content) = parse_slack_command("directive watch for bugs");
        assert_eq!(action, InboundAction::CreateDirective);
        assert_eq!(content, "watch for bugs");

        let (action, _) = parse_slack_command("status");
        assert_eq!(action, InboundAction::QueryStatus);

        let (action, content) = parse_slack_command("what's going on with the deploy?");
        assert_eq!(action, InboundAction::NaturalLanguage);
        assert!(content.contains("deploy"));
    }

    #[test]
    fn empty_text_is_help() {
        let (action, _) = parse_slack_command("");
        assert_eq!(action, InboundAction::Help);
    }
}
