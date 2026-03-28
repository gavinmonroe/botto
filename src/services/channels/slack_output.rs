// ---------------------------------------------------------------------------
// Slack output adapter — subscribes to outbound bus, filters for Slack
// channel, and posts messages via the Slack Web API.
//
// Supports Block Kit formatting for escalation messages with action buttons.
// ---------------------------------------------------------------------------

use anyhow::{Context, Result};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use super::audit;
use super::bus::MessageBus;
use super::types::{ChannelType, OutboundMessage, OutboundType};
use crate::config::SlackChannelConfig;

/// Spawn a background task that listens for outbound messages destined for
/// Slack and posts them via the Slack Web API.
pub fn spawn_slack_output_listener(
    pool: sqlx::SqlitePool,
    bus: MessageBus,
    slack_config: SlackChannelConfig,
    shutdown: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        info!("Slack output listener started");
        let mut rx = bus.subscribe_outbound();

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    info!("Slack output listener shutting down");
                    break;
                }
                result = rx.recv() => {
                    match result {
                        Ok(message) => {
                            if message.reply_context.channel != ChannelType::Slack {
                                continue;
                            }
                            if let Err(e) = deliver_slack(&pool, &slack_config, &message).await {
                                error!(
                                    id = %message.id,
                                    "Slack delivery failed: {:#}", e
                                );
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            warn!("Slack output listener lagged, missed {} messages", n);
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            info!("Slack output listener: outbound bus closed");
                            break;
                        }
                    }
                }
            }
        }
    })
}

async fn deliver_slack(
    pool: &sqlx::SqlitePool,
    slack_config: &SlackChannelConfig,
    message: &OutboundMessage,
) -> Result<()> {
    if slack_config.bot_token.is_empty() {
        warn!(id = %message.id, "Slack bot token not configured, skipping delivery");
        return Ok(());
    }

    let channel_id = &message.reply_context.target_id;
    let thread_ts = message.reply_context.thread_id.as_deref();

    let body = build_slack_payload(channel_id, thread_ts, message);

    let client = reqwest::Client::new();
    let resp = client
        .post("https://slack.com/api/chat.postMessage")
        .header("Authorization", format!("Bearer {}", slack_config.bot_token))
        .header("Content-Type", "application/json; charset=utf-8")
        .json(&body)
        .send()
        .await
        .context("send Slack message")?;

    let status = resp.status();
    if !status.is_success() {
        let text = resp.text().await.unwrap_or_default();
        warn!(
            id = %message.id,
            status = %status,
            body = %text,
            "Slack API returned non-200"
        );
        anyhow::bail!("Slack API error: {} {}", status, text);
    }

    let resp_json: serde_json::Value = resp.json().await.context("parse Slack response")?;
    if resp_json["ok"].as_bool() != Some(true) {
        let error = resp_json["error"].as_str().unwrap_or("unknown");
        warn!(id = %message.id, error = %error, "Slack API returned ok=false");
        anyhow::bail!("Slack API error: {}", error);
    }

    // Audit log (best-effort)
    if let Err(e) = audit::log_outbound(pool, message).await {
        warn!(id = %message.id, "audit log failed: {:#}", e);
    }

    info!(
        id = %message.id,
        channel = %channel_id,
        msg_type = %message.message_type.type_name(),
        "delivered to Slack"
    );
    Ok(())
}

/// Build the Slack API payload. Uses Block Kit for escalation messages
/// (with action buttons), plain text for everything else.
fn build_slack_payload(
    channel: &str,
    thread_ts: Option<&str>,
    message: &OutboundMessage,
) -> serde_json::Value {
    let mut payload = serde_json::json!({
        "channel": channel,
        "text": &message.content,
    });

    if let Some(ts) = thread_ts {
        payload["thread_ts"] = serde_json::json!(ts);
    }

    // Use Block Kit for escalation messages with interactive buttons
    if message.message_type == OutboundType::Escalation {
        let mut blocks = vec![
            serde_json::json!({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": format!(":rotating_light: *Human Input Needed*\n\n{}", message.content)
                }
            }),
        ];

        // Add action buttons if structured_data contains options
        if let Some(ref data) = message.structured_data {
            if let Some(options) = data["options"].as_array() {
                let buttons: Vec<serde_json::Value> = options
                    .iter()
                    .filter_map(|opt| {
                        let id = opt["id"].as_str()?;
                        let label = opt["label"].as_str()?;
                        let style = if id == "cancel" {
                            Some("danger")
                        } else {
                            Some("primary")
                        };
                        let mut btn = serde_json::json!({
                            "type": "button",
                            "text": {
                                "type": "plain_text",
                                "text": label,
                            },
                            "value": id,
                            "action_id": format!("escalation_{}", id),
                        });
                        if let Some(s) = style {
                            btn["style"] = serde_json::json!(s);
                        }
                        Some(btn)
                    })
                    .collect();

                if !buttons.is_empty() {
                    blocks.push(serde_json::json!({
                        "type": "actions",
                        "elements": buttons,
                    }));
                }
            }
        }

        payload["blocks"] = serde_json::json!(blocks);
    }

    payload
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::channels::types::ReplyTarget;

    #[test]
    fn build_simple_payload() {
        let msg = OutboundMessage {
            id: "test".into(),
            reply_context: ReplyTarget {
                channel: ChannelType::Slack,
                target_id: "C12345".into(),
                thread_id: Some("1234.5678".into()),
            },
            message_type: OutboundType::Completion,
            content: "Done!".into(),
            structured_data: None,
            session_id: None,
            directive_id: None,
            created_at: 0,
        };

        let payload = build_slack_payload("C12345", Some("1234.5678"), &msg);
        assert_eq!(payload["channel"], "C12345");
        assert_eq!(payload["thread_ts"], "1234.5678");
        assert_eq!(payload["text"], "Done!");
        assert!(payload.get("blocks").is_none());
    }

    #[test]
    fn build_escalation_payload_with_buttons() {
        let msg = OutboundMessage {
            id: "test".into(),
            reply_context: ReplyTarget {
                channel: ChannelType::Slack,
                target_id: "C12345".into(),
                thread_id: None,
            },
            message_type: OutboundType::Escalation,
            content: "Need help".into(),
            structured_data: Some(serde_json::json!({
                "options": [
                    { "id": "retry", "label": "Retry" },
                    { "id": "cancel", "label": "Cancel" },
                ]
            })),
            session_id: None,
            directive_id: None,
            created_at: 0,
        };

        let payload = build_slack_payload("C12345", None, &msg);
        let blocks = payload["blocks"].as_array().unwrap();
        assert_eq!(blocks.len(), 2); // section + actions
        let actions = blocks[1]["elements"].as_array().unwrap();
        assert_eq!(actions.len(), 2);
        assert_eq!(actions[1]["style"], "danger"); // cancel button
    }
}
