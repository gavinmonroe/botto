// ---------------------------------------------------------------------------
// EventBus → MessageBus bridge — converts session/escalation events from the
// internal EventBus into OutboundMessages on the MessageBus so that channel
// output adapters (GitLab, Slack) can deliver results back to users.
//
// Without this bridge, long-running workflow results stay trapped in the
// EventBus and never reach the originating channel thread.
// ---------------------------------------------------------------------------

use anyhow::Context;
use sqlx::SqlitePool;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use super::bus::MessageBus;
use super::types::{OutboundMessage, OutboundType, ReplyTarget};
use crate::services::events::{Event, EventBus, EventType};
use crate::services::workflow::crud;

/// Spawn the event bridge as a background task. Subscribes to the EventBus
/// and publishes corresponding OutboundMessages to the MessageBus.
pub fn spawn_event_bridge(
    pool: SqlitePool,
    event_bus: EventBus,
    message_bus: MessageBus,
    cancel: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        info!("event bridge started");
        let mut rx = event_bus.subscribe();

        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    info!("event bridge shutting down");
                    break;
                }
                result = rx.recv() => {
                    match result {
                        Ok(event) => {
                            if let Err(e) = handle_event(&pool, &message_bus, event).await {
                                warn!("event bridge: failed to handle event: {e:#}");
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            warn!("event bridge lagged, missed {n} events");
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            info!("event bridge: event bus closed");
                            break;
                        }
                    }
                }
            }
        }
    })
}

async fn handle_event(
    pool: &SqlitePool,
    message_bus: &MessageBus,
    event: Event,
) -> anyhow::Result<()> {
    match event.event_type {
        EventType::WorkflowRunCompleted => {
            handle_workflow_completed(pool, message_bus, &event).await
        }
        EventType::WorkflowStepCompleted => {
            handle_step_progress(pool, message_bus, &event, "step completed").await
        }
        EventType::WorkflowStepFailed => {
            handle_step_progress(pool, message_bus, &event, "step failed").await
        }
        EventType::SessionEscalation => {
            handle_escalation(pool, message_bus, &event).await
        }
        EventType::SessionResumed => {
            handle_session_resumed(pool, message_bus, &event).await
        }
        EventType::DirectiveEscalation => {
            handle_directive_escalation(pool, message_bus, &event).await
        }
        // Ignore other event types — they don't need channel delivery.
        _ => Ok(()),
    }
}

/// Extract the reply_context from a session's trigger_data.
/// Returns None if the session has no channel context (e.g. admin-created).
fn extract_reply_context(trigger_data: &serde_json::Value) -> Option<ReplyTarget> {
    let reply_context = trigger_data.get("reply_context")?;
    serde_json::from_value(reply_context.clone())
        .map_err(|e| {
            debug!("failed to parse reply_context from trigger_data: {e}");
            e
        })
        .ok()
}

/// Load a session and extract its reply_context. Returns None if the session
/// doesn't exist or has no channel context.
async fn load_reply_context(
    pool: &SqlitePool,
    session_id: &str,
) -> anyhow::Result<Option<ReplyTarget>> {
    let session = crud::load_session(pool, session_id)
        .await
        .context("load session for bridge")?;

    let session = match session {
        Some(s) => s,
        None => {
            debug!(session_id, "bridge: session not found");
            return Ok(None);
        }
    };

    let trigger_data = match session.trigger_data {
        Some(ref td) => td,
        None => return Ok(None),
    };

    Ok(extract_reply_context(trigger_data))
}

/// Extract session_id from an event payload.
fn get_session_id(event: &Event) -> Option<&str> {
    event
        .payload
        .as_ref()?
        .get("session_id")?
        .as_str()
}

// ---------------------------------------------------------------------------
// Event handlers
// ---------------------------------------------------------------------------

async fn handle_workflow_completed(
    pool: &SqlitePool,
    message_bus: &MessageBus,
    event: &Event,
) -> anyhow::Result<()> {
    let session_id = match get_session_id(event) {
        Some(id) => id,
        None => return Ok(()),
    };

    let reply_target = match load_reply_context(pool, session_id).await? {
        Some(rt) => rt,
        None => {
            debug!(session_id, "bridge: no reply_context for completed session, skipping");
            return Ok(());
        }
    };

    let status = event
        .payload
        .as_ref()
        .and_then(|p| p.get("status"))
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");

    let content = match status {
        "completed" => format!("Workflow session `{session_id}` completed successfully."),
        "failed" => format!("Workflow session `{session_id}` failed."),
        "cancelled" => format!("Workflow session `{session_id}` was cancelled."),
        other => format!("Workflow session `{session_id}` finished with status: {other}"),
    };

    let msg_type = if status == "completed" {
        OutboundType::Completion
    } else {
        OutboundType::Error
    };

    let mut msg = OutboundMessage::new_with_type(reply_target, msg_type, content);
    msg.session_id = Some(session_id.to_string());
    message_bus.publish_outbound(msg);

    info!(session_id, status, "bridge: published workflow completion to channel");
    Ok(())
}

async fn handle_step_progress(
    pool: &SqlitePool,
    message_bus: &MessageBus,
    event: &Event,
    label: &str,
) -> anyhow::Result<()> {
    let session_id = match get_session_id(event) {
        Some(id) => id,
        None => return Ok(()),
    };

    let reply_target = match load_reply_context(pool, session_id).await? {
        Some(rt) => rt,
        None => return Ok(()),
    };

    let step_id = event
        .payload
        .as_ref()
        .and_then(|p| p.get("current_step_id"))
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");

    let content = format!("Session `{session_id}`: {label} (step: {step_id})");
    let mut msg = OutboundMessage::progress(reply_target, content);
    msg.session_id = Some(session_id.to_string());
    message_bus.publish_outbound(msg);

    debug!(session_id, step_id, label, "bridge: published step progress to channel");
    Ok(())
}

async fn handle_escalation(
    pool: &SqlitePool,
    message_bus: &MessageBus,
    event: &Event,
) -> anyhow::Result<()> {
    let payload = match event.payload.as_ref() {
        Some(p) => p,
        None => return Ok(()),
    };

    let session_id = payload
        .get("session_id")
        .and_then(|v| v.as_str())
        .unwrap_or_default();

    if session_id.is_empty() {
        return Ok(());
    }

    let reply_target = match load_reply_context(pool, session_id).await? {
        Some(rt) => rt,
        None => {
            debug!(session_id, "bridge: no reply_context for escalation, skipping");
            return Ok(());
        }
    };

    let reason = payload
        .get("reason")
        .and_then(|v| v.as_str())
        .unwrap_or("Human input needed");
    let what_i_need = payload
        .get("what_i_need")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    let content = if what_i_need.is_empty() {
        format!("Session `{session_id}` needs your input:\n\n{reason}")
    } else {
        format!(
            "Session `{session_id}` needs your input:\n\n{reason}\n\n**What I need:** {what_i_need}"
        )
    };

    let mut msg = OutboundMessage::new_with_type(
        reply_target,
        OutboundType::Escalation,
        content,
    );
    msg.session_id = Some(session_id.to_string());

    // Attach escalation options as structured data for Slack buttons etc.
    if let Some(options) = payload.get("options") {
        msg.structured_data = Some(serde_json::json!({ "options": options }));
    }

    message_bus.publish_outbound(msg);

    info!(session_id, "bridge: published escalation to channel");
    Ok(())
}

async fn handle_session_resumed(
    pool: &SqlitePool,
    message_bus: &MessageBus,
    event: &Event,
) -> anyhow::Result<()> {
    let session_id = match get_session_id(event) {
        Some(id) => id,
        None => return Ok(()),
    };

    let reply_target = match load_reply_context(pool, session_id).await? {
        Some(rt) => rt,
        None => return Ok(()),
    };

    let new_status = event
        .payload
        .as_ref()
        .and_then(|p| p.get("new_status"))
        .and_then(|v| v.as_str())
        .unwrap_or("resumed");

    let content = format!("Session `{session_id}` resumed — now {new_status}.");
    let mut msg = OutboundMessage::progress(reply_target, content);
    msg.session_id = Some(session_id.to_string());
    message_bus.publish_outbound(msg);

    debug!(session_id, new_status, "bridge: published session resumed to channel");
    Ok(())
}

async fn handle_directive_escalation(
    _pool: &SqlitePool,
    message_bus: &MessageBus,
    event: &Event,
) -> anyhow::Result<()> {
    let payload = match event.payload.as_ref() {
        Some(p) => p,
        None => return Ok(()),
    };

    // Directive escalations carry reply_context directly in the event payload
    // (since directives aren't sessions).
    let reply_target = match payload.get("reply_context") {
        Some(rc) => match serde_json::from_value::<ReplyTarget>(rc.clone()) {
            Ok(rt) => rt,
            Err(_) => return Ok(()),
        },
        None => return Ok(()),
    };

    let directive_id = payload
        .get("directive_id")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");
    let reason = payload
        .get("reason")
        .and_then(|v| v.as_str())
        .unwrap_or("Directive needs attention");

    let content = format!(
        "Directive `{directive_id}` escalated:\n\n{reason}"
    );

    let mut msg = OutboundMessage::new_with_type(
        reply_target,
        OutboundType::Escalation,
        content,
    );
    msg.directive_id = Some(directive_id.to_string());
    message_bus.publish_outbound(msg);

    info!(directive_id, "bridge: published directive escalation to channel");
    Ok(())
}
