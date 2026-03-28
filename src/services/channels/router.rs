// ---------------------------------------------------------------------------
// Channel router — subscribes to inbound bus, checks permissions + rate
// limits, dispatches to core actions, and publishes outbound responses.
//
// Runs as a background task spawned from main.rs. Each inbound message is
// processed sequentially (single consumer) to keep ordering simple. Heavy
// work (AI calls, workflow triggers) is spawned as separate tasks.
// ---------------------------------------------------------------------------

use anyhow::{Context, Result};
use sqlx::SqlitePool;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use super::audit;
use super::bus::MessageBus;
use super::config::{check_permission, check_rate_limit};
use super::types::{
    InboundAction, InboundMessage, OutboundMessage, ReplyTarget,
};
use crate::config::{BottoConfig, ChannelConfig};
use crate::services::ai::client::{
    self as ai_client, AiClientConfig, ChatCompletionRequest, ChatMessage,
};
use crate::services::events::EventBus;
use crate::services::mentor::client::MentorClient;
use crate::services::queue::manager::QueueManager;
use crate::services::workflow::crud;
use crate::services::workflow::factory::AgentFactoryConfig;
use crate::services::workflow::session::{SessionManager, SessionManagerConfig};
use crate::types::workflow::{SessionState, SessionStatus, WorkflowDefinition, WorkflowMode};

/// Spawn the inbound message router as a background task.
/// Returns a JoinHandle that runs until the cancellation token fires.
pub fn spawn_router(
    pool: SqlitePool,
    bus: MessageBus,
    channel_config: ChannelConfig,
    botto_config: BottoConfig,
    event_bus: EventBus,
    queue_manager: Option<Arc<QueueManager>>,
    shutdown: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        info!("channel router started");
        let mut rx = bus.subscribe_inbound();

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    info!("channel router shutting down");
                    break;
                }
                result = rx.recv() => {
                    match result {
                        Ok(message) => {
                            if let Err(e) = handle_inbound(
                                &pool,
                                &bus,
                                &channel_config,
                                &botto_config,
                                &event_bus,
                                queue_manager.as_ref(),
                                message,
                            ).await {
                                error!("channel router error: {:#}", e);
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            warn!("channel router lagged, missed {} messages", n);
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            info!("channel router: inbound bus closed");
                            break;
                        }
                    }
                }
            }
        }
    })
}

async fn handle_inbound(
    pool: &SqlitePool,
    bus: &MessageBus,
    channel_config: &ChannelConfig,
    botto_config: &BottoConfig,
    event_bus: &EventBus,
    queue_manager: Option<&Arc<QueueManager>>,
    message: InboundMessage,
) -> Result<()> {
    let msg_id = &message.id;
    let channel = &message.context.channel;
    let user_id = &message.context.user_id;
    let action = &message.action;
    let is_reclassified = msg_id.contains("-reclassified");

    debug!(
        id = %msg_id,
        channel = %channel,
        user = %user_id,
        action = %action.action_name(),
        reclassified = is_reclassified,
        "routing inbound message"
    );

    // Audit log (best-effort — don't fail the message on audit errors)
    if let Err(e) = audit::log_inbound(pool, &message).await {
        warn!(id = %msg_id, "audit log failed: {:#}", e);
    }

    // Build reply target from the inbound context
    let reply_target = message
        .context
        .reply_to
        .clone()
        .unwrap_or_else(|| ReplyTarget {
            channel: channel.clone(),
            target_id: message.context.channel_id.clone(),
            thread_id: message.context.thread_id.clone(),
        });

    // Permission check
    let perm = check_permission(channel_config, channel, user_id);
    if !perm.allowed {
        let reason = perm.reason.unwrap_or_else(|| "permission denied".into());
        info!(id = %msg_id, reason = %reason, "message rejected by permission check");
        let reply = OutboundMessage::error(reply_target, format!("Permission denied: {}", reason));
        bus.publish_outbound(reply);
        return Ok(());
    }

    // Rate limit check — skip for reclassified messages (already counted on first pass)
    if !is_reclassified {
        match check_rate_limit(pool, channel_config, channel, user_id).await {
            Ok(true) => {}
            Ok(false) => {
                info!(id = %msg_id, user = %user_id, "message rate-limited");
                let reply = OutboundMessage::error(
                    reply_target,
                    "You're sending messages too quickly. Please wait a moment and try again.",
                );
                bus.publish_outbound(reply);
                return Ok(());
            }
            Err(e) => {
                warn!(id = %msg_id, "rate limit check failed, allowing: {:#}", e);
                // Fail open — don't block messages on rate limit DB errors
            }
        }
    }

    // Route to action handler
    match action {
        InboundAction::Help => {
            let reply = OutboundMessage::help(reply_target);
            bus.publish_outbound(reply);
        }

        InboundAction::QueryStatus => {
            let reply = OutboundMessage::status_report(
                reply_target,
                "Botto is running. Use the admin UI for detailed status.",
                serde_json::json!({ "status": "ok" }),
            );
            bus.publish_outbound(reply);
        }

        InboundAction::CreateDirective => {
            let ack = OutboundMessage::acknowledgment(
                reply_target.clone(),
                format!("Creating directive from: {}", truncate(&message.raw_content, 80)),
            );
            bus.publish_outbound(ack);

            // Directive creation is handled asynchronously — the AI parsing
            // can take several seconds. We spawn it so the router stays responsive.
            let pool = pool.clone();
            let bus = bus.clone();
            let content = message.raw_content.clone();
            let user = message.context.user_name.clone();
            let reply_target = reply_target.clone();
            let directive_reply_ctx = message.context.reply_to.clone();

            tokio::spawn(async move {
                match route_create_directive(&pool, &content, &user, directive_reply_ctx).await {
                    Ok(directive_id) => {
                        let reply = OutboundMessage::completion(
                            reply_target,
                            format!("Directive created: {}", directive_id),
                        );
                        bus.publish_outbound(reply);
                    }
                    Err(e) => {
                        let reply = OutboundMessage::error(
                            reply_target,
                            format!("Failed to create directive: {}", e),
                        );
                        bus.publish_outbound(reply);
                    }
                }
            });
        }

        InboundAction::TriggerWorkflow => {
            let ack = OutboundMessage::acknowledgment(
                reply_target.clone(),
                "Looking up workflow to trigger...",
            );
            bus.publish_outbound(ack);

            let pool = pool.clone();
            let bus = bus.clone();
            let botto_config = botto_config.clone();
            let event_bus = event_bus.clone();
            let message = message.clone();
            let reply_target = reply_target.clone();

            tokio::spawn(async move {
                match route_trigger_workflow(
                    &pool,
                    &botto_config,
                    &event_bus,
                    &message,
                )
                .await
                {
                    Ok(session_id) => {
                        let reply = OutboundMessage::completion(
                            reply_target,
                            format!("Workflow session started: {session_id}"),
                        );
                        bus.publish_outbound(reply);
                    }
                    Err(e) => {
                        warn!(error = %e, "route_trigger_workflow failed");
                        let reply = OutboundMessage::error(
                            reply_target,
                            format!("Failed to trigger workflow: {e}"),
                        );
                        bus.publish_outbound(reply);
                    }
                }
            });
        }

        InboundAction::RequestReview => {
            let ack = OutboundMessage::acknowledgment(
                reply_target.clone(),
                "Review request received. Queuing...",
            );
            bus.publish_outbound(ack);

            let pool = pool.clone();
            let bus = bus.clone();
            let botto_config = botto_config.clone();
            let queue_manager = queue_manager.cloned();
            let message = message.clone();
            let reply_target = reply_target.clone();

            tokio::spawn(async move {
                match route_request_review(
                    &pool,
                    &botto_config,
                    queue_manager.as_ref(),
                    &message,
                )
                .await
                {
                    Ok(msg) => {
                        let reply = OutboundMessage::completion(reply_target, msg);
                        bus.publish_outbound(reply);
                    }
                    Err(e) => {
                        warn!(error = %e, "route_request_review failed");
                        let reply = OutboundMessage::error(
                            reply_target,
                            format!("Failed to queue review: {e}"),
                        );
                        bus.publish_outbound(reply);
                    }
                }
            });
        }

        InboundAction::RequestFix => {
            let ack = OutboundMessage::acknowledgment(
                reply_target.clone(),
                "Fix request received. Spinning up a coding session...",
            );
            bus.publish_outbound(ack);

            let pool = pool.clone();
            let bus = bus.clone();
            let botto_config = botto_config.clone();
            let event_bus = event_bus.clone();
            let message = message.clone();
            let reply_target = reply_target.clone();

            tokio::spawn(async move {
                match route_request_fix(
                    &pool,
                    &botto_config,
                    &event_bus,
                    &message,
                )
                .await
                {
                    Ok(session_id) => {
                        let reply = OutboundMessage::completion(
                            reply_target,
                            format!("Fix session started: {session_id}"),
                        );
                        bus.publish_outbound(reply);
                    }
                    Err(e) => {
                        warn!(error = %e, "route_request_fix failed");
                        let reply = OutboundMessage::error(
                            reply_target,
                            format!("Failed to start fix session: {e}"),
                        );
                        bus.publish_outbound(reply);
                    }
                }
            });
        }

        InboundAction::RespondToEscalation => {
            let ack = OutboundMessage::acknowledgment(
                reply_target.clone(),
                "Processing your escalation response...",
            );
            bus.publish_outbound(ack);

            let pool = pool.clone();
            let bus = bus.clone();
            let event_bus = event_bus.clone();
            let message = message.clone();
            let reply_target = reply_target.clone();

            tokio::spawn(async move {
                match route_respond_to_escalation(
                    &pool,
                    &event_bus,
                    &message,
                )
                .await
                {
                    Ok(new_status) => {
                        let reply = OutboundMessage::completion(
                            reply_target,
                            format!("Escalation response processed. Session is now: {}", new_status.as_str()),
                        );
                        bus.publish_outbound(reply);
                    }
                    Err(e) => {
                        warn!(error = %e, "route_respond_to_escalation failed");
                        let reply = OutboundMessage::error(
                            reply_target,
                            format!("Failed to process escalation response: {e}"),
                        );
                        bus.publish_outbound(reply);
                    }
                }
            });
        }

        InboundAction::NaturalLanguage => {
            // Guard: don't re-classify a message that was already reclassified
            // to prevent infinite NL -> classify -> NL loops.
            if is_reclassified {
                warn!(id = %msg_id, "reclassified message mapped back to NaturalLanguage, sending help");
                let reply = OutboundMessage::completion(
                    reply_target,
                    "I'm not sure what you're asking for. Try `/botto help` to see available commands.",
                );
                bus.publish_outbound(reply);
                return Ok(());
            }

            let ack = OutboundMessage::acknowledgment(
                reply_target.clone(),
                "I received your message. Let me figure out what you need...",
            );
            bus.publish_outbound(ack);

            let pool = pool.clone();
            let bus = bus.clone();
            let botto_config = botto_config.clone();
            let event_bus = event_bus.clone();
            let message = message.clone();
            let reply_target = reply_target.clone();

            tokio::spawn(async move {
                match route_natural_language(
                    &pool,
                    &botto_config,
                    &event_bus,
                    &bus,
                    &message,
                    &reply_target,
                )
                .await
                {
                    Ok(()) => {}
                    Err(e) => {
                        warn!(error = %e, "route_natural_language failed");
                        let reply = OutboundMessage::error(
                            reply_target,
                            format!("Sorry, I couldn't process that: {e}"),
                        );
                        bus.publish_outbound(reply);
                    }
                }
            });
        }
    }

    Ok(())
}

/// Create a directive from natural language input.
/// This is a simplified version — full implementation would use the AI parser.
async fn route_create_directive(
    pool: &SqlitePool,
    content: &str,
    created_by: &str,
    reply_context: Option<ReplyTarget>,
) -> Result<String> {
    use crate::services::directive::types::{
        Directive, DirectiveConstraints, DirectiveStatus, WorkSource,
    };
    use crate::services::workflow::crud::epoch_secs;

    let now = epoch_secs();
    let id = uuid::Uuid::new_v4().to_string();

    let directive = Directive {
        id: id.clone(),
        name: format!("channel-{}", &id[..8]),
        intent: content.to_string(),
        sources: vec![WorkSource::Inferred {
            category: "connector".into(),
            filter: None,
        }],
        constraints: DirectiveConstraints::default(),
        priority: 5,
        status: DirectiveStatus::Active,
        poll_interval_secs: 300,
        last_poll_at: None,
        next_poll_at: Some(now + 300),
        escalation: None,
        created_by: Some(created_by.to_string()),
        reply_context: reply_context,
        created_at: now,
        updated_at: now,
    };

    crate::services::directive::crud::create_directive(pool, &directive)
        .await
        .context("create directive from channel")?;

    Ok(id)
}

/// Build a workflow session for a fix request and spawn the session manager.
///
/// 1. Creates a WorkflowDefinition describing the fix task
/// 2. Persists a new SessionState
/// 3. Spawns a SessionManager to drive it (planner will create a coding step)
async fn route_request_fix(
    pool: &SqlitePool,
    botto_config: &BottoConfig,
    event_bus: &EventBus,
    message: &InboundMessage,
) -> Result<String> {
    let now = crud::epoch_secs();
    let workflow_id = uuid::Uuid::new_v4();
    let session_id = uuid::Uuid::new_v4();

    let project_path = message
        .context
        .project_path
        .clone()
        .unwrap_or_else(|| "unknown".into());
    let project_id = message.context.project_id.unwrap_or(0);

    // Build a lightweight workflow definition for this fix request.
    let workflow = WorkflowDefinition {
        id: workflow_id,
        name: format!("channel-fix-{}", &session_id.to_string()[..8]),
        description: format!(
            "Fix requested via channel: {}",
            truncate(&message.raw_content, 200)
        ),
        project_id,
        steps: vec![],  // Planner will generate steps
        triggers: vec![],
        created_by: message.context.user_name.clone(),
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
        enabled: true,
        mode: WorkflowMode::Autonomous,
    };

    crud::create_workflow(pool, &workflow)
        .await
        .context("create fix workflow definition")?;

    // Build trigger data with all available context for the planner.
    let reply_context_json = message
        .context
        .reply_to
        .as_ref()
        .and_then(|rt| serde_json::to_value(rt).ok());

    let trigger_data = serde_json::json!({
        "type": "fix_request",
        "description": message.raw_content,
        "project_path": project_path,
        "project_id": project_id,
        "requested_by": message.context.user_name,
        "channel": message.context.channel.as_str(),
        "reply_context": reply_context_json,
    });

    let session = SessionState {
        id: session_id,
        workflow_id,
        status: SessionStatus::Created,
        trigger_type: "channel_fix".into(),
        trigger_data: Some(trigger_data),
        plan: None,
        step_outputs: std::collections::HashMap::new(),
        current_step_id: None,
        retry_count: 0,
        max_retries: 10,
        step_retry_count: 0,
        evaluator_feedback: None,
        escalation: None,
        pending_modification: None,
        started_at: now,
        completed_at: None,
        updated_at: now,
    };

    crud::create_session(pool, &session)
        .await
        .context("create fix session")?;

    info!(
        session_id = %session_id,
        workflow_id = %workflow_id,
        project = %project_path,
        "fix session created from channel request"
    );

    // Spawn the session manager to drive the session asynchronously.
    let pool = pool.clone();
    let botto_config = botto_config.clone();
    let event_bus = event_bus.clone();
    let workflow_name = workflow.name.clone();

    tokio::spawn(async move {
        let ai_config = AiClientConfig {
            base_url: botto_config.ai.base_url.clone(),
            api_key: botto_config.ai.api_key.clone(),
        };
        let agent_config = AgentFactoryConfig {
            gitlab: Some(crate::services::gitlab::client::GitLabConfig {
                base_url: botto_config.gitlab.url.clone(),
                token: botto_config.gitlab.bot_token.clone(),
            }),
            ai: Some(AiClientConfig {
                base_url: botto_config.ai.base_url.clone(),
                api_key: botto_config.ai.api_key.clone(),
            }),
            ai_default_model: botto_config.ai.models.workflow_orchestrate.clone(),
            sandbox_max_memory_mb: botto_config.sandbox.max_memory_mb,
            pool: pool.clone(),
            botto_config: Some(botto_config.clone()),
            event_bus: Some(event_bus.clone()),
        };
        let mentor = MentorClient::new(pool.clone(), "global".into());
        let sm_config = SessionManagerConfig {
            ai_model: botto_config.ai.models.workflow_orchestrate.clone(),
            ..Default::default()
        };

        let sm = SessionManager::new(
            pool.clone(),
            ai_config,
            agent_config,
            mentor,
            event_bus,
            sm_config,
        );

        let mut session = match crud::load_session(&pool, &session_id.to_string()).await {
            Ok(Some(s)) => s,
            Ok(None) => {
                error!(session_id = %session_id, "fix session vanished before driving");
                return;
            }
            Err(e) => {
                error!(session_id = %session_id, error = %e, "failed to reload fix session");
                return;
            }
        };

        if let Err(e) = sm.drive(&mut session, &workflow_name).await {
            error!(
                session_id = %session_id,
                error = %e,
                "fix session drive failed"
            );
        }
    });

    Ok(session_id.to_string())
}

/// Interpret a natural language message by asking the AI to classify it,
/// then re-dispatch as the appropriate typed action.
async fn route_natural_language(
    _pool: &SqlitePool,
    botto_config: &BottoConfig,
    _event_bus: &EventBus,
    bus: &MessageBus,
    message: &InboundMessage,
    reply_target: &ReplyTarget,
) -> Result<()> {
    let ai_config = AiClientConfig {
        base_url: botto_config.ai.base_url.clone(),
        api_key: botto_config.ai.api_key.clone(),
    };

    let system_prompt = r#"You are a message classifier for a DevOps bot called Botto.
Given a user message, determine which action it maps to. Respond with ONLY one of these action names (no explanation):

- create_directive: User wants to create a standing directive or rule
- trigger_workflow: User wants to trigger or run a workflow
- request_review: User wants a code review
- request_fix: User wants a bug fix or code change
- query_status: User wants to know the current status
- help: User wants help or information about available commands
- unknown: Message doesn't clearly map to any action

Respond with just the action name, nothing else."#;

    let user_prompt = format!(
        "Classify this user message:\n\n{}",
        truncate(&message.raw_content, 500)
    );

    let request = ChatCompletionRequest {
        model: botto_config.ai.models.workflow_orchestrate.clone(),
        messages: vec![
            ChatMessage {
                role: "system".into(),
                content: Some(system_prompt.to_string()),
                tool_calls: None,
                tool_call_id: None,
            },
            ChatMessage {
                role: "user".into(),
                content: Some(user_prompt),
                tool_calls: None,
                tool_call_id: None,
            },
        ],
        temperature: Some(0.0),
        max_tokens: Some(32),
        stream: None,
        tools: None,
        tool_choice: None,
    };

    let response = ai_client::chat_completion(&ai_config, request)
        .await
        .context("NL classification AI call failed")?;

    let classification = response
        .choices
        .first()
        .and_then(|c| c.message.content.as_deref())
        .unwrap_or("unknown")
        .trim()
        .to_lowercase();

    debug!(
        classification = classification.as_str(),
        raw = truncate(&message.raw_content, 80).as_str(),
        "natural language classified"
    );

    let mapped_action = match classification.as_str() {
        "create_directive" => Some(InboundAction::CreateDirective),
        "trigger_workflow" => Some(InboundAction::TriggerWorkflow),
        "request_review" => Some(InboundAction::RequestReview),
        "request_fix" => Some(InboundAction::RequestFix),
        "query_status" => Some(InboundAction::QueryStatus),
        "help" => Some(InboundAction::Help),
        _ => None,
    };

    match mapped_action {
        Some(action) => {
            info!(
                original = "natural_language",
                mapped = action.action_name(),
                "re-dispatching classified message"
            );

            // Re-publish as a new inbound message with the classified action.
            let reclassified = InboundMessage {
                id: format!("{}-reclassified", message.id),
                context: message.context.clone(),
                action,
                raw_content: message.raw_content.clone(),
                parsed_at: crud::epoch_secs(),
            };
            bus.publish_inbound(reclassified);
        }
        None => {
            debug!("natural language classified as unknown, sending help");
            let reply = OutboundMessage::completion(
                reply_target.clone(),
                "I'm not sure what you're asking for. Try `/botto help` to see available commands.",
            );
            bus.publish_outbound(reply);
        }
    }

    Ok(())
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        let mut end = max;
        while end > 0 && !s.is_char_boundary(end) {
            end -= 1;
        }
        format!("{}...", &s[..end])
    }
}

/// Look up a workflow by ID or name and spawn a session to execute it.
async fn route_trigger_workflow(
    pool: &SqlitePool,
    botto_config: &BottoConfig,
    event_bus: &EventBus,
    message: &InboundMessage,
) -> Result<String> {
    let input = message.raw_content.trim();
    let now = crud::epoch_secs();

    // Try to find the workflow — first by exact ID, then by name match.
    let workflow = if let Ok(Some(wf)) = crud::get_workflow(pool, input).await {
        wf
    } else {
        // Search enabled workflows for a name match
        let workflows = crud::list_enabled_workflows(pool)
            .await
            .context("list workflows for trigger")?;
        let input_lower = input.to_lowercase();
        workflows
            .into_iter()
            .find(|wf| wf.name.to_lowercase().contains(&input_lower))
            .ok_or_else(|| anyhow::anyhow!(
                "no workflow found matching '{}'. Use the admin UI to see available workflows.",
                truncate(input, 80)
            ))?
    };

    if !workflow.enabled {
        anyhow::bail!("workflow '{}' is disabled", workflow.name);
    }

    let session_id = uuid::Uuid::new_v4();
    let reply_context_json = message
        .context
        .reply_to
        .as_ref()
        .and_then(|rt| serde_json::to_value(rt).ok());

    let trigger_data = serde_json::json!({
        "type": "channel_trigger",
        "input": message.raw_content,
        "requested_by": message.context.user_name,
        "channel": message.context.channel.as_str(),
        "reply_context": reply_context_json,
    });

    let session = SessionState {
        id: session_id,
        workflow_id: workflow.id,
        status: SessionStatus::Created,
        trigger_type: "channel_trigger".into(),
        trigger_data: Some(trigger_data),
        plan: None,
        step_outputs: std::collections::HashMap::new(),
        current_step_id: None,
        retry_count: 0,
        max_retries: 10,
        step_retry_count: 0,
        evaluator_feedback: None,
        escalation: None,
        pending_modification: None,
        started_at: now,
        completed_at: None,
        updated_at: now,
    };

    crud::create_session(pool, &session)
        .await
        .context("create workflow trigger session")?;

    info!(
        session_id = %session_id,
        workflow_id = %workflow.id,
        workflow_name = %workflow.name,
        "workflow session created from channel trigger"
    );

    // Spawn the session manager to drive the workflow.
    let pool = pool.clone();
    let botto_config = botto_config.clone();
    let event_bus = event_bus.clone();
    let workflow_name = workflow.name.clone();

    tokio::spawn(async move {
        let ai_config = AiClientConfig {
            base_url: botto_config.ai.base_url.clone(),
            api_key: botto_config.ai.api_key.clone(),
        };
        let agent_config = AgentFactoryConfig {
            gitlab: Some(crate::services::gitlab::client::GitLabConfig {
                base_url: botto_config.gitlab.url.clone(),
                token: botto_config.gitlab.bot_token.clone(),
            }),
            ai: Some(AiClientConfig {
                base_url: botto_config.ai.base_url.clone(),
                api_key: botto_config.ai.api_key.clone(),
            }),
            ai_default_model: botto_config.ai.models.workflow_orchestrate.clone(),
            sandbox_max_memory_mb: botto_config.sandbox.max_memory_mb,
            pool: pool.clone(),
            botto_config: Some(botto_config.clone()),
            event_bus: Some(event_bus.clone()),
        };
        let mentor = MentorClient::new(pool.clone(), "global".into());
        let sm_config = SessionManagerConfig {
            ai_model: botto_config.ai.models.workflow_orchestrate.clone(),
            ..Default::default()
        };

        let sm = SessionManager::new(
            pool.clone(),
            ai_config,
            agent_config,
            mentor,
            event_bus,
            sm_config,
        );

        let mut session = match crud::load_session(&pool, &session_id.to_string()).await {
            Ok(Some(s)) => s,
            Ok(None) => {
                error!(session_id = %session_id, "workflow session vanished before driving");
                return;
            }
            Err(e) => {
                error!(session_id = %session_id, error = %e, "failed to reload workflow session");
                return;
            }
        };

        if let Err(e) = sm.drive(&mut session, &workflow_name).await {
            error!(
                session_id = %session_id,
                error = %e,
                "workflow session drive failed"
            );
        }
    });

    Ok(session_id.to_string())
}

/// Queue a review request via the queue manager.
///
/// The message content should reference a project and MR (e.g. "group/project !42").
/// Falls back to the message context's project_path and thread_id if available.
async fn route_request_review(
    _pool: &SqlitePool,
    _botto_config: &BottoConfig,
    queue_manager: Option<&Arc<QueueManager>>,
    message: &InboundMessage,
) -> Result<String> {
    let qm = queue_manager
        .ok_or_else(|| anyhow::anyhow!("review queue is not available"))?;

    // Try to extract project + MR from the message content or context.
    let (project_path, mr_iid) = parse_review_target(message)?;

    // Compute a default priority score — the queue manager will handle dedup.
    let priority_score = 50.0;

    info!(
        project = %project_path,
        mr_iid = mr_iid,
        "queuing review from channel request"
    );

    qm.enqueue(&project_path, mr_iid, priority_score)
        .await
        .map_err(|e| anyhow::anyhow!("{}", e))?;

    Ok(format!("Review queued for {} !{}", project_path, mr_iid))
}

/// Parse a review target (project_path, mr_iid) from the message content or context.
fn parse_review_target(message: &InboundMessage) -> Result<(String, u64)> {
    let content = message.raw_content.trim();

    // Try to parse "project/path !123" or "project/path 123" from content
    if !content.is_empty() {
        // Look for "!<number>" pattern
        if let Some(bang_pos) = content.find('!') {
            let mr_str = &content[bang_pos + 1..];
            let mr_num: String = mr_str.chars().take_while(|c| c.is_ascii_digit()).collect();
            if let Ok(mr_iid) = mr_num.parse::<u64>() {
                let project = content[..bang_pos].trim().to_string();
                if !project.is_empty() {
                    return Ok((project, mr_iid));
                }
                // No project in content — try context
                if let Some(ref pp) = message.context.project_path {
                    return Ok((pp.clone(), mr_iid));
                }
            }
        }
    }

    // Fall back to context (e.g. GitLab comment on an MR)
    if let Some(ref project_path) = message.context.project_path {
        if let Some(ref thread_id) = message.context.thread_id {
            // thread_id format: "mr:<iid>" from gitlab_input
            if let Some(iid_str) = thread_id.strip_prefix("mr:") {
                if let Ok(mr_iid) = iid_str.parse::<u64>() {
                    return Ok((project_path.clone(), mr_iid));
                }
            }
        }
    }

    anyhow::bail!(
        "couldn't determine which MR to review. Try: `review project/path !123`"
    )
}

/// Load the session referenced by the escalation response and call
/// escalation::handle_response to resume or cancel it.
async fn route_respond_to_escalation(
    pool: &SqlitePool,
    event_bus: &EventBus,
    message: &InboundMessage,
) -> Result<SessionStatus> {
    use crate::services::workflow::escalation;

    // The escalation response needs a session ID. We look for it in:
    // 1. The raw_payload (Slack interactive payloads embed it in the action value)
    // 2. The raw_content (formatted as "session_id:option" or just the option)
    let (session_id_str, chosen_option, response_text) =
        parse_escalation_response(message)?;

    let mut session = crud::load_session(pool, &session_id_str)
        .await
        .context("load session for escalation response")?
        .ok_or_else(|| anyhow::anyhow!("session '{}' not found", session_id_str))?;

    info!(
        session_id = %session_id_str,
        chosen_option = ?chosen_option,
        "routing escalation response to session"
    );

    let new_status = escalation::handle_response(
        pool,
        &mut session,
        event_bus,
        &response_text,
        chosen_option.as_deref(),
    )
    .await
    .context("handle escalation response")?;

    Ok(new_status)
}

/// Parse the escalation response content to extract session ID, chosen option,
/// and response text.
fn parse_escalation_response(
    message: &InboundMessage,
) -> Result<(String, Option<String>, String)> {
    let content = message.raw_content.trim();

    // Slack interactive payloads: action value is typically "session_id:option"
    if content.contains(':') {
        let parts: Vec<&str> = content.splitn(3, ':').collect();
        if parts.len() >= 2 {
            let session_id = parts[0].trim().to_string();
            let option = parts[1].trim().to_string();
            let text = if parts.len() == 3 {
                parts[2].trim().to_string()
            } else {
                option.clone()
            };
            // Validate session_id looks like a UUID
            if uuid::Uuid::parse_str(&session_id).is_ok() {
                return Ok((session_id, Some(option), text));
            }
        }
    }

    // Fall back: look for session_id in the raw payload (Slack interactions
    // may embed it in the message metadata or action block)
    if let Some(ref payload) = message.context.raw_payload {
        // Check actions[0].value for "session_id:option" format
        if let Some(actions) = payload["actions"].as_array() {
            if let Some(action_val) = actions.first().and_then(|a| a["value"].as_str()) {
                if action_val.contains(':') {
                    let parts: Vec<&str> = action_val.splitn(3, ':').collect();
                    if parts.len() >= 2 {
                        let session_id = parts[0].trim().to_string();
                        let option = parts[1].trim().to_string();
                        let text = if parts.len() == 3 {
                            parts[2].trim().to_string()
                        } else {
                            option.clone()
                        };
                        if uuid::Uuid::parse_str(&session_id).is_ok() {
                            return Ok((session_id, Some(option), text));
                        }
                    }
                }
            }
        }

        // Check for session_id in message metadata
        if let Some(sid) = payload["message"]["metadata"]["event_payload"]["session_id"].as_str() {
            return Ok((
                sid.to_string(),
                Some(content.to_string()),
                content.to_string(),
            ));
        }
    }

    anyhow::bail!(
        "couldn't determine which session to respond to. \
         Expected format: <session_id>:<option> (e.g. abc123:approve)"
    )
}
