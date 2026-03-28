// ---------------------------------------------------------------------------
// Directive Runner — background loops for polling, triage, and session spawning.
//
// Two background tasks:
//   1. Directive loop — every 30s, checks for directives due to poll, discovers
//      items, deduplicates, triages, and spawns workflow sessions.
//   2. Completion listener — subscribes to session completion events and updates
//      work item status accordingly.
// ---------------------------------------------------------------------------

use anyhow::{Context, Result};
use sqlx::SqlitePool;
use std::collections::HashMap;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use super::crud;
use super::discoverer::{ConnectorDiscoverer, WorkDiscoverer};
use super::triager::{AiTriager, WorkTriager};
use super::types::{
    Directive, DirectiveEscalation, DirectiveStatus, TriageDecision, WorkItemStatus,
};
use crate::services::ai::client::AiClientConfig;
use crate::services::events::{EventBus, EventType};
use crate::services::mentor::client::MentorClient;
use crate::services::workflow::crud::epoch_secs;
use crate::services::workflow::factory::AgentFactoryConfig;
use crate::services::workflow::session::{SessionManager, SessionManagerConfig};
use crate::types::workflow::{
    AgentType, RetryPolicy, Trigger,
    WorkflowDefinition, WorkflowMode, WorkflowStep,
};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Tuning knobs for the directive runner.
#[derive(Clone)]
pub struct DirectiveRunnerConfig {
    pub ai_config: AiClientConfig,
    pub ai_model: String,
    pub agent_config: AgentFactoryConfig,
    /// How often the loop checks for due directives (seconds).
    pub check_interval_secs: u64,
    /// Consecutive empty polls before escalating.
    pub escalate_after_empty_polls: u32,
    /// Failure rate threshold (0.0–1.0) for warning.
    pub failure_rate_warn_threshold: f64,
}

// ---------------------------------------------------------------------------
// Directive loop — main background task
// ---------------------------------------------------------------------------

/// Spawn the directive polling loop. Returns a JoinHandle for the background task.
pub fn spawn_directive_loop(
    pool: SqlitePool,
    config: DirectiveRunnerConfig,
    mentor: MentorClient,
    event_bus: EventBus,
    shutdown: tokio_util::sync::CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        info!("directive loop started (check every {}s)", config.check_interval_secs);
        let mut interval =
            tokio::time::interval(std::time::Duration::from_secs(config.check_interval_secs));

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    info!("directive loop shutting down");
                    break;
                }
                _ = interval.tick() => {
                    if let Err(e) = run_poll_cycle(&pool, &config, &mentor, &event_bus, &shutdown).await {
                        error!("directive poll cycle failed: {e:#}");
                    }
                }
            }
        }
    })
}

/// Single poll cycle: load due directives, discover, triage, spawn.
async fn run_poll_cycle(
    pool: &SqlitePool,
    config: &DirectiveRunnerConfig,
    mentor: &MentorClient,
    event_bus: &EventBus,
    shutdown: &tokio_util::sync::CancellationToken,
) -> Result<()> {
    let directives = crud::load_active_directives(pool)
        .await
        .context("load active directives")?;

    if directives.is_empty() {
        debug!("no directives due for polling");
        return Ok(());
    }

    debug!(count = directives.len(), "directives due for polling");

    for directive in directives {
        if let Err(e) = process_directive(pool, config, mentor, event_bus, &directive, shutdown).await {
            warn!(
                directive_id = %directive.id,
                directive_name = %directive.name,
                "directive processing failed: {e:#}"
            );
        }
    }

    Ok(())
}

/// Process a single directive: capacity check → discover → dedup → triage → spawn.
async fn process_directive(
    pool: &SqlitePool,
    config: &DirectiveRunnerConfig,
    mentor: &MentorClient,
    event_bus: &EventBus,
    directive: &Directive,
    shutdown: &tokio_util::sync::CancellationToken,
) -> Result<()> {
    info!(
        directive_id = %directive.id,
        name = %directive.name,
        "processing directive"
    );

    // 1. Working hours check.
    if !is_within_working_hours(&directive.constraints) {
        debug!(
            directive_id = %directive.id,
            "outside working hours, skipping"
        );
        update_next_poll(pool, directive).await?;
        return Ok(());
    }

    // 2. Capacity check.
    let active_sessions = crud::count_active_sessions_for_directive(pool, &directive.id).await?;
    let max_concurrent = directive.constraints.max_concurrent_sessions as i64;
    if active_sessions >= max_concurrent {
        debug!(
            directive_id = %directive.id,
            active = active_sessions,
            max = max_concurrent,
            "at capacity, skipping poll"
        );
        update_next_poll(pool, directive).await?;
        return Ok(());
    }

    let slots_available = (max_concurrent - active_sessions) as u32;

    // 3. Discover items.
    let discoverer = ConnectorDiscoverer::new(
        config.ai_config.clone(),
        config.ai_model.clone(),
        mentor.clone(),
    );

    let items = discoverer
        .discover(&directive.sources, &directive.id)
        .await
        .context("discover work items")?;

    info!(
        directive_id = %directive.id,
        discovered = items.len(),
        "discovery complete"
    );

    // 4. Dedup — filter out already-tracked items.
    let mut new_items = Vec::new();
    for item in &items {
        let tracked = crud::is_item_tracked(pool, &directive.id, &item.external_id).await?;
        if !tracked {
            new_items.push(item.clone());
        }
    }

    debug!(
        directive_id = %directive.id,
        new = new_items.len(),
        duplicates = items.len() - new_items.len(),
        "dedup complete"
    );

    // 5. Check for escalation on empty polls.
    if new_items.is_empty() {
        check_empty_poll_escalation(pool, config, directive, event_bus).await?;
        update_next_poll(pool, directive).await?;
        return Ok(());
    }

    // 6. Triage new items.
    let triager = AiTriager::new(
        config.ai_config.clone(),
        config.ai_model.clone(),
        mentor.clone(),
    );

    let max_items = directive
        .constraints
        .max_items_per_poll
        .min(slots_available);
    let mut accepted_count = 0u32;

    for item in new_items.iter().take(max_items as usize + 10) {
        // Stop if we've filled all slots.
        if accepted_count >= max_items {
            break;
        }

        let decision = match triager.triage(directive, item).await {
            Ok(d) => d,
            Err(e) => {
                warn!(
                    directive_id = %directive.id,
                    item_id = %item.external_id,
                    "triage failed: {e:#}"
                );
                // Track as discovered for retry later.
                crud::track_work_item(
                    pool,
                    &directive.id,
                    item,
                    &WorkItemStatus::Discovered,
                    Some("triage error"),
                    directive.priority,
                )
                .await?;
                continue;
            }
        };

        match decision {
            TriageDecision::Accept { reason, priority } => {
                info!(
                    directive_id = %directive.id,
                    item_id = %item.external_id,
                    title = %item.title,
                    priority,
                    "item accepted"
                );
                crud::track_work_item(
                    pool,
                    &directive.id,
                    item,
                    &WorkItemStatus::Accepted,
                    Some(&reason),
                    priority,
                )
                .await?;

                // 7. Spawn a session for the accepted item.
                if let Err(e) = spawn_session_for_item(
                    pool, config, mentor, event_bus, directive, item, priority, shutdown.clone(),
                )
                .await
                {
                    warn!(
                        directive_id = %directive.id,
                        item_id = %item.external_id,
                        "failed to spawn session: {e:#}"
                    );
                    crud::update_work_item_status(
                        pool,
                        &directive.id,
                        &item.external_id,
                        &WorkItemStatus::Failed,
                        None,
                    )
                    .await?;
                } else {
                    accepted_count += 1;
                }
            }
            TriageDecision::Reject { reason } => {
                debug!(
                    directive_id = %directive.id,
                    item_id = %item.external_id,
                    %reason,
                    "item rejected"
                );
                crud::track_work_item(
                    pool,
                    &directive.id,
                    item,
                    &WorkItemStatus::Rejected,
                    Some(&reason),
                    directive.priority,
                )
                .await?;
            }
            TriageDecision::NeedsMoreContext { question } => {
                debug!(
                    directive_id = %directive.id,
                    item_id = %item.external_id,
                    %question,
                    "item needs more context"
                );
                crud::track_work_item(
                    pool,
                    &directive.id,
                    item,
                    &WorkItemStatus::Discovered,
                    Some(&question),
                    directive.priority,
                )
                .await?;
            }
            TriageDecision::AlreadyTracked => {
                debug!(
                    directive_id = %directive.id,
                    item_id = %item.external_id,
                    "item already tracked"
                );
            }
        }
    }

    // 8. Check failure rate escalation.
    check_failure_rate_escalation(pool, config, directive, event_bus).await?;

    // 9. Update poll timestamps.
    update_next_poll(pool, directive).await?;

    info!(
        directive_id = %directive.id,
        accepted = accepted_count,
        "directive poll cycle complete"
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// Session spawning
// ---------------------------------------------------------------------------

/// Create a dynamic WorkflowDefinition from a directive + work item and spawn a session.
async fn spawn_session_for_item(
    pool: &SqlitePool,
    config: &DirectiveRunnerConfig,
    mentor: &MentorClient,
    event_bus: &EventBus,
    directive: &Directive,
    item: &super::types::WorkItem,
    priority: i32,
    shutdown: tokio_util::sync::CancellationToken,
) -> Result<()> {
    // Build a one-shot workflow definition for this work item.
    let workflow_id = Uuid::new_v4();
    let now = chrono::Utc::now();

    let workflow = WorkflowDefinition {
        id: workflow_id,
        name: format!("directive-{}-{}", directive.name, item.external_id),
        description: format!(
            "Auto-generated from directive '{}' for work item: {}\n\nDirective intent: {}\n\nItem: {} — {}",
            directive.name,
            item.external_id,
            directive.intent,
            item.title,
            item.description.as_deref().unwrap_or("(no description)"),
        ),
        project_id: 0, // Directives are cross-project.
        steps: vec![WorkflowStep {
            id: "execute".into(),
            action: format!(
                "Process work item '{}' according to directive intent: {}",
                item.title, directive.intent
            ),
            agent_type: AgentType::Ai,
            inputs: HashMap::new(),
            success_criteria: format!(
                "Work item '{}' has been processed according to the directive's intent",
                item.title
            ),
            depends_on: vec![],
            retry_policy: RetryPolicy::default(),
            timeout_secs: 300,
        }],
        triggers: vec![Trigger::Manual],
        created_by: directive
            .created_by
            .clone()
            .unwrap_or_else(|| "directive-runner".into()),
        created_at: now,
        updated_at: now,
        enabled: true,
        mode: WorkflowMode::Autonomous,
    };

    // Persist the workflow definition.
    crate::services::workflow::crud::create_workflow(pool, &workflow)
        .await
        .context("create directive workflow")?;

    // Create a session.
    let reply_context_json = directive
        .reply_context
        .as_ref()
        .and_then(|rc| serde_json::to_value(rc).ok());

    let trigger_data = serde_json::json!({
        "directive_id": directive.id,
        "directive_name": directive.name,
        "work_item_id": item.external_id,
        "work_item_title": item.title,
        "priority": priority,
        "reply_context": reply_context_json,
    });

    let mut session = crate::services::workflow::session::create_session(
        pool,
        workflow_id,
        "directive",
        Some(trigger_data),
    )
    .await
    .context("create directive session")?;

    let session_id = session.id.to_string();

    // Update work item with session reference.
    crud::update_work_item_status(
        pool,
        &directive.id,
        &item.external_id,
        &WorkItemStatus::InProgress,
        Some(&session_id),
    )
    .await?;

    // Build and spawn the session manager.
    let sm_config = SessionManagerConfig {
        ai_model: config.ai_model.clone(),
        ..Default::default()
    };

    let manager = SessionManager::new(
        pool.clone(),
        config.ai_config.clone(),
        config.agent_config.clone(),
        mentor.clone(),
        event_bus.clone(),
        sm_config,
    );

    let wf_name = workflow.name.clone();
    let pool_clone = pool.clone();
    let workflow_id_str = workflow_id.to_string();
    let directive_name = directive.name.clone();
    tokio::spawn(async move {
        tokio::select! {
            _ = shutdown.cancelled() => {
                // Graceful shutdown: checkpoint the session state before exiting.
                info!(
                    session_id = %session.id,
                    "directive session cancelled by shutdown, checkpointing"
                );
                if let Err(e) = crate::services::workflow::crud::checkpoint_session(
                    &pool_clone, &session,
                ).await {
                    warn!(
                        session_id = %session.id,
                        "failed to checkpoint session on shutdown: {e:#}"
                    );
                }
            }
            result = manager.drive(&mut session, &wf_name) => {
                if let Err(e) = result {
                    warn!(
                        session_id = %session.id,
                        error = %e,
                        "directive session drive failed"
                    );
                }

                // Bug #9: Clean up auto-created workflow definitions after session completes.
                if session.status.is_terminal() {
                    debug!(
                        workflow_id = %workflow_id_str,
                        directive = %directive_name,
                        "cleaning up auto-created directive workflow"
                    );
                    if let Err(e) = crate::services::workflow::crud::delete_workflow(
                        &pool_clone, &workflow_id_str,
                    ).await {
                        warn!(
                            workflow_id = %workflow_id_str,
                            "failed to clean up directive workflow: {e:#}"
                        );
                    }
                }
            }
        }
    });

    info!(
        directive_id = %directive.id,
        item_id = %item.external_id,
        %session_id,
        "spawned session for work item"
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// Completion listener — updates work item status on session completion
// ---------------------------------------------------------------------------

/// Spawn a background task that listens for session completion events and
/// updates the corresponding work item status.
pub fn spawn_completion_listener(
    pool: SqlitePool,
    event_bus: EventBus,
    shutdown: tokio_util::sync::CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        info!("directive completion listener started");
        let mut rx = event_bus.subscribe();

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    info!("directive completion listener shutting down");
                    break;
                }
                event = rx.recv() => {
                    let event = match event {
                        Ok(e) => e,
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            warn!(missed = n, "completion listener lagged");
                            continue;
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            info!("event bus closed, completion listener exiting");
                            break;
                        }
                    };

                    if event.event_type != EventType::WorkflowRunCompleted {
                        continue;
                    }

                    if let Some(ref payload) = event.payload {
                        if let Err(e) = handle_session_completion(&pool, payload).await {
                            warn!("failed to handle session completion: {e:#}");
                        }
                    }
                }
            }
        }
    })
}

/// Handle a session completion event — update the work item status.
async fn handle_session_completion(
    pool: &SqlitePool,
    payload: &serde_json::Value,
) -> Result<()> {
    let session_id = payload
        .get("session_id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("missing session_id in completion event"))?;

    let status_str = payload
        .get("status")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");

    // Find the work item linked to this session.
    let row: Option<(String, String)> = sqlx::query_as(
        "SELECT directive_id, external_id FROM directive_work_items WHERE session_id = ?",
    )
    .bind(session_id)
    .fetch_optional(pool)
    .await
    .context("lookup work item by session")?;

    let (directive_id, external_id) = match row {
        Some(r) => r,
        None => {
            // Not a directive-spawned session — ignore.
            return Ok(());
        }
    };

    let new_status = match status_str {
        "completed" => WorkItemStatus::Completed,
        "failed" | "cancelled" => WorkItemStatus::Failed,
        _ => return Ok(()),
    };

    crud::update_work_item_status(pool, &directive_id, &external_id, &new_status, None).await?;

    info!(
        directive_id = %directive_id,
        external_id = %external_id,
        %session_id,
        status = %new_status,
        "work item status updated from session completion"
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// Working hours check
// ---------------------------------------------------------------------------

fn is_within_working_hours(constraints: &super::types::DirectiveConstraints) -> bool {
    use chrono::Timelike;

    let (start, end) = match (constraints.working_hours_start, constraints.working_hours_end) {
        (Some(s), Some(e)) => (s, e),
        _ => return true, // No working hours configured — always active.
    };

    let now = chrono::Local::now();
    let hour = now.hour();

    if start <= end {
        hour >= start && hour < end
    } else {
        // Wraps midnight (e.g., 22–06).
        hour >= start || hour < end
    }
}

// ---------------------------------------------------------------------------
// Escalation checks
// ---------------------------------------------------------------------------

async fn check_empty_poll_escalation(
    pool: &SqlitePool,
    config: &DirectiveRunnerConfig,
    directive: &Directive,
    event_bus: &EventBus,
) -> Result<()> {
    let empty_count = crud::count_consecutive_empty_polls(pool, &directive.id).await?;

    if empty_count >= config.escalate_after_empty_polls as i64 {
        let now = epoch_secs();
        let escalation = DirectiveEscalation {
            reason: format!(
                "No new items discovered in {} consecutive polls. Sources may be exhausted or misconfigured.",
                empty_count
            ),
            severity: "warning".into(),
            consecutive_empty_polls: empty_count as u32,
            failure_rate: None,
            created_at: now,
        };

        // Update directive with escalation.
        let mut updated = directive.clone();
        updated.escalation = Some(escalation.clone());
        updated.status = DirectiveStatus::WaitingForHuman;
        crud::update_directive(pool, &updated).await?;

        // Publish to EventBus so the bridge can notify the channel.
        let mut payload = serde_json::json!({
            "directive_id": directive.id,
            "directive_name": directive.name,
            "reason": escalation.reason,
            "severity": escalation.severity,
        });
        if let Some(ref rc) = directive.reply_context {
            if let Ok(rc_val) = serde_json::to_value(rc) {
                payload["reply_context"] = rc_val;
            }
        }
        event_bus.publish(crate::services::events::Event {
            event_type: EventType::DirectiveEscalation,
            project_path: String::new(),
            mr_iid: None,
            user_id: None,
            payload: Some(payload),
        });

        warn!(
            directive_id = %directive.id,
            empty_polls = empty_count,
            "directive escalated: consecutive empty polls"
        );
    }

    Ok(())
}

async fn check_failure_rate_escalation(
    pool: &SqlitePool,
    config: &DirectiveRunnerConfig,
    directive: &Directive,
    event_bus: &EventBus,
) -> Result<()> {
    let failed = crud::count_failed_sessions(pool, &directive.id).await?;
    let total_items: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM directive_work_items
         WHERE directive_id = ? AND status IN ('completed', 'failed')",
    )
    .bind(&directive.id)
    .fetch_one(pool)
    .await
    .context("count total terminal items")?;

    let total = total_items.0;
    if total < 3 {
        return Ok(()); // Not enough data to judge.
    }

    let failure_rate = failed as f64 / total as f64;
    if failure_rate > config.failure_rate_warn_threshold {
        let now = epoch_secs();
        let escalation = DirectiveEscalation {
            reason: format!(
                "High failure rate: {:.0}% ({}/{} sessions failed). Review directive configuration.",
                failure_rate * 100.0,
                failed,
                total,
            ),
            severity: "warning".into(),
            consecutive_empty_polls: 0,
            failure_rate: Some(failure_rate),
            created_at: now,
        };

        let mut updated = directive.clone();
        updated.escalation = Some(escalation.clone());
        // Don't pause — just attach the warning.
        crud::update_directive(pool, &updated).await?;

        // Publish to EventBus so the bridge can notify the channel.
        let mut payload = serde_json::json!({
            "directive_id": directive.id,
            "directive_name": directive.name,
            "reason": escalation.reason,
            "severity": escalation.severity,
            "failure_rate": failure_rate,
        });
        if let Some(ref rc) = directive.reply_context {
            if let Ok(rc_val) = serde_json::to_value(rc) {
                payload["reply_context"] = rc_val;
            }
        }
        event_bus.publish(crate::services::events::Event {
            event_type: EventType::DirectiveEscalation,
            project_path: String::new(),
            mr_iid: None,
            user_id: None,
            payload: Some(payload),
        });

        warn!(
            directive_id = %directive.id,
            failure_rate = format!("{:.1}%", failure_rate * 100.0),
            "directive warning: high failure rate"
        );
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Poll timestamp management
// ---------------------------------------------------------------------------

async fn update_next_poll(pool: &SqlitePool, directive: &Directive) -> Result<()> {
    let now = epoch_secs();
    let next = now + directive.poll_interval_secs;

    sqlx::query(
        "UPDATE directives SET last_poll_at = ?, next_poll_at = ?, updated_at = ? WHERE id = ?",
    )
    .bind(now)
    .bind(next)
    .bind(now)
    .bind(&directive.id)
    .execute(pool)
    .await
    .context("update directive poll timestamps")?;

    debug!(
        directive_id = %directive.id,
        next_poll_at = next,
        "poll timestamps updated"
    );

    Ok(())
}
