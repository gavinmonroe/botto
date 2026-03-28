// ---------------------------------------------------------------------------
// Escalation — human-in-the-loop protocol for the v2 session orchestrator.
//
// When an agent hits a blocker it can't resolve autonomously, the session
// pauses via `escalate()`. The human responds through the WebSocket or API,
// and `handle_response()` resumes (or cancels) the session.
// ---------------------------------------------------------------------------

use anyhow::{bail, Context, Result};
use sqlx::SqlitePool;
use tracing::{debug, info, warn};

use crate::services::events::{Event, EventBus, EventType};
use crate::services::workflow::crud;
use crate::types::workflow::{
    EscalationMessage, EscalationOption, EscalationSeverity, SessionState, SessionStatus,
};

// ---------------------------------------------------------------------------
// Core escalation lifecycle
// ---------------------------------------------------------------------------

/// Pause a session and notify the human that intervention is needed.
///
/// This is the critical path — if it fails the workflow is stuck. Each step
/// is ordered so that the most important mutation (status change + checkpoint)
/// happens first, and the nice-to-haves (event, message log) come after.
pub async fn escalate(
    pool: &SqlitePool,
    session: &mut SessionState,
    event_bus: &EventBus,
    message: EscalationMessage,
) -> Result<()> {
    let session_id = session.id;
    let severity = &message.severity;
    let reason = &message.reason;

    info!(
        session_id = %session_id,
        severity = ?severity,
        reason = %reason,
        step_id = ?message.step_id,
        "escalating session to human"
    );

    // 1. Transition state — must succeed before anything else.
    session.status = SessionStatus::WaitingForHuman;
    session.escalation = Some(message.clone());
    session.updated_at = crud::epoch_secs();

    // 2. Persist to SQLite so the session survives a crash while waiting.
    crud::checkpoint_session(pool, session)
        .await
        .context("checkpoint session during escalation")?;

    // 3. Log the escalation as a conversation message so the thread is complete.
    let content = format!(
        "[Escalation — {:?}] {}\n\nWhat I need: {}",
        severity, reason, message.what_i_need
    );
    let metadata = serde_json::to_value(&message)
        .ok(); // best-effort; don't fail the escalation over serialization
    crud::add_session_message(
        pool,
        &session_id,
        "agent_to_human",
        &content,
        metadata.as_ref(),
    )
    .await
    .context("log escalation message")?;

    // 4. Broadcast for WebSocket listeners.
    let payload = serde_json::to_value(&message).ok();
    event_bus.publish(Event {
        event_type: EventType::SessionEscalation,
        project_path: String::new(),
        mr_iid: None,
        user_id: None,
        payload,
    });

    debug!(session_id = %session_id, "escalation complete, session paused");
    Ok(())
}

/// Process a human's response to an escalation and resume (or cancel) the session.
///
/// Returns the new `SessionStatus` so the caller knows what to do next.
pub async fn handle_response(
    pool: &SqlitePool,
    session: &mut SessionState,
    event_bus: &EventBus,
    response_content: &str,
    chosen_option: Option<&str>,
) -> Result<SessionStatus> {
    let session_id = session.id;

    // Guard: only respond to sessions that are actually waiting.
    if session.status != SessionStatus::WaitingForHuman {
        warn!(
            session_id = %session_id,
            current_status = %session.status,
            "handle_response called but session is not waiting for human"
        );
        bail!(
            "session {} is in status '{}', not 'waiting_for_human'",
            session_id,
            session.status
        );
    }

    info!(
        session_id = %session_id,
        chosen_option = ?chosen_option,
        "received human response to escalation"
    );

    // 1. Log the human's reply in the conversation thread.
    let metadata = chosen_option.map(|opt| serde_json::json!({ "chosen_option": opt }));
    crud::add_session_message(
        pool,
        &session_id,
        "human_to_agent",
        response_content,
        metadata.as_ref(),
    )
    .await
    .context("log human response message")?;

    // 2. Determine next status from the chosen option.
    let next_status = match chosen_option {
        Some("cancel") => SessionStatus::Cancelled,
        Some(opt) if opt.starts_with("replan") => SessionStatus::Adapting,
        _ => SessionStatus::Executing,
    };

    debug!(
        session_id = %session_id,
        next_status = %next_status,
        "transitioning session after human response"
    );

    // 3. Update session state.
    session.status = next_status.clone();
    session.escalation = None;
    session.updated_at = crud::epoch_secs();
    if next_status == SessionStatus::Cancelled {
        session.completed_at = Some(crud::epoch_secs());
    }

    // 4. Checkpoint.
    crud::checkpoint_session(pool, session)
        .await
        .context("checkpoint session after human response")?;

    // 5. Broadcast resume event.
    let payload = serde_json::json!({
        "session_id": session_id.to_string(),
        "new_status": next_status.as_str(),
        "chosen_option": chosen_option,
    });
    event_bus.publish(Event {
        event_type: EventType::SessionResumed,
        project_path: String::new(),
        mr_iid: None,
        user_id: None,
        payload: Some(payload),
    });

    info!(
        session_id = %session_id,
        new_status = %next_status,
        "session resumed after human response"
    );

    Ok(next_status)
}

// ---------------------------------------------------------------------------
// Escalation builders — produce clear, actionable messages
// ---------------------------------------------------------------------------

/// Build an escalation for a missing or unavailable capability (e.g. credentials,
/// API access, tool not configured).
pub fn capability_escalation(
    session: &SessionState,
    workflow_name: &str,
    step_id: &str,
    capability: &str,
    description: &str,
) -> EscalationMessage {
    EscalationMessage {
        session_id: session.id,
        workflow_name: workflow_name.to_string(),
        step_id: Some(step_id.to_string()),
        severity: EscalationSeverity::Blocking,
        reason: format!(
            "Step '{}' requires the '{}' capability which is not available.",
            step_id, capability
        ),
        what_i_need: description.to_string(),
        options: vec![
            EscalationOption {
                id: "provide_credentials".into(),
                label: "Provide credentials".into(),
                description: Some(format!(
                    "Supply the missing '{}' capability so the step can proceed.",
                    capability
                )),
            },
            EscalationOption {
                id: "skip".into(),
                label: "Skip this step".into(),
                description: Some("Mark the step as skipped and continue with the rest of the workflow.".into()),
            },
            EscalationOption {
                id: "cancel".into(),
                label: "Cancel workflow".into(),
                description: Some("Stop the entire workflow.".into()),
            },
        ],
        created_at: crud::epoch_secs(),
    }
}

/// Build an escalation for a step that has failed evaluation repeatedly.
pub fn evaluation_failure_escalation(
    session: &SessionState,
    workflow_name: &str,
    step_id: &str,
    attempts: u32,
    last_feedback: &str,
) -> EscalationMessage {
    EscalationMessage {
        session_id: session.id,
        workflow_name: workflow_name.to_string(),
        step_id: Some(step_id.to_string()),
        severity: EscalationSeverity::Blocking,
        reason: format!(
            "Step '{}' has failed evaluation {} time(s). Last feedback: {}",
            step_id, attempts, last_feedback
        ),
        what_i_need: "Guidance on how to proceed — retry with a different approach, skip, replan, or cancel.".into(),
        options: vec![
            EscalationOption {
                id: "retry_different".into(),
                label: "Retry with a different approach".into(),
                description: Some("I'll try an alternative strategy for this step.".into()),
            },
            EscalationOption {
                id: "skip".into(),
                label: "Skip this step".into(),
                description: Some("Accept the current state and move on.".into()),
            },
            EscalationOption {
                id: "replan".into(),
                label: "Replan the workflow".into(),
                description: Some("Go back to planning and find a different path to the goal.".into()),
            },
            EscalationOption {
                id: "cancel".into(),
                label: "Cancel workflow".into(),
                description: Some("Stop the entire workflow.".into()),
            },
        ],
        created_at: crud::epoch_secs(),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db;
    use crate::services::workflow::crud;
    use crate::types::workflow::{
        EscalationOption, EscalationSeverity, SessionState, SessionStatus,
    };
    use std::collections::HashMap;
    use uuid::Uuid;

    async fn test_pool() -> sqlx::SqlitePool {
        db::test_pool().await
    }

    async fn seed_workflow(pool: &sqlx::SqlitePool) -> Uuid {
        let wf = crate::types::workflow::WorkflowDefinition {
            id: Uuid::new_v4(),
            name: "test-wf".into(),
            description: "test".into(),
            project_id: 1,
            steps: vec![],
            triggers: vec![],
            created_by: "test".into(),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            enabled: true,
            mode: Default::default(),
        };
        crud::create_workflow(pool, &wf).await.unwrap();
        wf.id
    }

    fn make_session(wf_id: Uuid, status: SessionStatus) -> SessionState {
        let now = crud::epoch_secs();
        SessionState {
            id: Uuid::new_v4(),
            workflow_id: wf_id,
            status,
            trigger_type: "manual".into(),
            trigger_data: None,
            plan: None,
            step_outputs: HashMap::new(),
            current_step_id: None,
            retry_count: 0,
            max_retries: 3,
            step_retry_count: 0,
            evaluator_feedback: None,
            escalation: None,
            pending_modification: None,
            started_at: now,
            completed_at: None,
            updated_at: now,
        }
    }

    fn make_escalation_message(session_id: Uuid) -> EscalationMessage {
        EscalationMessage {
            session_id,
            workflow_name: "test-wf".into(),
            step_id: Some("step-1".into()),
            severity: EscalationSeverity::Blocking,
            reason: "something broke".into(),
            what_i_need: "human guidance".into(),
            options: vec![
                EscalationOption {
                    id: "retry".into(),
                    label: "Retry".into(),
                    description: Some("Try again".into()),
                },
                EscalationOption {
                    id: "cancel".into(),
                    label: "Cancel".into(),
                    description: None,
                },
            ],
            created_at: crud::epoch_secs(),
        }
    }

    // -- escalate ------------------------------------------------------------

    #[tokio::test]
    async fn escalate_changes_status_and_persists() {
        let pool = test_pool().await;
        let wf_id = seed_workflow(&pool).await;
        let event_bus = EventBus::new();
        let mut session = make_session(wf_id, SessionStatus::Executing);
        crud::create_session(&pool, &session).await.unwrap();

        let msg = make_escalation_message(session.id);
        escalate(&pool, &mut session, &event_bus, msg).await.unwrap();

        assert_eq!(session.status, SessionStatus::WaitingForHuman);
        assert!(session.escalation.is_some());

        let loaded = crud::load_session(&pool, &session.id.to_string())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded.status, SessionStatus::WaitingForHuman);
        assert!(loaded.escalation.is_some());
        assert_eq!(loaded.escalation.unwrap().reason, "something broke");

        let messages =
            crud::load_session_messages(&pool, &session.id.to_string(), 100)
                .await
                .unwrap();
        assert_eq!(messages.len(), 1);
        assert!(messages[0].content.contains("something broke"));
    }

    // -- handle_response -----------------------------------------------------

    #[tokio::test]
    async fn handle_response_cancel() {
        let pool = test_pool().await;
        let wf_id = seed_workflow(&pool).await;
        let event_bus = EventBus::new();
        let mut session = make_session(wf_id, SessionStatus::WaitingForHuman);
        session.escalation = Some(make_escalation_message(session.id));
        crud::create_session(&pool, &session).await.unwrap();

        let next = handle_response(
            &pool,
            &mut session,
            &event_bus,
            "Please cancel",
            Some("cancel"),
        )
        .await
        .unwrap();

        assert_eq!(next, SessionStatus::Cancelled);
        assert_eq!(session.status, SessionStatus::Cancelled);
        assert!(session.completed_at.is_some());
        assert!(session.escalation.is_none());
    }

    #[tokio::test]
    async fn handle_response_replan() {
        let pool = test_pool().await;
        let wf_id = seed_workflow(&pool).await;
        let event_bus = EventBus::new();
        let mut session = make_session(wf_id, SessionStatus::WaitingForHuman);
        session.escalation = Some(make_escalation_message(session.id));
        crud::create_session(&pool, &session).await.unwrap();

        let next = handle_response(
            &pool,
            &mut session,
            &event_bus,
            "Try a different approach",
            Some("replan"),
        )
        .await
        .unwrap();

        assert_eq!(next, SessionStatus::Adapting);
        assert_eq!(session.status, SessionStatus::Adapting);
        assert!(session.completed_at.is_none());
    }

    #[tokio::test]
    async fn handle_response_default_resumes_executing() {
        let pool = test_pool().await;
        let wf_id = seed_workflow(&pool).await;
        let event_bus = EventBus::new();
        let mut session = make_session(wf_id, SessionStatus::WaitingForHuman);
        session.escalation = Some(make_escalation_message(session.id));
        crud::create_session(&pool, &session).await.unwrap();

        let next =
            handle_response(&pool, &mut session, &event_bus, "Go ahead", None)
                .await
                .unwrap();

        assert_eq!(next, SessionStatus::Executing);
    }

    #[tokio::test]
    async fn handle_response_on_non_waiting_session_errors() {
        let pool = test_pool().await;
        let wf_id = seed_workflow(&pool).await;
        let event_bus = EventBus::new();
        let mut session = make_session(wf_id, SessionStatus::Executing);
        crud::create_session(&pool, &session).await.unwrap();

        let result =
            handle_response(&pool, &mut session, &event_bus, "hello", None).await;

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("not 'waiting_for_human'"));
    }

    #[tokio::test]
    async fn handle_response_logs_human_message() {
        let pool = test_pool().await;
        let wf_id = seed_workflow(&pool).await;
        let event_bus = EventBus::new();
        let mut session = make_session(wf_id, SessionStatus::WaitingForHuman);
        session.escalation = Some(make_escalation_message(session.id));
        crud::create_session(&pool, &session).await.unwrap();

        handle_response(
            &pool,
            &mut session,
            &event_bus,
            "My response text",
            Some("retry"),
        )
        .await
        .unwrap();

        let messages =
            crud::load_session_messages(&pool, &session.id.to_string(), 100)
                .await
                .unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].content, "My response text");
    }

    // -- capability_escalation builder ---------------------------------------

    #[test]
    fn capability_escalation_message_structure() {
        let dummy_wf = Uuid::new_v4();
        let session = make_session(dummy_wf, SessionStatus::Executing);
        let msg = capability_escalation(
            &session,
            "deploy-workflow",
            "deploy-step",
            "slack_notify",
            "Need Slack bot token to post notifications",
        );

        assert_eq!(msg.session_id, session.id);
        assert_eq!(msg.workflow_name, "deploy-workflow");
        assert_eq!(msg.step_id.as_deref(), Some("deploy-step"));
        assert_eq!(msg.severity, EscalationSeverity::Blocking);
        assert!(msg.reason.contains("slack_notify"));
        assert!(msg.what_i_need.contains("Slack bot token"));
        assert_eq!(msg.options.len(), 3);
        let ids: Vec<&str> = msg.options.iter().map(|o| o.id.as_str()).collect();
        assert!(ids.contains(&"provide_credentials"));
        assert!(ids.contains(&"skip"));
        assert!(ids.contains(&"cancel"));
    }

    // -- evaluation_failure_escalation builder --------------------------------

    #[test]
    fn evaluation_failure_escalation_message_structure() {
        let dummy_wf = Uuid::new_v4();
        let session = make_session(dummy_wf, SessionStatus::Executing);
        let msg = evaluation_failure_escalation(
            &session,
            "review-workflow",
            "lint-step",
            3,
            "Output doesn't match criteria",
        );

        assert_eq!(msg.session_id, session.id);
        assert_eq!(msg.workflow_name, "review-workflow");
        assert_eq!(msg.step_id.as_deref(), Some("lint-step"));
        assert_eq!(msg.severity, EscalationSeverity::Blocking);
        assert!(msg.reason.contains("3 time(s)"));
        assert!(msg.reason.contains("Output doesn't match criteria"));
        assert_eq!(msg.options.len(), 4);
        let ids: Vec<&str> = msg.options.iter().map(|o| o.id.as_str()).collect();
        assert!(ids.contains(&"retry_different"));
        assert!(ids.contains(&"skip"));
        assert!(ids.contains(&"replan"));
        assert!(ids.contains(&"cancel"));
    }
}
