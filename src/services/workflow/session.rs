// ---------------------------------------------------------------------------
// Session Manager — top-level coordinator for the v2 autonomous orchestrator.
//
// Manages the full lifecycle of a workflow session:
//   Created → Planning → Executing ↔ Evaluating → Completed
//                           ↕              ↕
//                       Adapting    WaitingForHuman
//
// Each state transition is checkpointed to SQLite. If botto crashes, sessions
// in non-terminal states are resumed on restart.
//
// The Session Manager builds fresh context for each agent invocation — no
// accumulated memory. This prevents context degradation on long runs.
// ---------------------------------------------------------------------------

use anyhow::{Context, Result};
use sqlx::SqlitePool;
use std::collections::HashMap;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::services::ai::client::AiClientConfig;
use crate::services::events::{Event, EventBus, EventType};
use crate::services::mentor::client::MentorClient;
use crate::services::workflow::{connector, crud, escalation, evaluator, generator, planner, registry};
use crate::services::workflow::factory::AgentFactoryConfig;
use crate::types::workflow::{
    EscalationSeverity, GeneratorOutcome, PlanStep, SessionPlan, SessionState, SessionStatus,
    PendingPlanModification, TraceEventType,
};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Tuning knobs for the session manager.
#[derive(Debug, Clone)]
pub struct SessionManagerConfig {
    /// Max retries per step before escalating.
    pub max_step_retries: u32,
    /// Max total retries across the session before failing.
    pub max_session_retries: u32,
    /// Evaluator pass threshold (0.0–1.0).
    pub eval_threshold: f64,
    /// AI model to use for planner/evaluator.
    pub ai_model: String,
}

impl Default for SessionManagerConfig {
    fn default() -> Self {
        Self {
            max_step_retries: 3,
            max_session_retries: 10,
            eval_threshold: 0.6,
            ai_model: "claude-sonnet-4-5".into(),
        }
    }
}

// ---------------------------------------------------------------------------
// Session Manager
// ---------------------------------------------------------------------------

/// The Session Manager — drives a single workflow session through the
/// three-agent pipeline.
pub struct SessionManager {
    pool: SqlitePool,
    ai_config: AiClientConfig,
    agent_config: AgentFactoryConfig,
    mentor: MentorClient,
    event_bus: EventBus,
    config: SessionManagerConfig,
}

impl SessionManager {
    pub fn new(
        pool: SqlitePool,
        ai_config: AiClientConfig,
        agent_config: AgentFactoryConfig,
        mentor: MentorClient,
        event_bus: EventBus,
        config: SessionManagerConfig,
    ) -> Self {
        Self {
            pool,
            ai_config,
            agent_config,
            mentor,
            event_bus,
            config,
        }
    }

    // -----------------------------------------------------------------------
    // Entry point — run a session to completion (or until it blocks)
    // -----------------------------------------------------------------------

    /// Drive a session forward from its current state. Returns when the session
    /// reaches a terminal state or blocks on human input.
    ///
    /// This is the main loop. It's re-entrant: call it after crash recovery
    /// or after a human responds to an escalation.
    pub async fn drive(&self, session: &mut SessionState, workflow_name: &str) -> Result<()> {
        info!(
            session_id = %session.id,
            status = %session.status,
            "session manager: driving session"
        );

        const MAX_ITERATIONS: u32 = 100;
        let max_duration = std::time::Duration::from_secs(3600); // 1 hour
        let drive_start = std::time::Instant::now();
        let mut iterations: u32 = 0;

        loop {
            // Guard: iteration limit.
            iterations += 1;
            if iterations > MAX_ITERATIONS {
                warn!(
                    session_id = %session.id,
                    iterations,
                    "session exceeded maximum iterations, escalating"
                );
                let msg = crate::types::workflow::EscalationMessage {
                    session_id: session.id,
                    workflow_name: workflow_name.to_string(),
                    step_id: session.current_step_id.clone(),
                    severity: EscalationSeverity::Blocking,
                    reason: format!("Session exceeded maximum iterations ({MAX_ITERATIONS})"),
                    what_i_need: "The session has been running too long. Please review and decide whether to continue, replan, or cancel.".into(),
                    options: vec![
                        crate::types::workflow::EscalationOption {
                            id: "continue".into(),
                            label: "Continue".into(),
                            description: Some("Resume the session for another round of iterations.".into()),
                        },
                        crate::types::workflow::EscalationOption {
                            id: "replan".into(),
                            label: "Replan".into(),
                            description: Some("Go back to planning with a simpler approach.".into()),
                        },
                        crate::types::workflow::EscalationOption {
                            id: "cancel".into(),
                            label: "Cancel".into(),
                            description: Some("Stop the workflow.".into()),
                        },
                    ],
                    created_at: crud::epoch_secs(),
                };
                escalation::escalate(&self.pool, session, &self.event_bus, msg).await?;
                return Ok(());
            }

            // Guard: total duration limit.
            if drive_start.elapsed() > max_duration {
                warn!(
                    session_id = %session.id,
                    elapsed_secs = drive_start.elapsed().as_secs(),
                    "session exceeded maximum duration, escalating"
                );
                let msg = crate::types::workflow::EscalationMessage {
                    session_id: session.id,
                    workflow_name: workflow_name.to_string(),
                    step_id: session.current_step_id.clone(),
                    severity: EscalationSeverity::Blocking,
                    reason: "Session exceeded maximum duration (1 hour)".into(),
                    what_i_need: "The session has been running too long. Please review and decide whether to continue or cancel.".into(),
                    options: vec![
                        crate::types::workflow::EscalationOption {
                            id: "continue".into(),
                            label: "Continue".into(),
                            description: Some("Resume the session.".into()),
                        },
                        crate::types::workflow::EscalationOption {
                            id: "cancel".into(),
                            label: "Cancel".into(),
                            description: Some("Stop the workflow.".into()),
                        },
                    ],
                    created_at: crud::epoch_secs(),
                };
                escalation::escalate(&self.pool, session, &self.event_bus, msg).await?;
                return Ok(());
            }

            match &session.status {
                SessionStatus::Created => {
                    self.transition_to_planning(session).await?;
                }
                SessionStatus::Planning => {
                    if let Err(e) = self.run_planning(session).await {
                        warn!(session_id = %session.id, error = ?e, "planning failed");
                        // Don't leave the session stuck in planning — fail it
                        self.fail_session(session, &format!("planning failed: {e:#}")).await?;
                    }
                }
                SessionStatus::Executing => {
                    if let Err(e) = self.run_next_step(session, workflow_name).await {
                        warn!(session_id = %session.id, error = %e, "step execution failed");
                        self.fail_session(session, &format!("execution failed: {e}")).await?;
                    }
                }
                SessionStatus::Evaluating => {
                    if let Err(e) = self.run_evaluation(session, workflow_name).await {
                        warn!(session_id = %session.id, error = %e, "evaluation failed");
                        self.fail_session(session, &format!("evaluation failed: {e}")).await?;
                    }
                }
                SessionStatus::Adapting => {
                    if let Err(e) = self.run_replanning(session).await {
                        warn!(session_id = %session.id, error = %e, "replanning failed");
                        self.fail_session(session, &format!("replanning failed: {e}")).await?;
                    }
                }
                SessionStatus::Clarifying => {
                    // Clarifying works like WaitingForHuman — session is paused
                    // until the user provides answers. Re-emit the escalation.
                    if let Some(ref esc) = session.escalation {
                        info!(
                            session_id = %session.id,
                            reason = %esc.reason,
                            "session waiting for clarification — re-emitting notification"
                        );
                        self.event_bus.publish(crate::services::events::Event {
                            event_type: crate::services::events::EventType::SessionEscalation,
                            project_path: String::new(),
                            mr_iid: None,
                            user_id: None,
                            payload: Some(serde_json::json!({
                                "type": "clarification_needed",
                                "session_id": session.id.to_string(),
                                "reason": esc.reason,
                                "what_i_need": esc.what_i_need,
                            })),
                        });
                    } else {
                        debug!(session_id = %session.id, "session blocked on clarification (no escalation data)");
                    }
                    return Ok(());
                }
                SessionStatus::WaitingForHuman => {
                    // Re-emit escalation on recovery — if we crashed after
                    // checkpointing WaitingForHuman but before the notification
                    // reached the user, they were never notified. Re-publish
                    // the escalation event so the user sees it.
                    if let Some(ref esc) = session.escalation {
                        info!(
                            session_id = %session.id,
                            reason = %esc.reason,
                            "session waiting for human — re-emitting escalation notification"
                        );
                        self.event_bus.publish(crate::services::events::Event {
                            event_type: crate::services::events::EventType::SessionEscalation,
                            project_path: String::new(),
                            mr_iid: None,
                            user_id: None,
                            payload: Some(serde_json::json!({
                                "type": "escalation_reminder",
                                "session_id": session.id.to_string(),
                                "reason": esc.reason,
                                "what_i_need": esc.what_i_need,
                            })),
                        });
                    } else {
                        debug!(session_id = %session.id, "session blocked on human input (no escalation data)");
                    }
                    return Ok(());
                }
                SessionStatus::Completed | SessionStatus::Failed | SessionStatus::Cancelled => {
                    debug!(
                        session_id = %session.id,
                        status = %session.status,
                        "session reached terminal state"
                    );
                    return Ok(());
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // State: Created → Planning
    // -----------------------------------------------------------------------

    async fn transition_to_planning(&self, session: &mut SessionState) -> Result<()> {
        session.status = SessionStatus::Planning;
        session.updated_at = crud::epoch_secs();
        crud::checkpoint_session(&self.pool, session).await?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // State: Planning — invoke the Planner agent
    // -----------------------------------------------------------------------

    async fn run_planning(&self, session: &mut SessionState) -> Result<()> {
        let trigger_data = session
            .trigger_data
            .clone()
            .unwrap_or(serde_json::json!({}));

        // Load the workflow description for the planner prompt.
        let workflow_desc = self.load_workflow_description(session).await?;

        // Build the tool catalog from agent configuration.
        let tool_catalog = registry::build_tool_catalog(&self.agent_config);

        // Check if we're resuming from Clarifying with user answers.
        let clarification_answers = if session.status == SessionStatus::Planning {
            // Check for recent human_to_agent messages that contain clarification answers.
            let messages = crud::load_session_messages(
                &self.pool,
                &session.id.to_string(),
                10,
            ).await.unwrap_or_default();

            let answers: Vec<String> = messages
                .iter()
                .filter(|m| m.direction == crate::types::workflow::MessageDirection::HumanToAgent)
                .map(|m| m.content.clone())
                .collect();

            if answers.is_empty() { None } else { Some(answers) }
        } else {
            None
        };

        // Build trigger data with clarification answers if available.
        let planning_trigger = if let Some(answers) = clarification_answers {
            let mut data = trigger_data.clone();
            if let Some(obj) = data.as_object_mut() {
                obj.insert(
                    "clarification_answers".into(),
                    serde_json::Value::Array(
                        answers.into_iter().map(serde_json::Value::String).collect(),
                    ),
                );
            }
            data
        } else {
            trigger_data
        };

        let plan_result = planner::create_plan(
            &self.ai_config,
            &self.config.ai_model,
            &self.mentor,
            &planning_trigger,
            &workflow_desc,
            &tool_catalog,
        )
        .await
        .context("planner failed")?;

        match plan_result {
            planner::PlanResult::Plan(plan) => {
                info!(
                    session_id = %session.id,
                    goal = %plan.goal,
                    steps = plan.steps.len(),
                    "plan created"
                );

                // Trace: plan created.
                if let Err(e) = crud::append_trace(
                    &self.pool,
                    &session.id,
                    &TraceEventType::PlanCreated,
                    None,
                    None,
                    None,
                    Some(&serde_json::json!({
                        "goal": plan.goal,
                        "step_count": plan.steps.len(),
                        "steps": plan.steps.iter().map(|s| serde_json::json!({
                            "id": s.id,
                            "tool": s.tool,
                            "agent_type": s.agent_type.as_str(),
                        })).collect::<Vec<_>>(),
                    })),
                    None,
                    None,
                    None,
                ).await {
                    warn!(session_id = %session.id, error = %e, "failed to append PlanCreated trace");
                }

                session.plan = Some(plan);
                session.status = SessionStatus::Executing;
                session.updated_at = crud::epoch_secs();
                crud::checkpoint_session(&self.pool, session).await?;

                self.publish_event(session, EventType::WorkflowRunStarted);
            }
            planner::PlanResult::NeedsClarification { questions, reason } => {
                info!(
                    session_id = %session.id,
                    questions = questions.len(),
                    %reason,
                    "planner needs clarification"
                );

                // Trace: clarification requested.
                if let Err(e) = crud::append_trace(
                    &self.pool,
                    &session.id,
                    &TraceEventType::ClarificationRequested,
                    None,
                    None,
                    None,
                    Some(&serde_json::json!({
                        "questions": questions,
                        "reason": reason,
                    })),
                    None,
                    None,
                    None,
                ).await {
                    warn!(session_id = %session.id, error = %e, "failed to append ClarificationRequested trace");
                }

                // Build escalation message with the questions.
                let questions_text = questions
                    .iter()
                    .enumerate()
                    .map(|(i, q)| format!("{}. {}", i + 1, q))
                    .collect::<Vec<_>>()
                    .join("\n");

                let msg = crate::types::workflow::EscalationMessage {
                    session_id: session.id,
                    workflow_name: workflow_desc.clone(),
                    step_id: None,
                    severity: EscalationSeverity::Info,
                    reason: reason.clone(),
                    what_i_need: questions_text,
                    options: vec![],
                    created_at: crud::epoch_secs(),
                };

                // Set status to Clarifying and escalate.
                session.status = SessionStatus::Clarifying;
                session.escalation = Some(msg.clone());
                session.updated_at = crud::epoch_secs();
                crud::checkpoint_session(&self.pool, session).await?;

                // Record the questions as an agent message.
                let _ = crud::add_session_message(
                    &self.pool,
                    &session.id,
                    "agent_to_human",
                    &format!("{}\n\n{}", reason, questions.iter()
                        .enumerate()
                        .map(|(i, q)| format!("{}. {}", i + 1, q))
                        .collect::<Vec<_>>()
                        .join("\n")),
                    None,
                ).await;

                // Publish escalation event.
                self.event_bus.publish(Event {
                    event_type: EventType::SessionEscalation,
                    project_path: String::new(),
                    mr_iid: None,
                    user_id: None,
                    payload: serde_json::to_value(&msg).ok(),
                });
            }
        }

        Ok(())
    }

    // -----------------------------------------------------------------------
    // State: Executing — pick the next ready step and run the Generator
    // -----------------------------------------------------------------------

    async fn run_next_step(
        &self,
        session: &mut SessionState,
        workflow_name: &str,
    ) -> Result<()> {
        let plan = session
            .plan
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("session in Executing state but has no plan"))?;

        // Find the next step whose dependencies are all satisfied.
        let next_step = find_next_step(plan, &session.step_outputs);

        let step = match next_step {
            Some(s) => s.clone(),
            None => {
                // No more steps — all done or all blocked.
                if all_steps_complete(plan, &session.step_outputs) {
                    // Move to final evaluation.
                    session.status = SessionStatus::Evaluating;
                    session.current_step_id = None;
                    session.updated_at = crud::epoch_secs();
                    crud::checkpoint_session(&self.pool, session).await?;
                    return Ok(());
                }
                // Steps remain but none are ready — something is wrong.
                warn!(session_id = %session.id, "no executable steps but plan not complete");
                self.fail_session(session, "deadlocked — steps remain but none are ready")
                    .await?;
                return Ok(());
            }
        };

        // Reset step retry count when moving to a new step.
        if session.current_step_id.as_deref() != Some(&step.id) {
            session.step_retry_count = 0;
        }

        session.current_step_id = Some(step.id.clone());
        session.updated_at = crud::epoch_secs();
        crud::checkpoint_session(&self.pool, session).await?;

        self.publish_event(session, EventType::WorkflowStepStarted);

        // Build tool catalog for the generator.
        let tool_catalog = registry::build_tool_catalog(&self.agent_config);

        // Trace: tool call started.
        let step_start = std::time::Instant::now();
        if let Err(e) = crud::append_trace(
            &self.pool,
            &session.id,
            &TraceEventType::ToolCallStarted,
            Some(&step.id),
            step.tool.as_deref(),
            Some(&serde_json::json!({
                "description": step.description,
                "agent_type": step.agent_type.as_str(),
            })),
            None,
            None,
            None,
            None,
        ).await {
            warn!(session_id = %session.id, step_id = %step.id, error = %e, "failed to append ToolCallStarted trace");
        }

        // Run the Generator.
        let eval_feedback = session.evaluator_feedback.as_ref();
        let session_context = session.trigger_data.as_ref();
        let outcome = generator::execute_step(
            &step,
            &session.step_outputs,
            &self.mentor,
            &self.agent_config,
            &self.ai_config,
            &self.config.ai_model,
            eval_feedback,
            session_context,
            &tool_catalog,
        )
        .await
        .context("generator failed")?;

        let step_duration_ms = step_start.elapsed().as_millis() as i64;

        // Handle the outcome.
        match outcome {
            GeneratorOutcome::Success { output, .. } => {
                info!(session_id = %session.id, step_id = %step.id, "step succeeded");

                // Trace: tool call completed.
                if let Err(e) = crud::append_trace(
                    &self.pool,
                    &session.id,
                    &TraceEventType::ToolCallCompleted,
                    Some(&step.id),
                    step.tool.as_deref(),
                    None,
                    Some(&output),
                    None,
                    Some(step_duration_ms),
                    None,
                ).await {
                    warn!(session_id = %session.id, step_id = %step.id, error = %e, "failed to append ToolCallCompleted trace");
                }

                // Bug #7: Evaluate step output if the step has non-empty success criteria.
                if !step.success_criteria.is_empty() {
                    match evaluator::evaluate_step(
                        &self.ai_config,
                        &self.config.ai_model,
                        &step,
                        &output,
                        Some(self.config.eval_threshold),
                    )
                    .await
                    {
                        Ok(verdict) => {
                            // Trace: step evaluation run.
                            if let Err(e) = crud::append_trace(
                                &self.pool,
                                &session.id,
                                &TraceEventType::EvaluationRun,
                                Some(&step.id),
                                step.tool.as_deref(),
                                None,
                                Some(&serde_json::json!({
                                    "passed": verdict.passed,
                                    "score": verdict.score,
                                    "feedback": verdict.feedback,
                                })),
                                None,
                                None,
                                None,
                            ).await {
                                warn!(session_id = %session.id, step_id = %step.id, error = %e, "failed to append EvaluationRun trace");
                            }

                            if !verdict.passed {
                                info!(
                                    session_id = %session.id,
                                    step_id = %step.id,
                                    score = verdict.score,
                                    "step output failed evaluation"
                                );
                                session.step_retry_count += 1;

                                if session.step_retry_count >= self.config.max_step_retries {
                                    let msg = escalation::evaluation_failure_escalation(
                                        session,
                                        workflow_name,
                                        &step.id,
                                        session.step_retry_count,
                                        &verdict.feedback,
                                    );
                                    escalation::escalate(&self.pool, session, &self.event_bus, msg).await?;
                                } else {
                                    // Retry with evaluator feedback.
                                    session.evaluator_feedback = Some(verdict);
                                    session.updated_at = crud::epoch_secs();
                                    crud::checkpoint_session(&self.pool, session).await?;
                                }
                                self.publish_event(session, EventType::WorkflowStepFailed);
                                return Ok(());
                            }
                        }
                        Err(e) => {
                            // Evaluator failure is non-fatal — accept the output.
                            warn!(
                                session_id = %session.id,
                                step_id = %step.id,
                                "step evaluator failed, accepting output: {e:#}"
                            );
                        }
                    }
                }

                session.step_outputs.insert(step.id.clone(), output);
                session.evaluator_feedback = None;
                session.step_retry_count = 0;
                session.updated_at = crud::epoch_secs();
                crud::checkpoint_session(&self.pool, session).await?;
                self.publish_event(session, EventType::WorkflowStepCompleted);
                // Stay in Executing — the loop will pick the next step.
            }

            GeneratorOutcome::Failure { error } => {
                warn!(session_id = %session.id, step_id = %step.id, %error, "step failed");

                // Trace: tool call failed.
                if let Err(e) = crud::append_trace(
                    &self.pool,
                    &session.id,
                    &TraceEventType::ToolCallFailed,
                    Some(&step.id),
                    step.tool.as_deref(),
                    None,
                    None,
                    Some(&error),
                    Some(step_duration_ms),
                    None,
                ).await {
                    warn!(session_id = %session.id, step_id = %step.id, error = %e, "failed to append ToolCallFailed trace");
                }

                session.step_retry_count += 1;

                if session.step_retry_count >= self.config.max_step_retries {
                    // Escalate after max retries.
                    let msg = escalation::evaluation_failure_escalation(
                        session,
                        workflow_name,
                        &step.id,
                        session.step_retry_count,
                        &error,
                    );
                    escalation::escalate(&self.pool, session, &self.event_bus, msg).await?;
                } else {
                    // Retry — stay in Executing with the same current_step_id.
                    session.updated_at = crud::epoch_secs();
                    crud::checkpoint_session(&self.pool, session).await?;
                }
                self.publish_event(session, EventType::WorkflowStepFailed);
            }

            GeneratorOutcome::NeedsCapability {
                capability,
                description,
            } => {
                info!(
                    session_id = %session.id,
                    step_id = %step.id,
                    %capability,
                    "step needs capability — attempting connector lookup"
                );

                // Try to find or build a connector before escalating.
                match connector::find_or_build(
                    &self.ai_config,
                    &self.config.ai_model,
                    &self.mentor,
                    &capability,
                    &description,
                )
                .await
                {
                    Ok(connector::ConnectorResult::Found(spec))
                    | Ok(connector::ConnectorResult::Built(spec)) => {
                        info!(
                            session_id = %session.id,
                            step_id = %step.id,
                            connector = %spec.name,
                            "connector available — retrying step"
                        );
                        // Store the connector info in step_outputs so the agent
                        // can use it on retry.
                        let connector_info = serde_json::json!({
                            "connector_available": true,
                            "connector_name": spec.name,
                            "connector_base_url": spec.base_url,
                            "actions": spec.actions.keys().collect::<Vec<_>>(),
                        });
                        session.step_outputs.insert(
                            format!("_connector_{}", capability),
                            connector_info,
                        );
                        // Stay in Executing — the loop will retry the same step
                        // with the connector info now available in step_outputs.
                        session.updated_at = crud::epoch_secs();
                        crud::checkpoint_session(&self.pool, session).await?;
                    }
                    Ok(connector::ConnectorResult::NeedsHuman {
                        capability: cap,
                        reason,
                    }) => {
                        info!(
                            session_id = %session.id,
                            step_id = %step.id,
                            %cap,
                            %reason,
                            "connector build needs human help — escalating"
                        );
                        let msg = escalation::capability_escalation(
                            session,
                            workflow_name,
                            &step.id,
                            &cap,
                            &format!("{}\n\nConnector build failed: {}", description, reason),
                        );
                        escalation::escalate(&self.pool, session, &self.event_bus, msg).await?;
                    }
                    Err(e) => {
                        warn!(
                            session_id = %session.id,
                            step_id = %step.id,
                            %capability,
                            "connector lookup failed: {e:#} — escalating"
                        );
                        let msg = escalation::capability_escalation(
                            session,
                            workflow_name,
                            &step.id,
                            &capability,
                            &description,
                        );
                        escalation::escalate(&self.pool, session, &self.event_bus, msg).await?;
                    }
                }
            }

            GeneratorOutcome::NeedsHuman {
                reason,
                what_i_need,
                options,
            } => {
                info!(
                    session_id = %session.id,
                    step_id = %step.id,
                    %reason,
                    "step needs human input"
                );
                let msg = crate::types::workflow::EscalationMessage {
                    session_id: session.id,
                    workflow_name: workflow_name.to_string(),
                    step_id: Some(step.id.clone()),
                    severity: EscalationSeverity::Blocking,
                    reason,
                    what_i_need,
                    options,
                    created_at: crud::epoch_secs(),
                };
                escalation::escalate(&self.pool, session, &self.event_bus, msg).await?;
            }

            GeneratorOutcome::PlanModification {
                output,
                add_steps,
                remove_step_ids,
                reason,
            } => {
                info!(
                    session_id = %session.id,
                    step_id = %step.id,
                    add = add_steps.len(),
                    remove = remove_step_ids.len(),
                    %reason,
                    "step requested plan modification"
                );
                // Record the step output.
                session.step_outputs.insert(step.id.clone(), output);
                // Store the modification so run_replanning can apply it directly.
                session.pending_modification = Some(PendingPlanModification {
                    add_steps,
                    remove_step_ids,
                    reason,
                });
                session.status = SessionStatus::Adapting;
                session.updated_at = crud::epoch_secs();
                crud::checkpoint_session(&self.pool, session).await?;
            }
        }

        Ok(())
    }

    // -----------------------------------------------------------------------
    // State: Evaluating — run the Evaluator on the session outcome
    // -----------------------------------------------------------------------

    async fn run_evaluation(
        &self,
        session: &mut SessionState,
        workflow_name: &str,
    ) -> Result<()> {
        let plan = session
            .plan
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("session in Evaluating state but has no plan"))?;

        let step_summaries: Vec<(String, serde_json::Value)> = plan
            .steps
            .iter()
            .filter_map(|s| {
                session
                    .step_outputs
                    .get(&s.id)
                    .map(|o| (s.id.clone(), o.clone()))
            })
            .collect();

        let verdict = evaluator::evaluate_session(
            &self.ai_config,
            &self.config.ai_model,
            &plan.goal,
            &step_summaries,
            Some(self.config.eval_threshold),
        )
        .await
        .context("session evaluation failed")?;

        info!(
            session_id = %session.id,
            passed = verdict.passed,
            score = verdict.score,
            "session evaluation complete"
        );

        // Trace: session evaluation run.
        if let Err(e) = crud::append_trace(
            &self.pool,
            &session.id,
            &TraceEventType::EvaluationRun,
            None,
            None,
            None,
            Some(&serde_json::json!({
                "scope": "session",
                "passed": verdict.passed,
                "score": verdict.score,
                "feedback": verdict.feedback,
            })),
            None,
            None,
            None,
        ).await {
            warn!(session_id = %session.id, error = %e, "failed to append session EvaluationRun trace");
        }

        if verdict.passed {
            self.complete_session(session).await?;
        } else {
            session.evaluator_feedback = Some(verdict.clone());
            session.retry_count += 1;

            if session.retry_count >= self.config.max_session_retries {
                let msg = escalation::evaluation_failure_escalation(
                    session,
                    workflow_name,
                    "session",
                    session.retry_count,
                    &verdict.feedback,
                );
                escalation::escalate(&self.pool, session, &self.event_bus, msg).await?;
            } else {
                // Replan to try a different approach.
                session.status = SessionStatus::Adapting;
                session.updated_at = crud::epoch_secs();
                crud::checkpoint_session(&self.pool, session).await?;
            }
        }

        Ok(())
    }

    // -----------------------------------------------------------------------
    // State: Adapting — invoke the Planner to revise the plan
    // -----------------------------------------------------------------------

    async fn run_replanning(&self, session: &mut SessionState) -> Result<()> {
        let plan = session
            .plan
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("session in Adapting state but has no plan"))?
            .clone();

        // Bug #3: If there's a pending modification from the Generator, apply it
        // directly instead of calling the AI replanner.
        if let Some(modification) = session.pending_modification.take() {
            info!(
                session_id = %session.id,
                add = modification.add_steps.len(),
                remove = modification.remove_step_ids.len(),
                reason = %modification.reason,
                "applying pending plan modification directly"
            );

            let mut new_steps: Vec<PlanStep> = plan
                .steps
                .into_iter()
                .filter(|s| !modification.remove_step_ids.contains(&s.id))
                .collect();
            new_steps.extend(modification.add_steps);

            let revised_plan = SessionPlan {
                goal: plan.goal,
                steps: new_steps,
                capabilities_needed: plan.capabilities_needed,
            };

            session.plan = Some(revised_plan.clone());
            session.status = SessionStatus::Executing;
            session.evaluator_feedback = None;
            session.updated_at = crud::epoch_secs();

            // Clean up stale step_outputs — remove outputs for steps that no longer
            // exist in the revised plan. This prevents dead weight from accumulating
            // and avoids find_next_step incorrectly skipping reused step IDs.
            let valid_step_ids: std::collections::HashSet<&str> =
                revised_plan.steps.iter().map(|s| s.id.as_str()).collect();
            let stale_keys: Vec<String> = session
                .step_outputs
                .keys()
                .filter(|k| !valid_step_ids.contains(k.as_str()))
                .cloned()
                .collect();
            if !stale_keys.is_empty() {
                info!(
                    session_id = %session.id,
                    removed = stale_keys.len(),
                    "pruned stale step_outputs after replanning"
                );
                for key in &stale_keys {
                    session.step_outputs.remove(key);
                }
            }

            crud::checkpoint_session(&self.pool, session).await?;
            return Ok(());
        }

        // No pending modification — use the AI replanner (evaluation failure path).
        let completed: Vec<&str> = plan
            .steps
            .iter()
            .filter(|s| session.step_outputs.contains_key(&s.id))
            .map(|s| s.id.as_str())
            .collect();

        // Steps that were attempted but have no output are considered failed.
        let failed: Vec<(&str, &str)> = if let Some(ref feedback) = session.evaluator_feedback {
            vec![("session", feedback.feedback.as_str())]
        } else {
            vec![]
        };

        let new_context = session
            .evaluator_feedback
            .as_ref()
            .and_then(|f| f.suggestion.as_deref());

        let tool_catalog = registry::build_tool_catalog(&self.agent_config);

        let revised_plan = planner::replan(
            &self.ai_config,
            &self.config.ai_model,
            &self.mentor,
            &plan,
            &completed,
            &failed,
            new_context,
            &tool_catalog,
        )
        .await
        .context("replanning failed")?;

        info!(
            session_id = %session.id,
            new_steps = revised_plan.steps.len(),
            "plan revised"
        );

        session.plan = Some(revised_plan.clone());
        session.status = SessionStatus::Executing;
        session.evaluator_feedback = None;
        session.updated_at = crud::epoch_secs();

        // Clean up stale step_outputs from the AI-revised plan too.
        let valid_step_ids: std::collections::HashSet<&str> =
            revised_plan.steps.iter().map(|s| s.id.as_str()).collect();
        let stale_keys: Vec<String> = session
            .step_outputs
            .keys()
            .filter(|k| !valid_step_ids.contains(k.as_str()))
            .cloned()
            .collect();
        if !stale_keys.is_empty() {
            info!(
                session_id = %session.id,
                removed = stale_keys.len(),
                "pruned stale step_outputs after AI replanning"
            );
            for key in &stale_keys {
                session.step_outputs.remove(key);
            }
        }

        crud::checkpoint_session(&self.pool, session).await?;

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Terminal transitions
    // -----------------------------------------------------------------------

    async fn complete_session(&self, session: &mut SessionState) -> Result<()> {
        session.status = SessionStatus::Completed;
        session.completed_at = Some(crud::epoch_secs());
        session.updated_at = crud::epoch_secs();
        crud::checkpoint_session(&self.pool, session).await?;

        // Trace: session completed.
        if let Err(e) = crud::append_trace(
            &self.pool,
            &session.id,
            &TraceEventType::SessionCompleted,
            None,
            None,
            None,
            Some(&serde_json::json!({ "status": "completed" })),
            None,
            None,
            None,
        ).await {
            warn!(session_id = %session.id, error = %e, "failed to append SessionCompleted trace");
        }

        self.publish_event(session, EventType::WorkflowRunCompleted);

        info!(session_id = %session.id, "session completed successfully");
        Ok(())
    }

    async fn fail_session(&self, session: &mut SessionState, reason: &str) -> Result<()> {
        warn!(session_id = %session.id, %reason, "session failed");

        // Trace: session completed (failed).
        if let Err(e) = crud::append_trace(
            &self.pool,
            &session.id,
            &TraceEventType::SessionCompleted,
            None,
            None,
            None,
            None,
            Some(reason),
            None,
            Some(&serde_json::json!({ "status": "failed" })),
        ).await {
            warn!(session_id = %session.id, error = %e, "failed to append SessionCompleted (failed) trace");
        }

        session.status = SessionStatus::Failed;
        session.completed_at = Some(crud::epoch_secs());
        session.updated_at = crud::epoch_secs();
        crud::checkpoint_session(&self.pool, session).await?;

        self.publish_event(session, EventType::WorkflowRunCompleted);
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    async fn load_workflow_description(&self, session: &SessionState) -> Result<String> {
        let wf = crud::get_workflow(&self.pool, &session.workflow_id.to_string())
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!("workflow {} not found", session.workflow_id)
            })?;
        Ok(wf.description)
    }

    fn publish_event(&self, session: &SessionState, event_type: EventType) {
        let payload = serde_json::json!({
            "session_id": session.id.to_string(),
            "status": session.status.as_str(),
            "current_step_id": session.current_step_id,
        });
        self.event_bus.publish(Event {
            event_type,
            project_path: String::new(),
            mr_iid: None,
            user_id: None,
            payload: Some(payload),
        });
    }
}

// ---------------------------------------------------------------------------
// Session creation helper
// ---------------------------------------------------------------------------

/// Create a new session for a workflow trigger. Persists to SQLite and returns
/// the initial `SessionState` ready to be driven.
pub async fn create_session(
    pool: &SqlitePool,
    workflow_id: Uuid,
    trigger_type: &str,
    trigger_data: Option<serde_json::Value>,
) -> Result<SessionState> {
    let now = crud::epoch_secs();
    let session = SessionState {
        id: Uuid::new_v4(),
        workflow_id,
        status: SessionStatus::Created,
        trigger_type: trigger_type.to_string(),
        trigger_data,
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
    };

    crud::create_session(pool, &session).await?;
    info!(session_id = %session.id, workflow_id = %workflow_id, "new session created");
    Ok(session)
}

/// Resume all non-terminal sessions after a crash/restart.
pub async fn resume_sessions(
    pool: &SqlitePool,
    manager: &SessionManager,
) -> Result<Vec<Uuid>> {
    let sessions = crud::load_resumable_sessions(pool).await?;
    let count = sessions.len();

    if count == 0 {
        debug!("no sessions to resume");
        return Ok(vec![]);
    }

    info!(count, "resuming sessions after restart");

    let mut resumed = Vec::new();
    for mut session in sessions {
        let sid = session.id;
        // Load workflow name for escalation messages.
        let wf_name = crud::get_workflow(pool, &session.workflow_id.to_string())
            .await
            .ok()
            .flatten()
            .map(|w| w.name)
            .unwrap_or_else(|| "unknown".into());

        // Bug #4 & #5: Re-emit escalation event for WaitingForHuman/Clarifying sessions
        // so that listeners (WebSocket, notifications) are aware of the pending
        // escalation even if the original event was lost in a crash.
        if session.status == SessionStatus::WaitingForHuman
            || session.status == SessionStatus::Clarifying
        {
            if let Some(ref esc) = session.escalation {
                info!(
                    session_id = %sid,
                    reason = %esc.reason,
                    "re-emitting escalation event for recovered WaitingForHuman session"
                );
                let payload = serde_json::to_value(esc).ok();
                manager.event_bus.publish(Event {
                    event_type: EventType::SessionEscalation,
                    project_path: String::new(),
                    mr_iid: None,
                    user_id: None,
                    payload,
                });
            } else {
                warn!(
                    session_id = %sid,
                    "WaitingForHuman session has no escalation data, cannot re-notify"
                );
            }
            // Don't call drive() — it would just return immediately.
            resumed.push(sid);
            debug!(session_id = %sid, status = %session.status, "session resumed (re-notified)");
            continue;
        }

        match manager.drive(&mut session, &wf_name).await {
            Ok(()) => {
                resumed.push(sid);
                debug!(session_id = %sid, status = %session.status, "session resumed");
            }
            Err(e) => {
                warn!(session_id = %sid, "failed to resume session: {e:#}");
            }
        }
    }

    info!(
        total = count,
        resumed = resumed.len(),
        "session recovery complete"
    );
    Ok(resumed)
}

// ---------------------------------------------------------------------------
// Plan navigation helpers
// ---------------------------------------------------------------------------

/// Find the next step in the plan whose dependencies are all satisfied.
fn find_next_step<'a>(
    plan: &'a SessionPlan,
    step_outputs: &HashMap<String, serde_json::Value>,
) -> Option<&'a PlanStep> {
    plan.steps.iter().find(|step| {
        // Skip already-completed steps.
        if step_outputs.contains_key(&step.id) {
            return false;
        }
        // All dependencies must be satisfied.
        step.depends_on
            .iter()
            .all(|dep| step_outputs.contains_key(dep))
    })
}

/// Check if all steps in the plan have outputs.
fn all_steps_complete(
    plan: &SessionPlan,
    step_outputs: &HashMap<String, serde_json::Value>,
) -> bool {
    plan.steps.iter().all(|s| step_outputs.contains_key(&s.id))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::workflow::AgentType;

    fn step(id: &str, deps: &[&str]) -> PlanStep {
        PlanStep {
            id: id.into(),
            description: format!("step {id}"),
            agent_type: AgentType::Ai,
            success_criteria: "done".into(),
            depends_on: deps.iter().map(|s| s.to_string()).collect(),
            capabilities_needed: vec![],
            tool: None,
        }
    }

    fn plan(steps: Vec<PlanStep>) -> SessionPlan {
        SessionPlan {
            goal: "test goal".into(),
            steps,
            capabilities_needed: vec![],
        }
    }

    // -- find_next_step ------------------------------------------------------

    #[test]
    fn find_next_step_first_with_no_deps() {
        let p = plan(vec![step("a", &[]), step("b", &["a"])]);
        let outputs = HashMap::new();
        let next = find_next_step(&p, &outputs);
        assert_eq!(next.unwrap().id, "a");
    }

    #[test]
    fn find_next_step_skips_completed() {
        let p = plan(vec![step("a", &[]), step("b", &["a"])]);
        let mut outputs = HashMap::new();
        outputs.insert("a".into(), serde_json::json!({}));
        let next = find_next_step(&p, &outputs);
        assert_eq!(next.unwrap().id, "b");
    }

    #[test]
    fn find_next_step_none_when_blocked() {
        let p = plan(vec![step("a", &["b"]), step("b", &["a"])]);
        let outputs = HashMap::new();
        // Both steps depend on each other — neither is ready.
        // (This shouldn't happen with cycle detection, but tests the logic.)
        assert!(find_next_step(&p, &outputs).is_none());
    }

    #[test]
    fn find_next_step_none_when_all_done() {
        let p = plan(vec![step("a", &[])]);
        let mut outputs = HashMap::new();
        outputs.insert("a".into(), serde_json::json!({}));
        assert!(find_next_step(&p, &outputs).is_none());
    }

    // -- all_steps_complete --------------------------------------------------

    #[test]
    fn all_complete_true() {
        let p = plan(vec![step("a", &[]), step("b", &[])]);
        let mut outputs = HashMap::new();
        outputs.insert("a".into(), serde_json::json!({}));
        outputs.insert("b".into(), serde_json::json!({}));
        assert!(all_steps_complete(&p, &outputs));
    }

    #[test]
    fn all_complete_false() {
        let p = plan(vec![step("a", &[]), step("b", &[])]);
        let mut outputs = HashMap::new();
        outputs.insert("a".into(), serde_json::json!({}));
        assert!(!all_steps_complete(&p, &outputs));
    }

    #[test]
    fn all_complete_empty_plan() {
        let p = plan(vec![]);
        let outputs = HashMap::new();
        assert!(all_steps_complete(&p, &outputs));
    }

    // -- create_session (DB-backed) ------------------------------------------

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

    #[tokio::test]
    async fn create_session_persists_and_returns_correct_state() {
        let pool = crate::db::test_pool().await;
        let workflow_id = seed_workflow(&pool).await;
        let trigger_data = serde_json::json!({"mr_iid": 42});

        let session =
            create_session(&pool, workflow_id, "event", Some(trigger_data.clone()))
                .await
                .unwrap();

        assert_eq!(session.workflow_id, workflow_id);
        assert_eq!(session.status, SessionStatus::Created);
        assert_eq!(session.trigger_type, "event");
        assert_eq!(session.trigger_data, Some(trigger_data));
        assert!(session.plan.is_none());
        assert!(session.step_outputs.is_empty());
        assert_eq!(session.retry_count, 0);
        assert!(session.completed_at.is_none());

        // Verify it's in the DB.
        let loaded = crud::load_session(&pool, &session.id.to_string())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded.id, session.id);
        assert_eq!(loaded.status, SessionStatus::Created);
    }

    #[tokio::test]
    async fn create_session_with_no_trigger_data() {
        let pool = crate::db::test_pool().await;
        let workflow_id = seed_workflow(&pool).await;
        let session =
            create_session(&pool, workflow_id, "cron", None).await.unwrap();

        assert_eq!(session.trigger_type, "cron");
        assert!(session.trigger_data.is_none());

        let loaded = crud::load_session(&pool, &session.id.to_string())
            .await
            .unwrap()
            .unwrap();
        assert!(loaded.trigger_data.is_none());
    }

    // -- SessionStatus properties --------------------------------------------

    #[test]
    fn terminal_states_are_correct() {
        assert!(SessionStatus::Completed.is_terminal());
        assert!(SessionStatus::Failed.is_terminal());
        assert!(SessionStatus::Cancelled.is_terminal());

        assert!(!SessionStatus::Created.is_terminal());
        assert!(!SessionStatus::Planning.is_terminal());
        assert!(!SessionStatus::Executing.is_terminal());
        assert!(!SessionStatus::Evaluating.is_terminal());
        assert!(!SessionStatus::Adapting.is_terminal());
        assert!(!SessionStatus::WaitingForHuman.is_terminal());
        assert!(!SessionStatus::Clarifying.is_terminal());
    }

    #[test]
    fn session_status_display_roundtrips() {
        let statuses = [
            SessionStatus::Created,
            SessionStatus::Planning,
            SessionStatus::Executing,
            SessionStatus::Evaluating,
            SessionStatus::Adapting,
            SessionStatus::WaitingForHuman,
            SessionStatus::Clarifying,
            SessionStatus::Completed,
            SessionStatus::Failed,
            SessionStatus::Cancelled,
        ];
        for status in &statuses {
            let s = status.as_str();
            assert_eq!(
                crud::parse_session_status(s),
                *status,
                "roundtrip failed for {s}"
            );
        }
    }
}
