// ---------------------------------------------------------------------------
// Workflow CRUD — database queries for workflow definitions and runs.
//
// All operations go through SQLite. Workflow definitions are stored as JSON
// blobs with indexed metadata columns. Runs are stored with step_states as
// JSON for flexible schema evolution.
// ---------------------------------------------------------------------------

use anyhow::{Context, Result};
use sqlx::SqlitePool;
use tracing::{debug, warn};

use crate::types::workflow::{
    EscalationMessage, EvaluatorVerdict, MessageDirection, RunStatus, SessionMessage, SessionPlan,
    SessionState, SessionStatus, WorkflowDefinition, WorkflowRun,
};
use std::collections::HashMap;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Workflow Definitions
// ---------------------------------------------------------------------------

/// Create a new workflow definition.
pub async fn create_workflow(pool: &SqlitePool, workflow: &WorkflowDefinition) -> Result<()> {
    let definition_json =
        serde_json::to_string(workflow).context("serialize workflow definition")?;
    let now = epoch_secs();

    sqlx::query(
        "INSERT INTO workflows (id, name, description, project_id, definition, enabled, created_by, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(workflow.id.to_string())
    .bind(&workflow.name)
    .bind(&workflow.description)
    .bind(workflow.project_id)
    .bind(&definition_json)
    .bind(workflow.enabled as i32)
    .bind(&workflow.created_by)
    .bind(now)
    .bind(now)
    .execute(pool)
    .await
    .context("insert workflow")?;

    debug!(id = %workflow.id, name = %workflow.name, "workflow created");
    Ok(())
}

/// Get a workflow definition by ID.
pub async fn get_workflow(pool: &SqlitePool, id: &str) -> Result<Option<WorkflowDefinition>> {
    let row: Option<(String,)> = sqlx::query_as(
        "SELECT definition FROM workflows WHERE id = ?",
    )
    .bind(id)
    .fetch_optional(pool)
    .await
    .context("fetch workflow")?;

    match row {
        Some((json,)) => {
            let wf = serde_json::from_str(&json).context("parse workflow definition")?;
            Ok(Some(wf))
        }
        None => Ok(None),
    }
}

/// List all workflows for a project.
pub async fn list_workflows_for_project(
    pool: &SqlitePool,
    project_id: i64,
) -> Result<Vec<WorkflowDefinition>> {
    let rows: Vec<(String,)> = sqlx::query_as(
        "SELECT definition FROM workflows WHERE project_id = ? ORDER BY created_at DESC",
    )
    .bind(project_id)
    .fetch_all(pool)
    .await
    .context("list workflows")?;

    parse_workflow_rows(rows)
}

/// List all workflows (enabled and disabled).
pub async fn list_all_workflows(pool: &SqlitePool) -> Result<Vec<WorkflowDefinition>> {
    let rows: Vec<(String,)> = sqlx::query_as(
        "SELECT definition FROM workflows ORDER BY created_at DESC",
    )
    .fetch_all(pool)
    .await
    .context("list all workflows")?;

    parse_workflow_rows(rows)
}

/// List all enabled workflows.
pub async fn list_enabled_workflows(pool: &SqlitePool) -> Result<Vec<WorkflowDefinition>> {
    let rows: Vec<(String,)> = sqlx::query_as(
        "SELECT definition FROM workflows WHERE enabled = 1 ORDER BY created_at DESC",
    )
    .fetch_all(pool)
    .await
    .context("list enabled workflows")?;

    parse_workflow_rows(rows)
}

/// Update a workflow definition (full replace).
pub async fn update_workflow(pool: &SqlitePool, workflow: &WorkflowDefinition) -> Result<bool> {
    let definition_json =
        serde_json::to_string(workflow).context("serialize workflow definition")?;
    let now = epoch_secs();

    let result = sqlx::query(
        "UPDATE workflows SET name = ?, description = ?, definition = ?, enabled = ?, updated_at = ?
         WHERE id = ?",
    )
    .bind(&workflow.name)
    .bind(&workflow.description)
    .bind(&definition_json)
    .bind(workflow.enabled as i32)
    .bind(now)
    .bind(workflow.id.to_string())
    .execute(pool)
    .await
    .context("update workflow")?;

    Ok(result.rows_affected() > 0)
}

/// Enable or disable a workflow.
pub async fn set_workflow_enabled(pool: &SqlitePool, id: &str, enabled: bool) -> Result<bool> {
    let now = epoch_secs();
    let result = sqlx::query(
        "UPDATE workflows SET enabled = ?, updated_at = ? WHERE id = ?",
    )
    .bind(enabled as i32)
    .bind(now)
    .bind(id)
    .execute(pool)
    .await
    .context("set workflow enabled")?;

    Ok(result.rows_affected() > 0)
}

/// Delete a workflow definition and all its runs.
pub async fn delete_workflow(pool: &SqlitePool, id: &str) -> Result<bool> {
    let mut tx = pool.begin().await.context("begin tx")?;

    // Delete run logs first (FK constraint).
    sqlx::query(
        "DELETE FROM workflow_run_log WHERE run_id IN (SELECT id FROM workflow_runs WHERE workflow_id = ?)",
    )
    .bind(id)
    .execute(&mut *tx)
    .await
    .context("delete run logs")?;

    // Delete runs.
    sqlx::query("DELETE FROM workflow_runs WHERE workflow_id = ?")
        .bind(id)
        .execute(&mut *tx)
        .await
        .context("delete runs")?;

    // Delete the workflow.
    let result = sqlx::query("DELETE FROM workflows WHERE id = ?")
        .bind(id)
        .execute(&mut *tx)
        .await
        .context("delete workflow")?;

    tx.commit().await.context("commit tx")?;

    Ok(result.rows_affected() > 0)
}

// ---------------------------------------------------------------------------
// Workflow Runs
// ---------------------------------------------------------------------------

/// Get a workflow run by ID.
pub async fn get_run(pool: &SqlitePool, run_id: &str) -> Result<Option<WorkflowRun>> {
    let row: Option<(String, String, String, Option<String>, String, String, Option<String>, i64, Option<i64>)> =
        sqlx::query_as(
            "SELECT id, workflow_id, trigger_type, trigger_data, status, step_states, final_verification, started_at, completed_at
             FROM workflow_runs WHERE id = ?",
        )
        .bind(run_id)
        .fetch_optional(pool)
        .await
        .context("fetch run")?;

    match row {
        Some(r) => Ok(Some(parse_run_row(r)?)),
        None => Ok(None),
    }
}

/// List runs for a workflow, most recent first.
pub async fn list_runs_for_workflow(
    pool: &SqlitePool,
    workflow_id: &str,
    limit: u32,
) -> Result<Vec<WorkflowRun>> {
    let rows: Vec<(String, String, String, Option<String>, String, String, Option<String>, i64, Option<i64>)> =
        sqlx::query_as(
            "SELECT id, workflow_id, trigger_type, trigger_data, status, step_states, final_verification, started_at, completed_at
             FROM workflow_runs WHERE workflow_id = ?
             ORDER BY started_at DESC LIMIT ?",
        )
        .bind(workflow_id)
        .bind(limit)
        .fetch_all(pool)
        .await
        .context("list runs")?;

    rows.into_iter().map(parse_run_row).collect()
}

/// List active (non-terminal) runs.
pub async fn list_active_runs(pool: &SqlitePool) -> Result<Vec<WorkflowRun>> {
    let rows: Vec<(String, String, String, Option<String>, String, String, Option<String>, i64, Option<i64>)> =
        sqlx::query_as(
            "SELECT id, workflow_id, trigger_type, trigger_data, status, step_states, final_verification, started_at, completed_at
             FROM workflow_runs WHERE status IN ('pending', 'running')
             ORDER BY started_at ASC",
        )
        .fetch_all(pool)
        .await
        .context("list active runs")?;

    rows.into_iter().map(parse_run_row).collect()
}

/// Append a log entry for a workflow run.
pub async fn append_run_log(
    pool: &SqlitePool,
    run_id: &str,
    step_id: Option<&str>,
    event_type: &str,
    data: Option<&str>,
) -> Result<()> {
    let now = epoch_secs();
    sqlx::query(
        "INSERT INTO workflow_run_log (run_id, step_id, event_type, data, created_at)
         VALUES (?, ?, ?, ?, ?)",
    )
    .bind(run_id)
    .bind(step_id)
    .bind(event_type)
    .bind(data)
    .bind(now)
    .execute(pool)
    .await
    .context("append run log")?;

    Ok(())
}

/// Get log entries for a run.
pub async fn get_run_log(
    pool: &SqlitePool,
    run_id: &str,
) -> Result<Vec<(Option<String>, String, Option<String>, i64)>> {
    let rows: Vec<(Option<String>, String, Option<String>, i64)> = sqlx::query_as(
        "SELECT step_id, event_type, data, created_at
         FROM workflow_run_log WHERE run_id = ?
         ORDER BY created_at ASC",
    )
    .bind(run_id)
    .fetch_all(pool)
    .await
    .context("get run log")?;

    Ok(rows)
}

/// Count runs for a workflow by status.
pub async fn count_runs(
    pool: &SqlitePool,
    workflow_id: &str,
    status: Option<&str>,
) -> Result<i64> {
    let count: (i64,) = if let Some(status) = status {
        sqlx::query_as(
            "SELECT COUNT(*) FROM workflow_runs WHERE workflow_id = ? AND status = ?",
        )
        .bind(workflow_id)
        .bind(status)
        .fetch_one(pool)
        .await?
    } else {
        sqlx::query_as(
            "SELECT COUNT(*) FROM workflow_runs WHERE workflow_id = ?",
        )
        .bind(workflow_id)
        .fetch_one(pool)
        .await?
    };
    Ok(count.0)
}

// ---------------------------------------------------------------------------
// Workflow Sessions (v2 orchestrator)
// ---------------------------------------------------------------------------

/// Insert a new session into the database.
pub async fn create_session(pool: &SqlitePool, session: &SessionState) -> Result<()> {
    let plan_json = session
        .plan
        .as_ref()
        .map(|p| serde_json::to_string(p))
        .transpose()
        .context("serialize session plan")?;
    let step_outputs_json =
        serde_json::to_string(&session.step_outputs).context("serialize step_outputs")?;
    let trigger_data_json = session
        .trigger_data
        .as_ref()
        .map(|d| serde_json::to_string(d))
        .transpose()
        .context("serialize trigger_data")?;
    let evaluator_json = session
        .evaluator_feedback
        .as_ref()
        .map(|e| serde_json::to_string(e))
        .transpose()
        .context("serialize evaluator_feedback")?;
    let escalation_json = session
        .escalation
        .as_ref()
        .map(|e| serde_json::to_string(e))
        .transpose()
        .context("serialize escalation")?;

    sqlx::query(
        "INSERT INTO workflow_sessions
         (id, workflow_id, status, trigger_type, trigger_data, plan, step_outputs,
          current_step_id, retry_count, max_retries, evaluator_feedback, escalation,
          started_at, completed_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(session.id.to_string())
    .bind(session.workflow_id.to_string())
    .bind(session.status.as_str())
    .bind(&session.trigger_type)
    .bind(&trigger_data_json)
    .bind(&plan_json)
    .bind(&step_outputs_json)
    .bind(&session.current_step_id)
    .bind(session.retry_count as i64)
    .bind(session.max_retries as i64)
    .bind(&evaluator_json)
    .bind(&escalation_json)
    .bind(session.started_at)
    .bind(session.completed_at)
    .bind(session.updated_at)
    .execute(pool)
    .await
    .context("insert session")?;

    tracing::info!(id = %session.id, workflow_id = %session.workflow_id, "session created");
    Ok(())
}

/// Checkpoint mutable session fields after a state transition.
/// Only updates the columns that change — not the whole row.
pub async fn checkpoint_session(pool: &SqlitePool, session: &SessionState) -> Result<()> {
    let plan_json = session
        .plan
        .as_ref()
        .map(|p| serde_json::to_string(p))
        .transpose()
        .context("serialize session plan")?;
    let step_outputs_json =
        serde_json::to_string(&session.step_outputs).context("serialize step_outputs")?;
    let evaluator_json = session
        .evaluator_feedback
        .as_ref()
        .map(|e| serde_json::to_string(e))
        .transpose()
        .context("serialize evaluator_feedback")?;
    let escalation_json = session
        .escalation
        .as_ref()
        .map(|e| serde_json::to_string(e))
        .transpose()
        .context("serialize escalation")?;

    sqlx::query(
        "UPDATE workflow_sessions
         SET status = ?, plan = ?, step_outputs = ?, current_step_id = ?,
             retry_count = ?, evaluator_feedback = ?, escalation = ?,
             completed_at = ?, updated_at = ?
         WHERE id = ?",
    )
    .bind(session.status.as_str())
    .bind(&plan_json)
    .bind(&step_outputs_json)
    .bind(&session.current_step_id)
    .bind(session.retry_count as i64)
    .bind(&evaluator_json)
    .bind(&escalation_json)
    .bind(session.completed_at)
    .bind(session.updated_at)
    .bind(session.id.to_string())
    .execute(pool)
    .await
    .context("checkpoint session")?;

    debug!(id = %session.id, status = %session.status, "session checkpointed");
    Ok(())
}

/// Load a session by ID, parsing all JSON fields.
pub async fn load_session(pool: &SqlitePool, id: &str) -> Result<Option<SessionState>> {
    let row: Option<(
        String,         // id
        String,         // workflow_id
        String,         // status
        String,         // trigger_type
        Option<String>, // trigger_data
        Option<String>, // plan
        String,         // step_outputs
        Option<String>, // current_step_id
        i64,            // retry_count
        i64,            // max_retries
        Option<String>, // evaluator_feedback
        Option<String>, // escalation
        i64,            // started_at
        Option<i64>,    // completed_at
        i64,            // updated_at
    )> = sqlx::query_as(
        "SELECT id, workflow_id, status, trigger_type, trigger_data, plan, step_outputs,
                current_step_id, retry_count, max_retries, evaluator_feedback, escalation,
                started_at, completed_at, updated_at
         FROM workflow_sessions WHERE id = ?",
    )
    .bind(id)
    .fetch_optional(pool)
    .await
    .context("fetch session")?;

    match row {
        Some(r) => Ok(Some(parse_session_row(r)?)),
        None => Ok(None),
    }
}

/// Load all non-terminal sessions for crash recovery on startup.
pub async fn load_resumable_sessions(pool: &SqlitePool) -> Result<Vec<SessionState>> {
    let rows: Vec<(
        String, String, String, String, Option<String>, Option<String>, String,
        Option<String>, i64, i64, Option<String>, Option<String>, i64, Option<i64>, i64,
    )> = sqlx::query_as(
        "SELECT id, workflow_id, status, trigger_type, trigger_data, plan, step_outputs,
                current_step_id, retry_count, max_retries, evaluator_feedback, escalation,
                started_at, completed_at, updated_at
         FROM workflow_sessions
         WHERE status NOT IN ('completed', 'failed', 'cancelled')
         ORDER BY started_at ASC",
    )
    .fetch_all(pool)
    .await
    .context("load resumable sessions")?;

    let count = rows.len();
    let sessions: Vec<SessionState> = rows
        .into_iter()
        .filter_map(|r| match parse_session_row(r) {
            Ok(s) => Some(s),
            Err(e) => {
                warn!("skipping corrupt session row: {e:#}");
                None
            }
        })
        .collect();

    tracing::info!(total = count, loaded = sessions.len(), "resumable sessions loaded");
    Ok(sessions)
}

/// Load all non-terminal (active) sessions.
pub async fn load_active_sessions(pool: &SqlitePool) -> Result<Vec<SessionState>> {
    let rows: Vec<(
        String, String, String, String, Option<String>, Option<String>, String,
        Option<String>, i64, i64, Option<String>, Option<String>, i64, Option<i64>, i64,
    )> = sqlx::query_as(
        "SELECT id, workflow_id, status, trigger_type, trigger_data, plan, step_outputs,
                current_step_id, retry_count, max_retries, evaluator_feedback, escalation,
                started_at, completed_at, updated_at
         FROM workflow_sessions
         WHERE status NOT IN ('completed', 'failed', 'cancelled')
         ORDER BY updated_at DESC",
    )
    .fetch_all(pool)
    .await
    .context("load active sessions")?;

    rows.into_iter().map(parse_session_row).collect()
}

/// Load recent terminal sessions (completed/failed/cancelled).
pub async fn load_recent_sessions(pool: &SqlitePool, limit: u32) -> Result<Vec<SessionState>> {
    let rows: Vec<(
        String, String, String, String, Option<String>, Option<String>, String,
        Option<String>, i64, i64, Option<String>, Option<String>, i64, Option<i64>, i64,
    )> = sqlx::query_as(
        "SELECT id, workflow_id, status, trigger_type, trigger_data, plan, step_outputs,
                current_step_id, retry_count, max_retries, evaluator_feedback, escalation,
                started_at, completed_at, updated_at
         FROM workflow_sessions
         WHERE status IN ('completed', 'failed', 'cancelled')
         ORDER BY completed_at DESC
         LIMIT ?",
    )
    .bind(limit)
    .fetch_all(pool)
    .await
    .context("load recent sessions")?;

    rows.into_iter().map(parse_session_row).collect()
}

/// Load sessions waiting for human input.
pub async fn load_waiting_sessions(pool: &SqlitePool) -> Result<Vec<SessionState>> {
    let rows: Vec<(
        String, String, String, String, Option<String>, Option<String>, String,
        Option<String>, i64, i64, Option<String>, Option<String>, i64, Option<i64>, i64,
    )> = sqlx::query_as(
        "SELECT id, workflow_id, status, trigger_type, trigger_data, plan, step_outputs,
                current_step_id, retry_count, max_retries, evaluator_feedback, escalation,
                started_at, completed_at, updated_at
         FROM workflow_sessions
         WHERE status = 'waiting_for_human'
         ORDER BY updated_at ASC",
    )
    .fetch_all(pool)
    .await
    .context("load waiting sessions")?;

    rows.into_iter().map(parse_session_row).collect()
}

/// Append a message to a session's conversation thread. Returns the row ID.
pub async fn add_session_message(
    pool: &SqlitePool,
    session_id: &Uuid,
    direction: &str,
    content: &str,
    metadata: Option<&serde_json::Value>,
) -> Result<i64> {
    let now = epoch_secs();
    let metadata_json = metadata
        .map(|m| serde_json::to_string(m))
        .transpose()
        .context("serialize message metadata")?;

    let result = sqlx::query(
        "INSERT INTO session_messages (session_id, direction, content, metadata, created_at)
         VALUES (?, ?, ?, ?, ?)",
    )
    .bind(session_id.to_string())
    .bind(direction)
    .bind(content)
    .bind(&metadata_json)
    .bind(now)
    .execute(pool)
    .await
    .context("insert session message")?;

    let row_id = result.last_insert_rowid();
    debug!(session_id = %session_id, direction, row_id, "session message added");
    Ok(row_id)
}

/// Load the conversation thread for a session, most recent last.
pub async fn load_session_messages(
    pool: &SqlitePool,
    session_id: &str,
    limit: i64,
) -> Result<Vec<SessionMessage>> {
    let rows: Vec<(i64, String, String, String, Option<String>, i64)> = sqlx::query_as(
        "SELECT id, session_id, direction, content, metadata, created_at
         FROM session_messages
         WHERE session_id = ?
         ORDER BY created_at ASC
         LIMIT ?",
    )
    .bind(session_id)
    .bind(limit)
    .fetch_all(pool)
    .await
    .context("load session messages")?;

    let mut messages = Vec::with_capacity(rows.len());
    for (id, sid, direction, content, metadata_json, created_at) in rows {
        let direction = match direction.as_str() {
            "agent_to_human" => MessageDirection::AgentToHuman,
            "human_to_agent" => MessageDirection::HumanToAgent,
            other => {
                warn!(session_id, direction = other, "unknown message direction, defaulting to agent_to_human");
                MessageDirection::AgentToHuman
            }
        };
        let metadata = metadata_json.and_then(|j| {
            serde_json::from_str(&j)
                .map_err(|e| {
                    warn!(session_id, "failed to parse message metadata: {e}");
                    e
                })
                .ok()
        });
        messages.push(SessionMessage {
            id,
            session_id: sid.parse().context("parse session_id in message")?,
            direction,
            content,
            metadata,
            created_at,
        });
    }
    Ok(messages)
}

// ---------------------------------------------------------------------------
// Session helpers
// ---------------------------------------------------------------------------

/// Parse a status string into a `SessionStatus`, warning on unknown values.
pub fn parse_session_status(s: &str) -> SessionStatus {
    match s {
        "created" => SessionStatus::Created,
        "planning" => SessionStatus::Planning,
        "executing" => SessionStatus::Executing,
        "evaluating" => SessionStatus::Evaluating,
        "adapting" => SessionStatus::Adapting,
        "waiting_for_human" => SessionStatus::WaitingForHuman,
        "completed" => SessionStatus::Completed,
        "failed" => SessionStatus::Failed,
        "cancelled" => SessionStatus::Cancelled,
        other => {
            warn!(status = other, "unknown session status, defaulting to Failed");
            SessionStatus::Failed
        }
    }
}

fn parse_session_row(
    row: (
        String, String, String, String, Option<String>, Option<String>, String,
        Option<String>, i64, i64, Option<String>, Option<String>, i64, Option<i64>, i64,
    ),
) -> Result<SessionState> {
    let (
        id, workflow_id, status, trigger_type, trigger_data_json, plan_json,
        step_outputs_json, current_step_id, retry_count, max_retries,
        evaluator_json, escalation_json, started_at, completed_at, updated_at,
    ) = row;

    let trigger_data = trigger_data_json.and_then(|j| {
        serde_json::from_str(&j)
            .map_err(|e| {
                warn!(session_id = %id, "failed to parse trigger_data: {e}");
                e
            })
            .ok()
    });

    let plan: Option<SessionPlan> = plan_json.and_then(|j| {
        serde_json::from_str(&j)
            .map_err(|e| {
                warn!(session_id = %id, "failed to parse session plan: {e}");
                e
            })
            .ok()
    });

    let step_outputs: HashMap<String, serde_json::Value> =
        serde_json::from_str(&step_outputs_json).unwrap_or_else(|e| {
            warn!(session_id = %id, "failed to parse step_outputs: {e}");
            HashMap::new()
        });

    let evaluator_feedback: Option<EvaluatorVerdict> = evaluator_json.and_then(|j| {
        serde_json::from_str(&j)
            .map_err(|e| {
                warn!(session_id = %id, "failed to parse evaluator_feedback: {e}");
                e
            })
            .ok()
    });

    let escalation: Option<EscalationMessage> = escalation_json.and_then(|j| {
        serde_json::from_str(&j)
            .map_err(|e| {
                warn!(session_id = %id, "failed to parse escalation: {e}");
                e
            })
            .ok()
    });

    Ok(SessionState {
        id: id.parse().context("parse session id")?,
        workflow_id: workflow_id.parse().context("parse session workflow_id")?,
        status: parse_session_status(&status),
        trigger_type,
        trigger_data,
        plan,
        step_outputs,
        current_step_id,
        retry_count: retry_count as u32,
        max_retries: max_retries as u32,
        step_retry_count: 0, // Not persisted separately; resets on load.
        evaluator_feedback,
        escalation,
        pending_modification: None, // Transient; rebuilt from session state on recovery.
        started_at,
        completed_at,
        updated_at,
    })
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn parse_workflow_rows(rows: Vec<(String,)>) -> Result<Vec<WorkflowDefinition>> {
    let mut workflows = Vec::with_capacity(rows.len());
    for (json,) in rows {
        let wf: WorkflowDefinition =
            serde_json::from_str(&json).context("parse workflow definition")?;
        workflows.push(wf);
    }
    Ok(workflows)
}

fn parse_run_row(
    row: (String, String, String, Option<String>, String, String, Option<String>, i64, Option<i64>),
) -> Result<WorkflowRun> {
    let (id, workflow_id, trigger_type, trigger_data, status, step_states_json, verification_json, started_at, completed_at) = row;

    let trigger = match (trigger_type.as_str(), trigger_data) {
        ("cron", _) => crate::types::workflow::TriggerSource::Cron {
            fired_at: chrono::DateTime::from_timestamp(started_at, 0)
                .unwrap_or_default(),
        },
        ("event", Some(data)) => {
            serde_json::from_str(&data).unwrap_or(crate::types::workflow::TriggerSource::Manual {
                user: "unknown".into(),
            })
        }
        ("manual", Some(data)) => {
            serde_json::from_str(&data).unwrap_or(crate::types::workflow::TriggerSource::Manual {
                user: "unknown".into(),
            })
        }
        _ => crate::types::workflow::TriggerSource::Manual {
            user: "unknown".into(),
        },
    };

    let run_status = match status.as_str() {
        "pending" => RunStatus::Pending,
        "running" => RunStatus::Running,
        "completed" => RunStatus::Completed,
        "failed" => RunStatus::Failed,
        "cancelled" => RunStatus::Cancelled,
        other => {
            warn!(
                run_id = %id,
                status = %other,
                "crud: unknown run status, defaulting to Failed"
            );
            RunStatus::Failed
        }
    };

    let step_states = serde_json::from_str(&step_states_json).unwrap_or_default();
    let final_verification = verification_json
        .and_then(|j| serde_json::from_str(&j).ok());

    Ok(WorkflowRun {
        id: id.parse().context("parse run id")?,
        workflow_id: workflow_id.parse().context("parse workflow id")?,
        trigger,
        status: run_status,
        step_states,
        started_at: chrono::DateTime::from_timestamp(started_at, 0)
            .unwrap_or_default(),
        completed_at: completed_at
            .and_then(|t| chrono::DateTime::from_timestamp(t, 0)),
        final_verification,
        mentor_queries: Vec::new(), // Not persisted per-row; available in run log.
    })
}

pub fn epoch_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db;
    use crate::types::workflow::{
        AgentType, EscalationMessage, EscalationOption, EscalationSeverity, EvaluatorVerdict,
        PlanStep, SessionPlan,
    };

    async fn test_pool() -> SqlitePool {
        db::test_pool().await
    }

    /// Create a workflow and return its ID — needed because sessions have a FK to workflows.
    async fn seed_workflow(pool: &SqlitePool) -> Uuid {
        let wf = make_workflow();
        create_workflow(pool, &wf).await.unwrap();
        wf.id
    }

    fn make_session_for(workflow_id: Uuid, status: SessionStatus) -> SessionState {
        let now = epoch_secs();
        SessionState {
            id: Uuid::new_v4(),
            workflow_id,
            status,
            trigger_type: "manual".into(),
            trigger_data: Some(serde_json::json!({"user": "test"})),
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

    fn make_workflow() -> crate::types::workflow::WorkflowDefinition {
        crate::types::workflow::WorkflowDefinition {
            id: Uuid::new_v4(),
            name: "test-workflow".into(),
            description: "A test workflow".into(),
            project_id: 42,
            steps: vec![],
            triggers: vec![],
            created_by: "tester".into(),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            enabled: true,
            mode: Default::default(),
        }
    }

    // -- Session CRUD --------------------------------------------------------

    #[tokio::test]
    async fn create_and_load_session() {
        let pool = test_pool().await;
        let wf_id = seed_workflow(&pool).await;
        let session = make_session_for(wf_id, SessionStatus::Created);
        let id = session.id;

        create_session(&pool, &session).await.unwrap();
        let loaded = load_session(&pool, &id.to_string()).await.unwrap().unwrap();

        assert_eq!(loaded.id, session.id);
        assert_eq!(loaded.workflow_id, session.workflow_id);
        assert_eq!(loaded.status, SessionStatus::Created);
        assert_eq!(loaded.trigger_type, "manual");
        assert_eq!(loaded.retry_count, 0);
        assert_eq!(loaded.max_retries, 3);
        assert!(loaded.plan.is_none());
        assert!(loaded.completed_at.is_none());
        assert_eq!(loaded.trigger_data, session.trigger_data);
    }

    #[tokio::test]
    async fn load_nonexistent_session_returns_none() {
        let pool = test_pool().await;
        let result = load_session(&pool, &Uuid::new_v4().to_string()).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn checkpoint_session_updates_mutable_fields() {
        let pool = test_pool().await;
        let wf_id = seed_workflow(&pool).await;
        let mut session = make_session_for(wf_id, SessionStatus::Created);
        create_session(&pool, &session).await.unwrap();

        // Mutate and checkpoint.
        let plan = SessionPlan {
            goal: "do the thing".into(),
            steps: vec![PlanStep {
                id: "step-1".into(),
                description: "first step".into(),
                agent_type: AgentType::Ai,
                success_criteria: "it works".into(),
                depends_on: vec![],
                capabilities_needed: vec![],
            }],
            capabilities_needed: vec!["gitlab".into()],
        };
        session.status = SessionStatus::Executing;
        session.plan = Some(plan);
        session.current_step_id = Some("step-1".into());
        session.retry_count = 2;
        session
            .step_outputs
            .insert("step-1".into(), serde_json::json!({"result": "ok"}));
        session.updated_at = epoch_secs();

        checkpoint_session(&pool, &session).await.unwrap();

        let loaded = load_session(&pool, &session.id.to_string())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded.status, SessionStatus::Executing);
        assert_eq!(loaded.plan.as_ref().unwrap().goal, "do the thing");
        assert_eq!(loaded.plan.as_ref().unwrap().steps.len(), 1);
        assert_eq!(loaded.current_step_id.as_deref(), Some("step-1"));
        assert_eq!(loaded.retry_count, 2);
        assert!(loaded.step_outputs.contains_key("step-1"));
    }

    #[tokio::test]
    async fn checkpoint_with_large_step_outputs() {
        let pool = test_pool().await;
        let wf_id = seed_workflow(&pool).await;
        let mut session = make_session_for(wf_id, SessionStatus::Executing);
        create_session(&pool, &session).await.unwrap();

        // Build a large step_outputs map (~100KB of JSON).
        for i in 0..100 {
            let big_value = serde_json::json!({
                "data": "x".repeat(1000),
                "index": i,
            });
            session.step_outputs.insert(format!("step-{i}"), big_value);
        }
        session.updated_at = epoch_secs();
        checkpoint_session(&pool, &session).await.unwrap();

        let loaded = load_session(&pool, &session.id.to_string())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded.step_outputs.len(), 100);
        assert!(loaded.step_outputs.contains_key("step-0"));
        assert!(loaded.step_outputs.contains_key("step-99"));
    }

    #[tokio::test]
    async fn checkpoint_with_evaluator_feedback_and_escalation() {
        let pool = test_pool().await;
        let wf_id = seed_workflow(&pool).await;
        let mut session = make_session_for(wf_id, SessionStatus::WaitingForHuman);
        create_session(&pool, &session).await.unwrap();

        session.evaluator_feedback = Some(EvaluatorVerdict {
            passed: false,
            score: 0.4,
            threshold: 0.8,
            feedback: "not good enough".into(),
            suggestion: Some("try harder".into()),
        });
        session.escalation = Some(EscalationMessage {
            session_id: session.id,
            workflow_name: "test-wf".into(),
            step_id: Some("step-1".into()),
            severity: EscalationSeverity::Blocking,
            reason: "step failed".into(),
            what_i_need: "help".into(),
            options: vec![EscalationOption {
                id: "retry".into(),
                label: "Retry".into(),
                description: Some("Try again".into()),
            }],
            created_at: epoch_secs(),
        });
        session.updated_at = epoch_secs();
        checkpoint_session(&pool, &session).await.unwrap();

        let loaded = load_session(&pool, &session.id.to_string())
            .await
            .unwrap()
            .unwrap();
        let fb = loaded.evaluator_feedback.unwrap();
        assert!(!fb.passed);
        assert!((fb.score - 0.4).abs() < f64::EPSILON);
        assert_eq!(fb.suggestion.as_deref(), Some("try harder"));

        let esc = loaded.escalation.unwrap();
        assert_eq!(esc.reason, "step failed");
        assert_eq!(esc.options.len(), 1);
        assert_eq!(esc.options[0].id, "retry");
    }

    #[tokio::test]
    async fn load_resumable_sessions_excludes_terminal() {
        let pool = test_pool().await;
        let wf_id = seed_workflow(&pool).await;

        let statuses = [
            SessionStatus::Created,
            SessionStatus::Planning,
            SessionStatus::Executing,
            SessionStatus::Evaluating,
            SessionStatus::Adapting,
            SessionStatus::WaitingForHuman,
            SessionStatus::Completed,
            SessionStatus::Failed,
            SessionStatus::Cancelled,
        ];

        for status in &statuses {
            let session = make_session_for(wf_id, status.clone());
            create_session(&pool, &session).await.unwrap();
        }

        let resumable = load_resumable_sessions(&pool).await.unwrap();
        // Terminal: Completed, Failed, Cancelled — should be excluded.
        assert_eq!(resumable.len(), 6);
        for s in &resumable {
            assert!(!s.status.is_terminal());
        }
    }

    #[tokio::test]
    async fn load_waiting_sessions_only_returns_waiting() {
        let pool = test_pool().await;
        let wf_id = seed_workflow(&pool).await;

        let statuses = [
            SessionStatus::Executing,
            SessionStatus::WaitingForHuman,
            SessionStatus::WaitingForHuman,
            SessionStatus::Completed,
        ];

        for status in &statuses {
            let session = make_session_for(wf_id, status.clone());
            create_session(&pool, &session).await.unwrap();
        }

        let waiting = load_waiting_sessions(&pool).await.unwrap();
        assert_eq!(waiting.len(), 2);
        for s in &waiting {
            assert_eq!(s.status, SessionStatus::WaitingForHuman);
        }
    }

    // -- Session messages ----------------------------------------------------

    #[tokio::test]
    async fn add_and_load_messages() {
        let pool = test_pool().await;
        let wf_id = seed_workflow(&pool).await;
        let session = make_session_for(wf_id, SessionStatus::Executing);
        create_session(&pool, &session).await.unwrap();

        add_session_message(&pool, &session.id, "agent_to_human", "Hello human", None)
            .await
            .unwrap();
        add_session_message(
            &pool,
            &session.id,
            "human_to_agent",
            "Hello agent",
            Some(&serde_json::json!({"chosen_option": "retry"})),
        )
        .await
        .unwrap();
        add_session_message(&pool, &session.id, "agent_to_human", "Retrying...", None)
            .await
            .unwrap();

        let messages = load_session_messages(&pool, &session.id.to_string(), 100)
            .await
            .unwrap();

        assert_eq!(messages.len(), 3);
        assert_eq!(messages[0].content, "Hello human");
        assert_eq!(messages[0].direction, MessageDirection::AgentToHuman);
        assert_eq!(messages[1].content, "Hello agent");
        assert_eq!(messages[1].direction, MessageDirection::HumanToAgent);
        assert!(messages[1].metadata.is_some());
        assert_eq!(messages[2].content, "Retrying...");
    }

    #[tokio::test]
    async fn load_messages_respects_limit() {
        let pool = test_pool().await;
        let wf_id = seed_workflow(&pool).await;
        let session = make_session_for(wf_id, SessionStatus::Executing);
        create_session(&pool, &session).await.unwrap();

        for i in 0..10 {
            add_session_message(
                &pool,
                &session.id,
                "agent_to_human",
                &format!("msg {i}"),
                None,
            )
            .await
            .unwrap();
        }

        let messages = load_session_messages(&pool, &session.id.to_string(), 3)
            .await
            .unwrap();
        assert_eq!(messages.len(), 3);
    }

    // -- parse_session_status ------------------------------------------------

    #[test]
    fn parse_session_status_all_variants() {
        assert_eq!(parse_session_status("created"), SessionStatus::Created);
        assert_eq!(parse_session_status("planning"), SessionStatus::Planning);
        assert_eq!(parse_session_status("executing"), SessionStatus::Executing);
        assert_eq!(
            parse_session_status("evaluating"),
            SessionStatus::Evaluating
        );
        assert_eq!(parse_session_status("adapting"), SessionStatus::Adapting);
        assert_eq!(
            parse_session_status("waiting_for_human"),
            SessionStatus::WaitingForHuman
        );
        assert_eq!(parse_session_status("completed"), SessionStatus::Completed);
        assert_eq!(parse_session_status("failed"), SessionStatus::Failed);
        assert_eq!(parse_session_status("cancelled"), SessionStatus::Cancelled);
    }

    #[test]
    fn parse_session_status_unknown_defaults_to_failed() {
        assert_eq!(parse_session_status("bogus"), SessionStatus::Failed);
    }

    // -- Workflow CRUD -------------------------------------------------------

    #[tokio::test]
    async fn create_and_get_workflow() {
        let pool = test_pool().await;
        let wf = make_workflow();
        create_workflow(&pool, &wf).await.unwrap();

        let loaded = get_workflow(&pool, &wf.id.to_string())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded.id, wf.id);
        assert_eq!(loaded.name, "test-workflow");
        assert_eq!(loaded.description, "A test workflow");
    }

    #[tokio::test]
    async fn get_nonexistent_workflow_returns_none() {
        let pool = test_pool().await;
        let result = get_workflow(&pool, &Uuid::new_v4().to_string())
            .await
            .unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn list_workflows_for_project_filters_correctly() {
        let pool = test_pool().await;

        let mut wf1 = make_workflow();
        wf1.project_id = 1;
        let mut wf2 = make_workflow();
        wf2.project_id = 1;
        let mut wf3 = make_workflow();
        wf3.project_id = 2;

        create_workflow(&pool, &wf1).await.unwrap();
        create_workflow(&pool, &wf2).await.unwrap();
        create_workflow(&pool, &wf3).await.unwrap();

        let list = list_workflows_for_project(&pool, 1).await.unwrap();
        assert_eq!(list.len(), 2);

        let list = list_workflows_for_project(&pool, 2).await.unwrap();
        assert_eq!(list.len(), 1);

        let list = list_workflows_for_project(&pool, 999).await.unwrap();
        assert!(list.is_empty());
    }

    #[tokio::test]
    async fn update_workflow_changes_fields() {
        let pool = test_pool().await;
        let mut wf = make_workflow();
        create_workflow(&pool, &wf).await.unwrap();

        wf.name = "updated-name".into();
        wf.description = "updated description".into();
        wf.enabled = false;
        let updated = update_workflow(&pool, &wf).await.unwrap();
        assert!(updated);

        let loaded = get_workflow(&pool, &wf.id.to_string())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded.name, "updated-name");
        assert_eq!(loaded.description, "updated description");
        assert!(!loaded.enabled);
    }

    #[tokio::test]
    async fn delete_workflow_removes_it() {
        let pool = test_pool().await;
        let wf = make_workflow();
        create_workflow(&pool, &wf).await.unwrap();

        let deleted = delete_workflow(&pool, &wf.id.to_string()).await.unwrap();
        assert!(deleted);

        let loaded = get_workflow(&pool, &wf.id.to_string()).await.unwrap();
        assert!(loaded.is_none());

        // Deleting again returns false.
        let deleted = delete_workflow(&pool, &wf.id.to_string()).await.unwrap();
        assert!(!deleted);
    }

    #[tokio::test]
    async fn set_workflow_enabled_toggle() {
        let pool = test_pool().await;
        let wf = make_workflow();
        create_workflow(&pool, &wf).await.unwrap();

        set_workflow_enabled(&pool, &wf.id.to_string(), false)
            .await
            .unwrap();
        let list = list_enabled_workflows(&pool).await.unwrap();
        assert!(list.iter().all(|w| w.id != wf.id));

        set_workflow_enabled(&pool, &wf.id.to_string(), true)
            .await
            .unwrap();
        let list = list_enabled_workflows(&pool).await.unwrap();
        assert!(list.iter().any(|w| w.id == wf.id));
    }
}
