// ---------------------------------------------------------------------------
// Directive CRUD — database queries for directives and work items.
//
// All operations go through SQLite. Directives are stored with JSON blob
// columns for sources, constraints, and escalation. Work items use a
// composite primary key (directive_id, external_id).
// ---------------------------------------------------------------------------

use anyhow::{Context, Result};
use sqlx::SqlitePool;
use tracing::{debug, warn};

use super::types::{
    Directive, DirectiveConstraints, DirectiveEscalation, DirectiveStatus, TrackedWorkItem,
    WorkItemStatus, WorkSource,
};
use crate::services::workflow::crud::epoch_secs;

// ---------------------------------------------------------------------------
// Directives
// ---------------------------------------------------------------------------

/// Create a new directive.
pub async fn create_directive(pool: &SqlitePool, directive: &Directive) -> Result<()> {
    let sources_json = serde_json::to_string(&directive.sources).context("serialize sources")?;
    let constraints_json =
        serde_json::to_string(&directive.constraints).context("serialize constraints")?;
    let escalation_json = directive
        .escalation
        .as_ref()
        .map(|e| serde_json::to_string(e))
        .transpose()
        .context("serialize escalation")?;
    let now = epoch_secs();

    sqlx::query(
        "INSERT INTO directives
         (id, name, intent, sources, constraints, priority, status,
          poll_interval_secs, last_poll_at, next_poll_at, escalation,
          created_by, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&directive.id)
    .bind(&directive.name)
    .bind(&directive.intent)
    .bind(&sources_json)
    .bind(&constraints_json)
    .bind(directive.priority)
    .bind(directive.status.as_str())
    .bind(directive.poll_interval_secs)
    .bind(directive.last_poll_at)
    .bind(directive.next_poll_at)
    .bind(&escalation_json)
    .bind(&directive.created_by)
    .bind(now)
    .bind(now)
    .execute(pool)
    .await
    .context("insert directive")?;

    debug!(id = %directive.id, name = %directive.name, "directive created");
    Ok(())
}

/// Load a directive by ID.
pub async fn load_directive(pool: &SqlitePool, id: &str) -> Result<Option<Directive>> {
    let row: Option<(
        String,         // id
        String,         // name
        String,         // intent
        String,         // sources
        String,         // constraints
        i32,            // priority
        String,         // status
        i64,            // poll_interval_secs
        Option<i64>,    // last_poll_at
        Option<i64>,    // next_poll_at
        Option<String>, // escalation
        Option<String>, // created_by
        i64,            // created_at
        i64,            // updated_at
    )> = sqlx::query_as(
        "SELECT id, name, intent, sources, constraints, priority, status,
                poll_interval_secs, last_poll_at, next_poll_at, escalation,
                created_by, created_at, updated_at
         FROM directives WHERE id = ?",
    )
    .bind(id)
    .fetch_optional(pool)
    .await
    .context("fetch directive")?;

    match row {
        Some(r) => Ok(Some(parse_directive_row(r)?)),
        None => Ok(None),
    }
}

/// List all non-retired directives.
pub async fn list_directives(pool: &SqlitePool) -> Result<Vec<Directive>> {
    let rows: Vec<(
        String, String, String, String, String, i32, String, i64,
        Option<i64>, Option<i64>, Option<String>, Option<String>, i64, i64,
    )> = sqlx::query_as(
        "SELECT id, name, intent, sources, constraints, priority, status,
                poll_interval_secs, last_poll_at, next_poll_at, escalation,
                created_by, created_at, updated_at
         FROM directives
         WHERE status != 'retired'
         ORDER BY priority ASC, created_at DESC",
    )
    .fetch_all(pool)
    .await
    .context("list directives")?;

    rows.into_iter().map(parse_directive_row).collect()
}

/// Load all active directives that are due for polling.
pub async fn load_active_directives(pool: &SqlitePool) -> Result<Vec<Directive>> {
    let now = epoch_secs();
    let rows: Vec<(
        String, String, String, String, String, i32, String, i64,
        Option<i64>, Option<i64>, Option<String>, Option<String>, i64, i64,
    )> = sqlx::query_as(
        "SELECT id, name, intent, sources, constraints, priority, status,
                poll_interval_secs, last_poll_at, next_poll_at, escalation,
                created_by, created_at, updated_at
         FROM directives
         WHERE status = 'active'
           AND (next_poll_at IS NULL OR next_poll_at <= ?)
         ORDER BY priority ASC",
    )
    .bind(now)
    .fetch_all(pool)
    .await
    .context("load active directives")?;

    rows.into_iter().map(parse_directive_row).collect()
}

/// Update a directive's mutable fields.
pub async fn update_directive(pool: &SqlitePool, directive: &Directive) -> Result<bool> {
    let sources_json = serde_json::to_string(&directive.sources).context("serialize sources")?;
    let constraints_json =
        serde_json::to_string(&directive.constraints).context("serialize constraints")?;
    let escalation_json = directive
        .escalation
        .as_ref()
        .map(|e| serde_json::to_string(e))
        .transpose()
        .context("serialize escalation")?;
    let now = epoch_secs();

    let result = sqlx::query(
        "UPDATE directives
         SET name = ?, intent = ?, sources = ?, constraints = ?, priority = ?,
             status = ?, poll_interval_secs = ?, last_poll_at = ?, next_poll_at = ?,
             escalation = ?, updated_at = ?
         WHERE id = ?",
    )
    .bind(&directive.name)
    .bind(&directive.intent)
    .bind(&sources_json)
    .bind(&constraints_json)
    .bind(directive.priority)
    .bind(directive.status.as_str())
    .bind(directive.poll_interval_secs)
    .bind(directive.last_poll_at)
    .bind(directive.next_poll_at)
    .bind(&escalation_json)
    .bind(now)
    .bind(&directive.id)
    .execute(pool)
    .await
    .context("update directive")?;

    Ok(result.rows_affected() > 0)
}

/// Retire a directive (soft delete).
pub async fn retire_directive(pool: &SqlitePool, id: &str) -> Result<bool> {
    let now = epoch_secs();
    let result = sqlx::query(
        "UPDATE directives SET status = 'retired', updated_at = ? WHERE id = ?",
    )
    .bind(now)
    .bind(id)
    .execute(pool)
    .await
    .context("retire directive")?;

    Ok(result.rows_affected() > 0)
}

// ---------------------------------------------------------------------------
// Work items
// ---------------------------------------------------------------------------

/// Track a new work item (INSERT OR IGNORE for dedup).
pub async fn track_work_item(
    pool: &SqlitePool,
    directive_id: &str,
    item: &super::types::WorkItem,
    status: &WorkItemStatus,
    triage_reason: Option<&str>,
    priority: i32,
) -> Result<bool> {
    let now = epoch_secs();
    let metadata_json =
        serde_json::to_string(&item.metadata).context("serialize work item metadata")?;

    let result = sqlx::query(
        "INSERT OR IGNORE INTO directive_work_items
         (directive_id, external_id, source_type, source_url, title, description,
          metadata, session_id, status, triage_reason, priority, discovered_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, ?)",
    )
    .bind(directive_id)
    .bind(&item.external_id)
    .bind(&item.source_type)
    .bind(&item.source_url)
    .bind(&item.title)
    .bind(&item.description)
    .bind(&metadata_json)
    .bind(status.as_str())
    .bind(triage_reason)
    .bind(priority)
    .bind(now)
    .bind(now)
    .execute(pool)
    .await
    .context("track work item")?;

    Ok(result.rows_affected() > 0)
}

/// Update a work item's status and optionally its session_id.
pub async fn update_work_item_status(
    pool: &SqlitePool,
    directive_id: &str,
    external_id: &str,
    status: &WorkItemStatus,
    session_id: Option<&str>,
) -> Result<bool> {
    let now = epoch_secs();
    let result = sqlx::query(
        "UPDATE directive_work_items
         SET status = ?, session_id = COALESCE(?, session_id), updated_at = ?
         WHERE directive_id = ? AND external_id = ?",
    )
    .bind(status.as_str())
    .bind(session_id)
    .bind(now)
    .bind(directive_id)
    .bind(external_id)
    .execute(pool)
    .await
    .context("update work item status")?;

    Ok(result.rows_affected() > 0)
}

/// Check if a work item is already tracked for a directive.
pub async fn is_item_tracked(
    pool: &SqlitePool,
    directive_id: &str,
    external_id: &str,
) -> Result<bool> {
    let count: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM directive_work_items
         WHERE directive_id = ? AND external_id = ?",
    )
    .bind(directive_id)
    .bind(external_id)
    .fetch_one(pool)
    .await
    .context("check item tracked")?;

    Ok(count.0 > 0)
}

/// List work items for a directive, optionally filtered by status.
pub async fn list_work_items(
    pool: &SqlitePool,
    directive_id: &str,
    status_filter: Option<&str>,
    limit: u32,
) -> Result<Vec<TrackedWorkItem>> {
    let limit = if limit == 0 { 50 } else { limit.min(200) };

    let rows: Vec<(
        String, String, String, Option<String>, String, Option<String>,
        String, Option<String>, String, Option<String>, i32, i64, i64,
    )> = if let Some(status) = status_filter {
        sqlx::query_as(
            "SELECT directive_id, external_id, source_type, source_url, title, description,
                    metadata, session_id, status, triage_reason, priority, discovered_at, updated_at
             FROM directive_work_items
             WHERE directive_id = ? AND status = ?
             ORDER BY priority ASC, discovered_at DESC
             LIMIT ?",
        )
        .bind(directive_id)
        .bind(status)
        .bind(limit)
        .fetch_all(pool)
        .await
        .context("list work items (filtered)")?
    } else {
        sqlx::query_as(
            "SELECT directive_id, external_id, source_type, source_url, title, description,
                    metadata, session_id, status, triage_reason, priority, discovered_at, updated_at
             FROM directive_work_items
             WHERE directive_id = ?
             ORDER BY priority ASC, discovered_at DESC
             LIMIT ?",
        )
        .bind(directive_id)
        .bind(limit)
        .fetch_all(pool)
        .await
        .context("list work items")?
    };

    rows.into_iter().map(parse_work_item_row).collect()
}

/// Count active (non-terminal) sessions spawned by a directive.
pub async fn count_active_sessions_for_directive(
    pool: &SqlitePool,
    directive_id: &str,
) -> Result<i64> {
    let count: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM directive_work_items dwi
         JOIN workflow_sessions ws ON ws.id = dwi.session_id
         WHERE dwi.directive_id = ?
           AND dwi.status = 'in_progress'
           AND ws.status NOT IN ('completed', 'failed', 'cancelled')",
    )
    .bind(directive_id)
    .fetch_one(pool)
    .await
    .context("count active sessions for directive")?;

    Ok(count.0)
}

/// Count consecutive empty polls (polls that discovered zero items).
/// Uses last_poll_at and discovered_at to infer empty polls.
pub async fn count_consecutive_empty_polls(
    pool: &SqlitePool,
    directive_id: &str,
) -> Result<i64> {
    // Count how many poll intervals have passed since the last discovered item.
    let row: Option<(i64,)> = sqlx::query_as(
        "SELECT MAX(discovered_at) FROM directive_work_items WHERE directive_id = ?",
    )
    .bind(directive_id)
    .fetch_optional(pool)
    .await
    .context("count consecutive empty polls")?;

    let last_discovered = row.and_then(|r| if r.0 > 0 { Some(r.0) } else { None });

    // Load the directive to get poll interval and last_poll_at.
    let dir_row: Option<(i64, Option<i64>)> = sqlx::query_as(
        "SELECT poll_interval_secs, last_poll_at FROM directives WHERE id = ?",
    )
    .bind(directive_id)
    .fetch_optional(pool)
    .await
    .context("load directive for empty poll count")?;

    match (dir_row, last_discovered) {
        (Some((interval, Some(last_poll))), Some(last_disc)) => {
            if last_poll <= last_disc {
                Ok(0)
            } else {
                let gap = last_poll - last_disc;
                Ok(gap / interval.max(1))
            }
        }
        (Some((_, Some(last_poll))), None) => {
            // Never discovered anything — count polls since creation.
            let created: (i64,) = sqlx::query_as(
                "SELECT created_at FROM directives WHERE id = ?",
            )
            .bind(directive_id)
            .fetch_one(pool)
            .await?;
            let dir_interval: (i64,) = sqlx::query_as(
                "SELECT poll_interval_secs FROM directives WHERE id = ?",
            )
            .bind(directive_id)
            .fetch_one(pool)
            .await?;
            let gap = last_poll - created.0;
            Ok(gap / dir_interval.0.max(1))
        }
        _ => Ok(0),
    }
}

/// Count failed sessions for a directive.
pub async fn count_failed_sessions(pool: &SqlitePool, directive_id: &str) -> Result<i64> {
    let count: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM directive_work_items
         WHERE directive_id = ? AND status = 'failed'",
    )
    .bind(directive_id)
    .fetch_one(pool)
    .await
    .context("count failed sessions")?;

    Ok(count.0)
}

// ---------------------------------------------------------------------------
// Parsing helpers
// ---------------------------------------------------------------------------

/// Parse a directive status string.
pub fn parse_directive_status(s: &str) -> DirectiveStatus {
    match s {
        "active" => DirectiveStatus::Active,
        "paused" => DirectiveStatus::Paused,
        "waiting_for_human" => DirectiveStatus::WaitingForHuman,
        "retired" => DirectiveStatus::Retired,
        other => {
            warn!(status = other, "unknown directive status, defaulting to Paused");
            DirectiveStatus::Paused
        }
    }
}

fn parse_work_item_status(s: &str) -> WorkItemStatus {
    match s {
        "discovered" => WorkItemStatus::Discovered,
        "accepted" => WorkItemStatus::Accepted,
        "rejected" => WorkItemStatus::Rejected,
        "in_progress" => WorkItemStatus::InProgress,
        "completed" => WorkItemStatus::Completed,
        "failed" => WorkItemStatus::Failed,
        other => {
            warn!(status = other, "unknown work item status, defaulting to Discovered");
            WorkItemStatus::Discovered
        }
    }
}

fn parse_directive_row(
    row: (
        String, String, String, String, String, i32, String, i64,
        Option<i64>, Option<i64>, Option<String>, Option<String>, i64, i64,
    ),
) -> Result<Directive> {
    let (
        id, name, intent, sources_json, constraints_json, priority, status,
        poll_interval_secs, last_poll_at, next_poll_at, escalation_json,
        created_by, created_at, updated_at,
    ) = row;

    let sources: Vec<WorkSource> = serde_json::from_str(&sources_json).unwrap_or_else(|e| {
        warn!(directive_id = %id, "failed to parse sources: {e}");
        Vec::new()
    });

    let constraints: DirectiveConstraints =
        serde_json::from_str(&constraints_json).unwrap_or_else(|e| {
            warn!(directive_id = %id, "failed to parse constraints: {e}");
            DirectiveConstraints::default()
        });

    let escalation: Option<DirectiveEscalation> = escalation_json.and_then(|j| {
        serde_json::from_str(&j)
            .map_err(|e| {
                warn!(directive_id = %id, "failed to parse escalation: {e}");
                e
            })
            .ok()
    });

    Ok(Directive {
        id,
        name,
        intent,
        sources,
        constraints,
        priority,
        status: parse_directive_status(&status),
        poll_interval_secs,
        last_poll_at,
        next_poll_at,
        escalation,
        created_by,
        reply_context: None,
        created_at,
        updated_at,
    })
}

fn parse_work_item_row(
    row: (
        String, String, String, Option<String>, String, Option<String>,
        String, Option<String>, String, Option<String>, i32, i64, i64,
    ),
) -> Result<TrackedWorkItem> {
    let (
        directive_id, external_id, source_type, source_url, title, description,
        metadata_json, session_id, status, triage_reason, priority,
        discovered_at, updated_at,
    ) = row;

    let metadata: serde_json::Value =
        serde_json::from_str(&metadata_json).unwrap_or(serde_json::json!({}));

    Ok(TrackedWorkItem {
        directive_id,
        external_id,
        source_type,
        source_url,
        title,
        description,
        metadata,
        session_id,
        status: parse_work_item_status(&status),
        triage_reason,
        priority,
        discovered_at,
        updated_at,
    })
}
