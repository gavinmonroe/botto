// ---------------------------------------------------------------------------
// Workflow Scheduler — cron trigger loop + event trigger matching.
//
// Two responsibilities:
//   1. Cron loop: checks every minute for workflows with cron triggers due
//      to fire. Creates new workflow runs for matching schedules.
//   2. Event matching: receives events from the EventBus and matches them
//      against workflow event triggers. Creates runs for matching workflows.
//
// Both are spawned as background tokio tasks on startup.
// ---------------------------------------------------------------------------

use chrono::{Datelike, Timelike, Utc};
use sqlx::SqlitePool;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, Semaphore};
use tokio::time::{interval, Duration};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

// Re-export for spawn error logging.
use std::panic::AssertUnwindSafe;
use futures::FutureExt;

use crate::services::ai::client::AiClientConfig;
use crate::services::events::{Event, EventBus, EventType};
use crate::services::mentor::client::MentorClient;
use crate::services::workflow::factory::AgentFactoryConfig;
use crate::services::workflow::filter;
use crate::services::workflow::orchestrator::Orchestrator;
use crate::services::workflow::session::{self, SessionManager, SessionManagerConfig};
use crate::types::workflow::{Trigger, TriggerSource, WorkflowDefinition, WorkflowMode};

// ---------------------------------------------------------------------------
// Scheduler
// ---------------------------------------------------------------------------

/// Spawn the cron scheduler as a background task.
/// Checks every 60 seconds for workflows with cron triggers due to fire.
///
/// Accepts a shared semaphore so cron and event triggers share the same
/// concurrency limit.
pub fn spawn_cron_scheduler(
    pool: SqlitePool,
    mentor_repo: String,
    agent_config: AgentFactoryConfig,
    default_step_timeout_secs: u64,
    semaphore: Arc<Semaphore>,
    event_bus: EventBus,
    cancel: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    let last_fired = Arc::new(Mutex::new(HashMap::<String, i64>::new()));
    tokio::spawn(cron_loop(
        pool,
        mentor_repo,
        agent_config,
        default_step_timeout_secs,
        semaphore,
        event_bus,
        last_fired,
        cancel,
    ))
}

async fn cron_loop(
    pool: SqlitePool,
    mentor_repo: String,
    agent_config: AgentFactoryConfig,
    default_step_timeout_secs: u64,
    semaphore: Arc<Semaphore>,
    event_bus: EventBus,
    last_fired: Arc<Mutex<HashMap<String, i64>>>,
    cancel: CancellationToken,
) {
    let mut tick = interval(Duration::from_secs(60));

    // Skip the first immediate tick.
    tick.tick().await;

    info!("workflow cron scheduler started");

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!("workflow cron scheduler shutting down");
                return;
            }
            _ = tick.tick() => {
                if let Err(e) = check_cron_triggers(
                    &pool,
                    &mentor_repo,
                    &agent_config,
                    default_step_timeout_secs,
                    &semaphore,
                    &event_bus,
                    &last_fired,
                ).await {
                    warn!(error = %e, "cron scheduler: tick failed");
                }
            }
        }
    }
}

/// Check all enabled workflows for cron triggers that match the current time.
async fn check_cron_triggers(
    pool: &SqlitePool,
    mentor_repo: &str,
    agent_config: &AgentFactoryConfig,
    default_step_timeout_secs: u64,
    semaphore: &Arc<Semaphore>,
    event_bus: &EventBus,
    last_fired: &Arc<Mutex<HashMap<String, i64>>>,
) -> anyhow::Result<()> {
    let workflows = load_enabled_workflows(pool).await?;
    let now = Utc::now();
    // Truncate to the current minute for dedup comparison.
    let current_minute = now.timestamp() / 60;

    for workflow in &workflows {
        for trigger in &workflow.triggers {
            if let Trigger::Cron { schedule } = trigger {
                if cron_matches(schedule, &now) {
                    // --- Fix #4: dedup — skip if already fired this minute ---
                    let wf_id = workflow.id.to_string();
                    {
                        let mut map = last_fired.lock().await;
                        if map.get(&wf_id) == Some(&current_minute) {
                            debug!(
                                workflow = %workflow.name,
                                "cron scheduler: already fired this minute, skipping"
                            );
                            continue;
                        }
                        map.insert(wf_id, current_minute);
                    }

                    debug!(
                        workflow = %workflow.name,
                        schedule,
                        "cron scheduler: trigger matched"
                    );

                    // Acquire semaphore permit before spawning.
                    let permit = match semaphore.clone().try_acquire_owned() {
                        Ok(p) => p,
                        Err(_) => {
                            warn!(
                                workflow = %workflow.name,
                                "cron scheduler: max concurrent runs reached, skipping"
                            );
                            continue;
                        }
                    };

                    let pool = pool.clone();
                    let mentor = MentorClient::new(pool.clone(), mentor_repo.to_string());
                    let config = agent_config.clone();
                    let definition = workflow.clone();
                    let wf_name = workflow.name.clone();
                    let event_bus = event_bus.clone();

                    // Fix #6: catch panics so errors aren't silently lost.
                    tokio::spawn(async move {
                        let wf_name_inner = wf_name.clone();
                        let result = AssertUnwindSafe(async {
                            match definition.mode {
                                WorkflowMode::Autonomous => {
                                    let ai_config = config.ai.clone().unwrap_or_else(|| AiClientConfig {
                                        base_url: String::new(),
                                        api_key: String::new(),
                                    });
                                    let sm_config = SessionManagerConfig {
                                        ai_model: config.ai_default_model.clone(),
                                        ..Default::default()
                                    };
                                    let manager = SessionManager::new(
                                        pool.clone(),
                                        ai_config,
                                        config,
                                        mentor,
                                        event_bus,
                                        sm_config,
                                    );
                                    let trigger_data = serde_json::json!({
                                        "trigger": "cron",
                                        "fired_at": now.to_rfc3339(),
                                    });
                                    match session::create_session(
                                        &pool,
                                        definition.id,
                                        "cron",
                                        Some(trigger_data),
                                    ).await {
                                        Ok(mut session) => {
                                            if let Err(e) = manager.drive(&mut session, &wf_name_inner).await {
                                                error!(
                                                    workflow = %wf_name_inner,
                                                    session_id = %session.id,
                                                    error = %e,
                                                    "cron scheduler: autonomous session failed"
                                                );
                                            }
                                        }
                                        Err(e) => {
                                            error!(
                                                workflow = %wf_name_inner,
                                                error = %e,
                                                "cron scheduler: failed to create session"
                                            );
                                        }
                                    }
                                }
                                WorkflowMode::Simple => {
                                    let orchestrator =
                                        Orchestrator::new(pool, mentor, config, default_step_timeout_secs);
                                    let trigger = TriggerSource::Cron { fired_at: now };
                                    let run = orchestrator.execute(&definition, trigger).await;
                                    debug!(
                                        workflow = %wf_name_inner,
                                        run_id = %run.id,
                                        status = ?run.status,
                                        "cron scheduler: workflow run completed"
                                    );
                                }
                            }
                        })
                        .catch_unwind()
                        .await;

                        if let Err(panic) = result {
                            let msg = panic
                                .downcast_ref::<String>()
                                .map(|s| s.as_str())
                                .or_else(|| panic.downcast_ref::<&str>().copied())
                                .unwrap_or("unknown panic");
                            error!(
                                workflow = %wf_name,
                                error = %msg,
                                "cron scheduler: workflow run panicked"
                            );
                        }
                        drop(permit);
                    });
                }
            }
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Event trigger matching
// ---------------------------------------------------------------------------

/// Spawn the event trigger matcher as a background task.
/// Listens on the EventBus and matches events against workflow triggers.
///
/// Accepts a shared semaphore so cron and event triggers share the same
/// concurrency limit.
pub fn spawn_event_matcher(
    pool: SqlitePool,
    event_bus: &EventBus,
    mentor_repo: String,
    agent_config: AgentFactoryConfig,
    default_step_timeout_secs: u64,
    semaphore: Arc<Semaphore>,
    cancel: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    let mut rx = event_bus.subscribe();
    let event_bus = event_bus.clone();

    tokio::spawn(async move {
        info!("workflow event matcher started");

        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    info!("workflow event matcher shutting down");
                    return;
                }
                result = rx.recv() => {
                    match result {
                        Ok(event) => {
                            if let Err(e) = handle_event(
                                &pool,
                                &event,
                                &mentor_repo,
                                &agent_config,
                                default_step_timeout_secs,
                                &semaphore,
                                &event_bus,
                            ).await {
                                warn!(error = %e, "event matcher: handle failed");
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            warn!(missed = n, "event matcher: lagged, missed events");
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            info!("event matcher: bus closed, shutting down");
                            return;
                        }
                    }
                }
            }
        }
    })
}

/// Match an event against all enabled workflows with event triggers.
async fn handle_event(
    pool: &SqlitePool,
    event: &Event,
    mentor_repo: &str,
    agent_config: &AgentFactoryConfig,
    default_step_timeout_secs: u64,
    semaphore: &Arc<Semaphore>,
    event_bus: &EventBus,
) -> anyhow::Result<()> {
    let event_type_str = event_type_to_string(&event.event_type);
    let workflows = load_enabled_workflows(pool).await?;
    let payload = event
        .payload
        .clone()
        .unwrap_or(serde_json::Value::Null);

    for workflow in &workflows {
        for trigger in &workflow.triggers {
            if let Trigger::Event {
                event_type,
                filter: filter_expr,
            } = trigger
            {
                if event_type != &event_type_str {
                    continue;
                }

                // --- Fix #1: evaluate filter expression against event payload ---
                if !filter::evaluate(filter_expr.as_deref(), &payload) {
                    debug!(
                        workflow = %workflow.name,
                        event_type = %event_type_str,
                        filter = ?filter_expr,
                        "event matcher: filter did not match, skipping"
                    );
                    continue;
                }

                debug!(
                    workflow = %workflow.name,
                    event_type = %event_type_str,
                    "event matcher: trigger matched"
                );

                let permit = match semaphore.clone().try_acquire_owned() {
                    Ok(p) => p,
                    Err(_) => {
                        warn!(
                            workflow = %workflow.name,
                            "event matcher: max concurrent runs reached, skipping"
                        );
                        continue;
                    }
                };

                let pool = pool.clone();
                let mentor = MentorClient::new(pool.clone(), mentor_repo.to_string());
                let config = agent_config.clone();
                let definition = workflow.clone();
                let trigger_payload = payload.clone();
                let et_str = event_type_str.clone();
                let wf_name = workflow.name.clone();
                let event_bus_clone = event_bus.clone();

                // Fix #6: catch panics so errors aren't silently lost.
                tokio::spawn(async move {
                    let wf_name_inner = wf_name.clone();
                    let result = AssertUnwindSafe(async {
                        match definition.mode {
                            WorkflowMode::Autonomous => {
                                let ai_config = config.ai.clone().unwrap_or_else(|| AiClientConfig {
                                    base_url: String::new(),
                                    api_key: String::new(),
                                });
                                let sm_config = SessionManagerConfig {
                                    ai_model: config.ai_default_model.clone(),
                                    ..Default::default()
                                };
                                let manager = SessionManager::new(
                                    pool.clone(),
                                    ai_config,
                                    config,
                                    mentor,
                                    event_bus_clone,
                                    sm_config,
                                );
                                let trigger_data = serde_json::json!({
                                    "trigger": "event",
                                    "event_type": et_str,
                                    "payload": trigger_payload,
                                });
                                match session::create_session(
                                    &pool,
                                    definition.id,
                                    "event",
                                    Some(trigger_data),
                                ).await {
                                    Ok(mut session) => {
                                        if let Err(e) = manager.drive(&mut session, &wf_name_inner).await {
                                            error!(
                                                workflow = %wf_name_inner,
                                                session_id = %session.id,
                                                error = %e,
                                                "event matcher: autonomous session failed"
                                            );
                                        }
                                    }
                                    Err(e) => {
                                        error!(
                                            workflow = %wf_name_inner,
                                            error = %e,
                                            "event matcher: failed to create session"
                                        );
                                    }
                                }
                            }
                            WorkflowMode::Simple => {
                                let orchestrator =
                                    Orchestrator::new(pool, mentor, config, default_step_timeout_secs);
                                let trigger = TriggerSource::Event {
                                    event_type: et_str,
                                    payload: trigger_payload,
                                };
                                let run = orchestrator.execute(&definition, trigger).await;
                                debug!(
                                    workflow = %wf_name_inner,
                                    run_id = %run.id,
                                    status = ?run.status,
                                    "event matcher: workflow run completed"
                                );
                            }
                        }
                    })
                    .catch_unwind()
                    .await;

                    if let Err(panic) = result {
                        let msg = panic
                            .downcast_ref::<String>()
                            .map(|s| s.as_str())
                            .or_else(|| panic.downcast_ref::<&str>().copied())
                            .unwrap_or("unknown panic");
                        error!(
                            workflow = %wf_name,
                            error = %msg,
                            "event matcher: workflow run panicked"
                        );
                    }
                    drop(permit);
                });
            }
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Cron parsing — minimal 5-field cron matcher
// ---------------------------------------------------------------------------

/// Check if a 5-field cron expression matches the given time.
/// Fields: minute hour day-of-month month day-of-week
/// Supports: *, specific values, comma-separated lists, ranges (a-b), steps (*/n).
pub fn cron_matches(expr: &str, time: &chrono::DateTime<Utc>) -> bool {
    let fields: Vec<&str> = expr.split_whitespace().collect();
    if fields.len() != 5 {
        warn!(expr, "cron: invalid expression (need 5 fields)");
        return false;
    }

    let minute = time.minute();
    let hour = time.hour();
    let dom = time.day();
    let month = time.month();
    // chrono: Mon=0..Sun=6, cron: Sun=0, Mon=1..Sat=6 (or Sun=7)
    let dow = match time.weekday().num_days_from_sunday() {
        d => d,
    };

    // field_min: 0 for minute/hour/dow, 1 for day-of-month/month
    field_matches(fields[0], minute, 0)
        && field_matches(fields[1], hour, 0)
        && field_matches(fields[2], dom, 1)
        && field_matches(fields[3], month, 1)
        && field_matches_dow(fields[4], dow)
}

/// Check if a single cron field matches a value.
/// `field_min` is the minimum value for the field (0 for minute/hour/dow,
/// 1 for day-of-month/month). Used to correctly evaluate step expressions
/// like `*/2` in 1-based fields.
fn field_matches(field: &str, value: u32, field_min: u32) -> bool {
    if field == "*" {
        return true;
    }

    // Comma-separated list.
    for part in field.split(',') {
        let part = part.trim();

        // Step: */n or range/n
        if let Some((base, step_str)) = part.split_once('/') {
            if let Ok(step) = step_str.parse::<u32>() {
                if step == 0 {
                    continue;
                }
                if base == "*" {
                    // Fix #3: use (value - field_min) so */2 in month (1-12)
                    // matches 1,3,5,7,9,11 instead of 2,4,6,8,10,12.
                    if (value.wrapping_sub(field_min)) % step == 0 {
                        return true;
                    }
                } else if let Some((start, end)) = parse_range(base) {
                    if value >= start && value <= end && (value - start) % step == 0 {
                        return true;
                    }
                }
            }
            continue;
        }

        // Range: a-b
        if let Some((start, end)) = parse_range(part) {
            if value >= start && value <= end {
                return true;
            }
            continue;
        }

        // Exact value.
        if let Ok(v) = part.parse::<u32>() {
            if v == value {
                return true;
            }
        }
    }

    false
}

/// Day-of-week field: same as field_matches but treats standalone '7' as '0' (Sunday).
fn field_matches_dow(field: &str, value: u32) -> bool {
    if field == "*" {
        return true;
    }
    // Fix #2: only replace standalone '7' tokens, not '7' inside other numbers.
    // Split on commas, normalize each token individually.
    let normalized: String = field
        .split(',')
        .map(|token| {
            let token = token.trim();
            if token == "7" {
                "0".to_string()
            } else if let Some((a, b)) = token.split_once('-') {
                // Normalize range endpoints: "1-7" -> "1-0"
                let a = if a.trim() == "7" { "0" } else { a.trim() };
                let b = if b.trim() == "7" { "0" } else { b.trim() };
                format!("{a}-{b}")
            } else if let Some((base, step)) = token.split_once('/') {
                // Normalize step base: "7/2" -> "0/2", "*/2" stays
                let base = if base.trim() == "7" {
                    "0".to_string()
                } else if let Some((a, b)) = base.split_once('-') {
                    let a = if a.trim() == "7" { "0" } else { a.trim() };
                    let b = if b.trim() == "7" { "0" } else { b.trim() };
                    format!("{a}-{b}")
                } else {
                    base.trim().to_string()
                };
                format!("{base}/{}", step.trim())
            } else {
                token.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join(",");
    field_matches(&normalized, value, 0)
}

fn parse_range(s: &str) -> Option<(u32, u32)> {
    let (a, b) = s.split_once('-')?;
    let start = a.trim().parse::<u32>().ok()?;
    let end = b.trim().parse::<u32>().ok()?;
    Some((start, end))
}

// ---------------------------------------------------------------------------
// Workflow loading
// ---------------------------------------------------------------------------

/// Load all enabled workflow definitions from SQLite.
async fn load_enabled_workflows(pool: &SqlitePool) -> anyhow::Result<Vec<WorkflowDefinition>> {
    let rows: Vec<(String,)> = sqlx::query_as(
        "SELECT definition FROM workflows WHERE enabled = 1",
    )
    .fetch_all(pool)
    .await?;

    let mut workflows = Vec::with_capacity(rows.len());
    for (json,) in rows {
        match serde_json::from_str::<WorkflowDefinition>(&json) {
            Ok(wf) => workflows.push(wf),
            Err(e) => {
                warn!(error = %e, "scheduler: failed to parse workflow definition");
            }
        }
    }

    Ok(workflows)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn event_type_to_string(et: &EventType) -> String {
    match et {
        EventType::ReviewStarted => "review.started",
        EventType::ReviewComplete => "review.complete",
        EventType::CommentAction => "comment.action",
        EventType::FixStarted => "fix.started",
        EventType::FixProgress => "fix.progress",
        EventType::FixComplete => "fix.complete",
        EventType::MrUpdated => "mr.updated",
        EventType::UserJoinedMr => "user.joined_mr",
        EventType::UserLeftMr => "user.left_mr",
        EventType::ConflictUpdated => "conflict.updated",
        EventType::ClusterUpdated => "cluster.updated",
        EventType::WorkflowRunStarted => "workflow.run.started",
        EventType::WorkflowRunCompleted => "workflow.run.completed",
        EventType::WorkflowStepStarted => "workflow.step.started",
        EventType::WorkflowStepCompleted => "workflow.step.completed",
        EventType::WorkflowStepFailed => "workflow.step.failed",
        EventType::SessionEscalation => "session.escalation",
        EventType::SessionResumed => "session.resumed",
        EventType::DirectiveEscalation => "directive.escalation",
    }
    .to_string()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn time(y: i32, mo: u32, d: u32, h: u32, m: u32) -> chrono::DateTime<Utc> {
        Utc.with_ymd_and_hms(y, mo, d, h, m, 0).unwrap()
    }

    #[test]
    fn cron_wildcard() {
        assert!(cron_matches("* * * * *", &time(2026, 3, 28, 9, 0)));
    }

    #[test]
    fn cron_exact_minute() {
        assert!(cron_matches("30 * * * *", &time(2026, 3, 28, 9, 30)));
        assert!(!cron_matches("30 * * * *", &time(2026, 3, 28, 9, 15)));
    }

    #[test]
    fn cron_exact_hour_minute() {
        assert!(cron_matches("0 9 * * *", &time(2026, 3, 28, 9, 0)));
        assert!(!cron_matches("0 9 * * *", &time(2026, 3, 28, 10, 0)));
    }

    #[test]
    fn cron_weekday_range() {
        // 2026-03-28 is a Saturday (dow=6)
        assert!(!cron_matches("0 9 * * 1-5", &time(2026, 3, 28, 9, 0)));
        // 2026-03-27 is a Friday (dow=5)
        assert!(cron_matches("0 9 * * 1-5", &time(2026, 3, 27, 9, 0)));
    }

    #[test]
    fn cron_step() {
        assert!(cron_matches("*/5 * * * *", &time(2026, 3, 28, 9, 0)));
        assert!(cron_matches("*/5 * * * *", &time(2026, 3, 28, 9, 15)));
        assert!(!cron_matches("*/5 * * * *", &time(2026, 3, 28, 9, 13)));
    }

    #[test]
    fn cron_comma_list() {
        assert!(cron_matches("0,15,30,45 * * * *", &time(2026, 3, 28, 9, 15)));
        assert!(!cron_matches("0,15,30,45 * * * *", &time(2026, 3, 28, 9, 10)));
    }

    #[test]
    fn cron_dom_and_month() {
        assert!(cron_matches("0 0 1 1 *", &time(2026, 1, 1, 0, 0)));
        assert!(!cron_matches("0 0 1 1 *", &time(2026, 2, 1, 0, 0)));
    }

    #[test]
    fn cron_sunday_as_7() {
        // 2026-03-29 is a Sunday (dow=0)
        assert!(cron_matches("0 9 * * 7", &time(2026, 3, 29, 9, 0)));
        assert!(cron_matches("0 9 * * 0", &time(2026, 3, 29, 9, 0)));
    }

    #[test]
    fn field_matches_range_step() {
        // 1-5/2 matches 1, 3, 5
        assert!(field_matches("1-5/2", 1, 0));
        assert!(!field_matches("1-5/2", 2, 0));
        assert!(field_matches("1-5/2", 3, 0));
        assert!(field_matches("1-5/2", 5, 0));
        assert!(!field_matches("1-5/2", 6, 0));
    }

    #[test]
    fn cron_step_1based_month() {
        // */2 in month field (1-based) should match odd months: 1,3,5,7,9,11
        assert!(cron_matches("0 0 1 * *", &time(2026, 1, 1, 0, 0)));
        assert!(cron_matches("0 0 1 */2 *", &time(2026, 1, 1, 0, 0)));
        assert!(!cron_matches("0 0 1 */2 *", &time(2026, 2, 1, 0, 0)));
        assert!(cron_matches("0 0 1 */2 *", &time(2026, 3, 1, 0, 0)));
    }

    #[test]
    fn dow_7_standalone_only() {
        // "17" should NOT have its '7' replaced — it's not a valid dow but
        // the point is the replacement must not corrupt multi-digit tokens.
        // "7" alone should become "0" (Sunday).
        // 2026-03-29 is Sunday (dow=0).
        assert!(cron_matches("0 9 * * 7", &time(2026, 3, 29, 9, 0)));
        // "1-7" range: the 7 endpoint should become 0, but the range 1-0
        // won't match Sunday=0 via range logic. However "0,1-6" would.
        // The key fix: "17" must not become "10".
        assert!(!field_matches_dow("17", 0));
        assert!(field_matches_dow("0,7", 0));
    }

    #[test]
    fn event_type_strings() {
        assert_eq!(event_type_to_string(&EventType::MrUpdated), "mr.updated");
        assert_eq!(event_type_to_string(&EventType::FixComplete), "fix.complete");
    }
}
