// ---------------------------------------------------------------------------
// Database — SQLite with WAL mode, auto-migration.
//
// Uses sqlx with compile-time unchecked queries (runtime-checked) since we
// embed migrations and run them on startup. WAL mode gives us concurrent
// readers with a single writer, which is perfect for our workload.
// ---------------------------------------------------------------------------

use anyhow::{Context, Result};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::SqlitePool;
use std::path::Path;
use std::str::FromStr;
use tracing::info;

pub mod queries;

/// Initialize the database: create file, set WAL mode, run migrations.
pub async fn init(db_path: &Path) -> Result<SqlitePool> {
    let db_url = format!("sqlite:{}?mode=rwc", db_path.display());

    let options = SqliteConnectOptions::from_str(&db_url)?
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(SqliteSynchronous::Normal)
        // Increase cache for better read performance
        .pragma("cache_size", "-64000") // 64MB
        .pragma("temp_store", "memory")
        .create_if_missing(true);

    let pool = SqlitePoolOptions::new()
        .max_connections(8)
        .connect_with(options)
        .await
        .with_context(|| format!("failed to open database: {}", db_path.display()))?;

    // Run migrations
    migrate(&pool).await?;

    Ok(pool)
}

/// Run embedded migrations. Idempotent — safe to call on every startup.
/// Also used by tests via `init_test_db`.
pub(crate) async fn migrate(pool: &SqlitePool) -> Result<()> {
    info!("running database migrations...");

    sqlx::query(MIGRATION_001)
        .execute(pool)
        .await
        .context("migration 001 failed")?;

    sqlx::query(MIGRATION_002)
        .execute(pool)
        .await
        .context("migration 002 failed")?;

    sqlx::query(MIGRATION_003)
        .execute(pool)
        .await
        .context("migration 003 failed")?;

    sqlx::query(MIGRATION_004)
        .execute(pool)
        .await
        .context("migration 004 failed")?;

    sqlx::query(MIGRATION_005)
        .execute(pool)
        .await
        .context("migration 005 failed")?;

    sqlx::query(MIGRATION_006)
        .execute(pool)
        .await
        .context("migration 006 failed")?;

    // Migration 006 addendum: add category/severity columns to comment_actions.
    // ALTER TABLE ADD COLUMN isn't idempotent, so we check first.
    let has_category: bool = sqlx::query_scalar::<_, i32>(
        "SELECT COUNT(*) FROM pragma_table_info('comment_actions') WHERE name = 'category'",
    )
    .fetch_one(pool)
    .await
    .unwrap_or(0)
        > 0;

    if !has_category {
        sqlx::query("ALTER TABLE comment_actions ADD COLUMN category TEXT")
            .execute(pool)
            .await
            .context("migration 006: add category column")?;
        sqlx::query("ALTER TABLE comment_actions ADD COLUMN severity TEXT")
            .execute(pool)
            .await
            .context("migration 006: add severity column")?;
    }

    // Index for preference aggregation — safe to run every time (IF NOT EXISTS).
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_comment_actions_prefs
         ON comment_actions(project_path, category, severity, action)",
    )
    .execute(pool)
    .await
    .context("migration 006: create prefs index")?;

    sqlx::query(MIGRATION_007)
        .execute(pool)
        .await
        .context("migration 007 failed")?;

    sqlx::query(MIGRATION_008)
        .execute(pool)
        .await
        .context("migration 008 failed")?;

    sqlx::query(MIGRATION_009)
        .execute(pool)
        .await
        .context("migration 009 failed")?;

    sqlx::query(MIGRATION_010)
        .execute(pool)
        .await
        .context("migration 010 failed")?;

    sqlx::query(MIGRATION_011)
        .execute(pool)
        .await
        .context("migration 011 failed")?;

    // Migration 012: session_trace table + add 'clarifying' to workflow_sessions CHECK.
    // We use table recreation for the CHECK constraint update since SQLite can't ALTER CHECK.
    // The session_trace table is created with IF NOT EXISTS so it's idempotent.
    migrate_012(pool).await.context("migration 012 failed")?;

    // Additional indexes for query performance (idempotent via IF NOT EXISTS).
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_sandbox_jobs_mr
         ON sandbox_jobs(project_path, mr_iid)",
    )
    .execute(pool)
    .await
    .context("create sandbox_jobs MR index")?;

    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_sandbox_jobs_status
         ON sandbox_jobs(status, created_at)",
    )
    .execute(pool)
    .await
    .context("create sandbox_jobs status index")?;

    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_digests_lookup
         ON digests(project_path, period)",
    )
    .execute(pool)
    .await
    .context("create digests lookup index")?;

    info!("migrations complete");
    Ok(())
}

// ---------------------------------------------------------------------------
// Embedded migrations
// ---------------------------------------------------------------------------

const MIGRATION_001: &str = r#"
-- Connected Otto instances (ephemeral, cleared on restart)
CREATE TABLE IF NOT EXISTS connections (
    id TEXT PRIMARY KEY,
    user_id TEXT,
    connected_at INTEGER NOT NULL,
    last_seen_at INTEGER NOT NULL,
    viewing_mr TEXT
);

-- Cached reviews
CREATE TABLE IF NOT EXISTS review_cache (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_path TEXT NOT NULL,
    mr_iid INTEGER NOT NULL,
    diff_hash TEXT NOT NULL,
    data BLOB NOT NULL,
    file_diff_hashes TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    UNIQUE(project_path, mr_iid, diff_hash)
);
CREATE INDEX IF NOT EXISTS idx_review_cache_lookup
    ON review_cache(project_path, mr_iid);

-- User actions on review comments (accept/dismiss/edit)
CREATE TABLE IF NOT EXISTS comment_actions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_path TEXT NOT NULL,
    mr_iid INTEGER NOT NULL,
    comment_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    action TEXT NOT NULL,
    edited_body TEXT,
    created_at INTEGER NOT NULL,
    UNIQUE(project_path, mr_iid, comment_id, user_id)
);
CREATE INDEX IF NOT EXISTS idx_comment_actions_mr
    ON comment_actions(project_path, mr_iid);

-- Team-level settings (shared triage toggle, etc.)
CREATE TABLE IF NOT EXISTS team_settings (
    project_path TEXT PRIMARY KEY,
    shared_triage INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

-- Review queue
CREATE TABLE IF NOT EXISTS review_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_path TEXT NOT NULL,
    mr_iid INTEGER NOT NULL,
    priority_score REAL NOT NULL,
    status TEXT NOT NULL,
    mr_context BLOB NOT NULL,
    progress BLOB,
    error TEXT,
    enqueued_at INTEGER NOT NULL,
    started_at INTEGER,
    completed_at INTEGER,
    UNIQUE(project_path, mr_iid)
);
CREATE INDEX IF NOT EXISTS idx_review_queue_project
    ON review_queue(project_path, priority_score DESC);

-- Expiry index for periodic cache cleanup
CREATE INDEX IF NOT EXISTS idx_review_cache_expiry
    ON review_cache(expires_at);

-- Reviewer preferences (reserved for future accept/dismiss learning)
CREATE TABLE IF NOT EXISTS reviewer_prefs (
    project_path TEXT NOT NULL,
    host_url TEXT NOT NULL,
    prefs BLOB NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY(project_path, host_url)
);

-- Sandbox jobs
CREATE TABLE IF NOT EXISTS sandbox_jobs (
    id TEXT PRIMARY KEY,
    project_path TEXT NOT NULL,
    mr_iid INTEGER NOT NULL,
    comment_id TEXT,
    status TEXT NOT NULL,
    strategy TEXT NOT NULL,
    container_id TEXT,
    fix_diff TEXT,
    test_output TEXT,
    commit_sha TEXT,
    error TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

-- Event log
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_path TEXT NOT NULL,
    mr_iid INTEGER,
    event_type TEXT NOT NULL,
    user_id TEXT,
    payload BLOB,
    created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_mr
    ON events(project_path, mr_iid, created_at);

-- Clear ephemeral connections on startup
DELETE FROM connections;
"#;

const MIGRATION_002: &str = r#"
-- Team activity digests (cached, with TTL)
CREATE TABLE IF NOT EXISTS digests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_path TEXT NOT NULL,
    period TEXT NOT NULL,
    digest BLOB NOT NULL,
    generated_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    UNIQUE(project_path, period)
);
"#;

const MIGRATION_003: &str = r#"
-- Cached .otto.json repo configs (fetched from GitLab, TTL-based).
-- config_json = "{}" is the null sentinel: "we checked, no .otto.json exists".
-- formatted = pre-built prompt text (empty string for null sentinel).
-- sandbox_image = extracted sandbox.image for quick access by sandbox manager.
CREATE TABLE IF NOT EXISTS repo_configs (
    project_path TEXT PRIMARY KEY,
    config_json  TEXT NOT NULL,
    formatted    TEXT NOT NULL,
    sandbox_image TEXT,
    fetched_at   INTEGER NOT NULL,
    expires_at   INTEGER NOT NULL
);
"#;

const MIGRATION_004: &str = r#"
-- Cached setup recipes: the sequence of shell commands the AI discovered
-- during a successful sandbox setup. Keyed by project + base image because
-- different images need different setup steps (e.g., alpine vs debian).
-- Replayed on cold containers to skip the AI setup loop entirely.
CREATE TABLE IF NOT EXISTS setup_recipes (
    project_path TEXT NOT NULL,
    base_image   TEXT NOT NULL,
    commands     TEXT NOT NULL,
    setup_steps  INTEGER NOT NULL DEFAULT 0,
    created_at   INTEGER NOT NULL,
    last_used_at INTEGER NOT NULL,
    use_count    INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (project_path, base_image)
);
"#;

const MIGRATION_005: &str = r#"
-- Per-project knowledge store: structured facts extracted from successful
-- setups (Option C) and optional AI-distilled notes for complex projects
-- (Option B). Separate from setup_recipes because knowledge should survive
-- recipe invalidation — if a recipe replay fails, the knowledge ("rugged
-- needs libgit2-dev") is still valid for the next AI setup.
CREATE TABLE IF NOT EXISTS project_knowledge (
    project_path    TEXT NOT NULL,
    base_image      TEXT NOT NULL,
    facts           TEXT NOT NULL,
    notes           TEXT,
    notes_model     TEXT,
    created_at      INTEGER NOT NULL,
    updated_at      INTEGER NOT NULL,
    PRIMARY KEY (project_path, base_image)
);
"#;

const MIGRATION_006: &str = "";
// Migration 006 is handled programmatically below (ALTER TABLE isn't idempotent).

const MIGRATION_007: &str = r#"
-- MR changed files index — shared foundation for Conflict Radar and Cross-MR Clusters.
-- Populated from webhook events (MR open/update) and review pipeline side-effects.
-- Rows are deleted when an MR is merged or closed.
CREATE TABLE IF NOT EXISTS mr_changed_files (
    project_id INTEGER NOT NULL,
    mr_iid INTEGER NOT NULL,
    file_path TEXT NOT NULL,
    old_path TEXT,
    change_type TEXT NOT NULL,
    diff_hash TEXT NOT NULL,
    hunks TEXT NOT NULL DEFAULT '[]',
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (project_id, mr_iid, file_path)
);
CREATE INDEX IF NOT EXISTS idx_mcf_project_file
    ON mr_changed_files(project_id, file_path);
CREATE INDEX IF NOT EXISTS idx_mcf_project_mr
    ON mr_changed_files(project_id, mr_iid);

-- Cross-MR clusters — groups of related MRs (by ticket or file overlap).
-- summary_json and review_order_json are gzip-compressed, generated on demand.
CREATE TABLE IF NOT EXISTS mr_clusters (
    id TEXT PRIMARY KEY,
    project_id INTEGER NOT NULL,
    ticket_key TEXT,
    member_mrs TEXT NOT NULL,
    signals TEXT NOT NULL,
    relevance_score REAL NOT NULL,
    summary_json BLOB,
    summary_diff_hash TEXT,
    review_order_json BLOB,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_clusters_project
    ON mr_clusters(project_id);
CREATE INDEX IF NOT EXISTS idx_clusters_ticket
    ON mr_clusters(ticket_key);
"#;

const MIGRATION_008: &str = r#"
-- ---------------------------------------------------------------------------
-- Mentor knowledge store
-- ---------------------------------------------------------------------------

-- Core knowledge entries — execution patterns, domain knowledge, workflow
-- learnings, and user corrections. Scoped per-repo, per-linked-set, or global.
-- hit_count + last_queried_at drive the self-pruning system: entries that are
-- never queried decay in confidence and eventually get pruned.
CREATE TABLE IF NOT EXISTS mentor_entries (
    id                  TEXT PRIMARY KEY,
    content             TEXT NOT NULL,
    scope               TEXT NOT NULL,
    scope_type          TEXT NOT NULL CHECK(scope_type IN ('repo', 'linked', 'global')),
    category            TEXT NOT NULL CHECK(category IN ('execution', 'domain', 'workflow', 'correction')),
    source_workflow_id  TEXT,
    source_step_id      TEXT,
    created_at          INTEGER NOT NULL,
    last_queried_at     INTEGER,
    hit_count           INTEGER NOT NULL DEFAULT 0,
    confidence          REAL NOT NULL DEFAULT 1.0
);
CREATE INDEX IF NOT EXISTS idx_mentor_scope
    ON mentor_entries(scope, scope_type);
CREATE INDEX IF NOT EXISTS idx_mentor_scope_category
    ON mentor_entries(scope, category);
CREATE INDEX IF NOT EXISTS idx_mentor_confidence
    ON mentor_entries(confidence);

-- FTS5 virtual table for full-text search over mentor entries.
-- Uses external content mode: the actual data lives in mentor_entries,
-- FTS5 only maintains the inverted index. We index content, scope, and
-- category so queries can match on any of these.
CREATE VIRTUAL TABLE IF NOT EXISTS mentor_fts USING fts5(
    content,
    scope,
    category,
    content=mentor_entries,
    content_rowid=rowid
);

-- Sync triggers: keep mentor_fts in lockstep with mentor_entries.
-- COALESCE guards against NULL reaching the FTS index (content is NOT NULL,
-- but defence-in-depth costs nothing here).

CREATE TRIGGER IF NOT EXISTS mentor_fts_insert
AFTER INSERT ON mentor_entries BEGIN
    INSERT INTO mentor_fts(rowid, content, scope, category)
    VALUES (new.rowid, COALESCE(new.content, ''), COALESCE(new.scope, ''), COALESCE(new.category, ''));
END;

CREATE TRIGGER IF NOT EXISTS mentor_fts_delete
BEFORE DELETE ON mentor_entries BEGIN
    INSERT INTO mentor_fts(mentor_fts, rowid, content, scope, category)
    VALUES ('delete', old.rowid, COALESCE(old.content, ''), COALESCE(old.scope, ''), COALESCE(old.category, ''));
END;

-- Update is two-phase: delete old index entry before the row changes,
-- insert new entry after the row changes.
CREATE TRIGGER IF NOT EXISTS mentor_fts_update_delete
BEFORE UPDATE ON mentor_entries BEGIN
    INSERT INTO mentor_fts(mentor_fts, rowid, content, scope, category)
    VALUES ('delete', old.rowid, COALESCE(old.content, ''), COALESCE(old.scope, ''), COALESCE(old.category, ''));
END;

CREATE TRIGGER IF NOT EXISTS mentor_fts_update_insert
AFTER UPDATE ON mentor_entries BEGIN
    INSERT INTO mentor_fts(rowid, content, scope, category)
    VALUES (new.rowid, COALESCE(new.content, ''), COALESCE(new.scope, ''), COALESCE(new.category, ''));
END;

-- Explicit cross-project repo links. Users configure linked sets in botto.toml
-- (e.g., "payments" = ["service-auth", "service-users", "service-billing"]).
-- This table is synced from config on startup for admin visibility and querying.
CREATE TABLE IF NOT EXISTS mentor_repo_links (
    link_name   TEXT NOT NULL,
    repo_path   TEXT NOT NULL,
    created_at  INTEGER NOT NULL,
    PRIMARY KEY (link_name, repo_path)
);
CREATE INDEX IF NOT EXISTS idx_mentor_links_repo
    ON mentor_repo_links(repo_path);

-- ---------------------------------------------------------------------------
-- Workflow engine
-- ---------------------------------------------------------------------------

-- Workflow definitions — the "what to do" template.
-- definition is a JSON blob containing the full step DAG, triggers, and metadata.
-- description preserves the original natural language intent for AI verification.
CREATE TABLE IF NOT EXISTS workflows (
    id            TEXT PRIMARY KEY,
    name          TEXT NOT NULL,
    description   TEXT NOT NULL,
    project_id    INTEGER,
    definition    TEXT NOT NULL,
    enabled       INTEGER NOT NULL DEFAULT 1,
    created_by    TEXT,
    created_at    INTEGER NOT NULL,
    updated_at    INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_workflows_project
    ON workflows(project_id, enabled);
CREATE INDEX IF NOT EXISTS idx_workflows_enabled
    ON workflows(enabled);

-- Workflow run instances — one row per execution of a workflow.
-- step_states is a JSON object mapping step_id → StepState.
-- final_verification is a JSON object with the AI verification result.
-- Both are checkpointed after every step transition for crash recovery.
CREATE TABLE IF NOT EXISTS workflow_runs (
    id                  TEXT PRIMARY KEY,
    workflow_id         TEXT NOT NULL REFERENCES workflows(id),
    trigger_type        TEXT NOT NULL,
    trigger_data        TEXT,
    status              TEXT NOT NULL CHECK(status IN ('pending', 'running', 'completed', 'failed', 'cancelled')),
    step_states         TEXT NOT NULL DEFAULT '{}',
    final_verification  TEXT,
    started_at          INTEGER NOT NULL,
    completed_at        INTEGER
);
CREATE INDEX IF NOT EXISTS idx_wfruns_workflow
    ON workflow_runs(workflow_id, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_wfruns_status
    ON workflow_runs(status);

-- Step-level event log for workflow runs — append-only audit trail.
-- Used for monitoring dashboards and debugging failed runs.
CREATE TABLE IF NOT EXISTS workflow_run_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id      TEXT NOT NULL REFERENCES workflow_runs(id),
    step_id     TEXT,
    event_type  TEXT NOT NULL,
    data        TEXT,
    created_at  INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_wflog_run
    ON workflow_run_log(run_id, created_at);
"#;

const MIGRATION_009: &str = r#"
-- ---------------------------------------------------------------------------
-- Session-based orchestrator (v2)
-- ---------------------------------------------------------------------------

-- Workflow sessions — long-running stateful execution with human-in-the-loop.
-- Each session is a single run of a workflow through the plan-execute-evaluate
-- loop. Mutable state columns (status, plan, step_outputs, current_step_id,
-- retry_count, evaluator_feedback, escalation) are checkpointed after every
-- state transition for crash recovery.
CREATE TABLE IF NOT EXISTS workflow_sessions (
    id                TEXT PRIMARY KEY,
    workflow_id       TEXT NOT NULL REFERENCES workflows(id),
    status            TEXT NOT NULL CHECK(status IN (
                          'created', 'planning', 'executing', 'evaluating',
                          'adapting', 'waiting_for_human', 'completed',
                          'failed', 'cancelled'
                      )),
    trigger_type      TEXT NOT NULL,
    trigger_data      TEXT,
    plan              TEXT,
    step_outputs      TEXT NOT NULL DEFAULT '{}',
    current_step_id   TEXT,
    retry_count       INTEGER NOT NULL DEFAULT 0,
    max_retries       INTEGER NOT NULL DEFAULT 3,
    evaluator_feedback TEXT,
    escalation        TEXT,
    started_at        INTEGER NOT NULL,
    completed_at      INTEGER,
    updated_at        INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_sessions_status
    ON workflow_sessions(status);
CREATE INDEX IF NOT EXISTS idx_sessions_workflow
    ON workflow_sessions(workflow_id, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_sessions_waiting
    ON workflow_sessions(status) WHERE status = 'waiting_for_human';

-- Session messages — human conversation thread per session.
-- Supports the escalation / human-in-the-loop flow: agent asks a question,
-- human replies, agent resumes.
CREATE TABLE IF NOT EXISTS session_messages (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id  TEXT NOT NULL REFERENCES workflow_sessions(id),
    direction   TEXT NOT NULL CHECK(direction IN ('agent_to_human', 'human_to_agent')),
    content     TEXT NOT NULL,
    metadata    TEXT,
    created_at  INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_session_messages
    ON session_messages(session_id, created_at);
"#;

const MIGRATION_010: &str = r#"
-- ---------------------------------------------------------------------------
-- Directives — standing orders that continuously discover and process work.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS directives (
    id              TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    intent          TEXT NOT NULL,
    sources         TEXT NOT NULL DEFAULT '[]',
    constraints     TEXT NOT NULL DEFAULT '{}',
    priority        INTEGER NOT NULL DEFAULT 5,
    status          TEXT NOT NULL CHECK(status IN ('active', 'paused', 'waiting_for_human', 'retired')),
    poll_interval_secs INTEGER NOT NULL DEFAULT 300,
    last_poll_at    INTEGER,
    next_poll_at    INTEGER,
    escalation      TEXT,
    created_by      TEXT,
    created_at      INTEGER NOT NULL,
    updated_at      INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_directives_status
    ON directives(status);
CREATE INDEX IF NOT EXISTS idx_directives_active_poll
    ON directives(status, next_poll_at) WHERE status = 'active';

CREATE TABLE IF NOT EXISTS directive_work_items (
    directive_id    TEXT NOT NULL REFERENCES directives(id),
    external_id     TEXT NOT NULL,
    source_type     TEXT NOT NULL,
    source_url      TEXT,
    title           TEXT NOT NULL,
    description     TEXT,
    metadata        TEXT NOT NULL DEFAULT '{}',
    session_id      TEXT REFERENCES workflow_sessions(id),
    status          TEXT NOT NULL CHECK(status IN (
                        'discovered', 'accepted', 'rejected',
                        'in_progress', 'completed', 'failed'
                    )),
    triage_reason   TEXT,
    priority        INTEGER NOT NULL DEFAULT 5,
    discovered_at   INTEGER NOT NULL,
    updated_at      INTEGER NOT NULL,
    PRIMARY KEY (directive_id, external_id)
);
CREATE INDEX IF NOT EXISTS idx_dwi_directive_status
    ON directive_work_items(directive_id, status);
CREATE INDEX IF NOT EXISTS idx_dwi_session
    ON directive_work_items(session_id);
"#;

const MIGRATION_011: &str = r#"
-- ---------------------------------------------------------------------------
-- Channel Adapter — audit log and rate limiting
-- ---------------------------------------------------------------------------

-- Channel messages audit log — records all inbound and outbound messages
-- across all channels for debugging, compliance, and thread history.
CREATE TABLE IF NOT EXISTS channel_messages (
    id          TEXT PRIMARY KEY,
    direction   TEXT NOT NULL CHECK(direction IN ('inbound', 'outbound')),
    channel     TEXT NOT NULL,
    channel_id  TEXT NOT NULL,
    user_id     TEXT NOT NULL DEFAULT '',
    user_name   TEXT NOT NULL DEFAULT '',
    thread_id   TEXT,
    action      TEXT NOT NULL,
    content     TEXT NOT NULL,
    context_json TEXT NOT NULL DEFAULT '{}',
    created_at  INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_channel_messages_channel_thread
    ON channel_messages(channel, thread_id, created_at);
CREATE INDEX IF NOT EXISTS idx_channel_messages_user
    ON channel_messages(user_id, created_at);
CREATE INDEX IF NOT EXISTS idx_channel_messages_created
    ON channel_messages(created_at);

-- Rate limit tracking — sliding window token bucket.
-- Entries older than 2 minutes are periodically cleaned up.
CREATE TABLE IF NOT EXISTS channel_rate_limits (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    rate_key    TEXT NOT NULL,
    created_at  INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_channel_rate_limits_key
    ON channel_rate_limits(rate_key, created_at);
"#;

/// Migration 012: session_trace table + update workflow_sessions and session_messages
/// CHECK constraints to include 'clarifying' status.
async fn migrate_012(pool: &SqlitePool) -> Result<()> {
    // 1. Check if workflow_sessions already has 'clarifying' in its CHECK.
    let has_clarifying: bool = {
        let row: Option<(String,)> = sqlx::query_as(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='workflow_sessions'"
        )
        .fetch_optional(pool)
        .await
        .unwrap_or(None);

        row.map(|(sql,)| sql.contains("clarifying")).unwrap_or(false)
    };

    if !has_clarifying {
        sqlx::query("PRAGMA foreign_keys = OFF")
            .execute(pool)
            .await
            .context("migration 012: disable foreign_keys")?;

        let mut tx = pool.begin().await.context("migration 012: begin tx")?;

        // Drop session_trace first if it exists (it may have a bad FK from a previous run).
        sqlx::query("DROP TABLE IF EXISTS session_trace")
            .execute(&mut *tx)
            .await
            .context("migration 012: drop old session_trace")?;

        // Recreate workflow_sessions with updated CHECK.
        sqlx::query("ALTER TABLE workflow_sessions RENAME TO _ws_old_012")
            .execute(&mut *tx)
            .await
            .context("migration 012: rename workflow_sessions")?;

        sqlx::query(
            "CREATE TABLE workflow_sessions (
                id                TEXT PRIMARY KEY,
                workflow_id       TEXT NOT NULL REFERENCES workflows(id),
                status            TEXT NOT NULL CHECK(status IN (
                                      'created', 'planning', 'executing', 'evaluating',
                                      'adapting', 'waiting_for_human', 'clarifying',
                                      'completed', 'failed', 'cancelled'
                                  )),
                trigger_type      TEXT NOT NULL,
                trigger_data      TEXT,
                plan              TEXT,
                step_outputs      TEXT NOT NULL DEFAULT '{}',
                current_step_id   TEXT,
                retry_count       INTEGER NOT NULL DEFAULT 0,
                max_retries       INTEGER NOT NULL DEFAULT 3,
                evaluator_feedback TEXT,
                escalation        TEXT,
                started_at        INTEGER NOT NULL,
                completed_at      INTEGER,
                updated_at        INTEGER NOT NULL
            )"
        )
        .execute(&mut *tx)
        .await
        .context("migration 012: create new workflow_sessions")?;

        sqlx::query("INSERT INTO workflow_sessions SELECT * FROM _ws_old_012")
            .execute(&mut *tx)
            .await
            .context("migration 012: copy workflow_sessions data")?;

        // Recreate session_messages so its FK points to the new workflow_sessions.
        sqlx::query("ALTER TABLE session_messages RENAME TO _sm_old_012")
            .execute(&mut *tx)
            .await
            .context("migration 012: rename session_messages")?;

        sqlx::query(
            "CREATE TABLE session_messages (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id  TEXT NOT NULL REFERENCES workflow_sessions(id),
                direction   TEXT NOT NULL CHECK(direction IN ('agent_to_human', 'human_to_agent')),
                content     TEXT NOT NULL,
                metadata    TEXT,
                created_at  INTEGER NOT NULL
            )"
        )
        .execute(&mut *tx)
        .await
        .context("migration 012: create new session_messages")?;

        sqlx::query("INSERT INTO session_messages SELECT * FROM _sm_old_012")
            .execute(&mut *tx)
            .await
            .context("migration 012: copy session_messages data")?;

        // Drop old tables.
        sqlx::query("DROP TABLE _sm_old_012").execute(&mut *tx).await
            .context("migration 012: drop old session_messages")?;
        sqlx::query("DROP TABLE _ws_old_012").execute(&mut *tx).await
            .context("migration 012: drop old workflow_sessions")?;

        // Recreate indexes.
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_sessions_status ON workflow_sessions(status)")
            .execute(&mut *tx).await.context("migration 012: recreate sessions status index")?;
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_sessions_workflow ON workflow_sessions(workflow_id, started_at DESC)")
            .execute(&mut *tx).await.context("migration 012: recreate sessions workflow index")?;
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_sessions_waiting ON workflow_sessions(status) WHERE status IN ('waiting_for_human', 'clarifying')")
            .execute(&mut *tx).await.context("migration 012: recreate sessions waiting index")?;
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_session_messages ON session_messages(session_id, created_at)")
            .execute(&mut *tx).await.context("migration 012: recreate session_messages index")?;

        tx.commit().await.context("migration 012: commit tx")?;

        sqlx::query("PRAGMA foreign_keys = ON")
            .execute(pool)
            .await
            .context("migration 012: re-enable foreign_keys")?;
    }

    // 2. Create session_trace AFTER table recreation so FK is correct.
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS session_trace (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id  TEXT NOT NULL,
            event_type  TEXT NOT NULL,
            step_id     TEXT,
            tool_name   TEXT,
            inputs      TEXT,
            output      TEXT,
            error       TEXT,
            duration_ms INTEGER,
            metadata    TEXT,
            created_at  INTEGER NOT NULL
        )"
    )
    .execute(pool)
    .await
    .context("migration 012: create session_trace")?;

    sqlx::query("CREATE INDEX IF NOT EXISTS idx_session_trace_session ON session_trace(session_id, created_at)")
        .execute(pool).await.context("migration 012: create session_trace index")?;
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_session_trace_step ON session_trace(session_id, step_id)")
        .execute(pool).await.context("migration 012: create session_trace step index")?;

    Ok(())
}

/// Create an in-memory SQLite pool with all migrations applied. For tests only.
#[cfg(test)]
pub async fn test_pool() -> SqlitePool {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .expect("failed to create in-memory SQLite pool");
    migrate(&pool).await.expect("migrations failed on test pool");
    pool
}
