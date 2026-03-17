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
async fn migrate(pool: &SqlitePool) -> Result<()> {
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
