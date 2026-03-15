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
