// ---------------------------------------------------------------------------
// DB queries — typed wrappers around SQLite operations.
//
// Each function takes a &SqlitePool and returns domain types.
// All timestamps are Unix epoch seconds (i64).
// ---------------------------------------------------------------------------

use anyhow::Result;
use sqlx::SqlitePool;

// ---------------------------------------------------------------------------
// Connections (ephemeral — for presence tracking)
// ---------------------------------------------------------------------------

pub async fn upsert_connection(
    pool: &SqlitePool,
    id: &str,
    user_id: Option<&str>,
    viewing_mr: Option<&str>,
) -> Result<()> {
    let now = epoch_secs();
    sqlx::query(
        "INSERT INTO connections (id, user_id, connected_at, last_seen_at, viewing_mr)
         VALUES (?, ?, ?, ?, ?)
         ON CONFLICT(id) DO UPDATE SET
           user_id = excluded.user_id,
           last_seen_at = excluded.last_seen_at,
           viewing_mr = excluded.viewing_mr",
    )
    .bind(id)
    .bind(user_id)
    .bind(now)
    .bind(now)
    .bind(viewing_mr)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn remove_connection(pool: &SqlitePool, id: &str) -> Result<()> {
    sqlx::query("DELETE FROM connections WHERE id = ?")
        .bind(id)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn update_viewing_mr(
    pool: &SqlitePool,
    id: &str,
    viewing_mr: Option<&str>,
) -> Result<()> {
    let now = epoch_secs();
    sqlx::query("UPDATE connections SET viewing_mr = ?, last_seen_at = ? WHERE id = ?")
        .bind(viewing_mr)
        .bind(now)
        .bind(id)
        .execute(pool)
        .await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Review cache
// ---------------------------------------------------------------------------

pub async fn get_cached_review(
    pool: &SqlitePool,
    project_path: &str,
    mr_iid: i64,
    diff_hash: &str,
) -> Result<Option<(Vec<u8>, String)>> {
    let now = epoch_secs();
    let row: Option<(Vec<u8>, String)> = sqlx::query_as(
        "SELECT data, file_diff_hashes FROM review_cache
         WHERE project_path = ? AND mr_iid = ? AND diff_hash = ? AND expires_at > ?",
    )
    .bind(project_path)
    .bind(mr_iid)
    .bind(diff_hash)
    .bind(now)
    .fetch_optional(pool)
    .await?;
    Ok(row)
}

/// Get the most recent cached review for an MR regardless of diff hash.
/// Used for incremental re-review (per-file diff hash comparison).
pub async fn get_latest_cached_review(
    pool: &SqlitePool,
    project_path: &str,
    mr_iid: i64,
) -> Result<Option<(Vec<u8>, String, String)>> {
    let row: Option<(Vec<u8>, String, String)> = sqlx::query_as(
        "SELECT data, file_diff_hashes, diff_hash FROM review_cache
         WHERE project_path = ? AND mr_iid = ?
         ORDER BY created_at DESC LIMIT 1",
    )
    .bind(project_path)
    .bind(mr_iid)
    .fetch_optional(pool)
    .await?;
    Ok(row)
}

pub async fn save_cached_review(
    pool: &SqlitePool,
    project_path: &str,
    mr_iid: i64,
    diff_hash: &str,
    data: &[u8],
    file_diff_hashes: &str,
    ttl_days: u32,
) -> Result<()> {
    let now = epoch_secs();
    let expires_at = now + (ttl_days as i64 * 86400);

    sqlx::query(
        "INSERT INTO review_cache (project_path, mr_iid, diff_hash, data, file_diff_hashes, created_at, expires_at)
         VALUES (?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(project_path, mr_iid, diff_hash) DO UPDATE SET
           data = excluded.data,
           file_diff_hashes = excluded.file_diff_hashes,
           created_at = excluded.created_at,
           expires_at = excluded.expires_at",
    )
    .bind(project_path)
    .bind(mr_iid)
    .bind(diff_hash)
    .bind(data)
    .bind(file_diff_hashes)
    .bind(now)
    .bind(expires_at)
    .execute(pool)
    .await?;
    Ok(())
}

/// Evict oldest entries beyond the max count for a project.
pub async fn evict_old_reviews(
    pool: &SqlitePool,
    project_path: &str,
    max_count: u32,
) -> Result<u64> {
    let result = sqlx::query(
        "DELETE FROM review_cache WHERE id IN (
           SELECT id FROM review_cache
           WHERE project_path = ?
           ORDER BY created_at DESC
           LIMIT -1 OFFSET ?
         )",
    )
    .bind(project_path)
    .bind(max_count)
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}

/// Purge expired cache entries across all projects.
pub async fn purge_expired_reviews(pool: &SqlitePool) -> Result<u64> {
    let now = epoch_secs();
    let result = sqlx::query("DELETE FROM review_cache WHERE expires_at <= ?")
        .bind(now)
        .execute(pool)
        .await?;
    Ok(result.rows_affected())
}

/// Delete all cached reviews for a specific MR.
pub async fn invalidate_mr_review_cache(
    pool: &SqlitePool,
    project_path: &str,
    mr_iid: i64,
) -> Result<u64> {
    let result = sqlx::query(
        "DELETE FROM review_cache WHERE project_path = ? AND mr_iid = ?",
    )
    .bind(project_path)
    .bind(mr_iid)
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}

// ---------------------------------------------------------------------------
// Comment actions
// ---------------------------------------------------------------------------

pub async fn upsert_comment_action(
    pool: &SqlitePool,
    project_path: &str,
    mr_iid: i64,
    comment_id: &str,
    user_id: &str,
    action: &str,
    edited_body: Option<&str>,
    category: Option<&str>,
    severity: Option<&str>,
) -> Result<()> {
    let now = epoch_secs();
    sqlx::query(
        "INSERT INTO comment_actions (project_path, mr_iid, comment_id, user_id, action, edited_body, category, severity, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(project_path, mr_iid, comment_id, user_id) DO UPDATE SET
           action = excluded.action,
           edited_body = excluded.edited_body,
           category = COALESCE(excluded.category, comment_actions.category),
           severity = COALESCE(excluded.severity, comment_actions.severity),
           created_at = excluded.created_at",
    )
    .bind(project_path)
    .bind(mr_iid)
    .bind(comment_id)
    .bind(user_id)
    .bind(action)
    .bind(edited_body)
    .bind(category)
    .bind(severity)
    .bind(now)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn get_comment_actions(
    pool: &SqlitePool,
    project_path: &str,
    mr_iid: i64,
) -> Result<Vec<(String, String, String, Option<String>, i64, Option<String>, Option<String>)>> {
    let rows: Vec<(String, String, String, Option<String>, i64, Option<String>, Option<String>)> = sqlx::query_as(
        "SELECT comment_id, user_id, action, edited_body, created_at, category, severity
         FROM comment_actions
         WHERE project_path = ? AND mr_iid = ?
         ORDER BY created_at DESC",
    )
    .bind(project_path)
    .bind(mr_iid)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

// ---------------------------------------------------------------------------
// Team settings
// ---------------------------------------------------------------------------

pub async fn get_shared_triage(pool: &SqlitePool, project_path: &str) -> Result<bool> {
    let row: Option<(bool,)> = sqlx::query_as(
        "SELECT shared_triage FROM team_settings WHERE project_path = ?",
    )
    .bind(project_path)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|r| r.0).unwrap_or(false))
}

pub async fn set_shared_triage(
    pool: &SqlitePool,
    project_path: &str,
    enabled: bool,
) -> Result<()> {
    let now = epoch_secs();
    sqlx::query(
        "INSERT INTO team_settings (project_path, shared_triage, created_at, updated_at)
         VALUES (?, ?, ?, ?)
         ON CONFLICT(project_path) DO UPDATE SET
           shared_triage = excluded.shared_triage,
           updated_at = excluded.updated_at",
    )
    .bind(project_path)
    .bind(enabled)
    .bind(now)
    .bind(now)
    .execute(pool)
    .await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Review queue
// ---------------------------------------------------------------------------

pub async fn get_queue_items(
    pool: &SqlitePool,
    project_path: &str,
) -> Result<Vec<(i64, String, i64, f64, String, Option<String>, i64)>> {
    let rows: Vec<(i64, String, i64, f64, String, Option<String>, i64)> = sqlx::query_as(
        "SELECT id, project_path, mr_iid, priority_score, status, error, enqueued_at
         FROM review_queue WHERE project_path = ? ORDER BY priority_score DESC",
    )
    .bind(project_path)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

pub async fn enqueue_review(
    pool: &SqlitePool,
    project_path: &str,
    mr_iid: i64,
    priority_score: f64,
    mr_context: &[u8],
) -> Result<()> {
    let now = epoch_secs();
    sqlx::query(
        "INSERT INTO review_queue (project_path, mr_iid, priority_score, status, mr_context, enqueued_at)
         VALUES (?, ?, ?, 'queued', ?, ?)
         ON CONFLICT(project_path, mr_iid) DO UPDATE SET
           priority_score = excluded.priority_score,
           status = 'queued',
           enqueued_at = excluded.enqueued_at",
    )
    .bind(project_path)
    .bind(mr_iid)
    .bind(priority_score)
    .bind(mr_context)
    .bind(now)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn update_queue_status(
    pool: &SqlitePool,
    project_path: &str,
    mr_iid: i64,
    from_statuses: &[&str],
    to_status: &str,
) -> Result<u64> {
    // Build a dynamic IN clause for the from_statuses filter
    let placeholders: Vec<&str> = from_statuses.iter().map(|_| "?").collect();
    let sql = format!(
        "UPDATE review_queue SET status = ? WHERE project_path = ? AND mr_iid = ? AND status IN ({})",
        placeholders.join(", ")
    );
    let mut query = sqlx::query(&sql).bind(to_status).bind(project_path).bind(mr_iid);
    for s in from_statuses {
        query = query.bind(*s);
    }
    let result = query.execute(pool).await?;
    Ok(result.rows_affected())
}

pub async fn delete_queue_item(
    pool: &SqlitePool,
    project_path: &str,
    mr_iid: i64,
) -> Result<u64> {
    let result = sqlx::query(
        "DELETE FROM review_queue WHERE project_path = ? AND mr_iid = ?",
    )
    .bind(project_path)
    .bind(mr_iid)
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}

// ---------------------------------------------------------------------------
// Sandbox jobs
// ---------------------------------------------------------------------------

pub async fn get_sandbox_job(
    pool: &SqlitePool,
    id: &str,
) -> Result<Option<(String, String, i64, Option<String>, String, String, Option<String>, Option<String>, Option<String>, Option<String>, Option<String>, i64, i64)>> {
    let row = sqlx::query_as(
        "SELECT id, project_path, mr_iid, comment_id, status, strategy, container_id, fix_diff, test_output, commit_sha, error, created_at, updated_at
         FROM sandbox_jobs WHERE id = ?",
    )
    .bind(id)
    .fetch_optional(pool)
    .await?;
    Ok(row)
}

/// List cached reviews for a project, ordered by most recent first.
/// Returns metadata only (not the full review blob).
pub async fn list_cached_reviews(
    pool: &SqlitePool,
    project_path: &str,
) -> Result<Vec<(i64, String, i64, i64)>> {
    let now = epoch_secs();
    let rows: Vec<(i64, String, i64, i64)> = sqlx::query_as(
        "SELECT mr_iid, diff_hash, created_at, expires_at
         FROM review_cache
         WHERE project_path = ? AND expires_at > ?
         ORDER BY created_at DESC
         LIMIT 50",
    )
    .bind(project_path)
    .bind(now)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

/// Count indexed MRs and files for a project in the file index.
pub async fn get_file_index_stats(
    pool: &SqlitePool,
    project_id: i64,
) -> Result<(i64, i64)> {
    let row: Option<(i64, i64)> = sqlx::query_as(
        "SELECT COUNT(DISTINCT mr_iid), COUNT(*)
         FROM mr_changed_files
         WHERE project_id = ?",
    )
    .bind(project_id)
    .fetch_optional(pool)
    .await?;
    Ok(row.unwrap_or((0, 0)))
}

/// List sandbox jobs for a specific MR, ordered by most recent first.
pub async fn get_sandbox_jobs_for_mr(
    pool: &SqlitePool,
    project_path: &str,
    mr_iid: i64,
) -> Result<Vec<(String, String, Option<String>, String, Option<String>, Option<String>, i64, i64)>> {
    let rows = sqlx::query_as(
        "SELECT id, status, comment_id, strategy, commit_sha, error, created_at, updated_at
         FROM sandbox_jobs
         WHERE project_path = ? AND mr_iid = ?
         ORDER BY created_at DESC
         LIMIT 50",
    )
    .bind(project_path)
    .bind(mr_iid)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

// ---------------------------------------------------------------------------
// Events
// ---------------------------------------------------------------------------

pub async fn insert_sandbox_job(
    pool: &SqlitePool,
    id: &str,
    project_path: &str,
    mr_iid: i64,
    comment_id: Option<&str>,
    strategy: &str,
) -> Result<()> {
    let now = epoch_secs();
    sqlx::query(
        "INSERT INTO sandbox_jobs (id, project_path, mr_iid, comment_id, status, strategy, created_at, updated_at)
         VALUES (?, ?, ?, ?, 'pending', ?, ?, ?)",
    )
    .bind(id)
    .bind(project_path)
    .bind(mr_iid)
    .bind(comment_id)
    .bind(strategy)
    .bind(now)
    .bind(now)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn update_sandbox_job_status(
    pool: &SqlitePool,
    id: &str,
    status: &str,
    container_id: Option<&str>,
    fix_diff: Option<&str>,
    test_output: Option<&str>,
    commit_sha: Option<&str>,
    error: Option<&str>,
) -> Result<()> {
    let now = epoch_secs();
    sqlx::query(
        "UPDATE sandbox_jobs SET
           status = ?, container_id = COALESCE(?, container_id),
           fix_diff = COALESCE(?, fix_diff), test_output = COALESCE(?, test_output),
           commit_sha = COALESCE(?, commit_sha), error = COALESCE(?, error),
           updated_at = ?
         WHERE id = ?",
    )
    .bind(status)
    .bind(container_id)
    .bind(fix_diff)
    .bind(test_output)
    .bind(commit_sha)
    .bind(error)
    .bind(now)
    .bind(id)
    .execute(pool)
    .await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Events
// ---------------------------------------------------------------------------

pub async fn insert_event(
    pool: &SqlitePool,
    project_path: &str,
    mr_iid: Option<i64>,
    event_type: &str,
    user_id: Option<&str>,
    payload: Option<&[u8]>,
) -> Result<i64> {
    let now = epoch_secs();
    let result = sqlx::query(
        "INSERT INTO events (project_path, mr_iid, event_type, user_id, payload, created_at)
         VALUES (?, ?, ?, ?, ?, ?)",
    )
    .bind(project_path)
    .bind(mr_iid)
    .bind(event_type)
    .bind(user_id)
    .bind(payload)
    .bind(now)
    .execute(pool)
    .await?;
    Ok(result.last_insert_rowid())
}

pub async fn get_recent_events(
    pool: &SqlitePool,
    project_path: &str,
    mr_iid: Option<i64>,
    limit: i64,
) -> Result<Vec<(i64, String, Option<String>, Option<Vec<u8>>, i64)>> {
    let rows: Vec<(i64, String, Option<String>, Option<Vec<u8>>, i64)> = if let Some(iid) = mr_iid
    {
        sqlx::query_as(
            "SELECT id, event_type, user_id, payload, created_at
             FROM events
             WHERE project_path = ? AND mr_iid = ?
             ORDER BY created_at DESC LIMIT ?",
        )
        .bind(project_path)
        .bind(iid)
        .bind(limit)
        .fetch_all(pool)
        .await?
    } else {
        sqlx::query_as(
            "SELECT id, event_type, user_id, payload, created_at
             FROM events
             WHERE project_path = ?
             ORDER BY created_at DESC LIMIT ?",
        )
        .bind(project_path)
        .bind(limit)
        .fetch_all(pool)
        .await?
    };
    Ok(rows)
}

// ---------------------------------------------------------------------------
// Repo configs (cached .otto.json)
// ---------------------------------------------------------------------------

/// Get a cached repo config if it exists and hasn't expired.
/// Returns (config_json, formatted, sandbox_image, fetched_at).
pub async fn get_repo_config(
    pool: &SqlitePool,
    project_path: &str,
) -> Result<Option<(String, String, Option<String>, i64)>> {
    let now = epoch_secs();
    let row: Option<(String, String, Option<String>, i64)> = sqlx::query_as(
        "SELECT config_json, formatted, sandbox_image, fetched_at
         FROM repo_configs
         WHERE project_path = ? AND expires_at > ?",
    )
    .bind(project_path)
    .bind(now)
    .fetch_optional(pool)
    .await?;
    Ok(row)
}

/// Insert or update a cached repo config with TTL.
/// Use config_json="{}" and formatted="" for the null sentinel (no .otto.json in repo).
pub async fn upsert_repo_config(
    pool: &SqlitePool,
    project_path: &str,
    config_json: &str,
    formatted: &str,
    sandbox_image: Option<&str>,
    ttl_secs: i64,
) -> Result<()> {
    let now = epoch_secs();
    let expires_at = now + ttl_secs;
    sqlx::query(
        "INSERT INTO repo_configs (project_path, config_json, formatted, sandbox_image, fetched_at, expires_at)
         VALUES (?, ?, ?, ?, ?, ?)
         ON CONFLICT(project_path) DO UPDATE SET
           config_json = excluded.config_json,
           formatted = excluded.formatted,
           sandbox_image = excluded.sandbox_image,
           fetched_at = excluded.fetched_at,
           expires_at = excluded.expires_at",
    )
    .bind(project_path)
    .bind(config_json)
    .bind(formatted)
    .bind(sandbox_image)
    .bind(now)
    .bind(expires_at)
    .execute(pool)
    .await?;
    Ok(())
}

/// Delete a cached repo config (webhook invalidation).
pub async fn delete_repo_config(pool: &SqlitePool, project_path: &str) -> Result<()> {
    sqlx::query("DELETE FROM repo_configs WHERE project_path = ?")
        .bind(project_path)
        .execute(pool)
        .await?;
    Ok(())
}

/// List all cached repo configs (for admin API). Returns entries regardless of expiry.
pub async fn list_repo_configs(
    pool: &SqlitePool,
) -> Result<Vec<(String, String, String, Option<String>, i64, i64)>> {
    let rows: Vec<(String, String, String, Option<String>, i64, i64)> = sqlx::query_as(
        "SELECT project_path, config_json, formatted, sandbox_image, fetched_at, expires_at
         FROM repo_configs
         ORDER BY fetched_at DESC",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

// ---------------------------------------------------------------------------
// Setup recipes (cached AI setup command sequences)
// ---------------------------------------------------------------------------

/// Save a setup recipe after a successful AI setup loop.
/// Replaces any existing recipe for this project + image combination.
pub async fn upsert_setup_recipe(
    pool: &SqlitePool,
    project_path: &str,
    base_image: &str,
    commands: &[String],
    setup_steps: u32,
) -> Result<()> {
    let now = epoch_secs();
    let commands_json = serde_json::to_string(commands)
        .unwrap_or_else(|_| "[]".to_string());
    sqlx::query(
        "INSERT INTO setup_recipes (project_path, base_image, commands, setup_steps, created_at, last_used_at, use_count)
         VALUES (?, ?, ?, ?, ?, ?, 1)
         ON CONFLICT(project_path, base_image) DO UPDATE SET
           commands = excluded.commands,
           setup_steps = excluded.setup_steps,
           created_at = excluded.created_at,
           last_used_at = excluded.last_used_at,
           use_count = 1",
    )
    .bind(project_path)
    .bind(base_image)
    .bind(&commands_json)
    .bind(setup_steps as i64)
    .bind(now)
    .bind(now)
    .execute(pool)
    .await?;
    Ok(())
}

/// Fetch a cached setup recipe for a project + image.
/// Returns (commands_json, setup_steps, created_at, use_count) if found.
/// Does NOT check TTL — the caller decides whether the recipe is fresh enough.
pub async fn get_setup_recipe(
    pool: &SqlitePool,
    project_path: &str,
    base_image: &str,
) -> Result<Option<(String, i64, i64, i64)>> {
    let row: Option<(String, i64, i64, i64)> = sqlx::query_as(
        "SELECT commands, setup_steps, created_at, use_count
         FROM setup_recipes
         WHERE project_path = ? AND base_image = ?",
    )
    .bind(project_path)
    .bind(base_image)
    .fetch_optional(pool)
    .await?;
    Ok(row)
}

/// Bump last_used_at and use_count after a successful recipe replay.
pub async fn touch_setup_recipe(
    pool: &SqlitePool,
    project_path: &str,
    base_image: &str,
) -> Result<()> {
    let now = epoch_secs();
    sqlx::query(
        "UPDATE setup_recipes SET last_used_at = ?, use_count = use_count + 1
         WHERE project_path = ? AND base_image = ?",
    )
    .bind(now)
    .bind(project_path)
    .bind(base_image)
    .execute(pool)
    .await?;
    Ok(())
}

/// Delete a stale recipe when replay fails.
pub async fn delete_setup_recipe(
    pool: &SqlitePool,
    project_path: &str,
    base_image: &str,
) -> Result<()> {
    sqlx::query(
        "DELETE FROM setup_recipes WHERE project_path = ? AND base_image = ?",
    )
    .bind(project_path)
    .bind(base_image)
    .execute(pool)
    .await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Project knowledge (structured facts + AI-distilled notes)
// ---------------------------------------------------------------------------

/// Save structured project facts after a successful setup.
/// Preserves existing notes on conflict — re-extracting facts from a new
/// recipe shouldn't wipe out AI-distilled notes from a previous run.
pub async fn upsert_project_knowledge(
    pool: &SqlitePool,
    project_path: &str,
    base_image: &str,
    facts_json: &str,
    notes: Option<&str>,
    notes_model: Option<&str>,
) -> Result<()> {
    let now = epoch_secs();
    sqlx::query(
        "INSERT INTO project_knowledge (project_path, base_image, facts, notes, notes_model, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(project_path, base_image) DO UPDATE SET
           facts = excluded.facts,
           notes = COALESCE(excluded.notes, project_knowledge.notes),
           notes_model = COALESCE(excluded.notes_model, project_knowledge.notes_model),
           updated_at = excluded.updated_at",
    )
    .bind(project_path)
    .bind(base_image)
    .bind(facts_json)
    .bind(notes)
    .bind(notes_model)
    .bind(now)
    .bind(now)
    .execute(pool)
    .await?;
    Ok(())
}

/// Fetch project knowledge for prompt injection.
/// Returns (facts_json, notes, created_at) if found.
/// Does NOT check TTL — the caller decides freshness.
pub async fn get_project_knowledge(
    pool: &SqlitePool,
    project_path: &str,
    base_image: &str,
) -> Result<Option<(String, Option<String>, i64)>> {
    let row: Option<(String, Option<String>, i64)> = sqlx::query_as(
        "SELECT facts, notes, created_at
         FROM project_knowledge
         WHERE project_path = ? AND base_image = ?",
    )
    .bind(project_path)
    .bind(base_image)
    .fetch_optional(pool)
    .await?;
    Ok(row)
}

/// Update just the AI-distilled notes (Option B).
/// Called asynchronously after the distillation AI call completes.
/// The facts row must already exist (written by Option C first).
pub async fn update_project_notes(
    pool: &SqlitePool,
    project_path: &str,
    base_image: &str,
    notes: &str,
    notes_model: &str,
) -> Result<()> {
    let now = epoch_secs();
    sqlx::query(
        "UPDATE project_knowledge SET notes = ?, notes_model = ?, updated_at = ?
         WHERE project_path = ? AND base_image = ?",
    )
    .bind(notes)
    .bind(notes_model)
    .bind(now)
    .bind(project_path)
    .bind(base_image)
    .execute(pool)
    .await?;
    Ok(())
}

/// Delete project knowledge (manual invalidation).
pub async fn delete_project_knowledge(
    pool: &SqlitePool,
    project_path: &str,
    base_image: &str,
) -> Result<()> {
    sqlx::query(
        "DELETE FROM project_knowledge WHERE project_path = ? AND base_image = ?",
    )
    .bind(project_path)
    .bind(base_image)
    .execute(pool)
    .await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Reviewer preferences (team-wide learned patterns)
// ---------------------------------------------------------------------------

/// Get cached reviewer prefs for a project. Returns (prefs_text, updated_at).
pub async fn get_reviewer_prefs(
    pool: &SqlitePool,
    project_path: &str,
) -> Result<Option<(String, i64)>> {
    let row: Option<(Vec<u8>, i64)> = sqlx::query_as(
        "SELECT prefs, updated_at FROM reviewer_prefs WHERE project_path = ? AND host_url = ''",
    )
    .bind(project_path)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(blob, ts)| (String::from_utf8_lossy(&blob).into_owned(), ts)))
}

/// Upsert cached reviewer prefs for a project.
pub async fn upsert_reviewer_prefs(
    pool: &SqlitePool,
    project_path: &str,
    prefs_text: &str,
) -> Result<()> {
    let now = epoch_secs();
    sqlx::query(
        "INSERT INTO reviewer_prefs (project_path, host_url, prefs, updated_at)
         VALUES (?, '', ?, ?)
         ON CONFLICT(project_path, host_url) DO UPDATE SET
           prefs = excluded.prefs,
           updated_at = excluded.updated_at",
    )
    .bind(project_path)
    .bind(prefs_text.as_bytes())
    .bind(now)
    .execute(pool)
    .await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// MR changed files (shared index for Conflict Radar + Cross-MR Clusters)
// ---------------------------------------------------------------------------

/// Upsert a single changed file entry for an MR.
/// Called from webhook handlers and review pipeline side-effects.
pub async fn upsert_mr_changed_file(
    pool: &SqlitePool,
    project_id: i64,
    mr_iid: i64,
    file_path: &str,
    old_path: Option<&str>,
    change_type: &str,
    diff_hash: &str,
    hunks_json: &str,
) -> Result<()> {
    let now = epoch_secs();
    sqlx::query(
        "INSERT INTO mr_changed_files (project_id, mr_iid, file_path, old_path, change_type, diff_hash, hunks, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(project_id, mr_iid, file_path) DO UPDATE SET
           old_path = excluded.old_path,
           change_type = excluded.change_type,
           diff_hash = excluded.diff_hash,
           hunks = excluded.hunks,
           updated_at = excluded.updated_at",
    )
    .bind(project_id)
    .bind(mr_iid)
    .bind(file_path)
    .bind(old_path)
    .bind(change_type)
    .bind(diff_hash)
    .bind(hunks_json)
    .bind(now)
    .execute(pool)
    .await?;
    Ok(())
}

/// Delete all changed file entries for an MR (on merge or close).
pub async fn delete_mr_changed_files(
    pool: &SqlitePool,
    project_id: i64,
    mr_iid: i64,
) -> Result<u64> {
    let result = sqlx::query(
        "DELETE FROM mr_changed_files WHERE project_id = ? AND mr_iid = ?",
    )
    .bind(project_id)
    .bind(mr_iid)
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}

/// Get all changed files for a specific MR.
/// Returns (file_path, old_path, change_type, diff_hash, hunks_json, updated_at).
pub async fn get_mr_changed_files(
    pool: &SqlitePool,
    project_id: i64,
    mr_iid: i64,
) -> Result<Vec<(String, Option<String>, String, String, String, i64)>> {
    let rows: Vec<(String, Option<String>, String, String, String, i64)> = sqlx::query_as(
        "SELECT file_path, old_path, change_type, diff_hash, hunks, updated_at
         FROM mr_changed_files
         WHERE project_id = ? AND mr_iid = ?",
    )
    .bind(project_id)
    .bind(mr_iid)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

/// Find all other MRs in the same project that touch any of the same files.
/// Returns (mr_iid, file_path, old_path, change_type, diff_hash, hunks_json)
/// for every overlapping file in other MRs. The caller computes hunk-level
/// overlap in memory.
///
/// Uses a subquery to first find the file paths for the given MR, then joins
/// back to find other MRs touching those paths. This is efficient because
/// the idx_mcf_project_file index covers the join.
pub async fn get_conflicting_mr_files(
    pool: &SqlitePool,
    project_id: i64,
    mr_iid: i64,
) -> Result<Vec<(i64, String, Option<String>, String, String, String)>> {
    let rows: Vec<(i64, String, Option<String>, String, String, String)> = sqlx::query_as(
        "SELECT other.mr_iid, other.file_path, other.old_path, other.change_type, other.diff_hash, other.hunks
         FROM mr_changed_files other
         WHERE other.project_id = ?
           AND other.mr_iid != ?
           AND other.file_path IN (
               SELECT file_path FROM mr_changed_files
               WHERE project_id = ? AND mr_iid = ?
           )",
    )
    .bind(project_id)
    .bind(mr_iid)
    .bind(project_id)
    .bind(mr_iid)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

/// Get all changed file paths for all MRs in a project.
/// Used by ClusterDetector's FileOverlapStrategy to compute Jaccard similarity.
/// Returns (mr_iid, file_path) pairs — lightweight, no hunks needed.
pub async fn get_project_mr_file_paths(
    pool: &SqlitePool,
    project_id: i64,
) -> Result<Vec<(i64, String)>> {
    let rows: Vec<(i64, String)> = sqlx::query_as(
        "SELECT mr_iid, file_path FROM mr_changed_files WHERE project_id = ?",
    )
    .bind(project_id)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

/// Get all distinct MR IIDs in the file index for a project.
/// Used to know which MRs are currently tracked (for cleanup, etc.).
pub async fn get_indexed_mr_iids(
    pool: &SqlitePool,
    project_id: i64,
) -> Result<Vec<i64>> {
    let rows: Vec<(i64,)> = sqlx::query_as(
        "SELECT DISTINCT mr_iid FROM mr_changed_files WHERE project_id = ?",
    )
    .bind(project_id)
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(|(iid,)| iid).collect())
}

// ---------------------------------------------------------------------------
// MR clusters
// ---------------------------------------------------------------------------

/// Upsert a cluster. member_mrs and signals are JSON strings.
/// summary_json and review_order_json are optional gzip-compressed blobs.
pub async fn upsert_cluster(
    pool: &SqlitePool,
    id: &str,
    project_id: i64,
    ticket_key: Option<&str>,
    member_mrs_json: &str,
    signals_json: &str,
    relevance_score: f64,
    ttl_days: u32,
) -> Result<()> {
    let now = epoch_secs();
    let expires_at = now + (ttl_days as i64 * 86400);
    sqlx::query(
        "INSERT INTO mr_clusters (id, project_id, ticket_key, member_mrs, signals, relevance_score, created_at, updated_at, expires_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(id) DO UPDATE SET
           ticket_key = excluded.ticket_key,
           member_mrs = excluded.member_mrs,
           signals = excluded.signals,
           relevance_score = excluded.relevance_score,
           updated_at = excluded.updated_at,
           expires_at = excluded.expires_at",
    )
    .bind(id)
    .bind(project_id)
    .bind(ticket_key)
    .bind(member_mrs_json)
    .bind(signals_json)
    .bind(relevance_score)
    .bind(now)
    .bind(now)
    .bind(expires_at)
    .execute(pool)
    .await?;
    Ok(())
}

/// Get all clusters containing a specific MR.
/// Uses a JSON search on the member_mrs column — SQLite's json_each is
/// available in modern SQLite but we use a LIKE match for broader compat.
/// Returns (id, project_id, ticket_key, member_mrs, signals, relevance_score,
///          summary_json, summary_diff_hash, review_order_json, updated_at).
pub async fn get_clusters_for_mr(
    pool: &SqlitePool,
    project_id: i64,
    mr_iid: i64,
) -> Result<Vec<(String, i64, Option<String>, String, String, f64, Option<Vec<u8>>, Option<String>, Option<Vec<u8>>, i64)>> {
    let now = epoch_secs();
    // Match MR IID in the JSON array. The member_mrs column contains compact
    // serde_json like [{"mrIid":42,"mrTitle":"..."},...]. After the number, the
    // next char is always ',' (next field) or '}' (end of object). We match both
    // to avoid false positives (e.g. mrIid 42 matching 421).
    let pattern_comma = format!("%\"mrIid\":{},%", mr_iid);
    let pattern_brace = format!("%\"mrIid\":{}}}%", mr_iid);
    let rows = sqlx::query_as(
        "SELECT id, project_id, ticket_key, member_mrs, signals, relevance_score,
                summary_json, summary_diff_hash, review_order_json, updated_at
         FROM mr_clusters
         WHERE project_id = ?
           AND (member_mrs LIKE ? OR member_mrs LIKE ?)
           AND expires_at > ?",
    )
    .bind(project_id)
    .bind(&pattern_comma)
    .bind(&pattern_brace)
    .bind(now)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

/// Get a cluster by its ID.
/// Returns (id, project_id, ticket_key, member_mrs, signals, relevance_score,
///          summary_json, summary_diff_hash, review_order_json, updated_at).
pub async fn get_cluster_by_id(
    pool: &SqlitePool,
    id: &str,
) -> Result<Option<(String, i64, Option<String>, String, String, f64, Option<Vec<u8>>, Option<String>, Option<Vec<u8>>, i64)>> {
    let now = epoch_secs();
    let row = sqlx::query_as(
        "SELECT id, project_id, ticket_key, member_mrs, signals, relevance_score,
                summary_json, summary_diff_hash, review_order_json, updated_at
         FROM mr_clusters
         WHERE id = ? AND expires_at > ?",
    )
    .bind(id)
    .bind(now)
    .fetch_optional(pool)
    .await?;
    Ok(row)
}

/// Delete a cluster by ID.
pub async fn delete_cluster(pool: &SqlitePool, id: &str) -> Result<u64> {
    let result = sqlx::query("DELETE FROM mr_clusters WHERE id = ?")
        .bind(id)
        .execute(pool)
        .await?;
    Ok(result.rows_affected())
}

/// Delete all clusters for a project (cleanup).
pub async fn delete_project_clusters(pool: &SqlitePool, project_id: i64) -> Result<u64> {
    let result = sqlx::query("DELETE FROM mr_clusters WHERE project_id = ?")
        .bind(project_id)
        .execute(pool)
        .await?;
    Ok(result.rows_affected())
}

/// Update the cached summary for a cluster.
pub async fn update_cluster_summary(
    pool: &SqlitePool,
    id: &str,
    summary_json: &[u8],
    summary_diff_hash: &str,
) -> Result<()> {
    let now = epoch_secs();
    sqlx::query(
        "UPDATE mr_clusters SET summary_json = ?, summary_diff_hash = ?, updated_at = ?
         WHERE id = ?",
    )
    .bind(summary_json)
    .bind(summary_diff_hash)
    .bind(now)
    .bind(id)
    .execute(pool)
    .await?;
    Ok(())
}

/// Update the cached review order for a cluster.
pub async fn update_cluster_review_order(
    pool: &SqlitePool,
    id: &str,
    review_order_json: &[u8],
) -> Result<()> {
    let now = epoch_secs();
    sqlx::query(
        "UPDATE mr_clusters SET review_order_json = ?, updated_at = ?
         WHERE id = ?",
    )
    .bind(review_order_json)
    .bind(now)
    .bind(id)
    .execute(pool)
    .await?;
    Ok(())
}

/// Purge expired clusters across all projects.
pub async fn purge_expired_clusters(pool: &SqlitePool) -> Result<u64> {
    let now = epoch_secs();
    let result = sqlx::query("DELETE FROM mr_clusters WHERE expires_at <= ?")
        .bind(now)
        .execute(pool)
        .await?;
    Ok(result.rows_affected())
}

/// Purge expired digest entries across all projects.
pub async fn purge_expired_digests(pool: &SqlitePool) -> Result<u64> {
    let now = epoch_secs();
    let result = sqlx::query("DELETE FROM digests WHERE expires_at <= ?")
        .bind(now)
        .execute(pool)
        .await?;
    Ok(result.rows_affected())
}

/// Purge old events (older than 30 days) to prevent unbounded growth.
pub async fn purge_old_events(pool: &SqlitePool) -> Result<u64> {
    let cutoff = epoch_secs() - (30 * 86400); // 30 days
    let result = sqlx::query("DELETE FROM events WHERE created_at < ?")
        .bind(cutoff)
        .execute(pool)
        .await?;
    Ok(result.rows_affected())
}

/// Purge old completed/errored sandbox jobs (older than 30 days).
pub async fn purge_old_sandbox_jobs(pool: &SqlitePool) -> Result<u64> {
    let cutoff = epoch_secs() - (30 * 86400); // 30 days
    let result = sqlx::query(
        "DELETE FROM sandbox_jobs WHERE status IN ('complete', 'error', 'failed') AND created_at < ?",
    )
    .bind(cutoff)
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}

/// Purge expired repo configs.
pub async fn purge_expired_repo_configs(pool: &SqlitePool) -> Result<u64> {
    let now = epoch_secs();
    let result = sqlx::query("DELETE FROM repo_configs WHERE expires_at <= ?")
        .bind(now)
        .execute(pool)
        .await?;
    Ok(result.rows_affected())
}

/// List recent workflow runs, ordered by most recent first.
pub async fn list_workflow_runs(
    pool: &SqlitePool,
    limit: i64,
) -> Result<Vec<(String, String, String, Option<String>, String, String, Option<String>, i64, Option<i64>)>> {
    let rows = sqlx::query_as(
        "SELECT id, workflow_id, trigger_type, trigger_data, status, step_states, final_verification, started_at, completed_at
         FROM workflow_runs
         ORDER BY started_at DESC
         LIMIT ?",
    )
    .bind(limit)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

/// List all workflows (optionally filtered by enabled status).
pub async fn list_workflows(
    pool: &SqlitePool,
    enabled_only: bool,
) -> Result<Vec<(String, String, String, Option<i64>, String, bool, Option<String>, i64, i64)>> {
    let rows = if enabled_only {
        sqlx::query_as(
            "SELECT id, name, description, project_id, definition, enabled, created_by, created_at, updated_at
             FROM workflows WHERE enabled = 1 ORDER BY name",
        )
        .fetch_all(pool)
        .await?
    } else {
        sqlx::query_as(
            "SELECT id, name, description, project_id, definition, enabled, created_by, created_at, updated_at
             FROM workflows ORDER BY name",
        )
        .fetch_all(pool)
        .await?
    };
    Ok(rows)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn epoch_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

/// Public accessor for modules that need timestamps (e.g. prefs staleness check).
pub fn epoch_secs_pub() -> i64 {
    epoch_secs()
}
