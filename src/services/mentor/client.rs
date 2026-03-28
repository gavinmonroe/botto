// ---------------------------------------------------------------------------
// MentorClient — query/remember/forget interface to the Mentor knowledge store.
//
// All operations go through SQLite + FTS5. The client is scoped to a
// particular repo at construction time, and query resolution automatically
// fans out to linked repos and global scope.
// ---------------------------------------------------------------------------

use anyhow::{Context, Result};
use sqlx::SqlitePool;
use tracing::{debug, warn};
use uuid::Uuid;

/// A single knowledge entry returned from a Mentor query.
#[derive(Debug, Clone)]
pub struct MentorQueryResult {
    pub id: String,
    pub content: String,
    pub scope: String,
    pub scope_type: String,
    pub category: String,
    pub confidence: f64,
    pub hit_count: i64,
    /// FTS5 relevance rank (lower = more relevant).
    pub rank: f64,
}

/// Opaque entry ID for forget operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MentorEntryId(pub String);

impl std::fmt::Display for MentorEntryId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// The Mentor client — scoped to a repo, backed by SQLite + FTS5.
///
/// Cheap to clone (pool is Arc-wrapped internally by sqlx).
#[derive(Debug, Clone)]
pub struct MentorClient {
    pool: SqlitePool,
    current_repo: String,
}

impl MentorClient {
    /// Create a new MentorClient scoped to the given repo.
    pub fn new(pool: SqlitePool, current_repo: String) -> Self {
        Self { pool, current_repo }
    }

    /// The repo this client is scoped to.
    pub fn current_repo(&self) -> &str {
        &self.current_repo
    }

    // -----------------------------------------------------------------------
    // Query — semantic search across scoped knowledge
    // -----------------------------------------------------------------------

    /// Search for relevant knowledge. Resolution order:
    /// 1. Repo-scoped entries for the current repo
    /// 2. Entries scoped to sibling repos in any linked set
    /// 3. Global entries
    ///
    /// Results are ranked by FTS5 relevance * confidence * recency.
    /// Returns up to `limit` results (default 10).
    pub async fn query(&self, question: &str, limit: u32) -> Result<Vec<MentorQueryResult>> {
        let limit = if limit == 0 { 10 } else { limit };

        // Fix #8: if the sanitized query is empty, return immediately to avoid
        // a SQLite "malformed MATCH expression" error.
        let fts_query = sanitize_fts_query(question);
        if fts_query.is_empty() {
            debug!("mentor query: empty query after sanitization, returning no results");
            return Ok(Vec::new());
        }

        // Resolve linked scopes: find all repos in linked sets that include
        // the current repo, so we can fan out the search.
        let linked_scopes = self.resolve_linked_scopes().await?;

        // Build the scope list: current repo + linked siblings + "global".
        let mut scopes = vec![self.current_repo.clone(), "global".to_string()];
        scopes.extend(linked_scopes);

        // FTS5 query — match against content. We use the MATCH syntax and
        // rank by bm25() * confidence, with a recency boost.
        //
        // The placeholders for the IN clause are built dynamically since sqlx
        // doesn't support binding a Vec directly for IN.
        let placeholders: String = scopes.iter().map(|_| "?").collect::<Vec<_>>().join(", ");

        let sql = format!(
            "SELECT
                me.id,
                me.content,
                me.scope,
                me.scope_type,
                me.category,
                me.confidence,
                me.hit_count,
                rank
             FROM mentor_fts
             JOIN mentor_entries me ON mentor_fts.rowid = me.rowid
             WHERE mentor_fts MATCH ?
               AND me.scope IN ({placeholders})
               AND me.confidence > 0.0
             ORDER BY rank * (1.0 / COALESCE(me.confidence, 0.01))
             LIMIT ?"
        );

        let mut query = sqlx::query_as::<_, (String, String, String, String, String, f64, i64, f64)>(&sql);

        // fts_query was already sanitized above (and checked for empty).
        query = query.bind(fts_query);

        for scope in &scopes {
            query = query.bind(scope.clone());
        }
        query = query.bind(limit);

        let rows = query
            .fetch_all(&self.pool)
            .await
            .context("mentor query failed")?;

        // Bump hit_count and last_queried_at for returned entries.
        let now = epoch_secs();
        for (id, ..) in &rows {
            if let Err(e) = self.bump_hit(id, now).await {
                warn!(entry_id = %id, "failed to bump mentor hit count: {e}");
            }
        }

        let results = rows
            .into_iter()
            .map(|(id, content, scope, scope_type, category, confidence, hit_count, rank)| {
                MentorQueryResult {
                    id,
                    content,
                    scope,
                    scope_type,
                    category,
                    confidence,
                    hit_count,
                    rank,
                }
            })
            .collect();

        Ok(results)
    }

    // -----------------------------------------------------------------------
    // Remember — store new knowledge
    // -----------------------------------------------------------------------

    /// Store a new knowledge entry. Returns the entry ID.
    ///
    /// - `scope`: repo path, linked-set name, or "global"
    /// - `scope_type`: "repo", "linked", or "global"
    /// - `category`: "execution", "domain", "workflow", or "correction"
    pub async fn remember(
        &self,
        content: &str,
        scope: &str,
        scope_type: &str,
        category: &str,
        source_workflow_id: Option<&str>,
        source_step_id: Option<&str>,
    ) -> Result<MentorEntryId> {
        let id = Uuid::new_v4().to_string();
        let now = epoch_secs();

        sqlx::query(
            "INSERT INTO mentor_entries
                (id, content, scope, scope_type, category, source_workflow_id, source_step_id, created_at, hit_count, confidence)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, 1.0)",
        )
        .bind(&id)
        .bind(content)
        .bind(scope)
        .bind(scope_type)
        .bind(category)
        .bind(source_workflow_id)
        .bind(source_step_id)
        .bind(now)
        .execute(&self.pool)
        .await
        .context("mentor remember failed")?;

        debug!(entry_id = %id, scope, category, "mentor: remembered new entry");
        Ok(MentorEntryId(id))
    }

    /// Convenience: remember something scoped to the current repo.
    pub async fn remember_for_repo(
        &self,
        content: &str,
        category: &str,
        source_workflow_id: Option<&str>,
        source_step_id: Option<&str>,
    ) -> Result<MentorEntryId> {
        self.remember(
            content,
            &self.current_repo,
            "repo",
            category,
            source_workflow_id,
            source_step_id,
        )
        .await
    }

    // -----------------------------------------------------------------------
    // Forget — remove outdated or wrong knowledge
    // -----------------------------------------------------------------------

    /// Delete a knowledge entry by ID.
    pub async fn forget(&self, entry_id: &MentorEntryId) -> Result<bool> {
        let result = sqlx::query("DELETE FROM mentor_entries WHERE id = ?")
            .bind(&entry_id.0)
            .execute(&self.pool)
            .await
            .context("mentor forget failed")?;

        let deleted = result.rows_affected() > 0;
        if deleted {
            debug!(entry_id = %entry_id, "mentor: forgot entry");
        }
        Ok(deleted)
    }

    // -----------------------------------------------------------------------
    // Pruning — decay confidence and remove stale entries
    // -----------------------------------------------------------------------

    /// Decay confidence for entries that haven't been queried recently.
    /// Entries older than `max_age_secs` that have never been queried get a
    /// steeper decay. Returns the number of entries updated.
    pub async fn decay_confidence(&self, decay_factor: f64) -> Result<u64> {
        let result = sqlx::query(
            "UPDATE mentor_entries
             SET confidence = confidence * ?
             WHERE last_queried_at IS NULL
               OR last_queried_at < (unixepoch() - 86400)",
        )
        .bind(decay_factor)
        .execute(&self.pool)
        .await
        .context("mentor confidence decay failed")?;

        Ok(result.rows_affected())
    }

    /// Prune entries below the confidence threshold. Returns the number of
    /// entries deleted.
    pub async fn prune_below_confidence(&self, threshold: f64) -> Result<u64> {
        let result = sqlx::query(
            "DELETE FROM mentor_entries WHERE confidence < ?",
        )
        .bind(threshold)
        .execute(&self.pool)
        .await
        .context("mentor prune failed")?;

        let count = result.rows_affected();
        if count > 0 {
            debug!(count, threshold, "mentor: pruned low-confidence entries");
        }
        Ok(count)
    }

    // -----------------------------------------------------------------------
    // Stats
    // -----------------------------------------------------------------------

    /// Count total entries, optionally filtered by scope.
    pub async fn count(&self, scope: Option<&str>) -> Result<i64> {
        let count: (i64,) = if let Some(scope) = scope {
            sqlx::query_as("SELECT COUNT(*) FROM mentor_entries WHERE scope = ?")
                .bind(scope)
                .fetch_one(&self.pool)
                .await?
        } else {
            sqlx::query_as("SELECT COUNT(*) FROM mentor_entries")
                .fetch_one(&self.pool)
                .await?
        };
        Ok(count.0)
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /// Resolve all repos linked to the current repo via mentor_repo_links.
    async fn resolve_linked_scopes(&self) -> Result<Vec<String>> {
        // Find all link_names that include the current repo.
        let link_names: Vec<(String,)> = sqlx::query_as(
            "SELECT DISTINCT link_name FROM mentor_repo_links WHERE repo_path = ?",
        )
        .bind(&self.current_repo)
        .fetch_all(&self.pool)
        .await?;

        if link_names.is_empty() {
            return Ok(Vec::new());
        }

        // For each link_name, get all sibling repos (excluding current).
        let mut siblings = Vec::new();
        for (link_name,) in &link_names {
            let repos: Vec<(String,)> = sqlx::query_as(
                "SELECT repo_path FROM mentor_repo_links
                 WHERE link_name = ? AND repo_path != ?",
            )
            .bind(link_name)
            .bind(&self.current_repo)
            .fetch_all(&self.pool)
            .await?;

            for (repo,) in repos {
                if !siblings.contains(&repo) {
                    siblings.push(repo);
                }
            }

            // Also include the link_name itself as a scope (for entries
            // scoped to the linked set rather than a specific repo).
            if !siblings.contains(link_name) {
                siblings.push(link_name.clone());
            }
        }

        Ok(siblings)
    }

    /// Bump hit_count and last_queried_at for an entry.
    /// Fix #10: use MAX(..., 0.01) so entries at 0.0 confidence can recover.
    async fn bump_hit(&self, entry_id: &str, now: i64) -> Result<()> {
        sqlx::query(
            "UPDATE mentor_entries
             SET hit_count = hit_count + 1,
                 last_queried_at = ?,
                 confidence = MIN(MAX(confidence * 1.05, 0.01), 1.0)
             WHERE id = ?",
        )
        .bind(now)
        .bind(entry_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Feedback — mark entries as helpful or unhelpful
    // -----------------------------------------------------------------------

    /// Mark an entry as helpful — boosts confidence significantly.
    /// Fix #10: use MAX(..., 0.01) so entries at 0.0 confidence can recover.
    pub async fn mark_helpful(&self, entry_id: &MentorEntryId) -> Result<bool> {
        let now = epoch_secs();
        let result = sqlx::query(
            "UPDATE mentor_entries
             SET confidence = MIN(MAX(confidence * 1.2, 0.01), 1.0),
                 hit_count = hit_count + 1,
                 last_queried_at = ?
             WHERE id = ?",
        )
        .bind(now)
        .bind(&entry_id.0)
        .execute(&self.pool)
        .await
        .context("mentor mark_helpful failed")?;

        let updated = result.rows_affected() > 0;
        if updated {
            debug!(entry_id = %entry_id, "mentor: marked entry as helpful");
        }
        Ok(updated)
    }

    /// Mark an entry as unhelpful — reduces confidence significantly.
    pub async fn mark_unhelpful(&self, entry_id: &MentorEntryId) -> Result<bool> {
        let now = epoch_secs();
        let result = sqlx::query(
            "UPDATE mentor_entries
             SET confidence = confidence * 0.5,
                 last_queried_at = ?
             WHERE id = ?",
        )
        .bind(now)
        .bind(&entry_id.0)
        .execute(&self.pool)
        .await
        .context("mentor mark_unhelpful failed")?;

        let updated = result.rows_affected() > 0;
        if updated {
            debug!(entry_id = %entry_id, "mentor: marked entry as unhelpful");
        }
        Ok(updated)
    }
}

// ---------------------------------------------------------------------------
// FTS5 query sanitization
// ---------------------------------------------------------------------------

/// Sanitize user input for FTS5 MATCH queries.
/// Strips all non-alphanumeric/non-whitespace characters from each word,
/// then wraps each word in double quotes to prevent FTS5 syntax errors.
/// Returns an empty string if no usable words remain.
fn sanitize_fts_query(input: &str) -> String {
    let words: Vec<String> = input
        .split_whitespace()
        .filter(|w| !w.is_empty())
        .map(|w| {
            // Fix #9: strip all chars that have special FTS5 meaning.
            // Keep only alphanumeric and underscore.
            let clean: String = w.chars().filter(|c| c.is_alphanumeric() || *c == '_').collect();
            clean
        })
        .filter(|w| !w.is_empty())
        .map(|w| format!("\"{w}\""))
        .collect();

    if words.is_empty() {
        // Fix #8: return empty string so the caller can bail out early.
        String::new()
    } else {
        words.join(" OR ")
    }
}

// ---------------------------------------------------------------------------
// Utility
// ---------------------------------------------------------------------------

fn epoch_secs() -> i64 {
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

    #[test]
    fn sanitize_fts_simple() {
        assert_eq!(sanitize_fts_query("hello world"), "\"hello\" OR \"world\"");
    }

    #[test]
    fn sanitize_fts_special_chars() {
        // FTS5 keywords and special chars are stripped to just alphanumeric
        assert_eq!(
            sanitize_fts_query("rate AND limit"),
            "\"rate\" OR \"AND\" OR \"limit\""
        );
    }

    #[test]
    fn sanitize_fts_empty() {
        assert_eq!(sanitize_fts_query(""), "");
        assert_eq!(sanitize_fts_query("   "), "");
    }

    #[test]
    fn sanitize_fts_strips_quotes() {
        assert_eq!(
            sanitize_fts_query("\"already quoted\""),
            "\"already\" OR \"quoted\""
        );
    }

    #[test]
    fn sanitize_fts_strips_special_fts5_chars() {
        // foo*bar, foo:bar, foo(bar) — special FTS5 chars stripped
        assert_eq!(sanitize_fts_query("foo*bar"), "\"foobar\"");
        assert_eq!(sanitize_fts_query("foo:bar"), "\"foobar\"");
        assert_eq!(sanitize_fts_query("***"), "");
    }

    #[test]
    fn mentor_entry_id_display() {
        let id = MentorEntryId("abc-123".into());
        assert_eq!(id.to_string(), "abc-123");
    }
}
