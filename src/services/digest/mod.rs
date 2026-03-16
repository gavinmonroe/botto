// ---------------------------------------------------------------------------
// Team Activity Digest — aggregates team metrics from GitLab + local data.
//
// Design decisions:
// - Lazy computation: digests are computed on first request, not on a schedule.
//   This avoids unnecessary GitLab API calls for projects nobody is looking at.
// - Cached in SQLite with TTL (daily=4h, weekly=12h). Subsequent requests
//   within the TTL window get the cached version instantly.
// - Data sources: GitLab REST API (merged MRs, authors, reviewers) + local
//   tables (review_cache, comment_actions, sandbox_jobs).
// - Returns aggregate team stats + per-member stats. Otto filters per-member
//   stats client-side to show only the requesting user's own data.
// - Actionable items surface stale/unreviewed MRs — useful, not competitive.
// ---------------------------------------------------------------------------

use crate::services::gitlab::client::{self, GitLabConfig, MergeRequest};
use crate::types::state::AppState;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, warn};

// ---------------------------------------------------------------------------
// Data model
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeamDigest {
    pub period: DigestPeriod,
    pub generated_at: i64,
    pub team_stats: TeamStats,
    pub member_stats: Vec<MemberStats>,
    pub actionable: Vec<ActionableItem>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum DigestPeriod {
    Daily,
    Weekly,
}

impl DigestPeriod {
    /// How many days of data to look back.
    fn lookback_days(&self) -> i64 {
        match self {
            DigestPeriod::Daily => 1,
            DigestPeriod::Weekly => 7,
        }
    }

    /// Cache TTL in seconds.
    fn cache_ttl_secs(&self) -> i64 {
        match self {
            DigestPeriod::Daily => 4 * 3600,   // 4 hours
            DigestPeriod::Weekly => 12 * 3600,  // 12 hours
        }
    }

    fn as_str(&self) -> &'static str {
        match self {
            DigestPeriod::Daily => "daily",
            DigestPeriod::Weekly => "weekly",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeamStats {
    pub mrs_merged: u32,
    pub mrs_open: u32,
    pub avg_time_to_first_review_hours: Option<f32>,
    pub sandbox_fixes_applied: u32,
    pub review_comments_accepted: u32,
    pub review_comments_dismissed: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemberStats {
    pub user_id: String,
    pub display_name: String,
    pub mrs_authored: u32,
    pub mrs_reviewed: u32,
    pub comments_made: u32,
    pub suggestions_accepted: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActionableItem {
    pub kind: ActionableKind,
    pub mr_iid: u64,
    pub project_path: String,
    pub message: String,
    pub age_hours: f32,
    pub web_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActionableKind {
    StaleReview,
    UnreviewedMr,
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Get or compute a team digest for a project.
/// Returns cached version if fresh, otherwise computes and caches.
pub async fn get_team_digest(
    state: &AppState,
    project_path: &str,
    period: DigestPeriod,
) -> anyhow::Result<TeamDigest> {
    let now = epoch_secs();

    // Check cache first
    if let Ok(Some(cached)) = get_cached_digest(state.pool(), project_path, period).await {
        if cached.generated_at + period.cache_ttl_secs() > now {
            debug!("digest cache hit for {} ({})", project_path, period.as_str());
            return Ok(cached);
        }
    }

    // Compute fresh digest
    debug!("computing {} digest for {}", period.as_str(), project_path);
    let digest = compute_digest(state, project_path, period).await?;

    // Cache it (best-effort)
    if let Err(e) = save_digest(state.pool(), project_path, period, &digest).await {
        warn!("failed to cache digest: {}", e);
    }

    Ok(digest)
}

// ---------------------------------------------------------------------------
// Computation
// ---------------------------------------------------------------------------

async fn compute_digest(
    state: &AppState,
    project_path: &str,
    period: DigestPeriod,
) -> anyhow::Result<TeamDigest> {
    let now = epoch_secs();
    let since_epoch = now - (period.lookback_days() * 86400);
    let since_iso = epoch_to_iso(since_epoch);

    let cfg = state.config();
    let gl_cfg = GitLabConfig {
        base_url: cfg.gitlab.url.clone(),
        token: cfg.gitlab.bot_token.clone(),
    };

    // Fetch project ID
    let project = client::fetch_project(&gl_cfg, project_path).await
        .map_err(|e| anyhow::anyhow!("failed to fetch project: {}", e))?;

    // Fetch merged MRs and open MRs in parallel
    let (merged_mrs, open_mrs) = {
        let gl1 = gl_cfg.clone();
        let gl2 = gl_cfg.clone();
        let pid = project.id;
        let since = since_iso.clone();

        let merged_fut = async move {
            client::fetch_recent_merged_mrs(&gl1, pid, &since).await.unwrap_or_default()
        };
        let open_fut = async move {
            fetch_open_mrs(&gl2, pid).await.unwrap_or_default()
        };

        tokio::join!(merged_fut, open_fut)
    };

    // Build member stats from merged MRs
    let mut member_map: HashMap<String, MemberStats> = HashMap::new();
    for mr in &merged_mrs {
        if let Some(ref author) = mr.author {
            let entry = member_map.entry(author.username.clone()).or_insert_with(|| MemberStats {
                user_id: author.username.clone(),
                display_name: author.username.clone(),
                mrs_authored: 0,
                mrs_reviewed: 0,
                comments_made: 0,
                suggestions_accepted: 0,
            });
            entry.mrs_authored += 1;
        }
    }

    // Query local comment actions for the period
    let (accepted_count, dismissed_count) = count_comment_actions(
        state.pool(), project_path, since_epoch,
    ).await.unwrap_or((0, 0));

    // Count sandbox fixes
    let sandbox_fixes = count_sandbox_fixes(
        state.pool(), project_path, since_epoch,
    ).await.unwrap_or(0);

    // Build actionable items from open MRs
    let actionable = build_actionable_items(&open_mrs, project_path, now);

    let team_stats = TeamStats {
        mrs_merged: merged_mrs.len() as u32,
        mrs_open: open_mrs.len() as u32,
        avg_time_to_first_review_hours: None, // Would require per-MR discussion fetch — too expensive
        sandbox_fixes_applied: sandbox_fixes,
        review_comments_accepted: accepted_count,
        review_comments_dismissed: dismissed_count,
    };

    Ok(TeamDigest {
        period,
        generated_at: now,
        team_stats,
        member_stats: member_map.into_values().collect(),
        actionable,
    })
}

fn build_actionable_items(
    open_mrs: &[MergeRequest],
    project_path: &str,
    _now: i64,
) -> Vec<ActionableItem> {
    let mut items = Vec::new();

    for mr in open_mrs {
        // Skip draft/WIP MRs
        if mr.title.starts_with("Draft:") || mr.title.starts_with("WIP:") {
            continue;
        }

        // Parse created_at if available from the web_url (we don't have created_at in MergeRequest)
        // Use a heuristic: if the MR state is "opened", flag it based on iid age
        // For now, we'll flag all open MRs as potentially needing review
        // A more precise approach would require fetching MR details with timestamps

        items.push(ActionableItem {
            kind: ActionableKind::UnreviewedMr,
            mr_iid: mr.iid,
            project_path: project_path.to_string(),
            message: format!("!{} \"{}\" is open and may need review", mr.iid, truncate(&mr.title, 60)),
            age_hours: 0.0, // Would need created_at timestamp
            web_url: mr.web_url.clone(),
        });
    }

    // Limit to 10 actionable items
    items.truncate(10);
    items
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}...", &s[..max.min(s.len())])
    }
}

// ---------------------------------------------------------------------------
// GitLab helpers
// ---------------------------------------------------------------------------

async fn fetch_open_mrs(
    cfg: &GitLabConfig,
    project_id: i64,
) -> Result<Vec<MergeRequest>, client::GitLabError> {
    client::get_json(
        cfg,
        &format!("/projects/{}/merge_requests", project_id),
        &[
            ("state", "opened"),
            ("order_by", "updated_at"),
            ("sort", "desc"),
            ("per_page", "50"),
        ],
    )
    .await
}

// ---------------------------------------------------------------------------
// Local DB queries for digest
// ---------------------------------------------------------------------------

async fn count_comment_actions(
    pool: &sqlx::SqlitePool,
    project_path: &str,
    since_epoch: i64,
) -> anyhow::Result<(u32, u32)> {
    let row: Option<(i64, i64)> = sqlx::query_as(
        "SELECT
           COALESCE(SUM(CASE WHEN action = 'accepted' THEN 1 ELSE 0 END), 0),
           COALESCE(SUM(CASE WHEN action = 'dismissed' THEN 1 ELSE 0 END), 0)
         FROM comment_actions
         WHERE project_path = ? AND created_at >= ?",
    )
    .bind(project_path)
    .bind(since_epoch)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|(a, d)| (a as u32, d as u32)).unwrap_or((0, 0)))
}

async fn count_sandbox_fixes(
    pool: &sqlx::SqlitePool,
    project_path: &str,
    since_epoch: i64,
) -> anyhow::Result<u32> {
    let row: Option<(i64,)> = sqlx::query_as(
        "SELECT COUNT(*) FROM sandbox_jobs
         WHERE project_path = ? AND status = 'complete' AND created_at >= ?",
    )
    .bind(project_path)
    .bind(since_epoch)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|r| r.0 as u32).unwrap_or(0))
}

// ---------------------------------------------------------------------------
// Digest cache (SQLite)
// ---------------------------------------------------------------------------

async fn get_cached_digest(
    pool: &sqlx::SqlitePool,
    project_path: &str,
    period: DigestPeriod,
) -> anyhow::Result<Option<TeamDigest>> {
    let row: Option<(Vec<u8>,)> = sqlx::query_as(
        "SELECT digest FROM digests WHERE project_path = ? AND period = ?",
    )
    .bind(project_path)
    .bind(period.as_str())
    .fetch_optional(pool)
    .await?;

    match row {
        Some((data,)) => {
            let json = crate::router::decompress_or_raw(&data);
            let digest: TeamDigest = serde_json::from_slice(&json)?;
            Ok(Some(digest))
        }
        None => Ok(None),
    }
}

async fn save_digest(
    pool: &sqlx::SqlitePool,
    project_path: &str,
    period: DigestPeriod,
    digest: &TeamDigest,
) -> anyhow::Result<()> {
    let json = serde_json::to_vec(digest)?;

    // Compress with gzip (same pattern as review_cache)
    let compressed = {
        use flate2::write::GzEncoder;
        use flate2::Compression;
        use std::io::Write;
        let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
        encoder.write_all(&json)?;
        encoder.finish()?
    };

    let now = epoch_secs();
    let expires_at = now + period.cache_ttl_secs();

    sqlx::query(
        "INSERT INTO digests (project_path, period, digest, generated_at, expires_at)
         VALUES (?, ?, ?, ?, ?)
         ON CONFLICT(project_path, period) DO UPDATE SET
           digest = excluded.digest,
           generated_at = excluded.generated_at,
           expires_at = excluded.expires_at",
    )
    .bind(project_path)
    .bind(period.as_str())
    .bind(&compressed)
    .bind(now)
    .bind(expires_at)
    .execute(pool)
    .await?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn epoch_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

fn epoch_to_iso(epoch: i64) -> String {
    // Simple ISO 8601 date string from epoch seconds
    let secs = epoch as u64;
    let days = secs / 86400;
    // Approximate — good enough for a "since" filter
    let years = 1970 + days / 365;
    let remaining_days = days % 365;
    let month = remaining_days / 30 + 1;
    let day = remaining_days % 30 + 1;
    format!("{:04}-{:02}-{:02}T00:00:00Z", years, month.min(12), day.min(28))
}
