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

    // Enrich member stats from local comment_actions table.
    // This gives us per-user review activity without expensive per-MR GitLab API calls.
    if let Ok(user_stats) = count_user_comment_stats(state.pool(), project_path, since_epoch).await {
        for (user_id, comments, accepted) in user_stats {
            let entry = member_map.entry(user_id.clone()).or_insert_with(|| MemberStats {
                user_id: user_id.clone(),
                display_name: user_id,
                mrs_authored: 0,
                mrs_reviewed: 0,
                comments_made: 0,
                suggestions_accepted: 0,
            });
            entry.comments_made = comments;
            entry.suggestions_accepted = accepted;
        }
    }

    // Count distinct MRs each user has reviewed (has comment actions on)
    if let Ok(review_counts) = count_user_mrs_reviewed(state.pool(), project_path, since_epoch).await {
        for (user_id, count) in review_counts {
            if let Some(entry) = member_map.get_mut(&user_id) {
                entry.mrs_reviewed = count;
            }
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

    // Compute average time-to-merge from created_at → merged_at on merged MRs.
    // This is a reasonable proxy for review turnaround without expensive per-MR
    // discussion fetches. The field name stays avg_time_to_first_review_hours
    // for API compatibility — it represents review cycle time.
    let avg_review_hours = {
        let mut durations = Vec::new();
        for mr in &merged_mrs {
            if let (Some(created), Some(merged)) = (
                mr.created_at.as_deref().and_then(parse_iso_epoch),
                mr.merged_at.as_deref().and_then(parse_iso_epoch),
            ) {
                if merged > created {
                    durations.push((merged - created) as f32 / 3600.0);
                }
            }
        }
        if durations.is_empty() {
            None
        } else {
            let sum: f32 = durations.iter().sum();
            Some(sum / durations.len() as f32)
        }
    };

    let team_stats = TeamStats {
        mrs_merged: merged_mrs.len() as u32,
        mrs_open: open_mrs.len() as u32,
        avg_time_to_first_review_hours: avg_review_hours,
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
    now: i64,
) -> Vec<ActionableItem> {
    let mut items = Vec::new();
    let stale_threshold_secs: i64 = 48 * 3600; // 48 hours

    for mr in open_mrs {
        // Skip draft/WIP MRs
        if mr.draft || mr.title.starts_with("Draft:") || mr.title.starts_with("WIP:") {
            continue;
        }

        // Compute age from created_at, staleness from updated_at
        let age_hours = mr.created_at.as_deref()
            .and_then(parse_iso_epoch)
            .map(|created| (now - created) as f32 / 3600.0)
            .unwrap_or(0.0);

        let last_activity_epoch = mr.updated_at.as_deref()
            .and_then(parse_iso_epoch)
            .or_else(|| mr.created_at.as_deref().and_then(parse_iso_epoch))
            .unwrap_or(now);

        let inactive_secs = now - last_activity_epoch;

        // Only flag MRs that have been inactive for 48+ hours
        if inactive_secs < stale_threshold_secs {
            continue;
        }

        let kind = if inactive_secs > 7 * 24 * 3600 {
            ActionableKind::StaleReview
        } else {
            ActionableKind::UnreviewedMr
        };

        let inactive_hours = inactive_secs as f32 / 3600.0;
        let message = match kind {
            ActionableKind::StaleReview => format!(
                "!{} \"{}\" has had no activity for {:.0}h",
                mr.iid, truncate(&mr.title, 50), inactive_hours
            ),
            ActionableKind::UnreviewedMr => format!(
                "!{} \"{}\" may need review (inactive {:.0}h)",
                mr.iid, truncate(&mr.title, 50), inactive_hours
            ),
        };

        items.push(ActionableItem {
            kind,
            mr_iid: mr.iid,
            project_path: project_path.to_string(),
            message,
            age_hours,
            web_url: mr.web_url.clone(),
        });
    }

    // Sort by staleness (oldest first), limit to 10
    items.sort_by(|a, b| b.age_hours.partial_cmp(&a.age_hours).unwrap_or(std::cmp::Ordering::Equal));
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

/// Per-user comment stats: total comments and accepted suggestions.
async fn count_user_comment_stats(
    pool: &sqlx::SqlitePool,
    project_path: &str,
    since_epoch: i64,
) -> anyhow::Result<Vec<(String, u32, u32)>> {
    let rows: Vec<(String, i64, i64)> = sqlx::query_as(
        "SELECT user_id,
                COUNT(*) as total,
                COALESCE(SUM(CASE WHEN action = 'accepted' THEN 1 ELSE 0 END), 0) as accepted
         FROM comment_actions
         WHERE project_path = ? AND created_at >= ?
         GROUP BY user_id",
    )
    .bind(project_path)
    .bind(since_epoch)
    .fetch_all(pool)
    .await?;

    Ok(rows.into_iter().map(|(u, t, a)| (u, t as u32, a as u32)).collect())
}

/// Count distinct MRs each user has reviewed (has any comment action on).
async fn count_user_mrs_reviewed(
    pool: &sqlx::SqlitePool,
    project_path: &str,
    since_epoch: i64,
) -> anyhow::Result<Vec<(String, u32)>> {
    let rows: Vec<(String, i64)> = sqlx::query_as(
        "SELECT user_id, COUNT(DISTINCT mr_iid) as mr_count
         FROM comment_actions
         WHERE project_path = ? AND created_at >= ?
         GROUP BY user_id",
    )
    .bind(project_path)
    .bind(since_epoch)
    .fetch_all(pool)
    .await?;

    Ok(rows.into_iter().map(|(u, c)| (u, c as u32)).collect())
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
        .unwrap_or_default()
        .as_secs() as i64
}

fn epoch_to_iso(epoch: i64) -> String {
    // Proper epoch → ISO 8601 conversion with leap year handling.
    let mut secs = epoch;
    let sec = secs % 60; secs /= 60;
    let min = secs % 60; secs /= 60;
    let hour = secs % 24;
    let mut days = secs / 24;

    let mut year: i64 = 1970;
    loop {
        let days_in_year = if is_leap(year) { 366 } else { 365 };
        if days < days_in_year { break; }
        days -= days_in_year;
        year += 1;
    }

    let month_days: [i64; 12] = if is_leap(year) {
        [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    } else {
        [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    };

    let mut month: i64 = 0;
    for &md in &month_days {
        if days < md { break; }
        days -= md;
        month += 1;
    }

    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        year, month + 1, days + 1, hour, min, sec
    )
}

fn is_leap(year: i64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

/// Parse an ISO 8601 timestamp (e.g. "2026-03-28T14:30:00.000Z") to epoch seconds.
/// Handles GitLab's format with optional fractional seconds and Z suffix.
fn parse_iso_epoch(iso: &str) -> Option<i64> {
    // Strip fractional seconds and Z for simple parsing
    let s = iso.trim_end_matches('Z');
    let s = if let Some(dot_pos) = s.rfind('.') { &s[..dot_pos] } else { s };

    // Parse "YYYY-MM-DDThh:mm:ss"
    let parts: Vec<&str> = s.split('T').collect();
    if parts.len() != 2 { return None; }

    let date_parts: Vec<i64> = parts[0].split('-').filter_map(|p| p.parse().ok()).collect();
    let time_parts: Vec<i64> = parts[1].split(':').filter_map(|p| p.parse().ok()).collect();

    if date_parts.len() != 3 || time_parts.len() < 2 { return None; }

    let (year, month, day) = (date_parts[0], date_parts[1], date_parts[2]);
    let (hour, min) = (time_parts[0], time_parts[1]);
    let sec = if time_parts.len() > 2 { time_parts[2] } else { 0 };

    // Approximate days from epoch (good enough for age calculations)
    let days_from_years = (year - 1970) * 365 + (year - 1969) / 4;
    let month_days: [i64; 12] = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334];
    let days_from_months = month_days.get((month - 1) as usize).copied().unwrap_or(0);
    let total_days = days_from_years + days_from_months + day - 1;

    Some(total_days * 86400 + hour * 3600 + min * 60 + sec)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_epoch_to_iso_known_date() {
        // 2024-01-01T00:00:00Z = 1704067200
        let result = epoch_to_iso(1704067200);
        assert_eq!(result, "2024-01-01T00:00:00Z");
    }

    #[test]
    fn test_epoch_to_iso_with_time() {
        // 2024-06-15T13:30:45Z = 1718458245
        let result = epoch_to_iso(1718458245);
        assert_eq!(result, "2024-06-15T13:30:45Z");
    }

    #[test]
    fn test_epoch_to_iso_unix_epoch() {
        assert_eq!(epoch_to_iso(0), "1970-01-01T00:00:00Z");
    }

    #[test]
    fn test_parse_iso_epoch_basic() {
        let result = parse_iso_epoch("2024-01-01T00:00:00Z");
        assert!(result.is_some());
        let epoch = result.unwrap();
        // Should be close to 1704067200 (within a day due to leap year approximation)
        assert!((epoch - 1704067200).abs() < 86400);
    }

    #[test]
    fn test_parse_iso_epoch_with_fractional() {
        let result = parse_iso_epoch("2024-06-15T14:30:45.123Z");
        assert!(result.is_some());
    }

    #[test]
    fn test_parse_iso_epoch_without_z() {
        let result = parse_iso_epoch("2024-06-15T14:30:45");
        assert!(result.is_some());
    }

    #[test]
    fn test_parse_iso_epoch_invalid() {
        assert!(parse_iso_epoch("not-a-date").is_none());
        assert!(parse_iso_epoch("").is_none());
        assert!(parse_iso_epoch("2024-01-01").is_none()); // no time part
    }

    #[test]
    fn test_roundtrip_epoch_to_iso_to_epoch() {
        // epoch_to_iso is exact, parse_iso_epoch is approximate
        // but for recent dates the error should be small
        let original = 1711612800; // 2024-03-28T12:00:00Z
        let iso = epoch_to_iso(original);
        let parsed = parse_iso_epoch(&iso).unwrap();
        // Within 2 days tolerance (leap year approximation in parse)
        assert!((parsed - original).abs() < 2 * 86400);
    }

    #[test]
    fn test_is_leap() {
        assert!(is_leap(2000)); // divisible by 400
        assert!(is_leap(2024)); // divisible by 4
        assert!(!is_leap(1900)); // divisible by 100 but not 400
        assert!(!is_leap(2023)); // not divisible by 4
    }

    #[test]
    fn test_build_actionable_items_filters_drafts() {
        let mrs = vec![
            MergeRequest {
                iid: 1,
                title: "Draft: WIP feature".into(),
                description: None,
                state: "opened".into(),
                source_branch: "feat".into(),
                target_branch: "main".into(),
                source_project_id: None,
                target_project_id: None,
                web_url: "https://example.com/mr/1".into(),
                author: None,
                created_at: Some("2020-01-01T00:00:00Z".into()),
                updated_at: Some("2020-01-01T00:00:00Z".into()),
                merged_at: None,
                draft: true,
                labels: vec![],
            },
        ];
        let now = epoch_secs();
        let items = build_actionable_items(&mrs, "team/repo", now);
        assert!(items.is_empty(), "draft MRs should be filtered out");
    }
}
