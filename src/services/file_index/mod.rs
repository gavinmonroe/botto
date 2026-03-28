// ---------------------------------------------------------------------------
// File Index Service — populates and manages the mr_changed_files table.
//
// The mr_changed_files table is the shared foundation for both Conflict Radar
// and Cross-MR Clusters. It must be populated before either feature can work.
//
// Two entry points:
//   - `ensure_populated` — lazy: skips if data already exists for the MR.
//     Used by on-demand paths (GET_CONFLICTS, GET_CLUSTER, VIEWING_MR).
//   - `populate` — always re-fetches from GitLab and replaces existing data.
//     Used by webhook handlers where we know the diff has changed.
//
// Both are safe to call concurrently — the caller is expected to hold a
// per-MR lock (e.g., AppState::mr_webhook_locks) when needed.
// ---------------------------------------------------------------------------

use crate::db::queries;
use crate::services::gitlab::client::{self as gitlab, GitLabConfig};
use crate::types::cluster;
use crate::util::hash;
use anyhow::Result;
use sqlx::SqlitePool;
use tracing::{debug, warn};

/// Ensure the file index is populated for a given MR. If data already exists,
/// this is a no-op (single COUNT query). If not, fetches changes from GitLab
/// and populates the index.
///
/// Returns `true` if data was freshly fetched, `false` if it already existed.
///
/// This is the primary entry point for on-demand paths — it's cheap when data
/// exists and self-healing when it doesn't (e.g., Botto restarted, no webhook
/// configured, or MR predates Botto deployment).
pub async fn ensure_populated(
    pool: &SqlitePool,
    gitlab_cfg: &GitLabConfig,
    project_id: i64,
    mr_iid: u64,
) -> Result<bool> {
    // Fast path: check if we already have file index entries for this MR.
    let existing = queries::get_mr_changed_files(pool, project_id, mr_iid as i64).await?;
    if !existing.is_empty() {
        return Ok(false);
    }

    // Slow path: fetch from GitLab and populate.
    populate(pool, gitlab_cfg, project_id, mr_iid).await?;
    Ok(true)
}

/// Fetch MR changes from GitLab and (re-)populate the file index.
///
/// Clears existing entries first to handle force-pushes that remove files
/// from the diff. This is the authoritative path — always re-fetches.
///
/// Used by webhook handlers and the push-event backfill path.
pub async fn populate(
    pool: &SqlitePool,
    gitlab_cfg: &GitLabConfig,
    project_id: i64,
    mr_iid: u64,
) -> Result<usize> {
    let changes = gitlab::fetch_mr_changes(gitlab_cfg, project_id, mr_iid)
        .await
        .map_err(|e| anyhow::anyhow!("fetch_mr_changes: {}", e))?;

    // Clear stale entries before re-inserting (handles force-pushes).
    if let Err(e) = queries::delete_mr_changed_files(pool, project_id, mr_iid as i64).await {
        warn!(
            "file index cleanup failed for project {} !{}: {}",
            project_id, mr_iid, e
        );
    }

    let mut count = 0;
    for change in &changes.changes {
        let change_type = cluster::change_type_from_diff(
            change.new_file,
            change.deleted_file,
            change.renamed_file,
        );
        let hunks = cluster::parse_hunks(&change.diff);
        let diff_hash = hash::djb2(&change.diff);
        let hunks_json = serde_json::to_string(&hunks).unwrap_or_else(|_| "[]".into());

        if let Err(e) = queries::upsert_mr_changed_file(
            pool,
            project_id,
            mr_iid as i64,
            &change.new_path,
            if change.renamed_file {
                Some(change.old_path.as_str())
            } else {
                None
            },
            change_type.as_str(),
            &diff_hash,
            &hunks_json,
        )
        .await
        {
            warn!(
                "file index upsert failed for project {} !{} {}: {}",
                project_id, mr_iid, change.new_path, e
            );
        } else {
            count += 1;
        }
    }

    debug!(
        "file index: populated {} files for project {} !{}",
        count, project_id, mr_iid
    );

    Ok(count)
}

/// Ensure the file index is populated for all open MRs in a project.
///
/// Fetches the list of open MRs from GitLab and calls `ensure_populated`
/// for each one. Already-indexed MRs are skipped (cheap DB check).
///
/// This is the key function that solves the cold-start problem: when Botto
/// starts fresh (or webhooks were never configured), the first user to open
/// any MR in the project triggers population of the entire project's open MRs.
///
/// Returns the number of MRs that were freshly populated (0 if all were cached).
pub async fn ensure_project_populated(
    pool: &SqlitePool,
    gitlab_cfg: &GitLabConfig,
    project_id: i64,
) -> Result<usize> {
    let open_mrs = gitlab::fetch_open_mrs(gitlab_cfg, project_id)
        .await
        .map_err(|e| anyhow::anyhow!("fetch_open_mrs: {}", e))?;

    if open_mrs.is_empty() {
        return Ok(0);
    }

    let mut populated_count = 0;

    for mr in &open_mrs {
        // Skip draft MRs — they change frequently and are less likely to
        // conflict with or cluster alongside active MRs.
        if mr.draft {
            continue;
        }

        match ensure_populated(pool, gitlab_cfg, project_id, mr.iid).await {
            Ok(true) => populated_count += 1,
            Ok(false) => {} // Already indexed
            Err(e) => {
                // Log and continue — one MR failing shouldn't block the rest.
                warn!(
                    "file index: failed to populate project {} !{}: {}",
                    project_id, mr.iid, e
                );
            }
        }
    }

    if populated_count > 0 {
        debug!(
            "file index: populated {} of {} open MRs for project {}",
            populated_count,
            open_mrs.len(),
            project_id,
        );
    }

    Ok(populated_count)
}
