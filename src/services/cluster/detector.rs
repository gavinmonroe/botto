// ---------------------------------------------------------------------------
// ClusterDetector — composes strategies and persists clusters to SQLite.
//
// Runs all registered strategies, merges overlapping candidates (e.g., if
// ticket and file-overlap strategies both find the same MR pair), deduplicates,
// and persists to the mr_clusters table.
//
// Called from webhook handlers when MRs are opened/updated/closed.
// ---------------------------------------------------------------------------

use crate::db::queries;
use crate::services::gitlab::client::{self, GitLabConfig};
use crate::types::cluster::{cluster_id, ClusterMember, ClusterSignal, MrCluster};
use anyhow::Result;
use sqlx::SqlitePool;
use std::collections::HashMap;
use tracing::{debug, warn};

use super::strategies::{ClusterCandidate, ClusterStrategy};

/// Default TTL for cluster entries (days).
const CLUSTER_TTL_DAYS: u32 = 7;

/// Detect clusters for a given MR using all provided strategies.
///
/// Merges overlapping candidates, enriches with MR metadata, and persists
/// to the database. Returns the detected clusters for immediate broadcast.
pub async fn detect_clusters(
    pool: &SqlitePool,
    gitlab_cfg: &GitLabConfig,
    project_id: i64,
    mr_iid: u64,
    strategies: &[&dyn ClusterStrategy],
) -> Result<Vec<MrCluster>> {
    // 1. Run all strategies and collect candidates
    let mut all_candidates: Vec<ClusterCandidate> = Vec::new();

    for strategy in strategies {
        match strategy.find_clusters(pool, gitlab_cfg, project_id, mr_iid).await {
            Ok(candidates) => all_candidates.extend(candidates),
            Err(e) => {
                warn!("cluster strategy failed for !{}: {}", mr_iid, e);
                // Continue with other strategies — one failure shouldn't block all
            }
        }
    }

    if all_candidates.is_empty() {
        return Ok(Vec::new());
    }

    // 2. Merge candidates that share MR IIDs into unified clusters.
    //    Two candidates merge if they have any MR IID in common.
    let merged = merge_candidates(all_candidates);

    // 3. Collect unique MR IIDs across all merged clusters for metadata fetch
    let unique_iids: Vec<u64> = merged
        .iter()
        .flat_map(|mc| mc.mr_iids.iter().copied())
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();

    let mr_metadata = fetch_mr_metadata_batch(gitlab_cfg, project_id, &unique_iids).await;

    // 4. Build MrCluster objects and persist
    let mut clusters: Vec<MrCluster> = Vec::new();

    for mc in merged {
        let id = cluster_id(project_id, &mc.mr_iids);

        // Build member list with metadata
        let member_mrs: Vec<ClusterMember> = mc
            .mr_iids
            .iter()
            .map(|&iid| {
                let (title, author) = mr_metadata
                    .get(&iid)
                    .cloned()
                    .unwrap_or_else(|| (format!("!{}", iid), "unknown".to_string()));
                ClusterMember {
                    mr_iid: iid,
                    mr_title: title,
                    author,
                    role: None, // Populated by AI summary later
                }
            })
            .collect();

        let cluster = MrCluster {
            id: id.clone(),
            project_id,
            ticket_key: mc.ticket_key.clone(),
            member_mrs,
            relevance_score: mc.relevance,
            signals: mc.signals,
            summary: None,
            review_order: None,
        };

        // Persist to DB
        let member_mrs_json = serde_json::to_string(&cluster.member_mrs)
            .unwrap_or_else(|_| "[]".to_string());
        let signals_json = serde_json::to_string(&cluster.signals)
            .unwrap_or_else(|_| "[]".to_string());

        if let Err(e) = queries::upsert_cluster(
            pool,
            &id,
            project_id,
            mc.ticket_key.as_deref(),
            &member_mrs_json,
            &signals_json,
            mc.relevance,
            CLUSTER_TTL_DAYS,
        )
        .await
        {
            warn!("failed to persist cluster {}: {}", id, e);
            continue;
        }

        debug!(
            "cluster {}: {} MRs, relevance={:.2}, ticket={:?}",
            id,
            cluster.member_mrs.len(),
            cluster.relevance_score,
            cluster.ticket_key,
        );

        clusters.push(cluster);
    }

    Ok(clusters)
}

/// Remove an MR from all clusters (on merge/close). Deletes clusters that
/// drop below 2 members. Returns the IDs of affected clusters for broadcast.
pub async fn remove_mr_from_clusters(
    pool: &SqlitePool,
    project_id: i64,
    mr_iid: u64,
) -> Result<Vec<String>> {
    let existing = queries::get_clusters_for_mr(pool, project_id, mr_iid as i64).await?;
    let mut affected_ids = Vec::new();

    for (id, _proj_id, ticket_key, member_mrs_json, signals_json, relevance, _, _, _, _) in
        existing
    {
        let mut members: Vec<ClusterMember> =
            serde_json::from_str(&member_mrs_json).unwrap_or_default();

        // Remove the MR from the member list
        members.retain(|m| m.mr_iid != mr_iid);

        if members.len() < 2 {
            // Cluster is no longer meaningful — delete it
            let _ = queries::delete_cluster(pool, &id).await;
            debug!("deleted cluster {} (< 2 members after removing !{})", id, mr_iid);
        } else {
            // Update the cluster with the reduced member list
            let new_member_json =
                serde_json::to_string(&members).unwrap_or_else(|_| "[]".to_string());
            let _ = queries::upsert_cluster(
                pool,
                &id,
                project_id,
                ticket_key.as_deref(),
                &new_member_json,
                &signals_json,
                relevance,
                CLUSTER_TTL_DAYS,
            )
            .await;
            debug!("updated cluster {} (removed !{}, {} members remain)", id, mr_iid, members.len());
        }

        affected_ids.push(id);
    }

    Ok(affected_ids)
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// A merged candidate — multiple signals collapsed into one cluster.
struct MergedCandidate {
    mr_iids: Vec<u64>,
    signals: Vec<ClusterSignal>,
    relevance: f64,
    ticket_key: Option<String>,
}

/// Merge candidates that share any MR IIDs. Uses a simple union-find approach.
fn merge_candidates(candidates: Vec<ClusterCandidate>) -> Vec<MergedCandidate> {
    if candidates.is_empty() {
        return Vec::new();
    }

    // Group by overlapping MR IID sets using iterative merging
    let mut groups: Vec<MergedCandidate> = Vec::new();

    for candidate in candidates {
        let iid_set: std::collections::HashSet<u64> =
            candidate.mr_iids.iter().copied().collect();

        // Find existing groups that overlap with this candidate
        let mut merge_indices: Vec<usize> = Vec::new();
        for (i, group) in groups.iter().enumerate() {
            let group_set: std::collections::HashSet<u64> =
                group.mr_iids.iter().copied().collect();
            if !iid_set.is_disjoint(&group_set) {
                merge_indices.push(i);
            }
        }

        if merge_indices.is_empty() {
            // New group
            groups.push(MergedCandidate {
                mr_iids: candidate.mr_iids,
                signals: vec![candidate.signal],
                relevance: candidate.relevance,
                ticket_key: candidate.ticket_key,
            });
        } else {
            // Merge into the first overlapping group, absorb others
            // Process in reverse to maintain valid indices during removal
            let _target_idx = merge_indices[0];

            // Collect all MR IIDs and signals from groups being merged
            let mut all_iids: std::collections::HashSet<u64> =
                candidate.mr_iids.into_iter().collect();
            let mut all_signals = vec![candidate.signal];
            let mut max_relevance = candidate.relevance;
            let mut ticket = candidate.ticket_key;

            for &idx in merge_indices.iter().rev() {
                let removed = groups.remove(idx);
                all_iids.extend(removed.mr_iids);
                all_signals.extend(removed.signals);
                if removed.relevance > max_relevance {
                    max_relevance = removed.relevance;
                }
                if ticket.is_none() {
                    ticket = removed.ticket_key;
                }
            }

            let mut merged_iids: Vec<u64> = all_iids.into_iter().collect();
            merged_iids.sort_unstable();

            groups.push(MergedCandidate {
                mr_iids: merged_iids,
                signals: all_signals,
                relevance: max_relevance,
                ticket_key: ticket,
            });
        }
    }

    groups
}

/// Fetch MR metadata for a batch of IIDs. Returns iid -> (title, author).
async fn fetch_mr_metadata_batch(
    cfg: &GitLabConfig,
    project_id: i64,
    mr_iids: &[u64],
) -> HashMap<u64, (String, String)> {
    let mut result = HashMap::new();

    for &iid in mr_iids {
        match client::fetch_merge_request(cfg, project_id, iid).await {
            Ok(mr) => {
                let author = mr
                    .author
                    .map(|a| a.username)
                    .unwrap_or_else(|| "unknown".to_string());
                result.insert(iid, (mr.title, author));
            }
            Err(e) => {
                warn!("failed to fetch MR !{} metadata: {}", iid, e);
            }
        }
    }

    result
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_disjoint_candidates() {
        let candidates = vec![
            ClusterCandidate {
                mr_iids: vec![1, 2],
                signal: ClusterSignal::SharedTicket {
                    ticket_key: "A-1".into(),
                },
                relevance: 0.9,
                ticket_key: Some("A-1".into()),
            },
            ClusterCandidate {
                mr_iids: vec![3, 4],
                signal: ClusterSignal::SharedTicket {
                    ticket_key: "B-2".into(),
                },
                relevance: 0.9,
                ticket_key: Some("B-2".into()),
            },
        ];

        let merged = merge_candidates(candidates);
        assert_eq!(merged.len(), 2);
    }

    #[test]
    fn merge_overlapping_candidates() {
        let candidates = vec![
            ClusterCandidate {
                mr_iids: vec![1, 2],
                signal: ClusterSignal::SharedTicket {
                    ticket_key: "A-1".into(),
                },
                relevance: 0.9,
                ticket_key: Some("A-1".into()),
            },
            ClusterCandidate {
                mr_iids: vec![2, 3],
                signal: ClusterSignal::FileOverlap {
                    jaccard: 0.5,
                    shared_files: vec!["main.rs".into()],
                },
                relevance: 0.5,
                ticket_key: None,
            },
        ];

        let merged = merge_candidates(candidates);
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].mr_iids, vec![1, 2, 3]);
        assert_eq!(merged[0].signals.len(), 2);
        assert!((merged[0].relevance - 0.9).abs() < f64::EPSILON); // max of 0.9 and 0.5
        assert_eq!(merged[0].ticket_key, Some("A-1".into()));
    }

    #[test]
    fn merge_empty() {
        let merged = merge_candidates(Vec::new());
        assert!(merged.is_empty());
    }
}
