// ---------------------------------------------------------------------------
// FileOverlapStrategy — groups MRs with overlapping changed file sets.
//
// Uses Jaccard similarity (|intersection| / |union|) on the file path sets
// from the mr_changed_files index. This is a weaker signal than ticket
// matching — refactors that touch many files can create noise — so we
// apply a threshold (default 0.15) and cap cluster size at 8 MRs.
//
// Relevance: Jaccard score (0.15–1.0).
// ---------------------------------------------------------------------------

use super::{ClusterCandidate, ClusterStrategy};
use crate::db::queries;
use crate::services::gitlab::client::GitLabConfig;
use crate::types::cluster::ClusterSignal;
use anyhow::Result;
use sqlx::SqlitePool;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use tracing::debug;

/// Minimum Jaccard similarity to consider two MRs as related.
/// 0.15 means at least 15% of the combined file set is shared.
const DEFAULT_JACCARD_THRESHOLD: f64 = 0.15;

/// Maximum number of MRs in a file-overlap cluster.
/// Beyond this, it's not a coherent unit of work.
const MAX_CLUSTER_SIZE: usize = 8;

pub struct FileOverlapStrategy {
    pub jaccard_threshold: f64,
    pub max_cluster_size: usize,
}

impl Default for FileOverlapStrategy {
    fn default() -> Self {
        Self {
            jaccard_threshold: DEFAULT_JACCARD_THRESHOLD,
            max_cluster_size: MAX_CLUSTER_SIZE,
        }
    }
}

impl ClusterStrategy for FileOverlapStrategy {
    fn find_clusters(
        &self,
        pool: &SqlitePool,
        _gitlab_cfg: &GitLabConfig,
        project_id: i64,
        mr_iid: u64,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ClusterCandidate>>> + Send + '_>> {
        let threshold = self.jaccard_threshold;
        let max_size = self.max_cluster_size;
        let pool = pool.clone();
        Box::pin(async move {
            find_clusters_impl(&pool, project_id, mr_iid, threshold, max_size).await
        })
    }
}

async fn find_clusters_impl(
    pool: &SqlitePool,
    project_id: i64,
    mr_iid: u64,
    jaccard_threshold: f64,
    max_cluster_size: usize,
) -> Result<Vec<ClusterCandidate>> {
    // 1. Get all (mr_iid, file_path) pairs for the project from the index
    let all_paths = queries::get_project_mr_file_paths(pool, project_id).await?;

    if all_paths.is_empty() {
        return Ok(Vec::new());
    }

    // 2. Build per-MR file sets
    let mut mr_files: HashMap<u64, HashSet<String>> = HashMap::new();
    for (iid, file_path) in all_paths {
        mr_files
            .entry(iid as u64)
            .or_default()
            .insert(file_path);
    }

    // 3. Get our file set
    let our_files = match mr_files.get(&mr_iid) {
        Some(files) => files,
        None => return Ok(Vec::new()), // Our MR isn't in the index yet
    };

    if our_files.is_empty() {
        return Ok(Vec::new());
    }

    // 4. Compute Jaccard similarity with every other MR
    let mut related: Vec<(u64, f64, Vec<String>)> = Vec::new();

    for (&other_iid, other_files) in &mr_files {
        if other_iid == mr_iid {
            continue;
        }

        let intersection: HashSet<&String> =
            our_files.intersection(other_files).collect();
        let union: HashSet<&String> = our_files.union(other_files).collect();

        if union.is_empty() {
            continue;
        }

        let jaccard = intersection.len() as f64 / union.len() as f64;

        if jaccard >= jaccard_threshold {
            let shared: Vec<String> = intersection.into_iter().cloned().collect();
            related.push((other_iid, jaccard, shared));
        }
    }

    if related.is_empty() {
        return Ok(Vec::new());
    }

    // 5. Sort by Jaccard descending and cap at max_cluster_size - 1
    //    (the -1 accounts for our own MR)
    related.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    related.truncate(max_cluster_size.saturating_sub(1));

    // 6. Build a single cluster candidate with all related MRs
    let mut mr_iids: Vec<u64> = vec![mr_iid];
    let mut all_shared_files: HashSet<String> = HashSet::new();
    let mut total_jaccard = 0.0;

    for (other_iid, jaccard, shared) in &related {
        mr_iids.push(*other_iid);
        all_shared_files.extend(shared.iter().cloned());
        total_jaccard += jaccard;
    }

    // Average Jaccard across all pairs as the cluster relevance
    let avg_jaccard = total_jaccard / related.len() as f64;

    mr_iids.sort_unstable();
    mr_iids.dedup();

    let mut shared_files: Vec<String> = all_shared_files.into_iter().collect();
    shared_files.sort();

    debug!(
        "file overlap cluster: {} MRs with avg Jaccard {:.2} ({} shared files) for !{}",
        mr_iids.len(),
        avg_jaccard,
        shared_files.len(),
        mr_iid,
    );

    Ok(vec![ClusterCandidate {
        mr_iids,
        signal: ClusterSignal::FileOverlap {
            jaccard: avg_jaccard,
            shared_files,
        },
        relevance: avg_jaccard,
        ticket_key: None,
    }])
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jaccard_threshold_default() {
        let strategy = FileOverlapStrategy::default();
        assert!((strategy.jaccard_threshold - 0.15).abs() < f64::EPSILON);
    }
}
