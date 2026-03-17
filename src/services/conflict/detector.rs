// ---------------------------------------------------------------------------
// ConflictDetector — finds overlapping changes between in-flight MRs.
//
// Algorithm:
//   1. Query mr_changed_files for all files touched by the target MR
//   2. Query for all other MRs in the same project touching those files
//   3. For each file overlap, compare hunk ranges to determine severity
//   4. Enrich with MR metadata (title, author, web_url) from GitLab
//   5. Return a ConflictReport ready for the wire
//
// Performance: The SQL query does the heavy lifting via the
// idx_mcf_project_file index. Hunk comparison is O(h1 * h2) per file
// pair, which is tiny (typically <10 hunks per file).
// ---------------------------------------------------------------------------

use crate::db::queries;
use crate::services::gitlab::client::{self, GitLabConfig};
use crate::types::cluster::{
    ConflictReport, ConflictSeverity, ConflictingMr, DiffHunk, FileConflict, OverlapType,
    hunks_overlap,
};
use anyhow::Result;
use sqlx::SqlitePool;
use std::collections::HashMap;
use tracing::{debug, warn};

/// Detect all file/line-range conflicts for a given MR against other in-flight MRs.
///
/// Returns a complete ConflictReport enriched with MR metadata from GitLab.
/// If GitLab metadata fetch fails for a conflicting MR, the conflict is still
/// reported with placeholder metadata — we never suppress a real conflict due
/// to a transient API failure.
pub async fn detect_conflicts(
    pool: &SqlitePool,
    gitlab_cfg: &GitLabConfig,
    project_id: i64,
    mr_iid: u64,
) -> Result<ConflictReport> {
    // 1. Get all files touched by other MRs that overlap with our files
    let overlapping_rows =
        queries::get_conflicting_mr_files(pool, project_id, mr_iid as i64).await?;

    if overlapping_rows.is_empty() {
        return Ok(ConflictReport {
            mr_iid,
            conflicts: Vec::new(),
        });
    }

    // 2. Get our own files for hunk comparison
    let our_rows = queries::get_mr_changed_files(pool, project_id, mr_iid as i64).await?;
    let our_hunks: HashMap<String, Vec<DiffHunk>> = our_rows
        .into_iter()
        .map(|(file_path, _, _, _, hunks_json, _)| {
            let hunks: Vec<DiffHunk> =
                serde_json::from_str(&hunks_json).unwrap_or_default();
            (file_path, hunks)
        })
        .collect();

    // 3. Group overlapping rows by (file_path, mr_iid) and parse hunks
    //    Structure: file_path -> [(mr_iid, hunks)]
    let mut file_overlaps: HashMap<String, Vec<(u64, Vec<DiffHunk>)>> = HashMap::new();
    for (their_mr_iid, file_path, _old_path, _change_type, _diff_hash, hunks_json) in
        &overlapping_rows
    {
        let their_hunks: Vec<DiffHunk> =
            serde_json::from_str(hunks_json).unwrap_or_default();
        file_overlaps
            .entry(file_path.clone())
            .or_default()
            .push((*their_mr_iid as u64, their_hunks));
    }

    // 4. Collect unique conflicting MR IIDs for metadata fetch
    let unique_mr_iids: Vec<u64> = overlapping_rows
        .iter()
        .map(|(iid, _, _, _, _, _)| *iid as u64)
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();

    // 5. Fetch MR metadata in parallel (best-effort — failures get placeholders)
    let mr_metadata = fetch_mr_metadata_batch(gitlab_cfg, project_id, &unique_mr_iids).await;

    // 6. Build the conflict report
    let mut conflicts: Vec<FileConflict> = Vec::new();

    for (file_path, their_entries) in &file_overlaps {
        let our_file_hunks = our_hunks.get(file_path).cloned().unwrap_or_default();

        // Deduplicate by MR IID within this file (a single MR can only conflict
        // once per file, but the query might return multiple rows if the MR has
        // the file under both old_path and new_path due to rename).
        let mut seen_mrs: HashMap<u64, Vec<DiffHunk>> = HashMap::new();
        for (their_iid, their_hunks) in their_entries {
            seen_mrs
                .entry(*their_iid)
                .or_default()
                .extend(their_hunks.iter().cloned());
        }

        let mut conflicting_mrs: Vec<ConflictingMr> = Vec::new();

        for (their_iid, their_hunks) in &seen_mrs {
            // Determine overlap type and severity
            let has_line_overlap = our_file_hunks.iter().any(|our_hunk| {
                their_hunks
                    .iter()
                    .any(|their_hunk| hunks_overlap(our_hunk, their_hunk))
            });

            let (overlap_type, severity) = if has_line_overlap {
                (OverlapType::LineRange, ConflictSeverity::High)
            } else {
                (OverlapType::SameFile, ConflictSeverity::Medium)
            };

            // Get metadata (or placeholder)
            let meta = mr_metadata.get(their_iid);
            let (title, author, web_url) = match meta {
                Some(m) => (m.0.clone(), m.1.clone(), m.2.clone()),
                None => (
                    format!("!{}", their_iid),
                    "unknown".to_string(),
                    String::new(),
                ),
            };

            conflicting_mrs.push(ConflictingMr {
                mr_iid: *their_iid,
                mr_title: title,
                author,
                web_url,
                overlap_type,
                your_hunks: our_file_hunks.clone(),
                their_hunks: their_hunks.clone(),
                severity,
                semantic_note: None, // Populated by SemanticConflictAnalyzer if enabled
            });
        }

        // Sort by severity descending (High before Medium)
        conflicting_mrs.sort_by(|a, b| b.severity.cmp(&a.severity));

        if !conflicting_mrs.is_empty() {
            conflicts.push(FileConflict {
                file_path: file_path.clone(),
                conflicting_mrs,
            });
        }
    }

    // Sort files by highest severity first, then alphabetically
    conflicts.sort_by(|a, b| {
        let a_max = a
            .conflicting_mrs
            .first()
            .map(|c| &c.severity)
            .unwrap_or(&ConflictSeverity::Medium);
        let b_max = b
            .conflicting_mrs
            .first()
            .map(|c| &c.severity)
            .unwrap_or(&ConflictSeverity::Medium);
        b_max.cmp(a_max).then_with(|| a.file_path.cmp(&b.file_path))
    });

    debug!(
        "conflict detection for !{}: {} file conflicts across {} MRs",
        mr_iid,
        conflicts.len(),
        unique_mr_iids.len(),
    );

    Ok(ConflictReport {
        mr_iid,
        conflicts,
    })
}

/// Fetch MR metadata for a batch of MR IIDs. Returns a map of iid -> (title, author, web_url).
/// Failures are logged and skipped — the caller uses placeholder metadata.
async fn fetch_mr_metadata_batch(
    cfg: &GitLabConfig,
    project_id: i64,
    mr_iids: &[u64],
) -> HashMap<u64, (String, String, String)> {
    let mut result = HashMap::new();

    // Fetch sequentially — typically 1-5 MRs, not worth the complexity of
    // parallel fetches with semaphore management. If this becomes a bottleneck,
    // we can add a metadata cache in the DB.
    for &iid in mr_iids {
        match client::fetch_merge_request(cfg, project_id, iid).await {
            Ok(mr) => {
                let author = mr
                    .author
                    .map(|a| a.username)
                    .unwrap_or_else(|| "unknown".to_string());
                result.insert(iid, (mr.title, author, mr.web_url));
            }
            Err(e) => {
                warn!(
                    "failed to fetch metadata for MR !{} (project {}): {}",
                    iid, project_id, e
                );
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
    use crate::types::cluster::DiffHunk;

    /// Helper to build a ConflictingMr for testing
    fn make_conflicting(
        iid: u64,
        overlap: OverlapType,
        severity: ConflictSeverity,
    ) -> ConflictingMr {
        ConflictingMr {
            mr_iid: iid,
            mr_title: format!("MR !{}", iid),
            author: "test".into(),
            web_url: String::new(),
            overlap_type: overlap,
            your_hunks: vec![DiffHunk {
                old_start: 10,
                old_count: 5,
                new_start: 10,
                new_count: 5,
            }],
            their_hunks: vec![DiffHunk {
                old_start: 12,
                old_count: 3,
                new_start: 12,
                new_count: 3,
            }],
            severity,
            semantic_note: None,
        }
    }

    #[test]
    fn conflict_report_max_severity() {
        let report = ConflictReport {
            mr_iid: 42,
            conflicts: vec![FileConflict {
                file_path: "src/main.rs".into(),
                conflicting_mrs: vec![
                    make_conflicting(55, OverlapType::SameFile, ConflictSeverity::Medium),
                    make_conflicting(56, OverlapType::LineRange, ConflictSeverity::High),
                ],
            }],
        };
        assert_eq!(report.max_severity(), Some(&ConflictSeverity::High));
    }

    #[test]
    fn conflict_report_empty() {
        let report = ConflictReport {
            mr_iid: 42,
            conflicts: Vec::new(),
        };
        assert_eq!(report.max_severity(), None);
        assert_eq!(report.total_conflicts(), 0);
    }

    #[test]
    fn conflict_report_total_count() {
        let report = ConflictReport {
            mr_iid: 42,
            conflicts: vec![
                FileConflict {
                    file_path: "a.rs".into(),
                    conflicting_mrs: vec![
                        make_conflicting(55, OverlapType::LineRange, ConflictSeverity::High),
                    ],
                },
                FileConflict {
                    file_path: "b.rs".into(),
                    conflicting_mrs: vec![
                        make_conflicting(55, OverlapType::SameFile, ConflictSeverity::Medium),
                        make_conflicting(56, OverlapType::SameFile, ConflictSeverity::Medium),
                    ],
                },
            ],
        };
        assert_eq!(report.total_conflicts(), 3);
    }
}
