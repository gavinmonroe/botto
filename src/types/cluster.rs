// ---------------------------------------------------------------------------
// Cluster & Conflict types — shared data structures for cross-MR features.
//
// Two features share the `mr_changed_files` index:
//   1. Conflict Radar — detects overlapping changes across in-flight MRs
//   2. Cross-MR Clusters — groups related MRs by ticket or file overlap
//
// Types are serialized to JSON for both SQLite storage and WebSocket wire
// format. All structs use camelCase serialization to match Otto's conventions.
// ---------------------------------------------------------------------------

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// MR File Index — shared foundation for both features
// ---------------------------------------------------------------------------

/// A single diff hunk parsed from a unified diff header (`@@ -a,b +c,d @@`).
/// Stored as JSON in the `hunks` column of `mr_changed_files`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DiffHunk {
    pub old_start: u32,
    pub old_count: u32,
    pub new_start: u32,
    pub new_count: u32,
}

/// How a file was changed in an MR diff.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum FileChangeType {
    Added,
    Modified,
    Deleted,
    Renamed,
}

impl FileChangeType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Added => "added",
            Self::Modified => "modified",
            Self::Deleted => "deleted",
            Self::Renamed => "renamed",
        }
    }

    pub fn from_str(s: &str) -> Self {
        match s {
            "added" => Self::Added,
            "deleted" => Self::Deleted,
            "renamed" => Self::Renamed,
            _ => Self::Modified,
        }
    }
}

/// A file changed in an MR, stored in the `mr_changed_files` index.
/// Populated from webhook events and review pipeline side-effects.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MrChangedFile {
    pub project_id: i64,
    pub mr_iid: u64,
    pub file_path: String,
    pub old_path: Option<String>,
    pub change_type: FileChangeType,
    pub diff_hash: String,
    pub hunks: Vec<DiffHunk>,
    pub updated_at: i64,
}

// ---------------------------------------------------------------------------
// Conflict Radar types
// ---------------------------------------------------------------------------

/// Severity of a file conflict between two MRs.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "lowercase")]
pub enum ConflictSeverity {
    /// Same file, non-overlapping regions.
    Medium,
    /// Overlapping line ranges — high risk of merge conflict.
    High,
}

/// How two MRs overlap on a file.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OverlapType {
    /// Overlapping diff hunks.
    LineRange,
    /// Same file modified, but different regions.
    SameFile,
}

/// A single MR that conflicts with the current MR on a specific file.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConflictingMr {
    pub mr_iid: u64,
    pub mr_title: String,
    pub author: String,
    pub web_url: String,
    pub overlap_type: OverlapType,
    pub your_hunks: Vec<DiffHunk>,
    pub their_hunks: Vec<DiffHunk>,
    pub severity: ConflictSeverity,
    /// AI-generated explanation of the semantic conflict (if enabled).
    pub semantic_note: Option<String>,
}

/// All conflicts on a single file.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileConflict {
    pub file_path: String,
    pub conflicting_mrs: Vec<ConflictingMr>,
}

/// Complete conflict report for an MR.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConflictReport {
    pub mr_iid: u64,
    pub conflicts: Vec<FileConflict>,
}

impl ConflictReport {
    /// Returns the highest severity across all conflicts, or None if empty.
    pub fn max_severity(&self) -> Option<&ConflictSeverity> {
        self.conflicts
            .iter()
            .flat_map(|fc| fc.conflicting_mrs.iter())
            .map(|cm| &cm.severity)
            .max()
    }

    /// Total number of conflicting file/MR pairs.
    pub fn total_conflicts(&self) -> usize {
        self.conflicts
            .iter()
            .map(|fc| fc.conflicting_mrs.len())
            .sum()
    }
}

// ---------------------------------------------------------------------------
// Cross-MR Cluster types
// ---------------------------------------------------------------------------

/// Why MRs were grouped into a cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClusterSignal {
    /// MRs share a Jira/ticket key.
    SharedTicket {
        #[serde(rename = "ticketKey", alias = "ticket_key")]
        ticket_key: String,
    },
    /// MRs have overlapping changed file sets.
    FileOverlap {
        jaccard: f64,
        #[serde(rename = "sharedFiles", alias = "shared_files")]
        shared_files: Vec<String>,
    },
}

/// A member MR within a cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterMember {
    pub mr_iid: u64,
    pub mr_title: String,
    pub author: String,
    /// AI-assigned role within the cluster (e.g., "API layer", "frontend").
    /// Populated when the cluster summary is generated.
    pub role: Option<String>,
}

/// A group of related MRs.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MrCluster {
    /// Deterministic ID: djb2 hash of sorted MR IIDs + project_id.
    pub id: String,
    pub project_id: i64,
    pub ticket_key: Option<String>,
    pub member_mrs: Vec<ClusterMember>,
    pub relevance_score: f64,
    pub signals: Vec<ClusterSignal>,
    /// AI-generated unified summary (populated on demand, not eagerly).
    pub summary: Option<ClusterSummaryData>,
    /// AI-generated review order for guided cross-MR walkthrough.
    pub review_order: Option<ClusterReviewOrder>,
}

/// AI-generated unified narrative across clustered MRs.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterSummaryData {
    /// Unified narrative: "MR !42 adds the API, !43 adds the frontend..."
    pub narrative: String,
    /// What each MR contributes to the cluster.
    pub per_mr_roles: Vec<MrRole>,
    pub risk_assessment: String,
    pub integration_concerns: Vec<String>,
}

/// What a single MR contributes within a cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MrRole {
    pub mr_iid: u64,
    pub role: String,
    pub key_changes: Vec<String>,
}

/// AI-generated review order for cross-MR guided walkthrough.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterReviewOrder {
    pub phases: Vec<ReviewPhase>,
}

/// A single phase in a cross-MR guided review.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReviewPhase {
    /// Human-readable label: "API Layer", "Frontend", "Tests".
    pub label: String,
    pub mr_iid: u64,
    pub files: Vec<String>,
    pub rationale: String,
}

// ---------------------------------------------------------------------------
// Hunk parsing — extracts DiffHunk from unified diff text
// ---------------------------------------------------------------------------

/// Parse all diff hunks from a unified diff string.
/// Looks for lines matching `@@ -old_start,old_count +new_start,new_count @@`.
///
/// Handles edge cases:
/// - Single-line hunks with no count (e.g., `@@ -42 +42 @@` → count = 1)
/// - Hunks with trailing context after `@@` (e.g., `@@ -1,3 +1,5 @@ fn main()`)
/// - Empty diffs (returns empty vec)
pub fn parse_hunks(diff: &str) -> Vec<DiffHunk> {
    let mut hunks = Vec::new();

    for line in diff.lines() {
        if !line.starts_with("@@") {
            continue;
        }

        // Find the closing @@ to isolate the range spec
        let after_open = match line.get(2..) {
            Some(s) => s.trim_start(),
            None => continue,
        };
        let range_end = match after_open.find("@@") {
            Some(i) => i,
            None => continue,
        };
        let range_spec = after_open[..range_end].trim();

        // Parse "-old_start,old_count +new_start,new_count"
        let parts: Vec<&str> = range_spec.split_whitespace().collect();
        if parts.len() < 2 {
            continue;
        }

        let (old_start, old_count) = parse_range_part(parts[0].trim_start_matches('-'));
        let (new_start, new_count) = parse_range_part(parts[1].trim_start_matches('+'));

        hunks.push(DiffHunk {
            old_start,
            old_count,
            new_start,
            new_count,
        });
    }

    hunks
}

/// Parse a single range part like "42,18" or "42" (count defaults to 1).
fn parse_range_part(s: &str) -> (u32, u32) {
    if let Some((start_str, count_str)) = s.split_once(',') {
        let start = start_str.parse::<u32>().unwrap_or(0);
        let count = count_str.parse::<u32>().unwrap_or(0);
        (start, count)
    } else {
        let start = s.parse::<u32>().unwrap_or(0);
        (start, 1)
    }
}

/// Check if two hunk ranges overlap on the old-file side.
/// Used by ConflictDetector to determine line-range conflicts.
pub fn hunks_overlap(a: &DiffHunk, b: &DiffHunk) -> bool {
    // Zero-count hunks (pure additions) don't overlap on old-file side
    if a.old_count == 0 || b.old_count == 0 {
        return false;
    }
    let a_end = a.old_start + a.old_count;
    let b_end = b.old_start + b.old_count;
    a.old_start < b_end && b.old_start < a_end
}

/// Determine the FileChangeType from DiffFileData flags.
pub fn change_type_from_diff(is_new: bool, is_deleted: bool, is_renamed: bool) -> FileChangeType {
    if is_new {
        FileChangeType::Added
    } else if is_deleted {
        FileChangeType::Deleted
    } else if is_renamed {
        FileChangeType::Renamed
    } else {
        FileChangeType::Modified
    }
}

// ---------------------------------------------------------------------------
// Cluster ID generation
// ---------------------------------------------------------------------------

/// Generate a deterministic cluster ID from sorted MR IIDs and project ID.
/// Uses the same djb2 hash as the rest of the system.
pub fn cluster_id(project_id: i64, mr_iids: &[u64]) -> String {
    let mut sorted = mr_iids.to_vec();
    sorted.sort_unstable();
    let input = format!(
        "{}:{}",
        project_id,
        sorted
            .iter()
            .map(|iid| iid.to_string())
            .collect::<Vec<_>>()
            .join(",")
    );
    crate::util::hash::djb2(&input)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_hunks_standard() {
        let diff = r#"--- a/src/main.rs
+++ b/src/main.rs
@@ -42,18 +42,23 @@ fn main() {
 some context
@@ -100,5 +105,10 @@ fn helper() {
 more context"#;

        let hunks = parse_hunks(diff);
        assert_eq!(hunks.len(), 2);
        assert_eq!(
            hunks[0],
            DiffHunk {
                old_start: 42,
                old_count: 18,
                new_start: 42,
                new_count: 23
            }
        );
        assert_eq!(
            hunks[1],
            DiffHunk {
                old_start: 100,
                old_count: 5,
                new_start: 105,
                new_count: 10
            }
        );
    }

    #[test]
    fn parse_hunks_single_line() {
        let diff = "@@ -1 +1 @@\n-old\n+new";
        let hunks = parse_hunks(diff);
        assert_eq!(hunks.len(), 1);
        assert_eq!(
            hunks[0],
            DiffHunk {
                old_start: 1,
                old_count: 1,
                new_start: 1,
                new_count: 1
            }
        );
    }

    #[test]
    fn parse_hunks_empty_diff() {
        assert!(parse_hunks("").is_empty());
        assert!(parse_hunks("no hunks here").is_empty());
    }

    #[test]
    fn parse_hunks_new_file() {
        let diff = "@@ -0,0 +1,25 @@\n+new file content";
        let hunks = parse_hunks(diff);
        assert_eq!(hunks.len(), 1);
        assert_eq!(
            hunks[0],
            DiffHunk {
                old_start: 0,
                old_count: 0,
                new_start: 1,
                new_count: 25
            }
        );
    }

    #[test]
    fn hunks_overlap_basic() {
        let a = DiffHunk {
            old_start: 42,
            old_count: 18,
            new_start: 42,
            new_count: 23,
        };
        let b = DiffHunk {
            old_start: 50,
            old_count: 10,
            new_start: 55,
            new_count: 10,
        };
        assert!(hunks_overlap(&a, &b)); // 42..60 overlaps 50..60
    }

    #[test]
    fn hunks_no_overlap() {
        let a = DiffHunk {
            old_start: 10,
            old_count: 5,
            new_start: 10,
            new_count: 5,
        };
        let b = DiffHunk {
            old_start: 20,
            old_count: 5,
            new_start: 20,
            new_count: 5,
        };
        assert!(!hunks_overlap(&a, &b)); // 10..15 doesn't overlap 20..25
    }

    #[test]
    fn hunks_adjacent_no_overlap() {
        let a = DiffHunk {
            old_start: 10,
            old_count: 5,
            new_start: 10,
            new_count: 5,
        };
        let b = DiffHunk {
            old_start: 15,
            old_count: 5,
            new_start: 15,
            new_count: 5,
        };
        assert!(!hunks_overlap(&a, &b)); // 10..15 and 15..20 are adjacent, not overlapping
    }

    #[test]
    fn hunks_zero_count_no_overlap() {
        let a = DiffHunk {
            old_start: 0,
            old_count: 0,
            new_start: 1,
            new_count: 25,
        };
        let b = DiffHunk {
            old_start: 0,
            old_count: 0,
            new_start: 1,
            new_count: 10,
        };
        assert!(!hunks_overlap(&a, &b)); // pure additions don't conflict on old side
    }

    #[test]
    fn change_type_from_flags() {
        assert_eq!(
            change_type_from_diff(true, false, false),
            FileChangeType::Added
        );
        assert_eq!(
            change_type_from_diff(false, true, false),
            FileChangeType::Deleted
        );
        assert_eq!(
            change_type_from_diff(false, false, true),
            FileChangeType::Renamed
        );
        assert_eq!(
            change_type_from_diff(false, false, false),
            FileChangeType::Modified
        );
    }

    #[test]
    fn cluster_id_deterministic() {
        let a = cluster_id(42, &[3, 1, 2]);
        let b = cluster_id(42, &[2, 3, 1]);
        assert_eq!(a, b); // order doesn't matter
    }

    #[test]
    fn cluster_id_different_projects() {
        let a = cluster_id(42, &[1, 2]);
        let b = cluster_id(43, &[1, 2]);
        assert_ne!(a, b);
    }
}
