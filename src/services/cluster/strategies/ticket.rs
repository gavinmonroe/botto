// ---------------------------------------------------------------------------
// TicketClusterStrategy — groups MRs sharing a Jira/ticket key.
//
// Parses ticket keys from MR titles and branch names using the same regex
// pattern as Otto's ticket-grouper.ts. This is the strongest clustering
// signal — if two MRs reference the same ticket, they're almost certainly
// part of the same unit of work.
//
// Relevance: 0.9 (high confidence).
// ---------------------------------------------------------------------------

use super::{ClusterCandidate, ClusterStrategy};
use crate::services::gitlab::client::{self, GitLabConfig};
use crate::types::cluster::ClusterSignal;
use anyhow::Result;
use sqlx::SqlitePool;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use tracing::debug;

/// Regex pattern for Jira-style ticket keys: PROJ-123, ABC-1, etc.
/// Matches Otto's ticket-grouper.ts pattern exactly.
fn extract_ticket_keys(text: &str) -> Vec<String> {
    // Simple manual parser — avoids pulling in the regex crate for one pattern.
    // Matches: word boundary, 1+ alpha, hyphen, 1+ digit, word boundary.
    let mut keys = Vec::new();
    let chars: Vec<char> = text.chars().collect();
    let len = chars.len();
    let mut i = 0;

    while i < len {
        // Look for start of a potential key: alpha char at word boundary
        if chars[i].is_ascii_alphabetic() && (i == 0 || !chars[i - 1].is_ascii_alphanumeric()) {
            let start = i;
            // Consume alpha chars (project prefix)
            while i < len && chars[i].is_ascii_alphanumeric() && !chars[i].is_ascii_digit() {
                i += 1;
            }
            // Must have at least 1 alpha char, then a hyphen
            if i > start && i < len && chars[i] == '-' {
                let _prefix_end = i;
                i += 1; // skip hyphen
                let digit_start = i;
                // Consume digits
                while i < len && chars[i].is_ascii_digit() {
                    i += 1;
                }
                // Must have at least 1 digit, and end at word boundary
                if i > digit_start && (i >= len || !chars[i].is_ascii_alphanumeric()) {
                    let key: String = chars[start..i].iter().collect();
                    // Normalize to uppercase for consistent grouping
                    keys.push(key.to_uppercase());
                }
                continue;
            }
        }
        i += 1;
    }

    keys
}

/// Normalize a branch name for ticket key extraction.
/// Replaces slashes and underscores with spaces (matches Otto's behavior).
fn normalize_branch(branch: &str) -> String {
    branch.replace(['/', '_'], " ")
}

pub struct TicketClusterStrategy;

impl ClusterStrategy for TicketClusterStrategy {
    fn find_clusters(
        &self,
        _pool: &SqlitePool,
        gitlab_cfg: &GitLabConfig,
        project_id: i64,
        mr_iid: u64,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ClusterCandidate>>> + Send + '_>> {
        let gitlab_cfg = gitlab_cfg.clone();
        Box::pin(async move {
            find_clusters_impl(&gitlab_cfg, project_id, mr_iid).await
        })
    }
}

async fn find_clusters_impl(
    gitlab_cfg: &GitLabConfig,
    project_id: i64,
    mr_iid: u64,
) -> Result<Vec<ClusterCandidate>> {
    // 1. Fetch all open MRs for the project
    let open_mrs = client::fetch_open_mrs(gitlab_cfg, project_id).await?;

    if open_mrs.is_empty() {
        return Ok(Vec::new());
    }

    // 2. Extract ticket keys from each MR's title and branch name
    let mut ticket_to_mrs: HashMap<String, Vec<u64>> = HashMap::new();

    for mr in &open_mrs {
        let mut keys = extract_ticket_keys(&mr.title);
        keys.extend(extract_ticket_keys(&normalize_branch(&mr.source_branch)));

        // Deduplicate keys per MR
        keys.sort();
        keys.dedup();

        for key in keys {
            ticket_to_mrs.entry(key).or_default().push(mr.iid);
        }
    }

    // 3. Find groups containing our MR with 2+ members
    let mut candidates = Vec::new();

    for (ticket_key, mut mr_iids) in ticket_to_mrs {
        if !mr_iids.contains(&mr_iid) || mr_iids.len() < 2 {
            continue;
        }

        // Deduplicate and sort for deterministic cluster IDs
        mr_iids.sort_unstable();
        mr_iids.dedup();

        debug!(
            "ticket cluster: {} groups {} MRs (including !{})",
            ticket_key,
            mr_iids.len(),
            mr_iid
        );

        candidates.push(ClusterCandidate {
            mr_iids,
            signal: ClusterSignal::SharedTicket {
                ticket_key: ticket_key.clone(),
            },
            relevance: 0.9,
            ticket_key: Some(ticket_key),
        });
    }

    Ok(candidates)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_basic_ticket_key() {
        assert_eq!(extract_ticket_keys("PROJ-123 fix auth"), vec!["PROJ-123"]);
    }

    #[test]
    fn extract_multiple_keys() {
        let keys = extract_ticket_keys("PROJ-123 and FEAT-456 related");
        assert_eq!(keys, vec!["PROJ-123", "FEAT-456"]);
    }

    #[test]
    fn extract_from_branch_name() {
        let branch = normalize_branch("feature/PROJ-123_add-auth");
        let keys = extract_ticket_keys(&branch);
        assert_eq!(keys, vec!["PROJ-123"]);
    }

    #[test]
    fn extract_case_insensitive() {
        assert_eq!(extract_ticket_keys("proj-123"), vec!["PROJ-123"]);
    }

    #[test]
    fn no_false_positives() {
        // These should NOT match
        assert!(extract_ticket_keys("v1-2").is_empty()); // too short prefix? Actually v1 is alpha+digit
        assert!(extract_ticket_keys("123-456").is_empty()); // no alpha prefix
        assert!(extract_ticket_keys("no tickets here").is_empty());
        assert!(extract_ticket_keys("").is_empty());
    }

    #[test]
    fn extract_at_boundaries() {
        assert_eq!(extract_ticket_keys("PROJ-1"), vec!["PROJ-1"]);
        assert_eq!(extract_ticket_keys("[PROJ-123]"), vec!["PROJ-123"]);
        assert_eq!(extract_ticket_keys("(PROJ-123)"), vec!["PROJ-123"]);
    }

    #[test]
    fn normalize_branch_replaces_separators() {
        assert_eq!(normalize_branch("feature/PROJ-123_impl"), "feature PROJ-123 impl");
    }
}
