// ---------------------------------------------------------------------------
// Cluster strategies — trait + implementations for detecting MR clusters.
//
// Each strategy produces ClusterCandidate values from a different signal.
// The ClusterDetector composes all strategies and merges overlapping results.
// ---------------------------------------------------------------------------

pub mod ticket;
pub mod file_overlap;

use crate::services::gitlab::client::GitLabConfig;
use crate::types::cluster::ClusterSignal;
use anyhow::Result;
use sqlx::SqlitePool;
use std::future::Future;
use std::pin::Pin;

/// A candidate cluster found by a single strategy.
/// Multiple candidates may be merged by the detector if they share MR IIDs.
pub struct ClusterCandidate {
    /// MR IIDs that form this candidate cluster.
    pub mr_iids: Vec<u64>,
    /// The signal that produced this candidate.
    pub signal: ClusterSignal,
    /// Relevance score (0.0–1.0). Higher = stronger signal.
    pub relevance: f64,
    /// Optional ticket key (only set by TicketClusterStrategy).
    pub ticket_key: Option<String>,
}

/// Trait for cluster detection strategies. Each strategy independently
/// finds groups of related MRs from a different signal source.
///
/// Strategies are stateless — all context is passed per-call. This makes
/// them easy to test and compose without shared mutable state.
///
/// Uses `Pin<Box<dyn Future>>` return type for dyn compatibility — the
/// detector holds a `&[&dyn ClusterStrategy]` to compose multiple strategies.
pub trait ClusterStrategy: Send + Sync {
    /// Find cluster candidates involving the given MR.
    /// Returns zero or more candidates — the detector handles merging.
    fn find_clusters(
        &self,
        pool: &SqlitePool,
        gitlab_cfg: &GitLabConfig,
        project_id: i64,
        mr_iid: u64,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ClusterCandidate>>> + Send + '_>>;
}
