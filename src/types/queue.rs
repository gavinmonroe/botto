// ---------------------------------------------------------------------------
// Queue types — review queue items and priority scoring.
// ---------------------------------------------------------------------------

use serde::{Deserialize, Serialize};

use super::review::{MrContext, TaskProgress};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum QueueItemStatus {
    Queued,
    Running,
    Paused,
    Complete,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReviewPriority {
    pub score: f64,
    pub risk_level: String,
    pub signals: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReviewProgressSnapshot {
    pub overall_percent: f64,
    pub tasks: std::collections::HashMap<String, TaskProgress>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueuedReview {
    pub id: i64,
    pub project_path: String,
    pub mr_iid: u64,
    pub priority: ReviewPriority,
    pub status: QueueItemStatus,
    pub mr_context: MrContext,
    pub progress: Option<ReviewProgressSnapshot>,
    pub error: Option<String>,
    pub enqueued_at: i64,
    pub started_at: Option<i64>,
    pub completed_at: Option<i64>,
}
