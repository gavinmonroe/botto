// ---------------------------------------------------------------------------
// Directive types — data structures for the standing-order directive engine.
//
// Directives are persistent, polling-based work discoverers. They continuously
// scan sources (explicit URLs or inferred from connectors), triage discovered
// items against the directive's intent, and spawn workflow sessions for accepted
// work.
//
// All structs use camelCase serialization to match Otto's conventions.
// Enums use snake_case for DB-friendly storage.
// ---------------------------------------------------------------------------

use serde::{Deserialize, Serialize};

use crate::services::channels::types::ReplyTarget;

// ---------------------------------------------------------------------------
// Directive — the persistent standing order
// ---------------------------------------------------------------------------

/// A directive: a standing order that polls sources for work items.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Directive {
    pub id: String,
    pub name: String,
    /// The natural-language intent describing what work to look for and how to handle it.
    pub intent: String,
    pub sources: Vec<WorkSource>,
    pub constraints: DirectiveConstraints,
    pub priority: i32,
    pub status: DirectiveStatus,
    pub poll_interval_secs: i64,
    pub last_poll_at: Option<i64>,
    pub next_poll_at: Option<i64>,
    pub escalation: Option<DirectiveEscalation>,
    pub created_by: Option<String>,
    /// Channel reply context — if this directive was created from a channel command,
    /// this stores where to send notifications (escalations, completions).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reply_context: Option<ReplyTarget>,
    pub created_at: i64,
    pub updated_at: i64,
}

// ---------------------------------------------------------------------------
// Work sources
// ---------------------------------------------------------------------------

/// Where a directive discovers work items.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum WorkSource {
    /// An explicit source with a known type and optional URL.
    Explicit {
        source_type: String,
        url: Option<String>,
        #[serde(default)]
        params: serde_json::Value,
    },
    /// Inferred from Mentor connectors — polls all connectors matching a category.
    Inferred {
        category: String,
        #[serde(default)]
        filter: Option<String>,
    },
}

// ---------------------------------------------------------------------------
// Constraints
// ---------------------------------------------------------------------------

/// Constraints that limit when and how a directive operates.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct DirectiveConstraints {
    /// Maximum concurrent sessions spawned by this directive.
    #[serde(default = "default_max_concurrent")]
    pub max_concurrent_sessions: u32,
    /// Working hours start (0–23, local time).
    pub working_hours_start: Option<u32>,
    /// Working hours end (0–23, local time).
    pub working_hours_end: Option<u32>,
    /// Maximum items to accept per poll cycle.
    #[serde(default = "default_max_items_per_poll")]
    pub max_items_per_poll: u32,
}

fn default_max_concurrent() -> u32 {
    3
}

fn default_max_items_per_poll() -> u32 {
    10
}

// ---------------------------------------------------------------------------
// Directive status
// ---------------------------------------------------------------------------

/// Lifecycle status of a directive.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DirectiveStatus {
    Active,
    Paused,
    WaitingForHuman,
    Retired,
}

impl DirectiveStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Paused => "paused",
            Self::WaitingForHuman => "waiting_for_human",
            Self::Retired => "retired",
        }
    }

    pub fn is_active(&self) -> bool {
        matches!(self, Self::Active)
    }
}

impl std::fmt::Display for DirectiveStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

// ---------------------------------------------------------------------------
// Work items
// ---------------------------------------------------------------------------

/// A raw work item discovered from a source (before triage).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkItem {
    pub external_id: String,
    pub source_type: String,
    pub source_url: Option<String>,
    pub title: String,
    pub description: Option<String>,
    #[serde(default)]
    pub metadata: serde_json::Value,
}

/// A work item that has been persisted and tracked in the database.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrackedWorkItem {
    pub directive_id: String,
    pub external_id: String,
    pub source_type: String,
    pub source_url: Option<String>,
    pub title: String,
    pub description: Option<String>,
    pub metadata: serde_json::Value,
    pub session_id: Option<String>,
    pub status: WorkItemStatus,
    pub triage_reason: Option<String>,
    pub priority: i32,
    pub discovered_at: i64,
    pub updated_at: i64,
}

/// Status of a tracked work item.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WorkItemStatus {
    Discovered,
    Accepted,
    Rejected,
    InProgress,
    Completed,
    Failed,
}

impl WorkItemStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Discovered => "discovered",
            Self::Accepted => "accepted",
            Self::Rejected => "rejected",
            Self::InProgress => "in_progress",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }

    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Rejected)
    }
}

impl std::fmt::Display for WorkItemStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

// ---------------------------------------------------------------------------
// Triage decision
// ---------------------------------------------------------------------------

/// The AI triager's decision about a discovered work item.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "decision")]
pub enum TriageDecision {
    Accept {
        reason: String,
        priority: i32,
    },
    Reject {
        reason: String,
    },
    NeedsMoreContext {
        question: String,
    },
    AlreadyTracked,
}

// ---------------------------------------------------------------------------
// Escalation
// ---------------------------------------------------------------------------

/// Escalation state for a directive (e.g., too many empty polls, high failure rate).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DirectiveEscalation {
    pub reason: String,
    pub severity: String,
    pub consecutive_empty_polls: u32,
    pub failure_rate: Option<f64>,
    pub created_at: i64,
}
