// ---------------------------------------------------------------------------
// Review types — ported from Otto's types/review.ts.
// These are the core data structures for the review system.
// ---------------------------------------------------------------------------

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MrContext {
    pub project_path: String,
    pub project_id: Option<i64>,
    pub mr_iid: u64,
    pub host_url: String,
    pub title: String,
    pub description: Option<String>,
    pub source_branch: String,
    pub target_branch: String,
    pub author_username: Option<String>,
    pub diff_files: Vec<DiffFileData>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DiffFileData {
    pub file_path: String,
    pub old_path: Option<String>,
    #[serde(default)]
    pub is_new: bool,
    #[serde(default)]
    pub is_deleted: bool,
    #[serde(default)]
    pub is_renamed: bool,
    pub diff: String,
    #[serde(default)]
    pub added_lines: u32,
    #[serde(default)]
    pub removed_lines: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ReviewCommentSeverity {
    Critical,
    Warning,
    Suggestion,
    Info,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum ReviewCommentCategory {
    Bug,
    LogicError,
    Security,
    Performance,
    Readability,
    Style,
    ErrorHandling,
    Naming,
    Duplication,
    Other,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ReviewCommentStatus {
    Pending,
    Accepted,
    Dismissed,
    Edited,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReviewComment {
    pub id: String,
    pub file_path: String,
    pub start_line: Option<u32>,
    pub end_line: Option<u32>,
    pub severity: ReviewCommentSeverity,
    pub category: ReviewCommentCategory,
    pub title: String,
    pub body: String,
    pub original_code: Option<String>,
    pub suggestion: Option<String>,
    pub suggestion_summary: Option<String>,
    pub status: ReviewCommentStatus,
    pub edited_body: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum RiskLevel {
    Low,
    Medium,
    High,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileReview {
    pub file_path: String,
    pub comments: Vec<ReviewComment>,
    pub summary: String,
    pub risk_level: RiskLevel,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum RelatedFileRelationship {
    Imports,
    ImportedBy,
    SharedType,
    Test,
    Config,
    Other,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RelatedFile {
    pub file_path: String,
    pub reason: String,
    pub content: Option<String>,
    pub relationship: RelatedFileRelationship,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum EdgeCaseSeverity {
    Critical,
    Moderate,
    Minor,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum EdgeCaseCategory {
    ErrorHandling,
    BoundaryCondition,
    RaceCondition,
    NullSafety,
    TypeSafety,
    ResourceLeak,
    Other,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LineRange {
    pub start: u32,
    pub end: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EdgeCase {
    pub id: String,
    pub title: String,
    pub description: String,
    pub file_path: Option<String>,
    pub line_range: Option<LineRange>,
    pub severity: EdgeCaseSeverity,
    pub category: EdgeCaseCategory,
    pub hypothetical_trace: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MrSummary {
    pub overview: String,
    pub risk_assessment: String,
    pub key_changes: Vec<String>,
    pub affected_areas: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ReviewStatus {
    Idle,
    Loading,
    Streaming,
    Complete,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RecentMr {
    pub iid: u64,
    pub title: String,
    pub author: String,
    pub merged_at: String,
    pub web_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileActivity {
    pub file_path: String,
    pub recent_mrs: Vec<RecentMr>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileActivityData {
    pub file_activities: Vec<FileActivity>,
    pub total_recent_mrs: u32,
    pub lookback_days: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum AcValidationStatus {
    Satisfied,
    Unclear,
    NotFound,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AcEvidence {
    pub file_path: String,
    pub start_line: Option<u32>,
    pub end_line: Option<u32>,
    pub snippet: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AcCriterionResult {
    pub criterion: String,
    pub status: AcValidationStatus,
    pub explanation: String,
    pub evidence: Vec<AcEvidence>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AcValidationResult {
    pub ticket_key: String,
    pub criteria: Vec<AcCriterionResult>,
    pub summary: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AcValidationData {
    pub results: Vec<AcValidationResult>,
    pub satisfied_count: u32,
    pub unclear_count: u32,
    pub not_found_count: u32,
}

/// The full cached review — everything needed to hydrate the UI.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CachedReview {
    pub version: u32,
    pub summary: Option<MrSummary>,
    pub file_reviews: Vec<FileReview>,
    pub related_files: Vec<RelatedFile>,
    pub edge_cases: Vec<EdgeCase>,
    pub file_activity: Option<FileActivityData>,
    pub ac_validation: Option<AcValidationData>,
    pub verification: Option<crate::types::verification::VerificationData>,
}

/// Review task types that the orchestrator can execute.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub enum ReviewTask {
    Summary,
    CodeReview,
    EdgeCases,
    RelatedFiles,
    FileActivity,
    AdversarialTests,
    Contracts,
    BehavioralDelta,
}

/// Per-task progress tracking.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskProgress {
    pub status: ReviewStatus,
    pub error: Option<String>,
    pub files_total: u32,
    pub files_complete: u32,
}
