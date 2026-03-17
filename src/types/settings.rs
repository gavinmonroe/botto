// ---------------------------------------------------------------------------
// Settings types — server-side config exposed to Otto clients.
// Not a 1:1 port of Otto's settings (those are client-side).
// This represents what Botto tells Otto about its capabilities.
// ---------------------------------------------------------------------------

use serde::{Deserialize, Serialize};

/// AI task types — matches Otto's AiTaskType.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub enum AiTaskType {
    Summary,
    CodeReview,
    EdgeCases,
    RelatedFiles,
    FollowUp,
    Chat,
    AcValidation,
    AdversarialTests,
    Contracts,
    BehavioralDelta,
    Inquiry,
    /// AI-powered semantic conflict analysis (opt-in).
    SemanticConflict,
    /// Unified narrative across clustered MRs.
    ClusterSummary,
    /// Ordered phases for cross-MR guided walkthrough.
    ClusterReviewOrder,
}

/// Per-task AI configuration (model, temperature, max_tokens).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskAiConfig {
    pub model: String,
    pub temperature: f32,
    pub max_tokens: u32,
}

/// Default temperatures per task — matches Otto's defaults.
pub fn default_temperature(task: AiTaskType) -> f32 {
    match task {
        AiTaskType::Summary => 0.3,
        AiTaskType::CodeReview => 0.2,
        AiTaskType::EdgeCases => 0.4,
        AiTaskType::RelatedFiles => 0.1,
        AiTaskType::FollowUp => 0.3,
        AiTaskType::Chat => 0.4,
        AiTaskType::AcValidation => 0.2,
        AiTaskType::AdversarialTests => 0.3,
        AiTaskType::Contracts => 0.2,
        AiTaskType::BehavioralDelta => 0.3,
        AiTaskType::Inquiry => 0.3,
        AiTaskType::SemanticConflict => 0.2,
        AiTaskType::ClusterSummary => 0.3,
        AiTaskType::ClusterReviewOrder => 0.2,
    }
}
