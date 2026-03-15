// ---------------------------------------------------------------------------
// Message protocol types — matches Otto's types/messages.ts wire format.
//
// These are the payload shapes inside WsInbound::Request and StreamChunk.
// The WebSocket framing (WsInbound/WsOutbound) is in api/ws.rs.
// ---------------------------------------------------------------------------

use serde::{Deserialize, Serialize};

/// Generic result type matching Otto's Result<T>.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum OttoResult<T> {
    Ok { ok: bool, data: T },
    Err { ok: bool, error: String },
}

impl<T: Serialize> OttoResult<T> {
    pub fn success(data: T) -> serde_json::Value {
        serde_json::json!({ "ok": true, "data": data })
    }

    pub fn failure(error: &str) -> serde_json::Value {
        serde_json::json!({ "ok": false, "error": error })
    }
}

/// Stream chunk types — matches Otto's StreamChunk discriminated union.
/// Used as the `type` field in stream chunks sent over WebSocket.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum StreamChunkType {
    // Summary
    #[serde(rename = "STREAM_SUMMARY_DELTA")]
    SummaryDelta,
    #[serde(rename = "STREAM_SUMMARY_COMPLETE")]
    SummaryComplete,

    // Code review (per-file)
    #[serde(rename = "STREAM_FILE_REVIEW_DELTA")]
    FileReviewDelta,
    #[serde(rename = "STREAM_FILE_REVIEW_COMPLETE")]
    FileReviewComplete,

    // Edge cases
    #[serde(rename = "STREAM_EDGE_CASES_DELTA")]
    EdgeCasesDelta,
    #[serde(rename = "STREAM_EDGE_CASES_COMPLETE")]
    EdgeCasesComplete,

    // Related files
    #[serde(rename = "STREAM_RELATED_FILES_COMPLETE")]
    RelatedFilesComplete,

    // File activity
    #[serde(rename = "STREAM_FILE_ACTIVITY_COMPLETE")]
    FileActivityComplete,

    // AC validation
    #[serde(rename = "STREAM_AC_VALIDATION_COMPLETE")]
    AcValidationComplete,

    // Verification
    #[serde(rename = "STREAM_ADVERSARIAL_TESTS_COMPLETE")]
    AdversarialTestsComplete,
    #[serde(rename = "STREAM_CONTRACTS_COMPLETE")]
    ContractsComplete,
    #[serde(rename = "STREAM_BEHAVIORAL_DELTA_COMPLETE")]
    BehavioralDeltaComplete,
    #[serde(rename = "STREAM_TRUST_COMPLETE")]
    TrustComplete,

    // Progress + control
    #[serde(rename = "STREAM_PROGRESS")]
    Progress,
    #[serde(rename = "STREAM_TASK_ERROR")]
    TaskError,
    #[serde(rename = "STREAM_ALL_COMPLETE")]
    AllComplete,
    #[serde(rename = "STREAM_REVIEW_PAUSED")]
    ReviewPaused,

    // Chat
    #[serde(rename = "STREAM_CHAT_DELTA")]
    ChatDelta,
    #[serde(rename = "STREAM_CHAT_COMPLETE")]
    ChatComplete,
}
