// ---------------------------------------------------------------------------
// Sandbox types — job tracking for the auto-fix feature.
// ---------------------------------------------------------------------------

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SandboxJobStatus {
    Pending,
    Cloning,
    SettingUp,
    Running,
    Testing,
    Pushing,
    Complete,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SandboxStrategy {
    FullSetup,
    TestOnly,
    Auto,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SandboxJob {
    pub id: String,
    pub project_path: String,
    pub mr_iid: u64,
    pub comment_id: Option<String>,
    pub status: SandboxJobStatus,
    pub strategy: SandboxStrategy,
    pub container_id: Option<String>,
    pub fix_diff: Option<String>,
    pub test_output: Option<String>,
    pub commit_sha: Option<String>,
    pub error: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}
