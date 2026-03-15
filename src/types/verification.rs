// ---------------------------------------------------------------------------
// Verification types — ported from Otto's types/verification.ts.
// ---------------------------------------------------------------------------

use serde::{Deserialize, Serialize};

use super::review::LineRange;

// ---------------------------------------------------------------------------
// Adversarial Tests
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PropertyTest {
    pub id: String,
    pub property: String,
    pub test_code: String,
    pub target_function: String,
    pub file_path: String,
    pub line_range: Option<LineRange>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum PropertyTestStatus {
    Held,
    Counterexample,
    Error,
    NotRun,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PropertyTestResult {
    pub test_id: String,
    pub status: PropertyTestStatus,
    pub iterations: Option<u32>,
    pub counterexample: Option<String>,
    pub error_message: Option<String>,
    pub ai_reasoned: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileTestData {
    pub file_path: String,
    pub tests: Vec<PropertyTest>,
    pub results: Vec<PropertyTestResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AdversarialTestData {
    pub files: Vec<FileTestData>,
    pub total_tests: u32,
    pub total_held: u32,
    pub total_counterexamples: u32,
    pub total_errors: u32,
}

// ---------------------------------------------------------------------------
// Contracts
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContractStatement {
    pub human: String,
    pub code: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum VerificationStatus {
    Verified,
    ViolationPossible,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FunctionContract {
    pub id: String,
    pub function_name: String,
    pub file_path: String,
    pub line_range: Option<LineRange>,
    pub preconditions: Vec<ContractStatement>,
    pub postconditions: Vec<ContractStatement>,
    pub invariants: Vec<ContractStatement>,
    pub verification_status: VerificationStatus,
    pub violation_path: Option<String>,
    pub ai_reasoned: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContractData {
    pub contracts: Vec<FunctionContract>,
    pub total_verified: u32,
    pub total_violations: u32,
    pub total_unknown: u32,
}

// ---------------------------------------------------------------------------
// Behavioral Delta
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum BehaviorChangeType {
    Changed,
    Preserved,
    Unexpected,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BehaviorEntry {
    pub id: String,
    pub description: String,
    #[serde(rename = "type")]
    pub change_type: BehaviorChangeType,
    pub test_scenario: String,
    pub expected_outcome: String,
    pub actual_outcome: Option<String>,
    pub file_paths: Vec<String>,
    pub verified: bool,
    pub ai_reasoned: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BehavioralDeltaData {
    pub changed: Vec<BehaviorEntry>,
    pub preserved: Vec<BehaviorEntry>,
    pub unexpected: Vec<BehaviorEntry>,
    pub summary: String,
}

// ---------------------------------------------------------------------------
// CI Bridge
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum CiExecutionMethod {
    GitlabCi,
    Server,
    Local,
    AiOnly,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum CiJobStatus {
    Pending,
    Running,
    Success,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CiVerificationJob {
    pub method: CiExecutionMethod,
    pub pipeline_id: Option<i64>,
    pub pipeline_url: Option<String>,
    pub job_status: CiJobStatus,
    pub started_at: i64,
    pub completed_at: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CiExecutionResult {
    pub job: CiVerificationJob,
    pub test_results: Vec<PropertyTestResult>,
    pub mutation_score: Option<f64>,
    pub coverage_delta: Option<f64>,
    pub execution_time_ms: u64,
}

// ---------------------------------------------------------------------------
// Trust Calibration
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TrustLevel {
    High,
    Medium,
    Low,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrustSignals {
    pub mutation_score: Option<f64>,
    pub coverage_delta: Option<f64>,
    pub counterexample_quality: f64,
    pub test_independence: f64,
    pub non_tautological: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrustAssessment {
    pub level: TrustLevel,
    pub score: f64,
    pub signals: TrustSignals,
    pub explanation: String,
    pub surviving_mutants: Vec<String>,
    pub can_strengthen: bool,
}

// ---------------------------------------------------------------------------
// Composite
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum VerificationDataStatus {
    Idle,
    Generating,
    Executing,
    Complete,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VerificationData {
    pub status: VerificationDataStatus,
    pub error: Option<String>,
    pub adversarial_tests: Option<AdversarialTestData>,
    pub contracts: Option<ContractData>,
    pub behavioral_delta: Option<BehavioralDeltaData>,
    pub execution: Option<CiExecutionResult>,
    pub execution_method: CiExecutionMethod,
    pub trust: Option<TrustAssessment>,
    pub generated_at: Option<i64>,
    pub executed_at: Option<i64>,
}
