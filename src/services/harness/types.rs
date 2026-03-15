// ---------------------------------------------------------------------------
// Harness types — data structures for the prompt evolution harness.
//
// Design notes:
// - PromptVariant stores *templates* with named placeholders (e.g. {project},
//   {context}) because the sandbox prompts are format strings filled at runtime.
// - CodeParams has per-agent values because setup/fix/retry each use different
//   temperature and max_tokens in production.
// - All types derive Serialize + Deserialize for TOML persistence.
// ---------------------------------------------------------------------------

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Difficulty classification for test cases
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Difficulty {
    Easy,
    Medium,
    Hard,
}

impl std::fmt::Display for Difficulty {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Difficulty::Easy => write!(f, "easy"),
            Difficulty::Medium => write!(f, "medium"),
            Difficulty::Hard => write!(f, "hard"),
        }
    }
}

// ---------------------------------------------------------------------------
// Test case — a real MR with a known issue to fix
// ---------------------------------------------------------------------------

/// A test case extracted from a real GitLab MR. Contains enough information
/// to construct a `FixRequest` and run the sandbox pipeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestCase {
    /// Unique identifier (e.g. "tc-001")
    pub id: String,
    /// GitLab project path (e.g. "gitlab-org/gitlab-runner")
    pub project_path: String,
    /// MR IID within the project
    pub mr_iid: u64,
    /// Source branch name
    pub source_branch: String,
    /// Target branch name
    pub target_branch: String,
    /// File path being modified
    pub file_path: String,
    /// The code before the fix was applied (what the sandbox starts with)
    pub original_code: String,
    /// What's wrong — from the review comment
    pub expected_issue: String,
    /// The suggested fix (what the review comment proposed)
    pub suggestion: String,
    /// Judge-assessed difficulty
    pub difficulty: Difficulty,
    /// Test command if known (otherwise auto-detected by sandbox)
    pub test_command: Option<String>,
    /// MR title for context
    pub mr_title: Option<String>,
    /// MR description for context
    pub mr_description: Option<String>,
    /// The review comment body
    pub comment_body: Option<String>,
    /// Full file content at the time of the MR
    pub file_content: Option<String>,
    /// Unified diff of the file
    pub file_diff: Option<String>,
    /// When this test case was created
    pub created_at: DateTime<Utc>,
    /// Source URL for reference (the MR URL)
    pub source_url: Option<String>,
}

// ---------------------------------------------------------------------------
// Code parameters — tunable values extracted from hardcoded sandbox constants
// ---------------------------------------------------------------------------

/// Per-agent AI call parameters. The sandbox has 3 agent loops, each with
/// different defaults in production:
///   - Setup:  temperature=0.1, max_tokens=1000
///   - Fix:    temperature=0.2, max_tokens=2000
///   - Retry:  temperature=0.1, max_tokens=500
///
/// The harness can mutate these to find better values.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentParams {
    pub temperature: f32,
    pub max_tokens: u32,
}

/// All tunable code parameters across the 3 sandbox agent loops.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CodeParams {
    /// AI params for the project setup agent
    pub setup: AgentParams,
    /// AI params for the test-fix agent
    pub fix: AgentParams,
    /// AI params for the retry/env-fix agent
    pub retry: AgentParams,
    /// Conversation history trim threshold (messages.len() > this triggers trim)
    pub history_trim_threshold: u32,
    /// How many recent messages to keep after trimming (system msg + these)
    pub history_keep_count: u32,
}

impl Default for CodeParams {
    fn default() -> Self {
        // Matches the current hardcoded values in sandbox/manager.rs
        Self {
            setup: AgentParams {
                temperature: 0.1,
                max_tokens: 1000,
            },
            fix: AgentParams {
                temperature: 0.2,
                max_tokens: 2000,
            },
            retry: AgentParams {
                temperature: 0.1,
                max_tokens: 500,
            },
            history_trim_threshold: 42,
            history_keep_count: 40,
        }
    }
}

// ---------------------------------------------------------------------------
// Prompt variant — a set of prompt templates + code params to test
// ---------------------------------------------------------------------------

/// A prompt variant to be tested by the harness. Contains template strings
/// for all 3 sandbox agent prompts plus tunable code parameters.
///
/// Template placeholders (must be preserved in mutations):
///   Setup:  {project}, {file_path}, {test_cmd}
///   Fix:    {context}, {original}, {suggestion}, {test_cmd}
///   Retry:  {context}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PromptVariant {
    /// Unique identifier (e.g. "v000", "v001")
    pub id: String,
    /// Which evolution round produced this (0 = baseline)
    pub generation: u32,
    /// Which variant this was mutated from (None = baseline)
    pub parent_id: Option<String>,
    /// System prompt template for the project setup agent.
    /// Placeholders: {project}, {file_path}, {test_cmd}
    pub setup_prompt: String,
    /// System prompt template for the test-fix agent.
    /// Placeholders: {context}, {original}, {suggestion}, {test_cmd}
    pub fix_prompt: String,
    /// System prompt template for the retry/env-fix agent.
    /// Placeholder: {context}
    pub retry_prompt: String,
    /// Tunable code parameters
    pub code_params: CodeParams,
    /// Metadata
    pub metadata: PromptMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PromptMetadata {
    /// Who created this variant ("baseline", "judge", "manual")
    pub author: String,
    /// When it was created
    pub created_at: DateTime<Utc>,
    /// Free-form notes about what was changed
    pub notes: String,
    /// Mutation strategy used (e.g. "structural", "tonal", "aggressive")
    pub mutation_strategy: Option<String>,
}

// ---------------------------------------------------------------------------
// Run result — raw output from a single harness run
// ---------------------------------------------------------------------------

/// Raw result from running one prompt variant against one test case.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunResult {
    /// Which variant was tested
    pub variant_id: String,
    /// Which test case was used
    pub test_case_id: String,
    /// Did the tests pass?
    pub passed: bool,
    /// Number of AI steps taken across all agent loops
    pub total_iterations: u32,
    /// Breakdown: setup steps, fix steps, retry steps
    pub iteration_breakdown: IterationBreakdown,
    /// Wall clock time in seconds
    pub wall_time_secs: f64,
    /// Approximate token usage (if available from API)
    pub tokens_used: u64,
    /// Full AI conversation log (for judge analysis)
    pub conversation_log: Vec<ConversationEntry>,
    /// Error message if the run failed for infrastructure reasons
    pub error: Option<String>,
    /// The sandbox FixResult fields
    pub fix_output: Option<String>,
    pub commit_sha: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IterationBreakdown {
    pub setup_steps: u32,
    pub fix_steps: u32,
    pub retry_steps: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConversationEntry {
    pub agent: String, // "setup", "fix", "retry"
    pub role: String,  // "system", "user", "assistant"
    pub content: String,
    pub timestamp: DateTime<Utc>,
}

// ---------------------------------------------------------------------------
// Harness grade — scored result from the grader
// ---------------------------------------------------------------------------

/// Scored result for one variant on one test case.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HarnessGrade {
    pub variant_id: String,
    pub test_case_id: String,
    /// Did the fix pass tests?
    pub passed: bool,
    /// Total AI iterations across all agents
    pub iterations: u32,
    /// Wall clock seconds
    pub wall_time_secs: f64,
    /// Tokens consumed
    pub tokens_used: u64,
    /// Composite score 0-100
    pub score: f64,
    /// Breakdown of score components
    pub score_breakdown: ScoreBreakdown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScoreBreakdown {
    /// 0 or 50 — binary pass/fail
    pub pass_score: f64,
    /// 0-25 — fewer iterations = better
    pub iteration_score: f64,
    /// 0-15 — faster = better
    pub time_score: f64,
    /// 0-10 — fewer tokens = better
    pub token_score: f64,
}

// ---------------------------------------------------------------------------
// Round report — results from one evolution round
// ---------------------------------------------------------------------------

/// Aggregate results from one round of the evolution loop.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoundReport {
    /// Round number (1-indexed)
    pub round: u32,
    /// All variants tested in this round
    pub variants_tested: Vec<String>,
    /// Per-variant aggregate scores (variant_id → mean score across test cases)
    pub variant_scores: Vec<VariantScore>,
    /// The winning variant ID
    pub winner_id: String,
    /// The parent variant ID (what we evolved from)
    pub parent_id: String,
    /// Did the winner improve over the parent?
    pub improved: bool,
    /// Score delta (winner - parent)
    pub score_delta: f64,
    /// Judge's analysis of what worked and what didn't
    pub judge_analysis: String,
    /// Key learnings extracted by the judge
    pub learnings: Vec<String>,
    /// When this round completed
    pub completed_at: DateTime<Utc>,
    /// All individual grades for detailed analysis
    pub grades: Vec<HarnessGrade>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VariantScore {
    pub variant_id: String,
    /// Mean score across all test cases
    pub mean_score: f64,
    /// Number of test cases that passed
    pub pass_count: u32,
    /// Total test cases run
    pub total_cases: u32,
    /// Mean iterations for passing cases
    pub mean_iterations: f64,
}
