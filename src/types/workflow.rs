// ---------------------------------------------------------------------------
// Workflow types — core data structures for the autonomous workflow engine.
//
// These types cover the full lifecycle: definition (DAG of steps with
// triggers), runtime state (runs, step states, agent results), and
// supporting enums/structs for inputs, retries, and verification.
//
// Serialized to JSON for SQLite storage and WebSocket wire format.
// All structs use camelCase serialization to match Otto's conventions.
// ---------------------------------------------------------------------------

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Workflow Definition — the persistent DAG template
// ---------------------------------------------------------------------------

/// Whether the workflow uses the v1 step-by-step orchestrator or the v2
/// session-based autonomous orchestrator.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowMode {
    Simple,
    Autonomous,
}

impl Default for WorkflowMode {
    fn default() -> Self {
        Self::Autonomous
    }
}

/// A workflow definition: a named DAG of steps with triggers.
/// Persisted to SQLite as a JSON blob with indexed metadata columns.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkflowDefinition {
    pub id: Uuid,
    pub name: String,
    /// Original natural language intent from the user.
    pub description: String,
    /// Owning GitLab project.
    pub project_id: i64,
    pub steps: Vec<WorkflowStep>,
    pub triggers: Vec<Trigger>,
    pub created_by: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub enabled: bool,
    #[serde(default)]
    pub mode: WorkflowMode,
}

/// A single step in the workflow DAG.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkflowStep {
    /// Unique within the workflow (e.g., "fetch-mrs").
    pub id: String,
    /// What to do — interpreted by the agent.
    pub action: String,
    pub agent_type: AgentType,
    /// Static values or references to prior step outputs.
    pub inputs: HashMap<String, StepInput>,
    /// How to verify this step succeeded.
    pub success_criteria: String,
    /// Step IDs that must complete before this one runs.
    pub depends_on: Vec<String>,
    pub retry_policy: RetryPolicy,
    /// Step timeout in seconds.
    pub timeout_secs: u64,
}

/// Where a step gets its input value.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum StepInput {
    /// A literal JSON value.
    Static { value: serde_json::Value },
    /// Output from a previously completed step.
    StepOutput { step_id: String, field: String },
    /// Query the Mentor knowledge layer.
    MentorQuery { question: String },
}

/// Retry configuration for a step.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RetryPolicy {
    /// Maximum number of retries (default 2).
    pub max_retries: u32,
    pub backoff: BackoffStrategy,
    /// Ask Mentor for alternative strategies on failure.
    pub consult_mentor_on_failure: bool,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 2,
            backoff: BackoffStrategy::Fixed { delay_secs: 5 },
            consult_mentor_on_failure: false,
        }
    }
}

/// Backoff strategy between retries.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum BackoffStrategy {
    Fixed { delay_secs: u64 },
    Exponential { base_secs: u64, max_secs: u64 },
}

// ---------------------------------------------------------------------------
// Agent types
// ---------------------------------------------------------------------------

/// The kind of agent that executes a workflow step.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum AgentType {
    /// GitLab API operations (list MRs, post comments, manage branches).
    Gitlab,
    /// AI analysis, summarization, decision-making, code generation.
    Ai,
    /// Run scripts/tests in isolated Docker containers.
    Sandbox,
    /// Call external APIs (Slack, Jira, custom webhooks).
    Http,
    /// Run shell commands on the host with resource limits.
    Script,
    /// A mini-workflow — delegates to other agents.
    Composite,
    /// Multi-turn AI coding in a Docker sandbox (clone, fix, test, commit, push).
    Coding,
}

impl AgentType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Gitlab => "gitlab",
            Self::Ai => "ai",
            Self::Sandbox => "sandbox",
            Self::Http => "http",
            Self::Script => "script",
            Self::Composite => "composite",
            Self::Coding => "coding",
        }
    }
}

impl std::fmt::Display for AgentType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

// ---------------------------------------------------------------------------
// Triggers
// ---------------------------------------------------------------------------

/// What can kick off a workflow run.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum Trigger {
    /// Cron schedule (e.g., "0 9 * * 1-5").
    Cron { schedule: String },
    /// GitLab event (e.g., "mr.opened") with optional filter expression.
    Event {
        event_type: String,
        filter: Option<String>,
    },
    /// User-initiated via API or WebSocket.
    Manual,
}

/// What actually triggered a specific run (runtime, not definition).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum TriggerSource {
    Cron { fired_at: DateTime<Utc> },
    Event { event_type: String, payload: serde_json::Value },
    Manual { user: String },
}

// ---------------------------------------------------------------------------
// Workflow Run — runtime state
// ---------------------------------------------------------------------------

/// A single execution of a workflow definition.
/// State is checkpointed to SQLite after each step transition.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkflowRun {
    pub id: Uuid,
    pub workflow_id: Uuid,
    pub trigger: TriggerSource,
    pub status: RunStatus,
    pub step_states: HashMap<String, StepState>,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub final_verification: Option<VerificationResult>,
    pub mentor_queries: Vec<MentorInteraction>,
}

/// Overall status of a workflow run.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum RunStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl RunStatus {
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }
}

impl std::fmt::Display for RunStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        };
        f.write_str(s)
    }
}

/// State of a single step within a run.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "state")]
pub enum StepState {
    Pending,
    Running {
        agent_id: Uuid,
        started_at: DateTime<Utc>,
    },
    Completed {
        output: serde_json::Value,
        duration_secs: f64,
    },
    Failed {
        error: String,
        retries: u32,
        duration_secs: f64,
    },
    Skipped {
        reason: String,
    },
}

impl StepState {
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed { .. } | Self::Failed { .. } | Self::Skipped { .. })
    }

    pub fn is_success(&self) -> bool {
        matches!(self, Self::Completed { .. })
    }
}

// ---------------------------------------------------------------------------
// Agent results
// ---------------------------------------------------------------------------

/// Result returned by a workflow agent after executing a step.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentResult {
    pub status: AgentStatus,
    /// Structured output for downstream steps.
    pub output: serde_json::Value,
    pub duration_secs: f64,
    /// Things to feed back to the Mentor knowledge layer.
    pub learnings: Vec<MentorEntry>,
}

/// Outcome of an agent execution.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum AgentStatus {
    Success,
    Failure,
    /// Step produced usable but incomplete results.
    Partial,
}

// ---------------------------------------------------------------------------
// Verification & Mentor interaction
// ---------------------------------------------------------------------------

/// Result of the final verification step that checks whether the workflow
/// accomplished its original intent.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VerificationResult {
    pub passed: bool,
    pub summary: String,
    /// Per-deliverable status if the workflow had multiple goals.
    pub deliverables: Vec<DeliverableStatus>,
}

/// Status of a single deliverable within a verification result.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeliverableStatus {
    pub description: String,
    pub met: bool,
    pub evidence: String,
}

/// A knowledge entry to be stored in the Mentor layer.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MentorEntry {
    pub content: String,
    pub scope: String,
    pub category: MentorCategory,
}

/// Category of Mentor knowledge.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum MentorCategory {
    Execution,
    Domain,
    Workflow,
    Correction,
}

impl std::fmt::Display for MentorCategory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Execution => "execution",
            Self::Domain => "domain",
            Self::Workflow => "workflow",
            Self::Correction => "correction",
        };
        f.write_str(s)
    }
}

/// Record of a Mentor query made during a workflow run.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MentorInteraction {
    pub step_id: String,
    pub question: String,
    pub results_count: usize,
    pub queried_at: DateTime<Utc>,
}

// ---------------------------------------------------------------------------
// Adaptation events — logged when the orchestrator deviates from the plan
// ---------------------------------------------------------------------------

/// An adaptation the orchestrator made during execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AdaptationEvent {
    pub timestamp: DateTime<Utc>,
    pub kind: AdaptationKind,
    pub reason: String,
    /// The step that triggered the adaptation (if any).
    pub trigger_step_id: Option<String>,
}

/// What kind of adaptation was made.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum AdaptationKind {
    /// A new step was inserted into the DAG.
    StepInserted { step_id: String },
    /// A step was removed from the DAG.
    StepRemoved { step_id: String },
    /// A step's inputs or parameters were modified.
    StepModified { step_id: String },
    /// Dependent steps were skipped due to upstream failure.
    DependentsSkipped { step_ids: Vec<String> },
    /// Escalated to user for manual intervention.
    Escalated { message: String },
}

// ---------------------------------------------------------------------------
// Session-based orchestrator types (v2)
// ---------------------------------------------------------------------------

/// Lifecycle status of an autonomous session.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SessionStatus {
    Created,
    Planning,
    Executing,
    Evaluating,
    Adapting,
    WaitingForHuman,
    Completed,
    Failed,
    Cancelled,
}

impl SessionStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Planning => "planning",
            Self::Executing => "executing",
            Self::Evaluating => "evaluating",
            Self::Adapting => "adapting",
            Self::WaitingForHuman => "waiting_for_human",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }
}

impl std::fmt::Display for SessionStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Full session state persisted to SQLite.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionState {
    pub id: Uuid,
    pub workflow_id: Uuid,
    pub status: SessionStatus,
    pub trigger_type: String,
    pub trigger_data: Option<serde_json::Value>,
    pub plan: Option<SessionPlan>,
    pub step_outputs: HashMap<String, serde_json::Value>,
    pub current_step_id: Option<String>,
    pub retry_count: u32,
    pub max_retries: u32,
    /// Separate retry counter for step-level failures. Resets when moving to a new step.
    /// `retry_count` is reserved for session-level (evaluation) retries.
    #[serde(default)]
    pub step_retry_count: u32,
    pub evaluator_feedback: Option<EvaluatorVerdict>,
    pub escalation: Option<EscalationMessage>,
    /// Pending plan modification from a Generator PlanModification outcome.
    /// Applied directly in run_replanning instead of calling the AI replanner.
    #[serde(default)]
    pub pending_modification: Option<PendingPlanModification>,
    pub started_at: i64,
    pub completed_at: Option<i64>,
    pub updated_at: i64,
}

/// A plan produced by the Planner for a session.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionPlan {
    pub goal: String,
    pub steps: Vec<PlanStep>,
    pub capabilities_needed: Vec<String>,
}

/// A single step within a session plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PlanStep {
    pub id: String,
    pub description: String,
    pub agent_type: AgentType,
    pub success_criteria: String,
    pub depends_on: Vec<String>,
    pub capabilities_needed: Vec<String>,
}

/// A pending plan modification from the Generator, to be applied directly
/// during replanning instead of calling the AI replanner.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PendingPlanModification {
    pub add_steps: Vec<PlanStep>,
    pub remove_step_ids: Vec<String>,
    pub reason: String,
}

/// A message sent to the human when the session needs intervention.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EscalationMessage {
    pub session_id: Uuid,
    pub workflow_name: String,
    pub step_id: Option<String>,
    pub severity: EscalationSeverity,
    pub reason: String,
    pub what_i_need: String,
    pub options: Vec<EscalationOption>,
    pub created_at: i64,
}

/// How urgent an escalation is.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EscalationSeverity {
    Blocking,
    Warning,
    Info,
}

/// One option the human can choose when responding to an escalation.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EscalationOption {
    pub id: String,
    pub label: String,
    pub description: Option<String>,
}

/// Result of the Evaluator checking a step or session outcome.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EvaluatorVerdict {
    pub passed: bool,
    pub score: f64,
    pub threshold: f64,
    pub feedback: String,
    pub suggestion: Option<String>,
}

/// Outcome produced by the Generator after executing a plan step.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum GeneratorOutcome {
    Success {
        output: serde_json::Value,
        files_changed: Vec<String>,
    },
    Failure {
        error: String,
    },
    NeedsCapability {
        capability: String,
        description: String,
    },
    NeedsHuman {
        reason: String,
        what_i_need: String,
        options: Vec<EscalationOption>,
    },
    PlanModification {
        output: serde_json::Value,
        add_steps: Vec<PlanStep>,
        remove_step_ids: Vec<String>,
        reason: String,
    },
}

/// A message exchanged between the agent and a human during a session.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionMessage {
    pub id: i64,
    pub session_id: Uuid,
    pub direction: MessageDirection,
    pub content: String,
    pub metadata: Option<serde_json::Value>,
    pub created_at: i64,
}

/// Direction of a session message.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MessageDirection {
    AgentToHuman,
    HumanToAgent,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_status_terminal() {
        assert!(!RunStatus::Pending.is_terminal());
        assert!(!RunStatus::Running.is_terminal());
        assert!(RunStatus::Completed.is_terminal());
        assert!(RunStatus::Failed.is_terminal());
        assert!(RunStatus::Cancelled.is_terminal());
    }

    #[test]
    fn step_state_terminal_and_success() {
        assert!(!StepState::Pending.is_terminal());
        assert!(!StepState::Running {
            agent_id: Uuid::new_v4(),
            started_at: Utc::now(),
        }.is_terminal());

        let completed = StepState::Completed {
            output: serde_json::json!({"ok": true}),
            duration_secs: 1.5,
        };
        assert!(completed.is_terminal());
        assert!(completed.is_success());

        let failed = StepState::Failed {
            error: "boom".into(),
            retries: 2,
            duration_secs: 3.0,
        };
        assert!(failed.is_terminal());
        assert!(!failed.is_success());

        let skipped = StepState::Skipped {
            reason: "upstream failed".into(),
        };
        assert!(skipped.is_terminal());
        assert!(!skipped.is_success());
    }

    #[test]
    fn agent_type_display() {
        assert_eq!(AgentType::Gitlab.to_string(), "gitlab");
        assert_eq!(AgentType::Ai.to_string(), "ai");
        assert_eq!(AgentType::Sandbox.to_string(), "sandbox");
        assert_eq!(AgentType::Http.to_string(), "http");
        assert_eq!(AgentType::Script.to_string(), "script");
        assert_eq!(AgentType::Composite.to_string(), "composite");
        assert_eq!(AgentType::Coding.to_string(), "coding");
    }

    #[test]
    fn retry_policy_default() {
        let policy = RetryPolicy::default();
        assert_eq!(policy.max_retries, 2);
        assert!(!policy.consult_mentor_on_failure);
    }

    #[test]
    fn step_input_roundtrip() {
        let input = StepInput::StepOutput {
            step_id: "fetch-mrs".into(),
            field: "mr_list".into(),
        };
        let json = serde_json::to_string(&input).unwrap();
        let back: StepInput = serde_json::from_str(&json).unwrap();
        match back {
            StepInput::StepOutput { step_id, field } => {
                assert_eq!(step_id, "fetch-mrs");
                assert_eq!(field, "mr_list");
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn trigger_roundtrip() {
        let trigger = Trigger::Cron {
            schedule: "0 9 * * 1-5".into(),
        };
        let json = serde_json::to_string(&trigger).unwrap();
        assert!(json.contains("cron"));
        let back: Trigger = serde_json::from_str(&json).unwrap();
        match back {
            Trigger::Cron { schedule } => assert_eq!(schedule, "0 9 * * 1-5"),
            _ => panic!("wrong variant"),
        }
    }
}
