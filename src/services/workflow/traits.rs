// ---------------------------------------------------------------------------
// WorkflowAgent trait — the common interface all workflow agents implement.
//
// Agents are stateless: they receive inputs, do work, and return a result.
// The orchestrator manages lifecycle, retries, and state persistence.
// ---------------------------------------------------------------------------

use serde_json::Value;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;

use crate::services::mentor::client::MentorClient;
use crate::types::workflow::{AgentResult, AgentStatus, MentorEntry};

/// The core trait for workflow step execution.
///
/// Each agent type (gitlab, ai, sandbox, http, script, composite) provides
/// its own implementation. The orchestrator dispatches to the right agent
/// based on the step's `agent_type` field.
///
/// Uses `Pin<Box<dyn Future>>` for object safety — the orchestrator holds
/// agents as `Box<dyn WorkflowAgent>`.
pub trait WorkflowAgent: Send + Sync {
    /// Execute a step action with the given inputs.
    fn execute<'a>(
        &'a self,
        action: &str,
        inputs: HashMap<String, Value>,
        mentor: &'a MentorClient,
    ) -> Pin<Box<dyn Future<Output = AgentResult> + Send + 'a>>;

    /// Human-readable name for logging and diagnostics.
    fn agent_type_name(&self) -> &'static str;
}

/// Helper to build a successful AgentResult.
pub fn success_result(output: Value, duration_secs: f64) -> AgentResult {
    AgentResult {
        status: AgentStatus::Success,
        output,
        duration_secs,
        learnings: Vec::new(),
    }
}

/// Helper to build a failed AgentResult.
pub fn failure_result(error: &str, duration_secs: f64) -> AgentResult {
    AgentResult {
        status: AgentStatus::Failure,
        output: serde_json::json!({ "error": error }),
        duration_secs,
        learnings: Vec::new(),
    }
}

/// Helper to build a partial AgentResult.
pub fn partial_result(output: Value, duration_secs: f64) -> AgentResult {
    AgentResult {
        status: AgentStatus::Partial,
        output,
        duration_secs,
        learnings: Vec::new(),
    }
}

/// Extension trait for adding learnings to an AgentResult.
pub trait AgentResultExt {
    fn with_learnings(self, learnings: Vec<MentorEntry>) -> Self;
}

impl AgentResultExt for AgentResult {
    fn with_learnings(mut self, learnings: Vec<MentorEntry>) -> Self {
        self.learnings = learnings;
        self
    }
}
