// ---------------------------------------------------------------------------
// Composite workflow agent — a mini-workflow that delegates to other agents.
//
// A composite agent is just a workflow definition referenced as a step in
// another workflow. It spawns a nested orchestrator to execute the sub-flow.
//
// Supported actions:
//   - run: execute a sub-workflow by ID
//   - inline: execute an inline step list (passed as inputs)
// ---------------------------------------------------------------------------

use serde_json::Value;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::time::Instant;
use tracing::{debug, warn};

use crate::services::mentor::client::MentorClient;
use crate::services::workflow::crud;
use crate::services::workflow::factory::AgentFactoryConfig;
use crate::services::workflow::orchestrator::Orchestrator;
use crate::services::workflow::traits::{failure_result, success_result, WorkflowAgent};
use crate::types::workflow::{AgentResult, TriggerSource};
use sqlx::SqlitePool;

/// Maximum composite nesting depth. Prevents runaway recursion when
/// composite workflows reference each other.
const MAX_COMPOSITE_DEPTH: u32 = 5;

/// Composite workflow agent — nested orchestrator for sub-workflows.
pub struct CompositeAgent {
    pool: SqlitePool,
    agent_config: AgentFactoryConfig,
    default_step_timeout_secs: u64,
    /// Current nesting depth. Incremented each time a composite spawns another.
    depth: u32,
}

impl CompositeAgent {
    pub fn new(
        pool: SqlitePool,
        agent_config: AgentFactoryConfig,
        default_step_timeout_secs: u64,
        depth: u32,
    ) -> Self {
        Self {
            pool,
            agent_config,
            default_step_timeout_secs,
            depth,
        }
    }
}

impl WorkflowAgent for CompositeAgent {
    fn execute<'a>(
        &'a self,
        action: &str,
        inputs: HashMap<String, Value>,
        mentor: &'a MentorClient,
    ) -> Pin<Box<dyn Future<Output = AgentResult> + Send + 'a>> {
        let action = action.to_string();
        Box::pin(async move {
            let start = Instant::now();

            // Enforce recursion depth limit.
            if self.depth >= MAX_COMPOSITE_DEPTH {
                warn!(
                    depth = self.depth,
                    max = MAX_COMPOSITE_DEPTH,
                    "composite agent: max nesting depth exceeded"
                );
                return failure_result(
                    &format!(
                        "composite nesting depth {} exceeds maximum of {}",
                        self.depth, MAX_COMPOSITE_DEPTH
                    ),
                    start.elapsed().as_secs_f64(),
                );
            }

            if self.depth > 0 {
                warn!(
                    depth = self.depth,
                    action = action.as_str(),
                    "composite agent: nested execution (depth > 0)"
                );
            }

            debug!(action = action.as_str(), depth = self.depth, "composite agent: executing");

            let result = match action.as_str() {
                "run" => self.run_sub_workflow(&inputs, mentor).await,
                other => Err(format!("unknown composite action: {other}")),
            };

            let duration = start.elapsed().as_secs_f64();
            match result {
                Ok(output) => success_result(output, duration),
                Err(e) => {
                    warn!(action = action.as_str(), error = %e, "composite agent: failed");
                    failure_result(&e, duration)
                }
            }
        })
    }

    fn agent_type_name(&self) -> &'static str {
        "composite"
    }
}

impl CompositeAgent {
    /// Run a sub-workflow by ID. The sub-workflow's outputs are collected
    /// and returned as the composite step's output.
    async fn run_sub_workflow(
        &self,
        inputs: &HashMap<String, Value>,
        mentor: &MentorClient,
    ) -> Result<Value, String> {
        let workflow_id = inputs
            .get("workflow_id")
            .and_then(|v| v.as_str())
            .ok_or("missing input: workflow_id")?;

        let definition = crud::get_workflow(&self.pool, workflow_id)
            .await
            .map_err(|e| format!("load sub-workflow: {e}"))?
            .ok_or_else(|| format!("sub-workflow not found: {workflow_id}"))?;

        let nested_mentor = MentorClient::new(self.pool.clone(), mentor.current_repo().to_string());
        let orchestrator = Orchestrator::new_nested(
            self.pool.clone(),
            nested_mentor,
            self.agent_config.clone(),
            self.default_step_timeout_secs,
            self.depth + 1,
        );

        let trigger = TriggerSource::Manual {
            user: "composite-agent".into(),
        };

        let run = orchestrator.execute(&definition, trigger).await;

        // Collect outputs from all completed steps.
        let mut outputs = serde_json::Map::new();
        for (step_id, state) in &run.step_states {
            if let crate::types::workflow::StepState::Completed { output, .. } = state {
                outputs.insert(step_id.clone(), output.clone());
            }
        }

        Ok(serde_json::json!({
            "run_id": run.id.to_string(),
            "status": run.status.to_string(),
            "step_outputs": Value::Object(outputs),
        }))
    }
}
