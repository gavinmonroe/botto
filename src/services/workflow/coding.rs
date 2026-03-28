// ---------------------------------------------------------------------------
// Coding workflow agent — multi-turn AI coding via the SandboxManager.
//
// Bridges the workflow system to the full fix pipeline in SandboxManager.
// Takes task inputs (project, branch, description, optional code context),
// builds a FixRequest, runs the pipeline, and returns an AgentResult.
//
// Supported actions:
//   - fix: Run the full multi-turn fix pipeline (clone, understand, fix,
//          test, iterate, commit, push).
//
// Inputs:
//   - project_path (required): GitLab project path (e.g., "group/repo")
//   - branch (required): Source branch to work on
//   - task_description (required): What to fix or build
//   - file_path (optional): Specific file to focus on
//   - original_code (optional): Code snippet to fix
//   - suggestion (optional): Suggested fix or approach
//   - mr_iid (optional): MR IID for context (default 0)
//   - target_branch (optional): Target branch for the MR
//   - mr_title (optional): MR title for context
//   - mr_description (optional): MR description for context
// ---------------------------------------------------------------------------

use serde_json::Value;
use sqlx::SqlitePool;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, info, warn};

use crate::config::BottoConfig;
use crate::services::events::EventBus;
use crate::services::mentor::client::MentorClient;
use crate::services::sandbox::manager::{FixRequest, SandboxManager};
use crate::services::workflow::traits::{failure_result, success_result, WorkflowAgent};
use crate::types::workflow::AgentResult;

/// Coding workflow agent — wraps SandboxManager for multi-turn coding tasks.
pub struct CodingAgent {
    sandbox: Arc<SandboxManager>,
}

impl CodingAgent {
    /// Try to create a CodingAgent. Returns `None` if the sandbox is disabled
    /// in config or Docker is not available.
    pub fn try_new(
        cfg: BottoConfig,
        pool: SqlitePool,
        event_bus: EventBus,
    ) -> Option<Self> {
        if !cfg.sandbox.enabled {
            info!("coding agent: sandbox disabled in config, agent unavailable");
            return None;
        }

        // SandboxManager::new returns None if Docker isn't reachable.
        let noop_broadcaster: Arc<dyn Fn(&crate::types::state::MrRef, &str) + Send + Sync> =
            Arc::new(|_mr, _msg| {
                // Workflow-driven coding doesn't broadcast to Otto WebSocket clients.
                // Progress is tracked via the session/workflow event bus instead.
            });

        let sandbox = SandboxManager::new(cfg, pool, event_bus, noop_broadcaster, None)?;

        info!("coding agent: initialized with SandboxManager");
        Some(Self {
            sandbox: Arc::new(sandbox),
        })
    }
}

impl WorkflowAgent for CodingAgent {
    fn execute<'a>(
        &'a self,
        action: &str,
        inputs: HashMap<String, Value>,
        _mentor: &'a MentorClient,
    ) -> Pin<Box<dyn Future<Output = AgentResult> + Send + 'a>> {
        let action = action.to_string();
        Box::pin(async move {
            let start = Instant::now();
            info!(action = action.as_str(), "coding agent: starting execution");

            let result = match action.as_str() {
                "fix" => self.run_fix(&inputs).await,
                other => Err(format!("unknown coding action: {other}")),
            };

            let duration = start.elapsed().as_secs_f64();
            match result {
                Ok(output) => {
                    info!(
                        action = action.as_str(),
                        duration_secs = duration,
                        "coding agent: completed successfully"
                    );
                    success_result(output, duration)
                }
                Err(e) => {
                    warn!(
                        action = action.as_str(),
                        error = %e,
                        duration_secs = duration,
                        "coding agent: execution failed"
                    );
                    failure_result(&e, duration)
                }
            }
        })
    }

    fn agent_type_name(&self) -> &'static str {
        "coding"
    }
}

impl CodingAgent {
    /// Build a FixRequest from workflow inputs and run the full pipeline.
    async fn run_fix(&self, inputs: &HashMap<String, Value>) -> Result<Value, String> {
        let project_path = get_str(inputs, "project_path")?;
        let branch = get_str(inputs, "branch")?;
        let task_description = get_str(inputs, "task_description")?;

        let file_path = get_optional_str(inputs, "file_path");
        let original_code = get_optional_str(inputs, "original_code");
        let suggestion = get_optional_str(inputs, "suggestion");
        let mr_iid = inputs
            .get("mr_iid")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let target_branch = get_optional_str(inputs, "target_branch");
        let mr_title = get_optional_str(inputs, "mr_title");
        let mr_description = get_optional_str(inputs, "mr_description");

        let job_id = uuid::Uuid::new_v4().to_string();

        debug!(
            job_id = job_id.as_str(),
            project_path = project_path.as_str(),
            branch = branch.as_str(),
            "coding agent: building FixRequest"
        );

        let req = FixRequest {
            job_id: job_id.clone(),
            project_path,
            mr_iid,
            source_branch: branch,
            comment_id: job_id.clone(),
            file_path: file_path.unwrap_or_default(),
            original_code: original_code.unwrap_or_default(),
            suggestion: suggestion.unwrap_or_else(|| task_description.clone()),
            comment_body: Some(task_description),
            comment_title: None,
            severity: None,
            target_branch,
            start_line: None,
            end_line: None,
            file_content: None,
            mr_title,
            mr_description,
            file_diff: None,
            source_project_path: None,
        };

        debug!(job_id = req.job_id.as_str(), "coding agent: invoking SandboxManager::run_fix");

        let result = self.sandbox.run_fix(req).await;

        if result.success {
            let mut output = serde_json::json!({
                "success": true,
                "job_id": result.job_id,
            });
            if let Some(sha) = &result.commit_sha {
                output["commit_sha"] = serde_json::json!(sha);
            }
            if let Some(test_out) = &result.test_output {
                output["test_output"] = serde_json::json!(test_out);
            }
            if let Some(mr_url) = &result.fix_mr_url {
                output["fix_mr_url"] = serde_json::json!(mr_url);
            }
            Ok(output)
        } else {
            let error_msg = result
                .error
                .unwrap_or_else(|| "fix pipeline failed with no error message".into());
            Err(error_msg)
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn get_str(inputs: &HashMap<String, Value>, key: &str) -> Result<String, String> {
    inputs
        .get(key)
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| format!("missing or invalid input: {key} (expected string)"))
}

fn get_optional_str(inputs: &HashMap<String, Value>, key: &str) -> Option<String> {
    inputs.get(key).and_then(|v| v.as_str()).map(|s| s.to_string())
}
