// ---------------------------------------------------------------------------
// GitLab workflow agent — wraps the existing GitlabClient for workflow steps.
//
// Supported actions:
//   - list_open_mrs: list open MRs for a project
//   - fetch_mr: fetch MR metadata
//   - fetch_mr_changes: fetch MR diffs
//   - post_comment: post a note on an MR
//   - fetch_pipelines: list pipelines for an MR
//   - fetch_file: fetch raw file content from a ref
//
// Inputs are JSON objects with action-specific keys. Unknown actions fail
// with a clear error message.
// ---------------------------------------------------------------------------

use serde_json::Value;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::time::Instant;
use tracing::{debug, warn};

use crate::services::gitlab::client::{self as gl, GitLabConfig};
use crate::services::mentor::client::MentorClient;
use crate::services::workflow::traits::{failure_result, success_result, WorkflowAgent};
use crate::types::workflow::AgentResult;

/// GitLab workflow agent — delegates to the existing GitLab REST client.
pub struct GitLabAgent {
    config: GitLabConfig,
}

impl GitLabAgent {
    pub fn new(config: GitLabConfig) -> Self {
        Self { config }
    }
}

impl WorkflowAgent for GitLabAgent {
    fn execute<'a>(
        &'a self,
        action: &str,
        inputs: HashMap<String, Value>,
        _mentor: &'a MentorClient,
    ) -> Pin<Box<dyn Future<Output = AgentResult> + Send + 'a>> {
        let action = action.to_string();
        Box::pin(async move {
            let start = Instant::now();
            debug!(action = action.as_str(), "gitlab agent: executing");

            let result = match action.as_str() {
                "list_open_mrs" => self.list_open_mrs(&inputs).await,
                "fetch_mr" => self.fetch_mr(&inputs).await,
                "fetch_mr_changes" => self.fetch_mr_changes(&inputs).await,
                "post_comment" => self.post_comment(&inputs).await,
                "fetch_pipelines" => self.fetch_pipelines(&inputs).await,
                "fetch_file" => self.fetch_file(&inputs).await,
                other => Err(format!("unknown gitlab action: {other}")),
            };

            let duration = start.elapsed().as_secs_f64();
            match result {
                Ok(output) => success_result(output, duration),
                Err(e) => {
                    warn!(action = action.as_str(), error = %e, "gitlab agent: action failed");
                    failure_result(&e, duration)
                }
            }
        })
    }

    fn agent_type_name(&self) -> &'static str {
        "gitlab"
    }
}

impl GitLabAgent {
    async fn list_open_mrs(&self, inputs: &HashMap<String, Value>) -> Result<Value, String> {
        let project_id = get_i64(inputs, "project_id")?;
        let mrs = gl::fetch_open_mrs(&self.config, project_id)
            .await
            .map_err(|e| format!("list_open_mrs: {e}"))?;
        serde_json::to_value(&mrs).map_err(|e| format!("serialize: {e}"))
    }

    async fn fetch_mr(&self, inputs: &HashMap<String, Value>) -> Result<Value, String> {
        let project_id = get_i64(inputs, "project_id")?;
        let mr_iid = get_u64(inputs, "mr_iid")?;
        let mr = gl::fetch_merge_request(&self.config, project_id, mr_iid)
            .await
            .map_err(|e| format!("fetch_mr: {e}"))?;
        serde_json::to_value(&mr).map_err(|e| format!("serialize: {e}"))
    }

    async fn fetch_mr_changes(&self, inputs: &HashMap<String, Value>) -> Result<Value, String> {
        let project_id = get_i64(inputs, "project_id")?;
        let mr_iid = get_u64(inputs, "mr_iid")?;
        let changes = gl::fetch_mr_changes(&self.config, project_id, mr_iid)
            .await
            .map_err(|e| format!("fetch_mr_changes: {e}"))?;
        serde_json::to_value(&changes).map_err(|e| format!("serialize: {e}"))
    }

    async fn post_comment(&self, inputs: &HashMap<String, Value>) -> Result<Value, String> {
        let project_id = get_i64(inputs, "project_id")?;
        let mr_iid = get_u64(inputs, "mr_iid")?;
        let body = get_str(inputs, "body")?;
        let note = gl::post_mr_note(&self.config, project_id, mr_iid, &body)
            .await
            .map_err(|e| format!("post_comment: {e}"))?;
        serde_json::to_value(&note).map_err(|e| format!("serialize: {e}"))
    }

    async fn fetch_pipelines(&self, inputs: &HashMap<String, Value>) -> Result<Value, String> {
        let project_id = get_i64(inputs, "project_id")?;
        let mr_iid = get_u64(inputs, "mr_iid")?;
        // Use the MR metadata which includes pipeline info via head_pipeline.
        let mr = gl::fetch_merge_request(&self.config, project_id, mr_iid)
            .await
            .map_err(|e| format!("fetch_pipelines: {e}"))?;
        serde_json::to_value(&mr).map_err(|e| format!("serialize: {e}"))
    }

    async fn fetch_file(&self, inputs: &HashMap<String, Value>) -> Result<Value, String> {
        let project_id = get_i64(inputs, "project_id")?;
        let file_path = get_str(inputs, "file_path")?;
        let ref_name = get_str(inputs, "ref").unwrap_or_else(|_| "main".to_string());
        let content = gl::fetch_file_content(&self.config, project_id, &file_path, &ref_name)
            .await
            .map_err(|e| format!("fetch_file: {e}"))?;
        Ok(serde_json::json!({ "content": content, "file_path": file_path }))
    }
}

// ---------------------------------------------------------------------------
// Input extraction helpers
// ---------------------------------------------------------------------------

fn get_str(inputs: &HashMap<String, Value>, key: &str) -> Result<String, String> {
    inputs
        .get(key)
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| format!("missing or invalid input: {key} (expected string)"))
}

fn get_i64(inputs: &HashMap<String, Value>, key: &str) -> Result<i64, String> {
    inputs
        .get(key)
        .and_then(|v| v.as_i64())
        .ok_or_else(|| format!("missing or invalid input: {key} (expected integer)"))
}

fn get_u64(inputs: &HashMap<String, Value>, key: &str) -> Result<u64, String> {
    inputs
        .get(key)
        .and_then(|v| v.as_u64())
        .ok_or_else(|| format!("missing or invalid input: {key} (expected unsigned integer)"))
}
