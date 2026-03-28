// ---------------------------------------------------------------------------
// NL Decomposer — natural language to workflow DAG decomposition.
//
// Takes a user's natural language description of a workflow and uses the AI
// service to decompose it into a structured DAG of WorkflowSteps with
// triggers, dependencies, and success criteria.
//
// The decomposer is called during workflow creation (user describes what they
// want → AI produces the step DAG → user refines → system persists).
// ---------------------------------------------------------------------------

use chrono::Utc;
use serde::Deserialize;
use serde_json::Value;
use tracing::{debug, warn};
use uuid::Uuid;

use crate::services::ai::client::{
    self as ai, AiClientConfig, ChatCompletionRequest, ChatMessage,
};
use crate::types::workflow::{
    AgentType, RetryPolicy, Trigger, WorkflowDefinition, WorkflowMode, WorkflowStep,
};

// ---------------------------------------------------------------------------
// Decomposer
// ---------------------------------------------------------------------------

/// The NL decomposer — converts natural language into workflow DAGs.
pub struct NlDecomposer {
    ai_config: AiClientConfig,
    model: String,
}

impl NlDecomposer {
    pub fn new(ai_config: AiClientConfig, model: String) -> Self {
        Self { ai_config, model }
    }

    /// Decompose a natural language description into a WorkflowDefinition.
    ///
    /// - `description`: the user's natural language intent
    /// - `project_id`: owning GitLab project
    /// - `created_by`: user who created the workflow
    pub async fn decompose(
        &self,
        description: &str,
        project_id: i64,
        created_by: &str,
    ) -> Result<WorkflowDefinition, DecomposeError> {
        debug!(description, "decomposer: parsing intent");

        let request = ChatCompletionRequest {
            model: self.model.clone(),
            messages: vec![
                ChatMessage {
                    role: "system".into(),
                    content: Some(DECOMPOSE_SYSTEM_PROMPT.to_string()),
                    tool_calls: None,
                    tool_call_id: None,
                },
                ChatMessage {
                    role: "user".into(),
                    content: Some(description.to_string()),
                    tool_calls: None,
                    tool_call_id: None,
                },
            ],
            temperature: Some(0.1),
            max_tokens: Some(4096),
            stream: None,
            tools: None,
            tool_choice: None,
        };

        let resp = ai::chat_completion(&self.ai_config, request)
            .await
            .map_err(|e| DecomposeError::AiError(e.to_string()))?;

        let raw = resp
            .choices
            .first()
            .and_then(|c| c.message.content.clone())
            .ok_or_else(|| DecomposeError::EmptyResponse)?;

        debug!(raw_len = raw.len(), "decomposer: got AI response");

        // Extract JSON from the response (may be wrapped in markdown code fences).
        let json_str = extract_json(&raw)?;

        let parsed: DecomposedWorkflow = serde_json::from_str(&json_str).map_err(|e| {
            warn!(error = %e, json = %json_str, "decomposer: failed to parse AI output");
            DecomposeError::ParseError(e.to_string())
        })?;

        // Convert to WorkflowDefinition.
        let now = Utc::now();
        let steps: Vec<WorkflowStep> = parsed
            .steps
            .into_iter()
            .map(|s| WorkflowStep {
                id: s.id,
                action: s.action,
                agent_type: parse_agent_type(&s.agent_type),
                inputs: s
                    .inputs
                    .into_iter()
                    .map(|(k, v)| {
                        (
                            k,
                            crate::types::workflow::StepInput::Static { value: v },
                        )
                    })
                    .collect(),
                success_criteria: s.success_criteria,
                depends_on: s.depends_on,
                retry_policy: RetryPolicy::default(),
                timeout_secs: s.timeout_secs.unwrap_or(300),
            })
            .collect();

        // Fix #10: validate depends_on references — remove any that point to
        // non-existent step IDs and log a warning.
        let valid_ids: std::collections::HashSet<String> =
            steps.iter().map(|s: &WorkflowStep| s.id.clone()).collect();
        let mut steps = steps;
        for step in &mut steps {
            let before_len = step.depends_on.len();
            step.depends_on.retain(|dep_id| valid_ids.contains(dep_id));
            let removed = before_len - step.depends_on.len();
            if removed > 0 {
                warn!(
                    step_id = %step.id,
                    removed,
                    "decomposer: removed invalid depends_on references"
                );
            }
        }

        let triggers = parsed
            .triggers
            .into_iter()
            .map(|t| match t.trigger_type.as_str() {
                "cron" => Trigger::Cron {
                    schedule: t.value.unwrap_or_default(),
                },
                "event" => Trigger::Event {
                    event_type: t.value.unwrap_or_default(),
                    filter: t.filter,
                },
                _ => Trigger::Manual,
            })
            .collect();

        Ok(WorkflowDefinition {
            id: Uuid::new_v4(),
            name: parsed.name,
            description: description.to_string(),
            project_id,
            steps,
            triggers,
            created_by: created_by.to_string(),
            created_at: now,
            updated_at: now,
            enabled: true,
            mode: WorkflowMode::default(),
        })
    }
}

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum DecomposeError {
    #[error("AI service error: {0}")]
    AiError(String),
    #[error("empty response from AI")]
    EmptyResponse,
    #[error("failed to parse decomposition: {0}")]
    ParseError(String),
    #[error("no JSON found in AI response")]
    NoJson,
}

// ---------------------------------------------------------------------------
// AI response schema — intermediate format before conversion
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct DecomposedWorkflow {
    name: String,
    steps: Vec<DecomposedStep>,
    #[serde(default)]
    triggers: Vec<DecomposedTrigger>,
}

#[derive(Debug, Deserialize)]
struct DecomposedStep {
    id: String,
    action: String,
    agent_type: String,
    #[serde(default)]
    inputs: std::collections::HashMap<String, Value>,
    #[serde(default = "default_success_criteria")]
    success_criteria: String,
    #[serde(default)]
    depends_on: Vec<String>,
    timeout_secs: Option<u64>,
}

fn default_success_criteria() -> String {
    "step completes without error".to_string()
}

#[derive(Debug, Deserialize)]
struct DecomposedTrigger {
    #[serde(rename = "type")]
    trigger_type: String,
    value: Option<String>,
    filter: Option<String>,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn parse_agent_type(s: &str) -> AgentType {
    match s.to_lowercase().as_str() {
        "gitlab" => AgentType::Gitlab,
        "ai" => AgentType::Ai,
        "sandbox" => AgentType::Sandbox,
        "http" => AgentType::Http,
        "script" => AgentType::Script,
        "composite" => AgentType::Composite,
        _ => AgentType::Ai, // default to AI for unknown types
    }
}

/// Extract JSON from a string that may be wrapped in markdown code fences.
fn extract_json(raw: &str) -> Result<String, DecomposeError> {
    let trimmed = raw.trim();

    // Try direct parse first.
    if trimmed.starts_with('{') {
        return Ok(trimmed.to_string());
    }

    // Look for ```json ... ``` fences.
    if let Some(start) = trimmed.find("```json") {
        let after_fence = &trimmed[start + 7..];
        if let Some(end) = after_fence.find("```") {
            return Ok(after_fence[..end].trim().to_string());
        }
    }

    // Look for ``` ... ``` fences (no language tag).
    if let Some(start) = trimmed.find("```") {
        let after_fence = &trimmed[start + 3..];
        if let Some(end) = after_fence.find("```") {
            let candidate = after_fence[..end].trim();
            if candidate.starts_with('{') {
                return Ok(candidate.to_string());
            }
        }
    }

    // Look for first { to last } — last resort, may grab garbage.
    if let (Some(start), Some(end)) = (trimmed.find('{'), trimmed.rfind('}')) {
        if start < end {
            let candidate = &trimmed[start..=end];
            // Fix #9: validate that the extracted text is actually valid JSON
            // before returning it. If it's not, fall through to the error.
            if serde_json::from_str::<serde_json::Value>(candidate).is_ok() {
                return Ok(candidate.to_string());
            }
            warn!("decomposer: last-resort brace extraction produced invalid JSON, rejecting");
        }
    }

    Err(DecomposeError::NoJson)
}

// ---------------------------------------------------------------------------
// System prompt
// ---------------------------------------------------------------------------

const DECOMPOSE_SYSTEM_PROMPT: &str = r#"You are a workflow decomposition engine. Given a natural language description of a workflow, you produce a structured JSON DAG.

Output ONLY valid JSON with this schema:
{
  "name": "short-kebab-case-name",
  "steps": [
    {
      "id": "unique-step-id",
      "action": "what to do (interpreted by the agent)",
      "agent_type": "gitlab|ai|sandbox|http|script|composite",
      "inputs": { "key": "value" },
      "success_criteria": "how to verify success",
      "depends_on": ["step-ids-that-must-complete-first"],
      "timeout_secs": 300
    }
  ],
  "triggers": [
    { "type": "cron|event|manual", "value": "schedule or event type", "filter": "optional filter" }
  ]
}

Rules:
- Steps form a DAG: depends_on references must point to earlier steps.
- Use the most specific agent_type: "gitlab" for GitLab API ops, "ai" for analysis/decisions, "sandbox" for code execution, "http" for external APIs, "script" for shell commands.
- Each step should be atomic — one clear action.
- Include meaningful success_criteria for each step.
- If the user mentions a schedule, add a cron trigger. If they mention an event (MR opened, push, etc.), add an event trigger. Otherwise default to manual.
- Keep step IDs short and descriptive (kebab-case).
- Do NOT include any text outside the JSON object."#;

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_json_direct() {
        let input = r#"{"name": "test", "steps": []}"#;
        let result = extract_json(input).unwrap();
        assert_eq!(result, input);
    }

    #[test]
    fn extract_json_fenced() {
        let input = "Here's the workflow:\n```json\n{\"name\": \"test\", \"steps\": []}\n```\nDone.";
        let result = extract_json(input).unwrap();
        assert_eq!(result, r#"{"name": "test", "steps": []}"#);
    }

    #[test]
    fn extract_json_embedded() {
        let input = "The workflow is {\"name\": \"test\", \"steps\": []} and that's it.";
        let result = extract_json(input).unwrap();
        assert_eq!(result, r#"{"name": "test", "steps": []}"#);
    }

    #[test]
    fn extract_json_no_json() {
        let input = "No JSON here at all.";
        assert!(extract_json(input).is_err());
    }

    #[test]
    fn extract_json_garbage_braces_rejected() {
        // Fix #9: first-{ to last-} extraction should reject invalid JSON.
        let input = "Here is some prose { with braces } and more text { not json } end.";
        assert!(extract_json(input).is_err());
    }

    #[test]
    fn extract_json_valid_embedded_json() {
        // Valid JSON embedded in prose should still work.
        let input = r#"Result: {"name": "test", "steps": []} done."#;
        let result = extract_json(input).unwrap();
        assert_eq!(result, r#"{"name": "test", "steps": []}"#);
    }

    #[test]
    fn parse_agent_types() {
        assert_eq!(parse_agent_type("gitlab"), AgentType::Gitlab);
        assert_eq!(parse_agent_type("AI"), AgentType::Ai);
        assert_eq!(parse_agent_type("Sandbox"), AgentType::Sandbox);
        assert_eq!(parse_agent_type("HTTP"), AgentType::Http);
        assert_eq!(parse_agent_type("script"), AgentType::Script);
        assert_eq!(parse_agent_type("composite"), AgentType::Composite);
        assert_eq!(parse_agent_type("unknown"), AgentType::Ai);
    }

    #[test]
    fn parse_decomposed_workflow() {
        let json = r#"{
            "name": "stale-mr-pinger",
            "steps": [
                {
                    "id": "fetch-mrs",
                    "action": "list open MRs older than 3 days",
                    "agent_type": "gitlab",
                    "inputs": {"state": "opened"},
                    "success_criteria": "returns a list of MRs",
                    "depends_on": []
                },
                {
                    "id": "filter-stale",
                    "action": "filter MRs to those older than 3 days",
                    "agent_type": "ai",
                    "inputs": {},
                    "success_criteria": "returns filtered list",
                    "depends_on": ["fetch-mrs"]
                }
            ],
            "triggers": [
                {"type": "cron", "value": "0 9 * * 1-5"}
            ]
        }"#;

        let parsed: DecomposedWorkflow = serde_json::from_str(json).unwrap();
        assert_eq!(parsed.name, "stale-mr-pinger");
        assert_eq!(parsed.steps.len(), 2);
        assert_eq!(parsed.steps[1].depends_on, vec!["fetch-mrs"]);
        assert_eq!(parsed.triggers.len(), 1);
        assert_eq!(parsed.triggers[0].trigger_type, "cron");
    }
}
