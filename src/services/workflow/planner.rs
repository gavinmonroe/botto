// ---------------------------------------------------------------------------
// Planner agent — converts trigger context into a structured execution plan
// via AI. Invoked at session start and when replanning is needed.
// ---------------------------------------------------------------------------

use anyhow::{bail, Context, Result};
use std::collections::{HashMap, HashSet};
use tracing::{debug, info, warn};

use crate::services::ai::client::{
    self, AiClientConfig, ChatCompletionRequest, ChatMessage, ToolDefinition,
};
use crate::services::mentor::client::MentorClient;
use crate::types::workflow::{PlanStep, SessionPlan};

// ---------------------------------------------------------------------------
// PlanResult — the planner can produce a plan or request clarification
// ---------------------------------------------------------------------------

/// Result of the planning phase.
#[derive(Debug, Clone)]
pub enum PlanResult {
    /// A complete execution plan ready for the generator.
    Plan(SessionPlan),
    /// The planner needs more information from the user before it can plan.
    NeedsClarification {
        questions: Vec<String>,
        reason: String,
    },
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Create an execution plan from trigger context.
///
/// 1. Queries the mentor for relevant knowledge
/// 2. Builds a prompt with trigger data + mentor context + workflow description + tool catalog
/// 3. Calls the AI service with tools available for function calling
/// 4. If the AI calls the `clarify` tool → return PlanResult::NeedsClarification
/// 5. Otherwise parses and validates the response into a `SessionPlan`
pub async fn create_plan(
    ai_config: &AiClientConfig,
    model: &str,
    mentor: &MentorClient,
    trigger_data: &serde_json::Value,
    workflow_description: &str,
    tool_catalog: &[ToolDefinition],
) -> Result<PlanResult> {
    info!("creating execution plan for workflow");

    // 1. Query mentor for relevant context.
    let mentor_context = query_mentor_context(mentor, workflow_description).await;

    // 2. Build the prompt.
    let system_prompt = build_system_prompt(tool_catalog);
    let user_prompt = build_create_prompt(trigger_data, workflow_description, &mentor_context);

    // 3. Call AI — try with tools first, fall back to text-only if the endpoint
    //    doesn't support function calling (returns 400).
    let response = match call_ai_with_tools(ai_config, model, &system_prompt, &user_prompt, tool_catalog).await {
        Ok(resp) => resp,
        Err(e) => {
            warn!("planner AI call with tools failed ({e:#}), retrying without tools");
            let fallback_prompt = build_fallback_prompt(trigger_data, workflow_description, &mentor_context, tool_catalog);
            let text = call_ai_text_only(ai_config, model, &fallback_prompt).await?;
            PlannerAiResponse {
                text: Some(text),
                clarification: None,
            }
        }
    };

    // 4. Check if the AI called the clarify tool.
    if let Some(clarification) = response.clarification {
        info!(
            questions = clarification.questions.len(),
            reason = %clarification.reason,
            "planner requested clarification"
        );
        return Ok(PlanResult::NeedsClarification {
            questions: clarification.questions,
            reason: clarification.reason,
        });
    }

    // 5. Parse and validate the text response.
    let text = response.text.as_deref().unwrap_or("");

    // If the response is empty or doesn't look like JSON (AI ignored tools and
    // returned conversational text), retry without tools using a JSON-focused prompt.
    let plan_text = if text.is_empty() || (!text.contains('{') && !text.contains("goal")) {
        warn!("planner AI returned non-JSON response, retrying with text-only prompt");
        let fallback_prompt = build_fallback_prompt(trigger_data, workflow_description, &mentor_context, tool_catalog);
        let fallback = call_ai_text_only(ai_config, model, &fallback_prompt).await?;
        fallback
    } else {
        text.to_string()
    };

    if plan_text.is_empty() {
        bail!("planner AI returned empty response");
    }

    debug!(response_len = plan_text.len(), "planner response received");

    let plan = parse_plan(&plan_text).context("failed to parse planner response")?;
    let plan = validate_plan(plan)?;

    info!(
        goal = %plan.goal,
        step_count = plan.steps.len(),
        "execution plan created"
    );
    Ok(PlanResult::Plan(plan))
}

/// Replan after partial execution — keeps completed steps, replaces failed ones.
pub async fn replan(
    ai_config: &AiClientConfig,
    model: &str,
    mentor: &MentorClient,
    original_plan: &SessionPlan,
    completed_steps: &[&str],
    failed_steps: &[(&str, &str)], // (step_id, error_message)
    new_context: Option<&str>,
    tool_catalog: &[ToolDefinition],
) -> Result<SessionPlan> {
    info!(
        completed = completed_steps.len(),
        failed = failed_steps.len(),
        "replanning session"
    );

    let mentor_context = query_mentor_context(mentor, &original_plan.goal).await;

    let system_prompt = build_system_prompt(tool_catalog);
    let user_prompt = build_replan_prompt(
        original_plan,
        completed_steps,
        failed_steps,
        new_context,
        &mentor_context,
    );

    let response = call_ai(ai_config, model, &system_prompt, &user_prompt).await?;
    debug!(response_len = response.len(), "replanner AI response received");

    let plan = parse_plan(&response).context("failed to parse replanner response")?;
    let plan = validate_plan(plan)?;

    info!(
        goal = %plan.goal,
        step_count = plan.steps.len(),
        "revised execution plan created"
    );
    Ok(plan)
}

// ---------------------------------------------------------------------------
// Mentor integration
// ---------------------------------------------------------------------------

/// Best-effort mentor query — returns empty string on failure so planning
/// can proceed without knowledge context.
async fn query_mentor_context(mentor: &MentorClient, question: &str) -> String {
    match mentor.query(question, 5).await {
        Ok(results) if results.is_empty() => String::new(),
        Ok(results) => {
            let pieces: Vec<String> = results
                .iter()
                .map(|r| format!("- [{}] {}", r.category, r.content))
                .collect();
            pieces.join("\n")
        }
        Err(e) => {
            warn!("mentor query failed during planning, proceeding without context: {e}");
            String::new()
        }
    }
}

// ---------------------------------------------------------------------------
// AI interaction
// ---------------------------------------------------------------------------

/// Internal response from the AI call — either text content or a clarification request.
struct PlannerAiResponse {
    text: Option<String>,
    clarification: Option<ClarificationRequest>,
}

struct ClarificationRequest {
    questions: Vec<String>,
    reason: String,
}

/// Call AI with tool catalog for function calling. Checks if the model called
/// the `clarify` tool; otherwise returns the text response for plan parsing.
async fn call_ai_with_tools(
    cfg: &AiClientConfig,
    model: &str,
    system_prompt: &str,
    user_prompt: &str,
    tool_catalog: &[ToolDefinition],
) -> Result<PlannerAiResponse> {
    let tools = if tool_catalog.is_empty() {
        None
    } else {
        Some(tool_catalog.to_vec())
    };

    let request = ChatCompletionRequest {
        model: model.to_string(),
        messages: vec![
            ChatMessage {
                role: "system".into(),
                content: Some(system_prompt.to_string()),
                tool_calls: None,
                tool_call_id: None,
            },
            ChatMessage {
                role: "user".into(),
                content: Some(user_prompt.to_string()),
                tool_calls: None,
                tool_call_id: None,
            },
        ],
        temperature: Some(0.2),
        max_tokens: Some(4096),
        stream: None,
        tools,
        tool_choice: None,
    };

    let resp = client::chat_completion(cfg, request)
        .await
        .context("planner AI call failed")?;

    let choice = resp.choices.first().ok_or_else(|| {
        anyhow::anyhow!("planner AI returned no choices")
    })?;

    // Check for tool calls — specifically the clarify tool.
    if let Some(ref tool_calls) = choice.message.tool_calls {
        for tc in tool_calls {
            if tc.function.name == "clarify" {
                // Parse the clarify tool arguments.
                let args: serde_json::Value = serde_json::from_str(&tc.function.arguments)
                    .unwrap_or_else(|_| serde_json::json!({}));

                let questions = args
                    .get("questions")
                    .and_then(|v| v.as_array())
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|v| v.as_str().map(String::from))
                            .collect()
                    })
                    .unwrap_or_default();

                let reason = args
                    .get("reason")
                    .and_then(|v| v.as_str())
                    .unwrap_or("Clarification needed")
                    .to_string();

                return Ok(PlannerAiResponse {
                    text: None,
                    clarification: Some(ClarificationRequest { questions, reason }),
                });
            }
        }
    }

    // No clarify tool call — return text content.
    let content = choice
        .message
        .content
        .clone()
        .unwrap_or_default();

    if content.is_empty() {
        bail!("planner AI returned empty response");
    }

    Ok(PlannerAiResponse {
        text: Some(content),
        clarification: None,
    })
}

async fn call_ai(
    cfg: &AiClientConfig,
    model: &str,
    system_prompt: &str,
    user_prompt: &str,
) -> Result<String> {
    let request = ChatCompletionRequest {
        model: model.to_string(),
        messages: vec![
            ChatMessage {
                role: "system".into(),
                content: Some(system_prompt.to_string()),
                tool_calls: None,
                tool_call_id: None,
            },
            ChatMessage {
                role: "user".into(),
                content: Some(user_prompt.to_string()),
                tool_calls: None,
                tool_call_id: None,
            },
        ],
        temperature: Some(0.2),
        max_tokens: Some(4096),
        stream: None,
        tools: None,
        tool_choice: None,
    };

    let resp = client::chat_completion(cfg, request)
        .await
        .context("planner AI call failed")?;

    resp.choices
        .first()
        .and_then(|c| c.message.content.clone())
        .ok_or_else(|| anyhow::anyhow!("planner AI returned empty response"))
}

/// Fallback: call AI without tools, with a prompt that explicitly asks for JSON.
/// Used when the AI endpoint doesn't support function calling.
async fn call_ai_text_only(
    cfg: &AiClientConfig,
    model: &str,
    prompt: &str,
) -> Result<String> {
    call_ai(cfg, model, FALLBACK_SYSTEM_PROMPT, prompt).await
}

const FALLBACK_SYSTEM_PROMPT: &str = r#"You are a workflow planner. Create a structured execution plan as JSON.

Return ONLY a JSON object with this exact structure:
{
  "goal": "one sentence describing the goal",
  "steps": [
    {
      "id": "kebab-case-step-id",
      "description": "what this step does",
      "tool": "agent.action_name",
      "agent_type": "gitlab|ai|http|script|sandbox|coding",
      "inputs": {"param": "value"},
      "success_criteria": "how to verify success",
      "depends_on": ["other-step-id"],
      "capabilities_needed": []
    }
  ],
  "capabilities_needed": []
}

Return ONLY valid JSON. No markdown fences, no prose."#;

/// Build a fallback prompt that includes the tool catalog as text
/// (for AI endpoints that don't support function calling).
fn build_fallback_prompt(
    trigger_data: &serde_json::Value,
    workflow_description: &str,
    mentor_context: &str,
    tool_catalog: &[ToolDefinition],
) -> String {
    let tools_text: Vec<String> = tool_catalog
        .iter()
        .filter(|t| t.function.name != "clarify")
        .map(|t| {
            let params = t.function.parameters
                .get("properties")
                .and_then(|p| p.as_object())
                .map(|props| {
                    props.keys().cloned().collect::<Vec<_>>().join(", ")
                })
                .unwrap_or_default();
            format!("- {} ({}) — {}", t.function.name, params, t.function.description)
        })
        .collect();

    let mut prompt = format!(
        "Create an execution plan for this task:\n\n{}\n\nAvailable tools:\n{}\n",
        workflow_description,
        tools_text.join("\n"),
    );

    if !trigger_data.is_null() {
        prompt.push_str(&format!(
            "\nTrigger data:\n{}\n",
            serde_json::to_string_pretty(trigger_data).unwrap_or_default()
        ));
    }

    if !mentor_context.is_empty() {
        prompt.push_str(&format!("\nRelevant knowledge:\n{}\n", mentor_context));
    }

    prompt.push_str("\nUse the exact tool names from the list above in the 'tool' field of each step. Include the required inputs for each tool.");

    prompt
}

// ---------------------------------------------------------------------------
// Prompt construction
// ---------------------------------------------------------------------------

fn build_system_prompt(tool_catalog: &[ToolDefinition]) -> String {
    let mut prompt = r#"You are a workflow planner. Given a trigger event and workflow description, produce a JSON execution plan.

You have access to the following tools. Each step in your plan should specify which tool to use via the "tool" field:"#.to_string();

    // Include tool descriptions from the catalog.
    if !tool_catalog.is_empty() {
        prompt.push_str("\n\nAvailable tools:\n");
        for tool in tool_catalog {
            if tool.function.name == "clarify" {
                continue; // Clarify is handled via function calling, not plan steps.
            }
            prompt.push_str(&format!(
                "- \"{}\": {}\n",
                tool.function.name, tool.function.description
            ));
        }
    }

    prompt.push_str(r#"
If you need more information from the user before you can create a good plan, call the "clarify" tool with your questions instead of producing a plan.

Respond with ONLY a JSON object (no markdown, no explanation) matching this schema:
{
  "goal": "string — one-sentence summary of what the plan achieves",
  "steps": [
    {
      "id": "string — unique kebab-case identifier",
      "description": "string — what this step does",
      "agentType": "gitlab" | "ai" | "sandbox" | "http" | "script" | "composite" | "coding",
      "tool": "string — tool name from the catalog (e.g., 'gitlab.list_open_mrs')",
      "successCriteria": "string — how to verify this step succeeded",
      "dependsOn": ["step-id", ...],
      "capabilitiesNeeded": ["string", ...]
    }
  ],
  "capabilitiesNeeded": ["string — overall capabilities the plan requires"]
}

Rules:
- Step IDs must be unique within the plan.
- dependsOn must only reference IDs of other steps in the plan.
- The dependency graph must be acyclic (no circular dependencies).
- Order steps so dependencies come before dependents.
- Keep plans minimal — only include steps that are necessary.
- Each step should do one thing well.
- The "tool" field should match a tool name from the available tools list.
- The "agentType" should match the prefix of the tool name (e.g., "gitlab" for "gitlab.list_open_mrs").
- Use {{step-id.output}} in step descriptions to reference outputs from previous steps."#);

    prompt
}

fn build_create_prompt(
    trigger_data: &serde_json::Value,
    workflow_description: &str,
    mentor_context: &str,
) -> String {
    let mut prompt = format!(
        "Create an execution plan for this workflow.\n\n\
         Workflow description: {workflow_description}\n\n\
         Trigger data:\n```json\n{}\n```",
        serde_json::to_string_pretty(trigger_data).unwrap_or_else(|_| trigger_data.to_string()),
    );

    if !mentor_context.is_empty() {
        prompt.push_str(&format!(
            "\n\nRelevant knowledge from previous executions:\n{mentor_context}"
        ));
    }

    prompt
}

fn build_replan_prompt(
    original_plan: &SessionPlan,
    completed_steps: &[&str],
    failed_steps: &[(&str, &str)],
    new_context: Option<&str>,
    mentor_context: &str,
) -> String {
    let original_json =
        serde_json::to_string_pretty(original_plan).unwrap_or_else(|_| "{}".into());

    let mut prompt = format!(
        "Revise this execution plan. Keep completed steps unchanged, \
         replace or remove failed steps, and adjust remaining steps as needed.\n\n\
         Original plan:\n```json\n{original_json}\n```\n\n\
         Completed steps: {}\n",
        if completed_steps.is_empty() {
            "none".to_string()
        } else {
            completed_steps.join(", ")
        },
    );

    if !failed_steps.is_empty() {
        prompt.push_str("\nFailed steps:\n");
        for (id, err) in failed_steps {
            prompt.push_str(&format!("- {id}: {err}\n"));
        }
    }

    if let Some(ctx) = new_context {
        prompt.push_str(&format!("\nAdditional context: {ctx}\n"));
    }

    if !mentor_context.is_empty() {
        prompt.push_str(&format!(
            "\nRelevant knowledge from previous executions:\n{mentor_context}"
        ));
    }

    prompt
}

// ---------------------------------------------------------------------------
// Response parsing — fault-tolerant
// ---------------------------------------------------------------------------

/// Try to parse a `SessionPlan` from an AI response. Attempts, in order:
/// 1. Direct JSON parse of the full response
/// 2. Extract from markdown code fences
/// 3. Extract the first top-level `{ ... }` block
fn parse_plan(response: &str) -> Result<SessionPlan> {
    let trimmed = response.trim();

    // Attempt 1: direct parse.
    if let Ok(plan) = serde_json::from_str::<SessionPlan>(trimmed) {
        return Ok(plan);
    }

    // Attempt 2: extract from code fences.
    if let Some(json) = extract_fenced_json(trimmed) {
        if let Ok(plan) = serde_json::from_str::<SessionPlan>(&json) {
            return Ok(plan);
        }
        warn!("found fenced JSON but it didn't parse as SessionPlan");
    }

    // Attempt 3: find the first top-level brace block.
    if let Some(json) = extract_brace_block(trimmed) {
        if let Ok(plan) = serde_json::from_str::<SessionPlan>(&json) {
            return Ok(plan);
        }
        warn!("found brace block but it didn't parse as SessionPlan");
    }

    bail!(
        "could not extract a valid SessionPlan from AI response (len={})",
        response.len()
    )
}

/// Extract JSON content from the first markdown code fence (```json ... ``` or ``` ... ```).
fn extract_fenced_json(text: &str) -> Option<String> {
    // Look for ```json or just ```
    let fence_start = text.find("```json").or_else(|| text.find("```"))?;
    let content_start = text[fence_start..].find('\n')? + fence_start + 1;
    let content_end = text[content_start..].find("```")? + content_start;
    let content = text[content_start..content_end].trim();
    if content.is_empty() {
        None
    } else {
        Some(content.to_string())
    }
}

/// Extract the first balanced `{ ... }` block from the text.
fn extract_brace_block(text: &str) -> Option<String> {
    let start = text.find('{')?;
    let mut depth = 0i32;
    let mut in_string = false;
    let mut escape_next = false;

    for (i, ch) in text[start..].char_indices() {
        if escape_next {
            escape_next = false;
            continue;
        }
        match ch {
            '\\' if in_string => escape_next = true,
            '"' => in_string = !in_string,
            '{' if !in_string => depth += 1,
            '}' if !in_string => {
                depth -= 1;
                if depth == 0 {
                    return Some(text[start..start + i + 1].to_string());
                }
            }
            _ => {}
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Plan validation
// ---------------------------------------------------------------------------

/// Validate a parsed plan: unique IDs, valid depends_on refs, no cycles,
/// non-empty steps.
fn validate_plan(mut plan: SessionPlan) -> Result<SessionPlan> {
    if plan.steps.is_empty() {
        bail!("planner produced an empty plan with no steps");
    }

    // Check for duplicate IDs.
    let mut seen_ids = HashSet::new();
    let mut dupes = Vec::new();
    for step in &plan.steps {
        if !seen_ids.insert(&step.id) {
            dupes.push(step.id.clone());
        }
    }
    if !dupes.is_empty() {
        bail!("planner produced duplicate step IDs: {}", dupes.join(", "));
    }

    // Validate depends_on references — remove invalid ones with a warning.
    let valid_ids: HashSet<String> = plan.steps.iter().map(|s| s.id.clone()).collect();
    for step in &mut plan.steps {
        let before = step.depends_on.len();
        step.depends_on.retain(|dep| {
            if valid_ids.contains(dep.as_str() as &str) {
                true
            } else {
                warn!(
                    step_id = %step.id,
                    invalid_dep = %dep,
                    "removing invalid depends_on reference"
                );
                false
            }
        });
        if step.depends_on.len() != before {
            debug!(
                step_id = %step.id,
                removed = before - step.depends_on.len(),
                "pruned invalid dependency references"
            );
        }
    }

    // Check for cycles.
    if has_cycle(&plan.steps) {
        bail!("planner produced a plan with cyclic dependencies");
    }

    Ok(plan)
}

/// DFS-based cycle detection on the step dependency graph.
fn has_cycle(steps: &[PlanStep]) -> bool {
    let index_map: HashMap<&str, usize> = steps
        .iter()
        .enumerate()
        .map(|(i, s)| (s.id.as_str(), i))
        .collect();

    // Build adjacency list: step -> steps it depends on.
    let adj: Vec<Vec<usize>> = steps
        .iter()
        .map(|s| {
            s.depends_on
                .iter()
                .filter_map(|dep| index_map.get(dep.as_str()).copied())
                .collect()
        })
        .collect();

    // 0 = unvisited, 1 = in current path, 2 = fully explored
    let mut state = vec![0u8; steps.len()];

    fn dfs(node: usize, adj: &[Vec<usize>], state: &mut [u8]) -> bool {
        state[node] = 1;
        for &neighbor in &adj[node] {
            match state[neighbor] {
                1 => return true, // back edge → cycle
                0 => {
                    if dfs(neighbor, adj, state) {
                        return true;
                    }
                }
                _ => {} // already explored
            }
        }
        state[node] = 2;
        false
    }

    for i in 0..steps.len() {
        if state[i] == 0 && dfs(i, &adj, &mut state) {
            return true;
        }
    }

    false
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::workflow::AgentType;

    fn step(id: &str, deps: &[&str]) -> PlanStep {
        PlanStep {
            id: id.into(),
            description: format!("step {id}"),
            agent_type: AgentType::Ai,
            success_criteria: "done".into(),
            depends_on: deps.iter().map(|s| s.to_string()).collect(),
            capabilities_needed: vec![],
            tool: None,
        }
    }

    // -- parse_plan ----------------------------------------------------------

    #[test]
    fn parse_plan_direct_json() {
        let json = r#"{"goal":"test","steps":[],"capabilitiesNeeded":[]}"#;
        // Empty steps will fail validation, but parse should succeed.
        let plan = parse_plan(json).unwrap();
        assert_eq!(plan.goal, "test");
    }

    #[test]
    fn parse_plan_fenced_json() {
        let text = "Here is the plan:\n```json\n{\"goal\":\"test\",\"steps\":[],\"capabilitiesNeeded\":[]}\n```\nDone.";
        let plan = parse_plan(text).unwrap();
        assert_eq!(plan.goal, "test");
    }

    #[test]
    fn parse_plan_brace_extraction() {
        let text = "Sure! {\"goal\":\"test\",\"steps\":[],\"capabilitiesNeeded\":[]} Hope that helps.";
        let plan = parse_plan(text).unwrap();
        assert_eq!(plan.goal, "test");
    }

    #[test]
    fn parse_plan_garbage_fails() {
        assert!(parse_plan("no json here at all").is_err());
    }

    // -- extract_fenced_json -------------------------------------------------

    #[test]
    fn extract_fenced_json_basic() {
        let text = "```json\n{\"a\":1}\n```";
        assert_eq!(extract_fenced_json(text).unwrap(), "{\"a\":1}");
    }

    #[test]
    fn extract_fenced_json_no_lang() {
        let text = "```\n{\"a\":1}\n```";
        assert_eq!(extract_fenced_json(text).unwrap(), "{\"a\":1}");
    }

    #[test]
    fn extract_fenced_json_none() {
        assert!(extract_fenced_json("no fences").is_none());
    }

    // -- extract_brace_block -------------------------------------------------

    #[test]
    fn extract_brace_block_nested() {
        let text = "prefix {\"a\":{\"b\":1}} suffix";
        assert_eq!(extract_brace_block(text).unwrap(), "{\"a\":{\"b\":1}}");
    }

    #[test]
    fn extract_brace_block_with_strings() {
        let text = r#"x {"key": "val with } brace"} y"#;
        assert_eq!(
            extract_brace_block(text).unwrap(),
            r#"{"key": "val with } brace"}"#
        );
    }

    #[test]
    fn extract_brace_block_none() {
        assert!(extract_brace_block("no braces").is_none());
    }

    // -- validate_plan -------------------------------------------------------

    #[test]
    fn validate_plan_ok() {
        let plan = SessionPlan {
            goal: "test".into(),
            steps: vec![step("a", &[]), step("b", &["a"])],
            capabilities_needed: vec![],
        };
        let validated = validate_plan(plan).unwrap();
        assert_eq!(validated.steps.len(), 2);
    }

    #[test]
    fn validate_plan_empty_steps() {
        let plan = SessionPlan {
            goal: "test".into(),
            steps: vec![],
            capabilities_needed: vec![],
        };
        assert!(validate_plan(plan).is_err());
    }

    #[test]
    fn validate_plan_duplicate_ids() {
        let plan = SessionPlan {
            goal: "test".into(),
            steps: vec![step("a", &[]), step("a", &[])],
            capabilities_needed: vec![],
        };
        assert!(validate_plan(plan).is_err());
    }

    #[test]
    fn validate_plan_removes_invalid_deps() {
        let plan = SessionPlan {
            goal: "test".into(),
            steps: vec![step("a", &["nonexistent"])],
            capabilities_needed: vec![],
        };
        let validated = validate_plan(plan).unwrap();
        assert!(validated.steps[0].depends_on.is_empty());
    }

    #[test]
    fn validate_plan_cycle_detected() {
        let plan = SessionPlan {
            goal: "test".into(),
            steps: vec![step("a", &["b"]), step("b", &["a"])],
            capabilities_needed: vec![],
        };
        assert!(validate_plan(plan).is_err());
    }

    // -- has_cycle -----------------------------------------------------------

    #[test]
    fn has_cycle_linear() {
        let steps = vec![step("a", &[]), step("b", &["a"]), step("c", &["b"])];
        assert!(!has_cycle(&steps));
    }

    #[test]
    fn has_cycle_diamond() {
        let steps = vec![
            step("a", &[]),
            step("b", &["a"]),
            step("c", &["a"]),
            step("d", &["b", "c"]),
        ];
        assert!(!has_cycle(&steps));
    }

    #[test]
    fn has_cycle_self_loop() {
        let steps = vec![step("a", &["a"])];
        assert!(has_cycle(&steps));
    }

    #[test]
    fn has_cycle_triangle() {
        let steps = vec![
            step("a", &["c"]),
            step("b", &["a"]),
            step("c", &["b"]),
        ];
        assert!(has_cycle(&steps));
    }

    #[test]
    fn has_cycle_disconnected_with_cycle() {
        let steps = vec![
            step("a", &[]),
            step("b", &["c"]),
            step("c", &["b"]),
        ];
        assert!(has_cycle(&steps));
    }
}
