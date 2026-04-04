// ---------------------------------------------------------------------------
// Generator agent — executes a single plan step with fresh context.
//
// The Generator is the "hands" of the three-agent pipeline. For each step it:
//   1. Builds a fresh context document (no accumulated memory)
//   2. Dispatches to the appropriate workflow agent (gitlab, ai, sandbox, etc.)
//   3. Interprets the result into a structured GeneratorOutcome
//
// The Session Manager calls `execute_step()` once per step. If the Evaluator
// rejects the output, the Session Manager calls it again with evaluator
// feedback attached.
// ---------------------------------------------------------------------------

use anyhow::Result;
use std::collections::HashMap;
use tracing::{debug, info, warn};

use crate::services::ai::client::{
    self, AiClientConfig, ChatCompletionRequest, ChatMessage, ToolDefinition,
};
use crate::services::mentor::client::MentorClient;
use crate::services::workflow::factory::{self, AgentFactoryConfig};
use crate::services::workflow::registry;
use crate::types::workflow::{
    AgentStatus, AgentType, EscalationOption, EvaluatorVerdict, GeneratorOutcome, PlanStep,
};

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Execute a single plan step and return a structured outcome.
///
/// Fresh context is built from scratch each invocation — no accumulated state.
/// If `evaluator_feedback` is provided, this is a retry after a failed evaluation.
/// If `session_context` is provided, session-level fields (project_path, branch, etc.)
/// are merged into the step inputs so agents can find them.
/// If `tool_catalog` is provided, steps without a `tool` field use AI function calling
/// to pick the right tool. Steps WITH a `tool` field dispatch directly.
pub async fn execute_step(
    step: &PlanStep,
    step_outputs: &HashMap<String, serde_json::Value>,
    mentor: &MentorClient,
    agent_config: &AgentFactoryConfig,
    ai_config: &AiClientConfig,
    ai_model: &str,
    evaluator_feedback: Option<&EvaluatorVerdict>,
    session_context: Option<&serde_json::Value>,
    tool_catalog: &[ToolDefinition],
) -> Result<GeneratorOutcome> {
    info!(
        step_id = %step.id,
        agent_type = %step.agent_type,
        tool = step.tool.as_deref().unwrap_or("none"),
        "generator: executing step"
    );

    // 1. Gather dependency outputs for this step.
    let dep_outputs = gather_dependency_outputs(step, step_outputs);

    // 2. Query mentor for step-relevant knowledge.
    let mentor_context = query_mentor_for_step(mentor, step).await;

    // 3. Resolve the tool name and build inputs.
    let (agent_type_str, action, mut inputs) = if let Some(ref tool_name) = step.tool {
        // Step has an explicit tool — parse and dispatch directly.
        let (agent_str, act) = registry::parse_tool_name(tool_name);
        let inputs = build_tool_inputs(step, &dep_outputs, &mentor_context, evaluator_feedback, session_context, step_outputs);
        (agent_str.to_string(), act.to_string(), inputs)
    } else if !tool_catalog.is_empty() {
        // No explicit tool — use AI function calling to pick one.
        match ai_pick_tool(ai_config, ai_model, step, &dep_outputs, &mentor_context, evaluator_feedback, session_context, tool_catalog, step_outputs).await {
            Ok(Some((agent_str, act, tool_inputs))) => {
                (agent_str, act, tool_inputs)
            }
            Ok(None) => {
                // AI didn't call a tool — fall back to legacy resolution.
                let inputs = build_step_inputs(step, &dep_outputs, &mentor_context, evaluator_feedback, session_context);
                let action = resolve_agent_action_legacy(&step.agent_type, &step.description, &step.id);
                (step.agent_type.as_str().to_string(), action, inputs)
            }
            Err(e) => {
                warn!(step_id = %step.id, "AI tool selection failed: {e:#}, falling back to legacy");
                let inputs = build_step_inputs(step, &dep_outputs, &mentor_context, evaluator_feedback, session_context);
                let action = resolve_agent_action_legacy(&step.agent_type, &step.description, &step.id);
                (step.agent_type.as_str().to_string(), action, inputs)
            }
        }
    } else {
        // No tool catalog — use legacy keyword matching.
        let inputs = build_step_inputs(step, &dep_outputs, &mentor_context, evaluator_feedback, session_context);
        let action = resolve_agent_action_legacy(&step.agent_type, &step.description, &step.id);
        (step.agent_type.as_str().to_string(), action, inputs)
    };

    // Resolve {{step-id.output}} references in string input values.
    resolve_output_references(&mut inputs, step_outputs);

    // CRITICAL: For AI agent steps, always ensure dependency data is in content/text/prompt.
    // This is the last-resort injection — if build_tool_inputs or ai_pick_tool didn't
    // inject it, we do it here before the agent call.
    if matches!(agent_type_str.as_str(), "ai") && !dep_outputs.is_empty() {
        let has_content = inputs.get("content").map(|v| !v.is_null() && v.as_str().map(|s| !s.is_empty() && !s.contains("{{")).unwrap_or(true)).unwrap_or(false);
        if !has_content {
            let text_parts: Vec<String> = dep_outputs
                .iter()
                .map(|(sid, output)| {
                    let output_str = match output {
                        serde_json::Value::String(s) => s.clone(),
                        other => serde_json::to_string_pretty(other).unwrap_or_default(),
                    };
                    format!("--- Output from step '{}' ---\n{}", sid, output_str)
                })
                .collect();
            let combined = format!(
                "Task: {}\n\nSuccess criteria: {}\n\n{}",
                step.description, step.success_criteria, text_parts.join("\n\n")
            );
            let combined = truncate_for_ai_context(&combined, MAX_AI_CONTENT_CHARS);
            info!(
                step_id = %step.id,
                content_len = combined.len(),
                dep_count = dep_outputs.len(),
                "generator: last-resort injection of dependency data for AI step"
            );
            inputs.insert("text".into(), serde_json::Value::String(combined.clone()));
            inputs.insert("content".into(), serde_json::Value::String(combined));
            inputs.insert("prompt".into(), serde_json::Value::String(step.description.clone()));
        }
    }

    debug!(step_id = %step.id, input_keys = ?inputs.keys().collect::<Vec<_>>(), "final inputs before agent call");

    // 4. Map agent_type string to AgentType enum for the factory.
    let agent_type_enum = match agent_type_str.as_str() {
        "gitlab" => AgentType::Gitlab,
        "ai" => AgentType::Ai,
        "http" => AgentType::Http,
        "script" => AgentType::Script,
        "sandbox" => AgentType::Sandbox,
        "coding" => AgentType::Coding,
        "composite" => AgentType::Composite,
        _ => step.agent_type.clone(),
    };

    // 5. Create the agent.
    let agent = factory::create_agent(&agent_type_enum, agent_config, 0)
        .await
        .ok_or_else(|| {
            anyhow::anyhow!(
                "no agent available for type '{}' — missing configuration?",
                agent_type_str
            )
        })?;

    debug!(step_id = %step.id, resolved_action = %action, "dispatching to {} agent", agent.agent_type_name());
    let result = agent.execute(&action, inputs, mentor).await;

    debug!(
        step_id = %step.id,
        status = ?result.status,
        duration = result.duration_secs,
        "agent execution complete"
    );

    // 6. If the step failed, attempt adaptive recovery (max 2 attempts).
    if result.status == AgentStatus::Failure && !tool_catalog.is_empty() {
        let error_msg = result.output.get("error")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown error");

        if let Some(recovery_outcome) = try_adaptive_recovery(
            ai_config, ai_model, step, error_msg, step_outputs,
            &dep_outputs, &mentor_context, evaluator_feedback,
            session_context, tool_catalog, agent_config, mentor,
        ).await {
            return Ok(recovery_outcome);
        }
    }

    // 7. Interpret the raw AgentResult into a GeneratorOutcome.
    let outcome = interpret_result(result, step, ai_config, ai_model).await?;

    info!(
        step_id = %step.id,
        outcome_type = outcome_type_name(&outcome),
        "generator: step outcome determined"
    );

    Ok(outcome)
}

// ---------------------------------------------------------------------------
// Context building — fresh per invocation
// ---------------------------------------------------------------------------

/// Collect outputs from dependency steps that this step needs.
fn gather_dependency_outputs(
    step: &PlanStep,
    step_outputs: &HashMap<String, serde_json::Value>,
) -> HashMap<String, serde_json::Value> {
    let mut deps = HashMap::new();
    for dep_id in &step.depends_on {
        if let Some(output) = step_outputs.get(dep_id) {
            deps.insert(dep_id.clone(), output.clone());
        } else {
            warn!(
                step_id = %step.id,
                dep_id = %dep_id,
                "dependency output not found — step may have been skipped"
            );
        }
    }
    deps
}

/// Best-effort mentor query scoped to this step.
async fn query_mentor_for_step(mentor: &MentorClient, step: &PlanStep) -> String {
    let question = format!("{} {}", step.description, step.success_criteria);
    match mentor.query(&question, 3).await {
        Ok(results) if results.is_empty() => String::new(),
        Ok(results) => results
            .iter()
            .map(|r| format!("- [{}] {}", r.category, r.content))
            .collect::<Vec<_>>()
            .join("\n"),
        Err(e) => {
            warn!(step_id = %step.id, "mentor query failed for step: {e}");
            String::new()
        }
    }
}

/// Build the inputs map that gets passed to the workflow agent.
fn build_step_inputs(
    step: &PlanStep,
    dep_outputs: &HashMap<String, serde_json::Value>,
    mentor_context: &str,
    evaluator_feedback: Option<&EvaluatorVerdict>,
    session_context: Option<&serde_json::Value>,
) -> HashMap<String, serde_json::Value> {
    let mut inputs = HashMap::new();

    // Step metadata.
    inputs.insert(
        "step_id".into(),
        serde_json::Value::String(step.id.clone()),
    );
    inputs.insert(
        "description".into(),
        serde_json::Value::String(step.description.clone()),
    );
    inputs.insert(
        "success_criteria".into(),
        serde_json::Value::String(step.success_criteria.clone()),
    );

    // Session-level context from trigger_data (project_path, branch, task description, etc.)
    if let Some(ctx) = session_context {
        if let Some(obj) = ctx.as_object() {
            for key in &["project_path", "project_id", "branch", "description", "requested_by"] {
                if let Some(val) = obj.get(*key) {
                    if !val.is_null() {
                        inputs.insert(key.to_string(), val.clone());
                    }
                }
            }
        }
    }

    // Extract project_path from step description if not already in inputs.
    // With tool-based dispatch, the AI typically provides project_path in tool args.
    // This is a legacy fallback for NL descriptions like "from gitlab-org/gitlab-runner".
    if !inputs.contains_key("project_path") && !inputs.contains_key("project_id") {
        // Simple regex-free extraction: look for slash-separated tokens.
        let desc = &step.description;
        for word in desc.split_whitespace() {
            let cleaned = word.trim_matches(|c: char| !c.is_alphanumeric() && c != '/' && c != '-' && c != '_' && c != '.');
            if cleaned.contains('/') && !cleaned.contains("://") && cleaned.len() > 3 {
                let parts: Vec<&str> = cleaned.split('/').collect();
                if parts.len() >= 2 && parts.iter().all(|p| !p.is_empty()) {
                    inputs.insert("project_path".into(), serde_json::Value::String(cleaned.to_string()));
                    break;
                }
            }
        }
    }

    // Dependency outputs.
    if !dep_outputs.is_empty() {
        inputs.insert(
            "dependency_outputs".into(),
            serde_json::to_value(dep_outputs).unwrap_or_default(),
        );

        // For AI agent steps, flatten dependency outputs into a "text" field
        // so the summarize/analyze/chat actions can consume them directly.
        // The AI agent expects "text" as a string, not a nested JSON object.
        if matches!(step.agent_type, crate::types::workflow::AgentType::Ai) {
            let text_parts: Vec<String> = dep_outputs
                .iter()
                .map(|(step_id, output)| {
                    let output_str = match output {
                        serde_json::Value::String(s) => s.clone(),
                        serde_json::Value::Array(arr) => {
                            // For arrays (e.g., list of MRs), serialize compactly
                            serde_json::to_string_pretty(arr).unwrap_or_default()
                        }
                        other => serde_json::to_string_pretty(other).unwrap_or_default(),
                    };
                    format!("--- Output from step '{}' ---\n{}", step_id, output_str)
                })
                .collect();

            let combined_text = format!(
                "Task: {}\n\nSuccess criteria: {}\n\n{}",
                step.description,
                step.success_criteria,
                text_parts.join("\n\n")
            );
            let combined_text = truncate_for_ai_context(&combined_text, MAX_AI_CONTENT_CHARS);
            debug!(
                step_id = %step.id,
                content_len = combined_text.len(),
                dep_count = dep_outputs.len(),
                "build_step_inputs: injected dependency data for AI step"
            );
            inputs.insert("text".into(), serde_json::Value::String(combined_text));
            inputs.insert("prompt".into(), serde_json::Value::String(step.description.clone()));
        }
    }

    // Mentor knowledge.
    if !mentor_context.is_empty() {
        inputs.insert(
            "mentor_context".into(),
            serde_json::Value::String(mentor_context.to_string()),
        );
    }

    // Evaluator feedback (retry context).
    if let Some(feedback) = evaluator_feedback {
        inputs.insert(
            "evaluator_feedback".into(),
            serde_json::json!({
                "passed": feedback.passed,
                "score": feedback.score,
                "feedback": feedback.feedback,
                "suggestion": feedback.suggestion,
            }),
        );
    }

    inputs
}

// ---------------------------------------------------------------------------
// Tool-based input building
// ---------------------------------------------------------------------------

/// Build inputs from the tool's expected parameters, pulling values from
/// step description, dependency outputs, session context, and evaluator feedback.
fn build_tool_inputs(
    step: &PlanStep,
    dep_outputs: &HashMap<String, serde_json::Value>,
    mentor_context: &str,
    evaluator_feedback: Option<&EvaluatorVerdict>,
    session_context: Option<&serde_json::Value>,
    _step_outputs: &HashMap<String, serde_json::Value>,
) -> HashMap<String, serde_json::Value> {
    let mut inputs = HashMap::new();

    // Step metadata.
    inputs.insert("step_id".into(), serde_json::Value::String(step.id.clone()));
    inputs.insert("description".into(), serde_json::Value::String(step.description.clone()));
    inputs.insert("success_criteria".into(), serde_json::Value::String(step.success_criteria.clone()));

    // Session-level context.
    if let Some(ctx) = session_context {
        if let Some(obj) = ctx.as_object() {
            for key in &["project_path", "project_id", "branch", "description", "requested_by",
                         "task_description", "file_path", "suggestion", "mr_iid", "target_branch",
                         "mr_title", "mr_description"] {
                if let Some(val) = obj.get(*key) {
                    if !val.is_null() {
                        inputs.insert(key.to_string(), val.clone());
                    }
                }
            }
        }
    }

    // Dependency outputs — flatten for AI agent consumption.
    if !dep_outputs.is_empty() {
        inputs.insert(
            "dependency_outputs".into(),
            serde_json::to_value(dep_outputs).unwrap_or_default(),
        );

        // For AI agent steps, build text/content/prompt from deps.
        if let Some(ref tool_name) = step.tool {
            let (agent, _) = registry::parse_tool_name(tool_name);
            if agent == "ai" {
                let text_parts: Vec<String> = dep_outputs
                    .iter()
                    .map(|(sid, output)| {
                        let output_str = match output {
                            serde_json::Value::String(s) => s.clone(),
                            other => serde_json::to_string_pretty(other).unwrap_or_default(),
                        };
                        format!("--- Output from step '{}' ---\n{}", sid, output_str)
                    })
                    .collect();
                let combined = format!(
                    "Task: {}\n\nSuccess criteria: {}\n\n{}",
                    step.description, step.success_criteria, text_parts.join("\n\n")
                );
                let combined = truncate_for_ai_context(&combined, MAX_AI_CONTENT_CHARS);
                debug!(
                    step_id = %step.id,
                    content_len = combined.len(),
                    dep_count = dep_outputs.len(),
                    "build_tool_inputs: injected dependency data for AI step"
                );
                inputs.insert("text".into(), serde_json::Value::String(combined.clone()));
                inputs.insert("content".into(), serde_json::Value::String(combined));
                inputs.insert("prompt".into(), serde_json::Value::String(step.description.clone()));
                inputs.insert("user".into(), serde_json::Value::String(step.description.clone()));
            }
        }
    }

    // Mentor knowledge.
    if !mentor_context.is_empty() {
        inputs.insert("mentor_context".into(), serde_json::Value::String(mentor_context.to_string()));
    }

    // Evaluator feedback.
    if let Some(feedback) = evaluator_feedback {
        inputs.insert(
            "evaluator_feedback".into(),
            serde_json::json!({
                "passed": feedback.passed,
                "score": feedback.score,
                "feedback": feedback.feedback,
                "suggestion": feedback.suggestion,
            }),
        );
    }

    inputs
}

// ---------------------------------------------------------------------------
// AI-based tool selection (for steps without explicit tool field)
// ---------------------------------------------------------------------------

/// Ask the AI to pick the right tool for a step using function calling.
/// Returns (agent_type, action, inputs) or None if the AI didn't call a tool.
async fn ai_pick_tool(
    ai_config: &AiClientConfig,
    ai_model: &str,
    step: &PlanStep,
    dep_outputs: &HashMap<String, serde_json::Value>,
    _mentor_context: &str,
    _evaluator_feedback: Option<&EvaluatorVerdict>,
    session_context: Option<&serde_json::Value>,
    tool_catalog: &[ToolDefinition],
    _step_outputs: &HashMap<String, serde_json::Value>,
) -> Result<Option<(String, String, HashMap<String, serde_json::Value>)>> {
    let dep_summary = if dep_outputs.is_empty() {
        String::new()
    } else {
        let parts: Vec<String> = dep_outputs
            .iter()
            .map(|(k, v)| format!("Step '{}' output: {}", k, serde_json::to_string(v).unwrap_or_default()))
            .collect();
        format!("\n\nDependency outputs:\n{}", parts.join("\n"))
    };

    let ctx_summary = session_context
        .map(|c| format!("\n\nSession context: {}", serde_json::to_string_pretty(c).unwrap_or_default()))
        .unwrap_or_default();

    let user_msg = format!(
        "Execute this workflow step by calling the appropriate tool.\n\n\
         Step ID: {}\n\
         Description: {}\n\
         Success criteria: {}\n\
         Agent type hint: {}{}{}\n\n\
         Call the most appropriate tool with the correct arguments.",
        step.id, step.description, step.success_criteria, step.agent_type,
        dep_summary, ctx_summary,
    );

    // Filter out the clarify tool — it's not relevant for step execution.
    let step_tools: Vec<ToolDefinition> = tool_catalog
        .iter()
        .filter(|t| t.function.name != "clarify")
        .cloned()
        .collect();

    let request = ChatCompletionRequest {
        model: ai_model.to_string(),
        messages: vec![
            ChatMessage {
                role: "system".into(),
                content: Some(
                    "You are a workflow step executor. Given a step description, call the most \
                     appropriate tool with the correct arguments. Always call exactly one tool."
                        .to_string(),
                ),
                tool_calls: None,
                tool_call_id: None,
            },
            ChatMessage {
                role: "user".into(),
                content: Some(user_msg),
                tool_calls: None,
                tool_call_id: None,
            },
        ],
        temperature: Some(0.0),
        max_tokens: Some(1024),
        stream: None,
        tools: Some(step_tools),
        tool_choice: None,
    };

    let resp = client::chat_completion(ai_config, request).await?;

    let choice = match resp.choices.first() {
        Some(c) => c,
        None => return Ok(None),
    };

    if let Some(ref tool_calls) = choice.message.tool_calls {
        if let Some(tc) = tool_calls.first() {
            let (agent_str, action) = registry::parse_tool_name(&tc.function.name);

            // Parse the tool arguments into an inputs map.
            let args: serde_json::Value = serde_json::from_str(&tc.function.arguments)
                .unwrap_or_else(|_| serde_json::json!({}));

            let mut inputs = HashMap::new();
            if let Some(obj) = args.as_object() {
                for (k, v) in obj {
                    inputs.insert(k.clone(), v.clone());
                }
            }

            // Merge session context for fields the AI might not have included.
            if let Some(ctx) = session_context {
                if let Some(obj) = ctx.as_object() {
                    for key in &["project_path", "project_id", "branch", "mr_iid"] {
                        if !inputs.contains_key(*key) {
                            if let Some(val) = obj.get(*key) {
                                if !val.is_null() {
                                    inputs.insert(key.to_string(), val.clone());
                                }
                            }
                        }
                    }
                }
            }

            // Add step metadata.
            inputs.insert("step_id".into(), serde_json::Value::String(step.id.clone()));
            inputs.insert("description".into(), serde_json::Value::String(step.description.clone()));
            inputs.insert("success_criteria".into(), serde_json::Value::String(step.success_criteria.clone()));

            // For AI agent steps, inject dependency outputs as content/prompt.
            // The AI function-calling path builds inputs from tool args only,
            // which means dependency data (e.g., MR list from a prior step)
            // never reaches the AI agent's analyze/summarize/chat actions.
            if agent_str == "ai" && !dep_outputs.is_empty() {
                let text_parts: Vec<String> = dep_outputs
                    .iter()
                    .map(|(sid, output)| {
                        let output_str = match output {
                            serde_json::Value::String(s) => s.clone(),
                            other => serde_json::to_string_pretty(other).unwrap_or_default(),
                        };
                        format!("--- Output from step '{}' ---\n{}", sid, output_str)
                    })
                    .collect();
                let combined = format!(
                    "Task: {}\n\nSuccess criteria: {}\n\n{}",
                    step.description, step.success_criteria, text_parts.join("\n\n")
                );
                let combined = truncate_for_ai_context(&combined, MAX_AI_CONTENT_CHARS);
                info!(
                    step_id = %step.id,
                    content_len = combined.len(),
                    dep_count = dep_outputs.len(),
                    "ai_pick_tool: injecting dependency data into AI step inputs"
                );
                // Only inject if the AI didn't already provide these fields.
                if !inputs.contains_key("content") {
                    inputs.insert("content".into(), serde_json::Value::String(combined.clone()));
                }
                if !inputs.contains_key("text") {
                    inputs.insert("text".into(), serde_json::Value::String(combined));
                }
                if !inputs.contains_key("prompt") {
                    inputs.insert("prompt".into(), serde_json::Value::String(step.description.clone()));
                }
                if !inputs.contains_key("user") {
                    inputs.insert("user".into(), serde_json::Value::String(step.description.clone()));
                }
                // Also pass the raw dependency_outputs for agents that want structured data.
                inputs.insert(
                    "dependency_outputs".into(),
                    serde_json::to_value(dep_outputs).unwrap_or_default(),
                );
            }

            // For http.request, map the method to the action name.
            let final_action = if agent_str == "http" && action == "request" {
                inputs.get("method")
                    .and_then(|v| v.as_str())
                    .unwrap_or("get")
                    .to_string()
            } else {
                action.to_string()
            };

            return Ok(Some((agent_str.to_string(), final_action, inputs)));
        }
    }

    Ok(None)
}

// ---------------------------------------------------------------------------
// Output truncation for AI context windows
// ---------------------------------------------------------------------------

/// Maximum characters of dependency output to pass into an AI agent's context.
/// Large outputs (e.g., 123 MRs as JSON) would blow the context window.
const MAX_AI_CONTENT_CHARS: usize = 60_000;

/// Truncate a string to fit within the AI context budget, appending a notice
/// if truncation occurred.
fn truncate_for_ai_context(text: &str, max_chars: usize) -> String {
    if text.len() <= max_chars {
        return text.to_string();
    }
    let truncated = &text[..text.floor_char_boundary(max_chars)];
    format!(
        "{}\n\n[... truncated — original was {} chars, showing first {}]",
        truncated,
        text.len(),
        max_chars,
    )
}

// ---------------------------------------------------------------------------
// Output reference resolution
// ---------------------------------------------------------------------------

/// Replace {{step-id.output}} references in string values with actual step output data.
fn resolve_output_references(
    inputs: &mut HashMap<String, serde_json::Value>,
    step_outputs: &HashMap<String, serde_json::Value>,
) {
    let keys: Vec<String> = inputs.keys().cloned().collect();
    for key in keys {
        if let Some(serde_json::Value::String(val)) = inputs.get(&key) {
            if val.contains("{{") && val.contains("}}") {
                let mut resolved = val.clone();
                for (step_id, output) in step_outputs {
                    let pattern = format!("{{{{{}.output}}}}", step_id);
                    if resolved.contains(&pattern) {
                        let replacement = match output {
                            serde_json::Value::String(s) => s.clone(),
                            other => serde_json::to_string(other).unwrap_or_default(),
                        };
                        resolved = resolved.replace(&pattern, &replacement);
                    }
                }
                inputs.insert(key, serde_json::Value::String(resolved));
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Adaptive failure recovery
// ---------------------------------------------------------------------------

/// On step failure, ask the AI to pick a different tool/approach.
/// Max 2 recovery attempts. Returns Some(outcome) if recovery succeeded.
async fn try_adaptive_recovery(
    ai_config: &AiClientConfig,
    ai_model: &str,
    step: &PlanStep,
    error: &str,
    _step_outputs: &HashMap<String, serde_json::Value>,
    _dep_outputs: &HashMap<String, serde_json::Value>,
    _mentor_context: &str,
    _evaluator_feedback: Option<&EvaluatorVerdict>,
    session_context: Option<&serde_json::Value>,
    tool_catalog: &[ToolDefinition],
    agent_config: &AgentFactoryConfig,
    mentor: &MentorClient,
) -> Option<GeneratorOutcome> {
    const MAX_RECOVERY_ATTEMPTS: u32 = 2;

    for attempt in 1..=MAX_RECOVERY_ATTEMPTS {
        info!(
            step_id = %step.id,
            attempt,
            "attempting adaptive recovery"
        );

        // Ask AI to pick a different approach given the error.
        let step_tools: Vec<ToolDefinition> = tool_catalog
            .iter()
            .filter(|t| t.function.name != "clarify")
            .cloned()
            .collect();

        let user_msg = format!(
            "The previous attempt to execute this step failed.\n\n\
             Step: {} — {}\n\
             Error: {}\n\
             Attempt: {} of {}\n\n\
             Try a different tool or different arguments to accomplish the same goal.\n\
             Success criteria: {}",
            step.id, step.description, error, attempt, MAX_RECOVERY_ATTEMPTS,
            step.success_criteria,
        );

        let request = ChatCompletionRequest {
            model: ai_model.to_string(),
            messages: vec![
                ChatMessage {
                    role: "system".into(),
                    content: Some(
                        "You are a workflow recovery agent. A step failed. Pick a different tool \
                         or different arguments to accomplish the same goal. Call exactly one tool."
                            .to_string(),
                    ),
                    tool_calls: None,
                    tool_call_id: None,
                },
                ChatMessage {
                    role: "user".into(),
                    content: Some(user_msg),
                    tool_calls: None,
                    tool_call_id: None,
                },
            ],
            temperature: Some(0.3),
            max_tokens: Some(1024),
            stream: None,
            tools: Some(step_tools),
            tool_choice: None,
        };

        let resp = match client::chat_completion(ai_config, request).await {
            Ok(r) => r,
            Err(e) => {
                warn!(step_id = %step.id, attempt, "recovery AI call failed: {e}");
                continue;
            }
        };

        let choice = match resp.choices.first() {
            Some(c) => c,
            None => continue,
        };

        if let Some(ref tool_calls) = choice.message.tool_calls {
            if let Some(tc) = tool_calls.first() {
                let (agent_str, action) = registry::parse_tool_name(&tc.function.name);

                let agent_type_enum = match agent_str {
                    "gitlab" => AgentType::Gitlab,
                    "ai" => AgentType::Ai,
                    "http" => AgentType::Http,
                    "script" => AgentType::Script,
                    "sandbox" => AgentType::Sandbox,
                    "coding" => AgentType::Coding,
                    _ => step.agent_type.clone(),
                };

                let agent = match factory::create_agent(&agent_type_enum, agent_config, 0).await {
                    Some(a) => a,
                    None => continue,
                };

                let args: serde_json::Value = serde_json::from_str(&tc.function.arguments)
                    .unwrap_or_else(|_| serde_json::json!({}));

                let mut inputs = HashMap::new();
                if let Some(obj) = args.as_object() {
                    for (k, v) in obj {
                        inputs.insert(k.clone(), v.clone());
                    }
                }

                // Merge session context.
                if let Some(ctx) = session_context {
                    if let Some(obj) = ctx.as_object() {
                        for key in &["project_path", "project_id", "branch", "mr_iid"] {
                            if !inputs.contains_key(*key) {
                                if let Some(val) = obj.get(*key) {
                                    if !val.is_null() {
                                        inputs.insert(key.to_string(), val.clone());
                                    }
                                }
                            }
                        }
                    }
                }

                let final_action = if agent_str == "http" && action == "request" {
                    inputs.get("method").and_then(|v| v.as_str()).unwrap_or("get").to_string()
                } else {
                    action.to_string()
                };

                let result = agent.execute(&final_action, inputs, mentor).await;

                if result.status == AgentStatus::Success || result.status == AgentStatus::Partial {
                    let files_changed = result.output
                        .get("files_changed")
                        .and_then(|v| serde_json::from_value::<Vec<String>>(v.clone()).ok())
                        .unwrap_or_default();

                    info!(
                        step_id = %step.id,
                        attempt,
                        recovery_tool = %tc.function.name,
                        "adaptive recovery succeeded"
                    );

                    return Some(GeneratorOutcome::Success {
                        output: result.output,
                        files_changed,
                    });
                }

                warn!(
                    step_id = %step.id,
                    attempt,
                    recovery_tool = %tc.function.name,
                    "recovery attempt failed"
                );
            }
        }
    }

    None
}

// ---------------------------------------------------------------------------
// Legacy action resolution (fallback for steps without tool field)
// ---------------------------------------------------------------------------

/// Resolve a concrete agent action name from a step's NL description.
/// This is the legacy fallback used when no tool catalog is available
/// or when AI tool selection fails.
fn resolve_agent_action_legacy(agent_type: &crate::types::workflow::AgentType, description: &str, step_id: &str) -> String {
    use crate::types::workflow::AgentType;

    let desc_lower = description.to_lowercase();

    match agent_type {
        AgentType::Gitlab => {
            // Check for "list" or "all" + MR keywords → list_open_mrs
            if (desc_lower.contains("list") || desc_lower.contains("all") || desc_lower.contains("open merge request"))
                && (desc_lower.contains("merge request") || desc_lower.contains("mr"))
            {
                "list_open_mrs".into()
            } else if desc_lower.contains("fetch") && (desc_lower.contains("merge request") || desc_lower.contains("mr")) && desc_lower.contains("change") {
                "fetch_mr_changes".into()
            } else if desc_lower.contains("fetch") && (desc_lower.contains("merge request") || desc_lower.contains("mr")) && !desc_lower.contains("all") {
                "fetch_mr".into()
            } else if desc_lower.contains("comment") || desc_lower.contains("post") || desc_lower.contains("note") {
                "post_comment".into()
            } else if desc_lower.contains("pipeline") {
                "fetch_pipelines".into()
            } else if desc_lower.contains("file") && (desc_lower.contains("fetch") || desc_lower.contains("read") || desc_lower.contains("get")) {
                "fetch_file".into()
            } else if desc_lower.contains("retrieve") && (desc_lower.contains("merge request") || desc_lower.contains("mr")) {
                "list_open_mrs".into()
            } else {
                // Fallback: use the step_id as a hint, or default to a generic action
                warn!(
                    step_id,
                    description,
                    "generator: could not resolve gitlab action from description, using step_id"
                );
                step_id.replace('-', "_")
            }
        }
        AgentType::Ai => {
            if desc_lower.contains("summarize") || desc_lower.contains("summary") {
                "summarize".into()
            } else if desc_lower.contains("analyze") || desc_lower.contains("analysis") || desc_lower.contains("rank") || desc_lower.contains("prioritize") {
                "analyze".into()
            } else if desc_lower.contains("decide") || desc_lower.contains("decision") || desc_lower.contains("choose") {
                "decide".into()
            } else {
                // Default: use chat for general AI tasks
                "chat".into()
            }
        }
        AgentType::Http => {
            // HTTP agent uses the method as the action (GET, POST, etc.)
            if desc_lower.contains("post") || desc_lower.contains("send") || desc_lower.contains("create") {
                "POST".into()
            } else {
                "GET".into()
            }
        }
        AgentType::Script => {
            // Script agent uses the description as the command
            description.to_string()
        }
        AgentType::Sandbox | AgentType::Coding => {
            // Sandbox/Coding agents use the description as the task
            description.to_string()
        }
        AgentType::Composite => {
            description.to_string()
        }
    }
}

// ---------------------------------------------------------------------------
// Result interpretation
// ---------------------------------------------------------------------------

/// Convert a raw `AgentResult` into a structured `GeneratorOutcome`.
///
/// For simple success/failure this is straightforward. For complex outputs
/// (plan modifications, capability requests, human escalation) we check
/// for sentinel fields in the output JSON. If the agent output contains
/// structured signals, we honour them; otherwise we fall back to the
/// status code.
async fn interpret_result(
    result: crate::types::workflow::AgentResult,
    step: &PlanStep,
    ai_config: &AiClientConfig,
    ai_model: &str,
) -> Result<GeneratorOutcome> {
    let output = &result.output;

    // Check for structured signals in the output before falling back to status.

    // 1. Plan modification requested?
    if let Some(plan_mod) = output.get("plan_modification") {
        let add_steps = plan_mod
            .get("add_steps")
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default();
        let remove_step_ids = plan_mod
            .get("remove_step_ids")
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default();
        let reason = plan_mod
            .get("reason")
            .and_then(|v| v.as_str())
            .unwrap_or("agent requested plan modification")
            .to_string();

        return Ok(GeneratorOutcome::PlanModification {
            output: strip_signals(output),
            add_steps,
            remove_step_ids,
            reason,
        });
    }

    // 2. Needs a capability the system doesn't have?
    if let Some(needs_cap) = output.get("needs_capability") {
        let capability = needs_cap
            .get("capability")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string();
        let description = needs_cap
            .get("description")
            .and_then(|v| v.as_str())
            .unwrap_or("agent needs an unavailable capability")
            .to_string();

        return Ok(GeneratorOutcome::NeedsCapability {
            capability,
            description,
        });
    }

    // 3. Needs human intervention?
    if let Some(needs_human) = output.get("needs_human") {
        let reason = needs_human
            .get("reason")
            .and_then(|v| v.as_str())
            .unwrap_or("agent needs human input")
            .to_string();
        let what_i_need = needs_human
            .get("what_i_need")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let options: Vec<EscalationOption> = needs_human
            .get("options")
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default();

        return Ok(GeneratorOutcome::NeedsHuman {
            reason,
            what_i_need,
            options,
        });
    }

    // 4. Fall back to status-based interpretation.
    match result.status {
        AgentStatus::Success | AgentStatus::Partial => {
            let files_changed = output
                .get("files_changed")
                .and_then(|v| serde_json::from_value::<Vec<String>>(v.clone()).ok())
                .unwrap_or_default();

            Ok(GeneratorOutcome::Success {
                output: result.output,
                files_changed,
            })
        }
        AgentStatus::Failure => {
            let error = output
                .get("error")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| {
                    format!("step '{}' failed without error details", step.id)
                });

            // Use AI to check if this failure implies a missing capability
            // or need for human help, but only if the error message is
            // substantial enough to analyze.
            if error.len() > 20 {
                if let Some(outcome) =
                    try_classify_failure(ai_config, ai_model, step, &error).await
                {
                    return Ok(outcome);
                }
            }

            Ok(GeneratorOutcome::Failure { error })
        }
    }
}

/// Strip internal signal fields from the output so downstream consumers
/// get clean data.
fn strip_signals(output: &serde_json::Value) -> serde_json::Value {
    if let Some(obj) = output.as_object() {
        let cleaned: serde_json::Map<String, serde_json::Value> = obj
            .iter()
            .filter(|(k, _)| {
                !matches!(
                    k.as_str(),
                    "plan_modification" | "needs_capability" | "needs_human"
                )
            })
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        serde_json::Value::Object(cleaned)
    } else {
        output.clone()
    }
}

/// Ask the AI to classify a failure — is it a missing capability, a need
/// for human help, or just a plain failure? Returns `None` if classification
/// fails or the AI says it's a plain failure.
async fn try_classify_failure(
    ai_config: &AiClientConfig,
    ai_model: &str,
    step: &PlanStep,
    error: &str,
) -> Option<GeneratorOutcome> {
    let request = ChatCompletionRequest {
        model: ai_model.to_string(),
        messages: vec![
            ChatMessage {
                role: "system".into(),
                content: Some(
                    "You classify workflow step failures. Respond with ONLY one of:\n\
                     - {\"type\":\"failure\"} — a normal failure, retry may help\n\
                     - {\"type\":\"needs_capability\",\"capability\":\"...\",\"description\":\"...\"} — missing integration\n\
                     - {\"type\":\"needs_human\",\"reason\":\"...\",\"what_i_need\":\"...\"} — needs human decision\n\
                     Respond with only the JSON object, nothing else."
                        .to_string(),
                ),
                tool_calls: None,
                tool_call_id: None,
            },
            ChatMessage {
                role: "user".into(),
                content: Some(format!(
                    "Step: {}\nDescription: {}\nError: {}",
                    step.id, step.description, error
                )),
                tool_calls: None,
                tool_call_id: None,
            },
        ],
        temperature: Some(0.0),
        max_tokens: Some(256),
        stream: None,
        tools: None,
        tool_choice: None,
    };

    let resp = match client::chat_completion(ai_config, request).await {
        Ok(r) => r,
        Err(e) => {
            debug!("failure classification AI call failed: {e}");
            return None;
        }
    };

    let text = resp
        .choices
        .first()
        .and_then(|c| c.message.content.as_deref())?;

    let parsed: serde_json::Value = serde_json::from_str(text.trim()).ok()?;

    match parsed.get("type")?.as_str()? {
        "needs_capability" => Some(GeneratorOutcome::NeedsCapability {
            capability: parsed
                .get("capability")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
                .to_string(),
            description: parsed
                .get("description")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
        }),
        "needs_human" => Some(GeneratorOutcome::NeedsHuman {
            reason: parsed
                .get("reason")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            what_i_need: parsed
                .get("what_i_need")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            options: vec![],
        }),
        _ => None, // "failure" or unrecognized → let caller use plain Failure
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn outcome_type_name(outcome: &GeneratorOutcome) -> &'static str {
    match outcome {
        GeneratorOutcome::Success { .. } => "success",
        GeneratorOutcome::Failure { .. } => "failure",
        GeneratorOutcome::NeedsCapability { .. } => "needs_capability",
        GeneratorOutcome::NeedsHuman { .. } => "needs_human",
        GeneratorOutcome::PlanModification { .. } => "plan_modification",
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::workflow::AgentType;

    fn test_step(id: &str) -> PlanStep {
        PlanStep {
            id: id.into(),
            description: "do something".into(),
            agent_type: AgentType::Ai,
            success_criteria: "it worked".into(),
            depends_on: vec![],
            capabilities_needed: vec![],
            tool: None,
        }
    }

    // -- gather_dependency_outputs -------------------------------------------

    #[test]
    fn gather_deps_collects_matching() {
        let step = PlanStep {
            depends_on: vec!["a".into(), "b".into()],
            ..test_step("s1")
        };
        let mut outputs = HashMap::new();
        outputs.insert("a".into(), serde_json::json!({"x": 1}));
        outputs.insert("b".into(), serde_json::json!({"y": 2}));
        outputs.insert("c".into(), serde_json::json!({"z": 3}));

        let deps = gather_dependency_outputs(&step, &outputs);
        assert_eq!(deps.len(), 2);
        assert!(deps.contains_key("a"));
        assert!(deps.contains_key("b"));
        assert!(!deps.contains_key("c"));
    }

    #[test]
    fn gather_deps_missing_is_ok() {
        let step = PlanStep {
            depends_on: vec!["missing".into()],
            ..test_step("s1")
        };
        let outputs = HashMap::new();
        let deps = gather_dependency_outputs(&step, &outputs);
        assert!(deps.is_empty());
    }

    // -- build_step_inputs ---------------------------------------------------

    #[test]
    fn build_inputs_includes_metadata() {
        let step = test_step("s1");
        let inputs = build_step_inputs(&step, &HashMap::new(), "", None, None);
        assert_eq!(inputs.get("step_id").unwrap(), "s1");
        assert!(inputs.get("description").is_some());
        assert!(inputs.get("success_criteria").is_some());
        assert!(!inputs.contains_key("dependency_outputs"));
        assert!(!inputs.contains_key("mentor_context"));
        assert!(!inputs.contains_key("evaluator_feedback"));
    }

    #[test]
    fn build_inputs_includes_deps_and_mentor() {
        let step = test_step("s1");
        let mut deps = HashMap::new();
        deps.insert("a".into(), serde_json::json!(1));
        let inputs = build_step_inputs(&step, &deps, "some knowledge", None, None);
        assert!(inputs.contains_key("dependency_outputs"));
        assert!(inputs.contains_key("mentor_context"));
    }

    #[test]
    fn build_inputs_includes_evaluator_feedback() {
        let step = test_step("s1");
        let feedback = EvaluatorVerdict {
            passed: false,
            score: 0.4,
            threshold: 0.8,
            feedback: "not good enough".into(),
            suggestion: Some("try harder".into()),
        };
        let inputs = build_step_inputs(&step, &HashMap::new(), "", Some(&feedback), None);
        let fb = inputs.get("evaluator_feedback").unwrap();
        assert_eq!(fb.get("passed").unwrap(), false);
        assert_eq!(fb.get("feedback").unwrap(), "not good enough");
    }

    // -- strip_signals -------------------------------------------------------

    #[test]
    fn strip_signals_removes_internal_fields() {
        let output = serde_json::json!({
            "result": "ok",
            "plan_modification": {"reason": "test"},
            "needs_capability": {"capability": "x"},
            "needs_human": {"reason": "y"},
        });
        let cleaned = strip_signals(&output);
        assert!(cleaned.get("result").is_some());
        assert!(cleaned.get("plan_modification").is_none());
        assert!(cleaned.get("needs_capability").is_none());
        assert!(cleaned.get("needs_human").is_none());
    }

    #[test]
    fn strip_signals_non_object_passthrough() {
        let output = serde_json::json!("just a string");
        assert_eq!(strip_signals(&output), output);
    }

    // -- outcome_type_name ---------------------------------------------------

    #[test]
    fn outcome_names() {
        assert_eq!(
            outcome_type_name(&GeneratorOutcome::Success {
                output: serde_json::json!(null),
                files_changed: vec![],
            }),
            "success"
        );
        assert_eq!(
            outcome_type_name(&GeneratorOutcome::Failure {
                error: "x".into(),
            }),
            "failure"
        );
        assert_eq!(
            outcome_type_name(&GeneratorOutcome::NeedsCapability {
                capability: "x".into(),
                description: "y".into(),
            }),
            "needs_capability"
        );
        assert_eq!(
            outcome_type_name(&GeneratorOutcome::NeedsHuman {
                reason: "x".into(),
                what_i_need: "y".into(),
                options: vec![],
            }),
            "needs_human"
        );
        assert_eq!(
            outcome_type_name(&GeneratorOutcome::PlanModification {
                output: serde_json::json!(null),
                add_steps: vec![],
                remove_step_ids: vec![],
                reason: "x".into(),
            }),
            "plan_modification"
        );
    }
}
