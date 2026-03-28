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
    self, AiClientConfig, ChatCompletionRequest, ChatMessage,
};
use crate::services::mentor::client::MentorClient;
use crate::services::workflow::factory::{self, AgentFactoryConfig};
use crate::types::workflow::{
    AgentStatus, EscalationOption, EvaluatorVerdict, GeneratorOutcome, PlanStep,
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
pub async fn execute_step(
    step: &PlanStep,
    step_outputs: &HashMap<String, serde_json::Value>,
    mentor: &MentorClient,
    agent_config: &AgentFactoryConfig,
    ai_config: &AiClientConfig,
    ai_model: &str,
    evaluator_feedback: Option<&EvaluatorVerdict>,
    session_context: Option<&serde_json::Value>,
) -> Result<GeneratorOutcome> {
    info!(
        step_id = %step.id,
        agent_type = %step.agent_type,
        "generator: executing step"
    );

    // 1. Gather dependency outputs for this step.
    let dep_outputs = gather_dependency_outputs(step, step_outputs);

    // 2. Query mentor for step-relevant knowledge.
    let mentor_context = query_mentor_for_step(mentor, step).await;

    // 3. Build the action inputs by combining step description, dep outputs,
    //    mentor context, and evaluator feedback into a single inputs map.
    let inputs = build_step_inputs(step, &dep_outputs, &mentor_context, evaluator_feedback, session_context);

    // 4. Create the agent for this step's type.
    let agent = factory::create_agent(&step.agent_type, agent_config, 0)
        .await
        .ok_or_else(|| {
            anyhow::anyhow!(
                "no agent available for type '{}' — missing configuration?",
                step.agent_type
            )
        })?;

    // 5. Execute.
    debug!(step_id = %step.id, "dispatching to {} agent", agent.agent_type_name());
    let result = agent.execute(&step.description, inputs, mentor).await;

    debug!(
        step_id = %step.id,
        status = ?result.status,
        duration = result.duration_secs,
        "agent execution complete"
    );

    // 6. Interpret the raw AgentResult into a GeneratorOutcome.
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

    // Dependency outputs.
    if !dep_outputs.is_empty() {
        inputs.insert(
            "dependency_outputs".into(),
            serde_json::to_value(dep_outputs).unwrap_or_default(),
        );
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
