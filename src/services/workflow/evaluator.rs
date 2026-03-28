// ---------------------------------------------------------------------------
// Evaluator agent — independently verifies each step's output against its
// success criteria.
//
// The Evaluator is structurally separated from the Generator: it never sees
// Generator reasoning, prior conversation, or mentor context. It receives
// only the step spec, success criteria, and the step's output. This prevents
// self-evaluation bias.
//
// The Session Manager calls `evaluate_step()` after each Generator execution.
// If the verdict fails, the Session Manager can retry the Generator with the
// Evaluator's feedback attached.
// ---------------------------------------------------------------------------

use anyhow::{bail, Context, Result};
use tracing::{debug, info, warn};

use crate::services::ai::client::{
    self, AiClientConfig, ChatCompletionRequest, ChatMessage,
};
use crate::types::workflow::{EvaluatorVerdict, PlanStep};

/// Default pass threshold — steps scoring below this fail evaluation.
const DEFAULT_THRESHOLD: f64 = 0.8;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Evaluate a single step's output against its success criteria.
///
/// Context is intentionally minimal: step spec + output + criteria. No
/// Generator reasoning, no mentor knowledge, no conversation history.
pub async fn evaluate_step(
    ai_config: &AiClientConfig,
    ai_model: &str,
    step: &PlanStep,
    step_output: &serde_json::Value,
    threshold: Option<f64>,
) -> Result<EvaluatorVerdict> {
    let threshold = threshold.unwrap_or(DEFAULT_THRESHOLD);

    info!(
        step_id = %step.id,
        threshold,
        "evaluator: checking step output"
    );

    let system_prompt = build_system_prompt(threshold);
    let user_prompt = build_user_prompt(step, step_output);

    let response = call_ai(ai_config, ai_model, &system_prompt, &user_prompt).await?;
    debug!(
        step_id = %step.id,
        response_len = response.len(),
        "evaluator AI response received"
    );

    let verdict = parse_verdict(&response, threshold)
        .context("failed to parse evaluator response")?;

    info!(
        step_id = %step.id,
        passed = verdict.passed,
        score = verdict.score,
        "evaluator: verdict rendered"
    );

    Ok(verdict)
}

/// Evaluate the overall session outcome against the plan's goal.
///
/// Called once after all steps complete. Checks whether the combined outputs
/// actually achieve what the plan set out to do.
pub async fn evaluate_session(
    ai_config: &AiClientConfig,
    ai_model: &str,
    goal: &str,
    step_summaries: &[(String, serde_json::Value)], // (step_id, output)
    threshold: Option<f64>,
) -> Result<EvaluatorVerdict> {
    let threshold = threshold.unwrap_or(DEFAULT_THRESHOLD);

    info!(
        step_count = step_summaries.len(),
        threshold,
        "evaluator: checking overall session outcome"
    );

    let system_prompt = build_session_system_prompt(threshold);
    let user_prompt = build_session_user_prompt(goal, step_summaries);

    let response = call_ai(ai_config, ai_model, &system_prompt, &user_prompt).await?;
    debug!(
        response_len = response.len(),
        "evaluator session AI response received"
    );

    let verdict = parse_verdict(&response, threshold)
        .context("failed to parse session evaluator response")?;

    info!(
        passed = verdict.passed,
        score = verdict.score,
        "evaluator: session verdict rendered"
    );

    Ok(verdict)
}

// ---------------------------------------------------------------------------
// AI interaction
// ---------------------------------------------------------------------------

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
        temperature: Some(0.1), // Low temperature for consistent evaluation
        max_tokens: Some(1024),
        stream: None,
        tools: None,
        tool_choice: None,
    };

    let resp = client::chat_completion(cfg, request)
        .await
        .context("evaluator AI call failed")?;

    let content = resp
        .choices
        .first()
        .and_then(|c| c.message.content.as_deref())
        .unwrap_or("")
        .to_string();

    if content.is_empty() {
        bail!("evaluator AI returned empty response");
    }

    Ok(content)
}

// ---------------------------------------------------------------------------
// Prompt construction
// ---------------------------------------------------------------------------

fn build_system_prompt(threshold: f64) -> String {
    format!(
        r#"You are an independent evaluator. You verify whether a workflow step's output meets its success criteria.

You have NO knowledge of how the output was produced. Judge only by the result.

Respond with ONLY a JSON object (no markdown, no explanation):
{{
  "passed": true/false,
  "score": 0.0 to 1.0,
  "feedback": "brief explanation of your assessment",
  "suggestion": "if failed, a specific actionable suggestion for improvement; null if passed"
}}

Scoring guide:
- 1.0: Fully meets all success criteria with high quality
- 0.8: Meets criteria adequately
- 0.6: Partially meets criteria, notable gaps
- 0.4: Significant issues, major criteria unmet
- 0.2: Barely relevant to the criteria
- 0.0: Completely fails or is empty/nonsensical

The pass threshold is {threshold}. Set "passed" to true only if score >= {threshold}.

Be strict but fair. Do not give the benefit of the doubt — if the output doesn't clearly demonstrate meeting a criterion, score it down."#
    )
}

fn build_user_prompt(step: &PlanStep, step_output: &serde_json::Value) -> String {
    let output_str = serde_json::to_string_pretty(step_output)
        .unwrap_or_else(|_| step_output.to_string());

    format!(
        "Step ID: {}\n\
         Step description: {}\n\
         Success criteria: {}\n\n\
         Step output:\n```json\n{}\n```",
        step.id, step.description, step.success_criteria, output_str
    )
}

fn build_session_system_prompt(threshold: f64) -> String {
    format!(
        r#"You are an independent evaluator. You verify whether a workflow's combined outputs achieve its stated goal.

You have NO knowledge of how the outputs were produced. Judge only by the results.

Respond with ONLY a JSON object (no markdown, no explanation):
{{
  "passed": true/false,
  "score": 0.0 to 1.0,
  "feedback": "brief explanation of your assessment",
  "suggestion": "if failed, what's missing or needs fixing; null if passed"
}}

The pass threshold is {threshold}. Set "passed" to true only if score >= {threshold}.

Be strict but fair. The goal must be demonstrably achieved by the combined outputs."#
    )
}

fn build_session_user_prompt(
    goal: &str,
    step_summaries: &[(String, serde_json::Value)],
) -> String {
    let mut prompt = format!("Goal: {goal}\n\nStep outputs:\n");

    for (step_id, output) in step_summaries {
        let output_str = serde_json::to_string_pretty(output)
            .unwrap_or_else(|_| output.to_string());
        prompt.push_str(&format!("\n--- {step_id} ---\n{output_str}\n"));
    }

    prompt
}

// ---------------------------------------------------------------------------
// Response parsing — fault-tolerant
// ---------------------------------------------------------------------------

/// Parse an AI response into an EvaluatorVerdict. Tries direct parse, then
/// code fence extraction, then brace extraction.
fn parse_verdict(response: &str, threshold: f64) -> Result<EvaluatorVerdict> {
    let trimmed = response.trim();

    // Attempt 1: direct parse.
    if let Ok(mut v) = serde_json::from_str::<RawVerdict>(trimmed) {
        return Ok(v.into_verdict(threshold));
    }

    // Attempt 2: code fence extraction.
    if let Some(json) = extract_fenced_json(trimmed) {
        if let Ok(mut v) = serde_json::from_str::<RawVerdict>(&json) {
            return Ok(v.into_verdict(threshold));
        }
        warn!("found fenced JSON but it didn't parse as verdict");
    }

    // Attempt 3: brace extraction.
    if let Some(json) = extract_brace_block(trimmed) {
        if let Ok(mut v) = serde_json::from_str::<RawVerdict>(&json) {
            return Ok(v.into_verdict(threshold));
        }
        warn!("found brace block but it didn't parse as verdict");
    }

    bail!(
        "could not extract a valid EvaluatorVerdict from AI response (len={})",
        response.len()
    )
}

/// Intermediate struct for lenient parsing — the AI might omit fields or
/// use slightly different types.
#[derive(serde::Deserialize)]
struct RawVerdict {
    passed: Option<bool>,
    score: Option<f64>,
    feedback: Option<String>,
    suggestion: Option<String>,
}

impl RawVerdict {
    fn into_verdict(&mut self, threshold: f64) -> EvaluatorVerdict {
        let score = self.score.unwrap_or(0.0).clamp(0.0, 1.0);
        // If the AI didn't set `passed`, derive it from score vs threshold.
        let passed = self.passed.unwrap_or(score >= threshold);

        EvaluatorVerdict {
            passed,
            score,
            threshold,
            feedback: self
                .feedback
                .take()
                .unwrap_or_else(|| "no feedback provided".into()),
            suggestion: self.suggestion.take(),
        }
    }
}

// ---------------------------------------------------------------------------
// JSON extraction helpers (shared pattern with planner)
// ---------------------------------------------------------------------------

fn extract_fenced_json(text: &str) -> Option<String> {
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
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::workflow::AgentType;

    fn test_step() -> PlanStep {
        PlanStep {
            id: "test-step".into(),
            description: "do the thing".into(),
            agent_type: AgentType::Ai,
            success_criteria: "thing is done correctly".into(),
            depends_on: vec![],
            capabilities_needed: vec![],
        }
    }

    // -- parse_verdict -------------------------------------------------------

    #[test]
    fn parse_verdict_direct() {
        let json = r#"{"passed":true,"score":0.9,"feedback":"looks good","suggestion":null}"#;
        let v = parse_verdict(json, 0.8).unwrap();
        assert!(v.passed);
        assert!((v.score - 0.9).abs() < f64::EPSILON);
        assert_eq!(v.feedback, "looks good");
        assert!(v.suggestion.is_none());
    }

    #[test]
    fn parse_verdict_fenced() {
        let text = "Here's my evaluation:\n```json\n{\"passed\":false,\"score\":0.3,\"feedback\":\"bad\",\"suggestion\":\"fix it\"}\n```";
        let v = parse_verdict(text, 0.8).unwrap();
        assert!(!v.passed);
        assert_eq!(v.suggestion.as_deref(), Some("fix it"));
    }

    #[test]
    fn parse_verdict_brace_extraction() {
        let text = "I think {\"passed\":true,\"score\":0.85,\"feedback\":\"ok\"} is my answer.";
        let v = parse_verdict(text, 0.8).unwrap();
        assert!(v.passed);
    }

    #[test]
    fn parse_verdict_garbage_fails() {
        assert!(parse_verdict("no json here", 0.8).is_err());
    }

    #[test]
    fn parse_verdict_missing_fields_uses_defaults() {
        let json = r#"{"score":0.5}"#;
        let v = parse_verdict(json, 0.8).unwrap();
        // passed should be derived: 0.5 < 0.8 → false
        assert!(!v.passed);
        assert_eq!(v.feedback, "no feedback provided");
        assert!(v.suggestion.is_none());
    }

    #[test]
    fn parse_verdict_score_clamped() {
        let json = r#"{"score":1.5,"feedback":"over"}"#;
        let v = parse_verdict(json, 0.8).unwrap();
        assert!((v.score - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn parse_verdict_derives_passed_from_score() {
        // No explicit `passed` field — should derive from score vs threshold.
        let json = r#"{"score":0.9,"feedback":"good"}"#;
        let v = parse_verdict(json, 0.8).unwrap();
        assert!(v.passed);

        let json = r#"{"score":0.7,"feedback":"meh"}"#;
        let v = parse_verdict(json, 0.8).unwrap();
        assert!(!v.passed);
    }

    // -- prompt construction -------------------------------------------------

    #[test]
    fn user_prompt_contains_step_info() {
        let step = test_step();
        let output = serde_json::json!({"result": "done"});
        let prompt = build_user_prompt(&step, &output);
        assert!(prompt.contains("test-step"));
        assert!(prompt.contains("do the thing"));
        assert!(prompt.contains("thing is done correctly"));
        assert!(prompt.contains("\"result\""));
    }

    #[test]
    fn session_user_prompt_contains_goal_and_steps() {
        let summaries = vec![
            ("step-1".into(), serde_json::json!({"a": 1})),
            ("step-2".into(), serde_json::json!({"b": 2})),
        ];
        let prompt = build_session_user_prompt("fix the bug", &summaries);
        assert!(prompt.contains("fix the bug"));
        assert!(prompt.contains("step-1"));
        assert!(prompt.contains("step-2"));
    }

    #[test]
    fn system_prompt_contains_threshold() {
        let prompt = build_system_prompt(0.75);
        assert!(prompt.contains("0.75"));
    }

    // -- RawVerdict ----------------------------------------------------------

    #[test]
    fn raw_verdict_all_none() {
        let mut raw = RawVerdict {
            passed: None,
            score: None,
            feedback: None,
            suggestion: None,
        };
        let v = raw.into_verdict(0.8);
        assert!(!v.passed); // score 0.0 < 0.8
        assert!((v.score - 0.0).abs() < f64::EPSILON);
        assert_eq!(v.threshold, 0.8);
        assert_eq!(v.feedback, "no feedback provided");
    }
}
