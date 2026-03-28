// ---------------------------------------------------------------------------
// AI Final Verification — compares workflow outputs against the original
// natural language intent to determine if the workflow accomplished its goal.
//
// Called by the orchestrator after all steps complete. Uses the AI service
// to evaluate whether the collected outputs satisfy the original description.
// ---------------------------------------------------------------------------

use tracing::{debug, warn};

use crate::services::ai::client::{
    self as ai, AiClientConfig, ChatCompletionRequest, ChatMessage,
};
use crate::types::workflow::{DeliverableStatus, VerificationResult, WorkflowRun};

/// Truncate a string to at most `max_bytes` bytes without splitting a
/// multi-byte UTF-8 character. Returns the full string if it's already
/// within the limit.
fn safe_truncate(s: &str, max_bytes: usize) -> &str {
    if s.len() <= max_bytes {
        return s;
    }
    let mut end = max_bytes;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

/// Verify that a workflow run's outputs match the original intent.
pub async fn verify_run(
    ai_config: &AiClientConfig,
    model: &str,
    original_description: &str,
    run: &WorkflowRun,
) -> VerificationResult {
    debug!(run_id = %run.id, "final verification: starting");

    // Collect step outputs for context.
    let mut step_summaries = Vec::new();
    for (step_id, state) in &run.step_states {
        let summary = match state {
            crate::types::workflow::StepState::Completed { output, duration_secs } => {
                // Truncate large outputs to keep the prompt manageable.
                let output_str = serde_json::to_string(output).unwrap_or_default();
                let truncated = if output_str.len() > 2000 {
                    format!("{}...(truncated)", safe_truncate(&output_str, 2000))
                } else {
                    output_str
                };
                format!("- {step_id}: COMPLETED ({duration_secs:.1}s) — {truncated}")
            }
            crate::types::workflow::StepState::Failed { error, retries, .. } => {
                format!("- {step_id}: FAILED (retries: {retries}) — {error}")
            }
            crate::types::workflow::StepState::Skipped { reason } => {
                format!("- {step_id}: SKIPPED — {reason}")
            }
            _ => format!("- {step_id}: {state:?}"),
        };
        step_summaries.push(summary);
    }

    let steps_text = step_summaries.join("\n");

    let request = ChatCompletionRequest {
        model: model.to_string(),
        messages: vec![
            ChatMessage {
                role: "system".into(),
                content: Some(VERIFICATION_SYSTEM_PROMPT.to_string()),
                tool_calls: None,
                tool_call_id: None,
            },
            ChatMessage {
                role: "user".into(),
                content: Some(format!(
                    "## Original Work Order\n{original_description}\n\n## Step Results\n{steps_text}\n\n## Run Status\n{status}",
                    status = run.status,
                )),
                tool_calls: None,
                tool_call_id: None,
            },
        ],
        temperature: Some(0.1),
        max_tokens: Some(2048),
        stream: None,
        tools: None,
        tool_choice: None,
    };

    match ai::chat_completion(ai_config, request).await {
        Ok(resp) => {
            let raw = resp
                .choices
                .first()
                .and_then(|c| c.message.content.clone())
                .unwrap_or_default();

            parse_verification(&raw)
        }
        Err(e) => {
            warn!(error = %e, "final verification: AI call failed");
            VerificationResult {
                passed: false,
                summary: format!("Verification failed: {e}"),
                deliverables: Vec::new(),
            }
        }
    }
}

/// Parse the AI's verification response into a VerificationResult.
fn parse_verification(raw: &str) -> VerificationResult {
    // Try to extract JSON from the response.
    let json_str = extract_json(raw);

    if let Some(json_str) = json_str {
        if let Ok(parsed) = serde_json::from_str::<VerificationResponse>(&json_str) {
            return VerificationResult {
                passed: parsed.passed,
                summary: parsed.summary,
                deliverables: parsed
                    .deliverables
                    .into_iter()
                    .map(|d| DeliverableStatus {
                        description: d.description,
                        met: d.met,
                        evidence: d.evidence,
                    })
                    .collect(),
            };
        }
    }

    // Fallback: treat the raw text as the summary.
    // Fix #7: use whole-word matching instead of substring to avoid false
    // positives like "password", "bypass", "compass", etc.
    let lower = raw.to_lowercase();
    let words: Vec<&str> = lower.split_whitespace()
        .map(|w| w.trim_matches(|c: char| !c.is_alphanumeric()))
        .collect();
    let pass_words = ["pass", "passed", "success", "successful", "accomplished", "completed"];
    let passed = words.iter().any(|w| pass_words.contains(w));

    VerificationResult {
        passed,
        summary: raw.to_string(),
        deliverables: Vec::new(),
    }
}

fn extract_json(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.starts_with('{') {
        return Some(trimmed.to_string());
    }
    if let Some(start) = trimmed.find("```json") {
        let after = &trimmed[start + 7..];
        if let Some(end) = after.find("```") {
            return Some(after[..end].trim().to_string());
        }
    }
    if let (Some(start), Some(end)) = (trimmed.find('{'), trimmed.rfind('}')) {
        if start < end {
            return Some(trimmed[start..=end].to_string());
        }
    }
    None
}

// ---------------------------------------------------------------------------
// AI response schema
// ---------------------------------------------------------------------------

#[derive(serde::Deserialize)]
struct VerificationResponse {
    passed: bool,
    summary: String,
    #[serde(default)]
    deliverables: Vec<DeliverableResponse>,
}

#[derive(serde::Deserialize)]
struct DeliverableResponse {
    description: String,
    met: bool,
    evidence: String,
}

// ---------------------------------------------------------------------------
// System prompt
// ---------------------------------------------------------------------------

const VERIFICATION_SYSTEM_PROMPT: &str = r#"You are a workflow verification engine. Given the original work order (natural language description) and the results of each step, determine whether the workflow accomplished its goal.

Output ONLY valid JSON with this schema:
{
  "passed": true/false,
  "summary": "1-2 sentence summary of what was accomplished or what failed",
  "deliverables": [
    {
      "description": "what was expected",
      "met": true/false,
      "evidence": "brief evidence from step outputs"
    }
  ]
}

Rules:
- Be strict: if any critical deliverable is not met, passed should be false.
- Extract specific deliverables from the work order description.
- Reference actual step outputs as evidence.
- If steps failed or were skipped, explain the impact on the overall goal.
- Do NOT include any text outside the JSON object."#;

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_verification_json() {
        let raw = r#"{"passed": true, "summary": "All tasks completed.", "deliverables": [{"description": "fetch MRs", "met": true, "evidence": "returned 5 MRs"}]}"#;
        let result = parse_verification(raw);
        assert!(result.passed);
        assert_eq!(result.summary, "All tasks completed.");
        assert_eq!(result.deliverables.len(), 1);
        assert!(result.deliverables[0].met);
    }

    #[test]
    fn parse_verification_fenced() {
        let raw = "Here's the result:\n```json\n{\"passed\": false, \"summary\": \"Step 2 failed.\", \"deliverables\": []}\n```";
        let result = parse_verification(raw);
        assert!(!result.passed);
        assert_eq!(result.summary, "Step 2 failed.");
    }

    #[test]
    fn parse_verification_fallback() {
        let raw = "The workflow was successful and all goals were accomplished.";
        let result = parse_verification(raw);
        assert!(result.passed); // contains "accomplished"
        assert!(result.deliverables.is_empty());
    }

    #[test]
    fn parse_verification_fallback_failure() {
        let raw = "The workflow did not complete due to errors.";
        let result = parse_verification(raw);
        assert!(!result.passed);
    }

    #[test]
    fn parse_verification_fallback_no_false_positive_on_password() {
        // Fix #7: "password" should NOT match as "pass".
        let raw = "The workflow failed because the password was incorrect.";
        let result = parse_verification(raw);
        assert!(!result.passed);
    }

    #[test]
    fn parse_verification_fallback_no_false_positive_on_bypass() {
        let raw = "The workflow tried to bypass the security check.";
        let result = parse_verification(raw);
        assert!(!result.passed);
    }

    #[test]
    fn parse_verification_fallback_whole_word_pass() {
        let raw = "The workflow result: pass";
        let result = parse_verification(raw);
        assert!(result.passed);
    }

    #[test]
    fn safe_truncate_ascii() {
        assert_eq!(safe_truncate("hello world", 5), "hello");
        assert_eq!(safe_truncate("hello", 10), "hello");
    }

    #[test]
    fn safe_truncate_multibyte() {
        // "café" is 5 bytes: c(1) a(1) f(1) é(2)
        let s = "café";
        assert_eq!(s.len(), 5);
        // Truncating at 4 would split the é — should back up to 3.
        assert_eq!(safe_truncate(s, 4), "caf");
        // Truncating at 5 returns the whole string.
        assert_eq!(safe_truncate(s, 5), "café");
    }

    #[test]
    fn safe_truncate_emoji() {
        // 🎉 is 4 bytes
        let s = "ok🎉";
        assert_eq!(s.len(), 6);
        // Truncating at 3 would split the emoji — should back up to 2.
        assert_eq!(safe_truncate(s, 3), "ok");
    }
}
