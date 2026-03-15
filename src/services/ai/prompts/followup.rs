// ---------------------------------------------------------------------------
// Follow-up prompt — analyzes a comment thread for follow-up actions.
// Ported from Otto's prompts/followup.ts.
// ---------------------------------------------------------------------------

use super::shared::OTTO_IDENTITY;
use crate::services::ai::client::ChatMessage;

pub fn build(
    comment_body: &str,
    thread_context: &str,
    diff_context: &str,
    custom_prompt: Option<&str>,
) -> Vec<ChatMessage> {
    let system = if let Some(custom) = custom_prompt {
        format!("{}\n\n{}", OTTO_IDENTITY, custom)
    } else {
        format!(
            r#"{}

Analyze this MR comment thread and determine if follow-up action is needed.
Return a JSON object. Schema:
{{
  "needsFollowUp": boolean,
  "summary": "string — what the comment is about",
  "suggestedAction": "string | null — what should be done",
  "priority": "high" | "medium" | "low",
  "category": "bug-fix" | "refactor" | "test" | "docs" | "discussion" | "other"
}}

Return ONLY valid JSON."#,
            OTTO_IDENTITY
        )
    };

    let user = format!(
        "## Comment\n{}\n\n## Thread Context\n{}\n\n## Diff Context\n{}",
        comment_body, thread_context, diff_context
    );

    vec![
        ChatMessage {
            role: "system".into(),
            content: Some(system),
            tool_calls: None,
            tool_call_id: None,
        },
        ChatMessage {
            role: "user".into(),
            content: Some(user),
            tool_calls: None,
            tool_call_id: None,
        },
    ]
}
