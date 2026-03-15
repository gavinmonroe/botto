// ---------------------------------------------------------------------------
// Chat prompt — conversational AI in the context of a code review.
// Ported from Otto's prompts/chat.ts.
// ---------------------------------------------------------------------------

use super::shared::OTTO_IDENTITY;
use crate::services::ai::client::ChatMessage;

pub fn build_system(review_context: &str, custom_prompt: Option<&str>) -> ChatMessage {
    let system = if let Some(custom) = custom_prompt {
        format!("{}\n\n{}", OTTO_IDENTITY, custom)
    } else {
        format!(
            r#"{}

You are in a chat conversation about a code review. The user can ask questions about the code,
the review comments, or request explanations. Be helpful and concise.

## Current Review Context
{}"#,
            OTTO_IDENTITY, review_context
        )
    };

    ChatMessage {
        role: "system".into(),
        content: Some(system),
        tool_calls: None,
        tool_call_id: None,
    }
}
