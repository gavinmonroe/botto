// ---------------------------------------------------------------------------
// Edge cases prompt builder — identifies potential edge cases in the diff.
// Ported from Otto's prompts/edge-cases.ts.
// ---------------------------------------------------------------------------

use super::shared::OTTO_IDENTITY;
use crate::services::ai::client::ChatMessage;
use crate::types::review::{MrContext, MrSummary};

pub fn build(mr: &MrContext, summary: &MrSummary, custom_prompt: Option<&str>) -> Vec<ChatMessage> {
    let system = if let Some(custom) = custom_prompt {
        format!("{}\n\n{}", OTTO_IDENTITY, custom)
    } else {
        format!(
            r#"{}

Analyze the diff for edge cases, boundary conditions, and potential failure modes.
Return a JSON array of edge cases. Schema:
[
  {{
    "id": "string",
    "title": "string — one-line description",
    "description": "string — detailed analysis (markdown)",
    "filePath": "string | null",
    "lineRange": {{ "start": number, "end": number }} | null,
    "severity": "critical" | "moderate" | "minor",
    "category": "error-handling" | "boundary-condition" | "race-condition" | "null-safety" | "type-safety" | "resource-leak" | "other",
    "hypotheticalTrace": "string | null — stack trace scenario showing how this could fail"
  }}
]

Focus on:
- Unhandled error paths
- Boundary conditions (empty arrays, zero values, max values, null/undefined)
- Race conditions in async code
- Resource leaks (unclosed handles, missing cleanup)
- Type coercion issues

If no meaningful edge cases exist, return an empty array.
Return ONLY valid JSON."#,
            OTTO_IDENTITY
        )
    };

    let mut user_parts = vec![
        format!("## MR: {} ({})", mr.title, mr.project_path),
        format!("## Summary\n{}", summary.overview),
        format!("## Key Changes\n{}", summary.key_changes.join("\n- ")),
    ];

    user_parts.push("## Diffs".to_string());
    for file in &mr.diff_files {
        user_parts.push(format!(
            "### {}\n```diff\n{}\n```",
            file.file_path, file.diff
        ));
    }

    vec![
        ChatMessage {
            role: "system".into(),
            content: Some(system),
            tool_calls: None,
            tool_call_id: None,
        },
        ChatMessage {
            role: "user".into(),
            content: Some(user_parts.join("\n\n")),
            tool_calls: None,
            tool_call_id: None,
        },
    ]
}
