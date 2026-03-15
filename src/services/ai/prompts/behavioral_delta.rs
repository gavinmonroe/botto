// ---------------------------------------------------------------------------
// Behavioral delta prompt — identifies what behaviors changed vs. preserved.
// Ported from Otto's prompts/behavioral-delta.ts.
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

Analyze the behavioral delta of this MR: what behaviors changed, what was preserved, and what might be unexpected.

Return a JSON object. Schema:
{{
  "changed": [
    {{
      "id": "string",
      "description": "string",
      "type": "changed",
      "testScenario": "string — how to verify",
      "expectedOutcome": "string",
      "actualOutcome": null,
      "filePaths": ["string"],
      "verified": false,
      "aiReasoned": true
    }}
  ],
  "preserved": [same shape with "type": "preserved"],
  "unexpected": [same shape with "type": "unexpected"],
  "summary": "string — one-line overview"
}}

Rules:
- "changed": intentional behavior changes from the diff.
- "preserved": important behaviors that should still work after this change.
- "unexpected": side effects or behaviors that may have changed unintentionally.
- Return ONLY valid JSON."#,
            OTTO_IDENTITY
        )
    };

    let mut user_parts = vec![
        format!("## MR: {} ({})", mr.title, mr.project_path),
        format!("## Summary\n{}", summary.overview),
        format!("## Key Changes\n- {}", summary.key_changes.join("\n- ")),
        "## Diffs".to_string(),
    ];

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
