// ---------------------------------------------------------------------------
// Related files prompt — AI discovers files related to the diff via tool use.
// Ported from Otto's prompts/related-files.ts.
// ---------------------------------------------------------------------------

use super::shared::OTTO_IDENTITY;
use crate::services::ai::client::ChatMessage;
use crate::types::review::MrContext;

pub fn build(mr: &MrContext, custom_prompt: Option<&str>) -> Vec<ChatMessage> {
    let system = if let Some(custom) = custom_prompt {
        format!("{}\n\n{}", OTTO_IDENTITY, custom)
    } else {
        format!(
            r#"{}

Identify files in the repository that are related to the changes in this MR but are NOT in the diff.
You have tools to explore the repository. Use them to find real file paths.

Return a JSON array of related files. Schema:
[
  {{
    "filePath": "string — exact path in the repo",
    "reason": "string — why this file is relevant",
    "relationship": "imports" | "imported-by" | "shared-type" | "test" | "config" | "other"
  }}
]

Rules:
- Only include files that actually exist (use the tools to verify).
- Focus on files that a reviewer should look at to understand the full impact.
- Max 10 files. Prioritize by relevance.
- Return ONLY valid JSON."#,
            OTTO_IDENTITY
        )
    };

    let mut user_parts = vec![format!(
        "## MR: {} ({})\n**Changed files:**",
        mr.title, mr.project_path
    )];

    for file in &mr.diff_files {
        user_parts.push(format!("- {}", file.file_path));
    }

    user_parts.push("\n## Diffs".to_string());
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
            content: Some(user_parts.join("\n")),
            tool_calls: None,
            tool_call_id: None,
        },
    ]
}
