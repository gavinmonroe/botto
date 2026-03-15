// ---------------------------------------------------------------------------
// AC validation prompt — checks diff against acceptance criteria from tickets.
// Ported from Otto's prompts/ac-validation.ts.
// ---------------------------------------------------------------------------

use super::shared::OTTO_IDENTITY;
use crate::services::ai::client::ChatMessage;
use crate::types::review::MrContext;

pub fn build(
    mr: &MrContext,
    criteria: &[String],
    ticket_key: &str,
    custom_prompt: Option<&str>,
) -> Vec<ChatMessage> {
    let system = if let Some(custom) = custom_prompt {
        format!("{}\n\n{}", OTTO_IDENTITY, custom)
    } else {
        format!(
            r#"{}

Validate each acceptance criterion against the code changes in this MR.
Return a JSON object. Schema:
{{
  "ticketKey": "string",
  "criteria": [
    {{
      "criterion": "string — the original criterion text",
      "status": "satisfied" | "unclear" | "not-found",
      "explanation": "string — why this status (markdown)",
      "evidence": [
        {{
          "filePath": "string",
          "startLine": number | null,
          "endLine": number | null,
          "snippet": "string | null"
        }}
      ]
    }}
  ],
  "summary": "string — one-line overall assessment"
}}

Rules:
- "satisfied" = clear evidence in the diff that this criterion is met.
- "unclear" = partially addressed or can't determine from the diff alone.
- "not-found" = no evidence in the diff that this was addressed.
- Return ONLY valid JSON."#,
            OTTO_IDENTITY
        )
    };

    let criteria_text = criteria
        .iter()
        .enumerate()
        .map(|(i, c)| format!("{}. {}", i + 1, c))
        .collect::<Vec<_>>()
        .join("\n");

    let mut user_parts = vec![
        format!("## Ticket: {}", ticket_key),
        format!("## Acceptance Criteria\n{}", criteria_text),
        format!("## MR: {} ({})", mr.title, mr.project_path),
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
