// ---------------------------------------------------------------------------
// Summary prompt builder — generates MR overview + risk assessment.
// Ported from Otto's prompts/summary.ts.
// ---------------------------------------------------------------------------

use super::shared::OTTO_IDENTITY;
use crate::services::ai::client::ChatMessage;
use crate::types::review::MrContext;

pub fn build(
    mr: &MrContext,
    ticket_context: Option<&str>,
    custom_prompt: Option<&str>,
    repo_config: Option<&str>,
) -> Vec<ChatMessage> {
    let system = if let Some(custom) = custom_prompt {
        format!("{}\n\n{}", OTTO_IDENTITY, custom)
    } else {
        format!(
            r#"{}

Generate a JSON summary of this merge request. Schema:
{{
  "overview": "string — what changed and why (markdown, 2-4 sentences)",
  "riskAssessment": "string — overall risk level explanation (markdown)",
  "keyChanges": ["string — bullet points of the most important changes"],
  "affectedAreas": ["string — high-level areas of the codebase affected"]
}}

Return ONLY valid JSON. No markdown fences. No explanation outside the JSON."#,
            OTTO_IDENTITY
        )
    };

    let mut user_parts = vec![
        format!("## Merge Request: {} ({})", mr.title, mr.project_path),
        format!("**Branch:** {} → {}", mr.source_branch, mr.target_branch),
    ];

    if let Some(ref desc) = mr.description {
        if !desc.is_empty() {
            user_parts.push(format!("**Description:**\n{}", desc));
        }
    }

    if let Some(ticket) = ticket_context {
        if !ticket.is_empty() {
            user_parts.push(format!("## Linked Ticket\n{}", ticket));
        }
    }

    if let Some(rc) = repo_config {
        if !rc.is_empty() {
            user_parts.push(rc.to_string());
        }
    }

    user_parts.push("## Diffs".to_string());
    for file in &mr.diff_files {
        user_parts.push(format!(
            "### {}{}\n```diff\n{}\n```",
            file.file_path,
            if file.is_new {
                " (new)"
            } else if file.is_deleted {
                " (deleted)"
            } else if file.is_renamed {
                " (renamed)"
            } else {
                ""
            },
            file.diff
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
