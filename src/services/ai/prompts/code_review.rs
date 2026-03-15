// ---------------------------------------------------------------------------
// Code review prompt builder — per-file review with comments.
// Ported from Otto's prompts/code-review.ts.
// This is the most complex prompt — it conditionally includes many context sections.
// ---------------------------------------------------------------------------

use super::shared::OTTO_IDENTITY;
use crate::services::ai::client::ChatMessage;
use crate::services::ai::service::FileReviewContext;
use crate::types::review::{DiffFileData, MrContext};

pub fn build(
    mr: &MrContext,
    file: &DiffFileData,
    file_content: Option<&str>,
    context: &FileReviewContext,
    custom_prompt: Option<&str>,
) -> Vec<ChatMessage> {
    let system = if let Some(custom) = custom_prompt {
        format!("{}\n\n{}", OTTO_IDENTITY, custom)
    } else {
        format!(
            r#"{}

Review this file's changes and return a JSON object. Schema:
{{
  "filePath": "string",
  "summary": "string — one paragraph summary of changes in this file",
  "riskLevel": "low" | "medium" | "high",
  "comments": [
    {{
      "id": "string — unique ID (use a short random string)",
      "filePath": "string",
      "startLine": number | null,
      "endLine": number | null,
      "severity": "critical" | "warning" | "suggestion" | "info",
      "category": "bug" | "logic-error" | "security" | "performance" | "readability" | "style" | "error-handling" | "naming" | "duplication" | "other",
      "title": "string — one-line summary",
      "body": "string — detailed explanation (markdown)",
      "originalCode": "string | null — the code being replaced",
      "suggestion": "string | null — suggested replacement code",
      "suggestionSummary": "string | null — what the suggestion does",
      "status": "pending",
      "editedBody": null
    }}
  ]
}}

Rules:
- Line numbers reference the NEW file (post-change), not the diff.
- Only flag real issues. If the code is fine, return an empty comments array.
- For suggestions, include both originalCode and suggestion so a diff can be shown.
- severity=critical: bugs, security holes, data loss. warning: logic issues, error handling gaps. suggestion: improvements. info: observations.
- Return ONLY valid JSON."#,
            OTTO_IDENTITY
        )
    };

    let renamed_label = if file.is_renamed {
        format!(
            " [RENAMED from {}]",
            file.old_path.as_deref().unwrap_or("?")
        )
    } else {
        String::new()
    };

    let file_label = if file.is_new {
        " [NEW]"
    } else if file.is_deleted {
        " [DELETED]"
    } else if file.is_renamed {
        &renamed_label
    } else {
        ""
    };

    let mut user_parts = vec![
        format!(
            "## File: {}{} (in MR: {})",
            file.file_path, file_label, mr.title
        ),
        format!("**Branch:** {} → {}", mr.source_branch, mr.target_branch),
        format!(
            "**Changes:** +{} / -{} lines",
            file.added_lines, file.removed_lines
        ),
    ];

    // Full file content (if available) gives the AI context beyond the diff
    if let Some(content) = file_content {
        if !content.is_empty() && content.len() < 50_000 {
            user_parts.push(format!(
                "## Full file content (source branch)\n```\n{}\n```",
                content
            ));
        }
    }

    // Diff
    user_parts.push(format!("## Diff\n```diff\n{}\n```", file.diff));

    // Optional context sections — each conditionally appended
    if let Some(ref repo_ctx) = context.repo_context {
        if !repo_ctx.is_empty() {
            user_parts.push(format!("## Repository Context\n{}", repo_ctx));
        }
    }

    if let Some(ref callers) = context.caller_snippets {
        if !callers.is_empty() {
            user_parts.push(format!("## Callers / Dependents\n{}", callers));
        }
    }

    if let Some(ref ticket) = context.ticket_context {
        if !ticket.is_empty() {
            user_parts.push(format!("## Linked Ticket\n{}", ticket));
        }
    }

    if let Some(ref prefs) = context.reviewer_prefs {
        if !prefs.is_empty() {
            user_parts.push(format!("## Reviewer Preferences\n{}", prefs));
        }
    }

    if let Some(ref activity) = context.file_activity {
        if !activity.is_empty() {
            user_parts.push(format!("## Recent Activity on This File\n{}", activity));
        }
    }

    if let Some(ref repo_config) = context.repo_config {
        if !repo_config.is_empty() {
            user_parts.push(repo_config.clone());
        }
    }

    if context.is_self_review {
        user_parts.push(
            "**Note:** The MR author is reviewing their own code. Focus on issues they might have blind spots for."
                .to_string(),
        );
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
