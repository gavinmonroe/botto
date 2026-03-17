// ---------------------------------------------------------------------------
// Adversarial tests prompt — generates property-based tests to break the code.
// Ported from Otto's prompts/adversarial-tests.ts.
// ---------------------------------------------------------------------------

use super::shared::OTTO_IDENTITY;
use crate::services::ai::client::ChatMessage;
use crate::types::review::{EdgeCase, MrContext};

pub fn build(
    mr: &MrContext,
    edge_cases: &[EdgeCase],
    custom_prompt: Option<&str>,
    repo_config: Option<&str>,
) -> Vec<ChatMessage> {
    let system = if let Some(custom) = custom_prompt {
        format!("{}\n\n{}", OTTO_IDENTITY, custom)
    } else {
        format!(
            r#"{}

Generate property-based tests that attempt to find counterexamples in the changed code.
Focus on the edge cases identified in the review.

Return a JSON object. Schema:
{{
  "files": [
    {{
      "filePath": "string",
      "tests": [
        {{
          "id": "string",
          "property": "string — human-readable property description",
          "testCode": "string — runnable test code",
          "targetFunction": "string — function being tested",
          "filePath": "string",
          "lineRange": {{ "start": number, "end": number }} | null
        }}
      ],
      "results": [
        {{
          "testId": "string",
          "status": "held" | "counterexample" | "error" | "not-run",
          "iterations": number | null,
          "counterexample": "string | null",
          "errorMessage": "string | null",
          "aiReasoned": true
        }}
      ]
    }}
  ],
  "totalTests": number,
  "totalHeld": number,
  "totalCounterexamples": number,
  "totalErrors": number
}}

Rules:
- Generate tests that could realistically find bugs.
- For each test, reason about whether the property holds and provide a result.
- Set aiReasoned=true since these are not executed.
- Return ONLY valid JSON."#,
            OTTO_IDENTITY
        )
    };

    let mut user_parts = vec![format!("## MR: {} ({})", mr.title, mr.project_path)];

    if !edge_cases.is_empty() {
        user_parts.push("## Edge Cases Identified".to_string());
        for ec in edge_cases {
            user_parts.push(format!(
                "- **{}** ({}): {}",
                ec.title,
                format!("{:?}", ec.severity).to_lowercase(),
                ec.description
            ));
        }
    }

    user_parts.push("## Diffs".to_string());
    for file in &mr.diff_files {
        user_parts.push(format!(
            "### {}\n```diff\n{}\n```",
            file.file_path, file.diff
        ));
    }

    if let Some(rc) = repo_config {
        if !rc.is_empty() {
            user_parts.push(rc.to_string());
        }
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
