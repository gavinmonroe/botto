// ---------------------------------------------------------------------------
// Contracts prompt — infers preconditions/postconditions/invariants.
// Ported from Otto's prompts/contracts.ts.
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

Infer contracts (preconditions, postconditions, invariants) for functions changed in this diff.

Return a JSON object. Schema:
{{
  "contracts": [
    {{
      "id": "string",
      "functionName": "string",
      "filePath": "string",
      "lineRange": {{ "start": number, "end": number }} | null,
      "preconditions": [{{ "human": "string", "code": "string | null" }}],
      "postconditions": [{{ "human": "string", "code": "string | null" }}],
      "invariants": [{{ "human": "string", "code": "string | null" }}],
      "verificationStatus": "verified" | "violation-possible" | "unknown",
      "violationPath": "string | null — how the contract can be violated",
      "aiReasoned": true
    }}
  ],
  "totalVerified": number,
  "totalViolations": number,
  "totalUnknown": number
}}

Rules:
- Focus on non-trivial contracts that reveal real constraints.
- "code" should be a TypeScript/Zod-style assertion when expressible, null otherwise.
- Set aiReasoned=true since these are reasoned, not executed.
- Return ONLY valid JSON."#,
            OTTO_IDENTITY
        )
    };

    let mut user_parts = vec![format!("## MR: {} ({})", mr.title, mr.project_path)];

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
