// ---------------------------------------------------------------------------
// Inquiry prompt — focused code explainer for a selected line range.
// Mirrors Otto's prompts/inquiry.ts.
// ---------------------------------------------------------------------------

use super::shared::OTTO_IDENTITY;
use crate::services::ai::client::ChatMessage;

const SYSTEM_PROMPT: &str = r#"You are answering a developer's question about a specific section of code in a merge request diff.

## Your role

You are a code explainer. The developer has selected specific lines and asked a question about them. Answer precisely and concisely.

## Rules

- Answer ONLY the question asked. Do not volunteer review comments, suggestions, or unsolicited opinions about code quality.
- Match your depth to the question. "How does this work?" gets a walkthrough. "What calls this?" gets a list.
- Use markdown: fenced code blocks with language tags, bullet points, headers for longer answers.
- Reference specific line numbers from the selected range when relevant.
- If the question asks for an alternative approach, show concrete code — don't just describe it abstractly.
- If you don't have enough context to answer fully, say what you can and note what's missing.
- No preamble. Start with the answer."#;

/// Build the system message for an inquiry.
pub fn build_system(custom_prompt: Option<&str>) -> ChatMessage {
    let system = if let Some(custom) = custom_prompt {
        format!("{}\n\n{}", OTTO_IDENTITY, custom)
    } else {
        format!("{}\n\n{}", OTTO_IDENTITY, SYSTEM_PROMPT)
    };

    ChatMessage {
        role: "system".into(),
        content: Some(system),
        tool_calls: None,
        tool_call_id: None,
    }
}

/// Build the context message from Otto's InquiryContext JSON payload.
/// Extracts filePath, lineRange, diffSnippet, codeContent, fullFileDiff,
/// and MR metadata to give the AI full context about the selected code.
pub fn build_context_from_payload(payload: &serde_json::Value) -> String {
    let ctx = match payload.get("inquiryContext") {
        Some(c) if c.is_object() => c,
        _ => return String::new(),
    };

    let file_path = ctx
        .get("filePath")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");
    let start_line = ctx.get("startLine").and_then(|v| v.as_u64()).unwrap_or(0);
    let end_line = ctx.get("endLine").and_then(|v| v.as_u64()).unwrap_or(0);
    let diff_snippet = ctx
        .get("diffSnippet")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let code_content = ctx
        .get("codeContent")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let full_file_diff = ctx
        .get("fullFileDiff")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    let mr = &ctx["mrContext"];
    let title = mr.get("title").and_then(|v| v.as_str()).unwrap_or("");
    let source_branch = mr
        .get("sourceBranch")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let target_branch = mr
        .get("targetBranch")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let project_path = mr.get("projectPath").and_then(|v| v.as_str()).unwrap_or("");
    let description = mr.get("description").and_then(|v| v.as_str()).unwrap_or("");

    // Detect language from file extension
    let ext = file_path.rsplit('.').next().unwrap_or("");
    let lang = match ext {
        "ts" | "tsx" => "typescript",
        "js" | "jsx" => "javascript",
        "py" => "python",
        "rb" => "ruby",
        "rs" => "rust",
        "go" => "go",
        "java" => "java",
        "kt" => "kotlin",
        "swift" => "swift",
        "cs" => "csharp",
        "cpp" | "cc" | "cxx" => "cpp",
        "c" | "h" => "c",
        "php" => "php",
        "vue" => "vue",
        "svelte" => "svelte",
        "html" => "html",
        "css" => "css",
        "scss" => "scss",
        "sql" => "sql",
        "sh" | "bash" => "bash",
        "yml" | "yaml" => "yaml",
        "toml" => "toml",
        "json" => "json",
        other => other,
    };

    let line_range = if start_line == end_line {
        format!("line {}", start_line)
    } else {
        format!("lines {}-{}", start_line, end_line)
    };

    let mut content = format!(
        "# Selected Code\n\n\
         **File:** {}\n\
         **Lines:** {}\n\
         **MR:** {} ({} → {})\n\
         **Project:** {}",
        file_path, line_range, title, source_branch, target_branch, project_path
    );

    if !code_content.is_empty() {
        content.push_str(&format!(
            "\n\n## Selected Code ({})\n```{}\n{}\n```",
            line_range, lang, code_content
        ));
    }

    if !diff_snippet.is_empty() {
        content.push_str(&format!(
            "\n\n## Diff for Selected Lines\n```diff\n{}\n```",
            diff_snippet
        ));
    }

    if !full_file_diff.is_empty() {
        let max_chars = 12_000;
        let diff = if full_file_diff.len() > max_chars {
            format!("{}\n... (truncated)", &full_file_diff[..max_chars])
        } else {
            full_file_diff.to_string()
        };
        content.push_str(&format!(
            "\n\n## Full File Diff (for broader context)\n```diff\n{}\n```",
            diff
        ));
    }

    if !description.is_empty() {
        let desc = if description.len() > 2000 {
            format!("{}... (truncated)", &description[..2000])
        } else {
            description.to_string()
        };
        content.push_str(&format!("\n\n## MR Description\n{}", desc));
    }

    content
}
