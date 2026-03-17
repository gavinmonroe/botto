// ---------------------------------------------------------------------------
// Chat prompt — conversational AI in the context of a code review.
// Ported from Otto's prompts/chat.ts.
// ---------------------------------------------------------------------------

use super::shared::OTTO_IDENTITY;
use crate::services::ai::client::ChatMessage;

pub fn build_system(
    review_context: &str,
    custom_prompt: Option<&str>,
    repo_config: Option<&str>,
) -> ChatMessage {
    let system = if let Some(custom) = custom_prompt {
        format!("{}\n\n{}", OTTO_IDENTITY, custom)
    } else {
        let mut prompt = format!(
            r#"{}

You are in a chat conversation about a code review. The user can ask questions about the code,
the review comments, or request explanations. Be helpful and concise.

## Current Review Context
{}"#,
            OTTO_IDENTITY, review_context
        );

        if let Some(rc) = repo_config {
            if !rc.is_empty() {
                prompt.push_str("\n\n");
                prompt.push_str(rc);
            }
        }

        prompt
    };

    ChatMessage {
        role: "system".into(),
        content: Some(system),
        tool_calls: None,
        tool_call_id: None,
    }
}

/// Build a structured context string from Otto's ChatReviewContext JSON payload.
/// Ports Otto's buildContextMessage() from chat.ts so the AI gets the same
/// rich context whether the chat runs locally or through Botto.
pub fn build_context_from_payload(payload: &serde_json::Value) -> String {
    let review_ctx = match payload.get("reviewContext") {
        Some(ctx) if ctx.is_object() => ctx,
        // Fallback: if reviewContext is a string (shouldn't happen, but defensive)
        Some(ctx) if ctx.is_string() => return ctx.as_str().unwrap_or("").to_string(),
        _ => return String::new(),
    };

    let mr = &review_ctx["mrContext"];
    let summary = &review_ctx["summary"];
    let file_reviews = review_ctx["fileReviews"].as_array();
    let edge_cases = review_ctx["edgeCases"].as_array();
    let related_files = review_ctx["relatedFiles"].as_array();

    let mut content = format!(
        "# MR Context\n\n**Title:** {}\n**Source:** {} → **Target:** {}\n**Project:** {}\n\n## Description\n{}",
        mr["title"].as_str().unwrap_or(""),
        mr["sourceBranch"].as_str().unwrap_or(""),
        mr["targetBranch"].as_str().unwrap_or(""),
        mr["projectPath"].as_str().unwrap_or(""),
        mr["description"].as_str().unwrap_or("(No description provided)"),
    );

    // Summary
    if summary.is_object() && !summary.is_null() {
        content.push_str(&format!(
            "\n\n## Review Summary\n**Overview:** {}\n**Risk Assessment:** {}",
            summary["overview"].as_str().unwrap_or(""),
            summary["riskAssessment"].as_str().unwrap_or(""),
        ));
        if let Some(changes) = summary["keyChanges"].as_array() {
            content.push_str("\n**Key Changes:**");
            for c in changes {
                if let Some(s) = c.as_str() {
                    content.push_str(&format!("\n- {}", s));
                }
            }
        }
        if let Some(areas) = summary["affectedAreas"].as_array() {
            let area_strs: Vec<&str> = areas.iter().filter_map(|a| a.as_str()).collect();
            if !area_strs.is_empty() {
                content.push_str(&format!("\n**Affected Areas:** {}", area_strs.join(", ")));
            }
        }
    }

    // File reviews
    if let Some(reviews) = file_reviews {
        if !reviews.is_empty() {
            content.push_str("\n\n## Per-File Review Findings");
            for fr in reviews {
                content.push_str(&format!(
                    "\n\n### {} (risk: {})\n{}",
                    fr["filePath"].as_str().unwrap_or(""),
                    fr["riskLevel"].as_str().unwrap_or("unknown"),
                    fr["summary"].as_str().unwrap_or(""),
                ));
                if let Some(comments) = fr["comments"].as_array() {
                    if !comments.is_empty() {
                        content.push_str(&format!("\n**Comments ({}):**", comments.len()));
                        for c in comments {
                            let line_ref = match (c["startLine"].as_u64(), c["endLine"].as_u64()) {
                                (Some(start), Some(end)) if end != start => {
                                    format!(" (lines {}-{})", start, end)
                                }
                                (Some(start), _) => format!(" (line {})", start),
                                _ => String::new(),
                            };
                            content.push_str(&format!(
                                "\n- [{}/{}]{} {}: {}",
                                c["severity"].as_str().unwrap_or("info"),
                                c["category"].as_str().unwrap_or("other"),
                                line_ref,
                                c["title"].as_str().unwrap_or(""),
                                c["body"].as_str().unwrap_or(""),
                            ));
                        }
                    }
                }
            }
        }
    }

    // Edge cases
    if let Some(cases) = edge_cases {
        if !cases.is_empty() {
            content.push_str("\n\n## Edge Cases Found");
            for ec in cases {
                let file_ref = match (ec["filePath"].as_str(), ec["lineRange"].as_object()) {
                    (Some(fp), Some(lr)) => format!(
                        " ({fp}:{}-{})",
                        lr.get("start").and_then(|v| v.as_u64()).unwrap_or(0),
                        lr.get("end").and_then(|v| v.as_u64()).unwrap_or(0),
                    ),
                    (Some(fp), None) => format!(" ({})", fp),
                    _ => String::new(),
                };
                content.push_str(&format!(
                    "\n- [{}] {}{}: {}",
                    ec["severity"].as_str().unwrap_or("minor"),
                    ec["title"].as_str().unwrap_or(""),
                    file_ref,
                    ec["description"].as_str().unwrap_or(""),
                ));
            }
        }
    }

    // Related files
    if let Some(files) = related_files {
        if !files.is_empty() {
            content.push_str("\n\n## Related Files (not in diff but relevant)");
            for rf in files {
                content.push_str(&format!(
                    "\n- {} ({}): {}",
                    rf["filePath"].as_str().unwrap_or(""),
                    rf["relationship"].as_str().unwrap_or("other"),
                    rf["reason"].as_str().unwrap_or(""),
                ));
            }
        }
    }

    // Diffs — include with truncation budget matching Otto's limits
    if let Some(diff_files) = mr["diffFiles"].as_array() {
        if !diff_files.is_empty() {
            const MAX_DIFF_CHARS: usize = 60_000;
            const MAX_PER_FILE_CHARS: usize = 8_000;

            let total: usize = diff_files
                .iter()
                .map(|f| f["diff"].as_str().map_or(0, |d| d.len()))
                .sum();
            let needs_truncation = total > MAX_DIFF_CHARS;

            content.push_str(&format!(
                "\n\n## File Diffs ({} files{})",
                diff_files.len(),
                if needs_truncation {
                    ", some truncated for context limits"
                } else {
                    ""
                },
            ));

            let mut remaining = MAX_DIFF_CHARS;

            for f in diff_files {
                let fp = f["filePath"].as_str().unwrap_or("");
                let added = f["addedLines"].as_u64().unwrap_or(0);
                let removed = f["removedLines"].as_u64().unwrap_or(0);
                let status = if f["isNew"].as_bool().unwrap_or(false) {
                    "[NEW]"
                } else if f["isDeleted"].as_bool().unwrap_or(false) {
                    "[DELETED]"
                } else if f["isRenamed"].as_bool().unwrap_or(false) {
                    "[RENAMED]"
                } else {
                    "[MODIFIED]"
                };

                if remaining == 0 {
                    content.push_str(&format!(
                        "\n\n### {} {} (+{} -{})\n*(diff omitted — context limit reached)*",
                        status, fp, added, removed,
                    ));
                    continue;
                }

                let diff = f["diff"].as_str().unwrap_or("");
                let limit = if needs_truncation {
                    MAX_PER_FILE_CHARS.min(remaining)
                } else {
                    diff.len()
                };
                let truncated = if diff.len() > limit {
                    // Find safe UTF-8 boundary
                    let mut end = limit;
                    while end > 0 && !diff.is_char_boundary(end) {
                        end -= 1;
                    }
                    format!("{}\n... (truncated)", &diff[..end])
                } else {
                    diff.to_string()
                };

                remaining = remaining.saturating_sub(truncated.len());

                content.push_str(&format!(
                    "\n\n### {} {} (+{} -{})\n```diff\n{}\n```",
                    status, fp, added, removed, truncated,
                ));
            }
        }
    }

    content
}
