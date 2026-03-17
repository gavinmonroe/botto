// ---------------------------------------------------------------------------
// Semantic Conflict Analyzer — AI-powered analysis of high-severity conflicts.
//
// When two MRs modify overlapping line ranges, this analyzer explains *why*
// the changes are semantically incompatible (or compatible). This is the
// expensive, opt-in layer — gated by the `semantic_analysis` config toggle.
//
// Results are cached per (our_diff_hash, their_diff_hash) pair so re-analysis
// only happens when either MR pushes new commits.
// ---------------------------------------------------------------------------

use crate::services::ai::client::{
    AiClientConfig, AiError, ChatCompletionRequest, ChatMessage, chat_completion,
};
use crate::services::ai::service::TaskConfig;
use anyhow::Result;
use tokio::sync::Semaphore;
use tracing::debug;

/// Analyze the semantic relationship between two overlapping diffs on the same file.
///
/// Returns a human-readable explanation like:
/// "MR !42 adds rate limiting to `authenticate()`. MR !55 refactors
///  `authenticate()` to use async/await. These changes are semantically
///  incompatible — the rate limiting logic assumes synchronous execution."
///
/// The caller is responsible for checking the feature toggle and caching.
pub async fn analyze_semantic_conflict(
    client_cfg: &AiClientConfig,
    task_cfg: &TaskConfig,
    file_path: &str,
    our_mr_title: &str,
    our_mr_iid: u64,
    our_diff: &str,
    their_mr_title: &str,
    their_mr_iid: u64,
    their_diff: &str,
    file_content: Option<&str>,
    semaphore: &Semaphore,
) -> Result<String, AiError> {
    let _permit = semaphore
        .acquire()
        .await
        .map_err(|e| AiError::Network(format!("semaphore closed: {}", e)))?;

    let system_prompt = build_system_prompt();
    let user_prompt = build_user_prompt(
        file_path,
        our_mr_title,
        our_mr_iid,
        our_diff,
        their_mr_title,
        their_mr_iid,
        their_diff,
        file_content,
    );

    debug!(
        "semantic conflict analysis: {} (MR !{} vs MR !{})",
        file_path, our_mr_iid, their_mr_iid
    );

    let request = ChatCompletionRequest {
        model: task_cfg.model.clone(),
        messages: vec![
            ChatMessage {
                role: "system".into(),
                content: Some(system_prompt),
                tool_calls: None,
                tool_call_id: None,
            },
            ChatMessage {
                role: "user".into(),
                content: Some(user_prompt),
                tool_calls: None,
                tool_call_id: None,
            },
        ],
        temperature: Some(task_cfg.temperature),
        max_tokens: if task_cfg.max_tokens > 0 {
            Some(task_cfg.max_tokens)
        } else {
            Some(1024) // Semantic notes should be concise
        },
        stream: None,
        tools: None,
        tool_choice: None,
    };

    let response = chat_completion(client_cfg, request).await?;
    let content = response
        .choices
        .first()
        .and_then(|c| c.message.content.as_ref())
        .ok_or_else(|| AiError::Parse("empty response".into()))?;

    Ok(content.trim().to_string())
}

fn build_system_prompt() -> String {
    r#"You are a code conflict analyst. You analyze two merge request diffs that modify overlapping regions of the same file and determine whether they are semantically compatible.

Your analysis should be:
- Concise (2-4 sentences max)
- Specific about what each MR is doing to the overlapping code
- Clear about whether the changes are compatible, incompatible, or uncertain
- Actionable — if incompatible, briefly suggest which MR should merge first or how to resolve

Do NOT:
- Repeat the diff content
- Give generic advice about merge conflicts
- Suggest "communicate with the other developer" — that's obvious
- Use markdown formatting"#
        .to_string()
}

fn build_user_prompt(
    file_path: &str,
    our_mr_title: &str,
    our_mr_iid: u64,
    our_diff: &str,
    their_mr_title: &str,
    their_mr_iid: u64,
    their_diff: &str,
    file_content: Option<&str>,
) -> String {
    let mut prompt = format!(
        "File: {file_path}\n\n\
         MR !{our_mr_iid} \"{our_mr_title}\" changes:\n\
         ```\n{our_diff}\n```\n\n\
         MR !{their_mr_iid} \"{their_mr_title}\" changes:\n\
         ```\n{their_diff}\n```"
    );

    if let Some(content) = file_content {
        // Truncate to avoid blowing up context — the diffs are the primary signal
        let truncated: String = content.chars().take(4000).collect();
        prompt.push_str(&format!(
            "\n\nCurrent file content (may be truncated):\n```\n{}\n```",
            truncated
        ));
    }

    prompt.push_str(
        "\n\nAnalyze whether these changes are semantically compatible. \
         Will merging both cause logical issues beyond what git merge can detect?",
    );

    prompt
}
