// ---------------------------------------------------------------------------
// Cross-MR Summary Generator — AI tasks for cluster narratives and review order.
//
// Two AI tasks:
//   1. Cluster Summary — unified narrative of what each MR does and how they relate
//   2. Cluster Review Order — ordered phases for cross-MR guided walkthrough
//
// Both are generated on demand (not eagerly) and cached in the mr_clusters table.
// Cache invalidation is based on a composite diff hash of all member MRs.
// ---------------------------------------------------------------------------

use crate::db::queries;
use crate::services::ai::client::{
    AiClientConfig, AiError, ChatCompletionRequest, ChatMessage, chat_completion,
};
use crate::services::ai::service::TaskConfig;
use crate::types::cluster::{
    ClusterMember, ClusterReviewOrder, ClusterSummaryData, MrCluster,
};
use crate::types::review::MrContext;
use crate::util::json_repair::parse_ai_json;
use anyhow::Result;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use sqlx::SqlitePool;
use std::io::{Read, Write};
use tokio::sync::Semaphore;
use tracing::{debug, warn};

/// Generate a unified summary for a cluster of related MRs.
///
/// The summary explains what each MR does, how they relate, and what
/// integration risks exist. Results are cached in the mr_clusters table.
pub async fn generate_cluster_summary(
    pool: &SqlitePool,
    client_cfg: &AiClientConfig,
    task_cfg: &TaskConfig,
    cluster: &MrCluster,
    mr_contexts: &[MrContext],
    ticket_context: Option<&str>,
    semaphore: &Semaphore,
) -> Result<ClusterSummaryData, AiError> {
    // Check if we have a cached summary with a matching diff hash
    let current_hash = compute_composite_diff_hash(mr_contexts);

    if let Some(cached) = load_cached_summary(pool, &cluster.id, &current_hash).await {
        debug!("cluster summary cache hit for {}", cluster.id);
        return Ok(cached);
    }

    let _permit = semaphore
        .acquire()
        .await
        .map_err(|e| AiError::Network(format!("semaphore closed: {}", e)))?;

    debug!(
        "generating cluster summary for {} ({} MRs)",
        cluster.id,
        cluster.member_mrs.len()
    );

    let system_prompt = build_summary_system_prompt();
    let user_prompt =
        build_summary_user_prompt(&cluster.member_mrs, mr_contexts, ticket_context);

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
            Some(4096)
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

    let parsed_value = parse_ai_json(content).map_err(|e| AiError::Parse(e))?;
    let summary: ClusterSummaryData =
        serde_json::from_value(parsed_value).map_err(|e| AiError::Parse(e.to_string()))?;

    // Cache the result
    if let Err(e) = save_cached_summary(pool, &cluster.id, &summary, &current_hash).await {
        warn!("failed to cache cluster summary: {}", e);
    }

    Ok(summary)
}

/// Generate a review order for cross-MR guided walkthrough.
pub async fn generate_review_order(
    pool: &SqlitePool,
    client_cfg: &AiClientConfig,
    task_cfg: &TaskConfig,
    cluster: &MrCluster,
    mr_contexts: &[MrContext],
    semaphore: &Semaphore,
) -> Result<ClusterReviewOrder, AiError> {
    let _permit = semaphore
        .acquire()
        .await
        .map_err(|e| AiError::Network(format!("semaphore closed: {}", e)))?;

    debug!(
        "generating review order for cluster {} ({} MRs)",
        cluster.id,
        cluster.member_mrs.len()
    );

    let system_prompt = build_review_order_system_prompt();
    let user_prompt = build_review_order_user_prompt(&cluster.member_mrs, mr_contexts);

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
            Some(2048)
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

    let parsed_value = parse_ai_json(content).map_err(|e| AiError::Parse(e))?;
    let order: ClusterReviewOrder =
        serde_json::from_value(parsed_value).map_err(|e| AiError::Parse(e.to_string()))?;

    // Cache the result
    if let Ok(compressed) = compress_json(&order) {
        let _ = queries::update_cluster_review_order(pool, &cluster.id, &compressed).await;
    }

    Ok(order)
}

// ---------------------------------------------------------------------------
// Cache helpers
// ---------------------------------------------------------------------------

/// Compute a composite diff hash from all member MR contexts.
/// Used for cache invalidation — changes when any member MR's diff changes.
fn compute_composite_diff_hash(mr_contexts: &[MrContext]) -> String {
    let mut diffs: Vec<String> = mr_contexts
        .iter()
        .map(|ctx| {
            let mut mr_diffs: Vec<&str> = ctx.diff_files.iter().map(|f| f.diff.as_str()).collect();
            mr_diffs.sort(); // deterministic within each MR
            format!("{}:{}", ctx.mr_iid, mr_diffs.join("\n"))
        })
        .collect();
    diffs.sort(); // deterministic across MRs
    crate::util::hash::djb2(&diffs.join("\n---\n"))
}

/// Load a cached summary if the diff hash matches.
async fn load_cached_summary(
    pool: &SqlitePool,
    cluster_id: &str,
    expected_hash: &str,
) -> Option<ClusterSummaryData> {
    let row = queries::get_cluster_by_id(pool, cluster_id).await.ok()??;
    let (_id, _proj, _ticket, _members, _signals, _rel, summary_blob, summary_hash, _order, _updated) = row;

    // Check hash match
    let hash = summary_hash?;
    if hash != expected_hash {
        return None;
    }

    // Decompress and parse
    let blob = summary_blob?;
    let json_str = decompress_json(&blob)?;
    serde_json::from_str(&json_str).ok()
}

/// Save a summary to the cluster cache.
async fn save_cached_summary(
    pool: &SqlitePool,
    cluster_id: &str,
    summary: &ClusterSummaryData,
    diff_hash: &str,
) -> Result<()> {
    let compressed = compress_json(summary)?;
    queries::update_cluster_summary(pool, cluster_id, &compressed, diff_hash).await
}

fn compress_json<T: serde::Serialize>(value: &T) -> Result<Vec<u8>> {
    let json = serde_json::to_string(value)?;
    let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
    encoder.write_all(json.as_bytes())?;
    Ok(encoder.finish()?)
}

fn decompress_json(data: &[u8]) -> Option<String> {
    let mut decoder = GzDecoder::new(data);
    let mut json = String::new();
    decoder.read_to_string(&mut json).ok()?;
    Some(json)
}

// ---------------------------------------------------------------------------
// Prompt builders
// ---------------------------------------------------------------------------

fn build_summary_system_prompt() -> String {
    r#"You are a code review analyst. You analyze a group of related merge requests and produce a unified summary explaining how they fit together.

Respond with a JSON object matching this schema:
{
  "narrative": "A 2-4 sentence overview of what this group of MRs accomplishes together",
  "perMrRoles": [
    {
      "mrIid": 42,
      "role": "Short role description (e.g., 'API endpoint', 'frontend view', 'test coverage')",
      "keyChanges": ["Change 1", "Change 2"]
    }
  ],
  "riskAssessment": "Brief assessment of integration risks between these MRs",
  "integrationConcerns": ["Concern 1", "Concern 2"]
}

Be specific and technical. Focus on how the MRs interact, not just what each one does individually. If there are ordering dependencies (e.g., API must merge before frontend), mention them."#.to_string()
}

fn build_summary_user_prompt(
    members: &[ClusterMember],
    mr_contexts: &[MrContext],
    ticket_context: Option<&str>,
) -> String {
    let mut prompt = String::from("Analyze these related merge requests:\n\n");

    for ctx in mr_contexts {
        let member = members.iter().find(|m| m.mr_iid == ctx.mr_iid);
        let author = member.map(|m| m.author.as_str()).unwrap_or("unknown");

        prompt.push_str(&format!(
            "## MR !{} — \"{}\" (by {})\n",
            ctx.mr_iid, ctx.title, author
        ));

        if let Some(desc) = &ctx.description {
            if !desc.is_empty() {
                let truncated: String = desc.chars().take(500).collect();
                prompt.push_str(&format!("Description: {}\n", truncated));
            }
        }

        prompt.push_str(&format!(
            "Branch: {} → {}\n",
            ctx.source_branch, ctx.target_branch
        ));

        // Include file list and truncated diffs
        prompt.push_str("Changed files:\n");
        for file in &ctx.diff_files {
            prompt.push_str(&format!("  - {} (+{} -{})\n", file.file_path, file.added_lines, file.removed_lines));
        }

        // Include first 2000 chars of diff per MR to keep context manageable
        let combined_diff: String = ctx
            .diff_files
            .iter()
            .map(|f| format!("--- {}\n{}", f.file_path, f.diff))
            .collect::<Vec<_>>()
            .join("\n");
        let truncated_diff: String = combined_diff.chars().take(2000).collect();
        prompt.push_str(&format!("Diff (truncated):\n```\n{}\n```\n\n", truncated_diff));
    }

    if let Some(ticket) = ticket_context {
        prompt.push_str(&format!("Ticket context:\n{}\n\n", ticket));
    }

    prompt.push_str("Produce the unified summary JSON.");
    prompt
}

fn build_review_order_system_prompt() -> String {
    r#"You are a code review strategist. Given a group of related merge requests, determine the optimal order to review them for maximum understanding.

Respond with a JSON object matching this schema:
{
  "phases": [
    {
      "label": "Short phase name (e.g., 'Data Model', 'API Layer', 'Frontend', 'Tests')",
      "mrIid": 42,
      "files": ["src/api/users.ts", "src/api/auth.ts"],
      "rationale": "Brief explanation of why to review this phase at this point"
    }
  ]
}

Order phases so that foundational changes (data models, shared types, API contracts) come first, then implementation (business logic, UI), then verification (tests, configs). Within the same MR, group related files into a single phase.

A single MR may appear in multiple phases if it touches multiple layers."#.to_string()
}

fn build_review_order_user_prompt(
    members: &[ClusterMember],
    mr_contexts: &[MrContext],
) -> String {
    let mut prompt = String::from("Determine the review order for these related MRs:\n\n");

    for ctx in mr_contexts {
        let member = members.iter().find(|m| m.mr_iid == ctx.mr_iid);
        let author = member.map(|m| m.author.as_str()).unwrap_or("unknown");

        prompt.push_str(&format!(
            "MR !{} \"{}\" (by {}):\n",
            ctx.mr_iid, ctx.title, author
        ));

        for file in &ctx.diff_files {
            prompt.push_str(&format!(
                "  - {} (+{} -{})\n",
                file.file_path, file.added_lines, file.removed_lines
            ));
        }
        prompt.push('\n');
    }

    prompt.push_str("Produce the review order JSON.");
    prompt
}
