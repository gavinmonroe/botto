// ---------------------------------------------------------------------------
// Review orchestrator — the core review pipeline.
//
// Ported from Otto's review-orchestrator.ts. Coordinates all AI tasks for a
// single MR review, streaming results back via a callback.
//
// Pipeline structure (same as Otto):
//   Phase 1 (parallel): summary + context prep + file activity
//   Phase 2 (after context): code review (batched) + edge cases + related files
//   Phase 3 (after core): verification layer (adversarial, contracts, behavioral delta, trust)
//
// Key differences from Otto:
//   - Runs server-side with central credentials (no per-user config)
//   - Results are broadcast to ALL connected Ottos viewing the MR
//   - Cache is SQLite, not chrome.storage.local
//   - Cancellation via CancellationToken, not AbortSignal
// ---------------------------------------------------------------------------

use crate::config::BottoConfig;
use crate::services::ai::service::{self, FileReviewContext};
use crate::services::gitlab::client as gitlab;
use crate::services::repo_config;
use crate::services::review::cache;
use crate::services::verification::trust;
use crate::types::review::*;
use crate::types::verification::*;
use serde_json::{json, Value};
use sqlx::SqlitePool;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::{mpsc, Semaphore};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

/// Maximum concurrent file reviews.
const FILE_REVIEW_CONCURRENCY: usize = 3;

/// Per-file review timeout in seconds.
const FILE_REVIEW_TIMEOUT_SECS: u64 = 180;

/// File activity overall timeout in seconds.
const FILE_ACTIVITY_TIMEOUT_SECS: u64 = 120;

/// Max recent MRs to fetch changed paths for (diminishing returns beyond this).
const FILE_ACTIVITY_MAX_MRS: usize = 50;

/// Concurrent GitLab API calls when fetching changed paths for recent MRs.
const FILE_ACTIVITY_CONCURRENCY: usize = 10;

/// Callback for streaming chunks back to the caller.
/// Each chunk is a JSON Value matching Otto's StreamChunk types.
pub type ChunkSender = mpsc::Sender<Value>;

/// The set of tasks to execute. Allows partial re-runs.
pub type TaskSet = HashSet<ReviewTask>;

/// All tasks enabled.
pub fn all_tasks() -> TaskSet {
    let mut set = HashSet::new();
    set.insert(ReviewTask::Summary);
    set.insert(ReviewTask::CodeReview);
    set.insert(ReviewTask::EdgeCases);
    set.insert(ReviewTask::RelatedFiles);
    set.insert(ReviewTask::FileActivity);
    set.insert(ReviewTask::AdversarialTests);
    set.insert(ReviewTask::Contracts);
    set.insert(ReviewTask::BehavioralDelta);
    set
}

// ---------------------------------------------------------------------------
// Main entry point
// ---------------------------------------------------------------------------

/// Execute a full review pipeline for an MR.
///
/// Streams chunks via `send` as results become available.
/// Respects `cancel` for cooperative cancellation.
/// Returns the completed CachedReview on success.
pub async fn execute_review(
    cfg: &BottoConfig,
    pool: &SqlitePool,
    mr: &MrContext,
    tasks: &TaskSet,
    send: ChunkSender,
    cancel: CancellationToken,
    skip_cache: bool,
    ai_semaphore: Option<Arc<Semaphore>>,
) -> Option<CachedReview> {
    info!(
        "review started: {}:!{} ({} files, {} tasks)",
        mr.project_path,
        mr.mr_iid,
        mr.diff_files.len(),
        tasks.len()
    );

    // Compute diff hashes for caching
    let diffs: Vec<&str> = mr.diff_files.iter().map(|f| f.diff.as_str()).collect();
    let diff_hash = cache::compute_diff_hash(&diffs);
    let file_pairs: Vec<(&str, &str)> = mr
        .diff_files
        .iter()
        .map(|f| (f.file_path.as_str(), f.diff.as_str()))
        .collect();
    let current_file_hashes = cache::compute_file_diff_hashes(&file_pairs);

    // --- Populate the shared file index as a side-effect ---
    // This ensures conflict radar and cluster detection have data even if
    // webhooks weren't configured when the MR was opened. Non-blocking:
    // spawned so it doesn't delay the review pipeline.
    if let Some(project_id) = mr.project_id {
        let pool_clone = pool.clone();
        let diff_files = mr.diff_files.clone();
        let mr_iid = mr.mr_iid;
        tokio::spawn(async move {
            for file in &diff_files {
                let change_type = crate::types::cluster::change_type_from_diff(
                    file.is_new,
                    file.is_deleted,
                    file.is_renamed,
                );
                let hunks = crate::types::cluster::parse_hunks(&file.diff);
                let file_hash = crate::util::hash::djb2(&file.diff);
                let hunks_json = serde_json::to_string(&hunks).unwrap_or_else(|_| "[]".into());

                let _ = crate::db::queries::upsert_mr_changed_file(
                    &pool_clone,
                    project_id,
                    mr_iid as i64,
                    &file.file_path,
                    file.old_path.as_deref(),
                    change_type.as_str(),
                    &file_hash,
                    &hunks_json,
                )
                .await;
            }
            debug!("file index: populated {} files for !{} (review side-effect)", diff_files.len(), mr_iid);
        });
    }

    // --- Check cache (skip if forced regeneration) ---
    if !skip_cache {
        if let Some((cached, _hashes)) = cache::load_exact(pool, &mr.project_path, mr.mr_iid, &diff_hash).await {
            info!("cache hit for {}:!{}", mr.project_path, mr.mr_iid);
            emit_cached_review(&send, &cached).await;
            let _ = send.send(json!({ "type": "STREAM_ALL_COMPLETE" })).await;
            return Some(cached);
        }
    } else {
        // Delete existing cache for this MR so the fresh review replaces it
        cache::delete(pool, &mr.project_path, mr.mr_iid).await;
        info!("skip_cache: cleared cache for {}:!{}", mr.project_path, mr.mr_iid);
    }

    // Load latest cache for incremental re-review (only if not force-refreshing)
    let previous_cache = if !skip_cache {
        cache::load_latest(pool, &mr.project_path, mr.mr_iid).await
    } else {
        None
    };
    let (prev_review, prev_file_hashes) = match &previous_cache {
        Some((review, hashes, _hash)) => (Some(review), Some(hashes)),
        None => (None, None),
    };

    // Determine which files need re-review
    let files_to_review: Vec<&DiffFileData> = if let Some(prev_hashes) = prev_file_hashes {
        let changed = cache::changed_files(&current_file_hashes, prev_hashes);
        if changed.is_empty() && prev_review.is_some() {
            // All files unchanged — emit previous results
            info!("incremental: all files unchanged for {}:!{}", mr.project_path, mr.mr_iid);
            let cached = prev_review.unwrap();
            emit_cached_review(&send, cached).await;
            let _ = send.send(json!({ "type": "STREAM_ALL_COMPLETE" })).await;
            return Some(cached.clone());
        }
        debug!(
            "incremental: {}/{} files changed",
            changed.len(),
            mr.diff_files.len()
        );
        mr.diff_files
            .iter()
            .filter(|f| changed.contains(&f.file_path))
            .collect()
    } else {
        mr.diff_files.iter().collect()
    };

    // --- Build GitLab config for API calls ---
    let gl_cfg = gitlab::GitLabConfig {
        base_url: cfg.gitlab.url.clone(),
        token: cfg.gitlab.bot_token.clone(),
    };

    // --- Phase 1: Parallel — summary + file activity + repo config fetch ---
    let _ = send
        .send(json!({ "type": "STREAM_PROGRESS", "payload": { "message": "starting review..." } }))
        .await;

    // Spawn repo config fetch (parallel with everything else in Phase 1)
    let repo_config_handle = {
        let pool = pool.clone();
        let gl_cfg = gl_cfg.clone();
        let project_path = mr.project_path.clone();
        let project_id = mr.project_id;
        let source_branch = mr.source_branch.clone();
        tokio::spawn(async move {
            if let Some(pid) = project_id {
                repo_config::get_or_fetch(&pool, &gl_cfg, &project_path, pid, &source_branch).await
            } else {
                None
            }
        })
    };

    // Collect results
    let mut collected_summary: Option<MrSummary> = None;
    let mut collected_file_reviews: Vec<FileReview> = Vec::new();
    let mut collected_edge_cases: Vec<EdgeCase> = Vec::new();
    let mut collected_related_files: Vec<RelatedFile> = Vec::new();
    let mut collected_file_activity: Option<FileActivityData> = None;
    let mut collected_verification: Option<VerificationData> = None;

    // Carry forward unchanged file reviews from cache
    if let Some(prev) = prev_review {
        for fr in &prev.file_reviews {
            let is_changed = files_to_review.iter().any(|f| f.file_path == fr.file_path);
            if !is_changed {
                collected_file_reviews.push(fr.clone());
                // Emit cached file review — match Otto's StreamChunk shape
                let _ = send
                    .send(json!({
                        "type": "STREAM_FILE_REVIEW_COMPLETE",
                        "payload": { "fileReview": serde_json::to_value(fr).unwrap_or_default() },
                    }))
                    .await;
            }
        }
    }

    // =====================================================================
    // Phase 1: Summary + File Activity (parallel)
    // =====================================================================
    // Both run concurrently via tokio::spawn. File activity is pure GitLab
    // API work (no AI), so it doesn't need the AI semaphore. Summary does.
    // File activity results feed into Phase 2 code review prompts.

    // --- Spawn file activity task ---
    let file_activity_handle = if tasks.contains(&ReviewTask::FileActivity) {
        if let Some(project_id) = mr.project_id {
            let gl_cfg_fa = gl_cfg.clone();
            let cancel_fa = cancel.clone();
            let send_fa = send.clone();
            let mr_iid = mr.mr_iid;
            let changed_file_paths: HashSet<String> = mr
                .diff_files
                .iter()
                .map(|f| f.file_path.clone())
                .collect();

            Some(tokio::spawn(async move {
                let _ = send_fa
                    .send(json!({ "type": "STREAM_PROGRESS", "payload": { "message": "fetching file activity..." } }))
                    .await;

                let result = tokio::time::timeout(
                    std::time::Duration::from_secs(FILE_ACTIVITY_TIMEOUT_SECS),
                    fetch_file_activity(&gl_cfg_fa, project_id, mr_iid, &changed_file_paths, &cancel_fa),
                )
                .await;

                match result {
                    Ok(Ok(activity_data)) => {
                        let _ = send_fa
                            .send(json!({
                                "type": "STREAM_FILE_ACTIVITY_COMPLETE",
                                "payload": { "fileActivity": serde_json::to_value(&activity_data).unwrap_or_default() },
                            }))
                            .await;
                        Some(activity_data)
                    }
                    Ok(Err(e)) => {
                        warn!("file activity task failed: {}", e);
                        let _ = send_fa
                            .send(json!({ "type": "STREAM_TASK_ERROR", "payload": { "task": "fileActivity", "error": e.to_string() } }))
                            .await;
                        None
                    }
                    Err(_) => {
                        warn!("file activity task timed out after {}s", FILE_ACTIVITY_TIMEOUT_SECS);
                        let _ = send_fa
                            .send(json!({ "type": "STREAM_TASK_ERROR", "payload": { "task": "fileActivity", "error": format!("timed out after {}s", FILE_ACTIVITY_TIMEOUT_SECS) } }))
                            .await;
                        None
                    }
                }
            }))
        } else {
            None
        }
    } else {
        None
    };

    // --- Collect repo config (awaited before summary so all AI tasks can use it) ---
    let repo_config_result = match repo_config_handle.await {
        Ok(cfg) => cfg,
        Err(e) => {
            warn!("repo config task panicked: {}", e);
            None
        }
    };
    let repo_config_text = repo_config_result.as_ref().map(repo_config::format_for_prompt);
    let repo_config_str = repo_config_text.as_deref();

    // --- Run summary task (on current task, streams deltas inline) ---
    if tasks.contains(&ReviewTask::Summary) && !cancel.is_cancelled() {
        let _ = send
            .send(json!({ "type": "STREAM_PROGRESS", "payload": { "message": "generating summary..." } }))
            .await;

        let (delta_tx, mut delta_rx) = mpsc::channel::<String>(64);
        let send_clone = send.clone();

        // Forward deltas — must match Otto's StreamChunk shape: { type, payload: { content } }
        let delta_forwarder = tokio::spawn(async move {
            while let Some(delta) = delta_rx.recv().await {
                let _ = send_clone
                    .send(json!({ "type": "STREAM_SUMMARY_DELTA", "payload": { "content": delta } }))
                    .await;
            }
        });

        // Acquire AI semaphore for the summary call
        let _ai_permit = if let Some(ref sem) = ai_semaphore {
            Some(sem.clone().acquire_owned().await.expect("ai semaphore closed"))
        } else {
            None
        };

        match service::generate_summary(cfg, mr, None, Some(&delta_tx), cancel.clone(), repo_config_str).await {
            Ok(summary) => {
                drop(_ai_permit);
                drop(delta_tx);
                let _ = delta_forwarder.await;
                let _ = send
                    .send(json!({
                        "type": "STREAM_SUMMARY_COMPLETE",
                        "payload": { "summary": serde_json::to_value(&summary).unwrap_or_default() },
                    }))
                    .await;
                collected_summary = Some(summary);
            }
            Err(e) => {
                drop(_ai_permit);
                drop(delta_tx);
                let _ = delta_forwarder.await;
                warn!("summary task failed: {}", e);
                let _ = send
                    .send(json!({ "type": "STREAM_TASK_ERROR", "payload": { "task": "summary", "error": e.to_string() } }))
                    .await;
            }
        }
    }

    // --- Collect file activity result (wait for the spawned task) ---
    if let Some(handle) = file_activity_handle {
        match handle.await {
            Ok(Some(data)) => collected_file_activity = Some(data),
            Ok(None) => {} // task failed or timed out — already logged
            Err(e) => warn!("file activity task panicked: {}", e),
        }
    }

    if cancel.is_cancelled() {
        let _ = send.send(json!({ "type": "STREAM_REVIEW_PAUSED", "payload": { "reason": "cancelled" } })).await;
        return None;
    }

    // =====================================================================
    // Phase 2: Code review (batched, concurrent) + edge cases + related files
    // =====================================================================
    // File activity is now available to inject into per-file review context.

    // Fetch team-wide reviewer preferences (learned from accept/dismiss patterns).
    // Done once before the file loop — same prefs apply to all files in this MR.
    let team_prefs = if tasks.contains(&ReviewTask::CodeReview) {
        match super::prefs::get_team_prefs(pool, &mr.project_path).await {
            Ok(prefs) => prefs,
            Err(e) => {
                debug!("failed to load team prefs (non-fatal): {}", e);
                None
            }
        }
    } else {
        None
    };

    if tasks.contains(&ReviewTask::CodeReview) && !files_to_review.is_empty() {
        let total_files = files_to_review.len();
        let _ = send
            .send(json!({
                "type": "STREAM_PROGRESS",
                "payload": { "message": format!("reviewing {} files...", total_files) },
            }))
            .await;

        let mut completed = 0usize;

        // Process in batches of FILE_REVIEW_CONCURRENCY
        for batch in files_to_review.chunks(FILE_REVIEW_CONCURRENCY) {
            if cancel.is_cancelled() {
                break;
            }

            let mut handles = Vec::new();

            for file in batch {
                let cfg = cfg.clone();
                let mr = mr.clone();
                let file = (*file).clone();
                let cancel = cancel.clone();
                let send = send.clone();
                let gl_cfg = gl_cfg.clone();
                let ai_sem = ai_semaphore.clone();

                // Build file activity context string for this file (matches Otto's format)
                let file_activity_ctx = build_file_activity_context(
                    &file.file_path,
                    collected_file_activity.as_ref(),
                );

                // Clone repo config text for the spawned task ('static requirement)
                let repo_config_owned = repo_config_text.clone();
                let team_prefs_owned = team_prefs.clone();

                let handle = tokio::spawn(async move {
                    let context = FileReviewContext {
                        file_activity: file_activity_ctx,
                        repo_config: repo_config_owned,
                        reviewer_prefs: team_prefs_owned,
                        ..FileReviewContext::default()
                    };

                    // Fetch full file content from source branch for richer context
                    let file_content = if let Some(pid) = mr.project_id {
                        gitlab::fetch_file_content(
                            &gl_cfg,
                            pid,
                            &file.file_path,
                            &mr.source_branch,
                        )
                        .await
                        .ok()
                    } else {
                        None
                    };

                    // Acquire AI semaphore before the AI call
                    let _ai_permit = if let Some(ref sem) = ai_sem {
                        Some(sem.clone().acquire_owned().await.expect("ai semaphore closed"))
                    } else {
                        None
                    };

                    // Per-file timeout
                    let result = tokio::time::timeout(
                        std::time::Duration::from_secs(FILE_REVIEW_TIMEOUT_SECS),
                        service::review_file(
                            &cfg,
                            &mr,
                            &file,
                            file_content.as_deref(),
                            &context,
                            None, // no per-file streaming for now
                            cancel,
                        ),
                    )
                    .await;

                    drop(_ai_permit);

                    match result {
                        Ok(Ok(review)) => {
                            let _ = send
                                .send(json!({
                                    "type": "STREAM_FILE_REVIEW_COMPLETE",
                                    "payload": { "fileReview": serde_json::to_value(&review).unwrap_or_default() },
                                }))
                                .await;
                            Some(review)
                        }
                        Ok(Err(e)) => {
                            warn!("file review failed for {}: {}", file.file_path, e);
                            let _ = send
                                .send(json!({
                                    "type": "STREAM_TASK_ERROR",
                                    "payload": { "task": "codeReview", "error": format!("{}: {}", file.file_path, e) },
                                }))
                                .await;
                            None
                        }
                        Err(_) => {
                            warn!("file review timed out for {}", file.file_path);
                            let _ = send
                                .send(json!({
                                    "type": "STREAM_TASK_ERROR",
                                    "payload": { "task": "codeReview", "error": format!("{}: timed out after {}s", file.file_path, FILE_REVIEW_TIMEOUT_SECS) },
                                }))
                                .await;
                            None
                        }
                    }
                });

                handles.push(handle);
            }

            // Await batch
            for handle in handles {
                if let Ok(Some(review)) = handle.await {
                    collected_file_reviews.push(review);
                    completed += 1;
                    let _ = send
                        .send(json!({
                            "type": "STREAM_PROGRESS",
                            "payload": { "message": format!("reviewed {}/{} files", completed, total_files) },
                        }))
                        .await;
                }
            }
        }
    }

    if cancel.is_cancelled() {
        let _ = send.send(json!({ "type": "STREAM_REVIEW_PAUSED", "payload": { "reason": "cancelled" } })).await;
        return None;
    }

    // --- Edge cases ---
    if tasks.contains(&ReviewTask::EdgeCases) {
        if let Some(ref summary) = collected_summary {
            let _ = send
                .send(json!({ "type": "STREAM_PROGRESS", "payload": { "message": "analyzing edge cases..." } }))
                .await;

            let _ai_permit = if let Some(ref sem) = ai_semaphore {
                Some(sem.clone().acquire_owned().await.expect("ai semaphore closed"))
            } else {
                None
            };

            match service::analyze_edge_cases(cfg, mr, summary, None, cancel.clone(), repo_config_str).await {
                Ok(cases) => {
                    drop(_ai_permit);
                    let _ = send
                        .send(json!({
                            "type": "STREAM_EDGE_CASES_COMPLETE",
                            "payload": { "edgeCases": serde_json::to_value(&cases).unwrap_or_default() },
                        }))
                        .await;
                    collected_edge_cases = cases;
                }
                Err(e) => {
                    drop(_ai_permit);
                    warn!("edge cases task failed: {}", e);
                    let _ = send
                        .send(json!({ "type": "STREAM_TASK_ERROR", "payload": { "task": "edgeCases", "error": e.to_string() } }))
                        .await;
                }
            }
        }
    }

    if cancel.is_cancelled() {
        let _ = send.send(json!({ "type": "STREAM_REVIEW_PAUSED", "payload": { "reason": "cancelled" } })).await;
        return None;
    }

    // --- Related files ---
    if tasks.contains(&ReviewTask::RelatedFiles) {
        let _ = send
            .send(json!({ "type": "STREAM_PROGRESS", "payload": { "message": "discovering related files..." } }))
            .await;

        let _ai_permit = if let Some(ref sem) = ai_semaphore {
            Some(sem.clone().acquire_owned().await.expect("ai semaphore closed"))
        } else {
            None
        };

        match service::discover_related_files(cfg, mr, cancel.clone(), repo_config_str).await {
            Ok(files) => {
                drop(_ai_permit);
                let _ = send
                    .send(json!({
                        "type": "STREAM_RELATED_FILES_COMPLETE",
                        "payload": { "files": serde_json::to_value(&files).unwrap_or_default() },
                    }))
                    .await;
                collected_related_files = files;
            }
            Err(e) => {
                drop(_ai_permit);
                warn!("related files task failed: {}", e);
                let _ = send
                    .send(json!({ "type": "STREAM_TASK_ERROR", "payload": { "task": "relatedFiles", "error": e.to_string() } }))
                    .await;
            }
        }
    }

    if cancel.is_cancelled() {
        let _ = send.send(json!({ "type": "STREAM_REVIEW_PAUSED", "payload": { "reason": "cancelled" } })).await;
        return None;
    }

    // --- Phase 3: Verification layer ---
    let mut collected_adversarial: Option<AdversarialTestData> = None;
    let mut collected_contracts: Option<ContractData> = None;
    let mut collected_behavioral_delta: Option<BehavioralDeltaData> = None;

    // Adversarial tests (needs edge cases)
    if tasks.contains(&ReviewTask::AdversarialTests) && !collected_edge_cases.is_empty() {
        let _ = send
            .send(json!({ "type": "STREAM_PROGRESS", "payload": { "message": "generating adversarial tests..." } }))
            .await;

        let _ai_permit = if let Some(ref sem) = ai_semaphore {
            Some(sem.clone().acquire_owned().await.expect("ai semaphore closed"))
        } else {
            None
        };

        match service::generate_adversarial_tests(cfg, mr, &collected_edge_cases, cancel.clone(), repo_config_str).await {
            Ok(data) => {
                drop(_ai_permit);
                let _ = send
                    .send(json!({
                        "type": "STREAM_ADVERSARIAL_TESTS_COMPLETE",
                        "payload": { "data": serde_json::to_value(&data).unwrap_or_default() },
                    }))
                    .await;
                collected_adversarial = Some(data);
            }
            Err(e) => {
                drop(_ai_permit);
                warn!("adversarial tests task failed: {}", e);
                let _ = send
                    .send(json!({ "type": "STREAM_TASK_ERROR", "payload": { "task": "adversarialTests", "error": e.to_string() } }))
                    .await;
            }
        }
    }

    if cancel.is_cancelled() {
        let _ = send.send(json!({ "type": "STREAM_REVIEW_PAUSED", "payload": { "reason": "cancelled" } })).await;
        return None;
    }

    // Contracts
    if tasks.contains(&ReviewTask::Contracts) {
        let _ = send
            .send(json!({ "type": "STREAM_PROGRESS", "payload": { "message": "inferring contracts..." } }))
            .await;

        let _ai_permit = if let Some(ref sem) = ai_semaphore {
            Some(sem.clone().acquire_owned().await.expect("ai semaphore closed"))
        } else {
            None
        };

        match service::generate_contracts(cfg, mr, cancel.clone(), repo_config_str).await {
            Ok(data) => {
                drop(_ai_permit);
                let _ = send
                    .send(json!({
                        "type": "STREAM_CONTRACTS_COMPLETE",
                        "payload": { "data": serde_json::to_value(&data).unwrap_or_default() },
                    }))
                    .await;
                collected_contracts = Some(data);
            }
            Err(e) => {
                drop(_ai_permit);
                warn!("contracts task failed: {}", e);
                let _ = send
                    .send(json!({ "type": "STREAM_TASK_ERROR", "payload": { "task": "contracts", "error": e.to_string() } }))
                    .await;
            }
        }
    }

    if cancel.is_cancelled() {
        let _ = send.send(json!({ "type": "STREAM_REVIEW_PAUSED", "payload": { "reason": "cancelled" } })).await;
        return None;
    }

    // Behavioral delta (needs summary)
    if tasks.contains(&ReviewTask::BehavioralDelta) {
        if let Some(ref summary) = collected_summary {
            let _ = send
                .send(json!({ "type": "STREAM_PROGRESS", "payload": { "message": "analyzing behavioral delta..." } }))
                .await;

            let _ai_permit = if let Some(ref sem) = ai_semaphore {
                Some(sem.clone().acquire_owned().await.expect("ai semaphore closed"))
            } else {
                None
            };

            match service::analyze_behavioral_delta(cfg, mr, summary, cancel.clone(), repo_config_str).await {
                Ok(data) => {
                    drop(_ai_permit);
                    let _ = send
                        .send(json!({
                            "type": "STREAM_BEHAVIORAL_DELTA_COMPLETE",
                            "payload": { "data": serde_json::to_value(&data).unwrap_or_default() },
                        }))
                        .await;
                    collected_behavioral_delta = Some(data);
                }
                Err(e) => {
                    drop(_ai_permit);
                    warn!("behavioral delta task failed: {}", e);
                    let _ = send
                        .send(json!({ "type": "STREAM_TASK_ERROR", "payload": { "task": "behavioralDelta", "error": e.to_string() } }))
                        .await;
                }
            }
        }
    }

    // --- Trust assessment (uses verification results) ---
    let trust_assessment = trust::compute_trust(
        collected_adversarial.as_ref(),
        collected_contracts.as_ref(),
        None, // No CI execution — AI-only mode
    );
    let _ = send
        .send(json!({
            "type": "STREAM_TRUST_COMPLETE",
            "payload": { "trust": serde_json::to_value(&trust_assessment).unwrap_or_default() },
        }))
        .await;

    // Build verification data
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;

    let has_verification = collected_adversarial.is_some()
        || collected_contracts.is_some()
        || collected_behavioral_delta.is_some();

    collected_verification = if has_verification {
        Some(VerificationData {
            status: VerificationDataStatus::Complete,
            error: None,
            adversarial_tests: collected_adversarial,
            contracts: collected_contracts,
            behavioral_delta: collected_behavioral_delta,
            execution: None,
            execution_method: CiExecutionMethod::AiOnly,
            trust: Some(trust_assessment),
            generated_at: Some(now),
            executed_at: None,
        })
    } else {
        None
    };

    // --- Build and cache the final review ---
    let cached_review = CachedReview {
        version: 1,
        summary: collected_summary,
        file_reviews: collected_file_reviews,
        related_files: collected_related_files,
        edge_cases: collected_edge_cases,
        file_activity: collected_file_activity,
        ac_validation: None,
        verification: collected_verification,
    };

    // Save to cache
    cache::save(
        pool,
        &mr.project_path,
        mr.mr_iid,
        &diff_hash,
        &cached_review,
        &current_file_hashes,
        cfg.cache.review_ttl_days,
        cfg.cache.max_cached_reviews,
    )
    .await;

    // Signal completion
    let _ = send.send(json!({ "type": "STREAM_ALL_COMPLETE" })).await;

    info!(
        "review complete: {}:!{} ({} file reviews, {} edge cases)",
        mr.project_path,
        mr.mr_iid,
        cached_review.file_reviews.len(),
        cached_review.edge_cases.len(),
    );

    Some(cached_review)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Emit all parts of a cached review as stream chunks.
/// All chunks must match Otto's StreamChunk discriminated union shape.
async fn emit_cached_review(send: &ChunkSender, review: &CachedReview) {
    if let Some(ref summary) = review.summary {
        let _ = send
            .send(json!({
                "type": "STREAM_SUMMARY_COMPLETE",
                "payload": { "summary": serde_json::to_value(summary).unwrap_or_default() },
            }))
            .await;
    }

    for fr in &review.file_reviews {
        let _ = send
            .send(json!({
                "type": "STREAM_FILE_REVIEW_COMPLETE",
                "payload": { "fileReview": serde_json::to_value(fr).unwrap_or_default() },
            }))
            .await;
    }

    if !review.edge_cases.is_empty() {
        let _ = send
            .send(json!({
                "type": "STREAM_EDGE_CASES_COMPLETE",
                "payload": { "edgeCases": serde_json::to_value(&review.edge_cases).unwrap_or_default() },
            }))
            .await;
    }

    if !review.related_files.is_empty() {
        let _ = send
            .send(json!({
                "type": "STREAM_RELATED_FILES_COMPLETE",
                "payload": { "files": serde_json::to_value(&review.related_files).unwrap_or_default() },
            }))
            .await;
    }

    if let Some(ref activity) = review.file_activity {
        let _ = send
            .send(json!({
                "type": "STREAM_FILE_ACTIVITY_COMPLETE",
                "payload": { "fileActivity": serde_json::to_value(activity).unwrap_or_default() },
            }))
            .await;
    }

    if let Some(ref verification) = review.verification {
        if let Some(ref adversarial) = verification.adversarial_tests {
            let _ = send
                .send(json!({
                    "type": "STREAM_ADVERSARIAL_TESTS_COMPLETE",
                    "payload": { "data": serde_json::to_value(adversarial).unwrap_or_default() },
                }))
                .await;
        }

        if let Some(ref contracts) = verification.contracts {
            let _ = send
                .send(json!({
                    "type": "STREAM_CONTRACTS_COMPLETE",
                    "payload": { "data": serde_json::to_value(contracts).unwrap_or_default() },
                }))
                .await;
        }

        if let Some(ref behavioral_delta) = verification.behavioral_delta {
            let _ = send
                .send(json!({
                    "type": "STREAM_BEHAVIORAL_DELTA_COMPLETE",
                    "payload": { "data": serde_json::to_value(behavioral_delta).unwrap_or_default() },
                }))
                .await;
        }

        if let Some(ref trust) = verification.trust {
            let _ = send
                .send(json!({
                    "type": "STREAM_TRUST_COMPLETE",
                    "payload": { "trust": serde_json::to_value(trust).unwrap_or_default() },
                }))
                .await;
        }
    }
}

/// Fetch file activity data from GitLab with concurrency, cancellation, and MR cap.
///
/// This is extracted from the main pipeline so it can be spawned as a parallel task.
/// Uses `buffer_unordered` to fetch changed paths for multiple MRs concurrently
/// instead of the old sequential loop that could take hours on active projects.
async fn fetch_file_activity(
    gl_cfg: &gitlab::GitLabConfig,
    project_id: i64,
    current_mr_iid: u64,
    changed_file_paths: &HashSet<String>,
    cancel: &CancellationToken,
) -> Result<FileActivityData, String> {
    use futures::stream::{self, StreamExt};

    // Look back 30 days
    let since = chrono::Utc::now()
        .checked_sub_signed(chrono::Duration::days(30))
        .unwrap_or_else(chrono::Utc::now)
        .format("%Y-%m-%dT%H:%M:%SZ")
        .to_string();

    let recent_mrs = gitlab::fetch_recent_merged_mrs(gl_cfg, project_id, &since)
        .await
        .map_err(|e| e.to_string())?;

    // Filter out the current MR and cap at FILE_ACTIVITY_MAX_MRS
    let mrs_to_check: Vec<_> = recent_mrs
        .into_iter()
        .filter(|mr| mr.iid != current_mr_iid)
        .take(FILE_ACTIVITY_MAX_MRS)
        .collect();

    debug!(
        "file activity: checking {} recent MRs (cap={})",
        mrs_to_check.len(),
        FILE_ACTIVITY_MAX_MRS,
    );

    // Fetch changed paths concurrently with buffer_unordered.
    // Each future gets its own cloned config to satisfy 'static requirement.
    let results: Vec<_> = stream::iter(mrs_to_check.into_iter().map(|mr| {
        let gl_cfg = gl_cfg.clone();
        let cancel = cancel.clone();
        async move {
            if cancel.is_cancelled() {
                return None;
            }
            match gitlab::fetch_mr_changed_paths(&gl_cfg, project_id, mr.iid).await {
                Ok(paths) => Some((
                    mr.iid,
                    mr.title,
                    mr.author.map(|a| a.username).unwrap_or_default(),
                    mr.merged_at.unwrap_or_default(),
                    mr.web_url,
                    paths,
                )),
                Err(e) => {
                    debug!("failed to fetch changed paths for MR !{}: {}", mr.iid, e);
                    None
                }
            }
        }
    }))
    .buffer_unordered(FILE_ACTIVITY_CONCURRENCY)
    .collect()
    .await;

    // Build file activity entries from the results
    let mut file_activities: Vec<FileActivity> = Vec::new();
    let mut total_recent = 0u32;

    for result in results.into_iter().flatten() {
        let (iid, title, author, merged_at, web_url, paths) = result;
        for path in &paths {
            if changed_file_paths.contains(path) {
                let entry = RecentMr {
                    iid,
                    title: title.clone(),
                    author: author.clone(),
                    merged_at: merged_at.clone(),
                    web_url: web_url.clone(),
                };
                total_recent += 1;

                if let Some(fa) = file_activities.iter_mut().find(|fa| fa.file_path == *path) {
                    fa.recent_mrs.push(entry);
                } else {
                    file_activities.push(FileActivity {
                        file_path: path.clone(),
                        recent_mrs: vec![entry],
                    });
                }
            }
        }
    }

    Ok(FileActivityData {
        file_activities,
        total_recent_mrs: total_recent,
        lookback_days: 30,
    })
}

/// Build a markdown context string for a file's recent activity.
/// Matches Otto's format so the AI gets the same context regardless of
/// whether the review runs locally or through Botto.
fn build_file_activity_context(
    file_path: &str,
    activity_data: Option<&FileActivityData>,
) -> Option<String> {
    let data = activity_data?;
    let activity = data.file_activities.iter().find(|a| a.file_path == file_path)?;
    if activity.recent_mrs.is_empty() {
        return None;
    }

    let mut lines = vec![format!(
        "This file was also modified in {} recent MR(s) in the last {} days:",
        activity.recent_mrs.len(),
        data.lookback_days,
    )];

    for mr in &activity.recent_mrs {
        lines.push(format!(
            "- !{} \"{}\" by @{} (merged {})",
            mr.iid, mr.title, mr.author, mr.merged_at,
        ));
    }

    Some(lines.join("\n"))
}
