// ---------------------------------------------------------------------------
// Harness runner — executes a single prompt variant against a single test case
// using the EXACT same pipeline as production.
//
// Matches the production flow in router/mod.rs handle_fix_request:
//   1. Create DB job (status='pending')
//   2. Enrich FixRequest from GitLab (file_content, mr_title, file_diff, etc.)
//   3. Build SandboxManager with injected prompts + harness_mode
//   4. Call run_fix() — same time-based deadline, same AI conversation loop,
//      same history truncation, same everything
//   5. Capture telemetry (iterations, wall time)
//   6. Return RunResult for grading
//
// The sandbox fix pipeline is time-limited (not step-limited). The AI gets
// a conversation history that's truncated over time so it knows what it tried,
// what happened, and what to do next. We don't cap retries — the deadline does.
// ---------------------------------------------------------------------------

use crate::config::BottoConfig;
use crate::db;
use crate::services::events::EventBus;
use crate::services::gitlab::client::{self as gl, GitLabConfig};
use crate::services::harness::prompts::SandboxPrompts;
use crate::services::harness::types::{
    IterationBreakdown, PromptVariant, RunResult, TestCase,
};
use crate::services::sandbox::manager::{FixRequest, HarnessTelemetry, SandboxManager};
use crate::types::state::MrRef;
use sqlx::SqlitePool;
use std::sync::Arc;
use tracing::{info, warn};

/// Run a single prompt variant against a single test case.
/// Mirrors the production flow exactly — DB job, GitLab enrichment, run_fix.
pub async fn run_single(
    cfg: &BottoConfig,
    pool: &SqlitePool,
    event_bus: &EventBus,
    variant: &PromptVariant,
    test_case: &TestCase,
) -> RunResult {
    let start = std::time::Instant::now();
    let fail = |error: String| -> RunResult {
        RunResult {
            variant_id: variant.id.clone(),
            test_case_id: test_case.id.clone(),
            passed: false,
            total_iterations: 0,
            iteration_breakdown: IterationBreakdown {
                setup_steps: 0,
                fix_steps: 0,
                retry_steps: 0,
            },
            wall_time_secs: start.elapsed().as_secs_f64(),
            tokens_used: 0,
            conversation_log: vec![],
            error: Some(error),
            fix_output: None,
            commit_sha: None,
        }
    };

    // --- Step 1: Create DB job (same as production handle_fix_request) ---
    let job_id = uuid::Uuid::new_v4().to_string();
    let _ = db::queries::insert_sandbox_job(
        pool,
        &job_id,
        &test_case.project_path,
        test_case.mr_iid as i64,
        Some(&format!("harness-{}", test_case.id)),
        "harness",
    )
    .await;

    // --- Step 2: Enrich from GitLab (same parallel fetch as production) ---
    // Production fetches: mr_meta, file_content, mr_changes in parallel.
    // We use what the test case already has, but fill in gaps from GitLab.
    let gl_cfg = GitLabConfig {
        base_url: cfg.gitlab.url.clone(),
        token: cfg.gitlab.bot_token.clone(),
    };

    let (file_content, mr_title, mr_description, file_diff, source_project_path) = {
        // If test case already has these from discovery, use them
        let mut fc = test_case.file_content.clone();
        let mut mt = test_case.mr_title.clone();
        let mut md = test_case.mr_description.clone();
        let mut fd = test_case.file_diff.clone();
        let mut spp: Option<String> = None;

        // Fetch missing fields from GitLab (same as production)
        let project = gl::fetch_project(&gl_cfg, &test_case.project_path).await.ok();
        let project_id = project.as_ref().map(|p| p.id);

        if let Some(pid) = project_id {
            // Parallel fetch — same as production handle_fix_request
            let gl1 = gl_cfg.clone();
            let gl2 = gl_cfg.clone();
            let gl3 = gl_cfg.clone();
            let fp = test_case.file_path.clone();
            let sb = test_case.source_branch.clone();
            let mr_iid = test_case.mr_iid;

            let (mr_meta, fetched_content, mr_changes) = tokio::join!(
                async {
                    if mt.is_none() || md.is_none() {
                        gl::fetch_merge_request(&gl1, pid, mr_iid).await.ok()
                    } else {
                        None
                    }
                },
                async {
                    if fc.is_none() {
                        gl::fetch_file_content(&gl2, pid, &fp, &sb).await.ok()
                    } else {
                        None
                    }
                },
                async {
                    if fd.is_none() {
                        gl::fetch_mr_changes(&gl3, pid, mr_iid).await.ok()
                    } else {
                        None
                    }
                },
            );

            if let Some(meta) = &mr_meta {
                if mt.is_none() { mt = Some(meta.title.clone()); }
                if md.is_none() { md = meta.description.clone(); }
                // Fork detection (same as production)
                if let (Some(src), Some(tgt)) = (meta.source_project_id, meta.target_project_id) {
                    if src != tgt {
                        spp = gl::fetch_project_by_id(&gl_cfg, src)
                            .await
                            .map(|p| p.path_with_namespace)
                            .ok();
                    }
                }
            }
            if fetched_content.is_some() { fc = fetched_content; }
            if let Some(changes) = &mr_changes {
                fd = changes.changes.iter()
                    .find(|c| c.new_path == test_case.file_path || c.old_path == test_case.file_path)
                    .map(|c| c.diff.clone());
            }
        }

        (fc, mt, md, fd, spp)
    };

    // --- Step 3: Build FixRequest (identical fields to production) ---
    let fix_request = FixRequest {
        job_id: job_id.clone(),
        project_path: test_case.project_path.clone(),
        mr_iid: test_case.mr_iid,
        source_branch: test_case.source_branch.clone(),
        comment_id: format!("harness-{}", test_case.id),
        file_path: test_case.file_path.clone(),
        original_code: test_case.original_code.clone(),
        suggestion: test_case.suggestion.clone(),
        comment_body: test_case.comment_body.clone(),
        comment_title: Some(test_case.expected_issue.clone()),
        severity: Some("warning".into()),
        target_branch: Some(test_case.target_branch.clone()),
        start_line: None,
        end_line: None,
        file_content,
        mr_title,
        mr_description,
        file_diff,
        source_project_path,
    };

    // --- Step 4: Build SandboxManager with injected prompts ---
    let prompts = SandboxPrompts::from(variant);
    let telemetry = Arc::new(HarnessTelemetry::new());
    let broadcaster: Arc<dyn Fn(&MrRef, &str) + Send + Sync> =
        Arc::new(|_mr_ref, _msg| {});

    let manager = match SandboxManager::with_prompts(
        cfg.clone(),
        pool.clone(),
        event_bus.clone(),
        broadcaster,
        prompts,
        true, // harness_mode: skip push
        Some(telemetry.clone()),
        None, // no warm pool for harness runs
    ) {
        Some(m) => m,
        None => return fail("sandbox manager unavailable (Docker not running or sandbox disabled)".into()),
    };

    info!(
        "harness: variant {} x case {} — running fix pipeline (time-limited, same as production)",
        variant.id, test_case.id,
    );

    // --- Step 5: Run the fix — EXACT same pipeline as production ---
    // Time-based deadline, AI conversation loop with history truncation,
    // no step limit — the deadline controls when we stop.
    let fix_result = manager.run_fix(fix_request).await;

    // --- Step 6: Capture telemetry ---
    let wall_time = start.elapsed().as_secs_f64();
    let breakdown = telemetry.iteration_breakdown();
    let total = telemetry.total_steps();
    let conversation_log = telemetry.conversation_log.lock().await.clone();

    info!(
        "harness: variant {} x case {} => {} in {:.1}s ({} steps: setup={}, fix={}, retry={})",
        variant.id,
        test_case.id,
        if fix_result.success { "PASS" } else { "FAIL" },
        wall_time,
        total,
        breakdown.setup_steps,
        breakdown.fix_steps,
        breakdown.retry_steps,
    );

    RunResult {
        variant_id: variant.id.clone(),
        test_case_id: test_case.id.clone(),
        passed: fix_result.success,
        total_iterations: total,
        iteration_breakdown: breakdown,
        wall_time_secs: wall_time,
        tokens_used: 0, // TODO: capture from AI client response headers
        conversation_log,
        error: fix_result.error,
        fix_output: fix_result.test_output,
        commit_sha: fix_result.commit_sha,
    }
}

/// Run a variant against multiple test cases with bounded concurrency.
/// Each test case gets its own Docker container — parallel execution.
pub async fn run_variant(
    cfg: &BottoConfig,
    pool: &SqlitePool,
    event_bus: &EventBus,
    variant: &PromptVariant,
    test_cases: &[TestCase],
    max_concurrent: usize,
) -> Vec<RunResult> {
    let semaphore = Arc::new(tokio::sync::Semaphore::new(max_concurrent));
    let mut handles = Vec::new();

    for tc in test_cases {
        let sem = semaphore.clone();
        let cfg = cfg.clone();
        let pool = pool.clone();
        let event_bus = event_bus.clone();
        let variant = variant.clone();
        let tc = tc.clone();

        let handle = tokio::spawn(async move {
            let _permit = sem.acquire().await.expect("semaphore closed");
            run_single(&cfg, &pool, &event_bus, &variant, &tc).await
        });
        handles.push(handle);
    }

    let mut results = Vec::with_capacity(handles.len());
    for handle in handles {
        match handle.await {
            Ok(result) => results.push(result),
            Err(e) => {
                warn!("harness runner task panicked: {}", e);
            }
        }
    }
    results
}
