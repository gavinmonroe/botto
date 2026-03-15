// ---------------------------------------------------------------------------
// Harness orchestrator — the self-evolving prompt engineering loop.
//
// This is the main entry point for harness runs. It coordinates:
//   1. Loading/creating the baseline prompt variant
//   2. Loading/generating test cases (diverse selection)
//   3. For each round:
//      a. Judge generates N prompt mutations
//      b. Runner executes each variant against all test cases
//      c. Grader scores the results
//      d. Judge analyzes results and extracts learnings
//      e. Best variant becomes the new baseline
//      f. Everything is saved to memory (markdown + TOML)
//   4. Final report with the best prompt found
// ---------------------------------------------------------------------------

use crate::config::BottoConfig;
use crate::services::events::EventBus;
use crate::services::harness::{grader, judge, memory, prompts, runner, test_case};
use crate::services::harness::types::{HarnessGrade, RoundReport, VariantScore};
use anyhow::Result;
use chrono::Utc;
use sqlx::SqlitePool;
use tracing::{info, warn};

/// Options for a harness run, derived from HarnessConfig + CLI overrides.
#[derive(Debug, Clone)]
pub struct RunOptions {
    pub max_rounds: u32,
    pub variants_per_round: u32,
    pub concurrency: u32,
    pub test_case_count: u32,
}

/// Final summary of a harness run.
#[derive(Debug)]
pub struct RunSummary {
    pub rounds_completed: u32,
    pub best_variant_id: String,
    pub best_score: f64,
    pub baseline_score: f64,
    pub improvement: f64,
    pub total_test_runs: u32,
}

/// Run the full evolution loop.
pub async fn run(
    cfg: &BottoConfig,
    pool: &SqlitePool,
    event_bus: &EventBus,
    opts: RunOptions,
) -> Result<RunSummary> {
    let memory_dir = &cfg.harness.memory_dir;

    // Initialize directory structure
    memory::init_dirs(memory_dir).await?;

    // Load or create baseline variant (v000)
    let baseline = match memory::load_variant(memory_dir, "v000").await {
        Ok(v) => {
            info!("loaded existing baseline variant v000");
            v
        }
        Err(_) => {
            info!("creating baseline variant v000 from production prompts");
            let v = prompts::baseline_variant();
            memory::save_variant(memory_dir, &v).await?;
            v
        }
    };

    // Load or generate test cases
    // ONLY use real GitLab MRs — seed cases have fake branches and can't be cloned.
    // Discovery: hit gitlab-org group MRs → run our review → grab fixable findings.
    // If discovery fails, we can't run the harness — no point testing with fake data.
    let all_cases = {
        // Try dynamic discovery from GitLab
        info!("discovering test cases from real GitLab MRs...");
        let discovered = test_case::discover_from_gitlab(
            cfg,
            pool,
            opts.test_case_count as usize,
            42, // initial seed for first discovery
        )
        .await;

        if !discovered.is_empty() {
            info!("discovered {} test cases from real GitLab MRs", discovered.len());
            for tc in &discovered {
                if let Err(e) = memory::save_test_case(memory_dir, tc).await {
                    warn!("failed to save discovered test case {}: {}", tc.id, e);
                }
            }
            discovered
        } else {
            // Check for previously saved real cases
            let existing = memory::load_all_test_cases(memory_dir).await?;
            let real_cases: Vec<_> = existing.into_iter()
                .filter(|tc| tc.id.starts_with("gl-")) // only real GitLab cases
                .collect();
            if !real_cases.is_empty() {
                info!("using {} previously saved real test cases", real_cases.len());
                real_cases
            } else {
                anyhow::bail!(
                    "harness: could not discover any test cases from GitLab. \
                     Check your GitLab token and network connectivity."
                );
            }
        }
    };

    // Determine starting round
    let start_round = memory::latest_round(memory_dir).await? + 1;
    let end_round = start_round + opts.max_rounds - 1;

    info!(
        "starting harness evolution: rounds {}-{}, {} variants/round, {} test cases, concurrency={}",
        start_round, end_round, opts.variants_per_round, opts.test_case_count, opts.concurrency,
    );

    let mut current_best = baseline;
    let mut baseline_score = 0.0_f64;
    let mut total_test_runs = 0u32;

    for round in start_round..=end_round {
        info!("=== Round {} of {} ===", round, end_round);

        // Select diverse test cases for this round
        let round_cases = test_case::select_diverse(
            &all_cases,
            opts.test_case_count as usize,
            round as u64,
        );
        info!(
            "selected {} diverse test cases for round {}",
            round_cases.len(),
            round,
        );

        // Read evolution history for the judge
        let history = memory::read_summary(memory_dir).await?;

        // Judge generates mutations
        info!("judge generating {} prompt mutations...", opts.variants_per_round);
        let variants = judge::generate_mutations(
            cfg,
            &current_best,
            opts.variants_per_round,
            round,
            &history,
        )
        .await?;

        info!("generated {} variants (including control)", variants.len());

        // Save all variants
        for v in &variants {
            memory::save_variant(memory_dir, v).await?;
        }

        // Run ALL variants in PARALLEL — each variant gets its own Docker containers.
        // We spawn one task per variant, each running against all test cases.
        let mut all_grades: Vec<HarnessGrade> = Vec::new();

        info!(
            "launching {} variants in parallel against {} test cases each...",
            variants.len(),
            round_cases.len(),
        );

        let mut variant_handles = Vec::new();
        for variant in &variants {
            let cfg = cfg.clone();
            let pool = pool.clone();
            let event_bus = event_bus.clone();
            let variant = variant.clone();
            let cases = round_cases.clone();
            let concurrency = opts.concurrency as usize;

            let handle = tokio::spawn(async move {
                info!("variant {} starting against {} test cases...", variant.id, cases.len());
                let results = runner::run_variant(
                    &cfg,
                    &pool,
                    &event_bus,
                    &variant,
                    &cases,
                    concurrency,
                )
                .await;
                info!("variant {} finished — {} results", variant.id, results.len());
                (variant.id.clone(), results)
            });
            variant_handles.push(handle);
        }

        // Wait for ALL variants to finish
        for handle in variant_handles {
            match handle.await {
                Ok((variant_id, results)) => {
                    total_test_runs += results.len() as u32;
                    for result in &results {
                        let grade = grader::grade(result);
                        all_grades.push(grade);
                    }
                    info!("variant {} graded", variant_id);
                }
                Err(e) => {
                    warn!("variant task panicked: {}", e);
                }
            }
        }

        // Compute aggregate scores per variant
        let variant_ids: Vec<String> = variants.iter().map(|v| v.id.clone()).collect();
        let variant_scores: Vec<VariantScore> = variant_ids
            .iter()
            .map(|id| grader::aggregate_variant(id, &all_grades))
            .collect();

        // Find the winner
        let winner_id = variant_scores
            .iter()
            .max_by(|a, b| a.mean_score.partial_cmp(&b.mean_score).unwrap())
            .expect("at least one variant")
            .variant_id
            .clone();

        let winner_score = variant_scores
            .iter()
            .find(|v| v.variant_id == winner_id)
            .map(|v| v.mean_score)
            .unwrap_or(0.0);

        let parent_score = variant_scores
            .iter()
            .find(|v| {
                // The control variant has the same prompts as the parent
                variants
                    .iter()
                    .find(|var| var.id == v.variant_id)
                    .map(|var| var.metadata.mutation_strategy.as_deref() == Some("control"))
                    .unwrap_or(false)
            })
            .map(|v| v.mean_score)
            .unwrap_or(0.0);

        if round == start_round {
            baseline_score = parent_score;
        }

        let improved = winner_score > parent_score;
        let score_delta = winner_score - parent_score;

        info!(
            "round {} winner: {} (score: {:.1}, delta: {:+.1}, improved: {})",
            round, winner_id, winner_score, score_delta, improved,
        );

        // Judge analyzes results
        let (judge_analysis, learnings) = {
            // Build a preliminary report for the judge
            let prelim = RoundReport {
                round,
                variants_tested: variant_ids.clone(),
                variant_scores: variant_scores.clone(),
                winner_id: winner_id.clone(),
                parent_id: current_best.id.clone(),
                improved,
                score_delta,
                judge_analysis: String::new(),
                learnings: vec![],
                completed_at: Utc::now(),
                grades: all_grades.clone(),
            };
            judge::analyze_round(cfg, &prelim, &history).await?
        };

        // Build final round report
        let report = RoundReport {
            round,
            variants_tested: variant_ids,
            variant_scores,
            winner_id: winner_id.clone(),
            parent_id: current_best.id.clone(),
            improved,
            score_delta,
            judge_analysis,
            learnings,
            completed_at: Utc::now(),
            grades: all_grades,
        };

        // Save round report and update summary
        memory::save_round_report(memory_dir, &report).await?;
        memory::append_summary(memory_dir, &report).await?;

        // Update current best
        if improved {
            current_best = variants
                .into_iter()
                .find(|v| v.id == winner_id)
                .expect("winner must be in variants");
            info!("new best variant: {}", current_best.id);
        } else {
            info!(
                "no improvement in round {} — keeping {} as best",
                round, current_best.id,
            );
        }
    }

    let best_score = {
        // Quick score check on the final best
        let final_cases = test_case::select_diverse(
            &all_cases,
            opts.test_case_count as usize,
            end_round as u64 + 1000, // different seed
        );
        let results = runner::run_variant(
            cfg,
            pool,
            event_bus,
            &current_best,
            &final_cases,
            opts.concurrency as usize,
        )
        .await;
        let grades: Vec<_> = results.iter().map(|r| grader::grade(r)).collect();
        grader::mean_score(&grades)
    };

    let summary = RunSummary {
        rounds_completed: end_round - start_round + 1,
        best_variant_id: current_best.id.clone(),
        best_score,
        baseline_score,
        improvement: best_score - baseline_score,
        total_test_runs,
    };

    info!(
        "harness complete: {} rounds, best={} (score={:.1}, baseline={:.1}, improvement={:+.1})",
        summary.rounds_completed,
        summary.best_variant_id,
        summary.best_score,
        summary.baseline_score,
        summary.improvement,
    );

    Ok(summary)
}
