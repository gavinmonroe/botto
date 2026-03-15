// ---------------------------------------------------------------------------
// Harness grader — composite scoring for sandbox run results.
// ---------------------------------------------------------------------------

use crate::services::harness::types::{HarnessGrade, RunResult, ScoreBreakdown};

/// Maximum iterations we consider "reasonable" for scaling the iteration score.
/// Anything above this gets 0 points for iteration efficiency.
const MAX_REASONABLE_ITERATIONS: f64 = 50.0;

/// Maximum wall time (seconds) we consider "reasonable" for scaling the time score.
const MAX_REASONABLE_TIME_SECS: f64 = 600.0;

/// Maximum tokens we consider "reasonable" for scaling the token score.
const MAX_REASONABLE_TOKENS: f64 = 200_000.0;

// Score weights
const WEIGHT_PASS: f64 = 50.0;
const WEIGHT_ITERATIONS: f64 = 25.0;
const WEIGHT_TIME: f64 = 15.0;
const WEIGHT_TOKENS: f64 = 10.0;

/// Grade a single run result into a scored HarnessGrade.
pub fn grade(result: &RunResult) -> HarnessGrade {
    let pass_score = if result.passed { WEIGHT_PASS } else { 0.0 };

    // Iteration efficiency: fewer steps = better. Linear scale from max to 0.
    let iteration_score = if result.passed {
        let ratio = 1.0 - (result.total_iterations as f64 / MAX_REASONABLE_ITERATIONS).min(1.0);
        ratio * WEIGHT_ITERATIONS
    } else {
        0.0 // No credit for iterations if the fix didn't pass
    };

    // Time efficiency: faster = better.
    let time_score = if result.passed {
        let ratio = 1.0 - (result.wall_time_secs / MAX_REASONABLE_TIME_SECS).min(1.0);
        ratio * WEIGHT_TIME
    } else {
        0.0
    };

    // Token efficiency: fewer tokens = better.
    let token_score = if result.passed {
        let ratio = 1.0 - (result.tokens_used as f64 / MAX_REASONABLE_TOKENS).min(1.0);
        ratio * WEIGHT_TOKENS
    } else {
        0.0
    };

    let score = pass_score + iteration_score + time_score + token_score;

    HarnessGrade {
        variant_id: result.variant_id.clone(),
        test_case_id: result.test_case_id.clone(),
        passed: result.passed,
        iterations: result.total_iterations,
        wall_time_secs: result.wall_time_secs,
        tokens_used: result.tokens_used,
        score,
        score_breakdown: ScoreBreakdown {
            pass_score,
            iteration_score,
            time_score,
            token_score,
        },
    }
}

/// Compute the mean score across a set of grades for a single variant.
pub fn mean_score(grades: &[HarnessGrade]) -> f64 {
    if grades.is_empty() {
        return 0.0;
    }
    let sum: f64 = grades.iter().map(|g| g.score).sum();
    sum / grades.len() as f64
}

/// Compute aggregate stats for a variant across multiple test cases.
pub fn aggregate_variant(
    variant_id: &str,
    grades: &[HarnessGrade],
) -> crate::services::harness::types::VariantScore {
    let variant_grades: Vec<_> = grades
        .iter()
        .filter(|g| g.variant_id == variant_id)
        .collect();
    let total = variant_grades.len() as u32;
    let pass_count = variant_grades.iter().filter(|g| g.passed).count() as u32;
    let mean = if total > 0 {
        variant_grades.iter().map(|g| g.score).sum::<f64>() / total as f64
    } else {
        0.0
    };
    let mean_iters = {
        let passing: Vec<_> = variant_grades.iter().filter(|g| g.passed).collect();
        if passing.is_empty() {
            0.0
        } else {
            passing.iter().map(|g| g.iterations as f64).sum::<f64>() / passing.len() as f64
        }
    };

    crate::services::harness::types::VariantScore {
        variant_id: variant_id.to_string(),
        mean_score: mean,
        pass_count,
        total_cases: total,
        mean_iterations: mean_iters,
    }
}
