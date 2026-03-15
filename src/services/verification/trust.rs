// ---------------------------------------------------------------------------
// Trust calibrator — weighted scoring for verification results.
// Ported from Otto's trust-calibrator.ts.
//
// Weights: mutation(0.40) + coverage(0.20) + counterexample(0.20)
//          + independence(0.10) + non-tautological(0.10)
// AI-only scores capped at 65 (can never reach "high" without execution).
// ---------------------------------------------------------------------------

use crate::types::verification::*;

const WEIGHT_MUTATION: f64 = 0.40;
const WEIGHT_COVERAGE: f64 = 0.20;
const WEIGHT_COUNTEREXAMPLE: f64 = 0.20;
const WEIGHT_INDEPENDENCE: f64 = 0.10;
const WEIGHT_NON_TAUTOLOGICAL: f64 = 0.10;

const AI_ONLY_CAP: f64 = 65.0;

const THRESHOLD_HIGH: f64 = 70.0;
const THRESHOLD_MEDIUM: f64 = 40.0;

/// Compute a trust assessment from verification data.
pub fn compute_trust(
    adversarial: Option<&AdversarialTestData>,
    contracts: Option<&ContractData>,
    execution: Option<&CiExecutionResult>,
) -> TrustAssessment {
    let has_execution = execution.is_some();

    // Compute individual signals
    let mutation_score = compute_mutation_signal(adversarial, execution);
    let coverage_delta = compute_coverage_signal(execution);
    let counterexample_quality = compute_counterexample_signal(adversarial);
    let test_independence = compute_independence_signal(adversarial);
    let non_tautological = compute_non_tautological_signal(adversarial, contracts);

    let signals = TrustSignals {
        mutation_score,
        coverage_delta,
        counterexample_quality,
        test_independence,
        non_tautological,
    };

    // Weighted composite
    let mut score = 0.0;
    score += mutation_score.unwrap_or(0.0) * WEIGHT_MUTATION * 100.0;
    score += coverage_delta.unwrap_or(0.0) * WEIGHT_COVERAGE * 100.0;
    score += counterexample_quality * WEIGHT_COUNTEREXAMPLE * 100.0;
    score += test_independence * WEIGHT_INDEPENDENCE * 100.0;
    score += non_tautological * WEIGHT_NON_TAUTOLOGICAL * 100.0;

    // Cap AI-only scores
    if !has_execution && score > AI_ONLY_CAP {
        score = AI_ONLY_CAP;
    }

    let level = if score >= THRESHOLD_HIGH {
        TrustLevel::High
    } else if score >= THRESHOLD_MEDIUM {
        TrustLevel::Medium
    } else {
        TrustLevel::Low
    };

    // Identify surviving mutants (things tests didn't catch)
    let surviving_mutants = identify_surviving_mutants(adversarial);
    let can_strengthen = !surviving_mutants.is_empty();

    let explanation = build_explanation(&level, score, has_execution, &signals);

    TrustAssessment {
        level,
        score,
        signals,
        explanation,
        surviving_mutants,
        can_strengthen,
    }
}

// ---------------------------------------------------------------------------
// Signal computation
// ---------------------------------------------------------------------------

fn compute_mutation_signal(
    adversarial: Option<&AdversarialTestData>,
    execution: Option<&CiExecutionResult>,
) -> Option<f64> {
    // Prefer real execution data
    if let Some(exec) = execution {
        if let Some(ms) = exec.mutation_score {
            return Some(ms);
        }
    }

    // Fall back to AI-reasoned estimate
    if let Some(data) = adversarial {
        if data.total_tests == 0 {
            return None;
        }
        let held = data.total_held as f64;
        let counterexamples = data.total_counterexamples as f64;
        let total = data.total_tests as f64;
        // Heuristic: held tests contribute 1.0, counterexamples 0.3
        let estimate = (held * 1.0 + counterexamples * 0.3) / total;
        return Some(estimate.min(1.0));
    }

    None
}

fn compute_coverage_signal(execution: Option<&CiExecutionResult>) -> Option<f64> {
    execution.and_then(|e| e.coverage_delta)
}

fn compute_counterexample_signal(adversarial: Option<&AdversarialTestData>) -> f64 {
    let data = match adversarial {
        Some(d) => d,
        None => return 0.5, // neutral when no data
    };

    if data.total_tests == 0 {
        return 0.5;
    }

    // More counterexamples found = higher quality testing
    if data.total_counterexamples > 0 {
        // Found real issues — tests are doing their job
        let ratio = data.total_counterexamples as f64 / data.total_tests as f64;
        (0.6 + ratio * 0.4).min(1.0)
    } else if data.total_held > 0 {
        // All properties held — decent but could be tautological
        0.5
    } else {
        0.3
    }
}

fn compute_independence_signal(adversarial: Option<&AdversarialTestData>) -> f64 {
    let data = match adversarial {
        Some(d) => d,
        None => return 0.5,
    };

    if data.total_tests == 0 {
        return 0.5;
    }

    // Heuristic: more files covered = more independent tests
    let files_with_tests = data.files.iter().filter(|f| !f.tests.is_empty()).count();
    let total_files = data.files.len().max(1);
    let coverage_ratio = files_with_tests as f64 / total_files as f64;

    (0.3 + coverage_ratio * 0.7).min(1.0)
}

fn compute_non_tautological_signal(
    adversarial: Option<&AdversarialTestData>,
    contracts: Option<&ContractData>,
) -> f64 {
    let mut score: f64 = 0.5; // neutral baseline

    if let Some(data) = adversarial {
        if data.total_counterexamples > 0 {
            // Found counterexamples = definitely non-tautological
            score = score.max(0.8);
        }
    }

    if let Some(data) = contracts {
        if data.total_violations > 0 {
            // Found violations = contracts are meaningful
            score = score.max(0.7);
        }
        if data.total_verified > 0 && data.total_unknown == 0 {
            score = score.max(0.6);
        }
    }

    score
}

// ---------------------------------------------------------------------------
// Surviving mutants
// ---------------------------------------------------------------------------

fn identify_surviving_mutants(adversarial: Option<&AdversarialTestData>) -> Vec<String> {
    let data = match adversarial {
        Some(d) => d,
        None => return Vec::new(),
    };

    let mut mutants = Vec::new();
    for file in &data.files {
        for (test, result) in file.tests.iter().zip(file.results.iter()) {
            if result.status == PropertyTestStatus::Held && result.ai_reasoned {
                // AI said it holds but wasn't actually executed — potential surviving mutant
                mutants.push(format!(
                    "{}: {} (AI-reasoned only)",
                    test.file_path, test.property
                ));
            }
        }
    }

    mutants.truncate(10); // Cap at 10
    mutants
}

// ---------------------------------------------------------------------------
// Explanation builder
// ---------------------------------------------------------------------------

fn build_explanation(
    level: &TrustLevel,
    score: f64,
    has_execution: bool,
    signals: &TrustSignals,
) -> String {
    let level_str = match level {
        TrustLevel::High => "High",
        TrustLevel::Medium => "Medium",
        TrustLevel::Low => "Low",
    };

    let execution_note = if has_execution {
        "backed by real execution data"
    } else {
        "based on AI reasoning only (capped at 65)"
    };

    let mut parts = vec![format!(
        "{} confidence ({:.0}/100), {}.",
        level_str, score, execution_note
    )];

    if let Some(ms) = signals.mutation_score {
        parts.push(format!("Mutation score: {:.0}%.", ms * 100.0));
    }
    if let Some(cd) = signals.coverage_delta {
        parts.push(format!("Coverage delta: {:.0}%.", cd * 100.0));
    }

    parts.join(" ")
}
