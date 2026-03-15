// ---------------------------------------------------------------------------
// Priority scoring — determines review order in the queue.
// Ported from Otto's priority-scorer.ts.
//
// Score range: 0-100. Higher = reviewed sooner.
// Signals: file count, line count, risk labels, age, author activity.
// ---------------------------------------------------------------------------

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriorityInput {
    pub files_changed: u32,
    pub lines_added: u32,
    pub lines_removed: u32,
    pub has_risk_label: bool,
    pub has_security_label: bool,
    pub is_draft: bool,
    pub age_hours: f64,        // hours since MR was opened
    pub approvals_needed: u32, // how many approvals still needed
}

/// Compute a priority score from 0-100.
pub fn compute_score(input: &PriorityInput) -> f64 {
    let mut score: f64 = 50.0; // baseline

    // Size factor: larger MRs get slightly higher priority (more risk)
    // but cap it — massive MRs shouldn't dominate
    let total_lines = (input.lines_added + input.lines_removed) as f64;
    let size_bonus = (total_lines / 100.0).min(15.0);
    score += size_bonus;

    // File count: more files = more integration risk
    let file_bonus = (input.files_changed as f64 * 1.5).min(10.0);
    score += file_bonus;

    // Risk/security labels: significant boost
    if input.has_security_label {
        score += 20.0;
    } else if input.has_risk_label {
        score += 10.0;
    }

    // Age: older MRs get a gentle boost (don't let them rot)
    let age_bonus = (input.age_hours / 24.0).min(10.0); // max +10 after 10 days
    score += age_bonus;

    // Approvals needed: more approvals needed = higher priority
    let approval_bonus = (input.approvals_needed as f64 * 3.0).min(9.0);
    score += approval_bonus;

    // Draft penalty: drafts are lower priority
    if input.is_draft {
        score -= 20.0;
    }

    score.clamp(0.0, 100.0)
}

/// Determine a risk level string from the score.
pub fn risk_level(score: f64) -> &'static str {
    if score >= 75.0 {
        "high"
    } else if score >= 45.0 {
        "medium"
    } else {
        "low"
    }
}

/// Build signal descriptions for the priority.
pub fn describe_signals(input: &PriorityInput) -> Vec<String> {
    let mut signals = Vec::new();

    let total_lines = input.lines_added + input.lines_removed;
    if total_lines > 500 {
        signals.push(format!("large change ({} lines)", total_lines));
    }
    if input.files_changed > 10 {
        signals.push(format!("{} files changed", input.files_changed));
    }
    if input.has_security_label {
        signals.push("security label".into());
    } else if input.has_risk_label {
        signals.push("risk label".into());
    }
    if input.age_hours > 48.0 {
        signals.push(format!("open for {:.0}h", input.age_hours));
    }
    if input.is_draft {
        signals.push("draft".into());
    }

    signals
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_baseline_score() {
        let input = PriorityInput {
            files_changed: 1,
            lines_added: 10,
            lines_removed: 5,
            has_risk_label: false,
            has_security_label: false,
            is_draft: false,
            age_hours: 0.0,
            approvals_needed: 0,
        };
        let score = compute_score(&input);
        assert!(score >= 50.0 && score <= 55.0, "baseline score: {}", score);
    }

    #[test]
    fn test_security_label_boost() {
        let input = PriorityInput {
            files_changed: 1,
            lines_added: 10,
            lines_removed: 5,
            has_risk_label: false,
            has_security_label: true,
            is_draft: false,
            age_hours: 0.0,
            approvals_needed: 0,
        };
        let score = compute_score(&input);
        assert!(score >= 70.0, "security score: {}", score);
    }

    #[test]
    fn test_draft_penalty() {
        let input = PriorityInput {
            files_changed: 1,
            lines_added: 10,
            lines_removed: 5,
            has_risk_label: false,
            has_security_label: false,
            is_draft: true,
            age_hours: 0.0,
            approvals_needed: 0,
        };
        let score = compute_score(&input);
        assert!(score < 40.0, "draft score: {}", score);
    }
}
