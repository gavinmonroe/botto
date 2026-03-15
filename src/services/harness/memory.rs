// ---------------------------------------------------------------------------
// Harness memory — persistence layer for prompts, test cases, and learnings.
//
// Directory layout:
//   {memory_dir}/
//   ├── prompts/
//   │   ├── v000.toml          # Baseline (current production prompts)
//   │   ├── v001.toml          # First mutation
//   │   └── ...
//   ├── learnings/
//   │   ├── round-001.md       # Round 1 results + analysis
//   │   └── ...
//   ├── test-cases/
//   │   ├── tc-001.toml        # Cached test case definitions
//   │   └── ...
//   └── summary.md             # Running summary the judge reads for context
// ---------------------------------------------------------------------------

use crate::services::harness::types::{PromptVariant, RoundReport, TestCase};
use anyhow::{Context, Result};
use std::path::{Path, PathBuf};
use tracing::info;

/// Ensures the harness directory structure exists.
pub async fn init_dirs(memory_dir: &Path) -> Result<()> {
    let dirs = ["prompts", "learnings", "test-cases"];
    for sub in &dirs {
        tokio::fs::create_dir_all(memory_dir.join(sub))
            .await
            .with_context(|| format!("failed to create harness dir: {}", sub))?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Prompt variants
// ---------------------------------------------------------------------------

/// Save a prompt variant to `{memory_dir}/prompts/{id}.toml`.
pub async fn save_variant(memory_dir: &Path, variant: &PromptVariant) -> Result<()> {
    let path = memory_dir.join("prompts").join(format!("{}.toml", variant.id));
    let content = toml::to_string_pretty(variant)
        .with_context(|| format!("failed to serialize variant {}", variant.id))?;
    tokio::fs::write(&path, content)
        .await
        .with_context(|| format!("failed to write variant file: {}", path.display()))?;
    info!("saved prompt variant {} to {}", variant.id, path.display());
    Ok(())
}

/// Load a prompt variant from `{memory_dir}/prompts/{id}.toml`.
pub async fn load_variant(memory_dir: &Path, id: &str) -> Result<PromptVariant> {
    let path = memory_dir.join("prompts").join(format!("{}.toml", id));
    let content = tokio::fs::read_to_string(&path)
        .await
        .with_context(|| format!("failed to read variant file: {}", path.display()))?;
    let variant: PromptVariant = toml::from_str(&content)
        .with_context(|| format!("failed to parse variant file: {}", path.display()))?;
    Ok(variant)
}

/// List all saved variant IDs (sorted by name).
pub async fn list_variants(memory_dir: &Path) -> Result<Vec<String>> {
    let dir = memory_dir.join("prompts");
    let mut ids = Vec::new();
    let mut entries = tokio::fs::read_dir(&dir)
        .await
        .with_context(|| format!("failed to read prompts dir: {}", dir.display()))?;
    while let Some(entry) = entries.next_entry().await? {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.ends_with(".toml") {
            ids.push(name.trim_end_matches(".toml").to_string());
        }
    }
    ids.sort();
    Ok(ids)
}

// ---------------------------------------------------------------------------
// Test cases
// ---------------------------------------------------------------------------

/// Save a test case to `{memory_dir}/test-cases/{id}.toml`.
pub async fn save_test_case(memory_dir: &Path, tc: &TestCase) -> Result<()> {
    let path = memory_dir.join("test-cases").join(format!("{}.toml", tc.id));
    let content = toml::to_string_pretty(tc)
        .with_context(|| format!("failed to serialize test case {}", tc.id))?;
    tokio::fs::write(&path, content)
        .await
        .with_context(|| format!("failed to write test case: {}", path.display()))?;
    Ok(())
}

/// Load a test case from `{memory_dir}/test-cases/{id}.toml`.
pub async fn load_test_case(memory_dir: &Path, id: &str) -> Result<TestCase> {
    let path = memory_dir.join("test-cases").join(format!("{}.toml", id));
    let content = tokio::fs::read_to_string(&path)
        .await
        .with_context(|| format!("failed to read test case: {}", path.display()))?;
    let tc: TestCase = toml::from_str(&content)
        .with_context(|| format!("failed to parse test case: {}", path.display()))?;
    Ok(tc)
}

/// Load all test cases from `{memory_dir}/test-cases/`.
pub async fn load_all_test_cases(memory_dir: &Path) -> Result<Vec<TestCase>> {
    let dir = memory_dir.join("test-cases");
    let mut cases = Vec::new();
    let mut entries = tokio::fs::read_dir(&dir)
        .await
        .with_context(|| format!("failed to read test-cases dir: {}", dir.display()))?;
    while let Some(entry) = entries.next_entry().await? {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.ends_with(".toml") {
            let id = name.trim_end_matches(".toml");
            match load_test_case(memory_dir, id).await {
                Ok(tc) => cases.push(tc),
                Err(e) => tracing::warn!("skipping malformed test case {}: {}", id, e),
            }
        }
    }
    cases.sort_by(|a, b| a.id.cmp(&b.id));
    Ok(cases)
}

// ---------------------------------------------------------------------------
// Round reports (markdown)
// ---------------------------------------------------------------------------

/// Write a round report as markdown to `{memory_dir}/learnings/round-{NNN}.md`.
pub async fn save_round_report(memory_dir: &Path, report: &RoundReport) -> Result<PathBuf> {
    let filename = format!("round-{:03}.md", report.round);
    let path = memory_dir.join("learnings").join(&filename);
    let md = render_round_report(report);
    tokio::fs::write(&path, &md)
        .await
        .with_context(|| format!("failed to write round report: {}", path.display()))?;
    info!("saved round {} report to {}", report.round, path.display());
    Ok(path)
}

/// Render a round report as markdown.
fn render_round_report(report: &RoundReport) -> String {
    let mut md = String::new();

    md.push_str(&format!("# Round {} Report\n\n", report.round));
    md.push_str(&format!(
        "**Completed:** {}\n\n",
        report.completed_at.format("%Y-%m-%d %H:%M:%S UTC")
    ));
    md.push_str(&format!(
        "**Parent variant:** `{}`\n\n",
        report.parent_id
    ));
    md.push_str(&format!(
        "**Winner:** `{}` (improved: {})\n\n",
        report.winner_id, report.improved
    ));
    md.push_str(&format!(
        "**Score delta:** {:+.1}\n\n",
        report.score_delta
    ));

    // Variant scores table
    md.push_str("## Variant Scores\n\n");
    md.push_str("| Variant | Mean Score | Pass Rate | Mean Iterations |\n");
    md.push_str("|---------|-----------|-----------|----------------|\n");
    for vs in &report.variant_scores {
        md.push_str(&format!(
            "| `{}` | {:.1} | {}/{} | {:.1} |\n",
            vs.variant_id, vs.mean_score, vs.pass_count, vs.total_cases, vs.mean_iterations,
        ));
    }
    md.push_str("\n");

    // Detailed grades
    md.push_str("## Detailed Grades\n\n");
    md.push_str("| Variant | Test Case | Pass | Iters | Time (s) | Score |\n");
    md.push_str("|---------|-----------|------|-------|----------|-------|\n");
    for g in &report.grades {
        md.push_str(&format!(
            "| `{}` | `{}` | {} | {} | {:.1} | {:.1} |\n",
            g.variant_id,
            g.test_case_id,
            if g.passed { "yes" } else { "no" },
            g.iterations,
            g.wall_time_secs,
            g.score,
        ));
    }
    md.push_str("\n");

    // Judge analysis
    md.push_str("## Judge Analysis\n\n");
    md.push_str(&report.judge_analysis);
    md.push_str("\n\n");

    // Learnings
    if !report.learnings.is_empty() {
        md.push_str("## Key Learnings\n\n");
        for learning in &report.learnings {
            md.push_str(&format!("- {}\n", learning));
        }
        md.push_str("\n");
    }

    md
}

// ---------------------------------------------------------------------------
// Summary file — running log the judge reads for context
// ---------------------------------------------------------------------------

/// Append a round summary to `{memory_dir}/summary.md`.
/// Creates the file if it doesn't exist.
pub async fn append_summary(memory_dir: &Path, report: &RoundReport) -> Result<()> {
    let path = memory_dir.join("summary.md");

    let header = if !path.exists() {
        "# Harness Evolution Summary\n\nRunning log of prompt evolution rounds.\n\n---\n\n"
            .to_string()
    } else {
        String::new()
    };

    let entry = format!(
        "{}## Round {}\n\n\
         - **Winner:** `{}` (score: {:.1})\n\
         - **Parent:** `{}` (delta: {:+.1})\n\
         - **Pass rate:** {}/{}\n\
         - **Improved:** {}\n\
         - **Learnings:** {}\n\n---\n\n",
        header,
        report.round,
        report.winner_id,
        report
            .variant_scores
            .iter()
            .find(|v| v.variant_id == report.winner_id)
            .map(|v| v.mean_score)
            .unwrap_or(0.0),
        report.parent_id,
        report.score_delta,
        report
            .variant_scores
            .iter()
            .find(|v| v.variant_id == report.winner_id)
            .map(|v| v.pass_count)
            .unwrap_or(0),
        report
            .variant_scores
            .iter()
            .find(|v| v.variant_id == report.winner_id)
            .map(|v| v.total_cases)
            .unwrap_or(0),
        report.improved,
        if report.learnings.is_empty() {
            "none".to_string()
        } else {
            report.learnings.join("; ")
        },
    );

    use tokio::io::AsyncWriteExt;
    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .await
        .with_context(|| format!("failed to open summary file: {}", path.display()))?;
    file.write_all(entry.as_bytes()).await?;

    Ok(())
}

/// Read the full summary file (for the judge to use as context).
pub async fn read_summary(memory_dir: &Path) -> Result<String> {
    let path = memory_dir.join("summary.md");
    if path.exists() {
        Ok(tokio::fs::read_to_string(&path).await?)
    } else {
        Ok(String::new())
    }
}

/// Find the latest round number from existing report files.
pub async fn latest_round(memory_dir: &Path) -> Result<u32> {
    let dir = memory_dir.join("learnings");
    if !dir.exists() {
        return Ok(0);
    }
    let mut max_round = 0u32;
    let mut entries = tokio::fs::read_dir(&dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        // Parse "round-001.md" → 1
        if let Some(num_str) = name
            .strip_prefix("round-")
            .and_then(|s| s.strip_suffix(".md"))
        {
            if let Ok(n) = num_str.parse::<u32>() {
                max_round = max_round.max(n);
            }
        }
    }
    Ok(max_round)
}
