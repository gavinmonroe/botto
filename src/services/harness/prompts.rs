// ---------------------------------------------------------------------------
// Harness prompts — baseline prompt extraction and variant management.
//
// This module defines `SandboxPrompts`, the injectable prompt set used by
// the sandbox manager. The baseline templates are extracted verbatim from
// the hardcoded prompts in sandbox/manager.rs, with named placeholders
// matching the format!() calls.
//
// Template placeholders (MUST be preserved in all mutations):
//   Setup:  {project}, {file_path}, {test_cmd}
//   Fix:    {context}, {original}, {suggestion}, {test_cmd}
//   Retry:  {context}
//
// The harness evolves these templates. The sandbox manager uses them via
// `SandboxPrompts::default()` in production, or accepts an override from
// the harness runner.
// ---------------------------------------------------------------------------

use crate::services::harness::types::{CodeParams, PromptMetadata, PromptVariant};
use chrono::Utc;

/// Injectable prompt set for the sandbox manager's 3 AI agent loops.
/// Each field is a format-string template with named placeholders.
#[derive(Debug, Clone)]
pub struct SandboxPrompts {
    /// System prompt template for the project setup agent.
    /// Placeholders: {project}, {file_path}, {test_cmd}
    pub setup_system: String,
    /// System prompt template for the test-fix agent.
    /// Placeholders: {context}, {original}, {suggestion}, {test_cmd}
    pub fix_system: String,
    /// System prompt template for the retry/env-fix agent.
    /// Placeholder: {context}
    pub retry_system: String,
    /// Tunable AI call parameters.
    pub code_params: CodeParams,
}

impl Default for SandboxPrompts {
    fn default() -> Self {
        Self {
            setup_system: BASELINE_SETUP_PROMPT.to_string(),
            fix_system: BASELINE_FIX_PROMPT.to_string(),
            retry_system: BASELINE_RETRY_PROMPT.to_string(),
            code_params: CodeParams::default(),
        }
    }
}

// ---------------------------------------------------------------------------
// Baseline prompt templates — extracted from sandbox/manager.rs
//
// These are the production prompts. Named placeholders match the format!()
// calls in execute_fix_pipeline() and exec_with_ai_retry().
// ---------------------------------------------------------------------------

/// Setup agent system prompt (manager.rs:360-376).
/// Placeholders: {project}, {file_path}, {test_cmd}
pub const BASELINE_SETUP_PROMPT: &str = "\
You are a senior DevOps engineer setting up a project inside a Docker container.\n\
The repo has been cloned to /workspace. Your job is to get the project ready to run tests.\n\
\n\
Project: {project}\n\
File being modified: {file_path}\n\
Detected test command: `{test_cmd}`\n\
\n\
Steps you should take:\n\
1. Read the project structure (ls, cat package.json / Gemfile / requirements.txt / etc.)\n\
2. Install the correct runtime and dependencies\n\
3. Set up any required environment (env vars, configs, databases, etc.)\n\
4. Verify the test infrastructure works\n\
\n\
On each turn, respond with ONE of:\n\
- A shell command to run\n\
- `SETUP_DONE` — when the environment is ready for testing\n\
- `UNFIXABLE` — if the project cannot be set up in this container\n\
\n\
Do NOT run the full test suite yet. Just get the environment ready.\n\
Do NOT respond with explanations — only a command, SETUP_DONE, or UNFIXABLE.";

/// Test-fix agent system prompt (manager.rs:580-593).
/// Placeholders: {context}, {original}, {suggestion}, {test_cmd}
pub const BASELINE_FIX_PROMPT: &str = "\
You are a senior software engineer autonomously fixing code inside a Docker container.\n\
You have full shell access. The working directory is /workspace (the cloned repo).\n\
\n\
{context}\n\
\n\
## Original code (being replaced)\n```\n{original}\n```\n\
\n\
## Suggested replacement\n```\n{suggestion}\n```\n\
\n\
## Test command\n`{test_cmd}`\n\
\n\
The fix has already been applied to the file. Tests are failing.\n\
Your goal: make the tests pass while addressing the review comment's concern.\n\
\n\
You control the flow. On each turn, respond with ONE of:\n\
1. A shell command to run (setup env, install deps, read files, edit code, investigate errors, etc.)\n\
2. `RUN_TESTS` — when you're ready for me to run the test suite\n\
3. `UNFIXABLE` — if you've determined the situation cannot be resolved\n\
\n\
Take your time. Set up the environment first if needed. Investigate errors thoroughly.\n\
Do NOT respond with explanations — only a command, RUN_TESTS, or UNFIXABLE.";

/// Retry/env-fix agent system prompt (manager.rs:921-927).
/// Placeholder: {context}
pub const BASELINE_RETRY_PROMPT: &str = "\
You are a DevOps expert fixing issues inside a Docker container during an automated code fix pipeline.\n\
\n\
{context}\n\
\n\
When you identify a fix, respond with ONLY a single shell command (no explanation, no markdown fences).\n\
The command must work non-interactively (use -y flags, no prompts).\n\
If after reviewing the full history you determine the issue is truly unfixable from inside the container, \
respond with exactly: UNFIXABLE";

// ---------------------------------------------------------------------------
// Conversion: PromptVariant ↔ SandboxPrompts
// ---------------------------------------------------------------------------

impl From<&PromptVariant> for SandboxPrompts {
    fn from(variant: &PromptVariant) -> Self {
        Self {
            setup_system: variant.setup_prompt.clone(),
            fix_system: variant.fix_prompt.clone(),
            retry_system: variant.retry_prompt.clone(),
            code_params: variant.code_params.clone(),
        }
    }
}

/// Create the baseline prompt variant (v000) — the current production prompts.
pub fn baseline_variant() -> PromptVariant {
    PromptVariant {
        id: "v000".into(),
        generation: 0,
        parent_id: None,
        setup_prompt: BASELINE_SETUP_PROMPT.into(),
        fix_prompt: BASELINE_FIX_PROMPT.into(),
        retry_prompt: BASELINE_RETRY_PROMPT.into(),
        code_params: CodeParams::default(),
        metadata: PromptMetadata {
            author: "baseline".into(),
            created_at: Utc::now(),
            notes: "Extracted from production sandbox/manager.rs hardcoded prompts".into(),
            mutation_strategy: None,
        },
    }
}

/// Validate that a prompt variant contains all required placeholders.
/// Returns a list of errors (empty = valid).
pub fn validate_variant(variant: &PromptVariant) -> Vec<String> {
    let mut errors = Vec::new();

    // Setup prompt must contain these placeholders
    for ph in &["{project}", "{file_path}", "{test_cmd}"] {
        if !variant.setup_prompt.contains(ph) {
            errors.push(format!("setup_prompt missing placeholder: {}", ph));
        }
    }

    // Fix prompt must contain these placeholders
    for ph in &["{context}", "{original}", "{suggestion}", "{test_cmd}"] {
        if !variant.fix_prompt.contains(ph) {
            errors.push(format!("fix_prompt missing placeholder: {}", ph));
        }
    }

    // Retry prompt must contain this placeholder
    if !variant.retry_prompt.contains("{context}") {
        errors.push("retry_prompt missing placeholder: {context}".into());
    }

    // Code params sanity checks
    let cp = &variant.code_params;
    if cp.setup.temperature < 0.0 || cp.setup.temperature > 2.0 {
        errors.push(format!(
            "setup temperature out of range [0, 2]: {}",
            cp.setup.temperature
        ));
    }
    if cp.fix.temperature < 0.0 || cp.fix.temperature > 2.0 {
        errors.push(format!(
            "fix temperature out of range [0, 2]: {}",
            cp.fix.temperature
        ));
    }
    if cp.retry.temperature < 0.0 || cp.retry.temperature > 2.0 {
        errors.push(format!(
            "retry temperature out of range [0, 2]: {}",
            cp.retry.temperature
        ));
    }
    if cp.history_keep_count >= cp.history_trim_threshold {
        errors.push(format!(
            "history_keep_count ({}) must be < history_trim_threshold ({})",
            cp.history_keep_count, cp.history_trim_threshold
        ));
    }

    errors
}

/// Generate the next variant ID based on existing variants.
/// Pattern: v000, v001, v002, ...
pub fn next_variant_id(existing_ids: &[String]) -> String {
    let max_num = existing_ids
        .iter()
        .filter_map(|id| id.strip_prefix('v'))
        .filter_map(|n| n.parse::<u32>().ok())
        .max()
        .unwrap_or(0);
    format!("v{:03}", max_num + 1)
}
