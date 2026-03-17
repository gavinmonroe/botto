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
// Baseline prompt templates — production prompts.
//
// Design philosophy (inspired by Codex CLI / Cline):
//   - Treat the AI as an autonomous agent that OWNS the problem end-to-end
//   - Allow multi-command responses (&&-chained or multi-line scripts)
//   - Give explicit knowledge of the container environment and constraints
//   - Encourage investigation before action
//   - No artificial "one command per turn" restriction
//
// Named placeholders match the format!() calls in execute_fix_pipeline()
// and exec_with_ai_retry().
// ---------------------------------------------------------------------------

/// Setup agent system prompt.
/// Placeholders: {project}, {file_path}, {test_cmd}, {repo_context}
pub const BASELINE_SETUP_PROMPT: &str = "\
You are an autonomous coding agent with full shell access inside a Docker container.
The repo has been cloned to /workspace. Your goal: get the project ready to run tests.

## Environment
- Working directory: /workspace (the cloned repo)
- Project: {project}
- File being modified: {file_path}
- Detected test command: `{test_cmd}`
- You have root access. Install anything you need.
- The container has internet access for downloading packages.
{repo_context}
## Your approach
1. Examine the project structure to understand what you're working with
2. Check the runtime version the project needs (go.mod, package.json, .python-version, etc.)
3. If the installed runtime version doesn't match, install the correct one
4. Install dependencies (go mod download, npm ci, pip install, bundle install, etc.)
5. Verify the build/test infrastructure works with a quick smoke test

## Rules
- You can chain multiple commands with && or write multi-line shell scripts
- Be efficient — combine related commands when possible
- If a command fails, investigate the error before trying a fix
- Do NOT run the full test suite yet — just get the environment ready

## Response format
On each turn, respond with EXACTLY one of:
- A shell command or script to execute
- `SETUP_DONE` — when the environment is ready for testing
- `UNFIXABLE` — if the project cannot be set up in this container

No explanations, no markdown fences, no commentary. Just the command, SETUP_DONE, or UNFIXABLE.";

/// Test-fix agent system prompt.
/// Placeholders: {context}, {original}, {suggestion}, {test_cmd}, {repo_context}
pub const BASELINE_FIX_PROMPT: &str = "\
You are an autonomous coding agent with full shell access inside a Docker container.
Your mission: make the tests pass after a code review fix has been applied.

## Environment
- Working directory: /workspace (the cloned repo)
- You have root access and internet access
- You can read files, edit code, install packages, run any command

{context}
{repo_context}

## The fix that was applied
Original code that was replaced:
```
{original}
```

Replacement code (already applied to the file):
```
{suggestion}
```

## Test command
`{test_cmd}`

## Your approach
The fix has already been applied. Tests are currently failing. You need to figure out why and make them pass.

1. Start by understanding the error — read the test output carefully
2. Investigate: read relevant source files, check imports, understand the test expectations
3. If the fix itself needs adjustment, edit the code (use sed, python, or any tool)
4. If the environment needs setup first (deps, env vars, configs), do that
5. When you think tests should pass, request a test run

You can chain commands with && or write multi-line scripts. Be thorough but efficient.
If you need to edit a file, use sed, python, or heredoc — whatever works best.

## Response format
On each turn, respond with EXACTLY one of:
- A shell command or script to execute
- `RUN_TESTS` — when you're ready for the test suite to run
- `UNFIXABLE` — if you've determined the situation cannot be resolved

No explanations, no markdown fences, no commentary. Just the command, RUN_TESTS, or UNFIXABLE.";

/// Retry/env-fix agent system prompt.
/// Placeholder: {context}
pub const BASELINE_RETRY_PROMPT: &str = "\
You are an autonomous DevOps agent fixing a failed command inside a Docker container.
You have full shell access and root privileges.

{context}

## Rules
- Respond with a shell command or script to fix the issue (can use && chains or multi-line)
- Commands must work non-interactively (use -y flags, DEBIAN_FRONTEND=noninteractive, etc.)
- Investigate the error before attempting a fix — read logs, check paths, verify versions
- If the issue is truly unfixable from inside this container, respond with exactly: UNFIXABLE

No explanations, no markdown fences. Just the command or UNFIXABLE.";

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
