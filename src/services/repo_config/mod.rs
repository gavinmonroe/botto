// ---------------------------------------------------------------------------
// Repo config — cached .otto.json loading, validation, and formatting.
//
// Ported from Otto's repo-config.ts. Provides a single get_or_fetch() entry
// point that the review orchestrator and sandbox manager both call. Results
// are cached in SQLite with a 1-hour TTL so we don't hammer the GitLab API.
//
// Design decisions:
//   - Null sentinel: when .otto.json doesn't exist (the common case), we
//     cache config_json="{}" so subsequent callers skip the API call.
//   - Validation matches Otto exactly: same field caps, same sanitization.
//   - format_for_prompt() produces identical markdown to Otto's version so
//     the AI gets the same context regardless of review source.
//   - GitLab API errors (network, auth) are NOT cached — we return None and
//     let the next caller retry. Only "file doesn't exist" is cached.
// ---------------------------------------------------------------------------

use crate::db;
use crate::services::gitlab::client::{self as gitlab, GitLabConfig, GitLabError};
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;
use tracing::{debug, warn};

/// Default cache TTL: 1 hour. Short enough to pick up .otto.json changes
/// reasonably fast even without webhooks, long enough to avoid API spam.
const DEFAULT_TTL_SECS: i64 = 3600;

/// The null sentinel stored in config_json when no .otto.json exists.
const NULL_SENTINEL: &str = "{}";

// ---------------------------------------------------------------------------
// RepoConfig — the validated, typed representation of .otto.json
// ---------------------------------------------------------------------------

/// Parsed and validated .otto.json content. All fields optional.
/// Matches Otto's RepoConfig type in repo-config.ts.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepoConfig {
    /// Free-text project context injected into all AI prompts.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<String>,

    /// Review focus areas — categories the AI should prioritize.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub focus: Option<Vec<String>>,

    /// Categories the AI should deprioritize or skip.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ignore: Option<Vec<String>>,

    /// Free-text review template/checklist injected into code review prompts.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub review_template: Option<String>,

    /// Jira custom field ID for acceptance criteria.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub acceptance_criteria_field: Option<String>,

    /// Pinned Docker image for sandbox auto-fix.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sandbox_image: Option<String>,
}

impl RepoConfig {
    /// Check if this config has any meaningful content.
    pub fn is_empty(&self) -> bool {
        self.context.is_none()
            && self.focus.as_ref().map_or(true, |v| v.is_empty())
            && self.ignore.as_ref().map_or(true, |v| v.is_empty())
            && self.review_template.is_none()
            && self.acceptance_criteria_field.is_none()
            && self.sandbox_image.is_none()
    }

    /// Convert to a serde_json::Value matching the .otto.json shape.
    /// Used by the sandbox detector which expects Option<&serde_json::Value>.
    pub fn to_otto_json_value(&self) -> serde_json::Value {
        let mut obj = serde_json::Map::new();
        if let Some(ref ctx) = self.context {
            obj.insert("context".into(), serde_json::Value::String(ctx.clone()));
        }
        if let Some(ref focus) = self.focus {
            obj.insert("focus".into(), serde_json::to_value(focus).unwrap_or_default());
        }
        if let Some(ref ignore) = self.ignore {
            obj.insert("ignore".into(), serde_json::to_value(ignore).unwrap_or_default());
        }
        if let Some(ref tmpl) = self.review_template {
            obj.insert("reviewTemplate".into(), serde_json::Value::String(tmpl.clone()));
        }
        if let Some(ref field) = self.acceptance_criteria_field {
            obj.insert("acceptanceCriteriaField".into(), serde_json::Value::String(field.clone()));
        }
        if let Some(ref img) = self.sandbox_image {
            let mut sandbox = serde_json::Map::new();
            sandbox.insert("image".into(), serde_json::Value::String(img.clone()));
            obj.insert("sandbox".into(), serde_json::Value::Object(sandbox));
        }
        serde_json::Value::Object(obj)
    }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Get the repo config for a project, using the SQLite cache with fallback
/// to the GitLab API. Returns None if:
///   - No .otto.json exists in the repo (cached as null sentinel)
///   - The .otto.json is malformed or has no useful fields
///   - GitLab API is unreachable (NOT cached — next caller retries)
pub async fn get_or_fetch(
    pool: &SqlitePool,
    gl_cfg: &GitLabConfig,
    project_path: &str,
    project_id: i64,
    ref_name: &str,
) -> Option<RepoConfig> {
    // 1. Check cache
    match db::queries::get_repo_config(pool, project_path).await {
        Ok(Some((config_json, _formatted, _sandbox_image, _fetched_at))) => {
            if config_json == NULL_SENTINEL {
                // Null sentinel — we already checked, no .otto.json exists
                debug!("repo config cache hit (null sentinel): {}", project_path);
                return None;
            }
            debug!("repo config cache hit: {}", project_path);
            return serde_json::from_str::<RepoConfig>(&config_json).ok();
        }
        Ok(None) => {
            // Cache miss — fetch from GitLab
            debug!("repo config cache miss: {}", project_path);
        }
        Err(e) => {
            warn!("repo config cache read error: {}", e);
            // Fall through to fetch
        }
    }

    // 2. Fetch from GitLab
    let raw_content = match gitlab::fetch_file_content(gl_cfg, project_id, ".otto.json", ref_name).await {
        Ok(content) => content,
        Err(GitLabError::NotFound(_)) => {
            // Normal case — no .otto.json. Cache the null sentinel.
            debug!("no .otto.json in {}, caching null sentinel", project_path);
            let _ = db::queries::upsert_repo_config(
                pool, project_path, NULL_SENTINEL, "", None, DEFAULT_TTL_SECS,
            ).await;
            return None;
        }
        Err(e) => {
            // API error — do NOT cache, let next caller retry
            warn!("failed to fetch .otto.json for {}: {}", project_path, e);
            return None;
        }
    };

    // 3. Parse and validate
    let config = match parse_and_validate(&raw_content) {
        Some(c) if !c.is_empty() => c,
        _ => {
            // Malformed or empty — cache as null sentinel
            debug!(".otto.json in {} is empty or invalid, caching null sentinel", project_path);
            let _ = db::queries::upsert_repo_config(
                pool, project_path, NULL_SENTINEL, "", None, DEFAULT_TTL_SECS,
            ).await;
            return None;
        }
    };

    // 4. Cache the valid config
    let config_json = serde_json::to_string(&config).unwrap_or_else(|_| NULL_SENTINEL.into());
    let formatted = format_for_prompt(&config);
    let sandbox_image = config.sandbox_image.as_deref();

    if let Err(e) = db::queries::upsert_repo_config(
        pool, project_path, &config_json, &formatted, sandbox_image, DEFAULT_TTL_SECS,
    ).await {
        warn!("failed to cache repo config for {}: {}", project_path, e);
    }

    Some(config)
}

/// Get only the pre-formatted prompt text from cache, without fetching.
/// Used when the caller already knows the config was recently fetched
/// (e.g., within the same review pipeline after get_or_fetch was called).
pub async fn get_cached_formatted(
    pool: &SqlitePool,
    project_path: &str,
) -> Option<String> {
    match db::queries::get_repo_config(pool, project_path).await {
        Ok(Some((config_json, formatted, _, _))) => {
            if config_json == NULL_SENTINEL || formatted.is_empty() {
                None
            } else {
                Some(formatted)
            }
        }
        _ => None,
    }
}

/// Invalidate the cached config for a project. Called by webhook handler
/// when .otto.json is modified on the default branch.
pub async fn invalidate(pool: &SqlitePool, project_path: &str) {
    match db::queries::delete_repo_config(pool, project_path).await {
        Ok(()) => debug!("invalidated repo config cache for {}", project_path),
        Err(e) => warn!("failed to invalidate repo config for {}: {}", project_path, e),
    }
}

// ---------------------------------------------------------------------------
// Format for prompt — matches Otto's formatRepoConfigForPrompt() exactly
// ---------------------------------------------------------------------------

/// Format a RepoConfig as markdown text for injection into AI prompts.
/// Produces identical output to Otto's formatRepoConfigForPrompt() so the
/// AI gets the same context regardless of whether the review runs locally
/// (Otto) or through Botto.
pub fn format_for_prompt(config: &RepoConfig) -> String {
    let mut lines: Vec<String> = vec!["## Project Configuration (from .otto.json)".into()];

    if let Some(ref context) = config.context {
        lines.push(format!("\n**Project context:** {}", context));
    }

    if let Some(ref focus) = config.focus {
        if !focus.is_empty() {
            lines.push(format!(
                "\n**Review focus areas** (prioritize these): {}",
                focus.join(", ")
            ));
        }
    }

    if let Some(ref ignore) = config.ignore {
        if !ignore.is_empty() {
            lines.push(format!(
                "\n**Deprioritized categories** (skip unless critical): {}",
                ignore.join(", ")
            ));
        }
    }

    if let Some(ref template) = config.review_template {
        lines.push(format!("\n**Review checklist:**\n{}", template));
    }

    lines.join("\n")
}

// ---------------------------------------------------------------------------
// Parse + validate — ported from Otto's validateRepoConfig()
// ---------------------------------------------------------------------------

/// Parse raw JSON and validate/sanitize. Returns None for invalid input.
/// Matches Otto's validation: same field caps, same sanitization rules.
fn parse_and_validate(raw: &str) -> Option<RepoConfig> {
    let obj: serde_json::Value = serde_json::from_str(raw).ok()?;
    let map = obj.as_object()?;

    let context = map
        .get("context")
        .and_then(|v| v.as_str())
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.chars().take(2000).collect::<String>());

    let focus = map
        .get("focus")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str())
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .take(20)
                .collect::<Vec<_>>()
        })
        .filter(|v| !v.is_empty());

    let ignore = map
        .get("ignore")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str())
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .take(20)
                .collect::<Vec<_>>()
        })
        .filter(|v| !v.is_empty());

    let review_template = map
        .get("reviewTemplate")
        .and_then(|v| v.as_str())
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.chars().take(2000).collect::<String>());

    let acceptance_criteria_field = map
        .get("acceptanceCriteriaField")
        .and_then(|v| v.as_str())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());

    let sandbox_image = map
        .get("sandbox")
        .and_then(|v| v.as_object())
        .and_then(|s| s.get("image"))
        .and_then(|v| v.as_str())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());

    Some(RepoConfig {
        context,
        focus,
        ignore,
        review_template,
        acceptance_criteria_field,
        sandbox_image,
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_full_config() {
        let raw = r#"{
            "context": "E-commerce platform built with Vue 3 + Node.js.",
            "focus": ["security", "error-handling", "performance"],
            "ignore": ["style", "naming"],
            "reviewTemplate": "Check for SQL injection, validate all user inputs.",
            "acceptanceCriteriaField": "customfield_10042",
            "sandbox": { "image": "node:22-slim" }
        }"#;
        let config = parse_and_validate(raw).unwrap();
        assert_eq!(config.context.as_deref(), Some("E-commerce platform built with Vue 3 + Node.js."));
        assert_eq!(config.focus.as_ref().unwrap().len(), 3);
        assert_eq!(config.ignore.as_ref().unwrap(), &["style", "naming"]);
        assert_eq!(config.review_template.as_deref(), Some("Check for SQL injection, validate all user inputs."));
        assert_eq!(config.acceptance_criteria_field.as_deref(), Some("customfield_10042"));
        assert_eq!(config.sandbox_image.as_deref(), Some("node:22-slim"));
        assert!(!config.is_empty());
    }

    #[test]
    fn parse_empty_config() {
        let config = parse_and_validate("{}").unwrap();
        assert!(config.is_empty());
    }

    #[test]
    fn parse_invalid_json() {
        assert!(parse_and_validate("not json").is_none());
        assert!(parse_and_validate("").is_none());
        assert!(parse_and_validate("null").is_none());
        assert!(parse_and_validate("[]").is_none());
    }

    #[test]
    fn parse_caps_context_at_2000() {
        let long_ctx = "x".repeat(3000);
        let raw = format!(r#"{{ "context": "{}" }}"#, long_ctx);
        let config = parse_and_validate(&raw).unwrap();
        assert_eq!(config.context.as_ref().unwrap().len(), 2000);
    }

    #[test]
    fn parse_caps_focus_at_20() {
        let items: Vec<String> = (0..30).map(|i| format!("\"item-{}\"", i)).collect();
        let raw = format!(r#"{{ "focus": [{}] }}"#, items.join(","));
        let config = parse_and_validate(&raw).unwrap();
        assert_eq!(config.focus.as_ref().unwrap().len(), 20);
    }

    #[test]
    fn parse_trims_whitespace() {
        let raw = r#"{ "context": "  hello world  ", "focus": ["  security  ", "  perf  "] }"#;
        let config = parse_and_validate(raw).unwrap();
        assert_eq!(config.context.as_deref(), Some("hello world"));
        assert_eq!(config.focus.as_ref().unwrap(), &["security", "perf"]);
    }

    #[test]
    fn parse_skips_empty_strings() {
        let raw = r#"{ "context": "  ", "focus": ["", "  ", "valid"], "reviewTemplate": "" }"#;
        let config = parse_and_validate(raw).unwrap();
        assert!(config.context.is_none());
        assert_eq!(config.focus.as_ref().unwrap(), &["valid"]);
        assert!(config.review_template.is_none());
    }

    #[test]
    fn format_matches_otto() {
        let config = RepoConfig {
            context: Some("Django 4.2 REST API".into()),
            focus: Some(vec!["security".into(), "performance".into()]),
            ignore: Some(vec!["style".into()]),
            review_template: Some("Check auth on new endpoints".into()),
            acceptance_criteria_field: None,
            sandbox_image: None,
        };
        let formatted = format_for_prompt(&config);
        assert!(formatted.starts_with("## Project Configuration (from .otto.json)"));
        assert!(formatted.contains("**Project context:** Django 4.2 REST API"));
        assert!(formatted.contains("**Review focus areas** (prioritize these): security, performance"));
        assert!(formatted.contains("**Deprioritized categories** (skip unless critical): style"));
        assert!(formatted.contains("**Review checklist:**\nCheck auth on new endpoints"));
    }

    #[test]
    fn to_otto_json_value_roundtrip() {
        let config = RepoConfig {
            context: Some("test".into()),
            focus: Some(vec!["security".into()]),
            ignore: None,
            review_template: None,
            acceptance_criteria_field: None,
            sandbox_image: Some("node:22-slim".into()),
        };
        let val = config.to_otto_json_value();
        assert_eq!(val["context"], "test");
        assert_eq!(val["focus"][0], "security");
        assert_eq!(val["sandbox"]["image"], "node:22-slim");
    }
}
