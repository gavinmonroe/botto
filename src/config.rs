// ---------------------------------------------------------------------------
// Config — auto-detection + file-based configuration.
//
// Priority: CLI flags > botto.toml > auto-detected defaults.
// On first run with no config file, Botto auto-detects everything it can
// and prints a summary so the user knows what's active.
// ---------------------------------------------------------------------------

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use tracing::{info, warn};

// ---------------------------------------------------------------------------
// Config schema (matches botto.toml structure)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
pub struct BottoConfig {
    pub server: ServerConfig,
    pub auth: AuthConfig,
    pub gitlab: GitLabConfig,
    pub ai: AiConfig,
    pub sandbox: SandboxConfig,
    pub cache: CacheConfig,
    pub harness: HarnessConfig,
    pub data_dir: PathBuf,
}

#[derive(Debug, Clone, Serialize)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    /// Max concurrent MR reviews running simultaneously.
    pub max_concurrent_reviews: usize,
    /// Max concurrent AI API calls across all reviews.
    pub max_concurrent_ai_calls: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct AuthConfig {
    /// Shared API key that Otto extensions use to authenticate with Botto.
    pub api_key: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct GitLabConfig {
    pub url: String,
    pub bot_token: String,
    /// Webhook secret for validating incoming GitLab webhooks.
    pub webhook_secret: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AiConfig {
    pub base_url: String,
    pub api_key: String,
    pub models: AiModelConfig,
}

#[derive(Debug, Clone, Serialize)]
pub struct AiModelConfig {
    pub summary: String,
    pub code_review: String,
    pub edge_cases: String,
    pub related_files: String,
    pub follow_up: String,
    pub chat: String,
    pub ac_validation: String,
    pub adversarial_tests: String,
    pub contracts: String,
    pub behavioral_delta: String,
    /// Model for sandbox fix iterations — needs strong reasoning for autonomous fixing.
    pub fix: String,
}

impl Default for AiModelConfig {
    fn default() -> Self {
        Self {
            summary: "claude-sonnet-4-5".into(),
            code_review: "claude-sonnet-4-5".into(),
            edge_cases: "claude-sonnet-4-5".into(),
            related_files: "claude-haiku-4-5".into(),
            follow_up: "claude-sonnet-4-5".into(),
            chat: "claude-sonnet-4-5".into(),
            ac_validation: "claude-sonnet-4-5".into(),
            adversarial_tests: "claude-sonnet-4-5".into(),
            contracts: "claude-sonnet-4-5".into(),
            behavioral_delta: "claude-sonnet-4-5".into(),
            fix: "claude-opus-4-6".into(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct SandboxConfig {
    pub enabled: bool,
    pub docker_available: bool,
    pub max_concurrent: u32,
    pub timeout_seconds: u64,
    pub max_memory_mb: u64,
    pub max_disk_mb: u64,
    /// How to push fix commits: directly to the source branch, or to a new branch with an MR.
    pub fix_branch_mode: FixBranchMode,
}

/// Controls where sandbox fix commits are pushed.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub enum FixBranchMode {
    /// Push directly to the MR's source branch (default, current behavior).
    SameBranch,
    /// Create a new branch (e.g., `botto/fix/mr-42-add-auth`) and open an MR
    /// targeting the original source branch.
    NewBranch,
}

#[derive(Debug, Clone, Serialize)]
pub struct CacheConfig {
    pub review_ttl_days: u32,
    pub max_cached_reviews: u32,
}

#[derive(Debug, Clone, Serialize)]
pub struct HarnessConfig {
    pub enabled: bool,
    /// Max evolution rounds per run.
    pub max_rounds: u32,
    /// Number of prompt variants to test per round.
    pub variants_per_round: u32,
    /// Max concurrent sandbox instances for harness runs.
    pub concurrency: u32,
    /// Number of test cases to run each variant against.
    pub test_cases: u32,
    /// Seed GitLab orgs/groups to discover MRs from.
    pub gitlab_seed_orgs: Vec<String>,
    /// Directory for harness memory (prompts, learnings, test cases).
    pub memory_dir: PathBuf,
    /// Model to use for the judge AI.
    pub judge_model: String,
}

// ---------------------------------------------------------------------------
// TOML file schema (optional fields — everything has defaults)
// ---------------------------------------------------------------------------

#[derive(Deserialize, Default)]
struct TomlConfig {
    server: Option<TomlServer>,
    auth: Option<TomlAuth>,
    gitlab: Option<TomlGitLab>,
    ai: Option<TomlAi>,
    sandbox: Option<TomlSandbox>,
    cache: Option<TomlCache>,
    harness: Option<TomlHarness>,
}

#[derive(Deserialize, Default)]
struct TomlServer {
    host: Option<String>,
    port: Option<u16>,
    max_concurrent_reviews: Option<usize>,
    max_concurrent_ai_calls: Option<usize>,
}

#[derive(Deserialize, Default)]
struct TomlAuth {
    api_key: Option<String>,
}

#[derive(Deserialize, Default)]
struct TomlGitLab {
    url: Option<String>,
    bot_token: Option<String>,
    webhook_secret: Option<String>,
}

#[derive(Deserialize, Default)]
struct TomlAi {
    base_url: Option<String>,
    api_key: Option<String>,
    models: Option<TomlAiModels>,
}

#[derive(Deserialize, Default)]
struct TomlAiModels {
    summary: Option<String>,
    code_review: Option<String>,
    edge_cases: Option<String>,
    related_files: Option<String>,
    follow_up: Option<String>,
    chat: Option<String>,
    ac_validation: Option<String>,
    adversarial_tests: Option<String>,
    contracts: Option<String>,
    behavioral_delta: Option<String>,
    fix: Option<String>,
}

#[derive(Deserialize, Default)]
struct TomlSandbox {
    enabled: Option<bool>,
    max_concurrent: Option<u32>,
    timeout_seconds: Option<u64>,
    max_memory_mb: Option<u64>,
    max_disk_mb: Option<u64>,
    /// "same_branch" (default) or "new_branch"
    fix_branch_mode: Option<String>,
}

#[derive(Deserialize, Default)]
struct TomlCache {
    review_ttl_days: Option<u32>,
    max_cached_reviews: Option<u32>,
}

#[derive(Deserialize, Default)]
struct TomlHarness {
    enabled: Option<bool>,
    max_rounds: Option<u32>,
    variants_per_round: Option<u32>,
    concurrency: Option<u32>,
    test_cases: Option<u32>,
    gitlab_seed_orgs: Option<Vec<String>>,
    memory_dir: Option<String>,
    judge_model: Option<String>,
}

// ---------------------------------------------------------------------------
// Auto-detection
// ---------------------------------------------------------------------------

/// Check if Docker is available by probing the socket.
async fn detect_docker() -> bool {
    match bollard::Docker::connect_with_local_defaults() {
        Ok(docker) => match docker.ping().await {
            Ok(_) => true,
            Err(_) => false,
        },
        Err(_) => false,
    }
}

/// Detect available system resources for sandbox limits.
fn detect_resources() -> (u32, u64) {
    let sys = sysinfo::System::new_all();
    let cpus = sys.cpus().len() as u32;
    let memory_mb = sys.total_memory() / (1024 * 1024);

    // Reserve half the cores and memory for the server itself.
    // At least 1 concurrent sandbox, cap at 4.
    let max_concurrent = ((cpus / 2).max(1)).min(4);
    // Each sandbox gets up to 2GB, but cap at 25% of total memory.
    let max_memory_mb = (memory_mb / 4).min(2048).max(512);

    (max_concurrent, max_memory_mb)
}

// ---------------------------------------------------------------------------
// Load
// ---------------------------------------------------------------------------

pub async fn load(config_path: &Option<PathBuf>, data_dir: &Path) -> Result<BottoConfig> {
    // Try to read config file
    let toml_cfg = if let Some(path) = config_path {
        let content = tokio::fs::read_to_string(path)
            .await
            .with_context(|| format!("failed to read config file: {}", path.display()))?;
        toml::from_str::<TomlConfig>(&content)
            .with_context(|| format!("failed to parse config file: {}", path.display()))?
    } else {
        // Try default location
        let default_path = data_dir.join("botto.toml");
        if default_path.exists() {
            let content = tokio::fs::read_to_string(&default_path).await?;
            toml::from_str::<TomlConfig>(&content).unwrap_or_default()
        } else {
            TomlConfig::default()
        }
    };

    // Also check environment variables for secrets
    let env_api_key = std::env::var("BOTTO_API_KEY").ok();
    let env_gitlab_token = std::env::var("BOTTO_GITLAB_TOKEN").ok();
    let env_gitlab_url = std::env::var("BOTTO_GITLAB_URL").ok();
    let env_ai_key = std::env::var("BOTTO_AI_KEY").ok();
    let env_ai_url = std::env::var("BOTTO_AI_URL").ok();
    let env_webhook_secret = std::env::var("BOTTO_WEBHOOK_SECRET").ok();

    // Auto-detect capabilities
    let docker_available = detect_docker().await;
    let (auto_concurrent, auto_memory) = detect_resources();

    let toml_server = toml_cfg.server.unwrap_or_default();
    let toml_auth = toml_cfg.auth.unwrap_or_default();
    let toml_gitlab = toml_cfg.gitlab.unwrap_or_default();
    let toml_ai = toml_cfg.ai.unwrap_or_default();
    let toml_sandbox = toml_cfg.sandbox.unwrap_or_default();
    let toml_cache = toml_cfg.cache.unwrap_or_default();
    let toml_harness = toml_cfg.harness.unwrap_or_default();
    let toml_models = toml_ai.models.unwrap_or_default();
    let default_models = AiModelConfig::default();

    let api_key = env_api_key
        .or(toml_auth.api_key)
        .unwrap_or_default();

    if api_key.is_empty() {
        warn!("no API key configured — set BOTTO_API_KEY or auth.api_key in botto.toml");
    }

    let gitlab_token = env_gitlab_token
        .or(toml_gitlab.bot_token)
        .unwrap_or_default();

    if gitlab_token.is_empty() {
        warn!("no GitLab bot token — set BOTTO_GITLAB_TOKEN or gitlab.bot_token in botto.toml");
    }

    Ok(BottoConfig {
        server: ServerConfig {
            host: toml_server.host.unwrap_or_else(|| "0.0.0.0".into()),
            port: toml_server.port.unwrap_or(7700),
            max_concurrent_reviews: toml_server.max_concurrent_reviews.unwrap_or(3),
            max_concurrent_ai_calls: toml_server.max_concurrent_ai_calls.unwrap_or(6),
        },
        auth: AuthConfig { api_key },
        gitlab: GitLabConfig {
            url: env_gitlab_url
                .or(toml_gitlab.url)
                .unwrap_or_else(|| "https://gitlab.com".into()),
            bot_token: gitlab_token,
            webhook_secret: env_webhook_secret.or(toml_gitlab.webhook_secret),
        },
        ai: AiConfig {
            base_url: env_ai_url
                .or(toml_ai.base_url)
                .unwrap_or_default(),
            api_key: env_ai_key
                .or(toml_ai.api_key)
                .unwrap_or_default(),
            models: AiModelConfig {
                summary: toml_models.summary.unwrap_or(default_models.summary),
                code_review: toml_models.code_review.unwrap_or(default_models.code_review),
                edge_cases: toml_models.edge_cases.unwrap_or(default_models.edge_cases),
                related_files: toml_models.related_files.unwrap_or(default_models.related_files),
                follow_up: toml_models.follow_up.unwrap_or(default_models.follow_up),
                chat: toml_models.chat.unwrap_or(default_models.chat),
                ac_validation: toml_models.ac_validation.unwrap_or(default_models.ac_validation),
                adversarial_tests: toml_models.adversarial_tests.unwrap_or(default_models.adversarial_tests),
                contracts: toml_models.contracts.unwrap_or(default_models.contracts),
                behavioral_delta: toml_models.behavioral_delta.unwrap_or(default_models.behavioral_delta),
                fix: toml_models.fix.unwrap_or(default_models.fix),
            },
        },
        sandbox: SandboxConfig {
            enabled: toml_sandbox.enabled.unwrap_or(docker_available),
            docker_available,
            max_concurrent: toml_sandbox.max_concurrent.unwrap_or(auto_concurrent),
            timeout_seconds: toml_sandbox.timeout_seconds.unwrap_or(300),
            max_memory_mb: toml_sandbox.max_memory_mb.unwrap_or(auto_memory),
            max_disk_mb: toml_sandbox.max_disk_mb.unwrap_or(4096),
            fix_branch_mode: match toml_sandbox.fix_branch_mode.as_deref() {
                Some("new_branch") => FixBranchMode::NewBranch,
                _ => FixBranchMode::SameBranch,
            },
        },
        cache: CacheConfig {
            review_ttl_days: toml_cache.review_ttl_days.unwrap_or(7),
            max_cached_reviews: toml_cache.max_cached_reviews.unwrap_or(500),
        },
        harness: HarnessConfig {
            enabled: toml_harness.enabled.unwrap_or(false),
            max_rounds: toml_harness.max_rounds.unwrap_or(10),
            variants_per_round: toml_harness.variants_per_round.unwrap_or(4),
            concurrency: toml_harness.concurrency.unwrap_or(3),
            test_cases: toml_harness.test_cases.unwrap_or(5),
            gitlab_seed_orgs: toml_harness.gitlab_seed_orgs.unwrap_or_else(|| {
                vec!["gitlab-org".into()]
            }),
            memory_dir: PathBuf::from(
                toml_harness.memory_dir.unwrap_or_else(|| "harness".into()),
            ),
            judge_model: toml_harness.judge_model.unwrap_or_else(|| "claude-opus-4-6".into()),
        },
        data_dir: data_dir.to_path_buf(),
    })
}

// ---------------------------------------------------------------------------
// Startup summary
// ---------------------------------------------------------------------------

pub fn print_summary(cfg: &BottoConfig) {
    info!("=== botto v{} ===", env!("CARGO_PKG_VERSION"));
    info!("listen: {}:{}", cfg.server.host, cfg.server.port);
    info!("gitlab: {}", if cfg.gitlab.bot_token.is_empty() { "(not configured)" } else { &cfg.gitlab.url });
    info!("ai:     {}", if cfg.ai.base_url.is_empty() { "(not configured)" } else { &cfg.ai.base_url });
    info!("auth:   {}", if cfg.auth.api_key.is_empty() { "OPEN (no API key)" } else { "API key required" });
    info!(
        "sandbox: {} (docker={}, max_concurrent={}, memory={}MB)",
        if cfg.sandbox.enabled { "enabled" } else { "disabled" },
        cfg.sandbox.docker_available,
        cfg.sandbox.max_concurrent,
        cfg.sandbox.max_memory_mb,
    );
    info!(
        "limits: max_reviews={}, max_ai_calls={}",
        cfg.server.max_concurrent_reviews,
        cfg.server.max_concurrent_ai_calls,
    );
    info!("cache:  ttl={}d, max={}/project", cfg.cache.review_ttl_days, cfg.cache.max_cached_reviews);
    info!("data:   {}", cfg.data_dir.display());
}

// ---------------------------------------------------------------------------
// Admin API types — redacted config for GET, partial update for PUT
// ---------------------------------------------------------------------------

/// Redact a secret string: show last 4 chars with a mask prefix.
/// Returns empty string for empty secrets.
fn redact_secret(s: &str) -> String {
    if s.is_empty() {
        String::new()
    } else if s.len() <= 4 {
        "••••".to_string()
    } else {
        format!("••••{}", &s[s.len() - 4..])
    }
}

/// Check if a value is a redacted placeholder (starts with "••").
fn is_redacted(s: &str) -> bool {
    s.starts_with("••")
}

/// Config response with secrets redacted. Safe to send over the wire.
#[derive(Debug, Serialize)]
pub struct ConfigResponse {
    pub server: ServerConfig,
    pub auth: AuthConfigRedacted,
    pub gitlab: GitLabConfigRedacted,
    pub ai: AiConfigRedacted,
    pub sandbox: SandboxConfig,
    pub cache: CacheConfig,
    pub harness: HarnessConfigRedacted,
    pub data_dir: String,
}

#[derive(Debug, Serialize)]
pub struct AuthConfigRedacted {
    pub api_key: String,
}

#[derive(Debug, Serialize)]
pub struct GitLabConfigRedacted {
    pub url: String,
    pub bot_token: String,
    pub webhook_secret: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct AiConfigRedacted {
    pub base_url: String,
    pub api_key: String,
    pub models: AiModelConfig,
}

#[derive(Debug, Serialize)]
pub struct HarnessConfigRedacted {
    pub enabled: bool,
    pub max_rounds: u32,
    pub variants_per_round: u32,
    pub concurrency: u32,
    pub test_cases: u32,
    pub gitlab_seed_orgs: Vec<String>,
    pub memory_dir: String,
    pub judge_model: String,
}

impl ConfigResponse {
    pub fn from_config(cfg: &BottoConfig) -> Self {
        Self {
            server: cfg.server.clone(),
            auth: AuthConfigRedacted {
                api_key: redact_secret(&cfg.auth.api_key),
            },
            gitlab: GitLabConfigRedacted {
                url: cfg.gitlab.url.clone(),
                bot_token: redact_secret(&cfg.gitlab.bot_token),
                webhook_secret: cfg.gitlab.webhook_secret.as_ref().map(|s| redact_secret(s)),
            },
            ai: AiConfigRedacted {
                base_url: cfg.ai.base_url.clone(),
                api_key: redact_secret(&cfg.ai.api_key),
                models: cfg.ai.models.clone(),
            },
            sandbox: cfg.sandbox.clone(),
            cache: cfg.cache.clone(),
            harness: HarnessConfigRedacted {
                enabled: cfg.harness.enabled,
                max_rounds: cfg.harness.max_rounds,
                variants_per_round: cfg.harness.variants_per_round,
                concurrency: cfg.harness.concurrency,
                test_cases: cfg.harness.test_cases,
                gitlab_seed_orgs: cfg.harness.gitlab_seed_orgs.clone(),
                memory_dir: cfg.harness.memory_dir.display().to_string(),
                judge_model: cfg.harness.judge_model.clone(),
            },
            data_dir: cfg.data_dir.display().to_string(),
        }
    }
}

/// Partial config update from the admin UI. All fields optional —
/// only provided fields are applied. Redacted secret values (starting
/// with "••") are ignored, preserving the existing secret.
#[derive(Debug, Deserialize)]
pub struct ConfigUpdate {
    pub server: Option<ServerConfigUpdate>,
    pub auth: Option<AuthConfigUpdate>,
    pub gitlab: Option<GitLabConfigUpdate>,
    pub ai: Option<AiConfigUpdate>,
    pub sandbox: Option<SandboxConfigUpdate>,
    pub cache: Option<CacheConfigUpdate>,
    pub harness: Option<HarnessConfigUpdate>,
}

#[derive(Debug, Deserialize)]
pub struct ServerConfigUpdate {
    pub host: Option<String>,
    pub port: Option<u16>,
    pub max_concurrent_reviews: Option<usize>,
    pub max_concurrent_ai_calls: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub struct AuthConfigUpdate {
    pub api_key: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct GitLabConfigUpdate {
    pub url: Option<String>,
    pub bot_token: Option<String>,
    pub webhook_secret: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct AiConfigUpdate {
    pub base_url: Option<String>,
    pub api_key: Option<String>,
    pub models: Option<AiModelConfigUpdate>,
}

#[derive(Debug, Deserialize)]
pub struct AiModelConfigUpdate {
    pub summary: Option<String>,
    pub code_review: Option<String>,
    pub edge_cases: Option<String>,
    pub related_files: Option<String>,
    pub follow_up: Option<String>,
    pub chat: Option<String>,
    pub ac_validation: Option<String>,
    pub adversarial_tests: Option<String>,
    pub contracts: Option<String>,
    pub behavioral_delta: Option<String>,
    pub fix: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct SandboxConfigUpdate {
    pub enabled: Option<bool>,
    pub max_concurrent: Option<u32>,
    pub timeout_seconds: Option<u64>,
    pub max_memory_mb: Option<u64>,
    pub max_disk_mb: Option<u64>,
    pub fix_branch_mode: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct CacheConfigUpdate {
    pub review_ttl_days: Option<u32>,
    pub max_cached_reviews: Option<u32>,
}

#[derive(Debug, Deserialize)]
pub struct HarnessConfigUpdate {
    pub enabled: Option<bool>,
    pub max_rounds: Option<u32>,
    pub variants_per_round: Option<u32>,
    pub concurrency: Option<u32>,
    pub test_cases: Option<u32>,
    pub gitlab_seed_orgs: Option<Vec<String>>,
    pub memory_dir: Option<String>,
    pub judge_model: Option<String>,
}

/// Fields that require a server restart to take effect.
const RESTART_FIELDS: &[&str] = &[
    "server.host",
    "server.port",
    "server.max_concurrent_reviews",
    "server.max_concurrent_ai_calls",
];

/// Apply a ConfigUpdate to an existing BottoConfig, returning the new config
/// and a list of changed fields that require restart.
pub fn apply_update(current: &BottoConfig, update: ConfigUpdate) -> (BottoConfig, Vec<String>) {
    let mut cfg = current.clone();
    let mut restart_needed = Vec::new();

    if let Some(s) = update.server {
        if let Some(v) = s.host {
            if v != cfg.server.host {
                restart_needed.push("server.host".into());
                cfg.server.host = v;
            }
        }
        if let Some(v) = s.port {
            if v != cfg.server.port {
                restart_needed.push("server.port".into());
                cfg.server.port = v;
            }
        }
        if let Some(v) = s.max_concurrent_reviews {
            if v != cfg.server.max_concurrent_reviews {
                restart_needed.push("server.max_concurrent_reviews".into());
                cfg.server.max_concurrent_reviews = v;
            }
        }
        if let Some(v) = s.max_concurrent_ai_calls {
            if v != cfg.server.max_concurrent_ai_calls {
                restart_needed.push("server.max_concurrent_ai_calls".into());
                cfg.server.max_concurrent_ai_calls = v;
            }
        }
    }

    if let Some(a) = update.auth {
        if let Some(v) = a.api_key {
            if !is_redacted(&v) { cfg.auth.api_key = v; }
        }
    }

    if let Some(g) = update.gitlab {
        if let Some(v) = g.url { cfg.gitlab.url = v; }
        if let Some(v) = g.bot_token {
            if !is_redacted(&v) { cfg.gitlab.bot_token = v; }
        }
        if let Some(v) = g.webhook_secret {
            if is_redacted(&v) {
                // keep existing
            } else if v.is_empty() {
                cfg.gitlab.webhook_secret = None;
            } else {
                cfg.gitlab.webhook_secret = Some(v);
            }
        }
    }

    if let Some(a) = update.ai {
        if let Some(v) = a.base_url { cfg.ai.base_url = v; }
        if let Some(v) = a.api_key {
            if !is_redacted(&v) { cfg.ai.api_key = v; }
        }
        if let Some(m) = a.models {
            if let Some(v) = m.summary { cfg.ai.models.summary = v; }
            if let Some(v) = m.code_review { cfg.ai.models.code_review = v; }
            if let Some(v) = m.edge_cases { cfg.ai.models.edge_cases = v; }
            if let Some(v) = m.related_files { cfg.ai.models.related_files = v; }
            if let Some(v) = m.follow_up { cfg.ai.models.follow_up = v; }
            if let Some(v) = m.chat { cfg.ai.models.chat = v; }
            if let Some(v) = m.ac_validation { cfg.ai.models.ac_validation = v; }
            if let Some(v) = m.adversarial_tests { cfg.ai.models.adversarial_tests = v; }
            if let Some(v) = m.contracts { cfg.ai.models.contracts = v; }
            if let Some(v) = m.behavioral_delta { cfg.ai.models.behavioral_delta = v; }
            if let Some(v) = m.fix { cfg.ai.models.fix = v; }
        }
    }

    if let Some(s) = update.sandbox {
        if let Some(v) = s.enabled { cfg.sandbox.enabled = v; }
        if let Some(v) = s.max_concurrent { cfg.sandbox.max_concurrent = v; }
        if let Some(v) = s.timeout_seconds { cfg.sandbox.timeout_seconds = v; }
        if let Some(v) = s.max_memory_mb { cfg.sandbox.max_memory_mb = v; }
        if let Some(v) = s.max_disk_mb { cfg.sandbox.max_disk_mb = v; }
        if let Some(v) = s.fix_branch_mode {
            cfg.sandbox.fix_branch_mode = match v.as_str() {
                "new_branch" => FixBranchMode::NewBranch,
                _ => FixBranchMode::SameBranch,
            };
        }
    }

    if let Some(c) = update.cache {
        if let Some(v) = c.review_ttl_days { cfg.cache.review_ttl_days = v; }
        if let Some(v) = c.max_cached_reviews { cfg.cache.max_cached_reviews = v; }
    }

    if let Some(h) = update.harness {
        if let Some(v) = h.enabled { cfg.harness.enabled = v; }
        if let Some(v) = h.max_rounds { cfg.harness.max_rounds = v; }
        if let Some(v) = h.variants_per_round { cfg.harness.variants_per_round = v; }
        if let Some(v) = h.concurrency { cfg.harness.concurrency = v; }
        if let Some(v) = h.test_cases { cfg.harness.test_cases = v; }
        if let Some(v) = h.gitlab_seed_orgs { cfg.harness.gitlab_seed_orgs = v; }
        if let Some(v) = h.memory_dir { cfg.harness.memory_dir = PathBuf::from(v); }
        if let Some(v) = h.judge_model { cfg.harness.judge_model = v; }
    }

    (cfg, restart_needed)
}

/// Serialize a BottoConfig to TOML for writing back to botto.toml.
/// Secrets are written in full (this is the server-side config file).
pub fn to_toml_string(cfg: &BottoConfig) -> Result<String> {
    // Build a TOML-friendly intermediate that maps cleanly to the file format.
    #[derive(Serialize)]
    struct TomlOut<'a> {
        server: &'a ServerConfig,
        auth: &'a AuthConfig,
        gitlab: GitLabOut<'a>,
        ai: AiOut<'a>,
        sandbox: SandboxOut<'a>,
        cache: &'a CacheConfig,
        harness: HarnessOut<'a>,
    }

    #[derive(Serialize)]
    struct GitLabOut<'a> {
        url: &'a str,
        bot_token: &'a str,
        #[serde(skip_serializing_if = "Option::is_none")]
        webhook_secret: Option<&'a str>,
    }

    #[derive(Serialize)]
    struct AiOut<'a> {
        base_url: &'a str,
        api_key: &'a str,
        models: &'a AiModelConfig,
    }

    #[derive(Serialize)]
    struct SandboxOut<'a> {
        enabled: bool,
        max_concurrent: u32,
        timeout_seconds: u64,
        max_memory_mb: u64,
        max_disk_mb: u64,
        fix_branch_mode: &'a str,
    }

    #[derive(Serialize)]
    struct HarnessOut<'a> {
        enabled: bool,
        max_rounds: u32,
        variants_per_round: u32,
        concurrency: u32,
        test_cases: u32,
        gitlab_seed_orgs: &'a [String],
        memory_dir: String,
        judge_model: &'a str,
    }

    let out = TomlOut {
        server: &cfg.server,
        auth: &cfg.auth,
        gitlab: GitLabOut {
            url: &cfg.gitlab.url,
            bot_token: &cfg.gitlab.bot_token,
            webhook_secret: cfg.gitlab.webhook_secret.as_deref(),
        },
        ai: AiOut {
            base_url: &cfg.ai.base_url,
            api_key: &cfg.ai.api_key,
            models: &cfg.ai.models,
        },
        sandbox: SandboxOut {
            enabled: cfg.sandbox.enabled,
            max_concurrent: cfg.sandbox.max_concurrent,
            timeout_seconds: cfg.sandbox.timeout_seconds,
            max_memory_mb: cfg.sandbox.max_memory_mb,
            max_disk_mb: cfg.sandbox.max_disk_mb,
            fix_branch_mode: match cfg.sandbox.fix_branch_mode {
                FixBranchMode::SameBranch => "same_branch",
                FixBranchMode::NewBranch => "new_branch",
            },
        },
        cache: &cfg.cache,
        harness: HarnessOut {
            enabled: cfg.harness.enabled,
            max_rounds: cfg.harness.max_rounds,
            variants_per_round: cfg.harness.variants_per_round,
            concurrency: cfg.harness.concurrency,
            test_cases: cfg.harness.test_cases,
            gitlab_seed_orgs: &cfg.harness.gitlab_seed_orgs,
            memory_dir: cfg.harness.memory_dir.display().to_string(),
            judge_model: &cfg.harness.judge_model,
        },
    };

    toml::to_string_pretty(&out).map_err(|e| anyhow::anyhow!("TOML serialize error: {}", e))
}

/// Write config to the botto.toml file in the data directory.
pub async fn save_to_file(cfg: &BottoConfig) -> Result<()> {
    let toml_str = to_toml_string(cfg)?;
    let path = cfg.data_dir.join("botto.toml");
    tokio::fs::write(&path, toml_str)
        .await
        .with_context(|| format!("failed to write config to {}", path.display()))?;
    info!("config saved to {}", path.display());
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a minimal test config for use in tests.
    fn test_config() -> BottoConfig {
        BottoConfig {
            server: ServerConfig {
                host: "0.0.0.0".into(),
                port: 7700,
                max_concurrent_reviews: 3,
                max_concurrent_ai_calls: 6,
            },
            auth: AuthConfig {
                api_key: "test-secret-key-1234".into(),
            },
            gitlab: GitLabConfig {
                url: "https://gitlab.com".into(),
                bot_token: "glpat-xxxxxxxxxxxxxxxxxxxx".into(),
                webhook_secret: Some("webhook-secret-abcd".into()),
            },
            ai: AiConfig {
                base_url: "https://api.example.com".into(),
                api_key: "sk-ai-key-5678".into(),
                models: AiModelConfig::default(),
            },
            sandbox: SandboxConfig {
                enabled: true,
                docker_available: true,
                max_concurrent: 2,
                timeout_seconds: 300,
                max_memory_mb: 2048,
                max_disk_mb: 4096,
                fix_branch_mode: FixBranchMode::SameBranch,
            },
            cache: CacheConfig {
                review_ttl_days: 7,
                max_cached_reviews: 500,
            },
            harness: HarnessConfig {
                enabled: false,
                max_rounds: 10,
                variants_per_round: 4,
                concurrency: 3,
                test_cases: 5,
                gitlab_seed_orgs: vec!["gitlab-org".into()],
                memory_dir: PathBuf::from("harness"),
                judge_model: "claude-opus-4-6".into(),
            },
            data_dir: PathBuf::from("/tmp/botto-test"),
        }
    }

    // -- redact_secret --

    #[test]
    fn redact_empty_secret() {
        assert_eq!(redact_secret(""), "");
    }

    #[test]
    fn redact_short_secret() {
        assert_eq!(redact_secret("abc"), "••••");
        assert_eq!(redact_secret("abcd"), "••••");
    }

    #[test]
    fn redact_normal_secret() {
        assert_eq!(redact_secret("glpat-xxxxxxxxxxxxxxxxxxxx"), "••••xxxx");
        assert_eq!(redact_secret("sk-ai-key-5678"), "••••5678");
    }

    // -- is_redacted --

    #[test]
    fn is_redacted_detects_mask() {
        assert!(is_redacted("••••xxxx"));
        assert!(is_redacted("••••"));
        assert!(!is_redacted("real-secret"));
        assert!(!is_redacted(""));
    }

    // -- ConfigResponse --

    #[test]
    fn config_response_redacts_secrets() {
        let cfg = test_config();
        let resp = ConfigResponse::from_config(&cfg);

        // Secrets are redacted
        assert_eq!(resp.auth.api_key, "••••1234");
        assert_eq!(resp.gitlab.bot_token, "••••xxxx");
        assert_eq!(resp.gitlab.webhook_secret, Some("••••abcd".into()));
        assert_eq!(resp.ai.api_key, "••••5678");

        // Non-secrets are preserved
        assert_eq!(resp.server.host, "0.0.0.0");
        assert_eq!(resp.server.port, 7700);
        assert_eq!(resp.gitlab.url, "https://gitlab.com");
        assert_eq!(resp.ai.base_url, "https://api.example.com");
    }

    // -- apply_update --

    #[test]
    fn apply_update_changes_non_secret_fields() {
        let cfg = test_config();
        let update = ConfigUpdate {
            server: None,
            auth: None,
            gitlab: Some(GitLabConfigUpdate {
                url: Some("https://gitlab.example.com".into()),
                bot_token: None,
                webhook_secret: None,
            }),
            ai: Some(AiConfigUpdate {
                base_url: Some("https://new-api.example.com".into()),
                api_key: None,
                models: Some(AiModelConfigUpdate {
                    summary: Some("gpt-4o".into()),
                    code_review: None, edge_cases: None, related_files: None,
                    follow_up: None, chat: None, ac_validation: None,
                    adversarial_tests: None, contracts: None, behavioral_delta: None,
                    fix: None,
                }),
            }),
            sandbox: None,
            cache: None,
            harness: None,
        };

        let (new_cfg, restart_fields) = apply_update(&cfg, update);
        assert_eq!(new_cfg.gitlab.url, "https://gitlab.example.com");
        assert_eq!(new_cfg.ai.base_url, "https://new-api.example.com");
        assert_eq!(new_cfg.ai.models.summary, "gpt-4o");
        // Unchanged fields preserved
        assert_eq!(new_cfg.ai.models.code_review, "claude-sonnet-4-5");
        assert!(restart_fields.is_empty());
    }

    #[test]
    fn apply_update_preserves_redacted_secrets() {
        let cfg = test_config();
        let update = ConfigUpdate {
            server: None,
            auth: Some(AuthConfigUpdate {
                api_key: Some("••••1234".into()), // redacted — should be ignored
            }),
            gitlab: Some(GitLabConfigUpdate {
                url: None,
                bot_token: Some("••••xxxx".into()), // redacted
                webhook_secret: Some("••••abcd".into()), // redacted
            }),
            ai: Some(AiConfigUpdate {
                base_url: None,
                api_key: Some("••••5678".into()), // redacted
                models: None,
            }),
            sandbox: None,
            cache: None,
            harness: None,
        };

        let (new_cfg, _) = apply_update(&cfg, update);
        // All secrets should be unchanged
        assert_eq!(new_cfg.auth.api_key, "test-secret-key-1234");
        assert_eq!(new_cfg.gitlab.bot_token, "glpat-xxxxxxxxxxxxxxxxxxxx");
        assert_eq!(new_cfg.gitlab.webhook_secret, Some("webhook-secret-abcd".into()));
        assert_eq!(new_cfg.ai.api_key, "sk-ai-key-5678");
    }

    #[test]
    fn apply_update_accepts_new_secrets() {
        let cfg = test_config();
        let update = ConfigUpdate {
            server: None,
            auth: Some(AuthConfigUpdate {
                api_key: Some("brand-new-key".into()),
            }),
            gitlab: Some(GitLabConfigUpdate {
                url: None,
                bot_token: Some("glpat-new-token".into()),
                webhook_secret: None,
            }),
            ai: None,
            sandbox: None,
            cache: None,
            harness: None,
        };

        let (new_cfg, _) = apply_update(&cfg, update);
        assert_eq!(new_cfg.auth.api_key, "brand-new-key");
        assert_eq!(new_cfg.gitlab.bot_token, "glpat-new-token");
    }

    #[test]
    fn apply_update_reports_restart_fields() {
        let cfg = test_config();
        let update = ConfigUpdate {
            server: Some(ServerConfigUpdate {
                host: Some("127.0.0.1".into()),
                port: Some(8080),
                max_concurrent_reviews: None,
                max_concurrent_ai_calls: None,
            }),
            auth: None,
            gitlab: None,
            ai: None,
            sandbox: None,
            cache: None,
            harness: None,
        };

        let (new_cfg, restart_fields) = apply_update(&cfg, update);
        assert_eq!(new_cfg.server.host, "127.0.0.1");
        assert_eq!(new_cfg.server.port, 8080);
        assert!(restart_fields.contains(&"server.host".to_string()));
        assert!(restart_fields.contains(&"server.port".to_string()));
        assert_eq!(restart_fields.len(), 2);
    }

    #[test]
    fn apply_update_no_restart_for_same_values() {
        let cfg = test_config();
        let update = ConfigUpdate {
            server: Some(ServerConfigUpdate {
                host: Some("0.0.0.0".into()), // same as current
                port: Some(7700),             // same as current
                max_concurrent_reviews: None,
                max_concurrent_ai_calls: None,
            }),
            auth: None,
            gitlab: None,
            ai: None,
            sandbox: None,
            cache: None,
            harness: None,
        };

        let (_, restart_fields) = apply_update(&cfg, update);
        assert!(restart_fields.is_empty());
    }

    #[test]
    fn apply_update_sandbox_fix_branch_mode() {
        let cfg = test_config();
        let update = ConfigUpdate {
            server: None, auth: None, gitlab: None, ai: None,
            sandbox: Some(SandboxConfigUpdate {
                enabled: None,
                max_concurrent: None,
                timeout_seconds: None,
                max_memory_mb: None,
                max_disk_mb: None,
                fix_branch_mode: Some("new_branch".into()),
            }),
            cache: None,
            harness: None,
        };

        let (new_cfg, _) = apply_update(&cfg, update);
        assert_eq!(new_cfg.sandbox.fix_branch_mode, FixBranchMode::NewBranch);
    }

    #[test]
    fn apply_update_clears_webhook_secret() {
        let cfg = test_config();
        let update = ConfigUpdate {
            server: None, auth: None,
            gitlab: Some(GitLabConfigUpdate {
                url: None,
                bot_token: None,
                webhook_secret: Some("".into()), // empty = clear
            }),
            ai: None, sandbox: None, cache: None, harness: None,
        };

        let (new_cfg, _) = apply_update(&cfg, update);
        assert_eq!(new_cfg.gitlab.webhook_secret, None);
    }

    // -- to_toml_string --

    #[test]
    fn to_toml_roundtrip() {
        let cfg = test_config();
        let toml_str = to_toml_string(&cfg).unwrap();

        // Should contain key sections
        assert!(toml_str.contains("[server]"));
        assert!(toml_str.contains("[auth]"));
        assert!(toml_str.contains("[gitlab]"));
        assert!(toml_str.contains("[ai]"));
        assert!(toml_str.contains("[sandbox]"));
        assert!(toml_str.contains("[cache]"));
        assert!(toml_str.contains("[harness]"));

        // Should contain actual values
        assert!(toml_str.contains("port = 7700"));
        assert!(toml_str.contains("\"https://gitlab.com\""));
        assert!(toml_str.contains("fix_branch_mode = \"same_branch\""));
        assert!(toml_str.contains("review_ttl_days = 7"));
    }
}
