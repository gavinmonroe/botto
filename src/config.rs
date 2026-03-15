// ---------------------------------------------------------------------------
// Config — auto-detection + file-based configuration.
//
// Priority: CLI flags > botto.toml > auto-detected defaults.
// On first run with no config file, Botto auto-detects everything it can
// and prints a summary so the user knows what's active.
// ---------------------------------------------------------------------------

use anyhow::{Context, Result};
use serde::Deserialize;
use std::path::{Path, PathBuf};
use tracing::{info, warn};

// ---------------------------------------------------------------------------
// Config schema (matches botto.toml structure)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct BottoConfig {
    pub server: ServerConfig,
    pub auth: AuthConfig,
    pub gitlab: GitLabConfig,
    pub ai: AiConfig,
    pub sandbox: SandboxConfig,
    pub cache: CacheConfig,
    pub data_dir: PathBuf,
}

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    /// Max concurrent MR reviews running simultaneously.
    pub max_concurrent_reviews: usize,
    /// Max concurrent AI API calls across all reviews.
    pub max_concurrent_ai_calls: usize,
}

#[derive(Debug, Clone)]
pub struct AuthConfig {
    /// Shared API key that Otto extensions use to authenticate with Botto.
    pub api_key: String,
}

#[derive(Debug, Clone)]
pub struct GitLabConfig {
    pub url: String,
    pub bot_token: String,
    /// Webhook secret for validating incoming GitLab webhooks.
    pub webhook_secret: Option<String>,
}

#[derive(Debug, Clone)]
pub struct AiConfig {
    pub base_url: String,
    pub api_key: String,
    pub models: AiModelConfig,
}

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
pub struct SandboxConfig {
    pub enabled: bool,
    pub docker_available: bool,
    pub max_concurrent: u32,
    pub timeout_seconds: u64,
    pub max_memory_mb: u64,
    pub max_disk_mb: u64,
}

#[derive(Debug, Clone)]
pub struct CacheConfig {
    pub review_ttl_days: u32,
    pub max_cached_reviews: u32,
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
}

#[derive(Deserialize, Default)]
struct TomlCache {
    review_ttl_days: Option<u32>,
    max_cached_reviews: Option<u32>,
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
        },
        cache: CacheConfig {
            review_ttl_days: toml_cache.review_ttl_days.unwrap_or(7),
            max_cached_reviews: toml_cache.max_cached_reviews.unwrap_or(500),
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
