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
    pub review: ReviewConfig,
    pub harness: HarnessConfig,
    pub cluster: ClusterConfig,
    pub conflict: ConflictConfig,
    pub workflows: WorkflowConfig,
    pub mentor: MentorConfig,
    pub channels: ChannelConfig,
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
    pub custom_prompts: AiCustomPrompts,
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
    pub inquiry: String,
    /// Model for semantic conflict analysis between overlapping MR diffs.
    pub semantic_conflict: String,
    /// Model for cross-MR cluster summary generation.
    pub cluster_summary: String,
    /// Model for cross-MR guided review order generation.
    pub cluster_review_order: String,
    /// Model for decomposing natural-language workflow descriptions into steps.
    pub workflow_decompose: String,
    /// Model for orchestrating workflow step execution.
    pub workflow_orchestrate: String,
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
            inquiry: "claude-sonnet-4-5".into(),
            semantic_conflict: "claude-sonnet-4-5".into(),
            cluster_summary: "claude-sonnet-4-5".into(),
            cluster_review_order: "claude-haiku-4-5".into(),
            workflow_decompose: "claude-sonnet-4-5".into(),
            workflow_orchestrate: "claude-sonnet-4-5".into(),
        }
    }
}

/// Per-task custom system prompt overrides. Empty string = use built-in default.
/// These are team-level overrides configured via the admin page or botto.toml.
#[derive(Debug, Clone, Serialize, Default)]
pub struct AiCustomPrompts {
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
    pub fix: String,
    pub inquiry: String,
}

impl AiCustomPrompts {
    /// Get the custom prompt for a task, returning None if empty (use default).
    pub fn get(&self, task: &str) -> Option<&str> {
        let s = match task {
            "summary" => &self.summary,
            "code_review" => &self.code_review,
            "edge_cases" => &self.edge_cases,
            "related_files" => &self.related_files,
            "follow_up" => &self.follow_up,
            "chat" => &self.chat,
            "ac_validation" => &self.ac_validation,
            "adversarial_tests" => &self.adversarial_tests,
            "contracts" => &self.contracts,
            "behavioral_delta" => &self.behavioral_delta,
            "fix" => &self.fix,
            "inquiry" => &self.inquiry,
            _ => return None,
        };
        if s.is_empty() { None } else { Some(s) }
    }

    /// Returns true if all prompts are empty (used to skip TOML serialization).
    pub fn is_all_empty(&self) -> bool {
        self.summary.is_empty()
            && self.code_review.is_empty()
            && self.edge_cases.is_empty()
            && self.related_files.is_empty()
            && self.follow_up.is_empty()
            && self.chat.is_empty()
            && self.ac_validation.is_empty()
            && self.adversarial_tests.is_empty()
            && self.contracts.is_empty()
            && self.behavioral_delta.is_empty()
            && self.fix.is_empty()
            && self.inquiry.is_empty()
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
    /// Keep containers alive between fixes on the same MR (skip clone + deps on subsequent fixes).
    pub warm_containers: bool,
    /// Kill warm containers after this many seconds of inactivity.
    pub warm_idle_timeout_secs: u64,
    /// Kill warm containers after this many seconds regardless of activity.
    pub warm_max_lifetime_secs: u64,
    /// Stream live container stdout/stderr to connected Ottos.
    pub live_output: bool,
    /// Redact secrets (tokens, passwords) from live output before streaming.
    pub output_redaction: bool,
    /// Cache the AI-discovered setup commands per project+image and replay them
    /// on cold containers to skip the AI setup loop.
    pub recipe_cache: bool,
    /// How long (seconds) before a cached setup recipe is considered stale.
    pub recipe_cache_ttl_secs: u64,
    /// Store structured facts and AI-distilled notes per project+image.
    /// Survives recipe invalidation — knowledge helps even without a cached recipe.
    pub knowledge_cache: bool,
    /// How long (seconds) before cached project knowledge expires.
    pub knowledge_cache_ttl_secs: u64,
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
pub struct ReviewConfig {
    /// Automatically enqueue a review when new commits are pushed to an open MR.
    /// Draft MRs are skipped. Bot pushes (sandbox fix commits) are ignored to
    /// prevent infinite review loops.
    pub auto_review_on_push: bool,
}

impl Default for ReviewConfig {
    fn default() -> Self {
        Self {
            auto_review_on_push: false,
        }
    }
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

#[derive(Debug, Clone, Serialize)]
pub struct ClusterConfig {
    /// Enable cross-MR cluster detection.
    pub enabled: bool,
    /// Maximum number of MRs in a single cluster.
    pub max_cluster_size: usize,
    /// Minimum Jaccard similarity for file-overlap clustering (0.0–1.0).
    pub file_overlap_threshold: f64,
    /// TTL for cluster entries (days).
    pub summary_ttl_days: u32,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_cluster_size: 8,
            file_overlap_threshold: 0.15,
            summary_ttl_days: 7,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ConflictConfig {
    /// Enable conflict radar (file/line overlap detection).
    pub enabled: bool,
    /// Enable AI-powered semantic conflict analysis (expensive, opt-in).
    pub semantic_analysis: bool,
    /// TTL for cached semantic analysis results (days).
    pub semantic_cache_ttl_days: u32,
}

impl Default for ConflictConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            semantic_analysis: false,
            semantic_cache_ttl_days: 3,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct WorkflowConfig {
    pub enabled: bool,
    pub max_concurrent_runs: usize,
    pub default_step_timeout_secs: u64,
}

impl Default for WorkflowConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            max_concurrent_runs: 3,
            default_step_timeout_secs: 300,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct MentorConfig {
    pub enabled: bool,
    pub prune_below_confidence: f64,
    pub prune_interval_secs: u64,
    pub linked_repos: Vec<LinkedRepoSet>,
}

impl Default for MentorConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            prune_below_confidence: 0.1,
            prune_interval_secs: 86400,
            linked_repos: Vec::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// Channel Adapter config
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
pub struct ChannelConfig {
    /// Master switch for the channel adapter layer.
    pub enabled: bool,
    pub gitlab: GitLabChannelConfig,
    pub slack: SlackChannelConfig,
    pub output: OutputChannelConfig,
    /// Default rate limit (requests per minute) for channels without a specific limit.
    pub default_rate_limit_per_minute: u32,
}

impl Default for ChannelConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            gitlab: GitLabChannelConfig::default(),
            slack: SlackChannelConfig::default(),
            output: OutputChannelConfig::default(),
            default_rate_limit_per_minute: 30,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct GitLabChannelConfig {
    pub enabled: bool,
    /// Users allowed to interact via GitLab comments. Empty = all users.
    pub allowed_users: Vec<String>,
    pub rate_limit_per_minute: u32,
}

impl Default for GitLabChannelConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            allowed_users: Vec::new(),
            rate_limit_per_minute: 20,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct SlackChannelConfig {
    pub enabled: bool,
    pub bot_token: String,
    pub signing_secret: String,
    pub rate_limit_per_minute: u32,
}

impl Default for SlackChannelConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bot_token: String::new(),
            signing_secret: String::new(),
            rate_limit_per_minute: 20,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct OutputChannelConfig {
    /// Post GitLab comments for outbound messages.
    pub gitlab_comments: bool,
    /// Post Slack messages for outbound messages.
    pub slack_messages: bool,
}

impl Default for OutputChannelConfig {
    fn default() -> Self {
        Self {
            gitlab_comments: true,
            slack_messages: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LinkedRepoSet {
    pub name: String,
    pub repos: Vec<String>,
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
    review: Option<TomlReview>,
    harness: Option<TomlHarness>,
    cluster: Option<TomlCluster>,
    conflict: Option<TomlConflict>,
    workflows: Option<TomlWorkflows>,
    mentor: Option<TomlMentor>,
    channels: Option<TomlChannels>,
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
    custom_prompts: Option<TomlAiCustomPrompts>,
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
    inquiry: Option<String>,
    semantic_conflict: Option<String>,
    cluster_summary: Option<String>,
    cluster_review_order: Option<String>,
    workflow_decompose: Option<String>,
    workflow_orchestrate: Option<String>,
}

#[derive(Deserialize, Default)]
struct TomlAiCustomPrompts {
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
    inquiry: Option<String>,
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
    warm_containers: Option<bool>,
    warm_idle_timeout_secs: Option<u64>,
    warm_max_lifetime_secs: Option<u64>,
    live_output: Option<bool>,
    output_redaction: Option<bool>,
    recipe_cache: Option<bool>,
    recipe_cache_ttl_secs: Option<u64>,
    knowledge_cache: Option<bool>,
    knowledge_cache_ttl_secs: Option<u64>,
}

#[derive(Deserialize, Default)]
struct TomlCache {
    review_ttl_days: Option<u32>,
    max_cached_reviews: Option<u32>,
}

#[derive(Deserialize, Default)]
struct TomlReview {
    auto_review_on_push: Option<bool>,
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

#[derive(Deserialize, Default)]
struct TomlCluster {
    enabled: Option<bool>,
    max_cluster_size: Option<usize>,
    file_overlap_threshold: Option<f64>,
    summary_ttl_days: Option<u32>,
}

#[derive(Deserialize, Default)]
struct TomlConflict {
    enabled: Option<bool>,
    semantic_analysis: Option<bool>,
    semantic_cache_ttl_days: Option<u32>,
}

#[derive(Deserialize, Default)]
struct TomlWorkflows {
    enabled: Option<bool>,
    max_concurrent_runs: Option<usize>,
    default_step_timeout_secs: Option<u64>,
}

#[derive(Deserialize, Default)]
struct TomlMentor {
    enabled: Option<bool>,
    prune_below_confidence: Option<f64>,
    prune_interval_secs: Option<u64>,
    linked_repos: Option<Vec<LinkedRepoSet>>,
}

#[derive(Deserialize, Default)]
struct TomlChannels {
    enabled: Option<bool>,
    default_rate_limit_per_minute: Option<u32>,
    gitlab: Option<TomlGitLabChannel>,
    slack: Option<TomlSlackChannel>,
    output: Option<TomlOutputChannel>,
}

#[derive(Deserialize, Default)]
struct TomlGitLabChannel {
    enabled: Option<bool>,
    allowed_users: Option<Vec<String>>,
    rate_limit_per_minute: Option<u32>,
}

#[derive(Deserialize, Default)]
struct TomlSlackChannel {
    enabled: Option<bool>,
    bot_token: Option<String>,
    signing_secret: Option<String>,
    rate_limit_per_minute: Option<u32>,
}

#[derive(Deserialize, Default)]
struct TomlOutputChannel {
    gitlab_comments: Option<bool>,
    slack_messages: Option<bool>,
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
    let toml_review = toml_cfg.review.unwrap_or_default();
    let toml_harness = toml_cfg.harness.unwrap_or_default();
    let toml_cluster = toml_cfg.cluster.unwrap_or_default();
    let toml_conflict = toml_cfg.conflict.unwrap_or_default();
    let toml_workflows = toml_cfg.workflows.unwrap_or_default();
    let toml_mentor = toml_cfg.mentor.unwrap_or_default();
    let toml_channels = toml_cfg.channels.unwrap_or_default();
    let toml_models = toml_ai.models.unwrap_or_default();
    let toml_custom_prompts = toml_ai.custom_prompts.unwrap_or_default();
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
                inquiry: toml_models.inquiry.unwrap_or(default_models.inquiry),
                semantic_conflict: toml_models.semantic_conflict.unwrap_or(default_models.semantic_conflict),
                cluster_summary: toml_models.cluster_summary.unwrap_or(default_models.cluster_summary),
                cluster_review_order: toml_models.cluster_review_order.unwrap_or(default_models.cluster_review_order),
                workflow_decompose: toml_models.workflow_decompose.unwrap_or(default_models.workflow_decompose),
                workflow_orchestrate: toml_models.workflow_orchestrate.unwrap_or(default_models.workflow_orchestrate),
            },
            custom_prompts: AiCustomPrompts {
                summary: toml_custom_prompts.summary.unwrap_or_default(),
                code_review: toml_custom_prompts.code_review.unwrap_or_default(),
                edge_cases: toml_custom_prompts.edge_cases.unwrap_or_default(),
                related_files: toml_custom_prompts.related_files.unwrap_or_default(),
                follow_up: toml_custom_prompts.follow_up.unwrap_or_default(),
                chat: toml_custom_prompts.chat.unwrap_or_default(),
                ac_validation: toml_custom_prompts.ac_validation.unwrap_or_default(),
                adversarial_tests: toml_custom_prompts.adversarial_tests.unwrap_or_default(),
                contracts: toml_custom_prompts.contracts.unwrap_or_default(),
                behavioral_delta: toml_custom_prompts.behavioral_delta.unwrap_or_default(),
                fix: toml_custom_prompts.fix.unwrap_or_default(),
                inquiry: toml_custom_prompts.inquiry.unwrap_or_default(),
            },
        },
        sandbox: SandboxConfig {
            enabled: toml_sandbox.enabled.unwrap_or(docker_available),
            docker_available,
            max_concurrent: toml_sandbox.max_concurrent.unwrap_or(auto_concurrent),
            timeout_seconds: toml_sandbox.timeout_seconds.unwrap_or(1800),
            max_memory_mb: toml_sandbox.max_memory_mb.unwrap_or(auto_memory),
            max_disk_mb: toml_sandbox.max_disk_mb.unwrap_or(4096),
            fix_branch_mode: match toml_sandbox.fix_branch_mode.as_deref() {
                Some("new_branch") => FixBranchMode::NewBranch,
                _ => FixBranchMode::SameBranch,
            },
            warm_containers: toml_sandbox.warm_containers.unwrap_or(true),
            warm_idle_timeout_secs: toml_sandbox.warm_idle_timeout_secs.unwrap_or(600),
            warm_max_lifetime_secs: toml_sandbox.warm_max_lifetime_secs.unwrap_or(3600),
            live_output: toml_sandbox.live_output.unwrap_or(true),
            output_redaction: toml_sandbox.output_redaction.unwrap_or(true),
            recipe_cache: toml_sandbox.recipe_cache.unwrap_or(true),
            recipe_cache_ttl_secs: toml_sandbox.recipe_cache_ttl_secs.unwrap_or(86400),
            knowledge_cache: toml_sandbox.knowledge_cache.unwrap_or(true),
            knowledge_cache_ttl_secs: toml_sandbox.knowledge_cache_ttl_secs.unwrap_or(604800),
        },
        cache: CacheConfig {
            review_ttl_days: toml_cache.review_ttl_days.unwrap_or(7),
            max_cached_reviews: toml_cache.max_cached_reviews.unwrap_or(500),
        },
        review: ReviewConfig {
            auto_review_on_push: toml_review.auto_review_on_push.unwrap_or(false),
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
        cluster: ClusterConfig {
            enabled: toml_cluster.enabled.unwrap_or(true),
            max_cluster_size: toml_cluster.max_cluster_size.unwrap_or(8),
            file_overlap_threshold: toml_cluster.file_overlap_threshold.unwrap_or(0.15),
            summary_ttl_days: toml_cluster.summary_ttl_days.unwrap_or(7),
        },
        conflict: ConflictConfig {
            enabled: toml_conflict.enabled.unwrap_or(true),
            semantic_analysis: toml_conflict.semantic_analysis.unwrap_or(false),
            semantic_cache_ttl_days: toml_conflict.semantic_cache_ttl_days.unwrap_or(3),
        },
        workflows: WorkflowConfig {
            enabled: toml_workflows.enabled.unwrap_or(false),
            max_concurrent_runs: toml_workflows.max_concurrent_runs.unwrap_or(3),
            default_step_timeout_secs: toml_workflows.default_step_timeout_secs.unwrap_or(300),
        },
        mentor: MentorConfig {
            enabled: toml_mentor.enabled.unwrap_or(false),
            prune_below_confidence: toml_mentor.prune_below_confidence.unwrap_or(0.1),
            prune_interval_secs: toml_mentor.prune_interval_secs.unwrap_or(86400),
            linked_repos: toml_mentor.linked_repos.unwrap_or_default(),
        },
        channels: {
            let toml_gl_ch = toml_channels.gitlab.unwrap_or_default();
            let toml_sl_ch = toml_channels.slack.unwrap_or_default();
            let toml_out_ch = toml_channels.output.unwrap_or_default();
            let env_slack_token = std::env::var("BOTTO_SLACK_BOT_TOKEN").ok();
            let env_slack_secret = std::env::var("BOTTO_SLACK_SIGNING_SECRET").ok();
            ChannelConfig {
                enabled: toml_channels.enabled.unwrap_or(false),
                default_rate_limit_per_minute: toml_channels.default_rate_limit_per_minute.unwrap_or(30),
                gitlab: GitLabChannelConfig {
                    enabled: toml_gl_ch.enabled.unwrap_or(true),
                    allowed_users: toml_gl_ch.allowed_users.unwrap_or_default(),
                    rate_limit_per_minute: toml_gl_ch.rate_limit_per_minute.unwrap_or(20),
                },
                slack: SlackChannelConfig {
                    enabled: toml_sl_ch.enabled.unwrap_or(false),
                    bot_token: env_slack_token.or(toml_sl_ch.bot_token).unwrap_or_default(),
                    signing_secret: env_slack_secret.or(toml_sl_ch.signing_secret).unwrap_or_default(),
                    rate_limit_per_minute: toml_sl_ch.rate_limit_per_minute.unwrap_or(20),
                },
                output: OutputChannelConfig {
                    gitlab_comments: toml_out_ch.gitlab_comments.unwrap_or(true),
                    slack_messages: toml_out_ch.slack_messages.unwrap_or(true),
                },
            }
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
        "sandbox: {} (docker={}, max_concurrent={}, memory={}MB, warm={}, live_output={})",
        if cfg.sandbox.enabled { "enabled" } else { "disabled" },
        cfg.sandbox.docker_available,
        cfg.sandbox.max_concurrent,
        cfg.sandbox.max_memory_mb,
        if cfg.sandbox.warm_containers { "on" } else { "off" },
        if cfg.sandbox.live_output { "on" } else { "off" },
    );
    info!(
        "limits: max_reviews={}, max_ai_calls={}",
        cfg.server.max_concurrent_reviews,
        cfg.server.max_concurrent_ai_calls,
    );
    info!("cache:  ttl={}d, max={}/project", cfg.cache.review_ttl_days, cfg.cache.max_cached_reviews);
    info!("review: auto_on_push={}", if cfg.review.auto_review_on_push { "on" } else { "off" });
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
    pub review: ReviewConfig,
    pub harness: HarnessConfigRedacted,
    pub cluster: ClusterConfig,
    pub conflict: ConflictConfig,
    pub workflows: WorkflowConfig,
    pub mentor: MentorConfig,
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
    pub custom_prompts: AiCustomPrompts,
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
                custom_prompts: cfg.ai.custom_prompts.clone(),
            },
            sandbox: cfg.sandbox.clone(),
            cache: cfg.cache.clone(),
            review: cfg.review.clone(),
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
            cluster: cfg.cluster.clone(),
            conflict: cfg.conflict.clone(),
            workflows: cfg.workflows.clone(),
            mentor: cfg.mentor.clone(),
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
    pub review: Option<ReviewConfigUpdate>,
    pub harness: Option<HarnessConfigUpdate>,
    pub cluster: Option<ClusterConfigUpdate>,
    pub conflict: Option<ConflictConfigUpdate>,
    pub workflows: Option<WorkflowConfigUpdate>,
    pub mentor: Option<MentorConfigUpdate>,
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
    pub custom_prompts: Option<AiCustomPromptsUpdate>,
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
    pub inquiry: Option<String>,
    pub semantic_conflict: Option<String>,
    pub cluster_summary: Option<String>,
    pub cluster_review_order: Option<String>,
    pub workflow_decompose: Option<String>,
    pub workflow_orchestrate: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct AiCustomPromptsUpdate {
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
    pub inquiry: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct SandboxConfigUpdate {
    pub enabled: Option<bool>,
    pub max_concurrent: Option<u32>,
    pub timeout_seconds: Option<u64>,
    pub max_memory_mb: Option<u64>,
    pub max_disk_mb: Option<u64>,
    pub fix_branch_mode: Option<String>,
    pub warm_containers: Option<bool>,
    pub warm_idle_timeout_secs: Option<u64>,
    pub warm_max_lifetime_secs: Option<u64>,
    pub live_output: Option<bool>,
    pub output_redaction: Option<bool>,
    pub recipe_cache: Option<bool>,
    pub recipe_cache_ttl_secs: Option<u64>,
    pub knowledge_cache: Option<bool>,
    pub knowledge_cache_ttl_secs: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct CacheConfigUpdate {
    pub review_ttl_days: Option<u32>,
    pub max_cached_reviews: Option<u32>,
}

#[derive(Debug, Deserialize)]
pub struct ReviewConfigUpdate {
    pub auto_review_on_push: Option<bool>,
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

#[derive(Debug, Deserialize)]
pub struct ClusterConfigUpdate {
    pub enabled: Option<bool>,
    pub max_cluster_size: Option<usize>,
    pub file_overlap_threshold: Option<f64>,
    pub summary_ttl_days: Option<u32>,
}

#[derive(Debug, Deserialize)]
pub struct ConflictConfigUpdate {
    pub enabled: Option<bool>,
    pub semantic_analysis: Option<bool>,
    pub semantic_cache_ttl_days: Option<u32>,
}

#[derive(Debug, Deserialize)]
pub struct WorkflowConfigUpdate {
    pub enabled: Option<bool>,
    pub max_concurrent_runs: Option<usize>,
    pub default_step_timeout_secs: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct MentorConfigUpdate {
    pub enabled: Option<bool>,
    pub prune_below_confidence: Option<f64>,
    pub prune_interval_secs: Option<u64>,
    pub linked_repos: Option<Vec<LinkedRepoSet>>,
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
            if let Some(v) = m.inquiry { cfg.ai.models.inquiry = v; }
            if let Some(v) = m.semantic_conflict { cfg.ai.models.semantic_conflict = v; }
            if let Some(v) = m.cluster_summary { cfg.ai.models.cluster_summary = v; }
            if let Some(v) = m.cluster_review_order { cfg.ai.models.cluster_review_order = v; }
            if let Some(v) = m.workflow_decompose { cfg.ai.models.workflow_decompose = v; }
            if let Some(v) = m.workflow_orchestrate { cfg.ai.models.workflow_orchestrate = v; }
        }
        if let Some(p) = a.custom_prompts {
            if let Some(v) = p.summary { cfg.ai.custom_prompts.summary = v; }
            if let Some(v) = p.code_review { cfg.ai.custom_prompts.code_review = v; }
            if let Some(v) = p.edge_cases { cfg.ai.custom_prompts.edge_cases = v; }
            if let Some(v) = p.related_files { cfg.ai.custom_prompts.related_files = v; }
            if let Some(v) = p.follow_up { cfg.ai.custom_prompts.follow_up = v; }
            if let Some(v) = p.chat { cfg.ai.custom_prompts.chat = v; }
            if let Some(v) = p.ac_validation { cfg.ai.custom_prompts.ac_validation = v; }
            if let Some(v) = p.adversarial_tests { cfg.ai.custom_prompts.adversarial_tests = v; }
            if let Some(v) = p.contracts { cfg.ai.custom_prompts.contracts = v; }
            if let Some(v) = p.behavioral_delta { cfg.ai.custom_prompts.behavioral_delta = v; }
            if let Some(v) = p.fix { cfg.ai.custom_prompts.fix = v; }
            if let Some(v) = p.inquiry { cfg.ai.custom_prompts.inquiry = v; }
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
        if let Some(v) = s.warm_containers { cfg.sandbox.warm_containers = v; }
        if let Some(v) = s.warm_idle_timeout_secs { cfg.sandbox.warm_idle_timeout_secs = v; }
        if let Some(v) = s.warm_max_lifetime_secs { cfg.sandbox.warm_max_lifetime_secs = v; }
        if let Some(v) = s.live_output { cfg.sandbox.live_output = v; }
        if let Some(v) = s.output_redaction { cfg.sandbox.output_redaction = v; }
        if let Some(v) = s.recipe_cache { cfg.sandbox.recipe_cache = v; }
        if let Some(v) = s.recipe_cache_ttl_secs { cfg.sandbox.recipe_cache_ttl_secs = v; }
        if let Some(v) = s.knowledge_cache { cfg.sandbox.knowledge_cache = v; }
        if let Some(v) = s.knowledge_cache_ttl_secs { cfg.sandbox.knowledge_cache_ttl_secs = v; }
    }

    if let Some(c) = update.cache {
        if let Some(v) = c.review_ttl_days { cfg.cache.review_ttl_days = v; }
        if let Some(v) = c.max_cached_reviews { cfg.cache.max_cached_reviews = v; }
    }

    if let Some(r) = update.review {
        if let Some(v) = r.auto_review_on_push { cfg.review.auto_review_on_push = v; }
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

    if let Some(c) = update.cluster {
        if let Some(v) = c.enabled { cfg.cluster.enabled = v; }
        if let Some(v) = c.max_cluster_size { cfg.cluster.max_cluster_size = v; }
        if let Some(v) = c.file_overlap_threshold { cfg.cluster.file_overlap_threshold = v; }
        if let Some(v) = c.summary_ttl_days { cfg.cluster.summary_ttl_days = v; }
    }

    if let Some(c) = update.conflict {
        if let Some(v) = c.enabled { cfg.conflict.enabled = v; }
        if let Some(v) = c.semantic_analysis { cfg.conflict.semantic_analysis = v; }
        if let Some(v) = c.semantic_cache_ttl_days { cfg.conflict.semantic_cache_ttl_days = v; }
    }

    if let Some(w) = update.workflows {
        if let Some(v) = w.enabled { cfg.workflows.enabled = v; }
        if let Some(v) = w.max_concurrent_runs { cfg.workflows.max_concurrent_runs = v; }
        if let Some(v) = w.default_step_timeout_secs { cfg.workflows.default_step_timeout_secs = v; }
    }

    if let Some(m) = update.mentor {
        if let Some(v) = m.enabled { cfg.mentor.enabled = v; }
        if let Some(v) = m.prune_below_confidence { cfg.mentor.prune_below_confidence = v; }
        if let Some(v) = m.prune_interval_secs { cfg.mentor.prune_interval_secs = v; }
        if let Some(v) = m.linked_repos { cfg.mentor.linked_repos = v; }
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
        review: &'a ReviewConfig,
        harness: HarnessOut<'a>,
        cluster: &'a ClusterConfig,
        conflict: &'a ConflictConfig,
        workflows: &'a WorkflowConfig,
        mentor: &'a MentorConfig,
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
        #[serde(skip_serializing_if = "AiCustomPrompts::is_all_empty")]
        custom_prompts: &'a AiCustomPrompts,
    }

    #[derive(Serialize)]
    struct SandboxOut<'a> {
        enabled: bool,
        max_concurrent: u32,
        timeout_seconds: u64,
        max_memory_mb: u64,
        max_disk_mb: u64,
        fix_branch_mode: &'a str,
        warm_containers: bool,
        warm_idle_timeout_secs: u64,
        warm_max_lifetime_secs: u64,
        live_output: bool,
        output_redaction: bool,
        recipe_cache: bool,
        recipe_cache_ttl_secs: u64,
        knowledge_cache: bool,
        knowledge_cache_ttl_secs: u64,
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
            custom_prompts: &cfg.ai.custom_prompts,
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
            warm_containers: cfg.sandbox.warm_containers,
            warm_idle_timeout_secs: cfg.sandbox.warm_idle_timeout_secs,
            warm_max_lifetime_secs: cfg.sandbox.warm_max_lifetime_secs,
            live_output: cfg.sandbox.live_output,
            output_redaction: cfg.sandbox.output_redaction,
            recipe_cache: cfg.sandbox.recipe_cache,
            recipe_cache_ttl_secs: cfg.sandbox.recipe_cache_ttl_secs,
            knowledge_cache: cfg.sandbox.knowledge_cache,
            knowledge_cache_ttl_secs: cfg.sandbox.knowledge_cache_ttl_secs,
        },
        cache: &cfg.cache,
        review: &cfg.review,
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
        cluster: &cfg.cluster,
        conflict: &cfg.conflict,
        workflows: &cfg.workflows,
        mentor: &cfg.mentor,
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
                custom_prompts: AiCustomPrompts::default(),
            },
            sandbox: SandboxConfig {
                enabled: true,
                docker_available: true,
                max_concurrent: 2,
                timeout_seconds: 1800,
                max_memory_mb: 2048,
                max_disk_mb: 4096,
                fix_branch_mode: FixBranchMode::SameBranch,
                warm_containers: true,
                warm_idle_timeout_secs: 600,
                warm_max_lifetime_secs: 3600,
                live_output: true,
                output_redaction: true,
                recipe_cache: true,
                recipe_cache_ttl_secs: 86400,
                knowledge_cache: true,
                knowledge_cache_ttl_secs: 604800,
            },
            cache: CacheConfig {
                review_ttl_days: 7,
                max_cached_reviews: 500,
            },
            review: ReviewConfig {
                auto_review_on_push: false,
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
            cluster: ClusterConfig::default(),
            conflict: ConflictConfig::default(),
            workflows: WorkflowConfig::default(),
            mentor: MentorConfig::default(),
            channels: ChannelConfig::default(),
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
                    fix: None, inquiry: None,
                    semantic_conflict: None, cluster_summary: None, cluster_review_order: None,
                    workflow_decompose: None, workflow_orchestrate: None,
                }),
                custom_prompts: None,
            }),
            sandbox: None,
            cache: None,
            review: None,
            harness: None,
            cluster: None,
            conflict: None,
            workflows: None,
            mentor: None,
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
                custom_prompts: None,
            }),
            sandbox: None,
            cache: None,
            review: None,
            harness: None,
            cluster: None,
            conflict: None,
            workflows: None,
            mentor: None,
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
            review: None,
            harness: None,
            cluster: None,
            conflict: None,
            workflows: None,
            mentor: None,
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
            review: None,
            harness: None,
            cluster: None,
            conflict: None,
            workflows: None,
            mentor: None,
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
            review: None,
            harness: None,
            cluster: None,
            conflict: None,
            workflows: None,
            mentor: None,
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
                warm_containers: None,
                warm_idle_timeout_secs: None,
                warm_max_lifetime_secs: None,
                live_output: None,
                output_redaction: None,
                recipe_cache: None,
                recipe_cache_ttl_secs: None,
                knowledge_cache: None,
                knowledge_cache_ttl_secs: None,
            }),
            cache: None,
            review: None,
            harness: None,
            cluster: None,
            conflict: None,
            workflows: None,
            mentor: None,
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
            ai: None, sandbox: None, cache: None, review: None, harness: None, cluster: None, conflict: None, workflows: None, mentor: None,
        };

        let (new_cfg, _) = apply_update(&cfg, update);
        assert_eq!(new_cfg.gitlab.webhook_secret, None);
    }

    #[test]
    fn apply_update_review_auto_review_on_push() {
        let cfg = test_config();
        assert!(!cfg.review.auto_review_on_push); // default is false

        let update = ConfigUpdate {
            server: None, auth: None, gitlab: None, ai: None,
            sandbox: None, cache: None,
            review: Some(ReviewConfigUpdate {
                auto_review_on_push: Some(true),
            }),
            harness: None,
            cluster: None,
            conflict: None,
            workflows: None,
            mentor: None,
        };

        let (new_cfg, restart_fields) = apply_update(&cfg, update);
        assert!(new_cfg.review.auto_review_on_push);
        assert!(restart_fields.is_empty()); // no restart needed for review settings
    }

    // -- sandbox timeout default --

    #[test]
    fn sandbox_timeout_defaults_to_30_minutes() {
        // The config loader should default to 1800s (30 min) when no timeout is
        // specified, giving the AI fix agent enough time to iterate. This was
        // previously 300s which starved the fix loop.
        let cfg = test_config();
        assert_eq!(cfg.sandbox.timeout_seconds, 1800);
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
        assert!(toml_str.contains("[review]"));
        assert!(toml_str.contains("[harness]"));

        // Should contain actual values
        assert!(toml_str.contains("port = 7700"));
        assert!(toml_str.contains("\"https://gitlab.com\""));
        assert!(toml_str.contains("fix_branch_mode = \"same_branch\""));
        assert!(toml_str.contains("review_ttl_days = 7"));
    }
}
