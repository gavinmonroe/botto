// ---------------------------------------------------------------------------
// Sandbox manager — Docker container lifecycle for auto-fix execution.
//
// The killer feature. When Botto detects an "easy fix" (review comment with
// a suggestion + originalCode), it can:
//   1. Clone the repo into a Docker container
//   2. Apply the fix
//   3. Run tests to validate
//   4. Commit + push on success
//
// Two strategies:
//   FullSetup — install deps, run full test suite
//   TestOnly  — run only relevant test files
//
// Container lifecycle:
//   CREATE → CLONE → SETUP (optional) → APPLY FIX → TEST → PUSH → CLEANUP
// ---------------------------------------------------------------------------

use crate::config::BottoConfig;
use crate::db;
use crate::services::ai::client::{
    self as ai_client, AiClientConfig, ChatCompletionRequest, ChatMessage,
};
use crate::services::events::{Event, EventBus, EventType};
use crate::services::harness::prompts::SandboxPrompts;
use crate::services::harness::types::{ConversationEntry, IterationBreakdown};
use crate::services::sandbox::detector::{self, FixStrategy};
use crate::types::state::MrRef;
use bollard::container::{
    Config as ContainerConfig, CreateContainerOptions, RemoveContainerOptions,
    StartContainerOptions,
};
use bollard::exec::{CreateExecOptions, StartExecResults};
use bollard::Docker;
use dashmap::DashMap;
use futures::StreamExt;
use serde_json::json;
use sqlx::SqlitePool;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::{Mutex, Semaphore};
use tracing::{debug, info, warn};

/// Default sandbox timeout — 30 minutes for the full pipeline.
const DEFAULT_SANDBOX_TIMEOUT_SECS: u64 = 1800;

// ---------------------------------------------------------------------------
// Warm container pool — reuse containers across fixes on the same MR.
//
// First fix on an MR pays the full setup cost (clone, deps, build).
// Subsequent fixes reuse the same container: git reset + pull, then
// jump straight to apply → test. Seconds instead of minutes.
//
// Lifecycle:
//   First fix  → cold path (create, clone, deps, setup) → keep alive
//   Next fix   → warm path (reset, pull) → apply → test → keep alive
//   Idle timer → kill
//   Author push → kill (stale checkout)
//   Botto push → reset + pull (own commit, keep warm)
//   MR merged/closed → kill
//   Max lifetime → kill
//   Botto restart → kill all
// ---------------------------------------------------------------------------

/// A warm container kept alive between fixes on the same MR.
struct WarmContainer {
    container_id: String,
    /// "{project_path}:{mr_iid}" — matches MrRef::key() format.
    mr_key: String,
    source_branch: String,
    /// Docker image used to create this container. If the detected image
    /// changes between fixes (e.g. .otto.json updated), we evict and cold-start.
    image: String,
    created_at: tokio::time::Instant,
    last_used: tokio::time::Instant,
    /// Commit SHAs pushed by Botto from this container. When a push webhook
    /// fires for one of these SHAs, we know it's our own push and keep the
    /// container warm instead of evicting it.
    bot_push_shas: HashSet<String>,
    /// Per-container mutex — only one fix at a time in a given container.
    /// Second fix on the same MR waits for the first to finish.
    lock: Arc<Mutex<()>>,
}

/// Pool of warm containers, keyed by MR. Shared across the application via AppState.
pub struct WarmPool {
    containers: DashMap<String, WarmContainer>,
    docker: Docker,
}

impl WarmPool {
    pub fn new() -> Option<Self> {
        let docker = Docker::connect_with_local_defaults().ok()?;
        Some(Self {
            containers: DashMap::new(),
            docker,
        })
    }

    /// Get the container ID and lock for a warm container, if one exists for this MR.
    /// Returns None if no warm container exists or the image doesn't match.
    pub fn get(&self, mr_key: &str, expected_image: &str) -> Option<(String, Arc<Mutex<()>>)> {
        let entry = self.containers.get(mr_key)?;
        // Image mismatch — .otto.json changed the sandbox image between fixes.
        // Evict and let the caller cold-start with the new image.
        if entry.image != expected_image {
            drop(entry);
            info!("warm pool: image mismatch for {}, evicting", mr_key);
            self.remove(mr_key);
            return None;
        }
        Some((entry.container_id.clone(), entry.lock.clone()))
    }

    /// Store a container in the warm pool after a successful fix.
    pub fn insert(
        &self,
        mr_key: String,
        container_id: String,
        source_branch: String,
        image: String,
    ) {
        let now = tokio::time::Instant::now();
        self.containers.insert(mr_key, WarmContainer {
            container_id,
            mr_key: String::new(), // not needed — the DashMap key is the mr_key
            source_branch,
            image,
            created_at: now,
            last_used: now,
            bot_push_shas: HashSet::new(),
            lock: Arc::new(Mutex::new(())),
        });
    }

    /// Update last_used timestamp (reset idle timer).
    pub fn touch(&self, mr_key: &str) {
        if let Some(mut entry) = self.containers.get_mut(mr_key) {
            entry.last_used = tokio::time::Instant::now();
        }
    }

    /// Register a commit SHA as pushed by Botto. Prevents self-eviction
    /// when the push webhook fires for this SHA.
    pub fn register_bot_push(&self, mr_key: &str, sha: &str) {
        if let Some(mut entry) = self.containers.get_mut(mr_key) {
            entry.bot_push_shas.insert(sha.to_string());
        }
    }

    /// Check if a commit SHA was pushed by Botto from this container.
    pub fn is_bot_push(&self, mr_key: &str, sha: &str) -> bool {
        self.containers
            .get(mr_key)
            .map(|e| e.bot_push_shas.contains(sha))
            .unwrap_or(false)
    }

    /// Get the source branch for a warm container (for webhook matching).
    pub fn get_branch(&self, mr_key: &str) -> Option<String> {
        self.containers.get(mr_key).map(|e| e.source_branch.clone())
    }

    /// Evict and destroy a warm container. Returns true if a container was removed.
    pub fn remove(&self, mr_key: &str) -> bool {
        if let Some((_, container)) = self.containers.remove(mr_key) {
            let docker = self.docker.clone();
            let container_id = container.container_id;
            // Spawn removal so we don't block the caller
            tokio::spawn(async move {
                let opts = RemoveContainerOptions { force: true, ..Default::default() };
                if let Err(e) = docker.remove_container(&container_id, Some(opts)).await {
                    warn!("warm pool: failed to remove container {}: {}", container_id, e);
                } else {
                    debug!("warm pool: removed container {}", container_id);
                }
            });
            true
        } else {
            false
        }
    }

    /// Evict all warm containers (shutdown cleanup).
    pub fn remove_all(&self) {
        let keys: Vec<String> = self.containers.iter().map(|e| e.key().clone()).collect();
        for key in keys {
            self.remove(&key);
        }
    }

    /// Reap idle and expired containers. Called periodically by the background task.
    pub fn reap(&self, idle_timeout_secs: u64, max_lifetime_secs: u64) -> usize {
        let now = tokio::time::Instant::now();
        let mut reaped = 0;

        let keys: Vec<String> = self.containers.iter().map(|e| e.key().clone()).collect();
        for key in keys {
            let should_remove = self.containers.get(&key).map(|e| {
                let idle = now.duration_since(e.last_used).as_secs() > idle_timeout_secs;
                let expired = now.duration_since(e.created_at).as_secs() > max_lifetime_secs;
                idle || expired
            }).unwrap_or(false);

            if should_remove {
                info!("warm pool: reaping container for {}", key);
                self.remove(&key);
                reaped += 1;
            }
        }
        reaped
    }

    /// Find all MR keys that have a warm container with the given source branch.
    /// Used by webhook handler to find which MRs are affected by a push to a branch.
    pub fn find_by_branch(&self, project_path: &str, branch: &str) -> Vec<String> {
        self.containers
            .iter()
            .filter(|e| {
                e.source_branch == branch && e.key().starts_with(project_path)
            })
            .map(|e| e.key().clone())
            .collect()
    }

    /// Number of warm containers currently in the pool.
    pub fn count(&self) -> usize {
        self.containers.len()
    }
}

/// Telemetry collector for harness runs. Zero-cost when None.
/// The sandbox manager writes to this during agent loops so the harness
/// runner can read iteration counts and conversation logs after the run.
pub struct HarnessTelemetry {
    pub setup_steps: std::sync::atomic::AtomicU32,
    pub fix_steps: std::sync::atomic::AtomicU32,
    pub retry_steps: std::sync::atomic::AtomicU32,
    pub conversation_log: Mutex<Vec<ConversationEntry>>,
}

impl HarnessTelemetry {
    pub fn new() -> Self {
        Self {
            setup_steps: std::sync::atomic::AtomicU32::new(0),
            fix_steps: std::sync::atomic::AtomicU32::new(0),
            retry_steps: std::sync::atomic::AtomicU32::new(0),
            conversation_log: Mutex::new(Vec::new()),
        }
    }

    pub fn iteration_breakdown(&self) -> IterationBreakdown {
        IterationBreakdown {
            setup_steps: self.setup_steps.load(std::sync::atomic::Ordering::Relaxed),
            fix_steps: self.fix_steps.load(std::sync::atomic::Ordering::Relaxed),
            retry_steps: self.retry_steps.load(std::sync::atomic::Ordering::Relaxed),
        }
    }

    pub fn total_steps(&self) -> u32 {
        let b = self.iteration_breakdown();
        b.setup_steps + b.fix_steps + b.retry_steps
    }
}

/// Manages sandbox container lifecycle with concurrency control.
pub struct SandboxManager {
    cfg: BottoConfig,
    pool: SqlitePool,
    docker: Docker,
    event_bus: EventBus,
    /// Limits concurrent sandbox containers.
    semaphore: Semaphore,
    /// Broadcast function for sending progress to connected Ottos.
    broadcaster: Arc<dyn Fn(&MrRef, &str) + Send + Sync>,
    /// Injectable prompt templates + code params. Defaults to production prompts.
    prompts: SandboxPrompts,
    /// When true, skip git push (used by harness runs).
    harness_mode: bool,
    /// Optional telemetry collector for harness runs.
    telemetry: Option<Arc<HarnessTelemetry>>,
    /// Warm container pool — reuse containers across fixes on the same MR.
    warm_pool: Option<Arc<WarmPool>>,
}

/// Input for a fix request.
#[derive(Debug, Clone)]
pub struct FixRequest {
    pub job_id: String,
    pub project_path: String,
    pub mr_iid: u64,
    pub source_branch: String,
    pub comment_id: String,
    pub file_path: String,
    pub original_code: String,
    pub suggestion: String,
    // Rich context for AI agent
    pub comment_body: Option<String>,
    pub comment_title: Option<String>,
    pub severity: Option<String>,
    pub target_branch: Option<String>,
    pub start_line: Option<u32>,
    pub end_line: Option<u32>,
    /// Full file content (fetched server-side from GitLab)
    pub file_content: Option<String>,
    /// MR title
    pub mr_title: Option<String>,
    /// MR description
    pub mr_description: Option<String>,
    /// Diff of the file being fixed (unified diff text)
    pub file_diff: Option<String>,
    /// For fork-based MRs: the source project path to clone from
    /// (differs from project_path when the MR comes from a fork)
    pub source_project_path: Option<String>,
}

/// Result of a fix execution.
#[derive(Debug, Clone)]
pub struct FixResult {
    pub job_id: String,
    pub success: bool,
    pub commit_sha: Option<String>,
    pub test_output: Option<String>,
    pub error: Option<String>,
    /// When fix_branch_mode is "new_branch", the URL of the auto-created MR.
    pub fix_mr_url: Option<String>,
}

impl SandboxManager {
    pub fn new(
        cfg: BottoConfig,
        pool: SqlitePool,
        event_bus: EventBus,
        broadcaster: Arc<dyn Fn(&MrRef, &str) + Send + Sync>,
        warm_pool: Option<Arc<WarmPool>>,
    ) -> Option<Self> {
        Self::with_prompts(cfg, pool, event_bus, broadcaster, SandboxPrompts::default(), false, None, warm_pool)
    }

    /// Create a sandbox manager with custom prompts and optional harness mode.
    /// Used by the harness runner to inject prompt variants and disable push.
    pub fn with_prompts(
        cfg: BottoConfig,
        pool: SqlitePool,
        event_bus: EventBus,
        broadcaster: Arc<dyn Fn(&MrRef, &str) + Send + Sync>,
        prompts: SandboxPrompts,
        harness_mode: bool,
        telemetry: Option<Arc<HarnessTelemetry>>,
        warm_pool: Option<Arc<WarmPool>>,
    ) -> Option<Self> {
        if !cfg.sandbox.enabled {
            return None;
        }

        let docker = match Docker::connect_with_local_defaults() {
            Ok(d) => d,
            Err(e) => {
                warn!("sandbox disabled: failed to connect to Docker: {}", e);
                return None;
            }
        };

        Some(Self {
            semaphore: Semaphore::new(cfg.sandbox.max_concurrent as usize),
            cfg,
            pool,
            docker,
            event_bus,
            broadcaster,
            prompts,
            harness_mode,
            telemetry,
            warm_pool,
        })
    }

    /// Execute a fix in a sandboxed Docker container.
    /// Checks the warm pool first — if a container exists for this MR, reuses it
    /// (git reset + pull instead of clone + deps). Falls back to cold path on miss
    /// or if the warm container is stale.
    pub async fn run_fix(&self, req: FixRequest) -> FixResult {
        // Acquire semaphore permit (blocks if at max concurrency)
        let _permit = match self.semaphore.acquire().await {
            Ok(p) => p,
            Err(_) => {
                return FixResult {
                    job_id: req.job_id,
                    success: false,
                    commit_sha: None,
                    test_output: None,
                    error: Some("sandbox semaphore closed".into()),
                    fix_mr_url: None,
                };
            }
        };

        let mr_ref = MrRef {
            project_path: req.project_path.clone(),
            mr_iid: req.mr_iid,
        };
        let mr_key = mr_ref.key();

        self.send_progress(&req.job_id, &req.comment_id, &mr_ref, "cloning", "preparing sandbox...");
        self.update_job_status(&req.job_id, "cloning").await;

        // Detect base image and strategy
        let gl_cfg = crate::services::gitlab::client::GitLabConfig {
            base_url: self.cfg.gitlab.url.clone(),
            token: self.cfg.gitlab.bot_token.clone(),
        };

        // Resolve project ID from path
        let project_id = match crate::services::gitlab::client::fetch_project(&gl_cfg, &req.project_path).await {
            Ok(p) => p.id,
            Err(e) => {
                return FixResult {
                    job_id: req.job_id,
                    success: false,
                    commit_sha: None,
                    test_output: None,
                    error: Some(format!("failed to resolve project: {}", e)),
                    fix_mr_url: None,
                };
            }
        };

        // Try to fetch .otto.json for user-configured sandbox settings (cached)
        let repo_cfg = crate::services::repo_config::get_or_fetch(
            &self.pool, &gl_cfg, &req.project_path, project_id, &req.source_branch,
        ).await;
        let otto_config = repo_cfg.as_ref().map(|c| c.to_otto_json_value());

        let detection = detector::detect_base_image(
            &gl_cfg,
            project_id,
            &req.source_branch,
            otto_config.as_ref(),
        )
        .await;

        let base_image = detection.image.clone();

        let strategy = detector::determine_strategy(
            &gl_cfg,
            project_id,
            &req.source_branch,
            self.cfg.sandbox.max_memory_mb,
        )
        .await;

        info!(
            "sandbox fix: job={} image={} lang={:?} strategy={:?}",
            req.job_id, base_image, detection.lang, strategy
        );

        // Build the clone URL with embedded token for auth.
        // For fork-based MRs, clone from the fork (where the branch lives).
        let clone_project = req.source_project_path.as_deref().unwrap_or(&req.project_path);
        let clone_url = format!(
            "{}/{}.git",
            self.cfg.gitlab.url, clone_project
        );
        let clone_url_authed = clone_url.replace(
            "://",
            &format!("://botto-bot:{}@", self.cfg.gitlab.bot_token),
        );

        if req.source_project_path.is_some() {
            info!("sandbox fix: fork detected, cloning from {}", clone_project);
        }

        // --- Warm container check ---
        // Try to reuse an existing container for this MR. The warm pool
        // returns the container ID + a per-MR mutex (only one fix at a time).
        // The lock guard must be held for the entire fix duration, not just
        // the reset+pull — otherwise a second fix could start in the same
        // container while the first is still running.
        let mut _warm_lock_guard: Option<tokio::sync::OwnedMutexGuard<()>> = None;
        let warm_hit = if let Some(ref pool) = self.warm_pool {
            if let Some((warm_id, lock)) = pool.get(&mr_key, &base_image) {
                // Acquire per-MR lock — second fix waits for first to finish.
                // Use owned guard so it lives as long as we need it.
                let guard = lock.lock_owned().await;

                self.send_progress(&req.job_id, &req.comment_id, &mr_ref, "cloning", "warm container found, syncing branch...");
                info!("sandbox fix: warm hit for {} (container {})", mr_key, &warm_id[..12]);

                // Reset to clean state, switch to source_branch, and sync to remote HEAD.
                //
                // Why checkout + fetch + reset instead of just pull:
                // 1. A previous fix in new_branch mode leaves the container on a
                //    different branch (git checkout -b botto-fix-...). Without an
                //    explicit checkout, `git pull` merges into the wrong branch and
                //    the pre-validate file check fails ("file not found in cloned repo").
                // 2. The source branch may have been force-pushed (rebased) since the
                //    last fix. `git pull` on a shallow clone can hit merge conflicts,
                //    but `git fetch + reset --hard` always lands on the remote tip.
                let reset_cmd = format!(
                    "cd /workspace && git reset --hard && git clean -fd && git fetch origin {} && git checkout {} && git reset --hard origin/{}",
                    shell_escape(&req.source_branch),
                    shell_escape(&req.source_branch),
                    shell_escape(&req.source_branch),
                );
                let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(60);
                match self.exec_in_container(&warm_id, &reset_cmd, deadline).await {
                    Ok(output) if output.exit_code == 0 => {
                        info!("sandbox fix: warm reset+pull succeeded for {}", mr_key);
                        pool.touch(&mr_key);
                        // Keep the guard alive through the entire fix
                        _warm_lock_guard = Some(guard);
                        Some(warm_id)
                    }
                    Ok(output) => {
                        warn!(
                            "sandbox fix: warm reset+pull failed (exit {}), evicting: {}",
                            output.exit_code,
                            truncate_output(&output.stdout, 200),
                        );
                        drop(guard);
                        pool.remove(&mr_key);
                        None
                    }
                    Err(e) => {
                        warn!("sandbox fix: warm container exec failed, evicting: {}", e);
                        drop(guard);
                        pool.remove(&mr_key);
                        None
                    }
                }
            } else {
                None
            }
        } else {
            None
        };

        // --- Cold path: create new container if no warm hit ---
        let (container_id, is_warm) = if let Some(warm_id) = warm_hit {
            (warm_id, true)
        } else {
            // Create container — use resource hints to size appropriately.
            let container_name = format!("botto-fix-{}", &req.job_id[..8]);
            let hints = &detection.resource_hints;
            let memory_limit = {
                let configured = self.cfg.sandbox.max_memory_mb;
                let recommended = hints.min_memory_mb;
                let effective = configured.max(recommended).min(configured * 2);
                (effective * 1024 * 1024) as i64
            };
            let cpu_quota = {
                let effective_cpus = (hints.min_cpus).min(4).max(1);
                (effective_cpus as i64) * 100_000
            };

            let container_env = build_container_env(&detection.lang);

            let cid = match self
                .create_container(&container_name, &base_image, memory_limit, cpu_quota, &container_env, &detection.lang)
                .await
            {
                Ok(id) => id,
                Err(e) => {
                    return FixResult {
                        job_id: req.job_id,
                        success: false,
                        commit_sha: None,
                        test_output: None,
                        error: Some(format!("failed to create container: {}", e)),
                        fix_mr_url: None,
                    };
                }
            };

            // Start container
            if let Err(e) = self
                .docker
                .start_container(&cid, None::<StartContainerOptions<String>>)
                .await
            {
                self.cleanup_container(&cid).await;
                return FixResult {
                    job_id: req.job_id,
                    success: false,
                    commit_sha: None,
                    test_output: None,
                    error: Some(format!("failed to start container: {}", e)),
                    fix_mr_url: None,
                };
            }

            (cid, false)
        };

        self.update_job_status_with_container(&req.job_id, "cloning", &container_id)
            .await;

        // Execute the fix pipeline inside the container.
        // Warm hits skip the setup phase (prereqs, clone, deps, AI setup).
        let repo_context_text = repo_cfg.as_ref().map(crate::services::repo_config::format_for_prompt);
        let result = self
            .execute_fix_pipeline(&container_id, &req, &clone_url_authed, &strategy, &mr_ref, &detection.lang, is_warm, repo_context_text.as_deref(), &base_image)
            .await;

        // --- Post-fix: warm pool management ---
        // Store in warm pool (or keep warm) instead of destroying the container.
        // Only if warm containers are enabled and this isn't a harness run.
        // On failure, evict the container — it may be in a corrupted state
        // (OOM killed, broken deps, stale files) and the next fix would fail too.
        let should_keep_warm = self.warm_pool.is_some() && !self.harness_mode;

        if should_keep_warm && result.success {
            let pool = self.warm_pool.as_ref().unwrap();

            // Register bot push SHA so webhook doesn't evict us
            if let Some(ref sha) = result.commit_sha {
                pool.register_bot_push(&mr_key, sha);
            }

            if !is_warm {
                // Cold path completed — store container in warm pool for next fix
                pool.insert(
                    mr_key.clone(),
                    container_id.clone(),
                    req.source_branch.clone(),
                    base_image.clone(),
                );
                info!("sandbox fix: stored warm container for {}", mr_key);
            } else {
                // Already warm — just touch to reset idle timer
                pool.touch(&mr_key);
            }
        } else if should_keep_warm && !result.success {
            // Fix failed — evict warm container if it was a warm hit,
            // or destroy the cold container. Don't keep broken state.
            if is_warm {
                let pool = self.warm_pool.as_ref().unwrap();
                pool.remove(&mr_key);
                info!("sandbox fix: evicted failed warm container for {}", mr_key);
            } else {
                self.cleanup_container(&container_id).await;
            }
        } else {
            // Warm containers disabled or harness mode — clean up as before
            self.cleanup_container(&container_id).await;
        }

        // Update DB
        let status = if result.success { "complete" } else { "failed" };
        let _ = db::queries::update_sandbox_job_status(
            &self.pool,
            &req.job_id,
            status,
            Some(&container_id),
            None,
            result.test_output.as_deref(),
            result.commit_sha.as_deref(),
            result.error.as_deref(),
        )
        .await;

        // Publish event
        self.event_bus.publish(Event {
            event_type: EventType::FixComplete,
            project_path: req.project_path.clone(),
            mr_iid: Some(req.mr_iid),
            user_id: None,
            payload: Some(json!({
                "job_id": req.job_id,
                "success": result.success,
                "commit_sha": result.commit_sha,
            })),
        });

        result
    }

    /// The core fix pipeline executed inside the container.
    /// Each step uses AI-assisted retry — if a command fails, the AI diagnoses
    /// the error, suggests a remediation command, and the step is retried.
    /// Test failures get special treatment: the AI rewrites the fix itself.
    ///
    /// When `is_warm` is true, steps 0-2 (prereqs, clone, native deps, AI setup)
    /// are skipped — the container already has the repo cloned and deps installed.
    /// The warm path starts directly at pre-validate → apply → test → push.
    async fn execute_fix_pipeline(
        &self,
        container_id: &str,
        req: &FixRequest,
        clone_url: &str,
        strategy: &FixStrategy,
        mr_ref: &MrRef,
        lang: &crate::services::sandbox::detector::ProjectLang,
        is_warm: bool,
        repo_context: Option<&str>,
        base_image: &str,
    ) -> FixResult {
        // Harness runs get 25 minutes — real projects (Rust, Java) need time
        // for cargo check, mvn compile, etc. Production uses the configured timeout.
        let timeout_secs = if self.harness_mode {
            1500 // 25 minutes
        } else {
            self.cfg.sandbox.timeout_seconds
        };
        let timeout = std::time::Duration::from_secs(timeout_secs);
        let deadline = tokio::time::Instant::now() + timeout;

        // Build output context for live streaming to Otto.
        // All exec_in_container_streaming calls use this to broadcast lines.
        let output_ctx = OutputContext {
            job_id: req.job_id.clone(),
            comment_id: req.comment_id.clone(),
            mr_ref: mr_ref.clone(),
        };

        // Steps 0-2 are the cold path: prereqs, clone, native deps, AI setup.
        // Warm containers already have all of this — skip straight to pre-validate.
        if !is_warm {

        // Step 0: Ensure git, python3, and CA certificates are available.
        // Slim/alpine base images don't ship git or CA certs, and python3 is
        // needed for the apply step. Detect the package manager and install
        // only what's missing. This is a no-op if everything is already present.
        let prereq_cmd = concat!(
            "( command -v git >/dev/null 2>&1 && command -v python3 >/dev/null 2>&1 && [ -f /etc/ssl/certs/ca-certificates.crt ] ) || ",
            "( command -v apk >/dev/null 2>&1 && apk add --no-cache git python3 ca-certificates ) || ",
            "( apt-get update -qq && apt-get install -y -qq --no-install-recommends git python3 ca-certificates && rm -rf /var/lib/apt/lists/* )",
        );
        match self.exec_in_container(container_id, prereq_cmd, deadline).await {
            Ok(output) if output.exit_code == 0 => {
                debug!("sandbox prereqs installed (or already present)");
            }
            Ok(output) => {
                // Non-fatal — the AI retry on clone/apply will handle missing tools
                warn!("prereq install returned exit {}: {}", output.exit_code, output.stdout);
            }
            Err(e) => {
                warn!("prereq exec failed (non-fatal): {}", e);
            }
        }

        // Step 1: Clone (with AI retry)
        self.send_progress(&req.job_id, &req.comment_id, mr_ref, "cloning", "cloning repository...");
        let clone_cmd = format!(
            "git clone --depth=1 --branch {} {} /workspace",
            shell_escape(&req.source_branch),
            shell_escape(clone_url),
        );
        if let Err(e) = self.exec_with_ai_retry(
            container_id, &clone_cmd, deadline,
            "git clone", req, mr_ref, "cloning",
        ).await {
            return FixResult {
                job_id: req.job_id.clone(),
                success: false,
                commit_sha: None,
                test_output: e.output,
                error: Some(e.error),
                fix_mr_url: None,
            };
        }

        // Step 1.5: Pre-install native dependencies for known problematic packages.
        // After clone, scan lockfiles for gems/packages that need system libraries.
        // This saves 10-20 AI setup steps per container for projects using packages
        // like rugged, nokogiri, pg, etc. that require native compilation.
        // Best-effort: if this fails, the AI setup loop handles it.
        {
            let native_deps_cmd = build_native_deps_command(lang);
            if let Some(cmd) = native_deps_cmd {
                debug!("pre-installing native deps for {:?} project", lang);
                match self.exec_in_container(container_id, &cmd, deadline).await {
                    Ok(output) if output.exit_code == 0 => {
                        debug!("native deps pre-installed successfully");
                    }
                    Ok(output) => {
                        debug!("native deps pre-install partial (exit {}), AI will handle remainder", output.exit_code);
                    }
                    Err(e) => {
                        debug!("native deps pre-install failed (non-fatal): {}", e);
                    }
                }
            }
        }

        // Step 1.75: Try cached setup recipe.
        // If a previous successful setup for this project+image was cached,
        // replay those commands instead of running the AI setup loop. This
        // saves 5-15 AI round-trips and 30-120 seconds per cold container.
        // On any failure, the recipe is deleted and we fall through to the
        // full AI setup.
        let mut recipe_hit = false;
        if self.cfg.sandbox.recipe_cache {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64;

            if let Ok(Some((commands_json, _steps, created_at, use_count))) =
                crate::db::queries::get_setup_recipe(&self.pool, &req.project_path, &base_image).await
            {
                let age_secs = now - created_at;
                if age_secs < self.cfg.sandbox.recipe_cache_ttl_secs as i64 {
                    if let Ok(commands) = serde_json::from_str::<Vec<String>>(&commands_json) {
                        if !commands.is_empty() {
                            info!(
                                "sandbox fix: replaying cached recipe for {} ({} commands, used {} times, age {}s)",
                                req.project_path, commands.len(), use_count, age_secs
                            );
                            self.send_progress(
                                &req.job_id, &req.comment_id, mr_ref, "setting_up",
                                "replaying cached setup recipe...",
                            );
                            self.update_job_status(&req.job_id, "setting_up").await;

                            // Give replay a shorter deadline — if the cached recipe
                            // takes longer than 5 minutes, something changed and we
                            // should fall back to the AI.
                            let replay_deadline = tokio::time::Instant::now()
                                + std::time::Duration::from_secs(300);
                            let replay_start = std::time::Instant::now();
                            let mut replay_ok = true;
                            let mut steps_completed = 0u32;

                            for (i, cmd) in commands.iter().enumerate() {
                                if tokio::time::Instant::now() >= replay_deadline {
                                    warn!("sandbox fix: recipe replay timed out at step {}/{}", i + 1, commands.len());
                                    replay_ok = false;
                                    break;
                                }

                                let detail = format!(
                                    "recipe step {}/{}: {}",
                                    i + 1, commands.len(), truncate_output(cmd, 100),
                                );
                                self.send_progress(
                                    &req.job_id, &req.comment_id, mr_ref, "setting_up", &detail,
                                );

                                match self.exec_in_container_streaming(container_id, cmd, replay_deadline, &output_ctx).await {
                                    Ok(output) if output.exit_code == 0 => {
                                        debug!("recipe step {}/{} succeeded", i + 1, commands.len());
                                        steps_completed += 1;
                                    }
                                    Ok(output) => {
                                        warn!(
                                            "sandbox fix: recipe step {}/{} failed (exit {}): {}",
                                            i + 1, commands.len(), output.exit_code,
                                            truncate_output(&output.stdout, 200),
                                        );
                                        replay_ok = false;
                                        break;
                                    }
                                    Err(e) => {
                                        warn!("sandbox fix: recipe step {}/{} exec error: {}", i + 1, commands.len(), e);
                                        replay_ok = false;
                                        break;
                                    }
                                }
                            }

                            let replay_elapsed = replay_start.elapsed();

                            if replay_ok {
                                info!(
                                    "sandbox fix: recipe replay succeeded for {} — {} steps in {:.1}s (cache hit, use_count={})",
                                    req.project_path, steps_completed, replay_elapsed.as_secs_f64(), use_count + 1,
                                );
                                recipe_hit = true;
                                // Bump usage stats
                                let _ = crate::db::queries::touch_setup_recipe(
                                    &self.pool, &req.project_path, &base_image,
                                ).await;
                            } else {
                                info!(
                                    "sandbox fix: recipe replay failed for {} — {}/{} steps in {:.1}s (cache miss, invalidating)",
                                    req.project_path, steps_completed, commands.len(), replay_elapsed.as_secs_f64(),
                                );
                                let _ = crate::db::queries::delete_setup_recipe(
                                    &self.pool, &req.project_path, &base_image,
                                ).await;
                            }
                        }
                    }
                } else {
                    debug!("sandbox fix: recipe for {} expired (age {}s > ttl {}s), skipping",
                        req.project_path, age_secs, self.cfg.sandbox.recipe_cache_ttl_secs);
                }
            }
        }

        // Step 2: AI-driven project setup.
        // The AI reads the project, understands it, installs deps, and gets
        // the environment ready to run tests. No hardcoded commands — the AI
        // figures out what the project needs.
        // Skipped entirely if a cached recipe was replayed successfully above.
        if !recipe_hit {
        self.send_progress(&req.job_id, &req.comment_id, mr_ref, "setting_up", "AI analyzing project...");
        self.update_job_status(&req.job_id, "setting_up").await;

        {
            let test_cmd_preview = detect_test_command(container_id, self, &req.file_path, strategy).await;

            let setup_system = ChatMessage {
                role: "system".into(),
                content: Some(
                    self.prompts.setup_system
                        .replace("{project}", &req.project_path)
                        .replace("{file_path}", &req.file_path)
                        .replace("{test_cmd}", &test_cmd_preview)
                        .replace("{repo_context}", &format_repo_context_block(repo_context)),
                ),
                tool_calls: None,
                tool_call_id: None,
            };

            let mut setup_messages = vec![
                setup_system,
                ChatMessage {
                    role: "user".into(),
                    content: Some(
                        "The repo is cloned at /workspace. Please analyze the project and set up the environment. \
                         Start by examining the project structure.".to_string()
                    ),
                    tool_calls: None,
                    tool_call_id: None,
                },
            ];

            let mut setup_step = 0u32;
            let mut setup_done = false;
            // Collect commands that succeeded — these form the recipe if setup completes.
            // Only commands with exit_code == 0 are included; failed commands are the AI
            // exploring dead ends and shouldn't be replayed.
            let mut recipe_commands: Vec<String> = Vec::new();

            loop {
                if tokio::time::Instant::now() >= deadline {
                    info!("sandbox timeout reached during AI setup");
                    break;
                }

                setup_step += 1;
                if let Some(ref t) = self.telemetry {
                    t.setup_steps.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                // Initial progress before AI responds (we don't know the command yet)
                self.send_progress(&req.job_id, &req.comment_id, mr_ref, "setting_up",
                    &format!("AI analyzing (step {})...", setup_step));

                let ai_request = ChatCompletionRequest {
                    model: self.cfg.ai.models.fix.clone(),
                    messages: setup_messages.clone(),
                    temperature: Some(self.prompts.code_params.setup.temperature),
                    max_tokens: Some(self.prompts.code_params.setup.max_tokens),
                    stream: None,
                    tools: None,
                    tool_choice: None,
                };

                let ai_response = match ai_client::chat_completion(&self.ai_config(), ai_request).await {
                    Ok(resp) => resp,
                    Err(e) => {
                        warn!("AI call failed during setup: {}", e);
                        break;
                    }
                };

                let ai_text = ai_response.choices.first()
                    .and_then(|c| c.message.content.as_ref())
                    .map(|s| s.trim().to_string())
                    .unwrap_or_default();

                if ai_text.is_empty() {
                    warn!("AI returned empty response during setup at step {}", setup_step);
                    break;
                }

                let cmd = strip_markdown_fences(&ai_text);

                if cmd == "UNFIXABLE" {
                    info!("AI determined project setup is unfixable");
                    return FixResult {
                        job_id: req.job_id.clone(),
                        success: false,
                        commit_sha: None,
                        test_output: None,
                        error: Some("AI could not set up the project environment".into()),
                        fix_mr_url: None,
                    };
                }

                if cmd == "SETUP_DONE" {
                    info!("AI completed project setup after {} steps", setup_step);
                    setup_done = true;
                    break;
                }

                setup_messages.push(ChatMessage {
                    role: "assistant".into(),
                    content: Some(cmd.clone()),
                    tool_calls: None,
                    tool_call_id: None,
                });

                info!("AI setup step {}: {}", setup_step, truncate_output(&cmd, 200));

                // Send richer progress with command preview
                let cmd_preview = truncate_output(&cmd, 120);
                let detail = format!("setup step {}: {}", setup_step, cmd_preview);
                self.send_progress(&req.job_id, &req.comment_id, mr_ref, "setting_up", &detail);

                let (cmd_exit, cmd_output) = match self.exec_in_container_streaming(container_id, &cmd, deadline, &output_ctx).await {
                    Ok(o) => (o.exit_code, o.stdout),
                    Err(e) => (-1, format!("exec error: {}", e)),
                };

                // Capture successful commands for the recipe cache.
                // Only exit_code == 0 commands are worth replaying — failed ones
                // are the AI probing or hitting errors it then recovers from.
                // Skip read-only exploratory commands (ls, cat, head, etc.) that
                // the AI uses to understand the project but don't set anything up.
                if cmd_exit == 0 && !is_exploratory_command(&cmd) {
                    recipe_commands.push(cmd.clone());
                }

                setup_messages.push(ChatMessage {
                    role: "user".into(),
                    content: Some(format!(
                        "Exit code: {}\nOutput:\n```\n{}\n```\n\nWhat next?",
                        cmd_exit, truncate_output(&cmd_output, 3000),
                    )),
                    tool_calls: None,
                    tool_call_id: None,
                });

                // Trim history if too long
                if setup_messages.len() > self.prompts.code_params.history_trim_threshold as usize {
                    let system = setup_messages[0].clone();
                    let keep = self.prompts.code_params.history_keep_count as usize;
                    let recent: Vec<_> = setup_messages[setup_messages.len() - keep..].to_vec();
                    setup_messages = std::iter::once(system).chain(recent).collect();
                }
            }

            // Cache the recipe if setup completed successfully (SETUP_DONE).
            // Don't cache on timeout, UNFIXABLE, or empty response — those
            // indicate the setup didn't fully succeed and replaying partial
            // commands could leave the environment in a broken state.
            if setup_done && !recipe_commands.is_empty() && self.cfg.sandbox.recipe_cache {
                info!(
                    "sandbox fix: caching setup recipe for {} ({} commands from {} steps)",
                    req.project_path, recipe_commands.len(), setup_step,
                );
                let _ = crate::db::queries::upsert_setup_recipe(
                    &self.pool, &req.project_path, &base_image, &recipe_commands, setup_step,
                ).await;
            }
        }
        } // end if !recipe_hit

        } // end if !is_warm (cold path: steps 0-2)

        // Step 3: Pre-validate — verify the target file exists and contains
        // the original code BEFORE attempting the apply. This catches the common
        // failure mode where the MR's source branch was rebased, the file was
        // renamed, or the review snippet doesn't match the actual file content.
        // Without this check, the apply step fails and the AI burns 12+ retry
        // steps trying to find a file that simply isn't there.
        {
            let check_cmd = format!(
                "test -f /workspace/{} && echo FILE_EXISTS || echo FILE_MISSING",
                shell_escape(&req.file_path),
            );
            match self.exec_in_container(container_id, &check_cmd, deadline).await {
                Ok(output) if output.stdout.contains("FILE_EXISTS") => {
                    debug!("pre-validate: file exists at /workspace/{}", req.file_path);
                }
                _ => {
                    warn!(
                        "pre-validate: file not found at /workspace/{} — skipping fix",
                        req.file_path
                    );
                    return FixResult {
                        job_id: req.job_id.clone(),
                        success: false,
                        commit_sha: None,
                        test_output: None,
                        error: Some(format!(
                            "pre-validate failed: file '{}' not found in cloned repo (branch may have been rebased or file renamed)",
                            req.file_path
                        )),
                        fix_mr_url: None,
                    };
                }
            }

            // Verify the original code snippet exists in the file
            use base64::Engine;
            let b64 = base64::engine::general_purpose::STANDARD;
            let original_b64 = b64.encode(req.original_code.as_bytes());
            let file_path_b64 = b64.encode(req.file_path.as_bytes());
            let verify_cmd = format!(
                r#"cd /workspace && python3 -c "
import base64, sys
path = base64.b64decode('{}').decode()
original = base64.b64decode('{}').decode()
with open(path, 'r') as f:
    content = f.read()
if original in content:
    print('SNIPPET_FOUND')
else:
    print('SNIPPET_MISSING')
    # Show first 200 chars of file for debugging
    print('File starts with:', repr(content[:200]))
""#,
                file_path_b64, original_b64,
            );
            match self.exec_in_container(container_id, &verify_cmd, deadline).await {
                Ok(output) if output.stdout.contains("SNIPPET_FOUND") => {
                    debug!("pre-validate: original code snippet found in {}", req.file_path);
                }
                Ok(output) if output.stdout.contains("SNIPPET_MISSING") => {
                    warn!(
                        "pre-validate: original code not found in {} — snippet may be stale",
                        req.file_path
                    );
                    return FixResult {
                        job_id: req.job_id.clone(),
                        success: false,
                        commit_sha: None,
                        test_output: Some(output.stdout),
                        error: Some(format!(
                            "pre-validate failed: original code snippet not found in '{}' (source branch may have been updated since review)",
                            req.file_path
                        )),
                        fix_mr_url: None,
                    };
                }
                _ => {
                    // python3 not available or other error — proceed anyway,
                    // the apply step will catch it
                    debug!("pre-validate: snippet check inconclusive, proceeding");
                }
            }
        }

        // Step 4: Apply fix (with AI retry)
        self.send_progress(&req.job_id, &req.comment_id, mr_ref, "running", "applying fix...");
        self.update_job_status(&req.job_id, "running").await;

        let apply_cmd = build_apply_command(&req.file_path, &req.original_code, &req.suggestion);
        if let Err(e) = self.exec_with_ai_retry(
            container_id, &apply_cmd, deadline,
            "apply fix", req, mr_ref, "running",
        ).await {
            return FixResult {
                job_id: req.job_id.clone(),
                success: false,
                commit_sha: None,
                test_output: e.output,
                error: Some(e.error),
                fix_mr_url: None,
            };
        }

        // Step 4: Run tests — with AI-powered autonomous agent loop.
        // The AI has full shell access and decides when to run tests.
        // No iteration cap — only the pipeline timeout (30 min default).
        self.send_progress(&req.job_id, &req.comment_id, mr_ref, "testing", "running tests...");
        self.update_job_status(&req.job_id, "testing").await;

        let test_cmd = detect_test_command(container_id, self, &req.file_path, strategy).await;
        let current_suggestion = req.suggestion.clone();
        let mut test_passed = false;
        let mut test_output;

        // First test run
        match self.exec_in_container_streaming(container_id, &test_cmd, deadline, &output_ctx).await {
            Ok(output) if output.exit_code == 0 => {
                test_passed = true;
                test_output = output.stdout;
            }
            Ok(output) => {
                test_output = output.stdout;
            }
            Err(e) => {
                test_output = format!("test exec failed: {}", e);
            }
        }

        // If tests failed, hand control to the AI agent
        if !test_passed {
            // Build rich context for the system message
            let mut context_sections = Vec::new();

            if let Some(title) = &req.mr_title {
                context_sections.push(format!("## Merge Request\nTitle: {}", title));
                if let Some(desc) = &req.mr_description {
                    context_sections.push(format!("Description:\n{}", truncate_output(desc, 500)));
                }
                context_sections.push(format!(
                    "Branch: {} → {}",
                    req.source_branch,
                    req.target_branch.as_deref().unwrap_or("unknown")
                ));
            }

            if let Some(title) = &req.comment_title {
                context_sections.push(format!(
                    "## Review Comment\n[{}] {}",
                    req.severity.as_deref().unwrap_or("suggestion"),
                    title
                ));
            }
            if let Some(body) = &req.comment_body {
                context_sections.push(truncate_output(body, 1000).to_string());
            }

            context_sections.push(format!("## File: {}", req.file_path));
            if let (Some(start), Some(end)) = (req.start_line, req.end_line) {
                context_sections.push(format!("Lines: {}-{}", start, end));
            }
            if let Some(content) = &req.file_content {
                context_sections.push(format!(
                    "### Full file content\n```\n{}\n```",
                    truncate_output(content, 6000)
                ));
            }
            if let Some(diff) = &req.file_diff {
                context_sections.push(format!(
                    "### File diff in this MR\n```diff\n{}\n```",
                    truncate_output(diff, 3000)
                ));
            }

            let system_msg = ChatMessage {
                role: "system".into(),
                content: Some(
                    self.prompts.fix_system
                        .replace("{context}", &context_sections.join("\n\n"))
                        .replace("{original}", &req.original_code)
                        .replace("{suggestion}", &current_suggestion)
                        .replace("{test_cmd}", &test_cmd)
                        .replace("{repo_context}", &format_repo_context_block(repo_context)),
                ),
                tool_calls: None,
                tool_call_id: None,
            };

            let mut messages = vec![
                system_msg,
                ChatMessage {
                    role: "user".into(),
                    content: Some(format!(
                        "Tests failed on the first run after applying the fix.\n\nTest command: `{}`\nOutput:\n```\n{}\n```\n\nWhat do you want to do?",
                        test_cmd,
                        truncate_output(&test_output, 3000),
                    )),
                    tool_calls: None,
                    tool_call_id: None,
                },
            ];

            let mut step_count = 0u32;

            loop {
                // Check timeout
                if tokio::time::Instant::now() >= deadline {
                    info!("sandbox timeout reached during AI test fix loop");
                    break;
                }

                step_count += 1;
                if let Some(ref t) = self.telemetry {
                    t.fix_steps.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                self.send_progress(&req.job_id, &req.comment_id, mr_ref, "testing",
                    &format!("AI analyzing (step {})...", step_count));

                // Ask AI
                let ai_request = ChatCompletionRequest {
                    model: self.cfg.ai.models.fix.clone(),
                    messages: messages.clone(),
                    temperature: Some(self.prompts.code_params.fix.temperature),
                    max_tokens: Some(self.prompts.code_params.fix.max_tokens),
                    stream: None,
                    tools: None,
                    tool_choice: None,
                };

                let ai_response = match ai_client::chat_completion(&self.ai_config(), ai_request).await {
                    Ok(resp) => resp,
                    Err(e) => {
                        warn!("AI call failed during test fix: {}", e);
                        break;
                    }
                };

                let ai_text = ai_response.choices.first()
                    .and_then(|c| c.message.content.as_ref())
                    .map(|s| s.trim().to_string())
                    .unwrap_or_default();

                if ai_text.is_empty() {
                    warn!("AI returned empty response at step {}", step_count);
                    break;
                }

                if ai_text == "UNFIXABLE" {
                    info!("AI determined test failure is unfixable at step {}", step_count);
                    break;
                }

                let cmd = strip_markdown_fences(&ai_text);

                // Add AI's response to history
                messages.push(ChatMessage {
                    role: "assistant".into(),
                    content: Some(cmd.clone()),
                    tool_calls: None,
                    tool_call_id: None,
                });

                if cmd == "RUN_TESTS" {
                    // AI wants to run the test suite
                    info!("AI requested test run at step {}", step_count);
                    let detail = format!("running tests (AI step {})...", step_count);
                    self.send_progress(&req.job_id, &req.comment_id, mr_ref, "testing", &detail);

                    match self.exec_in_container_streaming(container_id, &test_cmd, deadline, &output_ctx).await {
                        Ok(output) if output.exit_code == 0 => {
                            info!("tests passed after AI step {}", step_count);
                            test_passed = true;
                            test_output = output.stdout;
                            break;
                        }
                        Ok(output) => {
                            test_output = output.stdout;
                            messages.push(ChatMessage {
                                role: "user".into(),
                                content: Some(format!(
                                    "Tests still failing.\nOutput:\n```\n{}\n```\n\nWhat do you want to do next?",
                                    truncate_output(&test_output, 3000),
                                )),
                                tool_calls: None,
                                tool_call_id: None,
                            });
                        }
                        Err(e) => {
                            test_output = format!("test exec failed: {}", e);
                            messages.push(ChatMessage {
                                role: "user".into(),
                                content: Some(format!(
                                    "Test execution error: {}\n\nWhat do you want to do next?", e
                                )),
                                tool_calls: None,
                                tool_call_id: None,
                            });
                        }
                    }
                } else {
                    // AI wants to run a shell command
                    info!("AI step {}: {}", step_count, truncate_output(&cmd, 200));
                    let cmd_preview = truncate_output(&cmd, 120);
                    let detail = format!("fix step {}: {}", step_count, cmd_preview);
                    self.send_progress(&req.job_id, &req.comment_id, mr_ref, "testing", &detail);

                    let (cmd_exit, cmd_output) = match self.exec_in_container_streaming(container_id, &cmd, deadline, &output_ctx).await {
                        Ok(o) => (o.exit_code, o.stdout),
                        Err(e) => (-1, format!("exec error: {}", e)),
                    };

                    // Feed result back to AI
                    messages.push(ChatMessage {
                        role: "user".into(),
                        content: Some(format!(
                            "Command exit code: {}\nOutput:\n```\n{}\n```\n\nWhat do you want to do next?",
                            cmd_exit, truncate_output(&cmd_output, 3000),
                        )),
                        tool_calls: None,
                        tool_call_id: None,
                    });
                }

                // Trim conversation history if it gets too long
                if messages.len() > self.prompts.code_params.history_trim_threshold as usize {
                    let system = messages[0].clone();
                    let keep = self.prompts.code_params.history_keep_count as usize;
                    let recent: Vec<_> = messages[messages.len() - keep..].to_vec();
                    messages = std::iter::once(system).chain(recent).collect();
                }
            }
        }

        if !test_passed {
            return FixResult {
                job_id: req.job_id.clone(),
                success: false,
                commit_sha: None,
                test_output: Some(test_output),
                error: Some("tests failed after applying fix".into()),
                fix_mr_url: None,
            };
        }

        // Step 5: Commit + push
        // In harness mode, skip push entirely — we just care about test results.
        // Try git push first. If it fails (e.g., fork-based MR where bot can't
        // push), fall back to the GitLab Commits API.
        if self.harness_mode {
            info!("harness mode: skipping git push");
            return FixResult {
                job_id: req.job_id.clone(),
                success: true,
                commit_sha: None,
                test_output: Some(test_output),
                error: None,
                fix_mr_url: None,
            };
        }

        self.send_progress(&req.job_id, &req.comment_id, mr_ref, "pushing", "committing and pushing...");
        self.update_job_status(&req.job_id, "pushing").await;

        let commit_msg = format!(
            "fix: applied suggestion from review comment {}\n\nApplied by Botto sandbox",
            req.comment_id
        );

        // Determine the target branch to push to based on fix_branch_mode.
        let use_new_branch = self.cfg.sandbox.fix_branch_mode == crate::config::FixBranchMode::NewBranch;
        let push_branch = if use_new_branch {
            generate_fix_branch_name(
                req.mr_iid,
                req.mr_title.as_deref(),
                &req.file_path,
                &req.comment_id,
            )
        } else {
            req.source_branch.clone()
        };

        // In new_branch mode, create the branch from source_branch before pushing
        let git_cmds = if use_new_branch {
            format!(
                "cd /workspace && git config user.name 'Botto' && git config user.email 'botto@bot' && git checkout -b {} && git add -A && git commit -m {} && git push origin {}",
                shell_escape(&push_branch),
                shell_escape(&commit_msg),
                shell_escape(&push_branch),
            )
        } else {
            format!(
                "cd /workspace && git config user.name 'Botto' && git config user.email 'botto@bot' && git add -A && git commit -m {} && git push origin {}",
                shell_escape(&commit_msg),
                shell_escape(&req.source_branch),
            )
        };

        let push_result = self.exec_in_container(container_id, &git_cmds, deadline).await;
        let mut commit_sha = None;
        let mut fix_mr_url = None;

        match push_result {
            Ok(output) if output.exit_code == 0 => {
                // Git push succeeded — extract SHA
                let sha_cmd = "cd /workspace && git rev-parse HEAD";
                commit_sha = match self.exec_in_container(container_id, sha_cmd, deadline).await {
                    Ok(o) => Some(o.stdout.trim().to_string()),
                    Err(_) => None,
                };

                // In new_branch mode, create an MR targeting the original source branch
                if use_new_branch {
                    fix_mr_url = self.create_fix_mr(
                        &req, &push_branch, &commit_msg,
                    ).await;
                }
            }
            _ => {
                // Git push failed — try GitLab Commits API fallback
                info!("git push failed, attempting GitLab API commit fallback");
                self.send_progress(&req.job_id, &req.comment_id, mr_ref, "pushing", "push failed, trying API fallback...");

                // Read the modified file content from the container
                let read_cmd = format!("cat /workspace/{}", shell_escape(&req.file_path));
                let file_content = match self.exec_in_container(container_id, &read_cmd, deadline).await {
                    Ok(o) if o.exit_code == 0 => Some(o.stdout),
                    _ => None,
                };

                if let Some(content) = file_content {
                    let gl_cfg = crate::services::gitlab::client::GitLabConfig {
                        base_url: self.cfg.gitlab.url.clone(),
                        token: self.cfg.gitlab.bot_token.clone(),
                    };

                    // Try pushing to the source project (fork) first, then upstream
                    let target_project_ids: Vec<i64> = {
                        let mut ids = Vec::new();
                        // If it's a fork, try the fork's project first
                        if let Some(ref src_path) = req.source_project_path {
                            if let Ok(p) = crate::services::gitlab::client::fetch_project(&gl_cfg, src_path).await {
                                ids.push(p.id);
                            }
                        }
                        // Also try the upstream project
                        if let Ok(p) = crate::services::gitlab::client::fetch_project(&gl_cfg, &req.project_path).await {
                            ids.push(p.id);
                        }
                        ids
                    };

                    let action = crate::services::gitlab::client::CommitAction {
                        action: "update".to_string(),
                        file_path: req.file_path.clone(),
                        content: Some(content),
                    };

                    // For new_branch mode via API, we need start_branch to create the branch.
                    // The create_commit API supports a `start_branch` param to branch from.
                    let api_branch = if use_new_branch { &push_branch } else { &req.source_branch };

                    let mut api_success = false;
                    for pid in &target_project_ids {
                        // Build the commit body — include start_branch for new_branch mode
                        let body = if use_new_branch {
                            serde_json::json!({
                                "branch": api_branch,
                                "start_branch": req.source_branch,
                                "commit_message": commit_msg,
                                "actions": [action.clone()],
                            })
                        } else {
                            serde_json::json!({
                                "branch": api_branch,
                                "commit_message": commit_msg,
                                "actions": [action.clone()],
                            })
                        };

                        let url = format!(
                            "{}/api/v4/projects/{}/repository/commits",
                            gl_cfg.base_url, pid
                        );
                        let client = reqwest::Client::builder()
                            .timeout(std::time::Duration::from_secs(30))
                            .build()
                            .expect("failed to build HTTP client");

                        let mut headers = reqwest::header::HeaderMap::new();
                        headers.insert(
                            "PRIVATE-TOKEN",
                            reqwest::header::HeaderValue::from_str(&gl_cfg.token)
                                .unwrap_or_else(|_| reqwest::header::HeaderValue::from_static("")),
                        );

                        match client.post(&url).headers(headers).json(&body).send().await {
                            Ok(resp) if resp.status().is_success() => {
                                if let Ok(cr) = resp.json::<crate::services::gitlab::client::CommitResponse>().await {
                                    info!("GitLab API commit succeeded: {} on project {}", cr.id, pid);
                                    commit_sha = Some(cr.id);
                                    api_success = true;

                                    // Create MR in new_branch mode
                                    if use_new_branch {
                                        fix_mr_url = self.create_fix_mr(
                                            &req, &push_branch, &commit_msg,
                                        ).await;
                                    }
                                    break;
                                }
                            }
                            Ok(resp) => {
                                warn!("GitLab API commit failed on project {}: status {}", pid, resp.status());
                            }
                            Err(e) => {
                                warn!("GitLab API commit failed on project {}: {}", pid, e);
                            }
                        }
                    }

                    if !api_success {
                        return FixResult {
                            job_id: req.job_id.clone(),
                            success: false,
                            commit_sha: None,
                            test_output: Some(test_output),
                            error: Some("push failed: git push and API commit both failed (bot may lack write access to fork)".into()),
                            fix_mr_url: None,
                        };
                    }
                } else {
                    return FixResult {
                        job_id: req.job_id.clone(),
                        success: false,
                        commit_sha: None,
                        test_output: Some(test_output),
                        error: Some("push failed: could not read modified file from container".into()),
                        fix_mr_url: None,
                    };
                }
            }
        }

        FixResult {
            job_id: req.job_id.clone(),
            success: true,
            commit_sha,
            test_output: Some(test_output),
            error: None,
            fix_mr_url,
        }
    }

    // -----------------------------------------------------------------------
    // New-branch MR creation
    // -----------------------------------------------------------------------

    /// Create a merge request from the fix branch back to the original source branch.
    /// Returns the MR web URL on success, None on failure (logged but non-fatal).
    async fn create_fix_mr(
        &self,
        req: &FixRequest,
        fix_branch: &str,
        commit_msg: &str,
    ) -> Option<String> {
        let gl_cfg = crate::services::gitlab::client::GitLabConfig {
            base_url: self.cfg.gitlab.url.clone(),
            token: self.cfg.gitlab.bot_token.clone(),
        };

        // Resolve the project ID to push the MR to.
        // For fork-based MRs, create the MR on the fork (source project).
        let project_path = req.source_project_path.as_deref().unwrap_or(&req.project_path);
        let project_id = match crate::services::gitlab::client::fetch_project(&gl_cfg, project_path).await {
            Ok(p) => p.id,
            Err(e) => {
                warn!("failed to resolve project for fix MR: {}", e);
                return None;
            }
        };

        let title = format!(
            "fix: {} (Botto sandbox fix for !{})",
            req.comment_title.as_deref().unwrap_or(&req.file_path),
            req.mr_iid,
        );

        let description = format!(
            "Automated fix applied by Botto's sandbox.\n\n\
             **Source MR:** !{}\n\
             **File:** `{}`\n\
             **Commit message:** {}\n\n\
             This branch was auto-created because `fix_branch_mode = \"new_branch\"` is configured.\n\
             Merge this into `{}` to apply the fix.",
            req.mr_iid,
            req.file_path,
            commit_msg,
            req.source_branch,
        );

        match crate::services::gitlab::client::create_merge_request(
            &gl_cfg,
            project_id,
            fix_branch,
            &req.source_branch,
            &title,
            &description,
        ).await {
            Ok(mr) => {
                info!("created fix MR: {} ({})", mr.iid, mr.web_url);
                Some(mr.web_url)
            }
            Err(e) => {
                warn!("failed to create fix MR: {}", e);
                None
            }
        }
    }

    // -----------------------------------------------------------------------
    // AI self-healing helpers
    // -----------------------------------------------------------------------

    /// Build an AI client config from the server config.
    fn ai_config(&self) -> AiClientConfig {
        AiClientConfig {
            base_url: self.cfg.ai.base_url.clone(),
            api_key: self.cfg.ai.api_key.clone(),
        }
    }

    /// Execute a command with AI-assisted iteration on failure.
    /// Maintains a conversation history so the AI learns from each attempt.
    /// On failure: sends the error to the AI → AI suggests a fix → runs it → retries.
    /// The AI sees the full history of what was tried and what happened.
    async fn exec_with_ai_retry(
        &self,
        container_id: &str,
        cmd: &str,
        deadline: tokio::time::Instant,
        step_name: &str,
        req: &FixRequest,
        mr_ref: &MrRef,
        pipeline_status: &str,
    ) -> Result<ExecOutput, StepError> {
        // First attempt — no AI needed yet
        match self.exec_in_container(container_id, cmd, deadline).await {
            Ok(output) if output.exit_code == 0 => return Ok(output),
            Ok(output) => {
                // Failed — start the AI conversation loop
                let mut last_output = output.stdout;
                let mut last_exit_code = output.exit_code;

                // Build initial context for the AI
                let mut context_parts = Vec::new();
                context_parts.push(format!("Pipeline step: {}", step_name));
                context_parts.push(format!("Project: {}", req.project_path));
                context_parts.push(format!("Branch: {} → {}", req.source_branch, req.target_branch.as_deref().unwrap_or("unknown")));
                if let Some(title) = &req.mr_title {
                    context_parts.push(format!("MR: {}", title));
                }
                if let Some(title) = &req.comment_title {
                    context_parts.push(format!("Review comment: [{}] {}", req.severity.as_deref().unwrap_or("info"), title));
                }

                let system_msg = ChatMessage {
                    role: "system".into(),
                    content: Some(
                        self.prompts.retry_system
                            .replace("{context}", &context_parts.join("\n")),
                    ),
                    tool_calls: None,
                    tool_call_id: None,
                };

                // Start conversation with the first failure
                let mut messages = vec![
                    system_msg,
                    ChatMessage {
                        role: "user".into(),
                        content: Some(format!(
                            "The following command failed:\n```\n{}\n```\n\nExit code: {}\nOutput:\n```\n{}\n```\n\nWhat single command should I run to fix this?",
                            cmd, last_exit_code, truncate_output(&last_output, 2000),
                        )),
                        tool_calls: None,
                        tool_call_id: None,
                    },
                ];

                let mut step_count = 0u32;

                loop {
                    // Check timeout
                    if tokio::time::Instant::now() >= deadline {
                        info!("sandbox timeout reached during AI env fix for '{}'", step_name);
                        break;
                    }

                    step_count += 1;
                    if let Some(ref t) = self.telemetry {
                        t.retry_steps.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    let detail = format!(
                        "AI fixing {} (step {})...",
                        step_name, step_count
                    );
                    self.send_progress(&req.job_id, &req.comment_id, mr_ref, pipeline_status, &detail);

                    info!(
                        "sandbox step '{}' failed (exit {}), AI step {}",
                        step_name, last_exit_code, step_count
                    );

                    // Ask AI
                    let ai_request = ChatCompletionRequest {
                        model: self.cfg.ai.models.fix.clone(),
                        messages: messages.clone(),
                        temperature: Some(self.prompts.code_params.retry.temperature),
                        max_tokens: Some(self.prompts.code_params.retry.max_tokens),
                        stream: None,
                        tools: None,
                        tool_choice: None,
                    };

                    let ai_response = match ai_client::chat_completion(&self.ai_config(), ai_request).await {
                        Ok(resp) => resp,
                        Err(e) => {
                            warn!("AI call failed for '{}': {}", step_name, e);
                            break;
                        }
                    };

                    let ai_text = ai_response.choices.first()
                        .and_then(|c| c.message.content.as_ref())
                        .map(|s| s.trim().to_string())
                        .unwrap_or_default();

                    if ai_text == "UNFIXABLE" || ai_text.is_empty() {
                        info!("AI determined '{}' is unfixable at step {}", step_name, step_count);
                        break;
                    }

                    let fix_cmd = strip_markdown_fences(&ai_text);

                    // Add AI's response to history
                    messages.push(ChatMessage {
                        role: "assistant".into(),
                        content: Some(fix_cmd.clone()),
                        tool_calls: None,
                        tool_call_id: None,
                    });

                    info!("AI fix for '{}' step {}: {}", step_name, step_count, fix_cmd);

                    let cmd_preview = truncate_output(&fix_cmd, 120);
                    let detail = format!("retry {}: {}", step_name, cmd_preview);
                    self.send_progress(&req.job_id, &req.comment_id, mr_ref, pipeline_status, &detail);

                    // Run the AI's fix command
                    let fix_result = self.exec_in_container(container_id, &fix_cmd, deadline).await;
                    let (fix_exit, fix_output) = match fix_result {
                        Ok(o) => (o.exit_code, o.stdout),
                        Err(e) => (-1, format!("exec error: {}", e)),
                    };

                    // Now retry the original command
                    let detail = format!(
                        "retrying {} after AI fix (step {})...",
                        step_name, step_count
                    );
                    self.send_progress(&req.job_id, &req.comment_id, mr_ref, pipeline_status, &detail);

                    match self.exec_in_container(container_id, cmd, deadline).await {
                        Ok(output) if output.exit_code == 0 => {
                            info!("'{}' succeeded after AI step {}", step_name, step_count);
                            return Ok(output);
                        }
                        Ok(output) => {
                            last_exit_code = output.exit_code;
                            last_output = output.stdout;
                        }
                        Err(e) => {
                            return Err(StepError {
                                output: None,
                                error: format!("{} exec failed: {}", step_name, e),
                            });
                        }
                    }

                    // Feed the result back to the AI as the next user message
                    let mut feedback = String::new();
                    if fix_exit != 0 {
                        feedback.push_str(&format!(
                            "Your fix command exited with code {}:\n```\n{}\n```\n\n",
                            fix_exit, truncate_output(&fix_output, 1000),
                        ));
                    }
                    feedback.push_str(&format!(
                        "After running your fix, the original command still fails.\nExit code: {}\nOutput:\n```\n{}\n```\n\nWhat should I try next?",
                        last_exit_code, truncate_output(&last_output, 2000),
                    ));

                    messages.push(ChatMessage {
                        role: "user".into(),
                        content: Some(feedback),
                        tool_calls: None,
                        tool_call_id: None,
                    });

                    // Trim conversation history if too long
                    if messages.len() > self.prompts.code_params.history_trim_threshold as usize {
                        let system = messages[0].clone();
                        let keep = self.prompts.code_params.history_keep_count as usize;
                        let recent: Vec<_> = messages[messages.len() - keep..].to_vec();
                        messages = std::iter::once(system).chain(recent).collect();
                    }
                }

                Err(StepError {
                    output: Some(last_output),
                    error: format!("{} failed (exit {}) — AI could not resolve", step_name, last_exit_code),
                })
            }
            Err(e) => {
                Err(StepError {
                    output: None,
                    error: format!("{} exec failed: {}", step_name, e),
                })
            }
        }
    }

    // -----------------------------------------------------------------------
    // Docker helpers
    // -----------------------------------------------------------------------

    async fn create_container(
        &self,
        name: &str,
        image: &str,
        memory_limit: i64,
        cpu_quota: i64,
        env: &[String],
        lang: &crate::services::sandbox::detector::ProjectLang,
    ) -> Result<String, String> {
        // Pull image if not present
        let _ = self.docker
            .create_image(
                Some(bollard::image::CreateImageOptions {
                    from_image: image,
                    ..Default::default()
                }),
                None,
                None,
            )
            .collect::<Vec<_>>()
            .await;

        let host_config = bollard::models::HostConfig {
            memory: Some(memory_limit),
            memory_swap: Some(memory_limit), // no swap
            cpu_period: Some(100_000),
            cpu_quota: Some(cpu_quota),
            binds: Some(build_cache_volumes(lang)),
            ..Default::default()
        };

        let config = ContainerConfig {
            image: Some(image.to_string()),
            cmd: Some(vec!["sleep".to_string(), "3600".to_string()]), // keep alive
            working_dir: Some("/workspace".to_string()),
            env: Some(env.to_vec()),
            host_config: Some(host_config),
            ..Default::default()
        };

        let options = CreateContainerOptions { name, platform: None };

        match self.docker.create_container(Some(options), config).await {
            Ok(resp) => Ok(resp.id),
            Err(e) => Err(e.to_string()),
        }
    }

    async fn exec_in_container(
        &self,
        container_id: &str,
        cmd: &str,
        deadline: tokio::time::Instant,
    ) -> Result<ExecOutput, String> {
        self.exec_in_container_inner(container_id, cmd, deadline, None).await
    }

    /// Execute a command in the container and optionally stream live output
    /// to connected Otto extensions via the broadcaster.
    async fn exec_in_container_streaming(
        &self,
        container_id: &str,
        cmd: &str,
        deadline: tokio::time::Instant,
        output_ctx: &OutputContext,
    ) -> Result<ExecOutput, String> {
        if self.cfg.sandbox.live_output {
            self.exec_in_container_inner(container_id, cmd, deadline, Some(output_ctx)).await
        } else {
            self.exec_in_container_inner(container_id, cmd, deadline, None).await
        }
    }

    async fn exec_in_container_inner(
        &self,
        container_id: &str,
        cmd: &str,
        deadline: tokio::time::Instant,
        output_ctx: Option<&OutputContext>,
    ) -> Result<ExecOutput, String> {
        let exec_opts = CreateExecOptions {
            cmd: Some(vec!["sh", "-c", cmd]),
            attach_stdout: Some(true),
            attach_stderr: Some(true),
            working_dir: Some("/workspace"),
            ..Default::default()
        };

        let exec = self
            .docker
            .create_exec(container_id, exec_opts)
            .await
            .map_err(|e| e.to_string())?;

        let output = tokio::time::timeout_at(deadline, async {
            let start_result = self
                .docker
                .start_exec(&exec.id, None)
                .await
                .map_err(|e| e.to_string())?;

            let mut stdout = String::new();

            if let StartExecResults::Attached { mut output, .. } = start_result {
                // When streaming, buffer lines and flush periodically.
                let mut line_buffer: Vec<String> = Vec::new();
                let mut last_flush = tokio::time::Instant::now();
                let flush_interval = std::time::Duration::from_millis(100);
                let redact = self.cfg.sandbox.output_redaction;

                while let Some(Ok(msg)) = output.next().await {
                    let text = msg.to_string();
                    stdout.push_str(&text);

                    if let Some(ctx) = output_ctx {
                        // Split into lines and buffer
                        for line in text.lines() {
                            let line = if redact { redact_line(line) } else { line.to_string() };
                            line_buffer.push(line);
                        }

                        // Flush if buffer is large enough or enough time has passed
                        let now = tokio::time::Instant::now();
                        if line_buffer.len() >= 20 || now.duration_since(last_flush) >= flush_interval {
                            if !line_buffer.is_empty() {
                                self.send_output(ctx, &line_buffer, "stdout");
                                line_buffer.clear();
                            }
                            last_flush = now;
                        }
                    }
                }

                // Flush remaining lines
                if let Some(ctx) = output_ctx {
                    if !line_buffer.is_empty() {
                        self.send_output(ctx, &line_buffer, "stdout");
                    }
                }
            }

            // Get exit code
            let inspect = self
                .docker
                .inspect_exec(&exec.id)
                .await
                .map_err(|e| e.to_string())?;

            let exit_code = inspect.exit_code.unwrap_or(-1) as i32;

            Ok::<ExecOutput, String>(ExecOutput { stdout, exit_code })
        })
        .await
        .map_err(|_| "command timed out".to_string())?;

        output
    }

    async fn cleanup_container(&self, container_id: &str) {
        let opts = RemoveContainerOptions {
            force: true,
            ..Default::default()
        };
        if let Err(e) = self.docker.remove_container(container_id, Some(opts)).await {
            warn!("failed to remove container {}: {}", container_id, e);
        }
    }

    // -----------------------------------------------------------------------
    // Progress helpers
    // -----------------------------------------------------------------------

    fn send_progress(&self, job_id: &str, comment_id: &str, mr_ref: &MrRef, status: &str, detail: &str) {
        let msg = json!({
            "type": "FIX_PROGRESS",
            "job_id": job_id,
            "comment_id": comment_id,
            "status": status,
            "detail": detail,
        });
        (self.broadcaster)(mr_ref, &msg.to_string());
    }

    /// Send live container output lines to all MR viewers.
    fn send_output(&self, ctx: &OutputContext, lines: &[String], stream: &str) {
        let msg = json!({
            "type": "FIX_OUTPUT",
            "job_id": ctx.job_id,
            "comment_id": ctx.comment_id,
            "lines": lines,
            "stream": stream,
        });
        (self.broadcaster)(&ctx.mr_ref, &msg.to_string());
    }

    async fn update_job_status(&self, job_id: &str, status: &str) {
        let _ = db::queries::update_sandbox_job_status(
            &self.pool, job_id, status, None, None, None, None, None,
        )
        .await;
    }

    async fn update_job_status_with_container(&self, job_id: &str, status: &str, container_id: &str) {
        let _ = db::queries::update_sandbox_job_status(
            &self.pool,
            job_id,
            status,
            Some(container_id),
            None,
            None,
            None,
            None,
        )
        .await;
    }
}

// ---------------------------------------------------------------------------
// Exec output
// ---------------------------------------------------------------------------

struct ExecOutput {
    stdout: String,
    exit_code: i32,
}

/// Context for streaming live output to Otto during a fix.
/// Passed to `exec_in_container_streaming` so it knows where to broadcast.
struct OutputContext {
    job_id: String,
    comment_id: String,
    mr_ref: MrRef,
}

/// Error from a pipeline step (with optional captured output).
struct StepError {
    output: Option<String>,
    error: String,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Redact secrets from container output before streaming to Otto.
/// Catches common patterns: API keys, tokens, passwords in env vars, bearer tokens.
/// Best-effort — not a security boundary, just reduces accidental exposure.
fn redact_line(line: &str) -> String {
    // Fast path: most lines don't contain secrets
    let lower = line.to_lowercase();
    let has_suspect = lower.contains("token") || lower.contains("key") || lower.contains("secret")
        || lower.contains("password") || lower.contains("auth") || lower.contains("bearer")
        || lower.contains("glpat-") || lower.contains("sk-");
    if !has_suspect {
        return line.to_string();
    }

    let mut result = line.to_string();

    // GitLab PATs: glpat-XXXX
    if let Some(start) = result.find("glpat-") {
        let end = result[start..].find(|c: char| c.is_whitespace() || c == '\'' || c == '"' || c == '@')
            .map(|i| start + i)
            .unwrap_or(result.len());
        result.replace_range(start..end, "[REDACTED]");
    }

    // OpenAI-style keys: sk-XXXX
    if let Some(start) = result.find("sk-") {
        // Only redact if it looks like a real key (followed by alphanumeric chars)
        let after = &result[start + 3..];
        if after.starts_with(|c: char| c.is_alphanumeric()) {
            let end = result[start..].find(|c: char| c.is_whitespace() || c == '\'' || c == '"')
                .map(|i| start + i)
                .unwrap_or(result.len());
            result.replace_range(start..end, "[REDACTED]");
        }
    }

    // Bearer tokens
    if let Some(start) = lower.find("bearer ") {
        let value_start = start + 7;
        let end = result[value_start..].find(|c: char| c.is_whitespace() || c == '\'' || c == '"')
            .map(|i| value_start + i)
            .unwrap_or(result.len());
        if end > value_start {
            result.replace_range(value_start..end, "[REDACTED]");
        }
    }

    // Key=value patterns in env-like output: TOKEN=xxx, PASSWORD=xxx, etc.
    for pattern in &["TOKEN=", "KEY=", "SECRET=", "PASSWORD=", "token=", "key=", "secret=", "password="] {
        if let Some(start) = result.find(pattern) {
            let value_start = start + pattern.len();
            let end = result[value_start..].find(|c: char| c.is_whitespace() || c == '\'' || c == '"' || c == ';')
                .map(|i| value_start + i)
                .unwrap_or(result.len());
            if end > value_start {
                result.replace_range(value_start..end, "[REDACTED]");
            }
        }
    }

    result
}

/// Format repo context for injection into sandbox prompt templates.
/// Returns a newline-prefixed block when context exists, or an empty string
/// so the `{repo_context}` placeholder collapses cleanly when absent.
fn format_repo_context_block(repo_context: Option<&str>) -> String {
    match repo_context {
        Some(ctx) if !ctx.is_empty() => format!("\n{}\n", ctx),
        _ => String::new(),
    }
}

/// Truncate output to a max byte length for AI prompts (avoids blowing context).
fn truncate_output(s: &str, max_bytes: usize) -> &str {
    if s.len() <= max_bytes {
        s
    } else {
        // Find a safe UTF-8 boundary
        let mut end = max_bytes;
        while end > 0 && !s.is_char_boundary(end) {
            end -= 1;
        }
        &s[..end]
    }
}

/// Check if a command is purely exploratory (read-only) and not worth caching
/// in a setup recipe. The AI often starts with `ls`, `cat`, `head`, `which`,
/// `node --version` etc. to understand the project. These succeed but don't
/// install or configure anything — replaying them wastes time.
///
/// Only filters simple commands. If the command contains chaining operators
/// (`&&`, `||`, `;`, `|`) it might be doing real work alongside the read,
/// so we keep it.
fn is_exploratory_command(cmd: &str) -> bool {
    let trimmed = cmd.trim();

    // If the command chains multiple operations, it's likely doing real work
    if trimmed.contains("&&") || trimmed.contains("||") || trimmed.contains(" | ") || trimmed.contains(';') {
        return false;
    }

    // Extract the base command (first word, ignoring cd prefix)
    let base = trimmed
        .strip_prefix("cd /workspace && ")
        .or_else(|| trimmed.strip_prefix("cd /workspace; "))
        .unwrap_or(trimmed);

    let first_word = base.split_whitespace().next().unwrap_or("");

    matches!(first_word,
        "ls" | "cat" | "head" | "tail" | "less" | "more" | "file" | "wc" |
        "find" | "tree" | "which" | "whereis" | "type" | "command" |
        "echo" | "printf" | "pwd" | "env" | "printenv" | "id" | "whoami" |
        "uname" | "hostname" | "date" | "df" | "du" | "free" | "top" |
        "ps" | "test" | "stat" | "readlink" | "realpath" | "basename" | "dirname"
    ) || (
        // Version check commands: `node --version`, `ruby -v`, `python3 -V`, etc.
        base.split_whitespace().count() == 2 && base.split_whitespace().nth(1)
            .map(|arg| arg == "--version" || arg == "-v" || arg == "-V" || arg == "--help")
            .unwrap_or(false)
    )
}

/// Strip markdown code fences and optional language identifiers from AI responses.
fn strip_markdown_fences(s: &str) -> String {
    let trimmed = s.trim();

    // Try to strip ```lang\n...\n``` pattern
    let stripped = if trimmed.starts_with("```") && trimmed.ends_with("```") {
        let inner = &trimmed[3..trimmed.len() - 3];
        // Skip optional language identifier on first line
        if let Some(newline_pos) = inner.find('\n') {
            let first_line = inner[..newline_pos].trim();
            // If first line looks like a language id (single word, no spaces, no operators)
            if !first_line.is_empty()
                && !first_line.contains(' ')
                && first_line.chars().all(|c| c.is_alphanumeric() || c == '-' || c == '+')
            {
                inner[newline_pos + 1..].trim().to_string()
            } else {
                inner.trim().to_string()
            }
        } else {
            inner.trim().to_string()
        }
    } else {
        trimmed.to_string()
    };

    // Also strip single backticks
    stripped
        .trim_start_matches('`')
        .trim_end_matches('`')
        .trim()
        .to_string()
}

// ---------------------------------------------------------------------------
// Command builders
// ---------------------------------------------------------------------------

/// Build Docker bind-mount volumes for shared package caches.
///
/// These named volumes persist across container lifecycles, so the second
/// container to build a Go project reuses the module cache from the first.
/// This cuts `go mod download` / `bundle install` / `npm ci` from minutes
/// to seconds on repeat runs.
///
/// Uses Docker named volumes (not host paths) so they work on any host OS
/// and are automatically managed by Docker.
fn build_cache_volumes(lang: &crate::services::sandbox::detector::ProjectLang) -> Vec<String> {
    use crate::services::sandbox::detector::ProjectLang;

    let mut vols = Vec::new();

    match lang {
        ProjectLang::Go => {
            // Go module cache + build cache
            vols.push("botto-cache-gomod:/root/go/pkg/mod".to_string());
            vols.push("botto-cache-gobuild:/root/.cache/go-build".to_string());
        }
        ProjectLang::Ruby => {
            // Bundler gem cache (not the install dir — that's image-specific)
            vols.push("botto-cache-bundle:/usr/local/bundle/cache".to_string());
        }
        ProjectLang::Node => {
            // npm/yarn/pnpm caches
            vols.push("botto-cache-npm:/root/.npm".to_string());
            vols.push("botto-cache-yarn:/root/.cache/yarn".to_string());
            vols.push("botto-cache-pnpm:/root/.local/share/pnpm/store".to_string());
        }
        ProjectLang::Python => {
            // pip download cache
            vols.push("botto-cache-pip:/root/.cache/pip".to_string());
        }
        ProjectLang::Rust => {
            // Cargo registry + git cache (not target/ — that's project-specific)
            vols.push("botto-cache-cargo-registry:/usr/local/cargo/registry".to_string());
            vols.push("botto-cache-cargo-git:/usr/local/cargo/git".to_string());
        }
        ProjectLang::Java | ProjectLang::Scala | ProjectLang::Clojure => {
            // Maven/Gradle local repository
            vols.push("botto-cache-maven:/root/.m2/repository".to_string());
            vols.push("botto-cache-gradle:/root/.gradle/caches".to_string());
        }
        ProjectLang::DotNet => {
            // NuGet package cache
            vols.push("botto-cache-nuget:/root/.nuget/packages".to_string());
        }
        ProjectLang::Elixir => {
            // Hex + Mix cache
            vols.push("botto-cache-hex:/root/.hex".to_string());
            vols.push("botto-cache-mix:/root/.mix".to_string());
        }
        ProjectLang::Php => {
            // Composer cache
            vols.push("botto-cache-composer:/root/.composer/cache".to_string());
        }
        _ => {}
    }

    vols
}

/// Build container environment variables based on detected language.
/// These persist across all `exec_in_container` calls, preventing the AI
/// from wasting steps on `export PATH=...` after every command.
fn build_container_env(lang: &crate::services::sandbox::detector::ProjectLang) -> Vec<String> {
    use crate::services::sandbox::detector::ProjectLang;

    // Start with a comprehensive PATH that includes common runtime locations.
    // The base image already has its own PATH; we prepend additional paths
    // so tools installed by the image or by the AI are found immediately.
    let mut env = vec![
        // Comprehensive PATH covering all common runtime install locations
        "PATH=/usr/local/go/bin:/usr/local/cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/root/.local/bin:/root/go/bin".to_string(),
        // Non-interactive apt-get (no prompts)
        "DEBIAN_FRONTEND=noninteractive".to_string(),
        // Disable interactive pagers (git, man, etc.)
        "GIT_PAGER=cat".to_string(),
        "PAGER=cat".to_string(),
        // Sensible defaults
        "LANG=C.UTF-8".to_string(),
        "HOME=/root".to_string(),
    ];

    // Language-specific env vars
    match lang {
        ProjectLang::Go => {
            env.push("GOPATH=/root/go".to_string());
            env.push("GOFLAGS=-mod=mod".to_string());
        }
        ProjectLang::Node => {
            env.push("NODE_ENV=test".to_string());
            env.push("NPM_CONFIG_LOGLEVEL=warn".to_string());
            // Disable npm update notifier (noisy in CI)
            env.push("NO_UPDATE_NOTIFIER=1".to_string());
        }
        ProjectLang::Python => {
            // Don't write .pyc files (faster in ephemeral containers)
            env.push("PYTHONDONTWRITEBYTECODE=1".to_string());
            env.push("PYTHONUNBUFFERED=1".to_string());
            env.push("PIP_DISABLE_PIP_VERSION_CHECK=1".to_string());
        }
        ProjectLang::Rust => {
            env.push("CARGO_HOME=/usr/local/cargo".to_string());
            env.push("RUSTUP_HOME=/usr/local/rustup".to_string());
        }
        ProjectLang::DotNet => {
            // Suppress .NET telemetry and first-run experience
            env.push("DOTNET_CLI_TELEMETRY_OPTOUT=1".to_string());
            env.push("DOTNET_NOLOGO=1".to_string());
        }
        ProjectLang::Java | ProjectLang::Scala | ProjectLang::Clojure => {
            // Reduce JVM memory footprint in containers
            env.push("JAVA_TOOL_OPTIONS=-Xmx1536m".to_string());
            env.push("MAVEN_OPTS=-Xmx1536m".to_string());
        }
        ProjectLang::Ruby => {
            env.push("BUNDLE_SILENCE_ROOT_WARNING=1".to_string());
        }
        _ => {}
    }

    env
}

/// Pre-install native system dependencies for known problematic packages.
///
/// Many language ecosystems have packages that require native C libraries to
/// compile. The AI wastes 10-20 steps discovering and installing these one by
/// one. This function scans the cloned repo's lockfiles and pre-installs the
/// system packages needed for common native gems/packages.
///
/// Returns None if no pre-installation is needed for this language.
/// The command is best-effort — if it fails, the AI setup loop handles it.
fn build_native_deps_command(lang: &crate::services::sandbox::detector::ProjectLang) -> Option<String> {
    use crate::services::sandbox::detector::ProjectLang;

    match lang {
        ProjectLang::Ruby => {
            // Ruby gems with native extensions are the #1 source of setup pain.
            // Check Gemfile.lock for known problematic gems and pre-install their deps.
            // Uses apt-get (Debian/Ubuntu slim images) with fallback to apk (Alpine).
            Some(concat!(
                "if [ -f /workspace/Gemfile.lock ]; then ",
                  "DEPS=''; ",
                  // rugged (libgit2 bindings) — needs cmake, libgit2-dev, libssl-dev, libzstd-dev
                  "grep -q 'rugged' /workspace/Gemfile.lock && DEPS=\"$DEPS build-essential cmake pkg-config libgit2-dev libssl-dev libzstd-dev\"; ",
                  // nokogiri (XML parser) — needs libxml2-dev, libxslt-dev
                  "grep -q 'nokogiri' /workspace/Gemfile.lock && DEPS=\"$DEPS build-essential pkg-config libxml2-dev libxslt-dev\"; ",
                  // pg (PostgreSQL client) — needs libpq-dev
                  "grep -q '  pg ' /workspace/Gemfile.lock && DEPS=\"$DEPS libpq-dev\"; ",
                  // mysql2 — needs libmysqlclient-dev
                  "grep -q 'mysql2' /workspace/Gemfile.lock && DEPS=\"$DEPS default-libmysqlclient-dev\"; ",
                  // grpc — needs build tools
                  "grep -q 'grpc' /workspace/Gemfile.lock && DEPS=\"$DEPS build-essential\"; ",
                  // ffi — needs libffi-dev
                  "grep -q '  ffi ' /workspace/Gemfile.lock && DEPS=\"$DEPS libffi-dev\"; ",
                  // sassc — needs build tools
                  "grep -q 'sassc' /workspace/Gemfile.lock && DEPS=\"$DEPS build-essential\"; ",
                  "if [ -n \"$DEPS\" ]; then ",
                    "if command -v apt-get >/dev/null 2>&1; then ",
                      "apt-get update -qq && apt-get install -y -qq --no-install-recommends $DEPS 2>&1 | tail -5; ",
                    "elif command -v apk >/dev/null 2>&1; then ",
                      "apk add --no-cache build-base cmake pkgconfig libgit2-dev openssl-dev zstd-dev libxml2-dev libxslt-dev postgresql-dev libffi-dev 2>&1 | tail -5; ",
                    "fi; ",
                  "fi; ",
                "fi"
            ).to_string())
        }
        ProjectLang::Python => {
            // Python packages with C extensions
            Some(concat!(
                "if [ -f /workspace/requirements.txt ] || [ -f /workspace/pyproject.toml ]; then ",
                  "DEPS=''; ",
                  "FILES=$(cat /workspace/requirements.txt /workspace/pyproject.toml 2>/dev/null); ",
                  // psycopg2 — needs libpq-dev
                  "echo \"$FILES\" | grep -q 'psycopg2' && DEPS=\"$DEPS libpq-dev\"; ",
                  // lxml — needs libxml2-dev, libxslt-dev
                  "echo \"$FILES\" | grep -q 'lxml' && DEPS=\"$DEPS libxml2-dev libxslt-dev\"; ",
                  // Pillow — needs image libs
                  "echo \"$FILES\" | grep -qi 'pillow' && DEPS=\"$DEPS libjpeg-dev zlib1g-dev\"; ",
                  // cryptography — needs libssl-dev, libffi-dev
                  "echo \"$FILES\" | grep -q 'cryptography' && DEPS=\"$DEPS libssl-dev libffi-dev\"; ",
                  // mysqlclient — needs libmysqlclient-dev
                  "echo \"$FILES\" | grep -q 'mysqlclient' && DEPS=\"$DEPS default-libmysqlclient-dev\"; ",
                  "if [ -n \"$DEPS\" ]; then ",
                    "if command -v apt-get >/dev/null 2>&1; then ",
                      "apt-get update -qq && apt-get install -y -qq --no-install-recommends build-essential $DEPS 2>&1 | tail -5; ",
                    "fi; ",
                  "fi; ",
                "fi"
            ).to_string())
        }
        ProjectLang::Node => {
            // Node native addons (node-gyp based)
            Some(concat!(
                "if [ -f /workspace/package.json ]; then ",
                  "DEPS=''; ",
                  // sharp (image processing) — needs vips
                  "grep -q 'sharp' /workspace/package.json && DEPS=\"$DEPS libvips-dev\"; ",
                  // bcrypt — needs build tools
                  "grep -q 'bcrypt' /workspace/package.json && DEPS=\"$DEPS build-essential python3\"; ",
                  // canvas — needs cairo, pango
                  "grep -q 'canvas' /workspace/package.json && DEPS=\"$DEPS build-essential libcairo2-dev libpango1.0-dev libjpeg-dev libgif-dev librsvg2-dev\"; ",
                  // sqlite3 — needs build tools
                  "grep -q 'better-sqlite3\\|sqlite3' /workspace/package.json && DEPS=\"$DEPS build-essential python3\"; ",
                  "if [ -n \"$DEPS\" ]; then ",
                    "if command -v apt-get >/dev/null 2>&1; then ",
                      "apt-get update -qq && apt-get install -y -qq --no-install-recommends $DEPS 2>&1 | tail -5; ",
                    "fi; ",
                  "fi; ",
                "fi"
            ).to_string())
        }
        _ => None,
    }
}

/// Detect setup commands by checking what package manager files exist.
async fn detect_setup_commands(container_id: &str, mgr: &SandboxManager) -> Vec<String> {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    let mut cmds = Vec::new();

    // Check for package manager files
    let check = mgr
        .exec_in_container(container_id, "ls /workspace", deadline)
        .await;

    if let Ok(output) = check {
        let files = output.stdout;
        if files.contains("package-lock.json") {
            cmds.push("cd /workspace && npm ci --ignore-scripts".to_string());
        } else if files.contains("yarn.lock") {
            cmds.push("cd /workspace && yarn install --frozen-lockfile".to_string());
        } else if files.contains("pnpm-lock.yaml") {
            cmds.push("cd /workspace && pnpm install --frozen-lockfile".to_string());
        } else if files.contains("package.json") {
            cmds.push("cd /workspace && npm install".to_string());
        } else if files.contains("requirements.txt") {
            cmds.push("cd /workspace && pip install -r requirements.txt".to_string());
        } else if files.contains("Pipfile.lock") {
            cmds.push("cd /workspace && pipenv install".to_string());
        } else if files.contains("go.mod") {
            cmds.push("cd /workspace && go mod download".to_string());
        } else if files.contains("Gemfile.lock") {
            cmds.push("cd /workspace && bundle install".to_string());
        }
    }

    cmds
}

/// Detect the appropriate test command.
async fn detect_test_command(
    container_id: &str,
    mgr: &SandboxManager,
    file_path: &str,
    strategy: &FixStrategy,
) -> String {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);

    let check = mgr
        .exec_in_container(container_id, "ls /workspace", deadline)
        .await;

    if let Ok(output) = check {
        let files = output.stdout;

        // Node.js projects
        if files.contains("package.json") {
            if *strategy == FixStrategy::TestOnly {
                // Try to find a related test file
                let test_file = infer_test_file(file_path);
                return format!(
                    "cd /workspace && npx jest --passWithNoTests {} 2>&1 || npx vitest run {} 2>&1",
                    test_file, test_file
                );
            }
            return "cd /workspace && npm test 2>&1".to_string();
        }

        // Python
        if files.contains("pyproject.toml") || files.contains("setup.py") {
            if *strategy == FixStrategy::TestOnly {
                let test_file = infer_test_file(file_path);
                return format!("cd /workspace && python -m pytest {} -v 2>&1", test_file);
            }
            return "cd /workspace && python -m pytest -v 2>&1".to_string();
        }

        // Go
        if files.contains("go.mod") {
            let pkg = file_path
                .rsplit_once('/')
                .map(|(dir, _)| format!("./{}/...", dir))
                .unwrap_or_else(|| "./...".to_string());
            return format!("cd /workspace && go test {} -v 2>&1", pkg);
        }

        // Ruby
        if files.contains("Gemfile") {
            return "cd /workspace && bundle exec rspec 2>&1".to_string();
        }

        // Rust
        if files.contains("Cargo.toml") {
            return "cd /workspace && cargo test 2>&1".to_string();
        }
    }

    // Fallback: try make test
    "cd /workspace && make test 2>&1 || echo 'no test command found'".to_string()
}

/// Infer the test file path from a source file path.
fn infer_test_file(file_path: &str) -> String {
    // Common patterns: foo.ts → foo.test.ts, foo.py → test_foo.py
    let path = std::path::Path::new(file_path);
    let stem = path.file_stem().unwrap_or_default().to_str().unwrap_or("");
    let ext = path.extension().unwrap_or_default().to_str().unwrap_or("");
    let dir = path.parent().unwrap_or(std::path::Path::new(""));

    match ext {
        "ts" | "tsx" | "js" | "jsx" => {
            // Try: same dir with .test. or .spec. suffix
            format!("{}/{}.test.{}", dir.display(), stem, ext)
        }
        "py" => {
            // Try: test_<name>.py in same dir or tests/ dir
            format!("{}/test_{}.py", dir.display(), stem)
        }
        "go" => {
            format!("{}/{}_test.go", dir.display(), stem)
        }
        "rb" => {
            format!("spec/{}_spec.rb", stem)
        }
        _ => file_path.to_string(),
    }
}

/// Build the command to apply a code fix (replace original with suggestion).
fn build_apply_command(file_path: &str, original: &str, suggestion: &str) -> String {
    // Use base64 encoding to avoid ALL shell/Python quoting issues.
    // The old approach used escaped strings inside double-quoted Python,
    // which broke on code containing double quotes, backslashes, or
    // special characters (common in real-world code).
    //
    // Also handles the "already applied" case: if the original code is gone
    // but the suggestion is already present, exit 0 (success). This prevents
    // the AI retry loop from spinning when it already fixed the code.
    use base64::Engine;
    let b64 = base64::engine::general_purpose::STANDARD;
    let original_b64 = b64.encode(original.as_bytes());
    let suggestion_b64 = b64.encode(suggestion.as_bytes());
    let file_path_b64 = b64.encode(file_path.as_bytes());

    format!(
        r#"cd /workspace && python3 -c "
import base64, sys
path = base64.b64decode('{}').decode()
original = base64.b64decode('{}').decode()
replacement = base64.b64decode('{}').decode()
with open(path, 'r') as f:
    content = f.read()
if original in content:
    content = content.replace(original, replacement, 1)
    with open(path, 'w') as f:
        f.write(content)
    print('Fix applied successfully')
elif replacement in content:
    print('Fix already applied')
else:
    print('WARNING: original code not found in file')
    sys.exit(1)
""#,
        file_path_b64,
        original_b64,
        suggestion_b64,
    )
}

/// Simple shell escaping — wraps in single quotes.
fn shell_escape(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\\''"))
}

/// Generate a branch name for new_branch fix mode.
/// Format: `botto/fix/mr-{iid}-{slug}[-{suffix}]`
/// The slug is derived from the MR title (or file path as fallback),
/// lowercased, non-alphanumeric chars replaced with hyphens, truncated to 40 chars.
fn generate_fix_branch_name(mr_iid: u64, mr_title: Option<&str>, file_path: &str, comment_id: &str) -> String {
    let raw = mr_title.unwrap_or(file_path);

    let slug: String = raw
        .to_lowercase()
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect::<String>()
        .split('-')
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join("-");

    // Truncate slug and add a short suffix from comment_id for uniqueness
    // (multiple fixes on the same MR get different branches)
    let slug_truncated = if slug.len() > 40 { &slug[..40] } else { &slug };
    let suffix: String = comment_id
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .take(6)
        .collect();

    format!("botto/fix/mr-{}-{}-{}", mr_iid, slug_truncated, suffix)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn branch_name_from_mr_title() {
        let name = generate_fix_branch_name(42, Some("Add user authentication"), "src/auth.rs", "abc123");
        assert_eq!(name, "botto/fix/mr-42-add-user-authentication-abc123");
    }

    #[test]
    fn branch_name_falls_back_to_file_path() {
        let name = generate_fix_branch_name(7, None, "src/utils/parser.ts", "def456");
        assert_eq!(name, "botto/fix/mr-7-src-utils-parser-ts-def456");
    }

    #[test]
    fn branch_name_truncates_long_titles() {
        let long_title = "This is a very long merge request title that exceeds the forty character slug limit by quite a lot";
        let name = generate_fix_branch_name(99, Some(long_title), "file.rs", "xyz789");
        // Slug should be truncated to 40 chars
        assert!(name.starts_with("botto/fix/mr-99-"));
        assert!(name.ends_with("-xyz789"));
        // Total slug portion should be <= 40 chars
        let slug_part = name.strip_prefix("botto/fix/mr-99-").unwrap()
            .strip_suffix("-xyz789").unwrap();
        assert!(slug_part.len() <= 40);
    }

    #[test]
    fn branch_name_strips_special_chars() {
        let name = generate_fix_branch_name(1, Some("fix: handle `None` case (edge-case)"), "lib.rs", "aaa111");
        // Special chars become hyphens, consecutive hyphens collapsed
        assert_eq!(name, "botto/fix/mr-1-fix-handle-none-case-edge-case-aaa111");
    }

    #[test]
    fn branch_name_suffix_from_followup_key() {
        // Follow-up fix keys look like "followup-99887766-0"
        let name = generate_fix_branch_name(42, Some("Add auth"), "src/auth.rs", "followup-99887766-0");
        // Suffix takes first 6 alphanumeric chars: "follow"
        assert_eq!(name, "botto/fix/mr-42-add-auth-follow");
    }

    #[test]
    fn branch_name_suffix_from_numeric_comment_id() {
        let name = generate_fix_branch_name(42, Some("Fix bug"), "src/main.rs", "55443322");
        assert_eq!(name, "botto/fix/mr-42-fix-bug-554433");
    }

    #[test]
    fn branch_name_unique_per_comment() {
        let name1 = generate_fix_branch_name(42, Some("Fix bug"), "src/main.rs", "aaa111");
        let name2 = generate_fix_branch_name(42, Some("Fix bug"), "src/main.rs", "bbb222");
        assert_ne!(name1, name2);
    }

    // --- is_exploratory_command tests ---

    #[test]
    fn exploratory_simple_read_commands() {
        assert!(is_exploratory_command("ls /workspace"));
        assert!(is_exploratory_command("cat package.json"));
        assert!(is_exploratory_command("head -20 Gemfile"));
        assert!(is_exploratory_command("which node"));
        assert!(is_exploratory_command("find /workspace -name '*.rb'"));
        assert!(is_exploratory_command("pwd"));
        assert!(is_exploratory_command("wc -l src/main.rs"));
        assert!(is_exploratory_command("file /workspace/Makefile"));
    }

    #[test]
    fn exploratory_version_checks() {
        assert!(is_exploratory_command("node --version"));
        assert!(is_exploratory_command("ruby -v"));
        assert!(is_exploratory_command("python3 -V"));
        assert!(is_exploratory_command("go --version"));
        assert!(is_exploratory_command("cargo --help"));
    }

    #[test]
    fn not_exploratory_install_commands() {
        assert!(!is_exploratory_command("npm ci"));
        assert!(!is_exploratory_command("bundle install"));
        assert!(!is_exploratory_command("pip install -r requirements.txt"));
        assert!(!is_exploratory_command("apt-get update && apt-get install -y git"));
        assert!(!is_exploratory_command("cd /workspace && npm ci"));
        assert!(!is_exploratory_command("mkdir -p /tmp/build"));
        assert!(!is_exploratory_command("chmod +x scripts/setup.sh"));
    }

    #[test]
    fn not_exploratory_chained_commands() {
        // Even if first command is read-only, chaining means real work
        assert!(!is_exploratory_command("ls /workspace && npm install"));
        assert!(!is_exploratory_command("cat package.json || echo 'no package.json'"));
        assert!(!is_exploratory_command("node --version; nvm install 20"));
        assert!(!is_exploratory_command("which python3 | head -1"));
    }

    #[test]
    fn not_exploratory_multiline_scripts() {
        assert!(!is_exploratory_command("export NODE_ENV=test"));
        assert!(!is_exploratory_command("nvm install 20"));
        assert!(!is_exploratory_command("curl -fsSL https://example.com/setup.sh"));
    }
}
