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
use futures::StreamExt;
use serde_json::json;
use sqlx::SqlitePool;
use std::sync::Arc;
use tokio::sync::{Mutex, Semaphore};
use tracing::{debug, info, warn};

/// Default sandbox timeout — 30 minutes for the full pipeline.
const DEFAULT_SANDBOX_TIMEOUT_SECS: u64 = 1800;

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
}

impl SandboxManager {
    pub fn new(
        cfg: BottoConfig,
        pool: SqlitePool,
        event_bus: EventBus,
        broadcaster: Arc<dyn Fn(&MrRef, &str) + Send + Sync>,
    ) -> Option<Self> {
        Self::with_prompts(cfg, pool, event_bus, broadcaster, SandboxPrompts::default(), false, None)
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
        })
    }

    /// Execute a fix in a sandboxed Docker container.
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
                };
            }
        };

        let mr_ref = MrRef {
            project_path: req.project_path.clone(),
            mr_iid: req.mr_iid,
        };

        self.send_progress(&req.job_id, &req.comment_id, &mr_ref, "cloning", "cloning repository...");
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
                };
            }
        };

        // Try to fetch .otto.json for user-configured sandbox settings
        let otto_config = match crate::services::gitlab::client::fetch_file_content(
            &gl_cfg, project_id, ".otto.json", &req.source_branch,
        ).await {
            Ok(content) => serde_json::from_str::<serde_json::Value>(&content).ok(),
            Err(_) => None, // File doesn't exist or can't be read — that's fine
        };

        let base_image = detector::detect_base_image(
            &gl_cfg,
            project_id,
            &req.source_branch,
            otto_config.as_ref(),
        )
        .await;

        let strategy = detector::determine_strategy(
            &gl_cfg,
            project_id,
            &req.source_branch,
            self.cfg.sandbox.max_memory_mb,
        )
        .await;

        info!(
            "sandbox fix: job={} image={} strategy={:?}",
            req.job_id, base_image, strategy
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

        // Create container
        let container_name = format!("botto-fix-{}", &req.job_id[..8]);
        let memory_limit = (self.cfg.sandbox.max_memory_mb * 1024 * 1024) as i64;

        let container_id = match self
            .create_container(&container_name, &base_image, memory_limit)
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
                };
            }
        };

        self.update_job_status_with_container(&req.job_id, "cloning", &container_id)
            .await;

        // Start container
        if let Err(e) = self
            .docker
            .start_container(&container_id, None::<StartContainerOptions<String>>)
            .await
        {
            self.cleanup_container(&container_id).await;
            return FixResult {
                job_id: req.job_id,
                success: false,
                commit_sha: None,
                test_output: None,
                error: Some(format!("failed to start container: {}", e)),
            };
        }

        // Execute the fix pipeline inside the container
        let result = self
            .execute_fix_pipeline(&container_id, &req, &clone_url_authed, &strategy, &mr_ref)
            .await;

        // Cleanup
        self.cleanup_container(&container_id).await;

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
    async fn execute_fix_pipeline(
        &self,
        container_id: &str,
        req: &FixRequest,
        clone_url: &str,
        strategy: &FixStrategy,
        mr_ref: &MrRef,
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
            };
        }

        // Step 2: AI-driven project setup.
        // The AI reads the project, understands it, installs deps, and gets
        // the environment ready to run tests. No hardcoded commands — the AI
        // figures out what the project needs.
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
                        .replace("{test_cmd}", &test_cmd_preview),
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

            loop {
                if tokio::time::Instant::now() >= deadline {
                    info!("sandbox timeout reached during AI setup");
                    break;
                }

                setup_step += 1;
                if let Some(ref t) = self.telemetry {
                    t.setup_steps.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                let detail = format!("AI setting up project (step {})...", setup_step);
                self.send_progress(&req.job_id, &req.comment_id, mr_ref, "setting_up", &detail);

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
                    };
                }

                if cmd == "SETUP_DONE" {
                    info!("AI completed project setup after {} steps", setup_step);
                    break;
                }

                setup_messages.push(ChatMessage {
                    role: "assistant".into(),
                    content: Some(cmd.clone()),
                    tool_calls: None,
                    tool_call_id: None,
                });

                info!("AI setup step {}: {}", setup_step, truncate_output(&cmd, 200));

                let (cmd_exit, cmd_output) = match self.exec_in_container(container_id, &cmd, deadline).await {
                    Ok(o) => (o.exit_code, o.stdout),
                    Err(e) => (-1, format!("exec error: {}", e)),
                };

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
        }

        // Step 3: Apply fix (with AI retry)
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
        match self.exec_in_container(container_id, &test_cmd, deadline).await {
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
                        .replace("{test_cmd}", &test_cmd),
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
                let detail = format!("AI working on fix (step {})...", step_count);
                self.send_progress(&req.job_id, &req.comment_id, mr_ref, "testing", &detail);

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

                    match self.exec_in_container(container_id, &test_cmd, deadline).await {
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
                    let detail = format!("AI running command (step {})...", step_count);
                    self.send_progress(&req.job_id, &req.comment_id, mr_ref, "testing", &detail);

                    let (cmd_exit, cmd_output) = match self.exec_in_container(container_id, &cmd, deadline).await {
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
            };
        }

        self.send_progress(&req.job_id, &req.comment_id, mr_ref, "pushing", "committing and pushing...");
        self.update_job_status(&req.job_id, "pushing").await;

        let commit_msg = format!(
            "fix: applied suggestion from review comment {}\n\nApplied by Botto sandbox",
            req.comment_id
        );

        let git_cmds = format!(
            "cd /workspace && git config user.name 'Botto' && git config user.email 'botto@bot' && git add -A && git commit -m {} && git push origin {}",
            shell_escape(&commit_msg),
            shell_escape(&req.source_branch),
        );

        let push_result = self.exec_in_container(container_id, &git_cmds, deadline).await;
        let mut commit_sha = None;

        match push_result {
            Ok(output) if output.exit_code == 0 => {
                // Git push succeeded — extract SHA
                let sha_cmd = "cd /workspace && git rev-parse HEAD";
                commit_sha = match self.exec_in_container(container_id, sha_cmd, deadline).await {
                    Ok(o) => Some(o.stdout.trim().to_string()),
                    Err(_) => None,
                };
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

                    let mut api_success = false;
                    for pid in &target_project_ids {
                        match crate::services::gitlab::client::create_commit(
                            &gl_cfg,
                            *pid,
                            &req.source_branch,
                            &commit_msg,
                            vec![action.clone()],
                        ).await {
                            Ok(resp) => {
                                info!("GitLab API commit succeeded: {} on project {}", resp.id, pid);
                                commit_sha = Some(resp.id);
                                api_success = true;
                                break;
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
                        };
                    }
                } else {
                    return FixResult {
                        job_id: req.job_id.clone(),
                        success: false,
                        commit_sha: None,
                        test_output: Some(test_output),
                        error: Some("push failed: could not read modified file from container".into()),
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

                    let detail = format!(
                        "running AI fix for {} (step {})...",
                        step_name, step_count
                    );
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
            cpu_quota: Some(100_000), // 1 CPU
            ..Default::default()
        };

        let config = ContainerConfig {
            image: Some(image.to_string()),
            cmd: Some(vec!["sleep".to_string(), "3600".to_string()]), // keep alive
            working_dir: Some("/workspace".to_string()),
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
                while let Some(Ok(msg)) = output.next().await {
                    stdout.push_str(&msg.to_string());
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

/// Error from a pipeline step (with optional captured output).
struct StepError {
    output: Option<String>,
    error: String,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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
