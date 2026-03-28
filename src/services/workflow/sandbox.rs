// ---------------------------------------------------------------------------
// Sandbox workflow agent — run code/tests in isolated Docker containers.
//
// Wraps bollard (Docker API) for workflow steps that need isolated execution.
// Unlike the full SandboxManager (which handles the complete fix lifecycle),
// this agent provides simple container-based command execution for workflows.
//
// Supported actions:
//   - run_in_container: execute a command in a fresh container
//   - build_and_test: clone a repo, run a build/test command
//
// Inputs:
//   - image (required): Docker image to use
//   - command (required): command to run inside the container
//   - timeout_secs (optional): container timeout (default 300)
//   - env (optional): object of environment variable key-value pairs
//   - working_dir (optional): working directory inside the container
//   - clone_url (optional): git repo URL to clone before running command
//   - clone_branch (optional): branch to checkout (default "main")
//   - network_enabled (optional): allow network access (default false)
//
// Security:
//   - Shell injection is prevented by passing arguments as separate args
//     to Command, never via format!() string interpolation.
//   - Containers are CPU-limited (default 1 CPU).
//   - Network is disabled by default for untrusted workloads.
//   - Output truncation is UTF-8 safe.
//   - Docker connectivity is verified on agent creation.
// ---------------------------------------------------------------------------

use bollard::container::{
    Config as ContainerConfig, CreateContainerOptions, RemoveContainerOptions,
    StartContainerOptions,
};
use bollard::exec::{CreateExecOptions, StartExecResults};
use bollard::Docker;
use futures::StreamExt;
use serde_json::Value;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::time::Instant;
use tracing::{debug, info, warn};

use crate::services::mentor::client::MentorClient;
use crate::services::workflow::traits::{failure_result, success_result, WorkflowAgent};
use crate::types::workflow::AgentResult;

/// Default CPU limit for containers (in units of CPUs).
const DEFAULT_CPU_LIMIT: f64 = 1.0;

/// Sandbox workflow agent — isolated Docker container execution.
pub struct SandboxAgent {
    docker: Docker,
    /// Max memory per container in bytes (default 2GB).
    max_memory: i64,
    /// CPU limit in nanoCPUs. 1 CPU = 1_000_000_000 nanoCPUs.
    nano_cpus: i64,
}

impl SandboxAgent {
    pub fn new(docker: Docker, max_memory_mb: u64) -> Self {
        Self {
            docker,
            max_memory: (max_memory_mb * 1024 * 1024) as i64,
            nano_cpus: (DEFAULT_CPU_LIMIT * 1_000_000_000.0) as i64,
        }
    }

    /// Try to create from local Docker defaults.
    /// Returns None if Docker is unavailable or not responding.
    pub async fn try_new(max_memory_mb: u64) -> Option<Self> {
        let docker = Docker::connect_with_local_defaults().ok()?;

        // Verify Docker is actually reachable.
        match docker.ping().await {
            Ok(_) => {
                info!("sandbox agent: Docker connection verified");
                Some(Self::new(docker, max_memory_mb))
            }
            Err(e) => {
                warn!(error = %e, "sandbox agent: Docker ping failed, agent unavailable");
                None
            }
        }
    }
}

impl WorkflowAgent for SandboxAgent {
    fn execute<'a>(
        &'a self,
        action: &str,
        inputs: HashMap<String, Value>,
        _mentor: &'a MentorClient,
    ) -> Pin<Box<dyn Future<Output = AgentResult> + Send + 'a>> {
        let action = action.to_string();
        Box::pin(async move {
            let start = Instant::now();
            debug!(action = action.as_str(), "sandbox agent: executing");

            let result = match action.as_str() {
                "run_in_container" => self.run_in_container(&inputs).await,
                "build_and_test" => self.build_and_test(&inputs).await,
                other => Err(format!("unknown sandbox action: {other}")),
            };

            let duration = start.elapsed().as_secs_f64();
            match result {
                Ok(output) => success_result(output, duration),
                Err(e) => {
                    warn!(action = action.as_str(), error = %e, "sandbox agent: action failed");
                    failure_result(&e, duration)
                }
            }
        })
    }

    fn agent_type_name(&self) -> &'static str {
        "sandbox"
    }
}

impl SandboxAgent {
    async fn run_in_container(&self, inputs: &HashMap<String, Value>) -> Result<Value, String> {
        let image = get_str(inputs, "image")?;
        let command = get_str(inputs, "command")?;
        let timeout_secs = inputs
            .get("timeout_secs")
            .and_then(|v| v.as_u64())
            .unwrap_or(300);
        let network_enabled = inputs
            .get("network_enabled")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        let env_vars = build_env_vars(inputs);
        let working_dir = inputs
            .get("working_dir")
            .and_then(|v| v.as_str())
            .map(String::from);

        let container_id = self
            .create_container(&image, &env_vars, working_dir.as_deref(), network_enabled)
            .await?;

        let result = self
            .exec_with_timeout(&container_id, &command, timeout_secs)
            .await;

        // Always clean up the container.
        self.remove_container(&container_id).await;

        result
    }

    async fn build_and_test(&self, inputs: &HashMap<String, Value>) -> Result<Value, String> {
        let image = get_str(inputs, "image")?;
        let command = get_str(inputs, "command")?;
        let clone_url = get_str(inputs, "clone_url")?;
        let branch = inputs
            .get("clone_branch")
            .and_then(|v| v.as_str())
            .unwrap_or("main")
            .to_string();
        let timeout_secs = inputs
            .get("timeout_secs")
            .and_then(|v| v.as_u64())
            .unwrap_or(300);
        // build_and_test needs network for git clone, so default to true.
        let network_enabled = inputs
            .get("network_enabled")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);

        let env_vars = build_env_vars(inputs);

        let container_id = self
            .create_container(&image, &env_vars, Some("/workspace"), network_enabled)
            .await?;

        // Clone the repo using separate arguments — no shell interpolation.
        // We exec `git` directly with proper argument separation to prevent
        // injection via branch name or clone URL.
        info!(
            clone_url = clone_url.as_str(),
            branch = branch.as_str(),
            "sandbox agent: cloning repo"
        );
        let clone_result = self
            .exec_git_clone(&container_id, &clone_url, &branch, 120)
            .await;

        if let Err(e) = &clone_result {
            self.remove_container(&container_id).await;
            return Err(format!("clone failed: {e}"));
        }

        // Run the build/test command.
        let full_cmd = format!("cd /workspace && {command}");
        let result = self
            .exec_with_timeout(&container_id, &full_cmd, timeout_secs)
            .await;

        self.remove_container(&container_id).await;
        result
    }

    /// Execute a git clone inside the container using properly separated
    /// arguments to prevent shell injection via branch or URL values.
    async fn exec_git_clone(
        &self,
        container_id: &str,
        clone_url: &str,
        branch: &str,
        timeout_secs: u64,
    ) -> Result<Value, String> {
        let exec_opts = CreateExecOptions {
            cmd: Some(vec![
                "git",
                "clone",
                "--depth",
                "1",
                "--branch",
                branch,
                clone_url,
                "/workspace",
            ]),
            attach_stdout: Some(true),
            attach_stderr: Some(true),
            ..Default::default()
        };

        let exec = self
            .docker
            .create_exec(container_id, exec_opts)
            .await
            .map_err(|e| format!("create exec (git clone): {e}"))?;

        let start_result = self
            .docker
            .start_exec(&exec.id, None)
            .await
            .map_err(|e| format!("start exec (git clone): {e}"))?;

        let mut stdout = String::new();
        let mut stderr = String::new();

        let collect_output = async {
            if let StartExecResults::Attached { mut output, .. } = start_result {
                while let Some(Ok(msg)) = output.next().await {
                    match msg {
                        bollard::container::LogOutput::StdOut { message } => {
                            stdout.push_str(&String::from_utf8_lossy(&message));
                        }
                        bollard::container::LogOutput::StdErr { message } => {
                            stderr.push_str(&String::from_utf8_lossy(&message));
                        }
                        _ => {}
                    }
                }
            }
        };

        let timed_out = tokio::time::timeout(
            std::time::Duration::from_secs(timeout_secs),
            collect_output,
        )
        .await
        .is_err();

        if timed_out {
            return Err(format!("git clone timed out after {timeout_secs}s"));
        }

        let inspect = self
            .docker
            .inspect_exec(&exec.id)
            .await
            .map_err(|e| format!("inspect exec (git clone): {e}"))?;

        let exit_code = inspect.exit_code.unwrap_or(-1);
        if exit_code != 0 {
            let max_len = 64 * 1024;
            let stderr = truncate_utf8(&stderr, max_len);
            return Err(format!("git clone exit code {exit_code}\nstderr: {stderr}"));
        }

        Ok(serde_json::json!({ "cloned": true }))
    }

    async fn create_container(
        &self,
        image: &str,
        env_vars: &[String],
        working_dir: Option<&str>,
        network_enabled: bool,
    ) -> Result<String, String> {
        let name = format!("botto-wf-{}", uuid::Uuid::new_v4().simple());

        let network_mode = if network_enabled {
            None
        } else {
            Some("none".to_string())
        };

        let mut config = ContainerConfig {
            image: Some(image.to_string()),
            // Keep container alive with a long sleep so we can exec into it.
            cmd: Some(vec!["sleep".into(), "86400".into()]),
            env: Some(env_vars.to_vec()),
            host_config: Some(bollard::models::HostConfig {
                memory: Some(self.max_memory),
                memory_swap: Some(self.max_memory), // no swap
                nano_cpus: Some(self.nano_cpus),     // CPU limit
                network_mode,
                ..Default::default()
            }),
            ..Default::default()
        };

        if let Some(dir) = working_dir {
            config.working_dir = Some(dir.to_string());
        }

        let options = CreateContainerOptions {
            name: name.as_str(),
            platform: None,
        };

        info!(
            image,
            container_name = name.as_str(),
            network_enabled,
            cpu_limit = DEFAULT_CPU_LIMIT,
            memory_mb = self.max_memory / (1024 * 1024),
            "sandbox agent: creating container"
        );

        let resp = self
            .docker
            .create_container(Some(options), config)
            .await
            .map_err(|e| format!("create container: {e}"))?;

        self.docker
            .start_container(&resp.id, None::<StartContainerOptions<String>>)
            .await
            .map_err(|e| format!("start container: {e}"))?;

        info!(container_id = %resp.id, "sandbox agent: container started");
        Ok(resp.id)
    }

    async fn exec_with_timeout(
        &self,
        container_id: &str,
        command: &str,
        timeout_secs: u64,
    ) -> Result<Value, String> {
        let exec_opts = CreateExecOptions {
            cmd: Some(vec!["sh", "-c", command]),
            attach_stdout: Some(true),
            attach_stderr: Some(true),
            ..Default::default()
        };

        let exec = self
            .docker
            .create_exec(container_id, exec_opts)
            .await
            .map_err(|e| format!("create exec: {e}"))?;

        let start_result = self
            .docker
            .start_exec(&exec.id, None)
            .await
            .map_err(|e| format!("start exec: {e}"))?;

        let mut stdout = String::new();
        let mut stderr = String::new();

        let collect_output = async {
            if let StartExecResults::Attached { mut output, .. } = start_result {
                while let Some(Ok(msg)) = output.next().await {
                    match msg {
                        bollard::container::LogOutput::StdOut { message } => {
                            stdout.push_str(&String::from_utf8_lossy(&message));
                        }
                        bollard::container::LogOutput::StdErr { message } => {
                            stderr.push_str(&String::from_utf8_lossy(&message));
                        }
                        _ => {}
                    }
                }
            }
        };

        let timed_out = tokio::time::timeout(
            std::time::Duration::from_secs(timeout_secs),
            collect_output,
        )
        .await
        .is_err();

        if timed_out {
            return Err(format!("command timed out after {timeout_secs}s"));
        }

        // Check exit code.
        let inspect = self
            .docker
            .inspect_exec(&exec.id)
            .await
            .map_err(|e| format!("inspect exec: {e}"))?;

        let exit_code = inspect.exit_code.unwrap_or(-1);

        // Truncate output (UTF-8 safe).
        let max_len = 64 * 1024;
        let stdout = truncate_output(stdout, max_len);
        let stderr = truncate_output(stderr, max_len);

        if exit_code != 0 {
            return Err(format!(
                "exit code {exit_code}\nstdout: {stdout}\nstderr: {stderr}"
            ));
        }

        Ok(serde_json::json!({
            "exit_code": exit_code,
            "stdout": stdout,
            "stderr": stderr,
        }))
    }

    async fn remove_container(&self, container_id: &str) {
        let opts = RemoveContainerOptions {
            force: true,
            ..Default::default()
        };
        if let Err(e) = self.docker.remove_container(container_id, Some(opts)).await {
            warn!(container_id, error = %e, "sandbox agent: failed to remove container");
        } else {
            debug!(container_id, "sandbox agent: container removed");
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn get_str(inputs: &HashMap<String, Value>, key: &str) -> Result<String, String> {
    inputs
        .get(key)
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| format!("missing or invalid input: {key} (expected string)"))
}

fn build_env_vars(inputs: &HashMap<String, Value>) -> Vec<String> {
    inputs
        .get("env")
        .and_then(|v| v.as_object())
        .map(|env| {
            env.iter()
                .filter_map(|(k, v)| v.as_str().map(|val| format!("{k}={val}")))
                .collect()
        })
        .unwrap_or_default()
}

/// Truncate a string to at most `max_bytes` bytes, respecting UTF-8 character
/// boundaries so we never panic on multi-byte characters.
fn truncate_utf8(s: &str, max_bytes: usize) -> &str {
    if s.len() <= max_bytes {
        s
    } else {
        let mut end = max_bytes;
        // Walk backwards to find a valid char boundary.
        while end > 0 && !s.is_char_boundary(end) {
            end -= 1;
        }
        &s[..end]
    }
}

fn truncate_output(s: String, max_len: usize) -> String {
    if s.len() > max_len {
        let safe = truncate_utf8(&s, max_len);
        format!("{safe}...(truncated)")
    } else {
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncate_utf8_ascii() {
        let s = "hello world";
        assert_eq!(truncate_utf8(s, 5), "hello");
    }

    #[test]
    fn truncate_utf8_multibyte_boundary() {
        // Each CJK char is 3 bytes in UTF-8.
        let s = "\u{4e16}\u{754c}"; // "世界" — 6 bytes total
        // Cutting at 4 bytes would land inside the second char.
        let result = truncate_utf8(s, 4);
        assert_eq!(result, "\u{4e16}"); // Should back up to 3 bytes.
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn truncate_utf8_emoji() {
        let s = "a\u{1F600}b"; // 'a' (1) + emoji (4) + 'b' (1) = 6 bytes
        let result = truncate_utf8(s, 3);
        // Can't fit the emoji, so just 'a'.
        assert_eq!(result, "a");
    }

    #[test]
    fn truncate_utf8_no_truncation_needed() {
        let s = "short";
        assert_eq!(truncate_utf8(s, 100), "short");
    }
}
