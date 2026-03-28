// ---------------------------------------------------------------------------
// Script workflow agent — run shell commands on the host with resource limits.
//
// Supported actions:
//   - run: execute a shell command
//   - run_script: execute a multi-line script via a temp file
//
// Inputs:
//   - command (required for "run"): shell command string
//   - script (required for "run_script"): multi-line script content
//   - shell (optional): shell to use (default "sh", allowed: sh, bash,
//     /bin/sh, /bin/bash)
//   - timeout_secs (optional): command timeout (default 60)
//   - working_dir (optional): working directory for the command
//   - env (optional): object of environment variable key-value pairs
//
// Security:
//   - Shell binary is restricted to a known allowlist.
//   - Dangerous environment variables (PATH, LD_PRELOAD, etc.) are filtered.
//   - The orchestrator MUST enforce a policy layer for script execution —
//     this agent does NOT sandbox commands. It trusts that the orchestrator
//     has already validated the command against an allowlist or policy.
// ---------------------------------------------------------------------------

use serde_json::Value;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::time::Instant;
use tokio::process::Command;
use tracing::{debug, info, warn};

use crate::services::mentor::client::MentorClient;
use crate::services::workflow::traits::{failure_result, success_result, WorkflowAgent};
use crate::types::workflow::AgentResult;

/// Shells that are allowed for script execution.
const ALLOWED_SHELLS: &[&str] = &["sh", "bash", "/bin/sh", "/bin/bash"];

/// Environment variables that user-provided env maps must never override.
/// These can be used to hijack process behaviour or escalate privileges.
const DANGEROUS_ENV_VARS: &[&str] = &[
    "PATH",
    "LD_PRELOAD",
    "LD_LIBRARY_PATH",
    "HOME",
    "USER",
    "SHELL",
    "DYLD_INSERT_LIBRARIES",
    "DYLD_LIBRARY_PATH",
];

/// Maximum length of a command string to log (to avoid log flooding).
const MAX_COMMAND_LOG_LEN: usize = 512;

/// Script workflow agent — spawns shell processes with timeout.
pub struct ScriptAgent;

impl ScriptAgent {
    pub fn new() -> Self {
        Self
    }
}

impl Default for ScriptAgent {
    fn default() -> Self {
        Self::new()
    }
}

impl WorkflowAgent for ScriptAgent {
    fn execute<'a>(
        &'a self,
        action: &str,
        inputs: HashMap<String, Value>,
        _mentor: &'a MentorClient,
    ) -> Pin<Box<dyn Future<Output = AgentResult> + Send + 'a>> {
        let action = action.to_string();
        Box::pin(async move {
            let start = Instant::now();
            debug!(action = action.as_str(), "script agent: executing");

            let result = match action.as_str() {
                "run" => self.run_command(&inputs).await,
                "run_script" => self.run_script(&inputs).await,
                other => Err(format!("unknown script action: {other}")),
            };

            let duration = start.elapsed().as_secs_f64();
            match result {
                Ok(output) => success_result(output, duration),
                Err(e) => {
                    warn!(action = action.as_str(), error = %e, "script agent: action failed");
                    failure_result(&e, duration)
                }
            }
        })
    }

    fn agent_type_name(&self) -> &'static str {
        "script"
    }
}

// ---------------------------------------------------------------------------
// Shell validation
// ---------------------------------------------------------------------------

/// Validate that the requested shell is in the allowlist.
fn validate_shell(shell: &str) -> Result<&str, String> {
    if ALLOWED_SHELLS.contains(&shell) {
        Ok(shell)
    } else {
        warn!(
            requested_shell = shell,
            allowed = ?ALLOWED_SHELLS,
            "script agent: rejected disallowed shell"
        );
        Err(format!(
            "shell '{shell}' is not allowed; permitted shells: {}",
            ALLOWED_SHELLS.join(", ")
        ))
    }
}

/// Filter dangerous environment variables from user-provided env map.
/// Returns the sanitized set and logs any variables that were removed.
fn sanitize_env_vars(env: &serde_json::Map<String, Value>) -> Vec<(String, String)> {
    let mut result = Vec::new();
    for (key, val) in env {
        if let Some(val_str) = val.as_str() {
            let upper_key = key.to_uppercase();
            if DANGEROUS_ENV_VARS.iter().any(|d| d.eq_ignore_ascii_case(&upper_key)) {
                warn!(
                    env_var = key.as_str(),
                    "script agent: filtered dangerous environment variable"
                );
            } else {
                result.push((key.clone(), val_str.to_string()));
            }
        }
    }
    result
}

/// Truncate a string for logging purposes.
fn truncate_for_log(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        // Safe truncation at char boundary.
        let truncated = &s[..s.floor_char_boundary(max)];
        format!("{truncated}...(truncated)")
    }
}

// ---------------------------------------------------------------------------
// Execution
// ---------------------------------------------------------------------------

impl ScriptAgent {
    async fn run_command(&self, inputs: &HashMap<String, Value>) -> Result<Value, String> {
        let command = get_str(inputs, "command")?;
        let shell_input = inputs
            .get("shell")
            .and_then(|v| v.as_str())
            .unwrap_or("sh");
        let shell = validate_shell(shell_input)?;
        let timeout_secs = inputs
            .get("timeout_secs")
            .and_then(|v| v.as_u64())
            .unwrap_or(60);

        debug!(
            shell,
            command = truncate_for_log(&command, MAX_COMMAND_LOG_LEN).as_str(),
            "script agent: running command"
        );

        self.exec(shell, &["-c", &command], inputs, timeout_secs)
            .await
    }

    async fn run_script(&self, inputs: &HashMap<String, Value>) -> Result<Value, String> {
        let script = get_str(inputs, "script")?;
        let shell_input = inputs
            .get("shell")
            .and_then(|v| v.as_str())
            .unwrap_or("sh");
        let shell = validate_shell(shell_input)?;
        let timeout_secs = inputs
            .get("timeout_secs")
            .and_then(|v| v.as_u64())
            .unwrap_or(60);

        // Write script to a temp file.
        let tmp = std::env::temp_dir().join(format!("botto-script-{}", uuid::Uuid::new_v4()));
        tokio::fs::write(&tmp, &script)
            .await
            .map_err(|e| format!("write temp script: {e}"))?;

        // Make executable (async to avoid blocking the runtime).
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let perms = std::fs::Permissions::from_mode(0o755);
            tokio::fs::set_permissions(&tmp, perms)
                .await
                .map_err(|e| format!("chmod temp script: {e}"))?;
        }

        debug!(
            shell,
            script_len = script.len(),
            "script agent: running script from temp file"
        );

        let result = self
            .exec(shell, &[tmp.to_str().unwrap_or("")], inputs, timeout_secs)
            .await;

        // Clean up temp file (best-effort).
        let _ = tokio::fs::remove_file(&tmp).await;

        result
    }

    async fn exec(
        &self,
        shell: &str,
        args: &[&str],
        inputs: &HashMap<String, Value>,
        timeout_secs: u64,
    ) -> Result<Value, String> {
        let mut cmd = Command::new(shell);
        cmd.args(args);

        // Working directory.
        if let Some(dir) = inputs.get("working_dir").and_then(|v| v.as_str()) {
            cmd.current_dir(dir);
        }

        // Environment variables — filter dangerous ones.
        if let Some(env) = inputs.get("env").and_then(|v| v.as_object()) {
            let safe_vars = sanitize_env_vars(env);
            for (key, val) in &safe_vars {
                cmd.env(key, val);
            }
        }

        // Capture stdout/stderr.
        cmd.stdout(std::process::Stdio::piped());
        cmd.stderr(std::process::Stdio::piped());

        let child = cmd.spawn().map_err(|e| format!("spawn: {e}"))?;

        // Apply timeout.
        let output = tokio::time::timeout(
            std::time::Duration::from_secs(timeout_secs),
            child.wait_with_output(),
        )
        .await
        .map_err(|_| format!("command timed out after {timeout_secs}s"))?
        .map_err(|e| format!("wait: {e}"))?;

        let exit_code = output.status.code().unwrap_or(-1);
        let stdout = String::from_utf8_lossy(&output.stdout).to_string();
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();

        // Truncate output to prevent huge payloads (safe UTF-8 truncation).
        let max_len = 64 * 1024; // 64KB
        let stdout_trunc = truncate_output_safe(&stdout, max_len);
        let stderr_trunc = truncate_output_safe(&stderr, max_len);

        info!(
            exit_code,
            stdout_len = stdout.len(),
            stderr_len = stderr.len(),
            "script agent: command finished"
        );

        if exit_code != 0 {
            return Err(format!(
                "exit code {exit_code}\nstdout: {stdout_trunc}\nstderr: {stderr_trunc}"
            ));
        }

        Ok(serde_json::json!({
            "exit_code": exit_code,
            "stdout": stdout_trunc,
            "stderr": stderr_trunc,
        }))
    }
}

fn get_str(inputs: &HashMap<String, Value>, key: &str) -> Result<String, String> {
    inputs
        .get(key)
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| format!("missing or invalid input: {key} (expected string)"))
}

/// Truncate a string to at most `max_bytes` bytes, respecting UTF-8 char
/// boundaries so we never panic on multi-byte characters.
fn truncate_output_safe(s: &str, max_bytes: usize) -> String {
    if s.len() <= max_bytes {
        s.to_string()
    } else {
        let truncated = &s[..s.floor_char_boundary(max_bytes)];
        format!("{truncated}...(truncated)")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allowed_shells_accepted() {
        assert!(validate_shell("sh").is_ok());
        assert!(validate_shell("bash").is_ok());
        assert!(validate_shell("/bin/sh").is_ok());
        assert!(validate_shell("/bin/bash").is_ok());
    }

    #[test]
    fn disallowed_shells_rejected() {
        assert!(validate_shell("/usr/bin/python3").is_err());
        assert!(validate_shell("zsh").is_err());
        assert!(validate_shell("/bin/zsh").is_err());
        assert!(validate_shell("curl").is_err());
    }

    #[test]
    fn dangerous_env_vars_filtered() {
        let mut env = serde_json::Map::new();
        env.insert("PATH".into(), Value::String("/evil".into()));
        env.insert("LD_PRELOAD".into(), Value::String("/evil.so".into()));
        env.insert("MY_VAR".into(), Value::String("safe".into()));

        let result = sanitize_env_vars(&env);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, "MY_VAR");
    }

    #[test]
    fn truncate_output_safe_handles_multibyte() {
        // 3-byte UTF-8 chars
        let s = "aaaa\u{1F600}bbbb"; // emoji is 4 bytes
        let truncated = truncate_output_safe(s, 5);
        // Should not panic and should truncate at a char boundary.
        assert!(truncated.ends_with("...(truncated)"));
    }
}
