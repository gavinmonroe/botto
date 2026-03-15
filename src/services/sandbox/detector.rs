// ---------------------------------------------------------------------------
// Sandbox detector — auto-detects runtime capabilities for fix execution.
//
// Probes:
//   - Docker availability (socket + ping)
//   - System resources (CPU, memory, disk)
//   - Base image detection from repo (.otto.json → Dockerfile → heuristics)
// ---------------------------------------------------------------------------

use crate::services::gitlab::client as gitlab;
use tracing::{debug, info};

/// Detected capabilities of the sandbox environment.
#[derive(Debug, Clone)]
pub struct SandboxCapabilities {
    pub docker_available: bool,
    pub cpu_cores: u32,
    pub memory_mb: u64,
    pub disk_free_mb: u64,
    pub max_concurrent: u32,
    pub max_memory_per_container_mb: u64,
}

/// Detect sandbox capabilities on the current host.
pub async fn detect() -> SandboxCapabilities {
    let docker_available = probe_docker().await;
    let sys = sysinfo::System::new_all();
    let cpu_cores = sys.cpus().len() as u32;
    let memory_mb = sys.total_memory() / (1024 * 1024);

    // Disk free — check the data directory
    // Fallback to a conservative estimate if we can't determine
    let disk_free_mb = 10_000; // 10GB default assumption

    let max_concurrent = ((cpu_cores / 2).max(1)).min(4);
    let max_memory_per_container_mb = (memory_mb / 4).min(2048).max(512);

    let caps = SandboxCapabilities {
        docker_available,
        cpu_cores,
        memory_mb,
        disk_free_mb,
        max_concurrent,
        max_memory_per_container_mb,
    };

    info!(
        "sandbox capabilities: docker={}, cpus={}, mem={}MB, max_concurrent={}, per_container={}MB",
        caps.docker_available, caps.cpu_cores, caps.memory_mb,
        caps.max_concurrent, caps.max_memory_per_container_mb
    );

    caps
}

async fn probe_docker() -> bool {
    match bollard::Docker::connect_with_local_defaults() {
        Ok(docker) => docker.ping().await.is_ok(),
        Err(_) => false,
    }
}

/// Strategy for running a fix in the sandbox.
#[derive(Debug, Clone, PartialEq)]
pub enum FixStrategy {
    /// Full setup: clone, install deps, apply fix, run full test suite, push.
    FullSetup,
    /// Test only: clone (sparse), apply fix, run relevant tests only, push.
    TestOnly,
}

/// Determine the base Docker image for a repository.
/// Priority: .otto.json → Dockerfile → language heuristics from file extensions.
pub async fn detect_base_image(
    gl_cfg: &gitlab::GitLabConfig,
    project_id: i64,
    ref_name: &str,
    otto_config: Option<&serde_json::Value>,
) -> String {
    // 1. Check .otto.json for explicit image
    if let Some(config) = otto_config {
        if let Some(image) = config.get("sandbox").and_then(|s| s.get("image")).and_then(|i| i.as_str()) {
            debug!("base image from .otto.json: {}", image);
            return image.to_string();
        }
    }

    // 2. Check Dockerfile for FROM instruction
    if let Ok(dockerfile) = gitlab::fetch_file_content(gl_cfg, project_id, "Dockerfile", ref_name).await {
        if let Some(image) = parse_dockerfile_from(&dockerfile) {
            debug!("base image from Dockerfile: {}", image);
            return image;
        }
    }

    // 3. Language heuristics from file tree
    if let Ok(tree) = gitlab::fetch_file_tree(gl_cfg, project_id, "", ref_name, false).await {
        let filenames: Vec<&str> = tree.iter().map(|e| e.name.as_str()).collect();
        let image = detect_image_from_files(&filenames);
        debug!("base image from heuristics: {}", image);
        return image;
    }

    // 4. Fallback
    "ubuntu:22.04".to_string()
}

/// Determine fix strategy based on repo size and setup guide availability.
pub async fn determine_strategy(
    gl_cfg: &gitlab::GitLabConfig,
    project_id: i64,
    ref_name: &str,
    max_memory_mb: u64,
) -> FixStrategy {
    // Check for setup indicators
    let has_setup = has_setup_guide(gl_cfg, project_id, ref_name).await;

    // Check repo size (if available from project metadata)
    // For now, default to TestOnly for safety; FullSetup when setup guide exists
    // and resources are sufficient
    if has_setup && max_memory_mb >= 1024 {
        FixStrategy::FullSetup
    } else {
        FixStrategy::TestOnly
    }
}

/// Check if the repo has a setup guide we can follow.
async fn has_setup_guide(
    gl_cfg: &gitlab::GitLabConfig,
    project_id: i64,
    ref_name: &str,
) -> bool {
    let setup_files = ["Makefile", "docker-compose.yml", "docker-compose.yaml"];

    for file in &setup_files {
        if gitlab::fetch_file_content(gl_cfg, project_id, file, ref_name)
            .await
            .is_ok()
        {
            return true;
        }
    }

    false
}

/// Parse the FROM instruction from a Dockerfile.
fn parse_dockerfile_from(content: &str) -> Option<String> {
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.to_uppercase().starts_with("FROM ") {
            let image = trimmed[5..].trim();
            // Strip "AS builder" etc.
            let image = image.split_whitespace().next().unwrap_or(image);
            if !image.is_empty() && image != "scratch" {
                return Some(image.to_string());
            }
        }
    }
    None
}

/// Detect base image from root-level filenames.
fn detect_image_from_files(filenames: &[&str]) -> String {
    // Check for language-specific files
    for name in filenames {
        match *name {
            "package.json" | "package-lock.json" | "yarn.lock" | "pnpm-lock.yaml" => {
                return "node:22-slim".to_string()
            }
            "requirements.txt" | "pyproject.toml" | "setup.py" | "Pipfile" => {
                return "python:3.12-slim".to_string()
            }
            "go.mod" | "go.sum" => return "golang:1.22-alpine".to_string(),
            "Gemfile" | "Gemfile.lock" => return "ruby:3.3-slim".to_string(),
            "Cargo.toml" | "Cargo.lock" => return "rust:1.80-slim".to_string(),
            "pom.xml" | "build.gradle" | "build.gradle.kts" => {
                return "eclipse-temurin:21-jdk".to_string()
            }
            "composer.json" => return "php:8.3-cli".to_string(),
            _ => {}
        }
    }

    "ubuntu:22.04".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_dockerfile_from() {
        assert_eq!(
            parse_dockerfile_from("FROM node:20-alpine AS builder\nRUN npm install"),
            Some("node:20-alpine".to_string())
        );
        assert_eq!(
            parse_dockerfile_from("# comment\nFROM python:3.12"),
            Some("python:3.12".to_string())
        );
        assert_eq!(parse_dockerfile_from("RUN echo hello"), None);
    }

    #[test]
    fn test_detect_image_from_files() {
        assert_eq!(
            detect_image_from_files(&["package.json", "README.md"]),
            "node:22-slim"
        );
        assert_eq!(
            detect_image_from_files(&["go.mod", "main.go"]),
            "golang:1.22-alpine"
        );
        assert_eq!(
            detect_image_from_files(&["README.md", "LICENSE"]),
            "ubuntu:22.04"
        );
    }
}
