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

/// Detected project language for image selection and resource sizing.
#[derive(Debug, Clone, PartialEq)]
pub enum ProjectLang {
    Go,
    Node,       // JS/TS — both use Node runtime
    Python,
    Ruby,
    Rust,
    Java,       // also covers Kotlin/Groovy via Gradle/Maven
    Scala,
    Php,
    DotNet,     // C#, F#, VB.NET
    Swift,
    Elixir,
    Dart,       // Dart / Flutter
    Cpp,        // C / C++ (CMake, Meson, etc.)
    Zig,
    Haskell,
    Perl,
    Lua,
    R,
    Clojure,
    Terraform,  // HCL / OpenTofu
    Unknown,
}

/// Resource hints for container sizing based on language characteristics.
/// Compiled languages need more CPU/memory; interpreted ones are lighter.
#[derive(Debug, Clone)]
pub struct ResourceHints {
    /// Minimum CPU cores for reasonable build performance.
    pub min_cpus: u32,
    /// Minimum memory in MB. Compiled languages need more headroom.
    pub min_memory_mb: u64,
    /// Whether the build/test step typically needs network access
    /// (e.g. Terraform providers, NuGet restore).
    pub needs_network: bool,
    /// Estimated build time category — helps the harness set timeouts.
    pub build_speed: BuildSpeed,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BuildSpeed {
    /// Interpreted or very fast compile (Python, Ruby, Go, Zig)
    Fast,
    /// Moderate compile time (Java, C#, Scala, C++)
    Medium,
    /// Slow compile (Rust, large C++ projects, Haskell)
    Slow,
}

impl ResourceHints {
    fn for_lang(lang: &ProjectLang) -> Self {
        match lang {
            // Interpreted / fast-compile languages
            ProjectLang::Python | ProjectLang::Ruby | ProjectLang::Php
            | ProjectLang::Perl | ProjectLang::Lua | ProjectLang::R => Self {
                min_cpus: 1, min_memory_mb: 512, needs_network: false, build_speed: BuildSpeed::Fast,
            },
            ProjectLang::Node => Self {
                min_cpus: 1, min_memory_mb: 1024, needs_network: false, build_speed: BuildSpeed::Fast,
            },
            ProjectLang::Go | ProjectLang::Zig => Self {
                min_cpus: 2, min_memory_mb: 2048, needs_network: false, build_speed: BuildSpeed::Fast,
            },
            ProjectLang::Dart => Self {
                min_cpus: 2, min_memory_mb: 2048, needs_network: false, build_speed: BuildSpeed::Fast,
            },
            ProjectLang::Elixir => Self {
                min_cpus: 2, min_memory_mb: 1024, needs_network: false, build_speed: BuildSpeed::Fast,
            },
            // Medium compile languages
            ProjectLang::Java | ProjectLang::Scala => Self {
                min_cpus: 2, min_memory_mb: 4096, needs_network: false, build_speed: BuildSpeed::Medium,
            },
            ProjectLang::DotNet => Self {
                min_cpus: 2, min_memory_mb: 4096, needs_network: true, build_speed: BuildSpeed::Medium,
            },
            ProjectLang::Swift => Self {
                min_cpus: 2, min_memory_mb: 4096, needs_network: false, build_speed: BuildSpeed::Medium,
            },
            ProjectLang::Cpp => Self {
                min_cpus: 2, min_memory_mb: 2048, needs_network: false, build_speed: BuildSpeed::Medium,
            },
            ProjectLang::Clojure => Self {
                min_cpus: 2, min_memory_mb: 2048, needs_network: false, build_speed: BuildSpeed::Medium,
            },
            // Slow compile languages
            ProjectLang::Rust => Self {
                min_cpus: 4, min_memory_mb: 4096, needs_network: false, build_speed: BuildSpeed::Slow,
            },
            ProjectLang::Haskell => Self {
                min_cpus: 2, min_memory_mb: 4096, needs_network: false, build_speed: BuildSpeed::Slow,
            },
            // Infra-as-code
            ProjectLang::Terraform => Self {
                min_cpus: 1, min_memory_mb: 512, needs_network: true, build_speed: BuildSpeed::Fast,
            },
            ProjectLang::Unknown => Self {
                min_cpus: 1, min_memory_mb: 1024, needs_network: false, build_speed: BuildSpeed::Fast,
            },
        }
    }
}

/// Result of image detection — includes the Docker image tag and resource hints.
#[derive(Debug, Clone)]
pub struct ImageDetection {
    pub image: String,
    pub lang: ProjectLang,
    pub resource_hints: ResourceHints,
}

/// Determine the base Docker image for a repository.
///
/// Priority chain:
///   1. .otto.json explicit image override
///   2. Dockerfile FROM instruction
///   3. Language detection from file tree + version-aware image resolution
///   4. Fallback: ubuntu:22.04
///
/// Step 3 is version-aware: for Go projects it reads `go.mod` to get the
/// exact Go version, for Node it checks `.node-version`/`.nvmrc`/`package.json`
/// engines, etc. This avoids the AI wasting 8+ setup steps downloading and
/// installing the correct runtime version inside the container.
///
/// Returns an `ImageDetection` with the image tag, detected language, and
/// resource hints for container sizing.
pub async fn detect_base_image(
    gl_cfg: &gitlab::GitLabConfig,
    project_id: i64,
    ref_name: &str,
    otto_config: Option<&serde_json::Value>,
) -> ImageDetection {
    // 1. Check .otto.json for explicit image
    if let Some(config) = otto_config {
        if let Some(image) = config.get("sandbox").and_then(|s| s.get("image")).and_then(|i| i.as_str()) {
            debug!("base image from .otto.json: {}", image);
            return ImageDetection {
                image: image.to_string(),
                lang: ProjectLang::Unknown,
                resource_hints: ResourceHints::for_lang(&ProjectLang::Unknown),
            };
        }
    }

    // 2. Check Dockerfile for FROM instruction
    if let Ok(dockerfile) = gitlab::fetch_file_content(gl_cfg, project_id, "Dockerfile", ref_name).await {
        if let Some(image) = parse_dockerfile_from(&dockerfile) {
            // Try to infer language from the Dockerfile image name for resource hints
            let lang = infer_lang_from_image(&image);
            debug!("base image from Dockerfile: {} (inferred lang={:?})", image, lang);
            return ImageDetection {
                image,
                lang: lang.clone(),
                resource_hints: ResourceHints::for_lang(&lang),
            };
        }
    }

    // 3. Language heuristics from file tree + version-aware resolution
    if let Ok(tree) = gitlab::fetch_file_tree(gl_cfg, project_id, "", ref_name, false).await {
        let filenames: Vec<&str> = tree.iter().map(|e| e.name.as_str()).collect();
        let lang = detect_language_from_files(&filenames);

        if lang != ProjectLang::Unknown {
            let image = resolve_versioned_image(gl_cfg, project_id, ref_name, &lang).await;
            debug!("base image from version-aware detection: {} (lang={:?})", image, lang);
            return ImageDetection {
                image,
                lang: lang.clone(),
                resource_hints: ResourceHints::for_lang(&lang),
            };
        }
    }

    // 4. Fallback
    ImageDetection {
        image: "ubuntu:22.04".to_string(),
        lang: ProjectLang::Unknown,
        resource_hints: ResourceHints::for_lang(&ProjectLang::Unknown),
    }
}

/// Infer language from a Docker image name (best-effort for Dockerfile FROM).
fn infer_lang_from_image(image: &str) -> ProjectLang {
    let lower = image.to_lowercase();
    if lower.starts_with("golang:") || lower.starts_with("go:") { return ProjectLang::Go; }
    if lower.starts_with("node:") { return ProjectLang::Node; }
    if lower.starts_with("python:") { return ProjectLang::Python; }
    if lower.starts_with("ruby:") { return ProjectLang::Ruby; }
    if lower.starts_with("rust:") { return ProjectLang::Rust; }
    if lower.contains("temurin") || lower.contains("openjdk") || lower.starts_with("maven:") || lower.starts_with("gradle:") { return ProjectLang::Java; }
    if lower.starts_with("php:") { return ProjectLang::Php; }
    if lower.starts_with("mcr.microsoft.com/dotnet") || lower.starts_with("dotnet") { return ProjectLang::DotNet; }
    if lower.starts_with("swift:") { return ProjectLang::Swift; }
    if lower.starts_with("elixir:") || lower.starts_with("erlang:") { return ProjectLang::Elixir; }
    if lower.starts_with("dart:") { return ProjectLang::Dart; }
    if lower.starts_with("gcc:") || lower.starts_with("clang:") { return ProjectLang::Cpp; }
    if lower.starts_with("haskell:") { return ProjectLang::Haskell; }
    if lower.starts_with("perl:") { return ProjectLang::Perl; }
    if lower.starts_with("hashicorp/terraform") { return ProjectLang::Terraform; }
    if lower.starts_with("clojure:") { return ProjectLang::Clojure; }
    ProjectLang::Unknown
}

/// Resolve a Docker image tag with the exact runtime version the project needs.
/// Falls back to a sensible default for the language if version detection fails.
async fn resolve_versioned_image(
    gl_cfg: &gitlab::GitLabConfig,
    project_id: i64,
    ref_name: &str,
    lang: &ProjectLang,
) -> String {
    match lang {
        ProjectLang::Go => {
            if let Ok(go_mod) = gitlab::fetch_file_content(gl_cfg, project_id, "go.mod", ref_name).await {
                if let Some(version) = parse_go_version(&go_mod) {
                    info!("detected Go version {} from go.mod", version);
                    return format!("golang:{}-alpine", version);
                }
            }
            "golang:1.23-alpine".to_string()
        }
        ProjectLang::Node => {
            // .node-version → .nvmrc → package.json engines.node → .tool-versions
            if let Ok(nv) = gitlab::fetch_file_content(gl_cfg, project_id, ".node-version", ref_name).await {
                if let Some(major) = parse_node_version(&nv) {
                    info!("detected Node major version {} from .node-version", major);
                    return format!("node:{}-slim", major);
                }
            }
            if let Ok(nvmrc) = gitlab::fetch_file_content(gl_cfg, project_id, ".nvmrc", ref_name).await {
                if let Some(major) = parse_node_version(&nvmrc) {
                    info!("detected Node major version {} from .nvmrc", major);
                    return format!("node:{}-slim", major);
                }
            }
            if let Ok(pkg) = gitlab::fetch_file_content(gl_cfg, project_id, "package.json", ref_name).await {
                if let Some(major) = parse_node_engines_version(&pkg) {
                    info!("detected Node major version {} from package.json engines", major);
                    return format!("node:{}-slim", major);
                }
            }
            if let Ok(tv) = gitlab::fetch_file_content(gl_cfg, project_id, ".tool-versions", ref_name).await {
                if let Some(major) = parse_tool_versions(&tv, "nodejs") {
                    info!("detected Node major version {} from .tool-versions", major);
                    return format!("node:{}-slim", major);
                }
            }
            "node:22-slim".to_string()
        }
        ProjectLang::Python => {
            if let Ok(pv) = gitlab::fetch_file_content(gl_cfg, project_id, ".python-version", ref_name).await {
                if let Some(version) = parse_python_version(&pv) {
                    info!("detected Python version {} from .python-version", version);
                    return format!("python:{}-slim", version);
                }
            }
            if let Ok(tv) = gitlab::fetch_file_content(gl_cfg, project_id, ".tool-versions", ref_name).await {
                if let Some(version) = parse_tool_versions(&tv, "python") {
                    info!("detected Python version {} from .tool-versions", version);
                    return format!("python:{}-slim", version);
                }
            }
            "python:3.12-slim".to_string()
        }
        ProjectLang::Ruby => {
            if let Ok(rv) = gitlab::fetch_file_content(gl_cfg, project_id, ".ruby-version", ref_name).await {
                if let Some(version) = parse_ruby_version(&rv) {
                    info!("detected Ruby version {} from .ruby-version", version);
                    return format!("ruby:{}-slim", version);
                }
            }
            if let Ok(tv) = gitlab::fetch_file_content(gl_cfg, project_id, ".tool-versions", ref_name).await {
                if let Some(version) = parse_tool_versions(&tv, "ruby") {
                    info!("detected Ruby version {} from .tool-versions", version);
                    return format!("ruby:{}-slim", version);
                }
            }
            "ruby:3.3-slim".to_string()
        }
        ProjectLang::Rust => {
            if let Ok(tc) = gitlab::fetch_file_content(gl_cfg, project_id, "rust-toolchain.toml", ref_name).await {
                if let Some(version) = parse_rust_toolchain(&tc) {
                    info!("detected Rust version {} from rust-toolchain.toml", version);
                    return format!("rust:{}-slim", version);
                }
            }
            if let Ok(tc) = gitlab::fetch_file_content(gl_cfg, project_id, "rust-toolchain", ref_name).await {
                if let Some(version) = parse_rust_toolchain_plain(&tc) {
                    info!("detected Rust version {} from rust-toolchain", version);
                    return format!("rust:{}-slim", version);
                }
            }
            "rust:1.80-slim".to_string()
        }
        ProjectLang::Java => {
            // Check .java-version, .sdkmanrc, or pom.xml for Java version
            if let Ok(jv) = gitlab::fetch_file_content(gl_cfg, project_id, ".java-version", ref_name).await {
                if let Some(version) = parse_java_version(&jv) {
                    info!("detected Java version {} from .java-version", version);
                    return format!("eclipse-temurin:{}-jdk", version);
                }
            }
            if let Ok(sdk) = gitlab::fetch_file_content(gl_cfg, project_id, ".sdkmanrc", ref_name).await {
                if let Some(version) = parse_sdkmanrc_java(&sdk) {
                    info!("detected Java version {} from .sdkmanrc", version);
                    return format!("eclipse-temurin:{}-jdk", version);
                }
            }
            if let Ok(tv) = gitlab::fetch_file_content(gl_cfg, project_id, ".tool-versions", ref_name).await {
                if let Some(version) = parse_tool_versions(&tv, "java") {
                    info!("detected Java version {} from .tool-versions", version);
                    return format!("eclipse-temurin:{}-jdk", version);
                }
            }
            "eclipse-temurin:21-jdk".to_string()
        }
        ProjectLang::Scala => {
            // Scala runs on JVM — use same Java image
            "eclipse-temurin:21-jdk".to_string()
        }
        ProjectLang::DotNet => {
            // Check global.json for SDK version
            if let Ok(gj) = gitlab::fetch_file_content(gl_cfg, project_id, "global.json", ref_name).await {
                if let Some(version) = parse_dotnet_global_json(&gj) {
                    info!("detected .NET SDK version {} from global.json", version);
                    return format!("mcr.microsoft.com/dotnet/sdk:{}", version);
                }
            }
            "mcr.microsoft.com/dotnet/sdk:8.0".to_string()
        }
        ProjectLang::Swift => "swift:6.0".to_string(),
        ProjectLang::Elixir => {
            if let Ok(tv) = gitlab::fetch_file_content(gl_cfg, project_id, ".tool-versions", ref_name).await {
                if let Some(version) = parse_tool_versions(&tv, "elixir") {
                    info!("detected Elixir version {} from .tool-versions", version);
                    return format!("elixir:{}", version);
                }
            }
            "elixir:1.17".to_string()
        }
        ProjectLang::Dart => "dart:stable".to_string(),
        ProjectLang::Cpp => "gcc:14".to_string(),
        ProjectLang::Zig => "ubuntu:24.04".to_string(), // no official Zig image; AI installs via snap/curl
        ProjectLang::Haskell => "haskell:9.8".to_string(),
        ProjectLang::Perl => "perl:5.40".to_string(),
        ProjectLang::Lua => "ubuntu:24.04".to_string(), // no official Lua image
        ProjectLang::R => "r-base:4.4.0".to_string(),
        ProjectLang::Clojure => "clojure:temurin-21-tools-deps".to_string(),
        ProjectLang::Php => {
            if let Ok(cj) = gitlab::fetch_file_content(gl_cfg, project_id, "composer.json", ref_name).await {
                if let Some(version) = parse_php_version(&cj) {
                    info!("detected PHP version {} from composer.json", version);
                    return format!("php:{}-cli", version);
                }
            }
            "php:8.3-cli".to_string()
        }
        ProjectLang::Terraform => "hashicorp/terraform:latest".to_string(),
        ProjectLang::Unknown => "ubuntu:22.04".to_string(),
    }
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
            let after_from = trimmed[5..].trim();
            // Skip --platform=linux/amd64 and other flags before the image name.
            // Docker syntax: FROM [--platform=<platform>] <image> [AS <name>]
            let image = after_from
                .split_whitespace()
                .find(|token| !token.starts_with("--"))
                .unwrap_or(after_from);
            if !image.is_empty() && image != "scratch" {
                return Some(image.to_string());
            }
        }
    }
    None
}

/// Detect project language from root-level filenames.
fn detect_language_from_files(filenames: &[&str]) -> ProjectLang {
    for name in filenames {
        match *name {
            // Go
            "go.mod" | "go.sum" => return ProjectLang::Go,
            // Node / JS / TS (React, Vue, Svelte, etc. all use Node)
            "package.json" | "package-lock.json" | "yarn.lock" | "pnpm-lock.yaml" | "bun.lockb" => {
                return ProjectLang::Node
            }
            // Python
            "requirements.txt" | "pyproject.toml" | "setup.py" | "Pipfile" | "poetry.lock"
            | "setup.cfg" | "tox.ini" => return ProjectLang::Python,
            // Ruby
            "Gemfile" | "Gemfile.lock" => return ProjectLang::Ruby,
            // Rust
            "Cargo.toml" | "Cargo.lock" => return ProjectLang::Rust,
            // Java / Kotlin / Groovy (JVM)
            "pom.xml" | "build.gradle" | "build.gradle.kts" | "gradlew" | "mvnw" => {
                return ProjectLang::Java
            }
            // Scala (also JVM but distinct tooling)
            "build.sbt" => return ProjectLang::Scala,
            // PHP
            "composer.json" | "composer.lock" => return ProjectLang::Php,
            // .NET (C#, F#, VB.NET)
            "global.json" | "Directory.Build.props" => return ProjectLang::DotNet,
            // Swift
            "Package.swift" => return ProjectLang::Swift,
            // Elixir
            "mix.exs" | "mix.lock" => return ProjectLang::Elixir,
            // Dart / Flutter
            "pubspec.yaml" | "pubspec.lock" => return ProjectLang::Dart,
            // C / C++
            "CMakeLists.txt" | "Makefile.am" | "meson.build" | "conanfile.txt" | "conanfile.py"
            | "vcpkg.json" => return ProjectLang::Cpp,
            // Zig
            "build.zig" | "build.zig.zon" => return ProjectLang::Zig,
            // Haskell
            "stack.yaml" | "cabal.project" => return ProjectLang::Haskell,
            // Perl
            "cpanfile" | "Makefile.PL" | "Build.PL" | "dist.ini" => return ProjectLang::Perl,
            // Lua
            "rockspec" => return ProjectLang::Lua,
            // R
            "DESCRIPTION" => {
                // DESCRIPTION is also used by other ecosystems; check for R-specific content
                // For now, only match if there's also a NAMESPACE file (checked below)
            }
            // Clojure
            "project.clj" | "deps.edn" => return ProjectLang::Clojure,
            // Terraform / OpenTofu
            "main.tf" | "terraform.tf" | "versions.tf" => return ProjectLang::Terraform,
            _ => {}
        }
    }

    // Second pass for files that need context (e.g. .csproj, .fsproj, .sln, .cabal, .tf)
    let has_namespace = filenames.iter().any(|n| *n == "NAMESPACE");
    let has_description = filenames.iter().any(|n| *n == "DESCRIPTION");
    if has_namespace && has_description {
        return ProjectLang::R;
    }

    // Check for .sln or .csproj/.fsproj in root (common for .NET)
    for name in filenames {
        let lower = name.to_lowercase();
        if lower.ends_with(".sln") || lower.ends_with(".csproj") || lower.ends_with(".fsproj")
            || lower.ends_with(".vbproj")
        {
            return ProjectLang::DotNet;
        }
        if lower.ends_with(".cabal") {
            return ProjectLang::Haskell;
        }
        if lower.ends_with(".tf") {
            return ProjectLang::Terraform;
        }
        if lower.ends_with(".rockspec") {
            return ProjectLang::Lua;
        }
    }

    ProjectLang::Unknown
}

// ---------------------------------------------------------------------------
// Version parsers — extract runtime versions from project config files.
//
// Each parser is lenient: returns None on any parse failure so the caller
// falls back to a sensible default image. We never panic or propagate
// errors from malformed project files.
// ---------------------------------------------------------------------------

/// Parse the `go X.Y` or `go X.Y.Z` directive from go.mod content.
/// Returns the major.minor version (e.g. "1.25") suitable for Docker tags.
fn parse_go_version(go_mod: &str) -> Option<String> {
    for line in go_mod.lines() {
        let trimmed = line.trim();
        // Match "go 1.25" or "go 1.25.3" — the directive is always at the start of a line
        if let Some(rest) = trimmed.strip_prefix("go ") {
            let version = rest.trim();
            // Validate it looks like a version number
            if version.starts_with(|c: char| c.is_ascii_digit()) {
                // Return major.minor only (Docker tags use golang:1.25, not golang:1.25.3)
                let parts: Vec<&str> = version.split('.').collect();
                if parts.len() >= 2 {
                    return Some(format!("{}.{}", parts[0], parts[1]));
                }
            }
        }
    }
    None
}

/// Parse a Node.js major version from .node-version or .nvmrc content.
/// These files contain lines like "22", "v22.1.0", "lts/iron", "22.x", etc.
/// Returns just the major version number (e.g. "22").
fn parse_node_version(content: &str) -> Option<String> {
    let trimmed = content.trim().trim_start_matches('v');
    // Handle "lts/*" or "lts/iron" — fall back to default
    if trimmed.starts_with("lts") {
        return None;
    }
    // Extract major version from "22", "22.1.0", "22.x", etc.
    let major = trimmed.split('.').next()?.trim();
    if major.chars().all(|c| c.is_ascii_digit()) && !major.is_empty() {
        Some(major.to_string())
    } else {
        None
    }
}

/// Parse Node.js major version from package.json "engines.node" field.
/// Handles ranges like ">=18", "^20", "22.x", ">=18.0.0 <23".
/// Returns the minimum major version.
fn parse_node_engines_version(package_json: &str) -> Option<String> {
    let parsed: serde_json::Value = serde_json::from_str(package_json).ok()?;
    let engine_node = parsed.get("engines")?.get("node")?.as_str()?;

    // Strip range operators and grab the first version-like token
    let cleaned = engine_node
        .replace(">=", "")
        .replace("<=", "")
        .replace('>', "")
        .replace('<', "")
        .replace('^', "")
        .replace('~', "")
        .replace('=', "");

    // Take the first space-separated token that starts with a digit
    for token in cleaned.split_whitespace() {
        let trimmed = token.trim().trim_start_matches('v');
        if let Some(major) = trimmed.split('.').next() {
            if major.chars().all(|c| c.is_ascii_digit()) && !major.is_empty() {
                return Some(major.to_string());
            }
        }
    }
    None
}

/// Parse Python version from .python-version content.
/// Returns major.minor (e.g. "3.12").
fn parse_python_version(content: &str) -> Option<String> {
    let trimmed = content.trim();
    let parts: Vec<&str> = trimmed.split('.').collect();
    if parts.len() >= 2
        && parts[0].chars().all(|c| c.is_ascii_digit())
        && parts[1].chars().all(|c| c.is_ascii_digit())
    {
        Some(format!("{}.{}", parts[0], parts[1]))
    } else {
        None
    }
}

/// Parse Ruby version from .ruby-version content.
/// Returns major.minor (e.g. "3.3").
fn parse_ruby_version(content: &str) -> Option<String> {
    let trimmed = content.trim().trim_start_matches("ruby-");
    let parts: Vec<&str> = trimmed.split('.').collect();
    if parts.len() >= 2
        && parts[0].chars().all(|c| c.is_ascii_digit())
        && parts[1].chars().all(|c| c.is_ascii_digit())
    {
        Some(format!("{}.{}", parts[0], parts[1]))
    } else {
        None
    }
}

/// Parse Rust version from rust-toolchain.toml (TOML format).
/// Looks for `channel = "1.80"` or `channel = "stable"`.
fn parse_rust_toolchain(content: &str) -> Option<String> {
    // Simple line-based parse — avoid pulling in a TOML parser just for this
    for line in content.lines() {
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix("channel") {
            let rest = rest.trim().strip_prefix('=')?.trim();
            let version = rest.trim_matches('"').trim_matches('\'');
            // "stable", "nightly", "beta" → no specific version
            if version == "stable" || version == "nightly" || version == "beta" {
                return None;
            }
            if version.starts_with(|c: char| c.is_ascii_digit()) {
                return Some(version.to_string());
            }
        }
    }
    None
}

/// Parse Rust version from plain rust-toolchain file (just a version string).
fn parse_rust_toolchain_plain(content: &str) -> Option<String> {
    let trimmed = content.trim();
    if trimmed == "stable" || trimmed == "nightly" || trimmed == "beta" {
        return None;
    }
    if trimmed.starts_with(|c: char| c.is_ascii_digit()) && trimmed.contains('.') {
        Some(trimmed.to_string())
    } else {
        None
    }
}

/// Parse a version from .tool-versions (asdf/mise format).
/// Format: `<tool> <version>` per line, e.g. `nodejs 22.1.0`, `python 3.12.3`.
/// Returns major.minor for the requested tool name.
fn parse_tool_versions(content: &str, tool: &str) -> Option<String> {
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let mut parts = trimmed.split_whitespace();
        if let (Some(name), Some(version)) = (parts.next(), parts.next()) {
            if name == tool {
                // Return major.minor
                let vparts: Vec<&str> = version.split('.').collect();
                if vparts.len() >= 2
                    && vparts[0].chars().all(|c| c.is_ascii_digit())
                    && vparts[1].chars().all(|c| c.is_ascii_digit())
                {
                    return Some(format!("{}.{}", vparts[0], vparts[1]));
                }
                // Single number (e.g. "nodejs 22")
                if version.chars().all(|c| c.is_ascii_digit()) && !version.is_empty() {
                    return Some(version.to_string());
                }
            }
        }
    }
    None
}

/// Parse Java major version from .java-version file.
/// Content is typically "21", "17", "11", or "21.0.2".
fn parse_java_version(content: &str) -> Option<String> {
    let trimmed = content.trim();
    let major = trimmed.split('.').next()?.trim();
    if major.chars().all(|c| c.is_ascii_digit()) && !major.is_empty() {
        Some(major.to_string())
    } else {
        None
    }
}

/// Parse Java version from .sdkmanrc file.
/// Format: `java=21.0.2-tem` or `java=17.0.9-zulu`.
fn parse_sdkmanrc_java(content: &str) -> Option<String> {
    for line in content.lines() {
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix("java=") {
            let version_part = rest.split('-').next()?.trim();
            let major = version_part.split('.').next()?.trim();
            if major.chars().all(|c| c.is_ascii_digit()) && !major.is_empty() {
                return Some(major.to_string());
            }
        }
    }
    None
}

/// Parse .NET SDK version from global.json.
/// Format: `{"sdk":{"version":"8.0.100"}}`.
/// Returns major.minor (e.g. "8.0").
fn parse_dotnet_global_json(content: &str) -> Option<String> {
    let parsed: serde_json::Value = serde_json::from_str(content).ok()?;
    let version = parsed.get("sdk")?.get("version")?.as_str()?;
    let parts: Vec<&str> = version.split('.').collect();
    if parts.len() >= 2 {
        Some(format!("{}.{}", parts[0], parts[1]))
    } else {
        None
    }
}

/// Parse PHP version from composer.json "require.php" field.
/// Handles ranges like ">=8.1", "^8.2", "~8.3".
fn parse_php_version(composer_json: &str) -> Option<String> {
    let parsed: serde_json::Value = serde_json::from_str(composer_json).ok()?;
    let php_req = parsed.get("require")?.get("php")?.as_str()?;
    let cleaned = php_req
        .replace(">=", "")
        .replace("<=", "")
        .replace('^', "")
        .replace('~', "")
        .replace('>', "")
        .replace('<', "")
        .replace('|', " ")
        .replace("||", " ");
    for token in cleaned.split_whitespace() {
        let trimmed = token.trim();
        let parts: Vec<&str> = trimmed.split('.').collect();
        if parts.len() >= 2
            && parts[0].chars().all(|c| c.is_ascii_digit())
            && parts[1].chars().all(|c| c.is_ascii_digit())
        {
            return Some(format!("{}.{}", parts[0], parts[1]));
        }
    }
    None
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
        // --platform flag should be skipped
        assert_eq!(
            parse_dockerfile_from("FROM --platform=linux/amd64 golang:1.23-alpine AS builder"),
            Some("golang:1.23-alpine".to_string())
        );
        assert_eq!(
            parse_dockerfile_from("FROM --platform=$BUILDPLATFORM node:22-slim"),
            Some("node:22-slim".to_string())
        );
    }

    #[test]
    fn test_detect_language_from_files() {
        assert_eq!(detect_language_from_files(&["package.json", "README.md"]), ProjectLang::Node);
        assert_eq!(detect_language_from_files(&["go.mod", "main.go"]), ProjectLang::Go);
        assert_eq!(detect_language_from_files(&["README.md", "LICENSE"]), ProjectLang::Unknown);
        assert_eq!(detect_language_from_files(&["Cargo.toml", "src"]), ProjectLang::Rust);
        // New languages
        assert_eq!(detect_language_from_files(&["mix.exs", "lib"]), ProjectLang::Elixir);
        assert_eq!(detect_language_from_files(&["pubspec.yaml"]), ProjectLang::Dart);
        assert_eq!(detect_language_from_files(&["CMakeLists.txt", "src"]), ProjectLang::Cpp);
        assert_eq!(detect_language_from_files(&["build.zig"]), ProjectLang::Zig);
        assert_eq!(detect_language_from_files(&["build.sbt"]), ProjectLang::Scala);
        assert_eq!(detect_language_from_files(&["global.json", "src"]), ProjectLang::DotNet);
        assert_eq!(detect_language_from_files(&["Package.swift"]), ProjectLang::Swift);
        assert_eq!(detect_language_from_files(&["stack.yaml"]), ProjectLang::Haskell);
        assert_eq!(detect_language_from_files(&["cpanfile"]), ProjectLang::Perl);
        assert_eq!(detect_language_from_files(&["project.clj"]), ProjectLang::Clojure);
        assert_eq!(detect_language_from_files(&["main.tf"]), ProjectLang::Terraform);
        assert_eq!(detect_language_from_files(&["gradlew", "build.gradle"]), ProjectLang::Java);
        assert_eq!(detect_language_from_files(&["bun.lockb"]), ProjectLang::Node);
        assert_eq!(detect_language_from_files(&["poetry.lock", "pyproject.toml"]), ProjectLang::Python);
        // .sln in second pass
        assert_eq!(detect_language_from_files(&["MyApp.sln", "README.md"]), ProjectLang::DotNet);
        assert_eq!(detect_language_from_files(&["MyApp.csproj"]), ProjectLang::DotNet);
        // .cabal in second pass
        assert_eq!(detect_language_from_files(&["mylib.cabal"]), ProjectLang::Haskell);
        // .tf in second pass
        assert_eq!(detect_language_from_files(&["infra.tf"]), ProjectLang::Terraform);
        // R needs both DESCRIPTION and NAMESPACE
        assert_eq!(detect_language_from_files(&["DESCRIPTION", "NAMESPACE"]), ProjectLang::R);
        assert_eq!(detect_language_from_files(&["DESCRIPTION"]), ProjectLang::Unknown);
    }

    #[test]
    fn test_parse_go_version() {
        assert_eq!(
            parse_go_version("module gitlab.com/foo/bar\n\ngo 1.25\n\nrequire (\n"),
            Some("1.25".to_string())
        );
        assert_eq!(
            parse_go_version("module example.com/x\n\ngo 1.22.3\n"),
            Some("1.22".to_string())
        );
        assert_eq!(
            parse_go_version("module x\n\ngo 1.23\n\ntoolchain go1.23.4\n"),
            Some("1.23".to_string())
        );
        assert_eq!(parse_go_version("module x\n\nrequire (\n"), None);
    }

    #[test]
    fn test_parse_node_version() {
        assert_eq!(parse_node_version("22\n"), Some("22".to_string()));
        assert_eq!(parse_node_version("v22.1.0\n"), Some("22".to_string()));
        assert_eq!(parse_node_version("  20  \n"), Some("20".to_string()));
        assert_eq!(parse_node_version("22.x"), Some("22".to_string()));
        assert_eq!(parse_node_version("lts/iron"), None);
        assert_eq!(parse_node_version("lts/*"), None);
    }

    #[test]
    fn test_parse_node_engines_version() {
        assert_eq!(
            parse_node_engines_version(r#"{"engines":{"node":">=18"}}"#),
            Some("18".to_string())
        );
        assert_eq!(
            parse_node_engines_version(r#"{"engines":{"node":"^20.0.0"}}"#),
            Some("20".to_string())
        );
        assert_eq!(
            parse_node_engines_version(r#"{"engines":{"node":">=18.0.0 <23"}}"#),
            Some("18".to_string())
        );
        assert_eq!(
            parse_node_engines_version(r#"{"name":"foo","version":"1.0.0"}"#),
            None
        );
    }

    #[test]
    fn test_parse_python_version() {
        assert_eq!(parse_python_version("3.12\n"), Some("3.12".to_string()));
        assert_eq!(parse_python_version("3.11.5"), Some("3.11".to_string()));
        assert_eq!(parse_python_version("  3.13  "), Some("3.13".to_string()));
        assert_eq!(parse_python_version("pypy3"), None);
    }

    #[test]
    fn test_parse_ruby_version() {
        assert_eq!(parse_ruby_version("3.3.0\n"), Some("3.3".to_string()));
        assert_eq!(parse_ruby_version("ruby-3.2.1"), Some("3.2".to_string()));
        assert_eq!(parse_ruby_version("  3.1  "), Some("3.1".to_string()));
    }

    #[test]
    fn test_parse_rust_toolchain() {
        assert_eq!(
            parse_rust_toolchain("[toolchain]\nchannel = \"1.80\"\n"),
            Some("1.80".to_string())
        );
        assert_eq!(
            parse_rust_toolchain("[toolchain]\nchannel = \"1.82.0\"\n"),
            Some("1.82.0".to_string())
        );
        assert_eq!(parse_rust_toolchain("[toolchain]\nchannel = \"stable\"\n"), None);
        assert_eq!(parse_rust_toolchain("[toolchain]\nchannel = \"nightly\"\n"), None);
    }

    #[test]
    fn test_parse_rust_toolchain_plain() {
        assert_eq!(parse_rust_toolchain_plain("1.80.0\n"), Some("1.80.0".to_string()));
        assert_eq!(parse_rust_toolchain_plain("stable\n"), None);
        assert_eq!(parse_rust_toolchain_plain("nightly\n"), None);
    }

    #[test]
    fn test_parse_tool_versions() {
        let content = "nodejs 22.1.0\npython 3.12.3\nruby 3.3.0\n";
        assert_eq!(parse_tool_versions(content, "nodejs"), Some("22.1".to_string()));
        assert_eq!(parse_tool_versions(content, "python"), Some("3.12".to_string()));
        assert_eq!(parse_tool_versions(content, "ruby"), Some("3.3".to_string()));
        assert_eq!(parse_tool_versions(content, "golang"), None);
        // Single major version
        assert_eq!(parse_tool_versions("nodejs 22\n", "nodejs"), Some("22".to_string()));
        // Comments and blank lines
        assert_eq!(parse_tool_versions("# comment\n\nnodejs 20.5.1\n", "nodejs"), Some("20.5".to_string()));
    }

    #[test]
    fn test_parse_java_version() {
        assert_eq!(parse_java_version("21\n"), Some("21".to_string()));
        assert_eq!(parse_java_version("17.0.9"), Some("17".to_string()));
        assert_eq!(parse_java_version("  11  "), Some("11".to_string()));
    }

    #[test]
    fn test_parse_sdkmanrc_java() {
        assert_eq!(
            parse_sdkmanrc_java("java=21.0.2-tem\ngradle=8.5\n"),
            Some("21".to_string())
        );
        assert_eq!(
            parse_sdkmanrc_java("java=17.0.9-zulu\n"),
            Some("17".to_string())
        );
        assert_eq!(parse_sdkmanrc_java("gradle=8.5\n"), None);
    }

    #[test]
    fn test_parse_dotnet_global_json() {
        assert_eq!(
            parse_dotnet_global_json(r#"{"sdk":{"version":"8.0.100"}}"#),
            Some("8.0".to_string())
        );
        assert_eq!(
            parse_dotnet_global_json(r#"{"sdk":{"version":"9.0.100-preview.1"}}"#),
            Some("9.0".to_string())
        );
        assert_eq!(parse_dotnet_global_json(r#"{"msbuild":{}}"#), None);
    }

    #[test]
    fn test_parse_php_version() {
        assert_eq!(
            parse_php_version(r#"{"require":{"php":">=8.2"}}"#),
            Some("8.2".to_string())
        );
        assert_eq!(
            parse_php_version(r#"{"require":{"php":"^8.1"}}"#),
            Some("8.1".to_string())
        );
        assert_eq!(
            parse_php_version(r#"{"require":{"php":">=8.1 <8.4"}}"#),
            Some("8.1".to_string())
        );
        assert_eq!(
            parse_php_version(r#"{"require":{"laravel/framework":"^11.0"}}"#),
            None
        );
    }

    #[test]
    fn test_infer_lang_from_image() {
        assert_eq!(infer_lang_from_image("golang:1.23-alpine"), ProjectLang::Go);
        assert_eq!(infer_lang_from_image("node:22-slim"), ProjectLang::Node);
        assert_eq!(infer_lang_from_image("python:3.12"), ProjectLang::Python);
        assert_eq!(infer_lang_from_image("mcr.microsoft.com/dotnet/sdk:8.0"), ProjectLang::DotNet);
        assert_eq!(infer_lang_from_image("eclipse-temurin:21-jdk"), ProjectLang::Java);
        assert_eq!(infer_lang_from_image("swift:6.0"), ProjectLang::Swift);
        assert_eq!(infer_lang_from_image("ubuntu:22.04"), ProjectLang::Unknown);
    }

    #[test]
    fn test_resource_hints() {
        let py = ResourceHints::for_lang(&ProjectLang::Python);
        assert_eq!(py.min_cpus, 1);
        assert_eq!(py.min_memory_mb, 512);
        assert_eq!(py.build_speed, BuildSpeed::Fast);

        let rust = ResourceHints::for_lang(&ProjectLang::Rust);
        assert_eq!(rust.min_cpus, 4);
        assert_eq!(rust.min_memory_mb, 4096);
        assert_eq!(rust.build_speed, BuildSpeed::Slow);

        let dotnet = ResourceHints::for_lang(&ProjectLang::DotNet);
        assert!(dotnet.needs_network); // NuGet restore

        let tf = ResourceHints::for_lang(&ProjectLang::Terraform);
        assert!(tf.needs_network); // provider downloads
    }
}
