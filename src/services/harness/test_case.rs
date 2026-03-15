// ---------------------------------------------------------------------------
// Harness test case — generation, management, and diverse selection of test
// cases from GitLab MRs.
//
// Key design principle: test cases MUST be diverse across:
//   - Programming languages (Go, Python, Ruby, JS/TS, Rust, Java, etc.)
//   - Issue types (bugs, race conditions, error handling, security, perf, etc.)
//   - Difficulty levels (easy, medium, hard)
//   - Project types (CLI tools, web services, libraries, infra)
//
// The harness must produce a generalist senior engineer, not one overfit
// to a single language or issue pattern.
//
// DYNAMIC DISCOVERY FLOW:
//   1. Fetch projects from gitlab-org group
//   2. For each project, fetch recent merged MRs
//   3. For each MR, fetch discussions (review comments)
//   4. Filter for code-level comments with suggestions/bugs
//   5. Fetch the file content at the MR's source branch
//   6. Build a TestCase with real code, real review, real context
// ---------------------------------------------------------------------------

use crate::config::BottoConfig;
use crate::services::gitlab::client::{self as gl, GitLabConfig};
use crate::services::harness::types::{Difficulty, TestCase};
use crate::services::review::orchestrator;
use crate::types::review::{DiffFileData, MrContext, ReviewComment, ReviewCommentSeverity};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

/// Language tag for diversity tracking.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum Language {
    Go,
    Python,
    Ruby,
    JavaScript,
    TypeScript,
    Rust,
    Java,
    Other(String),
}

impl std::fmt::Display for Language {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Language::Go => write!(f, "go"),
            Language::Python => write!(f, "python"),
            Language::Ruby => write!(f, "ruby"),
            Language::JavaScript => write!(f, "javascript"),
            Language::TypeScript => write!(f, "typescript"),
            Language::Rust => write!(f, "rust"),
            Language::Java => write!(f, "java"),
            Language::Other(s) => write!(f, "{}", s),
        }
    }
}

/// Issue category for diversity tracking.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum IssueCategory {
    ErrorHandling,
    RaceCondition,
    NullSafety,
    Security,
    Performance,
    LogicBug,
    ResourceLeak,
    TypeSafety,
    BoundaryCondition,
    ApiMisuse,
}

impl std::fmt::Display for IssueCategory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IssueCategory::ErrorHandling => write!(f, "error_handling"),
            IssueCategory::RaceCondition => write!(f, "race_condition"),
            IssueCategory::NullSafety => write!(f, "null_safety"),
            IssueCategory::Security => write!(f, "security"),
            IssueCategory::Performance => write!(f, "performance"),
            IssueCategory::LogicBug => write!(f, "logic_bug"),
            IssueCategory::ResourceLeak => write!(f, "resource_leak"),
            IssueCategory::TypeSafety => write!(f, "type_safety"),
            IssueCategory::BoundaryCondition => write!(f, "boundary_condition"),
            IssueCategory::ApiMisuse => write!(f, "api_misuse"),
        }
    }
}

/// Extended test case metadata for diversity-aware selection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestCaseMeta {
    pub language: Language,
    pub issue_category: IssueCategory,
}

/// Infer language from file extension.
pub fn infer_language(file_path: &str) -> Language {
    let ext = file_path.rsplit('.').next().unwrap_or("");
    match ext {
        "go" => Language::Go,
        "py" => Language::Python,
        "rb" => Language::Ruby,
        "js" | "jsx" | "mjs" => Language::JavaScript,
        "ts" | "tsx" => Language::TypeScript,
        "rs" => Language::Rust,
        "java" | "kt" => Language::Java,
        other => Language::Other(other.to_string()),
    }
}

/// Select a diverse subset of test cases for a harness run.
/// Ensures variety across languages and issue categories by:
/// 1. Grouping by language
/// 2. Round-robin picking from each language group
/// 3. Within each group, preferring different issue categories
///
/// Uses a simple seed for reproducibility within a round, but different
/// rounds get different seeds so we don't always pick the same set.
pub fn select_diverse(cases: &[TestCase], count: usize, round_seed: u64) -> Vec<TestCase> {
    if cases.len() <= count {
        return cases.to_vec();
    }

    // Simple deterministic shuffle using the round seed
    let mut indexed: Vec<(usize, &TestCase)> = cases.iter().enumerate().collect();
    // Fisher-Yates with a simple LCG PRNG
    let mut rng_state = round_seed;
    let lcg_next = |state: &mut u64| -> u64 {
        *state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        *state >> 33
    };

    for i in (1..indexed.len()).rev() {
        let j = lcg_next(&mut rng_state) as usize % (i + 1);
        indexed.swap(i, j);
    }

    // Group by language
    let mut by_language: std::collections::HashMap<String, Vec<&TestCase>> =
        std::collections::HashMap::new();
    for (_, tc) in &indexed {
        let lang = infer_language(&tc.file_path).to_string();
        by_language.entry(lang).or_default().push(tc);
    }

    // Round-robin across language groups
    let mut selected = Vec::with_capacity(count);
    let _seen_categories: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut lang_keys: Vec<_> = by_language.keys().cloned().collect();
    lang_keys.sort(); // deterministic order
    let mut lang_indices: std::collections::HashMap<String, usize> =
        std::collections::HashMap::new();

    // First pass: pick one from each language, preferring unseen categories
    for lang in &lang_keys {
        if selected.len() >= count {
            break;
        }
        if let Some(cases_for_lang) = by_language.get(lang) {
            // Prefer a case with an unseen category
            let pick = cases_for_lang
                .iter()
                .enumerate()
                .find(|(_, _tc)| {
                    // We don't have category on TestCase directly, but we can
                    // use the test case ID as a proxy for now. The seed cases
                    // below are tagged with categories in their expected_issue.
                    true
                })
                .map(|(i, tc)| {
                    lang_indices.insert(lang.clone(), i + 1);
                    *tc
                });
            if let Some(tc) = pick {
                selected.push(tc.clone());
            }
        }
    }

    // Second pass: fill remaining slots round-robin
    let mut round = 0;
    while selected.len() < count {
        let mut added_any = false;
        for lang in &lang_keys {
            if selected.len() >= count {
                break;
            }
            let idx = lang_indices.get(lang).copied().unwrap_or(0);
            if let Some(cases_for_lang) = by_language.get(lang) {
                if idx + round < cases_for_lang.len() {
                    let tc = cases_for_lang[idx + round];
                    if !selected.iter().any(|s| s.id == tc.id) {
                        selected.push(tc.clone());
                        added_any = true;
                    }
                }
            }
        }
        round += 1;
        if !added_any {
            break; // exhausted all cases
        }
    }

    selected
}

// ---------------------------------------------------------------------------
// Dynamic MR discovery — pull real test cases from GitLab
// ---------------------------------------------------------------------------

/// Discover test cases from real GitLab MRs using our actual review pipeline.
///
/// THE REAL FLOW (end-to-end):
///   1. Hit gitlab-org group MR list (random page) — same as browsing
///      https://gitlab.com/groups/gitlab-org/-/merge_requests
///   2. Pick random MRs from the list
///   3. Build MrContext for each (same as QueueManager does)
///   4. Run OUR Botto review pipeline on the MR
///   5. Pick a ReviewComment that has original_code + suggestion
///   6. That finding IS the test case for the sandbox fix
///
/// What we're grading: the sandbox fix system prompt's ability to take
/// our review finding and actually fix the code in a Docker container.
pub async fn discover_from_gitlab(
    cfg: &BottoConfig,
    pool: &SqlitePool,
    count: usize,
    round_seed: u64,
) -> Vec<TestCase> {
    let gl_cfg = GitLabConfig {
        base_url: cfg.gitlab.url.clone(),
        token: cfg.gitlab.bot_token.clone(),
    };

    let mut all_cases: Vec<TestCase> = Vec::new();
    let mut tc_counter = 0u32;

    let mut rng = round_seed;
    let lcg = |state: &mut u64| -> u64 {
        *state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        *state >> 33
    };

    let mut orgs = cfg.harness.gitlab_seed_orgs.clone();
    if orgs.is_empty() {
        orgs.push("gitlab-org".into());
    }

    for org in &orgs {
        if all_cases.len() >= count {
            break;
        }

        // Pick a random page (1-10) for variety across rounds
        let page = (lcg(&mut rng) % 10 + 1) as usize;

        info!(
            "harness: fetching MRs from {group} (page {page})",
            group = org,
            page = page,
        );

        // Hit the group-level MR list — OPEN MRs so source branches still exist
        let mrs = match gl::fetch_group_merge_requests(
            &gl_cfg,
            org,
            "opened",
            20, // 20 per page
            page,
        )
        .await
        {
            Ok(m) => m,
            Err(e) => {
                warn!("harness: failed to fetch group MRs from {}: {}", org, e);
                continue;
            }
        };

        if mrs.is_empty() {
            warn!("harness: no MRs found in {} page {}", org, page);
            continue;
        }

        info!("harness: got {} MRs from {} page {}", mrs.len(), org, page);

        // Shuffle for randomness
        let mut mr_indices: Vec<usize> = (0..mrs.len()).collect();
        for i in (1..mr_indices.len()).rev() {
            let j = lcg(&mut rng) as usize % (i + 1);
            mr_indices.swap(i, j);
        }

        for &mi in &mr_indices {
            if all_cases.len() >= count {
                break;
            }

            let mr = &mrs[mi];

            // We need the project path — extract from web_url or use source_project_id
            let project_id = match mr.source_project_id.or(mr.target_project_id) {
                Some(id) => id,
                None => {
                    warn!("harness: MR {} has no project ID, skipping", mr.iid);
                    continue;
                }
            };

            // Fetch project to get path
            let project = match gl::fetch_project_by_id(&gl_cfg, project_id).await {
                Ok(p) => p,
                Err(e) => {
                    warn!("harness: failed to fetch project {}: {}", project_id, e);
                    continue;
                }
            };

            info!(
                "harness: reviewing {}!{} — \"{}\"",
                project.path_with_namespace, mr.iid, mr.title,
            );

            // Step 1: Build MrContext (same as QueueManager::build_mr_context)
            let mr_context = match build_mr_context(cfg, &project.path_with_namespace, mr.iid).await {
                Some(ctx) => ctx,
                None => {
                    warn!("harness: failed to build MrContext for {}!{}", project.path_with_namespace, mr.iid);
                    continue;
                }
            };

            // Skip small MRs — need enough files to get good findings
            let total_lines: u32 = mr_context.diff_files.iter()
                .map(|f| f.added_lines + f.removed_lines)
                .sum();
            let file_count = mr_context.diff_files.len();
            if file_count < 5 || total_lines < 30 {
                info!("harness: skipping {}!{} — too small ({} files, {} lines)", project.path_with_namespace, mr.iid, file_count, total_lines);
                continue;
            }

            // Skip huge MRs (too expensive to review)
            if total_lines > 2000 || file_count > 30 {
                info!("harness: skipping {}!{} — too large ({} lines, {} files)", project.path_with_namespace, mr.iid, total_lines, file_count);
                continue;
            }

            // Step 2: Run OUR Botto review pipeline on this MR
            // Retry up to 2 times if the review fails (API rate limits, transient errors)
            info!("harness: running Botto review on {}!{}...", project.path_with_namespace, mr.iid);
            let mut review = None;
            for attempt in 1..=3 {
                match run_review(cfg, pool, &mr_context).await {
                    Some(r) => {
                        // Check if we got any file reviews at all
                        if r.file_reviews.is_empty() {
                            warn!(
                                "harness: review returned 0 file reviews for {}!{} (attempt {})",
                                project.path_with_namespace, mr.iid, attempt,
                            );
                            if attempt < 3 {
                                info!("harness: retrying review in 5s...");
                                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                                continue;
                            }
                        } else {
                            info!(
                                "harness: review completed for {}!{} — {} file reviews",
                                project.path_with_namespace, mr.iid, r.file_reviews.len(),
                            );
                            review = Some(r);
                            break;
                        }
                    }
                    None => {
                        warn!(
                            "harness: review failed for {}!{} (attempt {})",
                            project.path_with_namespace, mr.iid, attempt,
                        );
                        if attempt < 3 {
                            info!("harness: retrying review in 5s...");
                            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                        }
                    }
                }
            }

            let review = match review {
                Some(r) => r,
                None => {
                    warn!("harness: giving up on {}!{} after 3 attempts", project.path_with_namespace, mr.iid);
                    continue;
                }
            };

            // Step 3: Grab ALL fixable findings from this MR
            let fixable: Vec<&ReviewComment> = review
                .file_reviews
                .iter()
                .flat_map(|fr| &fr.comments)
                .filter(|c| {
                    c.original_code.is_some()
                        && c.suggestion.is_some()
                        && !c.original_code.as_ref().unwrap().is_empty()
                        && !c.suggestion.as_ref().unwrap().is_empty()
                        && c.original_code != c.suggestion
                })
                .collect();

            let total_comments: usize = review.file_reviews.iter().map(|fr| fr.comments.len()).sum();

            if fixable.is_empty() {
                info!(
                    "harness: no fixable findings in {}!{} ({} comments, 0 with code suggestions)",
                    project.path_with_namespace, mr.iid, total_comments,
                );
                continue;
            }

            info!(
                "harness: {}!{} has {} fixable findings out of {} total — using all of them",
                project.path_with_namespace, mr.iid, fixable.len(), total_comments,
            );

            // Step 4: Convert ALL fixable findings into test cases (not just one)
            // If this MR gives us enough, we stop reviewing more MRs.
            // We use OPEN MRs so source branches still exist and can be cloned.
            for pick in &fixable {
                if all_cases.len() >= count {
                    break;
                }

                let file_content = gl::fetch_file_content(
                    &gl_cfg,
                    project.id,
                    &pick.file_path,
                    &mr.source_branch,
                )
                .await
                .ok();

                let file_diff = mr_context
                    .diff_files
                    .iter()
                    .find(|f| f.file_path == pick.file_path)
                    .map(|f| f.diff.clone());

                tc_counter += 1;
                let tc_id = format!("gl-{:04}", tc_counter);

                let difficulty = match pick.severity {
                    ReviewCommentSeverity::Critical => Difficulty::Hard,
                    ReviewCommentSeverity::Warning => Difficulty::Medium,
                    ReviewCommentSeverity::Suggestion => Difficulty::Easy,
                    ReviewCommentSeverity::Info => Difficulty::Easy,
                };

                let tc = TestCase {
                    id: tc_id.clone(),
                    project_path: project.path_with_namespace.clone(),
                    mr_iid: mr.iid,
                    source_branch: mr.source_branch.clone(),
                    target_branch: mr.target_branch.clone(),
                    file_path: pick.file_path.clone(),
                    original_code: pick.original_code.clone().unwrap(),
                    expected_issue: pick.title.clone(),
                    suggestion: pick.suggestion.clone().unwrap(),
                    difficulty,
                    test_command: None,
                    mr_title: Some(mr.title.clone()),
                    mr_description: mr.description.clone(),
                    comment_body: Some(pick.body.clone()),
                    file_content,
                    file_diff,
                    created_at: Utc::now(),
                    source_url: Some(mr.web_url.clone()),
                };

                info!(
                    "harness: TEST CASE {} — {}!{} [{:?}] \"{}\" ({})",
                    tc_id,
                    project.path_with_namespace,
                    mr.iid,
                    pick.severity,
                    pick.title,
                    infer_language(&pick.file_path),
                );

                all_cases.push(tc);
            }

            // If we have enough from this one MR, stop
            if all_cases.len() >= count {
                info!(
                    "harness: got {} test cases from {}!{} — enough for this round",
                    all_cases.len(), project.path_with_namespace, mr.iid,
                );
                break;
            }
        }
    }

    info!("harness: discovered {} test cases from real MRs", all_cases.len());
    all_cases
}

// ---------------------------------------------------------------------------
// Helpers for the real pipeline
// ---------------------------------------------------------------------------

/// Build an MrContext from a GitLab MR — same logic as QueueManager::build_mr_context.
async fn build_mr_context(
    cfg: &BottoConfig,
    project_path: &str,
    mr_iid: u64,
) -> Option<MrContext> {
    let gl_cfg = GitLabConfig {
        base_url: cfg.gitlab.url.clone(),
        token: cfg.gitlab.bot_token.clone(),
    };

    let project = gl::fetch_project(&gl_cfg, project_path).await.ok()?;
    let changes = gl::fetch_mr_changes(&gl_cfg, project.id, mr_iid).await.ok()?;

    let diff_files: Vec<DiffFileData> = changes
        .changes
        .into_iter()
        .map(|c| {
            let added = c.diff.lines().filter(|l| l.starts_with('+')).count() as u32;
            let removed = c.diff.lines().filter(|l| l.starts_with('-')).count() as u32;
            DiffFileData {
                file_path: c.new_path.clone(),
                old_path: if c.renamed_file { Some(c.old_path) } else { None },
                is_new: c.new_file,
                is_deleted: c.deleted_file,
                is_renamed: c.renamed_file,
                diff: c.diff,
                added_lines: added,
                removed_lines: removed,
            }
        })
        .collect();

    Some(MrContext {
        project_path: project_path.to_string(),
        project_id: Some(project.id),
        mr_iid,
        host_url: cfg.gitlab.url.clone(),
        title: changes.title,
        description: changes.description,
        source_branch: changes.source_branch,
        target_branch: changes.target_branch,
        author_username: None,
        diff_files,
    })
}

/// Run our actual Botto review pipeline on an MR.
/// Uses cache — if we've already reviewed this MR, grab findings instantly.
async fn run_review(
    cfg: &BottoConfig,
    pool: &SqlitePool,
    mr_context: &MrContext,
) -> Option<crate::types::review::CachedReview> {
    let (chunk_tx, mut chunk_rx) = tokio::sync::mpsc::channel::<serde_json::Value>(128);

    // Drain chunks in background (we don't need to stream them)
    let drainer = tokio::spawn(async move {
        while chunk_rx.recv().await.is_some() {}
    });

    let tasks = orchestrator::all_tasks();
    let result = orchestrator::execute_review(
        cfg,
        pool,
        mr_context,
        &tasks,
        chunk_tx,
        CancellationToken::new(),
        false, // USE cache — don't waste time re-reviewing MRs we've seen
        None,
    )
    .await;

    let _ = drainer.await;
    result
}

/// Check if a file is non-code (docs, configs, images, etc.)
fn is_non_code_file(path: &str) -> bool {
    let lower = path.to_lowercase();
    let non_code_exts = [
        ".md", ".txt", ".rst", ".yml", ".yaml", ".json", ".toml",
        ".xml", ".svg", ".png", ".jpg", ".gif", ".ico", ".lock",
        ".sum", ".mod", ".cfg", ".ini", ".env", ".gitignore",
        ".dockerignore", ".editorconfig", "license", "changelog",
        "makefile", ".csv",
    ];
    non_code_exts.iter().any(|ext| lower.ends_with(ext))
        || lower.contains("/vendor/")
        || lower.contains("/node_modules/")
        || lower.contains("/dist/")
        || lower.starts_with("doc/")
        || lower.starts_with("docs/")
}

// ---------------------------------------------------------------------------
// Seed test cases — diverse across languages and issue types
// ---------------------------------------------------------------------------

/// Seed test cases covering multiple languages and issue categories.
/// These provide a reliable baseline for harness runs while we build out
/// dynamic MR discovery from gitlab-org.
pub fn seed_test_cases() -> Vec<TestCase> {
    vec![
        // --- Go: Error handling ---
        TestCase {
            id: "tc-001".into(),
            project_path: "gitlab-org/gitlab-runner".into(),
            mr_iid: 0,
            source_branch: "fix/error-handling".into(),
            target_branch: "main".into(),
            file_path: "executors/docker/executor_docker.go".into(),
            original_code: r#"func (e *executor) pullImage(ctx context.Context, image string) error {
    _, err := e.client.ImagePull(ctx, image, types.ImagePullOptions{})
    return err
}"#.into(),
            expected_issue: "Missing error wrapping — callers lose context about which image failed to pull".into(),
            suggestion: r#"func (e *executor) pullImage(ctx context.Context, image string) error {
    _, err := e.client.ImagePull(ctx, image, types.ImagePullOptions{})
    if err != nil {
        return fmt.Errorf("pulling image %q: %w", image, err)
    }
    return nil
}"#.into(),
            difficulty: Difficulty::Easy,
            test_command: Some("cd /workspace && go test ./executors/docker/... -v -run TestPull 2>&1".into()),
            mr_title: Some("Fix error handling in Docker executor image pull".into()),
            mr_description: Some("Wrap errors from image pull with context about which image failed".into()),
            comment_body: Some("This loses context about which image failed. Wrap the error with fmt.Errorf.".into()),
            file_content: None,
            file_diff: None,
            created_at: Utc::now(),
            source_url: Some("https://gitlab.com/gitlab-org/gitlab-runner/-/merge_requests".into()),
        },
        // --- Go: Race condition ---
        TestCase {
            id: "tc-002".into(),
            project_path: "gitlab-org/gitlab-runner".into(),
            mr_iid: 0,
            source_branch: "fix/race-condition".into(),
            target_branch: "main".into(),
            file_path: "executors/docker/executor_docker.go".into(),
            original_code: r#"func (e *executor) cleanupContainer(ctx context.Context) {
    if e.containerID == "" {
        return
    }
    e.client.ContainerRemove(ctx, e.containerID, types.ContainerRemoveOptions{Force: true})
    e.containerID = ""
}"#.into(),
            expected_issue: "Race condition: containerID read/write not protected by mutex".into(),
            suggestion: r#"func (e *executor) cleanupContainer(ctx context.Context) {
    e.mu.Lock()
    id := e.containerID
    e.containerID = ""
    e.mu.Unlock()

    if id == "" {
        return
    }
    e.client.ContainerRemove(ctx, id, types.ContainerRemoveOptions{Force: true})
}"#.into(),
            difficulty: Difficulty::Hard,
            test_command: Some("cd /workspace && go test ./executors/docker/... -v -race 2>&1".into()),
            mr_title: Some("Fix race condition in container cleanup".into()),
            mr_description: None,
            comment_body: Some("TOCTOU race — another goroutine can modify containerID between check and clear.".into()),
            file_content: None,
            file_diff: None,
            created_at: Utc::now(),
            source_url: None,
        },
        // --- Python: Security (SQL injection) ---
        TestCase {
            id: "tc-003".into(),
            project_path: "gitlab-org/gitlab-triage".into(),
            mr_iid: 0,
            source_branch: "fix/sql-injection".into(),
            target_branch: "main".into(),
            file_path: "lib/database/query_builder.py".into(),
            original_code: r#"def find_issues(self, project_id, label):
    query = f"SELECT * FROM issues WHERE project_id = {project_id} AND label = '{label}'"
    return self.cursor.execute(query).fetchall()
"#.into(),
            expected_issue: "SQL injection vulnerability — label parameter is interpolated directly into query string".into(),
            suggestion: r#"def find_issues(self, project_id, label):
    query = "SELECT * FROM issues WHERE project_id = ? AND label = ?"
    return self.cursor.execute(query, (project_id, label)).fetchall()
"#.into(),
            difficulty: Difficulty::Medium,
            test_command: Some("cd /workspace && python -m pytest tests/test_query_builder.py -v 2>&1".into()),
            mr_title: Some("Fix SQL injection in query builder".into()),
            mr_description: Some("Use parameterized queries instead of string interpolation".into()),
            comment_body: Some("SQL injection: label is user input and gets interpolated directly. Use parameterized queries.".into()),
            file_content: None,
            file_diff: None,
            created_at: Utc::now(),
            source_url: None,
        },
        // --- Ruby: Resource leak ---
        TestCase {
            id: "tc-004".into(),
            project_path: "gitlab-org/gitlab".into(),
            mr_iid: 0,
            source_branch: "fix/file-handle-leak".into(),
            target_branch: "main".into(),
            file_path: "lib/gitlab/import_export/file_importer.rb".into(),
            original_code: r#"def read_config(path)
  file = File.open(path, 'r')
  data = JSON.parse(file.read)
  file.close
  data
end
"#.into(),
            expected_issue: "Resource leak — if JSON.parse raises, file handle is never closed".into(),
            suggestion: r#"def read_config(path)
  File.open(path, 'r') do |file|
    JSON.parse(file.read)
  end
end
"#.into(),
            difficulty: Difficulty::Easy,
            test_command: Some("cd /workspace && bundle exec rspec spec/lib/gitlab/import_export/file_importer_spec.rb 2>&1".into()),
            mr_title: Some("Fix file handle leak in config reader".into()),
            mr_description: None,
            comment_body: Some("If JSON.parse raises, the file handle leaks. Use a block form of File.open.".into()),
            file_content: None,
            file_diff: None,
            created_at: Utc::now(),
            source_url: None,
        },
        // --- TypeScript: Null safety ---
        TestCase {
            id: "tc-005".into(),
            project_path: "gitlab-org/gitlab-vscode-extension".into(),
            mr_iid: 0,
            source_branch: "fix/null-check".into(),
            target_branch: "main".into(),
            file_path: "src/services/git/git_service.ts".into(),
            original_code: r#"async function getCurrentBranch(repoPath: string): Promise<string> {
  const result = await exec('git rev-parse --abbrev-ref HEAD', { cwd: repoPath });
  return result.stdout.trim();
}
"#.into(),
            expected_issue: "No null check on result — exec can return null stdout on detached HEAD or error".into(),
            suggestion: r#"async function getCurrentBranch(repoPath: string): Promise<string | null> {
  const result = await exec('git rev-parse --abbrev-ref HEAD', { cwd: repoPath });
  if (!result.stdout) {
    return null;
  }
  const branch = result.stdout.trim();
  return branch === 'HEAD' ? null : branch;
}
"#.into(),
            difficulty: Difficulty::Medium,
            test_command: Some("cd /workspace && npx jest --passWithNoTests src/services/git/ 2>&1".into()),
            mr_title: Some("Handle detached HEAD and null stdout in getCurrentBranch".into()),
            mr_description: None,
            comment_body: Some("result.stdout can be null on error, and 'HEAD' means detached. Both cases need handling.".into()),
            file_content: None,
            file_diff: None,
            created_at: Utc::now(),
            source_url: None,
        },
        // --- JavaScript: Logic bug ---
        TestCase {
            id: "tc-006".into(),
            project_path: "gitlab-org/gitlab".into(),
            mr_iid: 0,
            source_branch: "fix/off-by-one".into(),
            target_branch: "main".into(),
            file_path: "app/assets/javascripts/lib/utils/pagination.js".into(),
            original_code: r#"export function getPageRange(currentPage, totalPages, windowSize = 5) {
  const half = Math.floor(windowSize / 2);
  let start = Math.max(currentPage - half, 1);
  let end = start + windowSize;
  if (end > totalPages) {
    end = totalPages;
    start = Math.max(end - windowSize, 1);
  }
  return Array.from({ length: end - start }, (_, i) => start + i);
}
"#.into(),
            expected_issue: "Off-by-one: end should be start + windowSize - 1, and Array length should be end - start + 1".into(),
            suggestion: r#"export function getPageRange(currentPage, totalPages, windowSize = 5) {
  const half = Math.floor(windowSize / 2);
  let start = Math.max(currentPage - half, 1);
  let end = Math.min(start + windowSize - 1, totalPages);
  if (end - start + 1 < windowSize) {
    start = Math.max(end - windowSize + 1, 1);
  }
  return Array.from({ length: end - start + 1 }, (_, i) => start + i);
}
"#.into(),
            difficulty: Difficulty::Medium,
            test_command: Some("cd /workspace && npx jest app/assets/javascripts/lib/utils/pagination 2>&1".into()),
            mr_title: Some("Fix off-by-one in pagination range calculation".into()),
            mr_description: None,
            comment_body: Some("Off-by-one: with windowSize=5 this generates 6 page numbers. end should be start + windowSize - 1.".into()),
            file_content: None,
            file_diff: None,
            created_at: Utc::now(),
            source_url: None,
        },
        // --- Rust: Boundary condition ---
        TestCase {
            id: "tc-007".into(),
            project_path: "gitlab-org/gitaly".into(),
            mr_iid: 0,
            source_branch: "fix/empty-slice".into(),
            target_branch: "main".into(),
            file_path: "internal/git/repository.go".into(),
            original_code: r#"func (r *Repository) GetBranch(ctx context.Context, name string) (*Reference, error) {
    refs, err := r.GetReferences(ctx, "refs/heads/"+name)
    if err != nil {
        return nil, err
    }
    return refs[0], nil
}"#.into(),
            expected_issue: "Index out of bounds panic when branch doesn't exist — refs slice will be empty".into(),
            suggestion: r#"func (r *Repository) GetBranch(ctx context.Context, name string) (*Reference, error) {
    refs, err := r.GetReferences(ctx, "refs/heads/"+name)
    if err != nil {
        return nil, err
    }
    if len(refs) == 0 {
        return nil, ErrBranchNotFound
    }
    return refs[0], nil
}"#.into(),
            difficulty: Difficulty::Medium,
            test_command: Some("cd /workspace && go test ./internal/git/... -v -run TestGetBranch 2>&1".into()),
            mr_title: Some("Fix panic on missing branch lookup".into()),
            mr_description: None,
            comment_body: Some("This will panic with index out of range if the branch doesn't exist.".into()),
            file_content: None,
            file_diff: None,
            created_at: Utc::now(),
            source_url: None,
        },
        // --- Python: Performance ---
        TestCase {
            id: "tc-008".into(),
            project_path: "gitlab-org/gitlab-triage".into(),
            mr_iid: 0,
            source_branch: "fix/n-plus-one".into(),
            target_branch: "main".into(),
            file_path: "lib/triage/processors/label_processor.py".into(),
            original_code: r#"def process_issues(self, project_id):
    issues = self.api.get_issues(project_id)
    results = []
    for issue in issues:
        labels = self.api.get_labels(project_id, issue['iid'])
        issue['resolved_labels'] = labels
        results.append(issue)
    return results
"#.into(),
            expected_issue: "N+1 query: fetches labels one-by-one per issue instead of batch".into(),
            suggestion: r#"def process_issues(self, project_id):
    issues = self.api.get_issues(project_id)
    if not issues:
        return []
    iids = [issue['iid'] for issue in issues]
    all_labels = self.api.get_labels_batch(project_id, iids)
    for issue in issues:
        issue['resolved_labels'] = all_labels.get(issue['iid'], [])
    return issues
"#.into(),
            difficulty: Difficulty::Hard,
            test_command: Some("cd /workspace && python -m pytest tests/test_label_processor.py -v 2>&1".into()),
            mr_title: Some("Fix N+1 query in label processor".into()),
            mr_description: Some("Batch label fetching to avoid N+1 API calls".into()),
            comment_body: Some("N+1: this makes one API call per issue. Use batch endpoint instead.".into()),
            file_content: None,
            file_diff: None,
            created_at: Utc::now(),
            source_url: None,
        },
        // --- Ruby: API misuse ---
        TestCase {
            id: "tc-009".into(),
            project_path: "gitlab-org/gitlab".into(),
            mr_iid: 0,
            source_branch: "fix/api-misuse".into(),
            target_branch: "main".into(),
            file_path: "app/services/merge_requests/merge_service.rb".into(),
            original_code: r#"def execute(merge_request)
  merge_request.update(state: 'merged')
  merge_request.target_project.repository.merge(
    merge_request.source_branch,
    merge_request.target_branch,
    message: merge_commit_message(merge_request)
  )
end
"#.into(),
            expected_issue: "State updated before merge — if repository.merge fails, MR is stuck in 'merged' state with unmerged code".into(),
            suggestion: r#"def execute(merge_request)
  merge_request.target_project.repository.merge(
    merge_request.source_branch,
    merge_request.target_branch,
    message: merge_commit_message(merge_request)
  )
  merge_request.update!(state: 'merged')
rescue StandardError => e
  merge_request.update(state: 'opened')
  raise
end
"#.into(),
            difficulty: Difficulty::Hard,
            test_command: Some("cd /workspace && bundle exec rspec spec/services/merge_requests/merge_service_spec.rb 2>&1".into()),
            mr_title: Some("Fix state update ordering in merge service".into()),
            mr_description: None,
            comment_body: Some("State is set to merged before the actual merge. If merge fails, MR is stuck. Do the merge first, then update state.".into()),
            file_content: None,
            file_diff: None,
            created_at: Utc::now(),
            source_url: None,
        },
        // --- TypeScript: Type safety ---
        TestCase {
            id: "tc-010".into(),
            project_path: "gitlab-org/gitlab-vscode-extension".into(),
            mr_iid: 0,
            source_branch: "fix/type-narrowing".into(),
            target_branch: "main".into(),
            file_path: "src/services/api/gitlab_api.ts".into(),
            original_code: r#"async function fetchMergeRequest(projectId: number, mrIid: number): Promise<MergeRequest> {
  const response = await fetch(`/api/v4/projects/${projectId}/merge_requests/${mrIid}`);
  const data = await response.json();
  return {
    id: data.id,
    title: data.title,
    author: data.author.username,
    labels: data.labels.join(', '),
  };
}
"#.into(),
            expected_issue: "No response.ok check, no type validation on data, data.author can be null for deleted users, data.labels can be null".into(),
            suggestion: r#"async function fetchMergeRequest(projectId: number, mrIid: number): Promise<MergeRequest> {
  const response = await fetch(`/api/v4/projects/${projectId}/merge_requests/${mrIid}`);
  if (!response.ok) {
    throw new Error(`GitLab API error: ${response.status} ${response.statusText}`);
  }
  const data = await response.json();
  return {
    id: data.id,
    title: data.title ?? '',
    author: data.author?.username ?? 'unknown',
    labels: (data.labels ?? []).join(', '),
  };
}
"#.into(),
            difficulty: Difficulty::Medium,
            test_command: Some("cd /workspace && npx jest src/services/api/ 2>&1".into()),
            mr_title: Some("Add defensive checks to MR API response handling".into()),
            mr_description: None,
            comment_body: Some("No error check on response, author can be null (deleted user), labels can be null. All need guarding.".into()),
            file_content: None,
            file_diff: None,
            created_at: Utc::now(),
            source_url: None,
        },
    ]
}
