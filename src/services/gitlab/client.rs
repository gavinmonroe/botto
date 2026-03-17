// ---------------------------------------------------------------------------
// GitLab REST API v4 client — pure functions using reqwest.
//
// Ported from Otto's gitlab-client.ts. Uses Botto's central bot PAT for auth.
// All functions return Result<T, GitLabError> for consistent error handling.
//
// Design decisions:
//   - Pure functions, no stored state. Config passed per-call.
//   - Pagination handled internally via Link header parsing.
//   - 429/5xx errors surfaced with clear messages (retry is caller's job).
//   - URL encoding for project paths handled by encode_project_path().
// ---------------------------------------------------------------------------

use reqwest::header::{HeaderMap, HeaderValue, LINK};
use reqwest::{Client, Response, StatusCode};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tracing::debug;

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum GitLabError {
    #[error("authentication failed (401) — check bot PAT")]
    Unauthorized,
    #[error("forbidden (403) — bot PAT lacks required scope")]
    Forbidden,
    #[error("not found (404) — {0}")]
    NotFound(String),
    #[error("rate limited (429) — try again later")]
    RateLimited,
    #[error("server error ({0})")]
    ServerError(u16),
    #[error("network error: {0}")]
    Network(String),
    #[error("parse error: {0}")]
    Parse(String),
}

impl GitLabError {
    fn from_status(status: StatusCode, context: &str) -> Self {
        match status.as_u16() {
            401 => Self::Unauthorized,
            403 => Self::Forbidden,
            404 => Self::NotFound(context.to_string()),
            429 => Self::RateLimited,
            s if s >= 500 => Self::ServerError(s),
            s => Self::ServerError(s),
        }
    }
}

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct GitLabConfig {
    pub base_url: String, // e.g., "https://gitlab.com" — no trailing slash
    pub token: String,    // Bot PAT
}

// ---------------------------------------------------------------------------
// Shared HTTP helpers
// ---------------------------------------------------------------------------

fn build_client() -> Client {
    Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .expect("failed to build HTTP client")
}

fn auth_headers(token: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        "PRIVATE-TOKEN",
        HeaderValue::from_str(token).unwrap_or_else(|_| HeaderValue::from_static("")),
    );
    headers
}

fn encode_project_path(path: &str) -> String {
    urlencoding::encode(path).to_string()
}

async fn check_response(resp: Response, context: &str) -> Result<Response, GitLabError> {
    let status = resp.status();
    if status.is_success() {
        Ok(resp)
    } else {
        Err(GitLabError::from_status(status, context))
    }
}

pub(crate) async fn get_json<T: DeserializeOwned>(
    cfg: &GitLabConfig,
    path: &str,
    query: &[(&str, &str)],
) -> Result<T, GitLabError> {
    let url = format!("{}/api/v4{}", cfg.base_url, path);
    let client = build_client();
    let resp = client
        .get(&url)
        .headers(auth_headers(&cfg.token))
        .query(query)
        .send()
        .await
        .map_err(|e| GitLabError::Network(e.to_string()))?;

    let resp = check_response(resp, path).await?;
    resp.json::<T>()
        .await
        .map_err(|e| GitLabError::Parse(e.to_string()))
}

/// Fetch all pages of a paginated endpoint. Follows Link: <...>; rel="next".
/// Safety cap at max_pages to prevent runaway pagination.
async fn get_all_pages<T: DeserializeOwned>(
    cfg: &GitLabConfig,
    path: &str,
    query: &[(&str, &str)],
    max_pages: usize,
) -> Result<Vec<T>, GitLabError> {
    let client = build_client();
    let base_url = format!("{}/api/v4{}", cfg.base_url, path);
    let mut all_items: Vec<T> = Vec::new();
    let mut url = base_url.clone();
    let mut page = 0;

    // Build initial query with per_page=100
    let mut full_query: Vec<(&str, &str)> = query.to_vec();
    full_query.push(("per_page", "100"));

    loop {
        if page >= max_pages {
            debug!("pagination cap reached at {} pages for {}", max_pages, path);
            break;
        }

        let resp = client
            .get(&url)
            .headers(auth_headers(&cfg.token))
            .query(&full_query)
            .send()
            .await
            .map_err(|e| GitLabError::Network(e.to_string()))?;

        let resp = check_response(resp, path).await?;

        // Extract next page URL from Link header before consuming body
        let next_url = resp
            .headers()
            .get(LINK)
            .and_then(|v| v.to_str().ok())
            .and_then(parse_next_link);

        let items: Vec<T> = resp
            .json()
            .await
            .map_err(|e| GitLabError::Parse(e.to_string()))?;

        let count = items.len();
        all_items.extend(items);
        page += 1;

        match next_url {
            Some(next) if count > 0 => {
                url = next;
                // Clear query params — they're in the next URL already
                full_query = Vec::new();
            }
            _ => break,
        }
    }

    Ok(all_items)
}

/// Parse the `next` URL from a Link header.
/// Format: `<https://...?page=2>; rel="next", <...>; rel="last"`
fn parse_next_link(header: &str) -> Option<String> {
    for part in header.split(',') {
        let part = part.trim();
        if part.contains("rel=\"next\"") {
            if let Some(start) = part.find('<') {
                if let Some(end) = part.find('>') {
                    return Some(part[start + 1..end].to_string());
                }
            }
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Public API functions
// ---------------------------------------------------------------------------

/// Test the connection by fetching the authenticated user.
pub async fn test_connection(cfg: &GitLabConfig) -> Result<GitLabUser, GitLabError> {
    get_json(cfg, "/user", &[]).await
}

/// Fetch project metadata by path (e.g., "namespace/project").
pub async fn fetch_project(cfg: &GitLabConfig, project_path: &str) -> Result<Project, GitLabError> {
    let encoded = encode_project_path(project_path);
    get_json(cfg, &format!("/projects/{}", encoded), &[]).await
}

/// Fetch project by numeric ID.
pub async fn fetch_project_by_id(cfg: &GitLabConfig, project_id: i64) -> Result<Project, GitLabError> {
    get_json(cfg, &format!("/projects/{}", project_id), &[]).await
}

/// Fetch MR metadata.
pub async fn fetch_merge_request(
    cfg: &GitLabConfig,
    project_id: i64,
    mr_iid: u64,
) -> Result<MergeRequest, GitLabError> {
    get_json(
        cfg,
        &format!("/projects/{}/merge_requests/{}", project_id, mr_iid),
        &[],
    )
    .await
}

/// Fetch MR changes (diffs). This is the primary input to the review pipeline.
pub async fn fetch_mr_changes(
    cfg: &GitLabConfig,
    project_id: i64,
    mr_iid: u64,
) -> Result<MergeRequestChanges, GitLabError> {
    get_json(
        cfg,
        &format!("/projects/{}/merge_requests/{}/changes", project_id, mr_iid),
        &[("access_raw_diffs", "true")],
    )
    .await
}

/// Fetch raw file content from a specific ref.
pub async fn fetch_file_content(
    cfg: &GitLabConfig,
    project_id: i64,
    file_path: &str,
    ref_name: &str,
) -> Result<String, GitLabError> {
    let encoded_path = urlencoding::encode(file_path);
    let url = format!(
        "{}/api/v4/projects/{}/repository/files/{}/raw",
        cfg.base_url, project_id, encoded_path
    );
    let client = build_client();
    let resp = client
        .get(&url)
        .headers(auth_headers(&cfg.token))
        .query(&[("ref", ref_name)])
        .send()
        .await
        .map_err(|e| GitLabError::Network(e.to_string()))?;

    let resp = check_response(resp, &format!("file:{}", file_path)).await?;
    resp.text()
        .await
        .map_err(|e| GitLabError::Parse(e.to_string()))
}

/// Fetch repository file tree (single level or recursive).
pub async fn fetch_file_tree(
    cfg: &GitLabConfig,
    project_id: i64,
    path: &str,
    ref_name: &str,
    recursive: bool,
) -> Result<Vec<TreeEntry>, GitLabError> {
    let mut query = vec![("ref", ref_name)];
    let path_owned;
    if !path.is_empty() {
        path_owned = path.to_string();
        query.push(("path", &path_owned));
    }
    let recursive_str;
    if recursive {
        recursive_str = "true".to_string();
        query.push(("recursive", &recursive_str));
    }

    get_all_pages(
        cfg,
        &format!("/projects/{}/repository/tree", project_id),
        &query.iter().map(|(k, v)| (*k, *v)).collect::<Vec<_>>(),
        10,
    )
    .await
}

/// Fetch MR discussions (threaded comments).
pub async fn fetch_mr_discussions(
    cfg: &GitLabConfig,
    project_id: i64,
    mr_iid: u64,
) -> Result<Vec<Discussion>, GitLabError> {
    get_all_pages(
        cfg,
        &format!(
            "/projects/{}/merge_requests/{}/discussions",
            project_id, mr_iid
        ),
        &[],
        10,
    )
    .await
}

/// Fetch recently merged MRs (for file activity / churn detection).
pub async fn fetch_recent_merged_mrs(
    cfg: &GitLabConfig,
    project_id: i64,
    since: &str, // ISO 8601 date
) -> Result<Vec<MergeRequest>, GitLabError> {
    get_all_pages(
        cfg,
        &format!("/projects/{}/merge_requests", project_id),
        &[
            ("state", "merged"),
            ("updated_after", since),
            ("order_by", "updated_at"),
            ("sort", "desc"),
        ],
        5,
    )
    .await
}

/// Fetch open merge requests whose source branch matches the given branch name.
/// Used by auto-review-on-push to find which MRs are affected by a push event.
/// Returns at most one page (20 results) — a branch rarely has more than one open MR.
pub async fn fetch_open_mrs_for_branch(
    cfg: &GitLabConfig,
    project_id: i64,
    source_branch: &str,
) -> Result<Vec<MergeRequest>, GitLabError> {
    get_json(
        cfg,
        &format!("/projects/{}/merge_requests", project_id),
        &[
            ("state", "opened"),
            ("source_branch", source_branch),
            ("per_page", "20"),
        ],
    )
    .await
}

/// Fetch all open merge requests for a project. Used by cluster detection
/// to find MRs sharing ticket keys. Paginated, capped at 5 pages (500 MRs).
pub async fn fetch_open_mrs(
    cfg: &GitLabConfig,
    project_id: i64,
) -> Result<Vec<MergeRequest>, GitLabError> {
    get_all_pages(
        cfg,
        &format!("/projects/{}/merge_requests", project_id),
        &[
            ("state", "opened"),
            ("order_by", "updated_at"),
            ("sort", "desc"),
        ],
        5,
    )
    .await
}

/// Fetch changed file paths for a specific MR (lightweight — no diff content).
pub async fn fetch_mr_changed_paths(
    cfg: &GitLabConfig,
    project_id: i64,
    mr_iid: u64,
) -> Result<Vec<String>, GitLabError> {
    let changes: MergeRequestChanges = fetch_mr_changes(cfg, project_id, mr_iid).await?;
    Ok(changes
        .changes
        .into_iter()
        .map(|c| c.new_path)
        .collect())
}

/// Fetch git blame for a file.
pub async fn fetch_blame(
    cfg: &GitLabConfig,
    project_id: i64,
    file_path: &str,
    ref_name: &str,
) -> Result<Vec<BlameEntry>, GitLabError> {
    let encoded_path = urlencoding::encode(file_path);
    get_json(
        cfg,
        &format!(
            "/projects/{}/repository/files/{}/blame",
            project_id, encoded_path
        ),
        &[("ref", ref_name)],
    )
    .await
}

/// Create a commit via the GitLab API (used as fallback when git push fails,
/// e.g., for fork-based MRs where the bot can't push to the fork).
pub async fn create_commit(
    cfg: &GitLabConfig,
    project_id: i64,
    branch: &str,
    commit_message: &str,
    actions: Vec<CommitAction>,
) -> Result<CommitResponse, GitLabError> {
    let url = format!(
        "{}/api/v4/projects/{}/repository/commits",
        cfg.base_url, project_id
    );
    let client = build_client();

    let body = serde_json::json!({
        "branch": branch,
        "commit_message": commit_message,
        "actions": actions,
    });

    let resp = client
        .post(&url)
        .headers(auth_headers(&cfg.token))
        .json(&body)
        .send()
        .await
        .map_err(|e| GitLabError::Network(e.to_string()))?;

    let resp = check_response(resp, "create_commit").await?;
    resp.json()
        .await
        .map_err(|e| GitLabError::Parse(e.to_string()))
}

/// Post a note (comment) on a merge request.
/// Used to notify the MR about successful sandbox fixes with commit links.
pub async fn post_mr_note(
    cfg: &GitLabConfig,
    project_id: i64,
    mr_iid: u64,
    body: &str,
) -> Result<Note, GitLabError> {
    let url = format!(
        "{}/api/v4/projects/{}/merge_requests/{}/notes",
        cfg.base_url, project_id, mr_iid
    );
    let client = build_client();

    let resp = client
        .post(&url)
        .headers(auth_headers(&cfg.token))
        .json(&serde_json::json!({ "body": body }))
        .send()
        .await
        .map_err(|e| GitLabError::Network(e.to_string()))?;

    let resp = check_response(resp, "post_mr_note").await?;
    resp.json()
        .await
        .map_err(|e| GitLabError::Parse(e.to_string()))
}

/// Reply to a specific discussion thread on a merge request.
/// This keeps the fix notification contextual — it appears as a reply
/// to the original review comment rather than a disconnected top-level note.
pub async fn reply_to_discussion(
    cfg: &GitLabConfig,
    project_id: i64,
    mr_iid: u64,
    discussion_id: &str,
    body: &str,
) -> Result<Note, GitLabError> {
    let url = format!(
        "{}/api/v4/projects/{}/merge_requests/{}/discussions/{}/notes",
        cfg.base_url, project_id, mr_iid, discussion_id
    );
    let client = build_client();

    let resp = client
        .post(&url)
        .headers(auth_headers(&cfg.token))
        .json(&serde_json::json!({ "body": body }))
        .send()
        .await
        .map_err(|e| GitLabError::Network(e.to_string()))?;

    let resp = check_response(resp, "reply_to_discussion").await?;
    resp.json()
        .await
        .map_err(|e| GitLabError::Parse(e.to_string()))
}

/// Find the discussion ID that contains a specific note (comment) ID.
/// Returns None if the note isn't found in any discussion.
pub async fn find_discussion_for_note(
    cfg: &GitLabConfig,
    project_id: i64,
    mr_iid: u64,
    note_id: i64,
) -> Result<Option<String>, GitLabError> {
    let discussions = fetch_mr_discussions(cfg, project_id, mr_iid).await?;
    for discussion in &discussions {
        for note in &discussion.notes {
            if note.id == note_id {
                return Ok(Some(discussion.id.clone()));
            }
        }
    }
    Ok(None)
}

/// Trigger a pipeline on a branch with optional variables.
pub async fn create_pipeline(
    cfg: &GitLabConfig,
    project_id: i64,
    ref_name: &str,
    variables: &[(&str, &str)],
) -> Result<Pipeline, GitLabError> {
    let url = format!(
        "{}/api/v4/projects/{}/pipeline",
        cfg.base_url, project_id
    );
    let client = build_client();

    let vars: Vec<serde_json::Value> = variables
        .iter()
        .map(|(k, v)| {
            serde_json::json!({
                "key": k,
                "value": v,
                "variable_type": "env_var"
            })
        })
        .collect();

    let body = serde_json::json!({
        "ref": ref_name,
        "variables": vars,
    });

    let resp = client
        .post(&url)
        .headers(auth_headers(&cfg.token))
        .json(&body)
        .send()
        .await
        .map_err(|e| GitLabError::Network(e.to_string()))?;

    let resp = check_response(resp, "create_pipeline").await?;
    resp.json()
        .await
        .map_err(|e| GitLabError::Parse(e.to_string()))
}

/// Get pipeline status.
pub async fn get_pipeline(
    cfg: &GitLabConfig,
    project_id: i64,
    pipeline_id: i64,
) -> Result<Pipeline, GitLabError> {
    get_json(
        cfg,
        &format!("/projects/{}/pipelines/{}", project_id, pipeline_id),
        &[],
    )
    .await
}

/// Fetch merge requests at the group level (across all projects in the group).
/// This maps to: https://gitlab.com/groups/gitlab-org/-/merge_requests
/// Used by harness to pick random MRs from the entire org.
pub async fn fetch_group_merge_requests(
    cfg: &GitLabConfig,
    group_path: &str,
    state: &str,
    per_page: usize,
    page: usize,
) -> Result<Vec<MergeRequest>, GitLabError> {
    let encoded = encode_project_path(group_path);
    let per_page_str = per_page.to_string();
    let page_str = page.to_string();
    get_json(
        cfg,
        &format!("/groups/{}/merge_requests", encoded),
        &[
            ("state", state),
            ("order_by", "updated_at"),
            ("sort", "desc"),
            ("per_page", &per_page_str),
            ("page", &page_str),
        ],
    )
    .await
}

/// Fetch projects under a group (for harness MR discovery).
/// Returns up to `max_pages` pages of projects, sorted by activity.
pub async fn fetch_group_projects(
    cfg: &GitLabConfig,
    group_path: &str,
    max_pages: usize,
) -> Result<Vec<Project>, GitLabError> {
    let encoded = encode_project_path(group_path);
    get_all_pages(
        cfg,
        &format!("/groups/{}/projects", encoded),
        &[
            ("order_by", "last_activity_at"),
            ("sort", "desc"),
            ("with_merge_requests_enabled", "true"),
            ("simple", "true"),
        ],
        max_pages,
    )
    .await
}

/// Fetch merged MRs for a project with discussion stats.
/// Used by harness to find MRs with review comments.
pub async fn fetch_merged_mrs_with_discussions(
    cfg: &GitLabConfig,
    project_id: i64,
    limit: usize,
) -> Result<Vec<MergeRequest>, GitLabError> {
    let limit_str = limit.to_string();
    get_json::<Vec<MergeRequest>>(
        cfg,
        &format!("/projects/{}/merge_requests", project_id),
        &[
            ("state", "merged"),
            ("order_by", "updated_at"),
            ("sort", "desc"),
            ("per_page", &limit_str),
        ],
    )
    .await
}

// ---------------------------------------------------------------------------
// Response types — only the fields we need, serde ignores the rest.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GitLabUser {
    pub id: i64,
    pub username: String,
    pub name: String,
    #[serde(default)]
    pub avatar_url: Option<String>,
}

/// Fetch a GitLab user by username. Returns None if not found.
pub async fn fetch_user_by_username(
    cfg: &GitLabConfig,
    username: &str,
) -> Result<Option<GitLabUser>, GitLabError> {
    let users: Vec<GitLabUser> = get_json(
        cfg,
        "/users",
        &[("username", username)],
    )
    .await?;
    Ok(users.into_iter().next())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Project {
    pub id: i64,
    pub path_with_namespace: String,
    pub name: String,
    pub default_branch: Option<String>,
    pub web_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MergeRequest {
    pub iid: u64,
    pub title: String,
    pub description: Option<String>,
    pub state: String,
    pub source_branch: String,
    pub target_branch: String,
    pub source_project_id: Option<i64>,
    pub target_project_id: Option<i64>,
    pub web_url: String,
    pub author: Option<MrAuthor>,
    pub merged_at: Option<String>,
    /// Whether the MR is a draft/WIP. Defaults to false if not present
    /// (e.g. older GitLab versions or list endpoints that omit it).
    #[serde(default)]
    pub draft: bool,
    /// Labels applied to the MR. Used for priority scoring (risk/security labels).
    #[serde(default)]
    pub labels: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MrAuthor {
    pub username: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MergeRequestChanges {
    pub iid: u64,
    pub title: String,
    pub description: Option<String>,
    pub source_branch: String,
    pub target_branch: String,
    pub changes: Vec<DiffChange>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiffChange {
    pub old_path: String,
    pub new_path: String,
    pub new_file: bool,
    pub renamed_file: bool,
    pub deleted_file: bool,
    pub diff: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TreeEntry {
    pub id: String,
    pub name: String,
    pub path: String,
    #[serde(rename = "type")]
    pub entry_type: String, // "blob" or "tree"
    pub mode: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Discussion {
    pub id: String,
    pub notes: Vec<Note>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Note {
    pub id: i64,
    pub body: String,
    pub author: NoteAuthor,
    pub created_at: String,
    pub system: bool,
    pub resolvable: bool,
    pub resolved: Option<bool>,
    pub position: Option<NotePosition>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NoteAuthor {
    pub username: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotePosition {
    pub new_path: Option<String>,
    pub old_path: Option<String>,
    pub new_line: Option<u32>,
    pub old_line: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlameEntry {
    pub lines: Vec<String>,
    pub commit: BlameCommit,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlameCommit {
    pub id: String,
    pub author_name: String,
    pub committed_date: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pipeline {
    pub id: i64,
    pub status: String,
    pub web_url: String,
    #[serde(rename = "ref")]
    pub ref_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitAction {
    pub action: String,        // "create", "update", "delete"
    pub file_path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitResponse {
    pub id: String,            // commit SHA
    pub short_id: String,
    pub title: String,
}

/// Create a merge request.
/// Used when fix_branch_mode is "new_branch" — Botto pushes to a new branch
/// and opens an MR targeting the original source branch.
pub async fn create_merge_request(
    cfg: &GitLabConfig,
    project_id: i64,
    source_branch: &str,
    target_branch: &str,
    title: &str,
    description: &str,
) -> Result<MergeRequest, GitLabError> {
    let url = format!(
        "{}/api/v4/projects/{}/merge_requests",
        cfg.base_url, project_id
    );
    let client = build_client();

    let body = serde_json::json!({
        "source_branch": source_branch,
        "target_branch": target_branch,
        "title": title,
        "description": description,
        "remove_source_branch_when_merged": true,
    });

    let resp = client
        .post(&url)
        .headers(auth_headers(&cfg.token))
        .json(&body)
        .send()
        .await
        .map_err(|e| GitLabError::Network(e.to_string()))?;

    let resp = check_response(resp, "create_merge_request").await?;
    resp.json()
        .await
        .map_err(|e| GitLabError::Parse(e.to_string()))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_next_link() {
        let header = r#"<https://gitlab.com/api/v4/projects/1/merge_requests?page=2&per_page=100>; rel="next", <https://gitlab.com/api/v4/projects/1/merge_requests?page=5&per_page=100>; rel="last""#;
        let next = parse_next_link(header);
        assert_eq!(
            next,
            Some("https://gitlab.com/api/v4/projects/1/merge_requests?page=2&per_page=100".to_string())
        );
    }

    #[test]
    fn test_parse_next_link_no_next() {
        let header = r#"<https://gitlab.com/api/v4/projects/1/merge_requests?page=5&per_page=100>; rel="last""#;
        let next = parse_next_link(header);
        assert!(next.is_none());
    }

    #[test]
    fn test_encode_project_path() {
        assert_eq!(encode_project_path("namespace/project"), "namespace%2Fproject");
    }
}
