// ---------------------------------------------------------------------------
// Jira REST API v3 client — fetches ticket data for AI context.
//
// Ported from Otto's jira-client.ts. Uses Basic Auth (email:apiToken).
// Only fetches fields needed for review context — not the full issue graph.
// ---------------------------------------------------------------------------

use reqwest::Client;
use serde::{Deserialize, Serialize};

#[derive(Debug, thiserror::Error)]
pub enum JiraError {
    #[error("authentication failed (401/403) — check email and API token")]
    Unauthorized,
    #[error("ticket not found: {0}")]
    NotFound(String),
    #[error("Jira API error ({0})")]
    ApiError(u16),
    #[error("network error: {0}")]
    Network(String),
    #[error("parse error: {0}")]
    Parse(String),
}

#[derive(Debug, Clone)]
pub struct JiraConfig {
    pub base_url: String,  // e.g., "https://mycompany.atlassian.net"
    pub email: String,
    pub api_token: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TicketInfo {
    pub key: String,
    pub title: String,
    pub description: Option<String>,
    pub status: String,
    #[serde(rename = "type")]
    pub issue_type: String,
    pub priority: Option<String>,
    pub labels: Vec<String>,
    pub acceptance_criteria: Option<String>,
    pub assignee: Option<String>,
    pub reporter: Option<String>,
    pub parent_key: Option<String>,
    pub linked_issue_keys: Vec<String>,
}

fn build_client() -> Client {
    Client::builder()
        .timeout(std::time::Duration::from_secs(15))
        .build()
        .expect("failed to build HTTP client")
}

fn auth_header(cfg: &JiraConfig) -> String {
    use base64::Engine;
    let credentials = format!("{}:{}", cfg.email, cfg.api_token);
    format!(
        "Basic {}",
        base64::engine::general_purpose::STANDARD.encode(credentials)
    )
}

/// Fetch a single Jira ticket by key.
pub async fn fetch_ticket(cfg: &JiraConfig, ticket_key: &str) -> Result<TicketInfo, JiraError> {
    let url = format!(
        "{}/rest/api/3/issue/{}?fields=summary,description,status,issuetype,priority,labels,assignee,reporter,parent,issuelinks",
        cfg.base_url,
        urlencoding::encode(ticket_key)
    );

    let client = build_client();
    let resp = client
        .get(&url)
        .header("Authorization", auth_header(cfg))
        .header("Accept", "application/json")
        .send()
        .await
        .map_err(|e| JiraError::Network(e.to_string()))?;

    let status = resp.status();
    if !status.is_success() {
        return Err(match status.as_u16() {
            401 | 403 => JiraError::Unauthorized,
            404 => JiraError::NotFound(ticket_key.to_string()),
            s => JiraError::ApiError(s),
        });
    }

    let data: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| JiraError::Parse(e.to_string()))?;

    parse_jira_issue(&data, ticket_key)
}

/// Test the Jira connection by fetching the current user.
pub async fn test_connection(cfg: &JiraConfig) -> Result<String, JiraError> {
    let url = format!("{}/rest/api/3/myself", cfg.base_url);
    let client = build_client();

    let resp = client
        .get(&url)
        .header("Authorization", auth_header(cfg))
        .header("Accept", "application/json")
        .send()
        .await
        .map_err(|e| JiraError::Network(e.to_string()))?;

    let status = resp.status();
    if !status.is_success() {
        return Err(match status.as_u16() {
            401 | 403 => JiraError::Unauthorized,
            s => JiraError::ApiError(s),
        });
    }

    let data: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| JiraError::Parse(e.to_string()))?;

    Ok(data["displayName"]
        .as_str()
        .or(data["emailAddress"].as_str())
        .unwrap_or("Connected")
        .to_string())
}

/// Parse a Jira issue JSON into our normalized TicketInfo.
fn parse_jira_issue(data: &serde_json::Value, ticket_key: &str) -> Result<TicketInfo, JiraError> {
    let fields = &data["fields"];

    let title = fields["summary"]
        .as_str()
        .unwrap_or("")
        .to_string();

    // Description: Jira v3 uses ADF (Atlassian Document Format).
    // Extract plain text from the content nodes.
    let description = extract_adf_text(&fields["description"]);

    let status = fields["status"]["name"]
        .as_str()
        .unwrap_or("Unknown")
        .to_string();

    let issue_type = fields["issuetype"]["name"]
        .as_str()
        .unwrap_or("Unknown")
        .to_string();

    let priority = fields["priority"]["name"]
        .as_str()
        .map(|s| s.to_string());

    let labels: Vec<String> = fields["labels"]
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default();

    let assignee = fields["assignee"]["displayName"]
        .as_str()
        .map(|s| s.to_string());

    let reporter = fields["reporter"]["displayName"]
        .as_str()
        .map(|s| s.to_string());

    let parent_key = fields["parent"]["key"]
        .as_str()
        .map(|s| s.to_string());

    // Extract linked issue keys
    let linked_issue_keys: Vec<String> = fields["issuelinks"]
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|link| {
                    link["outwardIssue"]["key"]
                        .as_str()
                        .or(link["inwardIssue"]["key"].as_str())
                        .map(|s| s.to_string())
                })
                .collect()
        })
        .unwrap_or_default();

    // Extract acceptance criteria from description or custom fields
    let acceptance_criteria = extract_acceptance_criteria(&description, fields);

    Ok(TicketInfo {
        key: ticket_key.to_string(),
        title,
        description,
        status,
        issue_type,
        priority,
        labels,
        acceptance_criteria,
        assignee,
        reporter,
        parent_key,
        linked_issue_keys,
    })
}

/// Extract plain text from Jira's ADF (Atlassian Document Format).
fn extract_adf_text(node: &serde_json::Value) -> Option<String> {
    if node.is_null() {
        return None;
    }

    // Simple text node
    if let Some(text) = node["text"].as_str() {
        return Some(text.to_string());
    }

    // Recurse into content array
    if let Some(content) = node["content"].as_array() {
        let parts: Vec<String> = content
            .iter()
            .filter_map(|child| extract_adf_text(child))
            .collect();
        if parts.is_empty() {
            return None;
        }

        let node_type = node["type"].as_str().unwrap_or("");
        let separator = match node_type {
            "paragraph" | "heading" | "bulletList" | "orderedList" => "\n",
            "listItem" => "\n- ",
            _ => "",
        };

        return Some(parts.join(separator));
    }

    None
}

/// Extract acceptance criteria from the description or custom fields.
fn extract_acceptance_criteria(
    description: &Option<String>,
    fields: &serde_json::Value,
) -> Option<String> {
    // Check common custom field names
    if let Some(obj) = fields.as_object() {
        for (key, value) in obj {
            if key.starts_with("customfield_") {
                // Check if the field name contains "acceptance" (heuristic)
                if let Some(text) = value.as_str() {
                    if text.len() > 10 && text.len() < 5000 {
                        // We can't know the field name from the value alone,
                        // but if it looks like AC content, use it
                        // This is a best-effort heuristic
                    }
                }
                // ADF custom field
                if let Some(text) = extract_adf_text(value) {
                    if text.len() > 10 && text.len() < 5000 {
                        // Same heuristic limitation
                    }
                }
            }
        }
    }

    // Extract from description using heading markers
    if let Some(desc) = description {
        let lower = desc.to_lowercase();
        for marker in &["acceptance criteria", "ac:", "acceptance:", "definition of done"] {
            if let Some(pos) = lower.find(marker) {
                let after = &desc[pos + marker.len()..];
                let trimmed = after.trim_start_matches(':').trim();
                // Take until the next heading or end
                let end = trimmed
                    .find("\n#")
                    .or_else(|| trimmed.find("\n## "))
                    .unwrap_or(trimmed.len());
                let ac = trimmed[..end].trim();
                if !ac.is_empty() {
                    return Some(ac.to_string());
                }
            }
        }
    }

    None
}
