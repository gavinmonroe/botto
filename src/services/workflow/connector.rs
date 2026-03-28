// ---------------------------------------------------------------------------
// Connector Registry — build, store, and find HTTP-based connectors.
//
// When an agent needs a capability that doesn't exist, the Connector Builder
// tries to create one:
//   1. Check Mentor for an existing connector (category "connector")
//   2. Not found → AI generates a connector spec
//   3. Validate the spec structure
//   4. Store in Mentor for reuse
//
// Connectors are HTTP-only. Auth tokens are never stored — only env var names.
// A connector that fails repeatedly gets confidence-decayed and eventually
// pruned by Mentor's normal lifecycle.
// ---------------------------------------------------------------------------

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, info, warn};

use crate::services::ai::client::{
    self, AiClientConfig, ChatCompletionRequest, ChatMessage,
};
use crate::services::mentor::client::MentorClient;

// ---------------------------------------------------------------------------
// Connector spec types
// ---------------------------------------------------------------------------

/// A reusable HTTP connector stored in Mentor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorSpec {
    pub name: String,
    pub description: String,
    pub base_url: String,
    pub auth: ConnectorAuth,
    pub actions: HashMap<String, ConnectorAction>,
}

/// Authentication configuration — references env vars, never stores secrets.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ConnectorAuth {
    None,
    Bearer {
        token_env: String,
    },
    BasicAuth {
        username_env: String,
        password_env: String,
    },
    Header {
        header_name: String,
        value_env: String,
    },
}

/// A single action a connector can perform.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorAction {
    pub method: String,
    pub path: String,
    #[serde(default)]
    pub headers: HashMap<String, String>,
    #[serde(default)]
    pub query_params: HashMap<String, String>,
    pub body_template: Option<serde_json::Value>,
    #[serde(default)]
    pub response_mapping: HashMap<String, String>,
}

/// Result of a connector lookup or build attempt.
#[derive(Debug)]
pub enum ConnectorResult {
    /// Found an existing connector in Mentor.
    Found(ConnectorSpec),
    /// Built a new connector and stored it.
    Built(ConnectorSpec),
    /// Could not build — needs human help (e.g., missing credentials).
    NeedsHuman {
        capability: String,
        reason: String,
    },
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Find or build a connector for the given capability.
///
/// 1. Search Mentor for an existing connector
/// 2. If not found, use AI to generate one
/// 3. Validate and store the new connector
///
/// Budget: max 3 AI calls. Exceeding budget → NeedsHuman.
pub async fn find_or_build(
    ai_config: &AiClientConfig,
    ai_model: &str,
    mentor: &MentorClient,
    capability: &str,
    description: &str,
) -> Result<ConnectorResult> {
    info!(%capability, "connector registry: looking up capability");

    // 1. Check Mentor for existing connector.
    if let Some(spec) = find_existing(mentor, capability).await? {
        info!(%capability, connector = %spec.name, "found existing connector");
        return Ok(ConnectorResult::Found(spec));
    }

    debug!(%capability, "no existing connector found, attempting to build");

    // 2. Build a new one with AI.
    let spec = match build_connector(ai_config, ai_model, capability, description).await {
        Ok(spec) => spec,
        Err(e) => {
            warn!(%capability, "connector build failed: {e}");
            return Ok(ConnectorResult::NeedsHuman {
                capability: capability.to_string(),
                reason: format!("Failed to auto-build connector: {e}"),
            });
        }
    };

    // 3. Validate.
    if let Err(e) = validate_spec(&spec) {
        warn!(%capability, "built connector failed validation: {e}");
        return Ok(ConnectorResult::NeedsHuman {
            capability: capability.to_string(),
            reason: format!("Auto-built connector is invalid: {e}"),
        });
    }

    // 4. Store in Mentor.
    let content = serde_json::to_string(&spec).context("serialize connector spec")?;
    mentor
        .remember_for_repo(&content, "connector", None, None)
        .await
        .context("store connector in mentor")?;

    info!(
        %capability,
        connector = %spec.name,
        actions = spec.actions.len(),
        "built and stored new connector"
    );

    Ok(ConnectorResult::Built(spec))
}

/// Look up a connector by capability name without building.
pub async fn find_existing(
    mentor: &MentorClient,
    capability: &str,
) -> Result<Option<ConnectorSpec>> {
    let query = format!("connector {capability}");
    let results = mentor.query(&query, 5).await?;

    for result in results {
        if result.category != "connector" {
            continue;
        }
        match serde_json::from_str::<ConnectorSpec>(&result.content) {
            Ok(spec) => {
                // Check if this connector actually provides the capability.
                if spec.name.contains(capability)
                    || spec.description.to_lowercase().contains(&capability.to_lowercase())
                    || spec.actions.contains_key(capability)
                {
                    return Ok(Some(spec));
                }
            }
            Err(e) => {
                debug!(
                    entry_id = %result.id,
                    "mentor entry tagged as connector but failed to parse: {e}"
                );
            }
        }
    }

    Ok(None)
}

// ---------------------------------------------------------------------------
// Connector builder — AI-powered
// ---------------------------------------------------------------------------

/// Budget: max 3 AI calls to build a connector.
const MAX_BUILD_ATTEMPTS: u32 = 3;

async fn build_connector(
    ai_config: &AiClientConfig,
    ai_model: &str,
    capability: &str,
    description: &str,
) -> Result<ConnectorSpec> {
    let system_prompt = build_system_prompt();
    let user_prompt = format!(
        "Build an HTTP connector for the following capability:\n\n\
         Capability: {capability}\n\
         Description: {description}\n\n\
         Generate the connector spec JSON."
    );

    for attempt in 1..=MAX_BUILD_ATTEMPTS {
        debug!(%capability, attempt, "connector build attempt");

        let request = ChatCompletionRequest {
            model: ai_model.to_string(),
            messages: vec![
                ChatMessage {
                    role: "system".into(),
                    content: Some(system_prompt.clone()),
                    tool_calls: None,
                    tool_call_id: None,
                },
                ChatMessage {
                    role: "user".into(),
                    content: Some(user_prompt.clone()),
                    tool_calls: None,
                    tool_call_id: None,
                },
            ],
            temperature: Some(0.2),
            max_tokens: Some(2048),
            stream: None,
            tools: None,
            tool_choice: None,
        };

        let resp = client::chat_completion(ai_config, request).await;

        let response_text = match resp {
            Ok(r) => r
                .choices
                .first()
                .and_then(|c| c.message.content.clone())
                .unwrap_or_default(),
            Err(e) => {
                warn!(%capability, attempt, "AI call failed: {e}");
                if attempt == MAX_BUILD_ATTEMPTS {
                    bail!("all {MAX_BUILD_ATTEMPTS} AI attempts failed, last error: {e}");
                }
                continue;
            }
        };

        match parse_connector_response(&response_text) {
            Ok(spec) => return Ok(spec),
            Err(e) => {
                warn!(%capability, attempt, "failed to parse connector response: {e}");
                if attempt == MAX_BUILD_ATTEMPTS {
                    bail!("all {MAX_BUILD_ATTEMPTS} parse attempts failed, last error: {e}");
                }
            }
        }
    }

    unreachable!()
}

fn build_system_prompt() -> String {
    r#"You are a connector builder. Given a capability description, generate an HTTP connector specification.

Respond with ONLY a JSON object (no markdown, no explanation):
{
  "name": "capability_name",
  "description": "What this connector does",
  "base_url": "https://api.example.com",
  "auth": {
    "type": "bearer",
    "token_env": "ENV_VAR_NAME_FOR_TOKEN"
  },
  "actions": {
    "action_name": {
      "method": "GET",
      "path": "/api/v1/resource/{id}",
      "headers": {},
      "query_params": {},
      "body_template": null,
      "response_mapping": {
        "field_name": "$.json.path"
      }
    }
  }
}

Auth types: "none", "bearer" (with token_env), "basic_auth" (with username_env + password_env), "header" (with header_name + value_env).

Rules:
- NEVER include actual secrets or tokens — only environment variable names.
- Use descriptive env var names like JIRA_API_TOKEN, SLACK_BOT_TOKEN.
- Path parameters use {param_name} syntax.
- response_mapping uses JSONPath-like dot notation.
- Keep it minimal — only include actions needed for the capability."#
        .to_string()
}

// ---------------------------------------------------------------------------
// Response parsing
// ---------------------------------------------------------------------------

fn parse_connector_response(response: &str) -> Result<ConnectorSpec> {
    let trimmed = response.trim();

    // Direct parse.
    if let Ok(spec) = serde_json::from_str::<ConnectorSpec>(trimmed) {
        return Ok(spec);
    }

    // Code fence extraction.
    if let Some(json) = extract_fenced_json(trimmed) {
        if let Ok(spec) = serde_json::from_str::<ConnectorSpec>(&json) {
            return Ok(spec);
        }
    }

    // Brace extraction.
    if let Some(json) = extract_brace_block(trimmed) {
        if let Ok(spec) = serde_json::from_str::<ConnectorSpec>(&json) {
            return Ok(spec);
        }
    }

    bail!("could not parse connector spec from AI response")
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

fn validate_spec(spec: &ConnectorSpec) -> Result<()> {
    if spec.name.is_empty() {
        bail!("connector name is empty");
    }
    if spec.base_url.is_empty() {
        bail!("connector base_url is empty");
    }
    if spec.actions.is_empty() {
        bail!("connector has no actions");
    }

    for (name, action) in &spec.actions {
        let method = action.method.to_uppercase();
        if !matches!(method.as_str(), "GET" | "POST" | "PUT" | "PATCH" | "DELETE" | "HEAD") {
            bail!("action '{name}' has invalid HTTP method: {}", action.method);
        }
        if action.path.is_empty() {
            bail!("action '{name}' has empty path");
        }
    }

    // Validate auth doesn't contain actual secrets (basic heuristic).
    let auth_json = serde_json::to_string(&spec.auth).unwrap_or_default();
    if auth_json.contains("sk-")
        || auth_json.contains("xoxb-")
        || auth_json.contains("ghp_")
        || auth_json.contains("glpat-")
    {
        bail!("auth appears to contain an actual secret — only env var names are allowed");
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// JSON extraction helpers
// ---------------------------------------------------------------------------

fn extract_fenced_json(text: &str) -> Option<String> {
    let fence_start = text.find("```json").or_else(|| text.find("```"))?;
    let content_start = text[fence_start..].find('\n')? + fence_start + 1;
    let content_end = text[content_start..].find("```")? + content_start;
    let content = text[content_start..content_end].trim();
    if content.is_empty() {
        None
    } else {
        Some(content.to_string())
    }
}

fn extract_brace_block(text: &str) -> Option<String> {
    let start = text.find('{')?;
    let mut depth = 0i32;
    let mut in_string = false;
    let mut escape_next = false;

    for (i, ch) in text[start..].char_indices() {
        if escape_next {
            escape_next = false;
            continue;
        }
        match ch {
            '\\' if in_string => escape_next = true,
            '"' => in_string = !in_string,
            '{' if !in_string => depth += 1,
            '}' if !in_string => {
                depth -= 1;
                if depth == 0 {
                    return Some(text[start..start + i + 1].to_string());
                }
            }
            _ => {}
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Connector execution
// ---------------------------------------------------------------------------

/// Execute a connector action: resolve auth, build the HTTP request, execute it,
/// and return the response body as a JSON value.
pub async fn execute_connector(
    spec: &ConnectorSpec,
    action_name: &str,
    params: &HashMap<String, String>,
) -> Result<serde_json::Value> {
    let action = spec
        .actions
        .get(action_name)
        .ok_or_else(|| anyhow::anyhow!(
            "connector '{}' has no action '{}'",
            spec.name,
            action_name
        ))?;

    // Resolve auth from environment variables.
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .context("build HTTP client for connector")?;

    // Build the URL with path parameter substitution.
    let mut path = action.path.clone();
    for (key, value) in params {
        path = path.replace(&format!("{{{}}}", key), value);
    }
    let url = format!("{}{}", spec.base_url, path);

    // Build the request.
    let method = reqwest::Method::from_bytes(action.method.to_uppercase().as_bytes())
        .map_err(|_| anyhow::anyhow!("invalid HTTP method: {}", action.method))?;

    let mut request = client.request(method, &url);

    // Apply auth.
    request = apply_auth(request, &spec.auth)?;

    // Apply static headers from the action.
    for (key, value) in &action.headers {
        request = request.header(key.as_str(), value.as_str());
    }

    // Apply query params (static + dynamic from params).
    let mut query_params: Vec<(String, String)> = action
        .query_params
        .iter()
        .map(|(k, v)| {
            let mut val = v.clone();
            for (pk, pv) in params {
                val = val.replace(&format!("{{{}}}", pk), pv);
            }
            (k.clone(), val)
        })
        .collect();
    // Add any params that weren't consumed by path substitution as query params.
    for (key, value) in params {
        if !action.path.contains(&format!("{{{}}}", key))
            && !action.query_params.contains_key(key)
        {
            query_params.push((key.clone(), value.clone()));
        }
    }
    if !query_params.is_empty() {
        request = request.query(&query_params);
    }

    // Apply body template if present.
    if let Some(ref template) = action.body_template {
        let mut body = template.clone();
        substitute_body_params(&mut body, params);
        request = request.json(&body);
    }

    debug!(
        connector = %spec.name,
        action = action_name,
        url = %url,
        "executing connector action"
    );

    let response = request
        .send()
        .await
        .context("connector HTTP request failed")?;

    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        bail!(
            "connector '{}' action '{}' returned HTTP {}: {}",
            spec.name,
            action_name,
            status,
            body
        );
    }

    let body: serde_json::Value = response
        .json()
        .await
        .context("parse connector response as JSON")?;

    info!(
        connector = %spec.name,
        action = action_name,
        "connector action executed successfully"
    );

    Ok(body)
}

/// Apply authentication to a request builder based on the connector's auth config.
fn apply_auth(
    request: reqwest::RequestBuilder,
    auth: &ConnectorAuth,
) -> Result<reqwest::RequestBuilder> {
    match auth {
        ConnectorAuth::None => Ok(request),
        ConnectorAuth::Bearer { token_env } => {
            let token = std::env::var(token_env)
                .with_context(|| format!("missing env var '{}' for bearer auth", token_env))?;
            Ok(request.bearer_auth(token))
        }
        ConnectorAuth::BasicAuth {
            username_env,
            password_env,
        } => {
            let username = std::env::var(username_env)
                .with_context(|| format!("missing env var '{}' for basic auth", username_env))?;
            let password = std::env::var(password_env)
                .with_context(|| format!("missing env var '{}' for basic auth", password_env))?;
            Ok(request.basic_auth(username, Some(password)))
        }
        ConnectorAuth::Header {
            header_name,
            value_env,
        } => {
            let value = std::env::var(value_env)
                .with_context(|| format!("missing env var '{}' for header auth", value_env))?;
            Ok(request.header(header_name.as_str(), value.as_str()))
        }
    }
}

/// Recursively substitute `{param}` placeholders in a JSON body template.
fn substitute_body_params(value: &mut serde_json::Value, params: &HashMap<String, String>) {
    match value {
        serde_json::Value::String(s) => {
            for (key, val) in params {
                *s = s.replace(&format!("{{{}}}", key), val);
            }
        }
        serde_json::Value::Object(map) => {
            for v in map.values_mut() {
                substitute_body_params(v, params);
            }
        }
        serde_json::Value::Array(arr) => {
            for v in arr.iter_mut() {
                substitute_body_params(v, params);
            }
        }
        _ => {}
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_spec() -> ConnectorSpec {
        ConnectorSpec {
            name: "jira_read".into(),
            description: "Read Jira tickets".into(),
            base_url: "https://company.atlassian.net".into(),
            auth: ConnectorAuth::Bearer {
                token_env: "JIRA_API_TOKEN".into(),
            },
            actions: {
                let mut m = HashMap::new();
                m.insert(
                    "get_ticket".into(),
                    ConnectorAction {
                        method: "GET".into(),
                        path: "/rest/api/3/issue/{ticket_key}".into(),
                        headers: HashMap::new(),
                        query_params: HashMap::new(),
                        body_template: None,
                        response_mapping: {
                            let mut r = HashMap::new();
                            r.insert("summary".into(), "$.fields.summary".into());
                            r
                        },
                    },
                );
                m
            },
        }
    }

    // -- validate_spec -------------------------------------------------------

    #[test]
    fn validate_valid_spec() {
        assert!(validate_spec(&valid_spec()).is_ok());
    }

    #[test]
    fn validate_empty_name() {
        let mut spec = valid_spec();
        spec.name = String::new();
        assert!(validate_spec(&spec).is_err());
    }

    #[test]
    fn validate_empty_base_url() {
        let mut spec = valid_spec();
        spec.base_url = String::new();
        assert!(validate_spec(&spec).is_err());
    }

    #[test]
    fn validate_no_actions() {
        let mut spec = valid_spec();
        spec.actions.clear();
        assert!(validate_spec(&spec).is_err());
    }

    #[test]
    fn validate_bad_method() {
        let mut spec = valid_spec();
        spec.actions.values_mut().next().unwrap().method = "INVALID".into();
        assert!(validate_spec(&spec).is_err());
    }

    #[test]
    fn validate_empty_path() {
        let mut spec = valid_spec();
        spec.actions.values_mut().next().unwrap().path = String::new();
        assert!(validate_spec(&spec).is_err());
    }

    #[test]
    fn validate_rejects_embedded_secrets() {
        let spec = ConnectorSpec {
            auth: ConnectorAuth::Bearer {
                token_env: "sk-abc123realtoken".into(),
            },
            ..valid_spec()
        };
        assert!(validate_spec(&spec).is_err());
    }

    // -- parse_connector_response --------------------------------------------

    #[test]
    fn parse_direct_json() {
        let json = serde_json::to_string(&valid_spec()).unwrap();
        let spec = parse_connector_response(&json).unwrap();
        assert_eq!(spec.name, "jira_read");
    }

    #[test]
    fn parse_fenced_json() {
        let json = serde_json::to_string(&valid_spec()).unwrap();
        let text = format!("Here's the connector:\n```json\n{json}\n```\nDone.");
        let spec = parse_connector_response(&text).unwrap();
        assert_eq!(spec.name, "jira_read");
    }

    #[test]
    fn parse_garbage_fails() {
        assert!(parse_connector_response("not json at all").is_err());
    }

    // -- ConnectorAuth serde -------------------------------------------------

    #[test]
    fn auth_roundtrip_bearer() {
        let auth = ConnectorAuth::Bearer {
            token_env: "MY_TOKEN".into(),
        };
        let json = serde_json::to_string(&auth).unwrap();
        let back: ConnectorAuth = serde_json::from_str(&json).unwrap();
        match back {
            ConnectorAuth::Bearer { token_env } => assert_eq!(token_env, "MY_TOKEN"),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn auth_roundtrip_none() {
        let auth = ConnectorAuth::None;
        let json = serde_json::to_string(&auth).unwrap();
        let back: ConnectorAuth = serde_json::from_str(&json).unwrap();
        assert!(matches!(back, ConnectorAuth::None));
    }

    #[test]
    fn auth_roundtrip_basic_auth() {
        let auth = ConnectorAuth::BasicAuth {
            username_env: "MY_USER".into(),
            password_env: "MY_PASS".into(),
        };
        let json = serde_json::to_string(&auth).unwrap();
        let back: ConnectorAuth = serde_json::from_str(&json).unwrap();
        match back {
            ConnectorAuth::BasicAuth {
                username_env,
                password_env,
            } => {
                assert_eq!(username_env, "MY_USER");
                assert_eq!(password_env, "MY_PASS");
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn auth_roundtrip_header() {
        let auth = ConnectorAuth::Header {
            header_name: "X-Api-Key".into(),
            value_env: "API_KEY_VAR".into(),
        };
        let json = serde_json::to_string(&auth).unwrap();
        let back: ConnectorAuth = serde_json::from_str(&json).unwrap();
        match back {
            ConnectorAuth::Header {
                header_name,
                value_env,
            } => {
                assert_eq!(header_name, "X-Api-Key");
                assert_eq!(value_env, "API_KEY_VAR");
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn connector_spec_serde_roundtrip() {
        let spec = valid_spec();
        let json = serde_json::to_string(&spec).unwrap();
        let back: ConnectorSpec = serde_json::from_str(&json).unwrap();
        assert_eq!(back.name, spec.name);
        assert_eq!(back.base_url, spec.base_url);
        assert_eq!(back.actions.len(), spec.actions.len());
        assert!(back.actions.contains_key("get_ticket"));
    }
}
