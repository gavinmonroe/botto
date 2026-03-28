// ---------------------------------------------------------------------------
// Work Discoverer — trait + connector-based implementation.
//
// The WorkDiscoverer trait abstracts how a directive finds new work items.
// ConnectorDiscoverer uses HTTP connectors from Mentor to poll sources.
// ---------------------------------------------------------------------------

use anyhow::{bail, Context, Result};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, ToSocketAddrs};
use tracing::{debug, info, warn};
use url::Url;

use super::types::{WorkItem, WorkSource};
use crate::services::ai::client::AiClientConfig;
use crate::services::mentor::client::MentorClient;
use crate::services::workflow::connector::{self, ConnectorResult, ConnectorSpec};

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// Discovers work items from external sources.
pub trait WorkDiscoverer: Send + Sync {
    /// Poll sources and return newly discovered work items.
    fn discover(
        &self,
        sources: &[WorkSource],
        directive_id: &str,
    ) -> impl std::future::Future<Output = Result<Vec<WorkItem>>> + Send;

    /// Which source types this discoverer supports.
    fn supported_types(&self) -> Vec<String>;
}

// ---------------------------------------------------------------------------
// ConnectorDiscoverer — uses Mentor connectors to poll sources
// ---------------------------------------------------------------------------

pub struct ConnectorDiscoverer {
    ai_config: AiClientConfig,
    ai_model: String,
    mentor: MentorClient,
}

impl ConnectorDiscoverer {
    pub fn new(ai_config: AiClientConfig, ai_model: String, mentor: MentorClient) -> Self {
        Self {
            ai_config,
            ai_model,
            mentor,
        }
    }

    /// Poll a single explicit source using a connector.
    async fn poll_explicit(
        &self,
        source_type: &str,
        url: Option<&str>,
        params: &serde_json::Value,
    ) -> Result<Vec<WorkItem>> {
        let description = format!(
            "Fetch items from {source_type}{}",
            url.map(|u| format!(" at {u}")).unwrap_or_default()
        );

        let connector_result = connector::find_or_build(
            &self.ai_config,
            &self.ai_model,
            &self.mentor,
            source_type,
            &description,
        )
        .await
        .context("find_or_build connector for explicit source")?;

        let spec = match connector_result {
            ConnectorResult::Found(s) | ConnectorResult::Built(s) => s,
            ConnectorResult::NeedsHuman { capability, reason } => {
                warn!(
                    %capability, %reason,
                    "connector not available for explicit source"
                );
                return Ok(Vec::new());
            }
        };

        self.execute_connector(&spec, source_type, url, params).await
    }

    /// Poll all connectors in Mentor matching a category (for inferred sources).
    async fn poll_inferred(
        &self,
        category: &str,
        filter: Option<&str>,
    ) -> Result<Vec<WorkItem>> {
        let query = format!("connector {category}");
        let results = self.mentor.query(&query, 20).await?;

        let mut items = Vec::new();
        for result in results {
            if result.category != "connector" {
                continue;
            }

            let spec = match serde_json::from_str::<ConnectorSpec>(&result.content) {
                Ok(s) => s,
                Err(e) => {
                    debug!(entry_id = %result.id, "skipping non-connector entry: {e}");
                    continue;
                }
            };

            // Apply filter if provided.
            if let Some(f) = filter {
                if !spec.name.contains(f) && !spec.description.to_lowercase().contains(&f.to_lowercase()) {
                    continue;
                }
            }

            match self
                .execute_connector(&spec, &spec.name, None, &serde_json::json!({}))
                .await
            {
                Ok(mut discovered) => items.append(&mut discovered),
                Err(e) => {
                    warn!(connector = %spec.name, "inferred poll failed: {e:#}");
                }
            }
        }

        Ok(items)
    }

    /// Execute a connector and parse the response into WorkItems.
    async fn execute_connector(
        &self,
        spec: &ConnectorSpec,
        source_type: &str,
        _url: Option<&str>,
        _params: &serde_json::Value,
    ) -> Result<Vec<WorkItem>> {
        // Find a "list" or first action to use for polling.
        let action = spec
            .actions
            .get("list")
            .or_else(|| spec.actions.get("search"))
            .or_else(|| spec.actions.values().next());

        let action = match action {
            Some(a) => a,
            None => {
                debug!(connector = %spec.name, "connector has no actions");
                return Ok(Vec::new());
            }
        };

        // Build the request URL.
        let url = format!("{}{}", spec.base_url, action.path);

        // Bug #8: SSRF protection — validate the URL before making any request.
        validate_connector_url(&url)?;

        // Resolve auth.
        let mut req = reqwest::Client::new().request(
            action.method.parse().unwrap_or(reqwest::Method::GET),
            &url,
        );

        match &spec.auth {
            connector::ConnectorAuth::Bearer { token_env } => {
                if let Ok(token) = std::env::var(token_env) {
                    req = req.header("Authorization", format!("Bearer {token}"));
                } else {
                    warn!(env = %token_env, "auth env var not set, skipping connector");
                    return Ok(Vec::new());
                }
            }
            connector::ConnectorAuth::Header {
                header_name,
                value_env,
            } => {
                if let Ok(val) = std::env::var(value_env) {
                    req = req.header(header_name.as_str(), val);
                }
            }
            connector::ConnectorAuth::BasicAuth {
                username_env,
                password_env,
            } => {
                // Bug #10: Fail if env vars are missing instead of sending empty credentials.
                let user = std::env::var(username_env).map_err(|_| {
                    anyhow::anyhow!(
                        "BasicAuth username env var '{}' is not set for connector '{}'",
                        username_env,
                        spec.name
                    )
                })?;
                let pass = std::env::var(password_env).map_err(|_| {
                    anyhow::anyhow!(
                        "BasicAuth password env var '{}' is not set for connector '{}'",
                        password_env,
                        spec.name
                    )
                })?;
                if user.is_empty() || pass.is_empty() {
                    bail!(
                        "BasicAuth credentials are empty for connector '{}' (check env vars '{}' and '{}')",
                        spec.name,
                        username_env,
                        password_env
                    );
                }
                req = req.basic_auth(user, Some(pass));
            }
            connector::ConnectorAuth::None => {}
        }

        // Add query params.
        for (k, v) in &action.query_params {
            req = req.query(&[(k, v)]);
        }

        // Add headers.
        for (k, v) in &action.headers {
            req = req.header(k.as_str(), v.as_str());
        }

        let resp = req
            .timeout(std::time::Duration::from_secs(30))
            .send()
            .await
            .context("connector HTTP request failed")?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            warn!(
                connector = %spec.name,
                %status,
                "connector returned error: {body}"
            );
            return Ok(Vec::new());
        }

        let body: serde_json::Value = resp
            .json()
            .await
            .context("parse connector response as JSON")?;

        parse_items_from_response(&body, source_type)
    }
}

#[allow(unused)]
impl WorkDiscoverer for ConnectorDiscoverer {
    async fn discover(
        &self,
        sources: &[WorkSource],
        directive_id: &str,
    ) -> Result<Vec<WorkItem>> {
        let mut all_items = Vec::new();

        for source in sources {
            let items = match source {
                WorkSource::Explicit {
                    source_type,
                    url,
                    params,
                } => {
                    self.poll_explicit(source_type, url.as_deref(), params)
                        .await
                }
                WorkSource::Inferred { category, filter } => {
                    self.poll_inferred(category, filter.as_deref()).await
                }
            };

            match items {
                Ok(mut discovered) => {
                    info!(
                        directive_id,
                        count = discovered.len(),
                        "discovered items from source"
                    );
                    all_items.append(&mut discovered);
                }
                Err(e) => {
                    warn!(directive_id, "source poll failed: {e:#}");
                }
            }
        }

        Ok(all_items)
    }

    fn supported_types(&self) -> Vec<String> {
        vec![
            "jira".into(),
            "github".into(),
            "gitlab".into(),
            "slack".into(),
            "http".into(),
        ]
    }
}

// ---------------------------------------------------------------------------
// SSRF protection — URL validation (mirrors http agent's validate_url)
// ---------------------------------------------------------------------------

/// Validate that a connector URL is safe to request (no SSRF).
///
/// Rejects non-HTTP(S) schemes, loopback, link-local, private networks,
/// and unspecified addresses. Same logic as the HTTP agent's validate_url.
fn validate_connector_url(raw_url: &str) -> Result<()> {
    let parsed = Url::parse(raw_url)
        .map_err(|e| anyhow::anyhow!("invalid connector URL '{}': {}", raw_url, e))?;

    match parsed.scheme() {
        "http" | "https" => {}
        scheme => {
            bail!(
                "connector URL scheme '{}' is not allowed; only http and https are permitted",
                scheme
            );
        }
    }

    let host = parsed
        .host_str()
        .ok_or_else(|| anyhow::anyhow!("connector URL has no host: {}", raw_url))?;

    let port = parsed.port_or_known_default().unwrap_or(80);
    let addr_str = format!("{host}:{port}");

    let addrs: Vec<std::net::SocketAddr> = addr_str
        .to_socket_addrs()
        .map_err(|e| anyhow::anyhow!("DNS resolution failed for connector host '{}': {}", host, e))?
        .collect();

    if addrs.is_empty() {
        bail!("DNS resolution returned no addresses for connector host '{}'", host);
    }

    for addr in &addrs {
        if is_dangerous_ip(&addr.ip()) {
            warn!(
                url = raw_url,
                resolved_ip = %addr.ip(),
                "connector: blocked request to internal/private IP"
            );
            bail!(
                "connector URL resolves to a blocked address ({}); requests to internal networks are not allowed",
                addr.ip()
            );
        }
    }

    Ok(())
}

/// Returns true if the IP address belongs to a loopback, link-local, private,
/// or otherwise dangerous network range.
fn is_dangerous_ip(ip: &IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => is_dangerous_ipv4(v4),
        IpAddr::V6(v6) => is_dangerous_ipv6(v6),
    }
}

fn is_dangerous_ipv4(ip: &Ipv4Addr) -> bool {
    ip.is_loopback()                          // 127.0.0.0/8
        || ip.is_unspecified()                // 0.0.0.0
        || ip.is_private()                    // 10/8, 172.16/12, 192.168/16
        || ip.is_link_local()                 // 169.254.0.0/16 (cloud metadata!)
        || ip.is_broadcast()                  // 255.255.255.255
        || ip.octets()[0] == 100 && (ip.octets()[1] & 0xC0) == 64  // 100.64.0.0/10 (CGNAT)
}

fn is_dangerous_ipv6(ip: &Ipv6Addr) -> bool {
    ip.is_loopback()                          // ::1
        || ip.is_unspecified()                // ::
        // fc00::/7 — unique local addresses (private)
        || (ip.segments()[0] & 0xfe00) == 0xfc00
        // fe80::/10 — link-local
        || (ip.segments()[0] & 0xffc0) == 0xfe80
        // IPv4-mapped IPv6 — check the embedded v4 address
        || ip.to_ipv4_mapped().map_or(false, |v4| is_dangerous_ipv4(&v4))
}

// ---------------------------------------------------------------------------
// Response parsing — extract work items from common JSON patterns
// ---------------------------------------------------------------------------

/// Parse raw JSON responses into WorkItem structs.
/// Handles common patterns: .issues, .items, .data, .results, or top-level array.
fn parse_items_from_response(
    body: &serde_json::Value,
    source_type: &str,
) -> Result<Vec<WorkItem>> {
    // Try common wrapper keys.
    let array = if let Some(arr) = body.as_array() {
        arr.clone()
    } else if let Some(arr) = body.get("issues").and_then(|v| v.as_array()) {
        arr.clone()
    } else if let Some(arr) = body.get("items").and_then(|v| v.as_array()) {
        arr.clone()
    } else if let Some(arr) = body.get("data").and_then(|v| v.as_array()) {
        arr.clone()
    } else if let Some(arr) = body.get("results").and_then(|v| v.as_array()) {
        arr.clone()
    } else {
        debug!("response is not an array or known wrapper, returning empty");
        return Ok(Vec::new());
    };

    let mut items = Vec::with_capacity(array.len());
    for entry in &array {
        let external_id = entry
            .get("id")
            .or_else(|| entry.get("key"))
            .or_else(|| entry.get("iid"))
            .or_else(|| entry.get("number"))
            .map(|v| match v {
                serde_json::Value::String(s) => s.clone(),
                other => other.to_string(),
            })
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

        let title = entry
            .get("title")
            .or_else(|| entry.get("summary"))
            .or_else(|| entry.get("name"))
            .and_then(|v| v.as_str())
            .unwrap_or("untitled")
            .to_string();

        let description = entry
            .get("description")
            .or_else(|| entry.get("body"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let source_url = entry
            .get("url")
            .or_else(|| entry.get("html_url"))
            .or_else(|| entry.get("web_url"))
            .or_else(|| entry.get("self"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        items.push(WorkItem {
            external_id,
            source_type: source_type.to_string(),
            source_url,
            title,
            description,
            metadata: entry.clone(),
        });
    }

    Ok(items)
}
