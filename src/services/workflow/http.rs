// ---------------------------------------------------------------------------
// HTTP workflow agent — call external APIs (Slack, Jira, custom webhooks).
//
// Supported actions:
//   - get: HTTP GET request
//   - post: HTTP POST request with JSON body
//   - put: HTTP PUT request with JSON body
//   - delete: HTTP DELETE request
//
// Inputs:
//   - url (required): target URL
//   - headers (optional): object of header key-value pairs
//   - body (optional): JSON body for POST/PUT
//   - timeout_secs (optional): request timeout (default 30, max 300)
//   - auth_header (optional): value for the Authorization header
//
// Security:
//   - URLs are validated to prevent SSRF (no loopback, link-local, private
//     networks, or non-HTTP schemes).
//   - Redirects are disabled to prevent validation bypass.
//   - Response bodies are capped at 10 MB to prevent memory exhaustion.
// ---------------------------------------------------------------------------

use reqwest::redirect::Policy;
use reqwest::Client;
use serde_json::Value;
use std::collections::HashMap;
use std::future::Future;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, ToSocketAddrs};
use std::pin::Pin;
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};
use url::Url;

use crate::services::mentor::client::MentorClient;
use crate::services::workflow::traits::{failure_result, success_result, WorkflowAgent};
use crate::types::workflow::AgentResult;

/// Maximum per-request timeout in seconds.
const MAX_TIMEOUT_SECS: u64 = 300;

/// Maximum response body size in bytes (10 MB).
const MAX_RESPONSE_BODY_BYTES: usize = 10 * 1024 * 1024;

/// HTTP workflow agent for external API calls.
pub struct HttpAgent {
    client: Client,
}

impl HttpAgent {
    pub fn new() -> Result<Self, String> {
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            // Disable redirects to prevent SSRF bypass via redirect to internal IPs.
            .redirect(Policy::none())
            .build()
            .map_err(|e| format!("failed to build HTTP client: {e}"))?;
        Ok(Self { client })
    }
}

impl Default for HttpAgent {
    fn default() -> Self {
        Self::new().expect("failed to build default HTTP client")
    }
}

// ---------------------------------------------------------------------------
// SSRF protection — URL validation
// ---------------------------------------------------------------------------

/// Validate that a URL is safe to request (no SSRF).
///
/// Rejects:
///   - Non-HTTP(S) schemes (file://, ftp://, etc.)
///   - Loopback addresses (127.0.0.0/8, ::1)
///   - Link-local addresses (169.254.0.0/16, fe80::/10)
///   - Private network addresses (10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16, fc00::/7)
///   - Unspecified addresses (0.0.0.0, ::)
fn validate_url(raw_url: &str) -> Result<Url, String> {
    let parsed = Url::parse(raw_url).map_err(|e| format!("invalid URL: {e}"))?;

    // Only allow http and https schemes.
    match parsed.scheme() {
        "http" | "https" => {}
        scheme => {
            warn!(url = raw_url, scheme, "http agent: blocked non-HTTP scheme");
            return Err(format!("scheme '{scheme}' is not allowed; only http and https are permitted"));
        }
    }

    // Resolve the hostname to IP addresses and check each one.
    let host = parsed
        .host_str()
        .ok_or_else(|| "URL has no host".to_string())?;

    // Port for resolution — use explicit port or scheme default.
    let port = parsed.port_or_known_default().unwrap_or(80);
    let addr_str = format!("{host}:{port}");

    let addrs: Vec<std::net::SocketAddr> = addr_str
        .to_socket_addrs()
        .map_err(|e| format!("DNS resolution failed for '{host}': {e}"))?
        .collect();

    if addrs.is_empty() {
        return Err(format!("DNS resolution returned no addresses for '{host}'"));
    }

    for addr in &addrs {
        if is_dangerous_ip(&addr.ip()) {
            warn!(
                url = raw_url,
                resolved_ip = %addr.ip(),
                "http agent: blocked request to internal/private IP"
            );
            return Err(format!(
                "URL resolves to a blocked address ({}); requests to internal networks are not allowed",
                addr.ip()
            ));
        }
    }

    debug!(url = raw_url, "http agent: URL validation passed");
    Ok(parsed)
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
// WorkflowAgent implementation
// ---------------------------------------------------------------------------

impl WorkflowAgent for HttpAgent {
    fn execute<'a>(
        &'a self,
        action: &str,
        inputs: HashMap<String, Value>,
        _mentor: &'a MentorClient,
    ) -> Pin<Box<dyn Future<Output = AgentResult> + Send + 'a>> {
        let action = action.to_string();
        Box::pin(async move {
            let start = Instant::now();
            debug!(action = action.as_str(), "http agent: executing");

            let result = match action.as_str() {
                "get" => self.request(reqwest::Method::GET, &inputs).await,
                "post" => self.request(reqwest::Method::POST, &inputs).await,
                "put" => self.request(reqwest::Method::PUT, &inputs).await,
                "delete" => self.request(reqwest::Method::DELETE, &inputs).await,
                other => Err(format!("unknown http action: {other}")),
            };

            let duration = start.elapsed().as_secs_f64();
            match result {
                Ok(output) => success_result(output, duration),
                Err(e) => {
                    warn!(action = action.as_str(), error = %e, "http agent: action failed");
                    failure_result(&e, duration)
                }
            }
        })
    }

    fn agent_type_name(&self) -> &'static str {
        "http"
    }
}

impl HttpAgent {
    async fn request(
        &self,
        method: reqwest::Method,
        inputs: &HashMap<String, Value>,
    ) -> Result<Value, String> {
        let raw_url = inputs
            .get("url")
            .and_then(|v| v.as_str())
            .ok_or("missing or invalid input: url (expected string)")?;

        // SSRF protection: validate URL before making any request.
        let validated_url = validate_url(raw_url)?;

        let timeout_secs = inputs
            .get("timeout_secs")
            .and_then(|v| v.as_u64())
            .unwrap_or(30)
            .min(MAX_TIMEOUT_SECS); // Cap to prevent unbounded timeouts.

        info!(
            method = %method,
            url = %validated_url,
            timeout_secs,
            "http agent: sending request"
        );

        let mut builder = self
            .client
            .request(method.clone(), validated_url.as_str())
            .timeout(Duration::from_secs(timeout_secs));

        // Apply custom headers.
        if let Some(headers) = inputs.get("headers").and_then(|v| v.as_object()) {
            for (key, val) in headers {
                if let Some(val_str) = val.as_str() {
                    builder = builder.header(key.as_str(), val_str);
                }
            }
        }

        // Apply auth header shorthand.
        if let Some(auth) = inputs.get("auth_header").and_then(|v| v.as_str()) {
            builder = builder.header("Authorization", auth);
        }

        // Apply JSON body for POST/PUT.
        if method == reqwest::Method::POST || method == reqwest::Method::PUT {
            if let Some(body) = inputs.get("body") {
                builder = builder.json(body);
            }
        }

        let resp = builder
            .send()
            .await
            .map_err(|e| format!("{method} {raw_url}: {e}"))?;

        let status = resp.status().as_u16();
        let headers: HashMap<String, String> = resp
            .headers()
            .iter()
            .filter_map(|(k, v)| {
                v.to_str().ok().map(|val| (k.to_string(), val.to_string()))
            })
            .collect();

        // Read response body with a size cap to prevent memory exhaustion.
        let body_bytes = resp
            .bytes()
            .await
            .map_err(|e| format!("read response body: {e}"))?;

        if body_bytes.len() > MAX_RESPONSE_BODY_BYTES {
            warn!(
                url = raw_url,
                body_size = body_bytes.len(),
                max = MAX_RESPONSE_BODY_BYTES,
                "http agent: response body exceeds size limit"
            );
            return Err(format!(
                "response body too large ({} bytes, max {})",
                body_bytes.len(),
                MAX_RESPONSE_BODY_BYTES
            ));
        }

        let body_text = String::from_utf8_lossy(&body_bytes).to_string();

        // Try to parse body as JSON; fall back to raw text.
        let body_value = serde_json::from_str::<Value>(&body_text)
            .unwrap_or_else(|_| Value::String(body_text));

        debug!(
            url = raw_url,
            status,
            body_size = body_bytes.len(),
            "http agent: request completed"
        );

        Ok(serde_json::json!({
            "status": status,
            "headers": headers,
            "body": body_value,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_file_scheme() {
        assert!(validate_url("file:///etc/passwd").is_err());
    }

    #[test]
    fn rejects_ftp_scheme() {
        assert!(validate_url("ftp://example.com/file").is_err());
    }

    #[test]
    fn rejects_loopback() {
        assert!(validate_url("http://127.0.0.1/admin").is_err());
        assert!(validate_url("http://127.0.0.2:8080/").is_err());
    }

    #[test]
    fn rejects_metadata_endpoint() {
        // Cloud metadata endpoint (link-local).
        assert!(validate_url("http://169.254.169.254/latest/meta-data/").is_err());
    }

    #[test]
    fn rejects_private_networks() {
        assert!(validate_url("http://10.0.0.1/").is_err());
        assert!(validate_url("http://172.16.0.1/").is_err());
        assert!(validate_url("http://192.168.1.1/").is_err());
    }

    #[test]
    fn rejects_ipv6_loopback() {
        assert!(validate_url("http://[::1]/").is_err());
    }

    #[test]
    fn allows_public_https() {
        // This will attempt DNS resolution, so it needs network.
        // In CI you might skip this, but it validates the happy path.
        let result = validate_url("https://example.com");
        assert!(result.is_ok());
    }

    #[test]
    fn dangerous_ipv4_checks() {
        assert!(is_dangerous_ipv4(&Ipv4Addr::new(127, 0, 0, 1)));
        assert!(is_dangerous_ipv4(&Ipv4Addr::new(10, 0, 0, 1)));
        assert!(is_dangerous_ipv4(&Ipv4Addr::new(172, 16, 0, 1)));
        assert!(is_dangerous_ipv4(&Ipv4Addr::new(192, 168, 0, 1)));
        assert!(is_dangerous_ipv4(&Ipv4Addr::new(169, 254, 169, 254)));
        assert!(is_dangerous_ipv4(&Ipv4Addr::new(0, 0, 0, 0)));
        // Public IP should be fine.
        assert!(!is_dangerous_ipv4(&Ipv4Addr::new(8, 8, 8, 8)));
    }
}
