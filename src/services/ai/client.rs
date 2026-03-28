// ---------------------------------------------------------------------------
// OpenAI-compatible AI client — reqwest + SSE streaming.
//
// Ported from Otto's ai-client.ts. Supports:
//   - Non-streaming chat completions (POST /chat/completions)
//   - Streaming chat completions (SSE, yields content deltas)
//   - Tool/function calling (for repo exploration)
//   - Model listing (GET /models)
//
// Design decisions:
//   - Returns typed errors, not Result<Value>.
//   - Streaming returns a channel receiver, not an async generator (Rust idiom).
//   - AbortSignal → CancellationToken for cooperative cancellation.
//   - SSE parsing is hand-rolled (same as Otto) for provider compatibility.
// ---------------------------------------------------------------------------

use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tracing::{debug, warn};

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum AiError {
    #[error("authentication failed (401) — check AI API key")]
    Unauthorized,
    #[error("rate limited (429) — try again later")]
    RateLimited,
    #[error("server error ({0})")]
    ServerError(u16),
    #[error("network error: {0}")]
    Network(String),
    #[error("parse error: {0}")]
    Parse(String),
    #[error("cancelled")]
    Cancelled,
}

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct AiClientConfig {
    pub base_url: String, // e.g., "http://localhost:8000/v1"
    pub api_key: String,
}

// ---------------------------------------------------------------------------
// Request/Response types (OpenAI-compatible)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatMessage {
    pub role: String, // "system" | "user" | "assistant" | "tool"
    pub content: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_calls: Option<Vec<ToolCall>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolCall {
    pub id: String,
    #[serde(rename = "type")]
    pub call_type: String,
    pub function: ToolCallFunction,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolCallFunction {
    pub name: String,
    pub arguments: String, // JSON string
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolDefinition {
    #[serde(rename = "type")]
    pub tool_type: String,
    pub function: FunctionDefinition,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionDefinition {
    pub name: String,
    pub description: String,
    pub parameters: serde_json::Value, // JSON Schema
}

#[derive(Debug, Clone, Serialize)]
pub struct ChatCompletionRequest {
    pub model: String,
    pub messages: Vec<ChatMessage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tools: Option<Vec<ToolDefinition>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_choice: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChatCompletionResponse {
    pub id: String,
    pub choices: Vec<Choice>,
    pub usage: Option<Usage>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Choice {
    pub index: u32,
    pub message: ChatMessage,
    pub finish_reason: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Usage {
    pub prompt_tokens: u32,
    pub completion_tokens: u32,
    pub total_tokens: u32,
}

/// A delta chunk from SSE streaming.
#[derive(Debug, Clone, Deserialize)]
pub struct StreamDelta {
    pub choices: Vec<StreamChoice>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StreamChoice {
    pub index: u32,
    pub delta: DeltaContent,
    pub finish_reason: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DeltaContent {
    pub content: Option<String>,
    pub tool_calls: Option<Vec<ToolCallDelta>>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ToolCallDelta {
    pub index: u32,
    pub id: Option<String>,
    pub function: Option<ToolCallFunctionDelta>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ToolCallFunctionDelta {
    pub name: Option<String>,
    pub arguments: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ModelList {
    pub data: Vec<Model>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Model {
    pub id: String,
}

// ---------------------------------------------------------------------------
// Client functions
// ---------------------------------------------------------------------------

fn build_client() -> Client {
    static CLIENT: std::sync::OnceLock<Client> = std::sync::OnceLock::new();
    CLIENT
        .get_or_init(|| {
            Client::builder()
                .timeout(std::time::Duration::from_secs(120))
                .build()
                .expect("failed to build HTTP client")
        })
        .clone()
}

/// Non-streaming chat completion. Returns the full response.
pub async fn chat_completion(
    cfg: &AiClientConfig,
    mut request: ChatCompletionRequest,
) -> Result<ChatCompletionResponse, AiError> {
    request.stream = Some(false);
    let url = format!("{}/chat/completions", cfg.base_url);
    let client = build_client();

    let resp = client
        .post(&url)
        .header("Authorization", format!("Bearer {}", cfg.api_key))
        .json(&request)
        .send()
        .await
        .map_err(|e| AiError::Network(e.to_string()))?;

    let status = resp.status();
    if !status.is_success() {
        return Err(match status.as_u16() {
            401 => AiError::Unauthorized,
            429 => AiError::RateLimited,
            s => AiError::ServerError(s),
        });
    }

    resp.json::<ChatCompletionResponse>()
        .await
        .map_err(|e| AiError::Parse(e.to_string()))
}

/// Streaming chat completion. Returns a channel that yields content deltas.
/// The channel closes when the stream ends or is cancelled.
pub async fn chat_completion_stream(
    cfg: &AiClientConfig,
    mut request: ChatCompletionRequest,
    cancel: tokio_util::sync::CancellationToken,
) -> Result<mpsc::Receiver<StreamEvent>, AiError> {
    request.stream = Some(true);
    let url = format!("{}/chat/completions", cfg.base_url);
    let client = build_client();

    let resp = client
        .post(&url)
        .header("Authorization", format!("Bearer {}", cfg.api_key))
        .json(&request)
        .send()
        .await
        .map_err(|e| AiError::Network(e.to_string()))?;

    let status = resp.status();
    if !status.is_success() {
        return Err(match status.as_u16() {
            401 => AiError::Unauthorized,
            429 => AiError::RateLimited,
            s => AiError::ServerError(s),
        });
    }

    let (tx, rx) = mpsc::channel::<StreamEvent>(64);

    // Spawn a task to read the SSE stream
    tokio::spawn(async move {
        use futures::StreamExt;
        let mut stream = resp.bytes_stream();
        let mut buffer = String::new();

        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    let _ = tx.send(StreamEvent::Done).await;
                    break;
                }
                chunk = stream.next() => {
                    match chunk {
                        Some(Ok(bytes)) => {
                            buffer.push_str(&String::from_utf8_lossy(&bytes));

                            // Process complete SSE lines
                            while let Some(line_end) = buffer.find('\n') {
                                let line = buffer[..line_end].trim_end_matches('\r').to_string();
                                buffer = buffer[line_end + 1..].to_string();

                                if line.is_empty() {
                                    continue;
                                }

                                if let Some(data) = line.strip_prefix("data: ") {
                                    if data.trim() == "[DONE]" {
                                        let _ = tx.send(StreamEvent::Done).await;
                                        return;
                                    }

                                    match serde_json::from_str::<StreamDelta>(data) {
                                        Ok(delta) => {
                                            // Extract content delta
                                            if let Some(choice) = delta.choices.first() {
                                                if let Some(ref content) = choice.delta.content {
                                                    if !content.is_empty() {
                                                        if tx.send(StreamEvent::Delta(content.clone())).await.is_err() {
                                                            return;
                                                        }
                                                    }
                                                }
                                                // Extract tool call deltas
                                                if let Some(ref tool_calls) = choice.delta.tool_calls {
                                                    for tc in tool_calls {
                                                        if tx.send(StreamEvent::ToolCallDelta(tc.clone())).await.is_err() {
                                                            return;
                                                        }
                                                    }
                                                }
                                                if choice.finish_reason.is_some() {
                                                    let _ = tx.send(StreamEvent::Done).await;
                                                    return;
                                                }
                                            }
                                        }
                                        Err(_) => {
                                            // Skip malformed chunks (same as Otto)
                                            debug!("skipping malformed SSE chunk: {}", data);
                                        }
                                    }
                                }
                            }
                        }
                        Some(Err(e)) => {
                            warn!("SSE stream error: {}", e);
                            let _ = tx.send(StreamEvent::Error(e.to_string())).await;
                            break;
                        }
                        None => {
                            let _ = tx.send(StreamEvent::Done).await;
                            break;
                        }
                    }
                }
            }
        }
    });

    Ok(rx)
}

/// Events emitted by the streaming client.
#[derive(Debug, Clone)]
pub enum StreamEvent {
    /// A content text delta.
    Delta(String),
    /// A tool call delta (for function calling).
    ToolCallDelta(ToolCallDelta),
    /// Stream completed normally.
    Done,
    /// Stream error.
    Error(String),
}

/// List available models.
pub async fn list_models(cfg: &AiClientConfig) -> Result<Vec<String>, AiError> {
    let url = format!("{}/models", cfg.base_url);
    let client = build_client();

    let resp = client
        .get(&url)
        .header("Authorization", format!("Bearer {}", cfg.api_key))
        .send()
        .await
        .map_err(|e| AiError::Network(e.to_string()))?;

    let status = resp.status();
    if !status.is_success() {
        return Err(match status.as_u16() {
            401 => AiError::Unauthorized,
            s => AiError::ServerError(s),
        });
    }

    let list: ModelList = resp
        .json()
        .await
        .map_err(|e| AiError::Parse(e.to_string()))?;

    Ok(list.data.into_iter().map(|m| m.id).collect())
}
