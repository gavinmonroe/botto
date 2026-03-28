// ---------------------------------------------------------------------------
// AI workflow agent — wraps the existing AI client for workflow steps.
//
// Supported actions:
//   - chat: send a chat completion request (non-streaming)
//   - summarize: summarize text content
//   - analyze: analyze code or content with a custom prompt
//   - decide: make a decision given context and options
//
// The agent builds ChatCompletionRequests from the step inputs and returns
// the AI response as structured JSON output.
// ---------------------------------------------------------------------------

use serde_json::Value;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::time::Instant;
use tracing::{debug, warn};

use crate::services::ai::client::{
    self as ai, AiClientConfig, ChatCompletionRequest, ChatMessage,
};
use crate::services::mentor::client::MentorClient;
use crate::services::workflow::traits::{failure_result, success_result, WorkflowAgent};
use crate::types::workflow::AgentResult;

/// AI workflow agent — delegates to the existing OpenAI-compatible AI client.
pub struct AiAgent {
    config: AiClientConfig,
    /// Default model to use when not specified in inputs.
    default_model: String,
}

impl AiAgent {
    pub fn new(config: AiClientConfig, default_model: String) -> Self {
        Self {
            config,
            default_model,
        }
    }
}

impl WorkflowAgent for AiAgent {
    fn execute<'a>(
        &'a self,
        action: &str,
        inputs: HashMap<String, Value>,
        mentor: &'a MentorClient,
    ) -> Pin<Box<dyn Future<Output = AgentResult> + Send + 'a>> {
        let action = action.to_string();
        Box::pin(async move {
            let start = Instant::now();
            debug!(action = action.as_str(), "ai agent: executing");

            let result = match action.as_str() {
                "chat" => self.chat(&inputs).await,
                "summarize" => self.summarize(&inputs).await,
                "analyze" => self.analyze(&inputs).await,
                "decide" => self.decide(&inputs, mentor).await,
                other => Err(format!("unknown ai action: {other}")),
            };

            let duration = start.elapsed().as_secs_f64();
            match result {
                Ok(output) => success_result(output, duration),
                Err(e) => {
                    warn!(action = action.as_str(), error = %e, "ai agent: action failed");
                    failure_result(&e, duration)
                }
            }
        })
    }

    fn agent_type_name(&self) -> &'static str {
        "ai"
    }
}

impl AiAgent {
    /// Raw chat completion — caller provides messages directly.
    async fn chat(&self, inputs: &HashMap<String, Value>) -> Result<Value, String> {
        let messages = build_messages(inputs)?;
        let model = get_model(inputs, &self.default_model);
        let temperature = inputs
            .get("temperature")
            .and_then(|v| v.as_f64())
            .map(|t| t as f32);
        let max_tokens = inputs
            .get("max_tokens")
            .and_then(|v| v.as_u64())
            .map(|t| t as u32);

        let request = ChatCompletionRequest {
            model,
            messages,
            temperature,
            max_tokens,
            stream: None,
            tools: None,
            tool_choice: None,
        };

        let resp = ai::chat_completion(&self.config, request)
            .await
            .map_err(|e| format!("chat: {e}"))?;

        let content = resp
            .choices
            .first()
            .and_then(|c| c.message.content.clone())
            .unwrap_or_default();

        Ok(serde_json::json!({
            "content": content,
            "usage": resp.usage.as_ref().map(|u| serde_json::json!({
                "prompt_tokens": u.prompt_tokens,
                "completion_tokens": u.completion_tokens,
                "total_tokens": u.total_tokens,
            })),
        }))
    }

    /// Summarize text content.
    async fn summarize(&self, inputs: &HashMap<String, Value>) -> Result<Value, String> {
        let text = get_str(inputs, "text")?;
        let context = inputs
            .get("context")
            .and_then(|v| v.as_str())
            .unwrap_or("Provide a concise summary.");
        let model = get_model(inputs, &self.default_model);

        let request = ChatCompletionRequest {
            model,
            messages: vec![
                ChatMessage {
                    role: "system".into(),
                    content: Some(context.to_string()),
                    tool_calls: None,
                    tool_call_id: None,
                },
                ChatMessage {
                    role: "user".into(),
                    content: Some(text),
                    tool_calls: None,
                    tool_call_id: None,
                },
            ],
            temperature: Some(0.3),
            max_tokens: None,
            stream: None,
            tools: None,
            tool_choice: None,
        };

        let resp = ai::chat_completion(&self.config, request)
            .await
            .map_err(|e| format!("summarize: {e}"))?;

        let summary = resp
            .choices
            .first()
            .and_then(|c| c.message.content.clone())
            .unwrap_or_default();

        Ok(serde_json::json!({ "summary": summary }))
    }

    /// Analyze content with a custom system prompt.
    async fn analyze(&self, inputs: &HashMap<String, Value>) -> Result<Value, String> {
        let content = get_str(inputs, "content")?;
        let prompt = get_str(inputs, "prompt")?;
        let model = get_model(inputs, &self.default_model);

        let request = ChatCompletionRequest {
            model,
            messages: vec![
                ChatMessage {
                    role: "system".into(),
                    content: Some(prompt),
                    tool_calls: None,
                    tool_call_id: None,
                },
                ChatMessage {
                    role: "user".into(),
                    content: Some(content),
                    tool_calls: None,
                    tool_call_id: None,
                },
            ],
            temperature: Some(0.2),
            max_tokens: None,
            stream: None,
            tools: None,
            tool_choice: None,
        };

        let resp = ai::chat_completion(&self.config, request)
            .await
            .map_err(|e| format!("analyze: {e}"))?;

        let analysis = resp
            .choices
            .first()
            .and_then(|c| c.message.content.clone())
            .unwrap_or_default();

        Ok(serde_json::json!({ "analysis": analysis }))
    }

    /// Make a decision given context, options, and optional Mentor knowledge.
    async fn decide(
        &self,
        inputs: &HashMap<String, Value>,
        mentor: &MentorClient,
    ) -> Result<Value, String> {
        let context = get_str(inputs, "context")?;
        let question = get_str(inputs, "question")?;
        let model = get_model(inputs, &self.default_model);

        // Optionally query Mentor for relevant knowledge.
        let mentor_context = if let Some(query) = inputs.get("mentor_query").and_then(|v| v.as_str())
        {
            match mentor.query(query, 5).await {
                Ok(results) if !results.is_empty() => {
                    let entries: Vec<String> = results
                        .iter()
                        .map(|r| format!("- [{}] {}", r.category, r.content))
                        .collect();
                    format!(
                        "\n\nRelevant institutional knowledge:\n{}",
                        entries.join("\n")
                    )
                }
                _ => String::new(),
            }
        } else {
            String::new()
        };

        let system_prompt = format!(
            "You are a decision-making assistant. Given the context and question, \
             provide a clear decision with reasoning. Respond with JSON: \
             {{\"decision\": \"...\", \"reasoning\": \"...\", \"confidence\": 0.0-1.0}}{mentor_context}"
        );

        let request = ChatCompletionRequest {
            model,
            messages: vec![
                ChatMessage {
                    role: "system".into(),
                    content: Some(system_prompt),
                    tool_calls: None,
                    tool_call_id: None,
                },
                ChatMessage {
                    role: "user".into(),
                    content: Some(format!("Context:\n{context}\n\nQuestion:\n{question}")),
                    tool_calls: None,
                    tool_call_id: None,
                },
            ],
            temperature: Some(0.1),
            max_tokens: None,
            stream: None,
            tools: None,
            tool_choice: None,
        };

        let resp = ai::chat_completion(&self.config, request)
            .await
            .map_err(|e| format!("decide: {e}"))?;

        let raw = resp
            .choices
            .first()
            .and_then(|c| c.message.content.clone())
            .unwrap_or_default();

        // Try to parse as JSON; fall back to wrapping the raw text.
        match serde_json::from_str::<Value>(&raw) {
            Ok(parsed) => Ok(parsed),
            Err(_) => Ok(serde_json::json!({
                "decision": raw,
                "reasoning": "raw response (not JSON)",
                "confidence": 0.5,
            })),
        }
    }
}

// ---------------------------------------------------------------------------
// Input extraction helpers
// ---------------------------------------------------------------------------

fn get_str(inputs: &HashMap<String, Value>, key: &str) -> Result<String, String> {
    inputs
        .get(key)
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| format!("missing or invalid input: {key} (expected string)"))
}

fn get_model(inputs: &HashMap<String, Value>, default: &str) -> String {
    inputs
        .get("model")
        .and_then(|v| v.as_str())
        .unwrap_or(default)
        .to_string()
}

/// Build ChatMessage vec from inputs. Supports:
/// - "messages": array of {role, content} objects
/// - "system" + "user": shorthand for a two-message conversation
fn build_messages(inputs: &HashMap<String, Value>) -> Result<Vec<ChatMessage>, String> {
    if let Some(msgs) = inputs.get("messages") {
        let arr = msgs
            .as_array()
            .ok_or("'messages' must be an array")?;
        let mut messages = Vec::with_capacity(arr.len());
        for m in arr {
            let role = m
                .get("role")
                .and_then(|v| v.as_str())
                .ok_or("each message needs a 'role'")?
                .to_string();
            let content = m.get("content").and_then(|v| v.as_str()).map(String::from);
            messages.push(ChatMessage {
                role,
                content,
                tool_calls: None,
                tool_call_id: None,
            });
        }
        Ok(messages)
    } else {
        // Shorthand: system + user
        let mut messages = Vec::new();
        if let Some(system) = inputs.get("system").and_then(|v| v.as_str()) {
            messages.push(ChatMessage {
                role: "system".into(),
                content: Some(system.to_string()),
                tool_calls: None,
                tool_call_id: None,
            });
        }
        let user = get_str(inputs, "user")
            .or_else(|_| get_str(inputs, "prompt"))
            .map_err(|_| "need 'messages' array or 'user'/'prompt' string".to_string())?;
        messages.push(ChatMessage {
            role: "user".into(),
            content: Some(user),
            tool_calls: None,
            tool_call_id: None,
        });
        Ok(messages)
    }
}
