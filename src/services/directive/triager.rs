// ---------------------------------------------------------------------------
// Work Triager — trait + AI-based implementation.
//
// The WorkTriager trait abstracts how a directive decides whether to accept
// or reject a discovered work item. AiTriager sends the directive intent +
// work item details to AI and parses the response into a TriageDecision.
// ---------------------------------------------------------------------------

use anyhow::{Context, Result};
use tracing::{debug, warn};

use super::types::{Directive, TriageDecision, WorkItem};
use crate::services::ai::client::{
    self, AiClientConfig, ChatCompletionRequest, ChatMessage,
};
use crate::services::mentor::client::MentorClient;

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// Decides whether a discovered work item should be accepted or rejected.
pub trait WorkTriager: Send + Sync {
    /// Triage a single work item against a directive's intent.
    fn triage(
        &self,
        directive: &Directive,
        item: &WorkItem,
    ) -> impl std::future::Future<Output = Result<TriageDecision>> + Send;
}

// ---------------------------------------------------------------------------
// AiTriager — uses AI to make triage decisions
// ---------------------------------------------------------------------------

pub struct AiTriager {
    ai_config: AiClientConfig,
    ai_model: String,
    mentor: MentorClient,
}

impl AiTriager {
    pub fn new(ai_config: AiClientConfig, ai_model: String, mentor: MentorClient) -> Self {
        Self {
            ai_config,
            ai_model,
            mentor,
        }
    }

    /// Build context from Mentor for better triage decisions.
    async fn gather_context(&self, directive: &Directive) -> String {
        let query = format!("directive triage {} {}", directive.name, directive.intent);
        match self.mentor.query(&query, 5).await {
            Ok(results) => {
                if results.is_empty() {
                    return String::new();
                }
                let mut ctx = String::from("\n\nRelevant knowledge from previous runs:\n");
                for r in results.iter().take(3) {
                    ctx.push_str(&format!("- {}\n", r.content));
                }
                ctx
            }
            Err(e) => {
                debug!("mentor query for triage context failed: {e}");
                String::new()
            }
        }
    }
}

#[allow(unused)]
impl WorkTriager for AiTriager {
    async fn triage(
        &self,
        directive: &Directive,
        item: &WorkItem,
    ) -> Result<TriageDecision> {
        let mentor_context = self.gather_context(directive).await;

        let system_prompt = format!(
            r#"You are a work item triager. Given a directive's intent and a discovered work item, decide whether to accept or reject it.

Directive: "{name}"
Intent: {intent}
Priority: {priority}
{mentor_context}

Respond with ONLY a JSON object (no markdown, no explanation):
{{
  "decision": "accept" | "reject" | "needs_more_context" | "already_tracked",
  "reason": "brief explanation",
  "priority": 1-10 (only if accepting, lower = higher priority)
}}

Rules:
- Accept items that clearly match the directive's intent.
- Reject items that are irrelevant, already resolved, or out of scope.
- Use needs_more_context sparingly — only when the item is ambiguous.
- Priority should reflect urgency relative to the directive's own priority ({priority})."#,
            name = directive.name,
            intent = directive.intent,
            priority = directive.priority,
            mentor_context = mentor_context,
        );

        let item_description = format!(
            "Work Item:\n  ID: {}\n  Source: {}\n  Title: {}\n  Description: {}\n  URL: {}",
            item.external_id,
            item.source_type,
            item.title,
            item.description.as_deref().unwrap_or("(none)"),
            item.source_url.as_deref().unwrap_or("(none)"),
        );

        let request = ChatCompletionRequest {
            model: self.ai_model.clone(),
            messages: vec![
                ChatMessage {
                    role: "system".into(),
                    content: Some(system_prompt),
                    tool_calls: None,
                    tool_call_id: None,
                },
                ChatMessage {
                    role: "user".into(),
                    content: Some(item_description),
                    tool_calls: None,
                    tool_call_id: None,
                },
            ],
            temperature: Some(0.1),
            max_tokens: Some(256),
            stream: None,
            tools: None,
            tool_choice: None,
        };

        let resp = client::chat_completion(&self.ai_config, request)
            .await
            .map_err(|e| anyhow::anyhow!("AI triage call failed: {e}"))?;

        let response_text = resp
            .choices
            .first()
            .and_then(|c| c.message.content.clone())
            .unwrap_or_default();

        parse_triage_response(&response_text)
    }
}

// ---------------------------------------------------------------------------
// Response parsing — fault-tolerant
// ---------------------------------------------------------------------------

fn parse_triage_response(response: &str) -> Result<TriageDecision> {
    let trimmed = response.trim();

    // Try direct parse.
    if let Ok(decision) = try_parse_decision(trimmed) {
        return Ok(decision);
    }

    // Try extracting JSON from code fences.
    if let Some(json) = extract_json_block(trimmed) {
        if let Ok(decision) = try_parse_decision(&json) {
            return Ok(decision);
        }
    }

    // Try extracting a brace block.
    if let Some(start) = trimmed.find('{') {
        if let Some(end) = trimmed.rfind('}') {
            let candidate = &trimmed[start..=end];
            if let Ok(decision) = try_parse_decision(candidate) {
                return Ok(decision);
            }
        }
    }

    // Fallback: look for keywords in the raw text.
    let lower = trimmed.to_lowercase();
    if lower.contains("reject") {
        return Ok(TriageDecision::Reject {
            reason: "AI response unparseable, defaulting to reject".into(),
        });
    }
    if lower.contains("accept") {
        return Ok(TriageDecision::Accept {
            reason: "AI response unparseable, defaulting to accept".into(),
            priority: 5,
        });
    }

    // Ultimate fallback: reject with explanation.
    warn!("could not parse triage response, defaulting to reject");
    Ok(TriageDecision::Reject {
        reason: "triage response could not be parsed".into(),
    })
}

fn try_parse_decision(text: &str) -> Result<TriageDecision> {
    let v: serde_json::Value = serde_json::from_str(text).context("parse JSON")?;

    let decision = v
        .get("decision")
        .and_then(|d| d.as_str())
        .unwrap_or("reject");
    let reason = v
        .get("reason")
        .and_then(|r| r.as_str())
        .unwrap_or("no reason given")
        .to_string();

    match decision {
        "accept" => {
            let priority = v
                .get("priority")
                .and_then(|p| p.as_i64())
                .unwrap_or(5) as i32;
            Ok(TriageDecision::Accept { reason, priority })
        }
        "reject" => Ok(TriageDecision::Reject { reason }),
        "needs_more_context" => Ok(TriageDecision::NeedsMoreContext { question: reason }),
        "already_tracked" => Ok(TriageDecision::AlreadyTracked),
        _ => Ok(TriageDecision::Reject { reason }),
    }
}

fn extract_json_block(text: &str) -> Option<String> {
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
