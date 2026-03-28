// ---------------------------------------------------------------------------
// Directive Parser — extract structured directives from natural language.
//
// Uses AI to parse a free-form description into a Directive struct with
// intent, sources, constraints, and priority. Fault-tolerant JSON parsing
// with multiple fallback strategies.
// ---------------------------------------------------------------------------

use anyhow::{Context, Result};
use tracing::{debug, warn};
use uuid::Uuid;

use super::types::{
    Directive, DirectiveConstraints, DirectiveStatus, WorkSource,
};
use crate::services::ai::client::{
    self, AiClientConfig, ChatCompletionRequest, ChatMessage,
};
use crate::services::workflow::crud::epoch_secs;

/// Parse a natural-language description into a structured Directive.
pub async fn parse_directive(
    ai_config: &AiClientConfig,
    ai_model: &str,
    description: &str,
    created_by: Option<&str>,
) -> Result<Directive> {
    let system_prompt = r#"You are a directive parser. Given a natural language description of a standing order, extract structured fields.

Respond with ONLY a JSON object (no markdown, no explanation):
{
  "name": "short-kebab-case-name",
  "intent": "clear description of what work to look for and how to handle it",
  "sources": [
    {"type": "explicit", "source_type": "jira", "url": "https://...", "params": {}}
    or
    {"type": "inferred", "category": "connector", "filter": null}
  ],
  "constraints": {
    "maxConcurrentSessions": 3,
    "workingHoursStart": 9,
    "workingHoursEnd": 17,
    "maxItemsPerPoll": 10
  },
  "priority": 5,
  "pollIntervalSecs": 300
}

Rules:
- name should be a short, descriptive kebab-case identifier
- intent should capture the full meaning of what the user wants
- sources: use "explicit" when the user names a specific system/URL, "inferred" when they want broad discovery
- If no source is mentioned, default to inferred with category "connector"
- priority: 1 (highest) to 10 (lowest), default 5
- pollIntervalSecs: how often to check, default 300 (5 minutes)
- constraints: reasonable defaults if not specified"#;

    let request = ChatCompletionRequest {
        model: ai_model.to_string(),
        messages: vec![
            ChatMessage {
                role: "system".into(),
                content: Some(system_prompt.into()),
                tool_calls: None,
                tool_call_id: None,
            },
            ChatMessage {
                role: "user".into(),
                content: Some(description.to_string()),
                tool_calls: None,
                tool_call_id: None,
            },
        ],
        temperature: Some(0.2),
        max_tokens: Some(1024),
        stream: None,
        tools: None,
        tool_choice: None,
    };

    let resp = client::chat_completion(ai_config, request)
        .await
        .map_err(|e| anyhow::anyhow!("AI directive parse call failed: {e}"))?;

    let response_text = resp
        .choices
        .first()
        .and_then(|c| c.message.content.clone())
        .unwrap_or_default();

    parse_directive_response(&response_text, description, created_by)
}

/// Parse the AI response into a Directive, with multiple fallback strategies.
fn parse_directive_response(
    response: &str,
    original_description: &str,
    created_by: Option<&str>,
) -> Result<Directive> {
    let trimmed = response.trim();
    let now = epoch_secs();

    // Try direct parse.
    if let Ok(d) = try_parse_directive_json(trimmed, created_by, now) {
        return Ok(d);
    }

    // Try code fence extraction.
    if let Some(json) = extract_json_block(trimmed) {
        if let Ok(d) = try_parse_directive_json(&json, created_by, now) {
            return Ok(d);
        }
    }

    // Try brace extraction.
    if let Some(start) = trimmed.find('{') {
        if let Some(end) = trimmed.rfind('}') {
            let candidate = &trimmed[start..=end];
            if let Ok(d) = try_parse_directive_json(candidate, created_by, now) {
                return Ok(d);
            }
        }
    }

    // Fallback: create a minimal directive from the original description.
    warn!("could not parse AI response for directive, using fallback");
    Ok(Directive {
        id: Uuid::new_v4().to_string(),
        name: slugify(original_description),
        intent: original_description.to_string(),
        sources: vec![WorkSource::Inferred {
            category: "connector".into(),
            filter: None,
        }],
        constraints: DirectiveConstraints::default(),
        priority: 5,
        status: DirectiveStatus::Active,
        poll_interval_secs: 300,
        last_poll_at: None,
        next_poll_at: Some(now),
        escalation: None,
        created_by: created_by.map(|s| s.to_string()),
        reply_context: None,
        created_at: now,
        updated_at: now,
    })
}

fn try_parse_directive_json(
    text: &str,
    created_by: Option<&str>,
    now: i64,
) -> Result<Directive> {
    let v: serde_json::Value = serde_json::from_str(text).context("parse JSON")?;

    let name = v
        .get("name")
        .and_then(|n| n.as_str())
        .unwrap_or("unnamed-directive")
        .to_string();

    let intent = v
        .get("intent")
        .and_then(|i| i.as_str())
        .unwrap_or("")
        .to_string();

    if intent.is_empty() {
        anyhow::bail!("parsed directive has empty intent");
    }

    let sources: Vec<WorkSource> = v
        .get("sources")
        .and_then(|s| serde_json::from_value(s.clone()).ok())
        .unwrap_or_else(|| {
            vec![WorkSource::Inferred {
                category: "connector".into(),
                filter: None,
            }]
        });

    let constraints: DirectiveConstraints = v
        .get("constraints")
        .and_then(|c| serde_json::from_value(c.clone()).ok())
        .unwrap_or_default();

    let priority = v
        .get("priority")
        .and_then(|p| p.as_i64())
        .unwrap_or(5) as i32;

    let poll_interval_secs = v
        .get("pollIntervalSecs")
        .or_else(|| v.get("poll_interval_secs"))
        .and_then(|p| p.as_i64())
        .unwrap_or(300);

    debug!(name = %name, intent = %intent, "parsed directive from AI response");

    Ok(Directive {
        id: Uuid::new_v4().to_string(),
        name,
        intent,
        sources,
        constraints,
        priority,
        status: DirectiveStatus::Active,
        poll_interval_secs,
        last_poll_at: None,
        next_poll_at: Some(now),
        escalation: None,
        created_by: created_by.map(|s| s.to_string()),
        reply_context: None,
        created_at: now,
        updated_at: now,
    })
}

/// Create a kebab-case slug from a description.
fn slugify(text: &str) -> String {
    let slug: String = text
        .chars()
        .take(40)
        .map(|c| {
            if c.is_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect();

    // Collapse multiple dashes and trim.
    let mut result = String::new();
    let mut last_dash = false;
    for c in slug.chars() {
        if c == '-' {
            if !last_dash && !result.is_empty() {
                result.push('-');
            }
            last_dash = true;
        } else {
            result.push(c);
            last_dash = false;
        }
    }
    result.trim_end_matches('-').to_string()
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
