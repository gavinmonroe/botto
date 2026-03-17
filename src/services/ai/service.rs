// ---------------------------------------------------------------------------
// AI service — per-task orchestration layer on top of the raw AI client.
//
// Ported from Otto's ai-service.ts. Each public function handles one AI task:
// builds the prompt, calls the AI, parses the response, returns typed data.
//
// Supports both streaming (for real-time UI updates) and non-streaming modes.
// JSON parsing is fault-tolerant: extract from fences, repair truncation.
// ---------------------------------------------------------------------------

use super::client::{
    AiClientConfig, AiError, ChatCompletionRequest, ChatMessage, StreamEvent,
    chat_completion, chat_completion_stream,
};
use super::prompts;
use crate::types::review::*;
use crate::util::json_repair::parse_ai_json;
use serde::de::DeserializeOwned;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// Maximum tool-use iterations for related files discovery.
const MAX_TOOL_ITERATIONS: usize = 8;

// ---------------------------------------------------------------------------
// Task config — model + temperature + max_tokens per task
// ---------------------------------------------------------------------------

pub struct TaskConfig {
    pub model: String,
    pub temperature: f32,
    pub max_tokens: u32, // 0 = let provider decide
}

impl TaskConfig {
    pub fn from_botto_config(
        cfg: &crate::config::BottoConfig,
        task: crate::types::settings::AiTaskType,
    ) -> Self {
        use crate::types::settings::AiTaskType::*;
        let model = match task {
            Summary => &cfg.ai.models.summary,
            CodeReview => &cfg.ai.models.code_review,
            EdgeCases => &cfg.ai.models.edge_cases,
            RelatedFiles => &cfg.ai.models.related_files,
            FollowUp => &cfg.ai.models.follow_up,
            Chat => &cfg.ai.models.chat,
            AcValidation => &cfg.ai.models.ac_validation,
            AdversarialTests => &cfg.ai.models.adversarial_tests,
            Contracts => &cfg.ai.models.contracts,
            BehavioralDelta => &cfg.ai.models.behavioral_delta,
            Inquiry => &cfg.ai.models.inquiry,
            SemanticConflict => &cfg.ai.models.semantic_conflict,
            ClusterSummary => &cfg.ai.models.cluster_summary,
            ClusterReviewOrder => &cfg.ai.models.cluster_review_order,
        };
        Self {
            model: model.clone(),
            temperature: crate::types::settings::default_temperature(task),
            max_tokens: 0,
        }
    }
}

fn ai_config(cfg: &crate::config::BottoConfig) -> AiClientConfig {
    AiClientConfig {
        base_url: cfg.ai.base_url.clone(),
        api_key: cfg.ai.api_key.clone(),
    }
}

// ---------------------------------------------------------------------------
// Non-streaming helpers
// ---------------------------------------------------------------------------

/// Call the AI and parse the response as JSON into type T.
async fn call_and_parse<T: DeserializeOwned>(
    client_cfg: &AiClientConfig,
    task_cfg: &TaskConfig,
    messages: Vec<ChatMessage>,
) -> Result<T, AiError> {
    let request = ChatCompletionRequest {
        model: task_cfg.model.clone(),
        messages,
        temperature: Some(task_cfg.temperature),
        max_tokens: if task_cfg.max_tokens > 0 {
            Some(task_cfg.max_tokens)
        } else {
            None
        },
        stream: None,
        tools: None,
        tool_choice: None,
    };

    let response = chat_completion(client_cfg, request).await?;
    let content = response
        .choices
        .first()
        .and_then(|c| c.message.content.as_ref())
        .ok_or_else(|| AiError::Parse("empty response".into()))?;

    let parsed = parse_ai_json(content).map_err(|e| AiError::Parse(e))?;
    serde_json::from_value::<T>(parsed).map_err(|e| AiError::Parse(e.to_string()))
}

/// Call the AI with streaming, collecting the full response, then parse as JSON.
async fn call_streaming_and_parse<T: DeserializeOwned>(
    client_cfg: &AiClientConfig,
    task_cfg: &TaskConfig,
    messages: Vec<ChatMessage>,
    delta_tx: Option<&mpsc::Sender<String>>,
    cancel: CancellationToken,
) -> Result<T, AiError> {
    let request = ChatCompletionRequest {
        model: task_cfg.model.clone(),
        messages,
        temperature: Some(task_cfg.temperature),
        max_tokens: if task_cfg.max_tokens > 0 {
            Some(task_cfg.max_tokens)
        } else {
            None
        },
        stream: None,
        tools: None,
        tool_choice: None,
    };

    let mut rx = chat_completion_stream(client_cfg, request, cancel).await?;
    let mut full_content = String::new();

    while let Some(event) = rx.recv().await {
        match event {
            StreamEvent::Delta(text) => {
                full_content.push_str(&text);
                if let Some(tx) = delta_tx {
                    let _ = tx.send(text).await;
                }
            }
            StreamEvent::Done => break,
            StreamEvent::Error(e) => return Err(AiError::Network(e)),
            StreamEvent::ToolCallDelta(_) => {
                // Tool calls not expected in streaming parse mode
            }
        }
    }

    let parsed = parse_ai_json(&full_content).map_err(|e| AiError::Parse(e))?;
    serde_json::from_value::<T>(parsed).map_err(|e| AiError::Parse(e.to_string()))
}

// ---------------------------------------------------------------------------
// Public task functions
// ---------------------------------------------------------------------------

/// Generate an MR summary.
pub async fn generate_summary(
    cfg: &crate::config::BottoConfig,
    mr: &MrContext,
    ticket_context: Option<&str>,
    delta_tx: Option<&mpsc::Sender<String>>,
    cancel: CancellationToken,
    repo_config: Option<&str>,
) -> Result<MrSummary, AiError> {
    let client_cfg = ai_config(cfg);
    let task_cfg = TaskConfig::from_botto_config(cfg, crate::types::settings::AiTaskType::Summary);
    let messages = prompts::summary::build(mr, ticket_context, cfg.ai.custom_prompts.get("summary"), repo_config);

    if delta_tx.is_some() {
        call_streaming_and_parse(&client_cfg, &task_cfg, messages, delta_tx, cancel).await
    } else {
        call_and_parse(&client_cfg, &task_cfg, messages).await
    }
}

/// Review a single file. Returns a FileReview with comments.
pub async fn review_file(
    cfg: &crate::config::BottoConfig,
    mr: &MrContext,
    file: &DiffFileData,
    file_content: Option<&str>,
    context: &FileReviewContext,
    delta_tx: Option<&mpsc::Sender<String>>,
    cancel: CancellationToken,
) -> Result<FileReview, AiError> {
    let client_cfg = ai_config(cfg);
    let task_cfg =
        TaskConfig::from_botto_config(cfg, crate::types::settings::AiTaskType::CodeReview);
    let messages = prompts::code_review::build(mr, file, file_content, context, cfg.ai.custom_prompts.get("code_review"));

    if delta_tx.is_some() {
        call_streaming_and_parse(&client_cfg, &task_cfg, messages, delta_tx, cancel).await
    } else {
        call_and_parse(&client_cfg, &task_cfg, messages).await
    }
}

/// Analyze edge cases in the diff.
pub async fn analyze_edge_cases(
    cfg: &crate::config::BottoConfig,
    mr: &MrContext,
    summary: &MrSummary,
    delta_tx: Option<&mpsc::Sender<String>>,
    cancel: CancellationToken,
    repo_config: Option<&str>,
) -> Result<Vec<EdgeCase>, AiError> {
    let client_cfg = ai_config(cfg);
    let task_cfg =
        TaskConfig::from_botto_config(cfg, crate::types::settings::AiTaskType::EdgeCases);
    let messages = prompts::edge_cases::build(mr, summary, cfg.ai.custom_prompts.get("edge_cases"), repo_config);

    if delta_tx.is_some() {
        call_streaming_and_parse(&client_cfg, &task_cfg, messages, delta_tx, cancel).await
    } else {
        call_and_parse(&client_cfg, &task_cfg, messages).await
    }
}

/// Generate a chat response in the context of a review.
pub async fn generate_chat_response(
    cfg: &crate::config::BottoConfig,
    messages: Vec<ChatMessage>,
    delta_tx: &mpsc::Sender<String>,
    cancel: CancellationToken,
) -> Result<String, AiError> {
    let client_cfg = ai_config(cfg);
    let task_cfg = TaskConfig::from_botto_config(cfg, crate::types::settings::AiTaskType::Chat);

    let request = ChatCompletionRequest {
        model: task_cfg.model,
        messages,
        temperature: Some(task_cfg.temperature),
        max_tokens: None,
        stream: None,
        tools: None,
        tool_choice: None,
    };

    let mut rx = chat_completion_stream(&client_cfg, request, cancel).await?;
    let mut full_content = String::new();

    while let Some(event) = rx.recv().await {
        match event {
            StreamEvent::Delta(text) => {
                full_content.push_str(&text);
                let _ = delta_tx.send(text).await;
            }
            StreamEvent::Done => break,
            StreamEvent::Error(e) => return Err(AiError::Network(e)),
            StreamEvent::ToolCallDelta(_) => {}
        }
    }

    Ok(full_content)
}

/// Generate an inquiry response for a line-range question.
/// Same streaming pattern as chat — returns plain markdown, not JSON.
pub async fn generate_inquiry_response(
    cfg: &crate::config::BottoConfig,
    messages: Vec<ChatMessage>,
    delta_tx: &mpsc::Sender<String>,
    cancel: CancellationToken,
) -> Result<String, AiError> {
    let client_cfg = ai_config(cfg);
    let task_cfg = TaskConfig::from_botto_config(cfg, crate::types::settings::AiTaskType::Inquiry);

    let request = ChatCompletionRequest {
        model: task_cfg.model,
        messages,
        temperature: Some(task_cfg.temperature),
        max_tokens: None,
        stream: None,
        tools: None,
        tool_choice: None,
    };

    let mut rx = chat_completion_stream(&client_cfg, request, cancel).await?;
    let mut full_content = String::new();

    while let Some(event) = rx.recv().await {
        match event {
            StreamEvent::Delta(text) => {
                full_content.push_str(&text);
                let _ = delta_tx.send(text).await;
            }
            StreamEvent::Done => break,
            StreamEvent::Error(e) => return Err(AiError::Network(e)),
            StreamEvent::ToolCallDelta(_) => {}
        }
    }

    Ok(full_content)
}

// ---------------------------------------------------------------------------
// Related files (non-streaming, no tool-use for now — simple AI inference)
// ---------------------------------------------------------------------------

/// Discover files related to the diff that aren't in the changeset.
pub async fn discover_related_files(
    cfg: &crate::config::BottoConfig,
    mr: &MrContext,
    cancel: CancellationToken,
    repo_config: Option<&str>,
) -> Result<Vec<RelatedFile>, AiError> {
    let client_cfg = ai_config(cfg);
    let task_cfg =
        TaskConfig::from_botto_config(cfg, crate::types::settings::AiTaskType::RelatedFiles);
    let messages = prompts::related_files::build(mr, cfg.ai.custom_prompts.get("related_files"), repo_config);

    call_streaming_and_parse(&client_cfg, &task_cfg, messages, None, cancel).await
}

// ---------------------------------------------------------------------------
// Verification layer tasks
// ---------------------------------------------------------------------------

/// Generate adversarial property-based tests.
pub async fn generate_adversarial_tests(
    cfg: &crate::config::BottoConfig,
    mr: &MrContext,
    edge_cases: &[EdgeCase],
    cancel: CancellationToken,
    repo_config: Option<&str>,
) -> Result<crate::types::verification::AdversarialTestData, AiError> {
    let client_cfg = ai_config(cfg);
    let task_cfg =
        TaskConfig::from_botto_config(cfg, crate::types::settings::AiTaskType::AdversarialTests);
    let messages = prompts::adversarial_tests::build(mr, edge_cases, cfg.ai.custom_prompts.get("adversarial_tests"), repo_config);

    call_streaming_and_parse(&client_cfg, &task_cfg, messages, None, cancel).await
}

/// Infer function contracts (preconditions, postconditions, invariants).
pub async fn generate_contracts(
    cfg: &crate::config::BottoConfig,
    mr: &MrContext,
    cancel: CancellationToken,
    repo_config: Option<&str>,
) -> Result<crate::types::verification::ContractData, AiError> {
    let client_cfg = ai_config(cfg);
    let task_cfg =
        TaskConfig::from_botto_config(cfg, crate::types::settings::AiTaskType::Contracts);
    let messages = prompts::contracts::build(mr, cfg.ai.custom_prompts.get("contracts"), repo_config);

    call_streaming_and_parse(&client_cfg, &task_cfg, messages, None, cancel).await
}

/// Analyze behavioral delta — what changed, what's preserved, what's unexpected.
pub async fn analyze_behavioral_delta(
    cfg: &crate::config::BottoConfig,
    mr: &MrContext,
    summary: &MrSummary,
    cancel: CancellationToken,
    repo_config: Option<&str>,
) -> Result<crate::types::verification::BehavioralDeltaData, AiError> {
    let client_cfg = ai_config(cfg);
    let task_cfg =
        TaskConfig::from_botto_config(cfg, crate::types::settings::AiTaskType::BehavioralDelta);
    let messages = prompts::behavioral_delta::build(mr, summary, cfg.ai.custom_prompts.get("behavioral_delta"), repo_config);

    call_streaming_and_parse(&client_cfg, &task_cfg, messages, None, cancel).await
}

// ---------------------------------------------------------------------------
// Context struct for file review (avoids massive parameter lists)
// ---------------------------------------------------------------------------

/// Additional context passed to the code review prompt builder.
pub struct FileReviewContext {
    pub repo_context: Option<String>,
    pub caller_snippets: Option<String>,
    pub ticket_context: Option<String>,
    pub reviewer_prefs: Option<String>,
    pub file_activity: Option<String>,
    pub repo_config: Option<String>,
    pub is_self_review: bool,
}

impl Default for FileReviewContext {
    fn default() -> Self {
        Self {
            repo_context: None,
            caller_snippets: None,
            ticket_context: None,
            reviewer_prefs: None,
            file_activity: None,
            repo_config: None,
            is_self_review: false,
        }
    }
}
