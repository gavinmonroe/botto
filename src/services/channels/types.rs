// ---------------------------------------------------------------------------
// Channel Adapter types — unified messaging abstraction for all input/output
// channels (GitLab, Slack, Admin UI, API, Cron).
//
// All types derive Clone for tokio::broadcast compatibility.
// ---------------------------------------------------------------------------

use serde::{Deserialize, Serialize};
use std::fmt;

// ---------------------------------------------------------------------------
// Channel + Thread enums
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChannelType {
    GitLab,
    Slack,
    AdminUI,
    Api,
    Cron,
}

impl ChannelType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::GitLab => "gitlab",
            Self::Slack => "slack",
            Self::AdminUI => "admin_ui",
            Self::Api => "api",
            Self::Cron => "cron",
        }
    }
}

impl fmt::Display for ChannelType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ThreadType {
    Issue,
    MergeRequest,
    SlackThread,
    DirectMessage,
    AdminUI,
}

impl ThreadType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Issue => "issue",
            Self::MergeRequest => "merge_request",
            Self::SlackThread => "slack_thread",
            Self::DirectMessage => "direct_message",
            Self::AdminUI => "admin_ui",
        }
    }
}

impl fmt::Display for ThreadType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

// ---------------------------------------------------------------------------
// MessageContext — origin metadata for an inbound message
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageContext {
    pub channel: ChannelType,
    pub channel_id: String,
    pub user_id: String,
    pub user_name: String,
    pub user_email: Option<String>,
    pub thread_id: Option<String>,
    pub thread_type: Option<ThreadType>,
    pub parent_message_id: Option<String>,
    pub project_path: Option<String>,
    pub project_id: Option<i64>,
    pub reply_to: Option<ReplyTarget>,
    pub received_at: i64,
    pub raw_payload: Option<serde_json::Value>,
}

// ---------------------------------------------------------------------------
// ReplyTarget — where to send a response
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplyTarget {
    pub channel: ChannelType,
    pub target_id: String,
    pub thread_id: Option<String>,
}

// ---------------------------------------------------------------------------
// InboundMessage — parsed input from any channel
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InboundMessage {
    pub id: String,
    pub context: MessageContext,
    pub action: InboundAction,
    pub raw_content: String,
    pub parsed_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InboundAction {
    CreateDirective,
    TriggerWorkflow,
    RequestReview,
    RequestFix,
    QueryStatus,
    RespondToEscalation,
    Help,
    NaturalLanguage,
}

impl InboundAction {
    pub fn action_name(&self) -> &'static str {
        match self {
            Self::CreateDirective => "create_directive",
            Self::TriggerWorkflow => "trigger_workflow",
            Self::RequestReview => "request_review",
            Self::RequestFix => "request_fix",
            Self::QueryStatus => "query_status",
            Self::RespondToEscalation => "respond_to_escalation",
            Self::Help => "help",
            Self::NaturalLanguage => "natural_language",
        }
    }
}

// ---------------------------------------------------------------------------
// OutboundMessage — response to send via any channel
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutboundMessage {
    pub id: String,
    pub reply_context: ReplyTarget,
    pub message_type: OutboundType,
    pub content: String,
    pub structured_data: Option<serde_json::Value>,
    pub session_id: Option<String>,
    pub directive_id: Option<String>,
    pub created_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutboundType {
    Error,
    Acknowledgment,
    Progress,
    Completion,
    Help,
    StatusReport,
    Escalation,
}

impl OutboundType {
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::Error => "error",
            Self::Acknowledgment => "acknowledgment",
            Self::Progress => "progress",
            Self::Completion => "completion",
            Self::Help => "help",
            Self::StatusReport => "status_report",
            Self::Escalation => "escalation",
        }
    }
}

// ---------------------------------------------------------------------------
// OutboundMessage constructors
// ---------------------------------------------------------------------------

impl OutboundMessage {
    fn new(reply_context: ReplyTarget, message_type: OutboundType, content: String) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            reply_context,
            message_type,
            content,
            structured_data: None,
            session_id: None,
            directive_id: None,
            created_at: crate::services::workflow::crud::epoch_secs(),
        }
    }

    /// Public constructor for bridge and other components that need to specify
    /// the message type directly.
    pub fn new_with_type(
        reply_context: ReplyTarget,
        message_type: OutboundType,
        content: impl Into<String>,
    ) -> Self {
        Self::new(reply_context, message_type, content.into())
    }

    pub fn error(reply_context: ReplyTarget, message: impl Into<String>) -> Self {
        Self::new(reply_context, OutboundType::Error, message.into())
    }

    pub fn acknowledgment(reply_context: ReplyTarget, message: impl Into<String>) -> Self {
        Self::new(reply_context, OutboundType::Acknowledgment, message.into())
    }

    pub fn progress(reply_context: ReplyTarget, message: impl Into<String>) -> Self {
        Self::new(reply_context, OutboundType::Progress, message.into())
    }

    pub fn completion(reply_context: ReplyTarget, message: impl Into<String>) -> Self {
        Self::new(reply_context, OutboundType::Completion, message.into())
    }

    pub fn help(reply_context: ReplyTarget) -> Self {
        let content = "\
**Available commands:**\n\
- `/botto directive <description>` — Create a standing directive\n\
- `/botto workflow <name>` — Trigger a workflow\n\
- `/botto review` — Request a code review\n\
- `/botto fix` — Request a fix for review comments\n\
- `/botto status` — Query current status\n\
- `/botto help` — Show this help message"
            .to_string();
        Self::new(reply_context, OutboundType::Help, content)
    }

    pub fn status_report(
        reply_context: ReplyTarget,
        content: impl Into<String>,
        data: serde_json::Value,
    ) -> Self {
        let mut msg = Self::new(reply_context, OutboundType::StatusReport, content.into());
        msg.structured_data = Some(data);
        msg
    }
}

// ---------------------------------------------------------------------------
// RawEvent — unprocessed webhook/event payload
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawEvent {
    pub channel: ChannelType,
    pub event_type: String,
    pub payload: serde_json::Value,
    pub received_at: i64,
}

// ---------------------------------------------------------------------------
// Adapter traits
// ---------------------------------------------------------------------------

/// Input adapter: parses raw events from a specific channel into InboundMessages.
#[allow(async_fn_in_trait)]
pub trait ChannelInputAdapter: Send + Sync {
    fn channel_type(&self) -> ChannelType;
    async fn parse(&self, event: &RawEvent) -> anyhow::Result<Option<InboundMessage>>;
    fn is_enabled(&self) -> bool;
}

/// Output adapter: delivers OutboundMessages to a specific channel.
#[allow(async_fn_in_trait)]
pub trait ChannelOutputAdapter: Send + Sync {
    fn channel_type(&self) -> ChannelType;
    async fn deliver(&self, message: &OutboundMessage) -> anyhow::Result<()>;
    fn is_enabled(&self) -> bool;
}
