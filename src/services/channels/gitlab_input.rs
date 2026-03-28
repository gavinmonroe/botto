// ---------------------------------------------------------------------------
// GitLab input adapter — parses @botto mentions and /botto commands from
// GitLab webhook note (comment) payloads.
//
// Integrates with the existing webhook handler in src/api/webhooks.rs by
// providing a function that can be called from handle_note_event.
// ---------------------------------------------------------------------------

use tracing::info;

use super::bus::MessageBus;
use super::types::{
    ChannelType, InboundAction, InboundMessage, MessageContext, ReplyTarget, ThreadType,
};
use crate::services::workflow::crud::epoch_secs;

/// Parse a GitLab note webhook payload and, if it contains a @botto mention
/// or /botto command, publish an InboundMessage to the bus.
///
/// Called from the webhook handler. Returns true if a message was published.
pub fn parse_gitlab_comment(
    bus: &MessageBus,
    payload: &serde_json::Value,
) -> bool {
    let note_body = match payload["object_attributes"]["note"].as_str() {
        Some(body) => body,
        None => return false,
    };

    // Check for @botto mention or /botto command
    let (action, content) = match extract_command(note_body) {
        Some(parsed) => parsed,
        None => return false,
    };

    let project_path = payload["project"]["path_with_namespace"]
        .as_str()
        .unwrap_or("")
        .to_string();
    let project_id = payload["project"]["id"].as_i64();

    let user_name = payload["user"]["username"]
        .as_str()
        .unwrap_or("unknown")
        .to_string();
    let user_id = payload["user"]["id"]
        .as_i64()
        .map(|id| id.to_string())
        .unwrap_or_else(|| user_name.clone());
    let user_email = payload["user"]["email"]
        .as_str()
        .map(|s| s.to_string());

    let note_id = payload["object_attributes"]["id"]
        .as_i64()
        .map(|id| id.to_string())
        .unwrap_or_default();
    let discussion_id = payload["object_attributes"]["discussion_id"]
        .as_str()
        .map(|s| s.to_string());

    // Determine thread type from the noteable_type
    let noteable_type = payload["object_attributes"]["noteable_type"]
        .as_str()
        .unwrap_or("");
    let thread_type = match noteable_type {
        "MergeRequest" => Some(ThreadType::MergeRequest),
        "Issue" => Some(ThreadType::Issue),
        _ => None,
    };

    // Thread ID: use MR iid or issue iid
    let thread_id = payload["merge_request"]["iid"]
        .as_u64()
        .map(|iid| format!("mr:{}", iid))
        .or_else(|| {
            payload["issue"]["iid"]
                .as_u64()
                .map(|iid| format!("issue:{}", iid))
        });

    let mr_iid = payload["merge_request"]["iid"].as_u64();

    // Build reply target — we need project_id and mr_iid/issue_iid/discussion_id to reply.
    // New format: "project_id:mr:iid" or "project_id:issue:iid"
    let reply_to = if let Some(pid) = project_id {
        let target_id = if let Some(iid) = mr_iid {
            format!("{}:mr:{}", pid, iid)
        } else if let Some(iid) = payload["issue"]["iid"].as_u64() {
            format!("{}:issue:{}", pid, iid)
        } else {
            // Fallback: project-level (no specific MR or issue)
            format!("{}:mr:0", pid)
        };
        Some(ReplyTarget {
            channel: ChannelType::GitLab,
            target_id,
            thread_id: discussion_id.clone(),
        })
    } else {
        None
    };

    let now = epoch_secs();
    let message = InboundMessage {
        id: uuid::Uuid::new_v4().to_string(),
        context: MessageContext {
            channel: ChannelType::GitLab,
            channel_id: format!("gitlab:{}", project_path),
            user_id,
            user_name: user_name.clone(),
            user_email,
            thread_id,
            thread_type,
            parent_message_id: Some(note_id),
            project_path: Some(project_path.clone()),
            project_id,
            reply_to,
            received_at: now,
            raw_payload: Some(payload.clone()),
        },
        action,
        raw_content: content,
        parsed_at: now,
    };

    info!(
        project = %project_path,
        user = %user_name,
        action = %message.action.action_name(),
        "parsed GitLab comment command"
    );

    bus.publish_inbound(message);
    true
}

/// Extract a command from a GitLab comment body.
/// Recognizes:
///   - `/botto <command> [args]`
///   - `@botto <command> [args]`
///   - Plain `@botto` mention (treated as natural language)
fn extract_command(body: &str) -> Option<(InboundAction, String)> {
    let body_lower = body.to_lowercase();

    // Find the trigger pattern
    let trigger_pos = body_lower
        .find("/botto ")
        .or_else(|| body_lower.find("/botto\n"))
        .or_else(|| body_lower.find("@botto "))
        .or_else(|| body_lower.find("@botto\n"));

    // Also check for exact match (just "/botto" or "@botto" with nothing after)
    let is_bare_mention = body_lower.trim() == "/botto" || body_lower.trim() == "@botto";

    if trigger_pos.is_none() && !is_bare_mention {
        return None;
    }

    if is_bare_mention {
        return Some((InboundAction::Help, String::new()));
    }

    let trigger_pos = trigger_pos.unwrap();
    // Skip past the trigger word ("/botto " or "@botto ")
    let after_trigger = &body[trigger_pos..];
    let rest = after_trigger
        .splitn(2, char::is_whitespace)
        .nth(1)
        .unwrap_or("")
        .trim();

    // Parse the command word
    let (command_word, args) = match rest.split_once(char::is_whitespace) {
        Some((cmd, args)) => (cmd.to_lowercase(), args.trim().to_string()),
        None => (rest.to_lowercase(), String::new()),
    };

    let (action, content) = match command_word.as_str() {
        "directive" | "create-directive" | "standing-order" => {
            (InboundAction::CreateDirective, args)
        }
        "workflow" | "trigger" | "run" => (InboundAction::TriggerWorkflow, args),
        "review" => (InboundAction::RequestReview, args),
        "fix" => (InboundAction::RequestFix, args),
        "status" => (InboundAction::QueryStatus, args),
        "respond" | "reply" => (InboundAction::RespondToEscalation, args),
        "help" | "?" => (InboundAction::Help, String::new()),
        _ => {
            // Unknown command — treat the whole thing as natural language
            (InboundAction::NaturalLanguage, rest.to_string())
        }
    };

    Some((action, content))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_slash_command() {
        let (action, content) = extract_command("/botto directive watch for new issues").unwrap();
        assert_eq!(action, InboundAction::CreateDirective);
        assert_eq!(content, "watch for new issues");
    }

    #[test]
    fn parse_at_mention_command() {
        let (action, content) = extract_command("Hey @botto review this MR please").unwrap();
        assert_eq!(action, InboundAction::RequestReview);
        assert_eq!(content, "this MR please");
    }

    #[test]
    fn parse_help_command() {
        let (action, _) = extract_command("/botto help").unwrap();
        assert_eq!(action, InboundAction::Help);
    }

    #[test]
    fn parse_bare_mention() {
        let (action, _) = extract_command("@botto").unwrap();
        assert_eq!(action, InboundAction::Help);
    }

    #[test]
    fn parse_unknown_command_as_natural_language() {
        let (action, content) =
            extract_command("/botto what's the status of the deploy pipeline?").unwrap();
        assert_eq!(action, InboundAction::NaturalLanguage);
        assert!(content.contains("status of the deploy"));
    }

    #[test]
    fn no_mention_returns_none() {
        assert!(extract_command("Just a regular comment").is_none());
    }
}
