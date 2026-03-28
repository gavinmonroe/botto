// ---------------------------------------------------------------------------
// GitLab output adapter — subscribes to outbound bus, filters for GitLab
// channel, and posts comments via the existing GitLab client.
// ---------------------------------------------------------------------------

use anyhow::{Context, Result};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use super::audit;
use super::bus::MessageBus;
use super::types::{ChannelType, OutboundMessage, OutboundType};
use crate::services::gitlab::client::{self as gitlab, GitLabConfig};

/// Spawn a background task that listens for outbound messages destined for
/// GitLab and posts them as MR/issue comments.
pub fn spawn_gitlab_output_listener(
    pool: sqlx::SqlitePool,
    bus: MessageBus,
    gl_config: GitLabConfig,
    shutdown: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        info!("GitLab output listener started");
        let mut rx = bus.subscribe_outbound();

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    info!("GitLab output listener shutting down");
                    break;
                }
                result = rx.recv() => {
                    match result {
                        Ok(message) => {
                            if message.reply_context.channel != ChannelType::GitLab {
                                continue;
                            }
                            if let Err(e) = deliver_gitlab(&pool, &gl_config, &message).await {
                                error!(
                                    id = %message.id,
                                    "GitLab delivery failed: {:#}", e
                                );
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            warn!("GitLab output listener lagged, missed {} messages", n);
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            info!("GitLab output listener: outbound bus closed");
                            break;
                        }
                    }
                }
            }
        }
    })
}

async fn deliver_gitlab(
    pool: &sqlx::SqlitePool,
    gl_config: &GitLabConfig,
    message: &OutboundMessage,
) -> Result<()> {
    // Parse target_id format. Supports two formats:
    //   Legacy:  "project_id:mr_iid" or "project_id:mr_iid:note_id"
    //   New:     "project_id:mr:iid" or "project_id:issue:iid"
    let parts: Vec<&str> = message.reply_context.target_id.split(':').collect();
    if parts.len() < 2 {
        warn!(
            id = %message.id,
            target = %message.reply_context.target_id,
            "invalid GitLab reply target format"
        );
        return Ok(());
    }

    let project_id: i64 = parts[0]
        .parse()
        .context("parse project_id from reply target")?;

    let comment_body = format_comment(message);

    // Determine if this is an issue or MR target.
    let is_issue = parts.len() >= 3 && parts[1] == "issue";
    let is_mr = parts.len() >= 3 && parts[1] == "mr";

    if is_issue {
        let issue_iid: u64 = parts[2]
            .parse()
            .context("parse issue_iid from reply target")?;

        debug!(
            id = %message.id,
            project_id = project_id,
            issue_iid = issue_iid,
            "posting GitLab issue note"
        );
        gitlab::post_issue_note(gl_config, project_id, issue_iid, &comment_body)
            .await
            .context("post GitLab issue note")?;
    } else {
        // MR path — handles both new "project_id:mr:iid" and legacy "project_id:iid" formats.
        let mr_iid: u64 = if is_mr {
            parts[2]
                .parse()
                .context("parse mr_iid from reply target (new format)")?
        } else {
            parts[1]
                .parse()
                .context("parse mr_iid from reply target (legacy format)")?
        };

        // If we have a discussion_id, reply to the thread; otherwise post a top-level note
        if let Some(ref discussion_id) = message.reply_context.thread_id {
            debug!(
                id = %message.id,
                project_id = project_id,
                mr_iid = mr_iid,
                discussion_id = %discussion_id,
                "replying to GitLab discussion"
            );
            gitlab::reply_to_discussion(gl_config, project_id, mr_iid, discussion_id, &comment_body)
                .await
                .context("reply to GitLab discussion")?;
        } else {
            debug!(
                id = %message.id,
                project_id = project_id,
                mr_iid = mr_iid,
                "posting GitLab MR note"
            );
            gitlab::post_mr_note(gl_config, project_id, mr_iid, &comment_body)
                .await
                .context("post GitLab MR note")?;
        }
    }

    // Audit log (best-effort)
    if let Err(e) = audit::log_outbound(pool, message).await {
        warn!(id = %message.id, "audit log failed: {:#}", e);
    }

    info!(
        id = %message.id,
        msg_type = %message.message_type.type_name(),
        "delivered to GitLab"
    );
    Ok(())
}

/// Format an OutboundMessage as a GitLab markdown comment.
fn format_comment(message: &OutboundMessage) -> String {
    match message.message_type {
        OutboundType::Error => {
            format!(":warning: **Error**\n\n{}", message.content)
        }
        OutboundType::Acknowledgment => {
            format!(":hourglass: {}", message.content)
        }
        OutboundType::Progress => {
            format!(":gear: {}", message.content)
        }
        OutboundType::Completion => {
            format!(":white_check_mark: {}", message.content)
        }
        OutboundType::Help => {
            format!(":robot: **Botto Help**\n\n{}", message.content)
        }
        OutboundType::StatusReport => {
            format!(":bar_chart: **Status**\n\n{}", message.content)
        }
        OutboundType::Escalation => {
            format!(
                ":rotating_light: **Human Input Needed**\n\n{}\n\n\
                 _Reply to this comment to respond._",
                message.content
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::channels::types::ReplyTarget;

    fn make_message(msg_type: OutboundType, content: &str) -> OutboundMessage {
        OutboundMessage {
            id: "test".into(),
            reply_context: ReplyTarget {
                channel: ChannelType::GitLab,
                target_id: "1:42:100".into(),
                thread_id: None,
            },
            message_type: msg_type,
            content: content.into(),
            structured_data: None,
            session_id: None,
            directive_id: None,
            created_at: 0,
        }
    }

    #[test]
    fn format_error_comment() {
        let msg = make_message(OutboundType::Error, "Something went wrong");
        let formatted = format_comment(&msg);
        assert!(formatted.contains(":warning:"));
        assert!(formatted.contains("Something went wrong"));
    }

    #[test]
    fn format_completion_comment() {
        let msg = make_message(OutboundType::Completion, "Done!");
        let formatted = format_comment(&msg);
        assert!(formatted.contains(":white_check_mark:"));
    }

    #[test]
    fn format_escalation_comment() {
        let msg = make_message(OutboundType::Escalation, "Need your input");
        let formatted = format_comment(&msg);
        assert!(formatted.contains("Human Input Needed"));
        assert!(formatted.contains("Reply to this comment"));
    }
}
