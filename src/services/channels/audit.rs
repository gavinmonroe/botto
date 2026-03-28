// ---------------------------------------------------------------------------
// Channel audit — log inbound/outbound messages to channel_messages table.
//
// Provides a complete audit trail of all channel interactions. Also supports
// fetching thread history for context in multi-turn conversations.
// ---------------------------------------------------------------------------

use anyhow::{Context, Result};
use sqlx::SqlitePool;
use tracing::debug;

use super::types::{InboundMessage, OutboundMessage};

/// Log an inbound message to the audit table.
pub async fn log_inbound(pool: &SqlitePool, message: &InboundMessage) -> Result<()> {
    let context_json =
        serde_json::to_string(&message.context).context("serialize inbound context")?;

    sqlx::query(
        "INSERT INTO channel_messages
         (id, direction, channel, channel_id, user_id, user_name, thread_id,
          action, content, context_json, created_at)
         VALUES (?, 'inbound', ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&message.id)
    .bind(message.context.channel.as_str())
    .bind(&message.context.channel_id)
    .bind(&message.context.user_id)
    .bind(&message.context.user_name)
    .bind(&message.context.thread_id)
    .bind(message.action.action_name())
    .bind(&message.raw_content)
    .bind(&context_json)
    .bind(message.parsed_at)
    .execute(pool)
    .await
    .context("insert inbound channel message")?;

    debug!(id = %message.id, channel = %message.context.channel, "logged inbound message");
    Ok(())
}

/// Log an outbound message to the audit table.
pub async fn log_outbound(pool: &SqlitePool, message: &OutboundMessage) -> Result<()> {
    let reply_json =
        serde_json::to_string(&message.reply_context).context("serialize reply context")?;

    sqlx::query(
        "INSERT INTO channel_messages
         (id, direction, channel, channel_id, user_id, user_name, thread_id,
          action, content, context_json, created_at)
         VALUES (?, 'outbound', ?, ?, '', '', ?, ?, ?, ?, ?)",
    )
    .bind(&message.id)
    .bind(message.reply_context.channel.as_str())
    .bind(&message.reply_context.target_id)
    .bind(&message.reply_context.thread_id)
    .bind(message.message_type.type_name())
    .bind(&message.content)
    .bind(&reply_json)
    .bind(message.created_at)
    .execute(pool)
    .await
    .context("insert outbound channel message")?;

    debug!(id = %message.id, channel = %message.reply_context.channel, "logged outbound message");
    Ok(())
}

/// Fetch message history for a thread (both inbound and outbound), ordered by time.
pub async fn get_thread_history(
    pool: &SqlitePool,
    channel: &str,
    thread_id: &str,
    limit: i64,
) -> Result<Vec<AuditEntry>> {
    let rows: Vec<(String, String, String, String, String, String, i64)> = sqlx::query_as(
        "SELECT id, direction, user_name, action, content, context_json, created_at
         FROM channel_messages
         WHERE channel = ? AND thread_id = ?
         ORDER BY created_at DESC
         LIMIT ?",
    )
    .bind(channel)
    .bind(thread_id)
    .bind(limit)
    .fetch_all(pool)
    .await
    .context("fetch thread history")?;

    let entries = rows
        .into_iter()
        .map(|(id, direction, user_name, action, content, context_json, created_at)| {
            AuditEntry {
                id,
                direction,
                user_name,
                action,
                content,
                context_json,
                created_at,
            }
        })
        .collect();

    Ok(entries)
}

#[derive(Debug, Clone)]
pub struct AuditEntry {
    pub id: String,
    pub direction: String,
    pub user_name: String,
    pub action: String,
    pub content: String,
    pub context_json: String,
    pub created_at: i64,
}
