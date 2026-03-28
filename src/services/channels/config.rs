// ---------------------------------------------------------------------------
// Channel config helpers — permission checks and rate limiting.
//
// Pure functions that operate on ChannelConfig from src/config.rs.
// No state — rate limit state lives in the SQLite channel_rate_limits table.
// ---------------------------------------------------------------------------

use anyhow::{Context, Result};
use sqlx::SqlitePool;
use tracing::{debug, warn};

use super::types::ChannelType;
use crate::config::ChannelConfig;
use crate::services::workflow::crud::epoch_secs;

// ---------------------------------------------------------------------------
// Permission check
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct PermissionCheck {
    pub allowed: bool,
    pub reason: Option<String>,
}

impl PermissionCheck {
    pub fn allowed() -> Self {
        Self {
            allowed: true,
            reason: None,
        }
    }

    pub fn denied(reason: impl Into<String>) -> Self {
        Self {
            allowed: false,
            reason: Some(reason.into()),
        }
    }
}

/// Check whether a channel + user combination is permitted to send messages.
pub fn check_permission(
    config: &ChannelConfig,
    channel: &ChannelType,
    user_id: &str,
) -> PermissionCheck {
    if !config.enabled {
        return PermissionCheck::denied("channel adapter is disabled");
    }

    match channel {
        ChannelType::GitLab => {
            if !config.gitlab.enabled {
                return PermissionCheck::denied("GitLab channel is disabled");
            }
            // Check user allowlist if configured
            if !config.gitlab.allowed_users.is_empty()
                && !config.gitlab.allowed_users.contains(&user_id.to_string())
            {
                return PermissionCheck::denied(format!(
                    "user '{}' is not in the GitLab allowed users list",
                    user_id
                ));
            }
        }
        ChannelType::Slack => {
            if !config.slack.enabled {
                return PermissionCheck::denied("Slack channel is disabled");
            }
        }
        ChannelType::AdminUI | ChannelType::Api => {
            // Always allowed if the top-level channel adapter is enabled
        }
        ChannelType::Cron => {
            // Cron is internal — always allowed
        }
    }

    PermissionCheck::allowed()
}

// ---------------------------------------------------------------------------
// Rate limiting (token bucket stored in SQLite)
// ---------------------------------------------------------------------------

/// Check and consume a rate limit token. Returns Ok(true) if allowed,
/// Ok(false) if rate-limited. Errors only on DB failures.
pub async fn check_rate_limit(
    pool: &SqlitePool,
    config: &ChannelConfig,
    channel: &ChannelType,
    user_id: &str,
) -> Result<bool> {
    let max_per_minute = match channel {
        ChannelType::GitLab => config.gitlab.rate_limit_per_minute,
        ChannelType::Slack => config.slack.rate_limit_per_minute,
        _ => config.default_rate_limit_per_minute,
    };

    if max_per_minute == 0 {
        // 0 = unlimited
        return Ok(true);
    }

    let now = epoch_secs();
    let window_start = now - 60;
    let key = format!("{}:{}", channel.as_str(), user_id);

    // Use a transaction to make the count check + insert atomic,
    // preventing a race where concurrent requests both pass the count
    // check before either inserts.
    let mut tx = pool.begin().await.context("begin rate limit transaction")?;

    // Count requests in the current window
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM channel_rate_limits
         WHERE rate_key = ? AND created_at > ?",
    )
    .bind(&key)
    .bind(window_start)
    .fetch_one(&mut *tx)
    .await
    .context("check rate limit count")?;

    if count >= max_per_minute as i64 {
        debug!(
            channel = %channel,
            user_id = %user_id,
            count = count,
            limit = max_per_minute,
            "rate limit exceeded"
        );
        // Roll back — nothing to commit
        tx.rollback().await.ok();
        return Ok(false);
    }

    // Record this request
    sqlx::query(
        "INSERT INTO channel_rate_limits (rate_key, created_at) VALUES (?, ?)",
    )
    .bind(&key)
    .bind(now)
    .execute(&mut *tx)
    .await
    .context("insert rate limit entry")?;

    tx.commit().await.context("commit rate limit transaction")?;

    // Opportunistic cleanup: remove old entries (best-effort, outside transaction)
    let cleanup_threshold = now - 120;
    if let Err(e) = sqlx::query("DELETE FROM channel_rate_limits WHERE created_at < ?")
        .bind(cleanup_threshold)
        .execute(pool)
        .await
    {
        warn!("rate limit cleanup failed: {}", e);
    }

    Ok(true)
}
