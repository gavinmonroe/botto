// ---------------------------------------------------------------------------
// Channel Adapter module — unified messaging layer for GitLab, Slack,
// Admin UI, API, and Cron channels.
// ---------------------------------------------------------------------------

pub mod audit;
pub mod bridge;
pub mod bus;
pub mod config;
pub mod gitlab_input;
pub mod gitlab_output;
pub mod router;
pub mod slack_input;
pub mod slack_output;
pub mod types;
