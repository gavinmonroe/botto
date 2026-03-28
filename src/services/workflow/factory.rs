// ---------------------------------------------------------------------------
// Agent factory — creates the right WorkflowAgent for a given AgentType.
//
// The orchestrator calls `create_agent()` for each step, passing the step's
// agent_type. The factory returns a boxed trait object that the orchestrator
// dispatches to without knowing the concrete type.
// ---------------------------------------------------------------------------

use crate::config::BottoConfig;
use crate::services::ai::client::AiClientConfig;
use crate::services::events::EventBus;
use crate::services::gitlab::client::GitLabConfig;
use crate::services::workflow::ai::AiAgent;
use crate::services::workflow::coding::CodingAgent;
use crate::services::workflow::composite::CompositeAgent;
use crate::services::workflow::gitlab::GitLabAgent;
use crate::services::workflow::http::HttpAgent;
use crate::services::workflow::sandbox::SandboxAgent;
use crate::services::workflow::script::ScriptAgent;
use crate::services::workflow::traits::WorkflowAgent;
use crate::types::workflow::AgentType;
use sqlx::SqlitePool;

/// Shared configuration for agent construction.
/// Passed to the factory so it can build any agent type.
#[derive(Clone)]
pub struct AgentFactoryConfig {
    pub gitlab: Option<GitLabConfig>,
    pub ai: Option<AiClientConfig>,
    pub ai_default_model: String,
    pub sandbox_max_memory_mb: u64,
    /// SQLite pool needed by CompositeAgent to load sub-workflow definitions.
    pub pool: SqlitePool,
    /// Full BottoConfig needed by CodingAgent (SandboxManager).
    pub botto_config: Option<BottoConfig>,
    /// EventBus needed by CodingAgent (SandboxManager).
    pub event_bus: Option<EventBus>,
}

/// Create a workflow agent for the given type.
///
/// Returns `None` if the required backing service isn't configured
/// (e.g., no GitLab token, no Docker).
///
/// `depth` tracks composite nesting level to prevent runaway recursion.
pub async fn create_agent(
    agent_type: &AgentType,
    config: &AgentFactoryConfig,
    depth: u32,
) -> Option<Box<dyn WorkflowAgent>> {
    match agent_type {
        AgentType::Gitlab => {
            let gl_cfg = config.gitlab.clone()?;
            Some(Box::new(GitLabAgent::new(gl_cfg)))
        }
        AgentType::Ai => {
            let ai_cfg = config.ai.clone()?;
            Some(Box::new(AiAgent::new(
                ai_cfg,
                config.ai_default_model.clone(),
            )))
        }
        AgentType::Http => match HttpAgent::new() {
            Ok(agent) => Some(Box::new(agent)),
            Err(e) => {
                tracing::warn!(error = %e, "failed to create HTTP agent");
                None
            }
        },
        AgentType::Script => Some(Box::new(ScriptAgent::new())),
        AgentType::Sandbox => {
            let agent = SandboxAgent::try_new(config.sandbox_max_memory_mb).await?;
            Some(Box::new(agent))
        }
        AgentType::Composite => {
            Some(Box::new(CompositeAgent::new(
                config.pool.clone(),
                config.clone(),
                config.sandbox_max_memory_mb, // reuse as default_step_timeout placeholder
                depth,
            )))
        }
        AgentType::Coding => {
            let botto_cfg = config.botto_config.clone()?;
            let event_bus = config.event_bus.clone()?;
            let agent = CodingAgent::try_new(
                botto_cfg,
                config.pool.clone(),
                event_bus,
            )?;
            Some(Box::new(agent))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> AgentFactoryConfig {
        AgentFactoryConfig {
            gitlab: Some(GitLabConfig {
                base_url: "https://gitlab.com".into(),
                token: "test-token".into(),
            }),
            ai: Some(AiClientConfig {
                base_url: "https://api.example.com".into(),
                api_key: "test-key".into(),
            }),
            ai_default_model: "claude-sonnet-4-5".into(),
            sandbox_max_memory_mb: 2048,
            pool: SqlitePool::connect_lazy("sqlite::memory:").unwrap(),
            botto_config: None,
            event_bus: None,
        }
    }

    #[tokio::test]
    async fn creates_gitlab_agent() {
        let cfg = test_config();
        let agent = create_agent(&AgentType::Gitlab, &cfg, 0).await;
        assert!(agent.is_some());
        assert_eq!(agent.unwrap().agent_type_name(), "gitlab");
    }

    #[tokio::test]
    async fn creates_ai_agent() {
        let cfg = test_config();
        let agent = create_agent(&AgentType::Ai, &cfg, 0).await;
        assert!(agent.is_some());
        assert_eq!(agent.unwrap().agent_type_name(), "ai");
    }

    #[tokio::test]
    async fn creates_http_agent() {
        let cfg = test_config();
        let agent = create_agent(&AgentType::Http, &cfg, 0).await;
        assert!(agent.is_some());
        assert_eq!(agent.unwrap().agent_type_name(), "http");
    }

    #[tokio::test]
    async fn creates_script_agent() {
        let cfg = test_config();
        let agent = create_agent(&AgentType::Script, &cfg, 0).await;
        assert!(agent.is_some());
        assert_eq!(agent.unwrap().agent_type_name(), "script");
    }

    #[tokio::test]
    async fn creates_composite_agent() {
        let cfg = test_config();
        let agent = create_agent(&AgentType::Composite, &cfg, 0).await;
        assert!(agent.is_some());
        assert_eq!(agent.unwrap().agent_type_name(), "composite");
    }

    #[tokio::test]
    async fn missing_gitlab_config_returns_none() {
        let mut cfg = test_config();
        cfg.gitlab = None;
        let agent = create_agent(&AgentType::Gitlab, &cfg, 0).await;
        assert!(agent.is_none());
    }

    #[tokio::test]
    async fn missing_ai_config_returns_none() {
        let mut cfg = test_config();
        cfg.ai = None;
        let agent = create_agent(&AgentType::Ai, &cfg, 0).await;
        assert!(agent.is_none());
    }
}
