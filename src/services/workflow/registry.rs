// ---------------------------------------------------------------------------
// Tool Registry — builds the OpenAI-compatible tool catalog for AI function
// calling. Replaces fragile keyword-matching with structured tool definitions.
//
// The catalog is built from the AgentFactoryConfig so it only includes tools
// for agents that are actually configured (e.g., no gitlab tools if no token).
//
// Each tool name follows the pattern "agenttype_action" (e.g., "gitlab_list_open_mrs").
// The special "clarify" tool has no agent prefix — it signals the planner
// needs more information from the user before it can create a plan.
// ---------------------------------------------------------------------------

use crate::services::ai::client::{FunctionDefinition, ToolDefinition};
use crate::services::workflow::factory::AgentFactoryConfig;
use serde_json::json;

/// Build the full tool catalog as OpenAI-compatible ToolDefinitions.
/// Only includes tools for agents that have valid configuration.
pub fn build_tool_catalog(config: &AgentFactoryConfig) -> Vec<ToolDefinition> {
    let mut tools = Vec::new();

    // GitLab tools — only if gitlab config is present.
    if config.gitlab.is_some() {
        tools.extend(gitlab_tools());
    }

    // AI tools — only if ai config is present.
    if config.ai.is_some() {
        tools.extend(ai_tools());
    }

    // HTTP tools — always available.
    tools.push(http_request_tool());

    // Script tools — always available.
    tools.push(script_run_tool());

    // Sandbox tools — always available (agent creation checks Docker at runtime).
    tools.push(sandbox_run_tool());

    // Coding tools — only if botto_config is present (needs sandbox).
    if config.botto_config.is_some() {
        tools.push(coding_fix_tool());
    }

    // Clarify — always available. Used by the planner to request clarification.
    tools.push(clarify_tool());

    tools
}

/// Parse a tool name into (agent_type, action).
/// e.g., "gitlab_list_open_mrs" → ("gitlab", "list_open_mrs")
/// Special case: "clarify" → ("clarify", "clarify")
pub fn parse_tool_name(tool_name: &str) -> (&str, &str) {
    // Match known agent prefixes
    for prefix in &["gitlab_", "ai_", "http_", "script_", "sandbox_", "coding_"] {
        if let Some(action) = tool_name.strip_prefix(prefix) {
            let agent = &tool_name[..prefix.len() - 1]; // strip trailing _
            return (agent, action);
        }
    }
    // Fallback: try dot notation for backwards compat
    if let Some((agent, action)) = tool_name.split_once('.') {
        return (agent, action);
    }
    // Single-word tools like "clarify"
    (tool_name, tool_name)
}

// ---------------------------------------------------------------------------
// Tool definition helpers
// ---------------------------------------------------------------------------

fn tool(name: &str, description: &str, parameters: serde_json::Value) -> ToolDefinition {
    ToolDefinition {
        tool_type: "function".into(),
        function: FunctionDefinition {
            name: name.into(),
            description: description.into(),
            parameters,
        },
    }
}

// ---------------------------------------------------------------------------
// GitLab tools
// ---------------------------------------------------------------------------

fn gitlab_tools() -> Vec<ToolDefinition> {
    vec![
        tool(
            "gitlab_list_open_mrs",
            "List open merge requests for a GitLab project.",
            json!({
                "type": "object",
                "properties": {
                    "project_path": {
                        "type": "string",
                        "description": "GitLab project path (e.g., 'group/repo')"
                    }
                },
                "required": ["project_path"]
            }),
        ),
        tool(
            "gitlab_fetch_mr",
            "Fetch metadata for a specific merge request.",
            json!({
                "type": "object",
                "properties": {
                    "project_path": {
                        "type": "string",
                        "description": "GitLab project path"
                    },
                    "mr_iid": {
                        "type": "integer",
                        "description": "Merge request IID"
                    }
                },
                "required": ["project_path", "mr_iid"]
            }),
        ),
        tool(
            "gitlab_fetch_mr_changes",
            "Fetch the diff/changes for a specific merge request.",
            json!({
                "type": "object",
                "properties": {
                    "project_path": {
                        "type": "string",
                        "description": "GitLab project path"
                    },
                    "mr_iid": {
                        "type": "integer",
                        "description": "Merge request IID"
                    }
                },
                "required": ["project_path", "mr_iid"]
            }),
        ),
        tool(
            "gitlab_post_comment",
            "Post a comment/note on a merge request.",
            json!({
                "type": "object",
                "properties": {
                    "project_path": {
                        "type": "string",
                        "description": "GitLab project path"
                    },
                    "mr_iid": {
                        "type": "integer",
                        "description": "Merge request IID"
                    },
                    "body": {
                        "type": "string",
                        "description": "Comment body text (markdown supported)"
                    }
                },
                "required": ["project_path", "mr_iid", "body"]
            }),
        ),
        tool(
            "gitlab_fetch_file",
            "Fetch raw file content from a GitLab repository.",
            json!({
                "type": "object",
                "properties": {
                    "project_path": {
                        "type": "string",
                        "description": "GitLab project path"
                    },
                    "file_path": {
                        "type": "string",
                        "description": "Path to the file in the repository"
                    },
                    "ref": {
                        "type": "string",
                        "description": "Git ref (branch, tag, or commit SHA). Defaults to 'main'."
                    }
                },
                "required": ["project_path", "file_path"]
            }),
        ),
    ]
}

// ---------------------------------------------------------------------------
// AI tools
// ---------------------------------------------------------------------------

fn ai_tools() -> Vec<ToolDefinition> {
    vec![
        tool(
            "ai_summarize",
            "Summarize text content using AI.",
            json!({
                "type": "object",
                "properties": {
                    "text": {
                        "type": "string",
                        "description": "Text to summarize"
                    },
                    "context": {
                        "type": "string",
                        "description": "Optional context/instructions for the summary"
                    }
                },
                "required": ["text"]
            }),
        ),
        tool(
            "ai_analyze",
            "Analyze content with a custom prompt using AI.",
            json!({
                "type": "object",
                "properties": {
                    "content": {
                        "type": "string",
                        "description": "Content to analyze"
                    },
                    "prompt": {
                        "type": "string",
                        "description": "Analysis instructions/prompt"
                    }
                },
                "required": ["content", "prompt"]
            }),
        ),
        tool(
            "ai_chat",
            "Send a chat completion request to the AI model.",
            json!({
                "type": "object",
                "properties": {
                    "system": {
                        "type": "string",
                        "description": "System prompt"
                    },
                    "user": {
                        "type": "string",
                        "description": "User message"
                    }
                },
                "required": ["user"]
            }),
        ),
    ]
}

// ---------------------------------------------------------------------------
// HTTP tool
// ---------------------------------------------------------------------------

fn http_request_tool() -> ToolDefinition {
    tool(
        "http_request",
        "Make an HTTP request to an external API. Supports GET, POST, PUT, DELETE.",
        json!({
            "type": "object",
            "properties": {
                "method": {
                    "type": "string",
                    "enum": ["get", "post", "put", "delete"],
                    "description": "HTTP method"
                },
                "url": {
                    "type": "string",
                    "description": "Target URL"
                },
                "headers": {
                    "type": "object",
                    "description": "Request headers as key-value pairs"
                },
                "body": {
                    "type": "object",
                    "description": "JSON request body (for POST/PUT)"
                },
                "auth_header": {
                    "type": "string",
                    "description": "Authorization header value"
                }
            },
            "required": ["method", "url"]
        }),
    )
}

// ---------------------------------------------------------------------------
// Script tool
// ---------------------------------------------------------------------------

fn script_run_tool() -> ToolDefinition {
    tool(
        "script_run",
        "Run a shell command on the host with resource limits.",
        json!({
            "type": "object",
            "properties": {
                "command": {
                    "type": "string",
                    "description": "Shell command to execute"
                },
                "shell": {
                    "type": "string",
                    "enum": ["sh", "bash"],
                    "description": "Shell to use (default: sh)"
                },
                "timeout_secs": {
                    "type": "integer",
                    "description": "Command timeout in seconds (default: 60)"
                },
                "working_dir": {
                    "type": "string",
                    "description": "Working directory for the command"
                }
            },
            "required": ["command"]
        }),
    )
}

// ---------------------------------------------------------------------------
// Sandbox tool
// ---------------------------------------------------------------------------

fn sandbox_run_tool() -> ToolDefinition {
    tool(
        "sandbox_run_in_container",
        "Run a command in an isolated Docker container.",
        json!({
            "type": "object",
            "properties": {
                "image": {
                    "type": "string",
                    "description": "Docker image to use"
                },
                "command": {
                    "type": "string",
                    "description": "Command to run inside the container"
                },
                "timeout_secs": {
                    "type": "integer",
                    "description": "Container timeout in seconds (default: 300)"
                },
                "env": {
                    "type": "object",
                    "description": "Environment variables as key-value pairs"
                },
                "network_enabled": {
                    "type": "boolean",
                    "description": "Allow network access (default: false)"
                }
            },
            "required": ["image", "command"]
        }),
    )
}

// ---------------------------------------------------------------------------
// Coding tool
// ---------------------------------------------------------------------------

fn coding_fix_tool() -> ToolDefinition {
    tool(
        "coding_fix_code",
        "Run the full multi-turn AI coding pipeline: clone repo, understand codebase, write code, run tests, iterate until passing, commit and push.",
        json!({
            "type": "object",
            "properties": {
                "project_path": {
                    "type": "string",
                    "description": "GitLab project path (e.g., 'group/repo')"
                },
                "branch": {
                    "type": "string",
                    "description": "Source branch to work on"
                },
                "task_description": {
                    "type": "string",
                    "description": "What to fix or build"
                },
                "file_path": {
                    "type": "string",
                    "description": "Specific file to focus on (optional)"
                },
                "suggestion": {
                    "type": "string",
                    "description": "Suggested fix or approach (optional)"
                }
            },
            "required": ["project_path", "branch", "task_description"]
        }),
    )
}

// ---------------------------------------------------------------------------
// Clarify tool — special: no agent, signals need for user input
// ---------------------------------------------------------------------------

fn clarify_tool() -> ToolDefinition {
    tool(
        "clarify",
        "Request clarification from the user before creating a plan. Use this when the trigger data or workflow description is ambiguous and you need more information to create a good plan.",
        json!({
            "type": "object",
            "properties": {
                "questions": {
                    "type": "array",
                    "items": { "type": "string" },
                    "description": "List of questions to ask the user"
                },
                "reason": {
                    "type": "string",
                    "description": "Why clarification is needed"
                }
            },
            "required": ["questions", "reason"]
        }),
    )
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::ai::client::AiClientConfig;
    use crate::services::gitlab::client::GitLabConfig;
    use sqlx::SqlitePool;

    fn full_config() -> AgentFactoryConfig {
        AgentFactoryConfig {
            gitlab: Some(GitLabConfig {
                base_url: "https://gitlab.com".into(),
                token: "test".into(),
            }),
            ai: Some(AiClientConfig {
                base_url: "https://api.example.com".into(),
                api_key: "test".into(),
            }),
            ai_default_model: "test-model".into(),
            sandbox_max_memory_mb: 2048,
            pool: SqlitePool::connect_lazy("sqlite::memory:").unwrap(),
            botto_config: None,
            event_bus: None,
        }
    }

    #[tokio::test]
    async fn build_catalog_includes_all_configured_tools() {
        let config = full_config();
        let catalog = build_tool_catalog(&config);

        let names: Vec<&str> = catalog.iter().map(|t| t.function.name.as_str()).collect();

        // GitLab tools
        assert!(names.contains(&"gitlab_list_open_mrs"));
        assert!(names.contains(&"gitlab_fetch_mr"));
        assert!(names.contains(&"gitlab_fetch_mr_changes"));
        assert!(names.contains(&"gitlab_post_comment"));
        assert!(names.contains(&"gitlab_fetch_file"));

        // AI tools
        assert!(names.contains(&"ai_summarize"));
        assert!(names.contains(&"ai_analyze"));
        assert!(names.contains(&"ai_chat"));

        // Other tools
        assert!(names.contains(&"http_request"));
        assert!(names.contains(&"script_run"));
        assert!(names.contains(&"sandbox_run_in_container"));

        // Clarify is always present
        assert!(names.contains(&"clarify"));
    }

    #[tokio::test]
    async fn build_catalog_excludes_unconfigured_agents() {
        let mut config = full_config();
        config.gitlab = None;
        config.ai = None;

        let catalog = build_tool_catalog(&config);
        let names: Vec<&str> = catalog.iter().map(|t| t.function.name.as_str()).collect();

        assert!(!names.contains(&"gitlab_list_open_mrs"));
        assert!(!names.contains(&"ai_summarize"));
        // These should still be present
        assert!(names.contains(&"http_request"));
        assert!(names.contains(&"clarify"));
    }

    #[test]
    fn parse_tool_name_dotted() {
        assert_eq!(parse_tool_name("gitlab_list_open_mrs"), ("gitlab", "list_open_mrs"));
        assert_eq!(parse_tool_name("ai_summarize"), ("ai", "summarize"));
        assert_eq!(parse_tool_name("http_request"), ("http", "request"));
    }

    #[test]
    fn parse_tool_name_single() {
        assert_eq!(parse_tool_name("clarify"), ("clarify", "clarify"));
    }

    #[tokio::test]
    async fn all_tools_have_function_type() {
        let config = full_config();
        let catalog = build_tool_catalog(&config);
        for tool in &catalog {
            assert_eq!(tool.tool_type, "function");
        }
    }

    #[tokio::test]
    async fn all_tools_have_valid_json_schema_parameters() {
        let config = full_config();
        let catalog = build_tool_catalog(&config);
        for tool in &catalog {
            let params = &tool.function.parameters;
            assert_eq!(params.get("type").and_then(|v| v.as_str()), Some("object"));
            assert!(params.get("properties").is_some());
        }
    }
}
