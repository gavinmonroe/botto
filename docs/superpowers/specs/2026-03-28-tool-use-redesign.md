# Agent Tool-Use Redesign — Design Spec

## Overview

Replace the keyword-matching translation layer between the Planner/Generator and agents with AI-native tool-use (function calling). Agents publish typed action catalogs. The Planner generates structured tool calls against the catalog. The Generator dispatches via AI tool-use, not keyword matching. Add a clarification loop for ambiguous requests and adaptive failure recovery.

## The Problem

The current system translates between three languages and loses meaning at every step:
1. User speaks NL → Planner generates NL step descriptions
2. Generator keyword-matches descriptions to agent action names (fragile, lossy)
3. Agents expect exact action names + typed inputs

This causes: wrong action selection ("fetch" → `fetch_mr` instead of `list_open_mrs`), missing inputs (agent needs `project_path` but Generator doesn't extract it), and no way to recover from mismatches.

## The Solution

Use AI's native tool-use capability. The same way Claude Code sees available tools and decides which to call with what parameters — our Generator should do the same.

## 1. Agent Action Registry

Every agent publishes its actions as structured tool definitions that can be passed to an AI model as function schemas.

```rust
/// A tool definition that an agent publishes.
struct ToolDefinition {
    name: String,                    // "gitlab.list_open_mrs"
    description: String,             // "List all open merge requests for a project"
    agent_type: AgentType,
    parameters: Vec<ToolParameter>,  // Typed parameter definitions
    returns: String,                 // Description of return value
}

struct ToolParameter {
    name: String,                    // "project_path"
    param_type: ParamType,          // String, Integer, Boolean, Json
    description: String,             // "GitLab project path (e.g., gitlab-org/gitlab-runner)"
    required: bool,
    default: Option<serde_json::Value>,
}

enum ParamType { String, Integer, Float, Boolean, Json }
```

Each agent implements a `tool_catalog()` method:

```rust
trait WorkflowAgent {
    fn tool_catalog(&self) -> Vec<ToolDefinition>;
    async fn execute(&self, action: &str, inputs: HashMap<String, Value>, mentor: &MentorClient) -> AgentResult;
}
```

The registry collects all tool definitions from all agents into a single catalog. This catalog is passed to the AI model as function definitions.

### Built-in Tool Catalog

```
gitlab.list_open_mrs(project_path: string) → MergeRequest[]
  "List all open merge requests for a GitLab project"

gitlab.fetch_mr(project_path: string, mr_iid: integer) → MergeRequest
  "Fetch a specific merge request by IID"

gitlab.fetch_mr_changes(project_path: string, mr_iid: integer) → MrChanges
  "Fetch the diff/changes for a merge request"

gitlab.post_comment(project_path: string, mr_iid: integer, body: string) → Note
  "Post a comment on a merge request"

gitlab.fetch_pipelines(project_path: string, mr_iid: integer) → Pipeline[]
  "Fetch pipeline status for a merge request"

gitlab.fetch_file(project_path: string, file_path: string, ref?: string) → FileContent
  "Fetch file content from a GitLab repository"

ai.summarize(text: string, context?: string) → string
  "Summarize the given text"

ai.analyze(text: string, criteria: string) → Analysis
  "Analyze text against specific criteria"

ai.chat(prompt: string, system_prompt?: string) → string
  "General-purpose AI chat completion"

ai.decide(question: string, options: string[], context?: string) → Decision
  "Make a decision between options with reasoning"

http.request(method: string, url: string, headers?: json, body?: json) → Response
  "Make an HTTP request to an external API"

script.run(command: string, working_dir?: string, env?: json) → Output
  "Run a shell command"

sandbox.run_in_container(image: string, command: string, timeout_secs?: integer) → Output
  "Run a command in an isolated Docker container"

coding.fix_code(project_path: string, branch: string, task_description: string) → FixResult
  "Clone a repo, understand the codebase, write a fix, run tests, and push"
```

Dynamic tools from the Connector Registry are also included — each connector's actions become tools in the catalog.

## 2. Planner with Clarification Loop

### Before Planning: Clarification

When the Planner receives a request, it first evaluates whether it has enough information:

```
User: "Fix the bug"
Planner: NeedsClarification {
    questions: [
        "Which project/repository is this in?",
        "Can you describe the bug or link to an issue?",
    ]
}
→ Questions sent back to user via channel
→ User answers
→ Planner re-runs with answers as context
```

The Planner's AI call includes a special "clarify" tool:

```
clarify(questions: string[])
  "Ask the user for more information before creating a plan"
```

If the AI calls this tool, the session enters a `Clarifying` state (new state between Created and Planning). The questions go to the user through the escalation protocol. When the user answers, the Planner re-runs.

### Planning with Tool Catalog

The Planner receives the full tool catalog as function definitions. Instead of generating NL step descriptions, it generates structured tool calls:

```json
{
  "goal": "List open MRs from gitlab-org/gitlab-runner and summarize top 3",
  "steps": [
    {
      "id": "fetch-mrs",
      "tool": "gitlab.list_open_mrs",
      "inputs": { "project_path": "gitlab-org/gitlab-runner" },
      "success_criteria": "Returns a non-empty list of MRs"
    },
    {
      "id": "summarize",
      "tool": "ai.summarize",
      "inputs": {
        "text": "{{fetch-mrs.output}}",
        "context": "Summarize the top 3 merge requests by priority"
      },
      "depends_on": ["fetch-mrs"],
      "success_criteria": "Summary covers exactly 3 MRs with titles and key details"
    }
  ]
}
```

Each step has a concrete `tool` name and typed `inputs` — no NL translation needed.

## 3. Generator with Native Tool Use

The Generator no longer keyword-matches. For each step:

1. Look up the tool definition from the registry
2. Resolve input references (`{{fetch-mrs.output}}` → actual data)
3. Call the agent's `execute()` with the exact action name and resolved inputs
4. If the tool call fails, make an AI call with the error + available tools and ask "what should I try instead?"

For steps where the Planner couldn't determine the exact tool (NL fallback), the Generator makes an AI call with the full tool catalog as functions and lets the model pick:

```
System: You are executing a workflow step. Use the available tools to accomplish the task.
User: Step description + dependency outputs + mentor context
Tools: [full catalog as function definitions]
→ AI picks the right tool and fills in parameters
```

This is the same pattern as Claude Code's tool use — the model sees what's available and decides.

## 4. Adaptive Failure Recovery

When a step fails, instead of just retrying:

```
Failure → Analyze error
  ├─ Input problem? → Fix inputs, retry
  │   "project_path not found" → extract from description or ask user
  ├─ Wrong tool? → AI picks a different tool
  │   "unknown action" → re-dispatch with tool catalog
  ├─ Missing capability? → Build a connector
  │   "no tool for jira" → Connector Registry builds one
  ├─ Permission error? → Escalate with specific ask
  │   "401 Unauthorized" → ask user for credentials
  └─ Unknown error? → Escalate with full context
```

The Generator's retry loop becomes an AI-driven recovery loop:

```
System: The previous tool call failed. Here's the error.
        Here are the available tools. What should we try instead?
Tools: [full catalog]
Error: [structured error from the failed call]
→ AI either picks a different tool, adjusts parameters, or says "I need human help"
```

## 5. Execution Trace

Every action is logged as a structured event:

```rust
struct TraceEvent {
    timestamp: i64,
    event_type: TraceEventType,
    step_id: Option<String>,
    data: serde_json::Value,
}

enum TraceEventType {
    PlanCreated,
    ClarificationRequested,
    ClarificationReceived,
    ToolCallStarted,      // tool name, inputs
    ToolCallCompleted,    // tool name, output, duration
    ToolCallFailed,       // tool name, error
    RecoveryAttempted,    // what was tried
    EvaluationRun,        // verdict
    EscalationSent,
    HumanResponseReceived,
    SessionCompleted,
}
```

Stored in a `session_trace` table. The dashboard shows the full trace — every tool call with inputs/outputs, every AI decision, every retry.

## 6. New Session State: Clarifying

```
Created → Clarifying → Planning → Executing ↔ Evaluating → Completed
              ↕                       ↕              ↕
         (user answers)           Adapting    WaitingForHuman
```

`Clarifying` is entered when the Planner needs more info. The session pauses, questions go to the user, answers come back, Planner re-runs.

## 7. Dashboard Improvements

The session detail view shows:
- **Plan** with structured tool calls (not just NL descriptions)
- **Execution trace** — every tool call with expandable inputs/outputs
- **Step outputs** — the actual data (MR list, summary text, etc.)
- **AI decisions** — when the Generator picked a tool, what it considered
- **Errors** — with the recovery attempts
- **Conversation** — clarification questions and answers

## 8. Module Changes

```
MODIFY: src/services/workflow/traits.rs     — add tool_catalog() to WorkflowAgent
CREATE: src/services/workflow/registry.rs   — ToolRegistry collects all tool definitions
MODIFY: src/services/workflow/planner.rs    — use tool catalog, add clarification
MODIFY: src/services/workflow/generator.rs  — replace keyword matching with tool dispatch
MODIFY: src/services/workflow/session.rs    — add Clarifying state, trace logging
MODIFY: src/services/workflow/gitlab.rs     — publish tool catalog
MODIFY: src/services/workflow/ai.rs         — publish tool catalog
MODIFY: src/services/workflow/http.rs       — publish tool catalog
MODIFY: src/services/workflow/script.rs     — publish tool catalog
MODIFY: src/services/workflow/sandbox.rs    — publish tool catalog
MODIFY: src/services/workflow/coding.rs     — publish tool catalog
MODIFY: src/types/workflow.rs               — add Clarifying status, TraceEvent, ToolDefinition
MODIFY: src/db/mod.rs                       — MIGRATION_012 for session_trace table
MODIFY: src/api/admin_workflows.html        — show execution trace, step outputs
```
