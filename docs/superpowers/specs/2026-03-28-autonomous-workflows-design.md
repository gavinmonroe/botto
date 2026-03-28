# Autonomous Workflow System — Design Spec

## Overview

An autonomous workflow engine for botto that lets users describe work in natural language, which gets decomposed into a structured DAG of steps, executed by spawned sub-agents, verified at each gate and at completion, and improved over time through a Mentor knowledge layer.

The system has four layers: Workflow API (creation & management), Orchestrator (adaptive DAG execution), Agent Pool (stateless workers), and Mentor (institutional memory).

## 1. Workflow Definition & Creation

### User Flow

1. User sends a natural language message describing the workflow (e.g., "Every morning, check all open MRs older than 3 days and ping the authors on Slack")
2. AI parses intent — extracts goal, trigger, and rough steps
3. System decomposes into a DAG and presents it back to the user
4. User refines: add/remove/reorder steps, adjust success criteria, change schedule
5. System persists the final definition to SQLite

### Workflow Definition Schema

```rust
struct WorkflowDefinition {
    id: Uuid,
    name: String,
    description: String,           // original natural language intent
    project_id: i64,               // owning GitLab project
    steps: Vec<WorkflowStep>,
    triggers: Vec<Trigger>,
    created_by: String,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    enabled: bool,
}

struct WorkflowStep {
    id: String,                    // unique within workflow (e.g., "fetch-mrs")
    action: String,                // what to do
    agent_type: AgentType,         // gitlab, ai, sandbox, http, script, composite
    inputs: HashMap<String, StepInput>,  // static values or references to prior step outputs
    success_criteria: String,      // how to verify this step succeeded
    depends_on: Vec<String>,       // step IDs that must complete first
    retry_policy: RetryPolicy,
    timeout: Duration,
}

enum StepInput {
    Static(serde_json::Value),
    StepOutput { step_id: String, field: String },
    MentorQuery(String),
}

struct RetryPolicy {
    max_retries: u32,              // default 2
    backoff: BackoffStrategy,      // fixed, exponential
    consult_mentor_on_failure: bool, // ask Mentor for alternative strategies
}

enum Trigger {
    Cron { schedule: String },                    // "0 9 * * 1-5"
    Event { event_type: String, filter: Option<String> }, // "mr.opened", optional filter
    Manual,
}
```

### Persistence

Workflow definitions are stored in SQLite as JSON blobs with indexed metadata columns (id, name, project_id, enabled). This keeps the schema flexible as the definition evolves.

## 2. The Orchestrator

Each workflow run gets its own orchestrator instance that manages the full lifecycle.

### Execution Flow

1. Load workflow definition (DAG of steps)
2. Query Mentor: "anything relevant to this workflow?" — retrieve past learnings, known failure patterns, domain context
3. Walk DAG in topological order. Steps with no unmet dependencies run in parallel.
4. For each step:
   a. Spawn the appropriate sub-agent with action, inputs (including resolved outputs from prior steps), and Mentor context
   b. Wait for agent result
   c. **Step gate**: evaluate result against `success_criteria`
      - Pass → mark complete, advance
      - Fail → consult retry policy. If `consult_mentor_on_failure`, query Mentor for recovery strategies. Retry or mark failed.
   d. If step fails after all retries, orchestrator adapts: insert recovery step, skip dependent steps, or escalate to user
5. **Final verification**: compare overall outcome against original work order description. Did we accomplish what was asked?
6. Report results. Feed learnings back to Mentor.

### Adaptive Behavior

The orchestrator holds the original natural language intent and can make judgment calls. Adaptation is implemented by calling the AI service with the current run state + original intent + Mentor context, and asking it to decide the next action. Possible adaptations:

- Partial results from a step → AI decides if good enough or retries with different parameters
- Unexpected error → query Mentor for similar past failures, AI picks a recovery strategy
- New information from a step changes the plan → AI proposes step insertions/removals, orchestrator validates they don't violate the DAG structure before applying
- A step that historically fails → Mentor surfaces the known workaround, orchestrator applies it preemptively

Adaptations are logged as `AdaptationEvent` entries in the run log so they're auditable.

### State & Checkpointing

```rust
struct WorkflowRun {
    id: Uuid,
    workflow_id: Uuid,
    trigger: TriggerSource,        // what kicked off this run
    status: RunStatus,             // pending, running, completed, failed, cancelled
    step_states: HashMap<String, StepState>,
    started_at: DateTime<Utc>,
    completed_at: Option<DateTime<Utc>>,
    final_verification: Option<VerificationResult>,
    mentor_queries: Vec<MentorInteraction>,
}

enum StepState {
    Pending,
    Running { agent_id: Uuid, started_at: DateTime<Utc> },
    Completed { output: serde_json::Value, duration: Duration },
    Failed { error: String, retries: u32, duration: Duration },
    Skipped { reason: String },
}
```

State is checkpointed to SQLite after each step transition. On crash recovery, the orchestrator resumes from the last checkpoint.

## 3. Agent Pool

Agents are stateless workers spawned by the orchestrator for each step.

### Agent Types

| Type | Purpose | Implementation |
|------|---------|---------------|
| `gitlab` | GitLab API operations (list MRs, post comments, check pipelines, manage branches) | Botto's existing `GitlabClient` |
| `ai` | AI analysis, summarization, decision-making, code generation | Botto's existing AI client |
| `sandbox` | Run scripts, build/test code, apply patches in isolated Docker containers | Botto's existing sandbox manager |
| `http` | Call external APIs (Slack, Jira, custom webhooks) | New HTTP agent with configurable auth |
| `script` | Run shell commands on the host | Spawned process with resource limits and timeout |
| `composite` | A mini-workflow — delegates to other agents | Nested orchestrator for reusable sub-flows |

### Agent Interface

All agents implement the same trait:

```rust
#[async_trait]
trait WorkflowAgent {
    async fn execute(
        &self,
        action: &str,
        inputs: HashMap<String, serde_json::Value>,
        mentor: &MentorClient,
    ) -> AgentResult;
}

struct AgentResult {
    status: AgentStatus,           // success, failure, partial
    output: serde_json::Value,     // structured output for downstream steps
    duration: Duration,
    learnings: Vec<MentorEntry>,   // things to feed back to Mentor
}
```

### Agent Lifecycle

1. Orchestrator spawns agent with action, resolved inputs, and Mentor context
2. Agent executes work
3. Agent can query Mentor mid-execution for guidance
4. Agent can write to Mentor: "remember this for next time"
5. Agent returns structured result
6. Agent is torn down — no state persists between runs

### Composite Agents

Common patterns (e.g., "fetch MR → analyze code → post comment") can be saved as composite agents and reused across workflows. Users build a library of reusable building blocks over time. A composite agent is just a workflow definition that can be referenced as a step in another workflow.

## 4. The Mentor

Institutional memory that gets smarter with every workflow run.

### Knowledge Types

- **Execution patterns**: rate limits, retry strategies, timing quirks, failure modes
- **Domain knowledge**: service dependencies, architecture facts, deployment constraints
- **Workflow learnings**: optimization tips, step ordering insights, common failure points
- **User corrections**: explicit "remember this" entries from agents or users

### Storage Schema

```sql
CREATE TABLE mentor_entries (
    id TEXT PRIMARY KEY,
    content TEXT NOT NULL,
    scope TEXT NOT NULL,            -- repo path or linked-set name or "global"
    scope_type TEXT NOT NULL,       -- "repo", "linked", "global"
    category TEXT NOT NULL,         -- "execution", "domain", "workflow", "correction"
    source_workflow_id TEXT,        -- which workflow created this (nullable for user entries)
    source_step_id TEXT,
    created_at INTEGER NOT NULL,
    last_queried_at INTEGER,
    hit_count INTEGER DEFAULT 0,
    confidence REAL DEFAULT 1.0     -- decays over time if never queried
);

CREATE VIRTUAL TABLE mentor_fts USING fts5(content, scope, category);
```

### Scoping

- **Repo-scoped**: knowledge specific to one repository. Most entries live here.
- **Project-linked**: spans explicitly linked repos. Configured via botto settings:
  ```toml
  [mentor]
  linked_repos = [
      { name = "payments", repos = ["service-auth", "service-users", "service-billing"] },
  ]
  ```
  When an agent queries the Mentor in any linked repo, results from the entire linked set are included.
- **Global**: cross-cutting knowledge that applies everywhere. Rare.

### Query Resolution

When an agent queries the Mentor:
1. Search repo-scoped entries for the current repo (FTS5 match)
2. If the repo belongs to a linked set, also search entries scoped to sibling repos
3. Search global entries
4. Rank by relevance (FTS5 score) × confidence × recency
5. Return top-N results

### Agent Interface

```rust
struct MentorClient {
    pool: SqlitePool,
    current_repo: String,
}

impl MentorClient {
    /// Semantic search across scoped knowledge
    async fn query(&self, question: &str) -> Vec<MentorEntry>;

    /// Store new knowledge
    async fn remember(&self, content: &str, scope: &str, category: &str) -> MentorEntryId;

    /// Remove outdated or wrong knowledge
    async fn forget(&self, entry_id: MentorEntryId);
}
```

### Automatic Learning

After every workflow run, the orchestrator feeds back:
- Steps that failed and how they were recovered → execution pattern
- New domain facts discovered during the run → domain knowledge
- Timing data and optimization opportunities → execution pattern
- User corrections or overrides during the run → correction

### Self-Pruning

Entries have a `confidence` score that decays over time if never queried. A background task periodically prunes entries below a confidence threshold. Entries that are frequently queried get their confidence boosted. This keeps the Mentor lean and relevant.

## 5. Triggers, Scheduling & Monitoring

### Trigger System

- **Cron triggers**: A tokio-based scheduler checks every minute for workflows due to run. Schedules are persisted in SQLite and survive restarts.
- **Event triggers**: GitLab webhooks (MR opened, pipeline failed, push, comment) are matched against workflow trigger definitions. Multiple workflows can fire from the same event. Event filters allow narrowing (e.g., only MRs with label "urgent").
- **Manual triggers**: User kicks off a workflow via WebSocket message or admin API endpoint.

Each trigger creates a new workflow run — a new orchestrator instance with its own state and logs.

### Monitoring

- **Run log**: Every run is logged to SQLite — start time, step-by-step progress, outcomes, duration, Mentor queries made.
- **Live status**: Active runs stream progress to connected Otto extensions via the existing WebSocket gateway. Users see which step is running, what's passed, what's failed.
- **Work order tracking**: The orchestrator tracks completion against the original work order. If the workflow has 5 deliverables to count as "done", the dashboard shows 3/5 complete with details on each.
- **Alerts**: Failed runs or stuck steps emit events on the existing event bus, which can be extended to push to Slack/email via an `http` agent.

### Concurrency

A configurable semaphore limits simultaneous workflow runs. Queued runs are priority-scored (similar to the existing review queue). Configuration:

```toml
[workflows]
max_concurrent_runs = 3
default_step_timeout = "5m"
checkpoint_interval = "after_each_step"
```

## 6. Integration with Existing Botto

This system builds on botto's existing infrastructure:

| Existing Component | How Workflows Use It |
|---|---|
| `GitlabClient` | `gitlab` agent type wraps it directly |
| AI client (`services/ai`) | `ai` agent type wraps it directly |
| Sandbox manager | `sandbox` agent type wraps it directly |
| WebSocket gateway | Streams workflow run progress to Otto extensions |
| Event bus | Workflow triggers listen for events; workflow completions emit events |
| SQLite + migrations | New tables for workflow definitions, runs, step states, mentor entries |
| Queue manager pattern | Workflow concurrency uses the same semaphore + priority pattern |
| Config (TOML + env) | New `[workflows]` and `[mentor]` config sections |

### New Modules

```
src/services/workflows/
├── mod.rs              // module root
├── definition.rs       // WorkflowDefinition, WorkflowStep, Trigger types
├── orchestrator.rs     // DAG execution, step gating, adaptive behavior
├── agents/
│   ├── mod.rs          // WorkflowAgent trait
│   ├── gitlab.rs       // GitLab agent
│   ├── ai.rs           // AI agent
│   ├── sandbox.rs      // Sandbox agent
│   ├── http.rs         // HTTP agent (new)
│   ├── script.rs       // Script agent (new)
│   └── composite.rs    // Composite agent
├── scheduler.rs        // Cron trigger loop
├── decomposer.rs       // NL → DAG decomposition via AI
└── monitor.rs          // Run logging, live status, work order tracking

src/services/mentor/
├── mod.rs              // MentorClient
├── store.rs            // SQLite + FTS5 storage
├── pruner.rs           // Background confidence decay + pruning
└── linker.rs           // Cross-project repo linking
```

### New Database Tables

- `workflows` — workflow definitions (JSON blob + indexed metadata)
- `workflow_runs` — run instances with status and checkpoint data
- `workflow_step_states` — per-step state within a run
- `mentor_entries` — knowledge store with FTS5 index
- `mentor_repo_links` — explicit repo-to-repo linking

### New Config Sections

```toml
[workflows]
enabled = true
max_concurrent_runs = 3
default_step_timeout = "5m"

[mentor]
enabled = true
prune_below_confidence = 0.1
prune_interval = "24h"
linked_repos = [
    { name = "payments", repos = ["service-auth", "service-users", "service-billing"] },
]
```

### New API Endpoints

| Route | Method | Description |
|-------|--------|-------------|
| `/api/workflows` | GET | List all workflows |
| `/api/workflows` | POST | Create workflow (NL → DAG) |
| `/api/workflows/:id` | GET/PUT/DELETE | Manage a workflow |
| `/api/workflows/:id/runs` | GET | List runs for a workflow |
| `/api/workflows/:id/run` | POST | Manually trigger a run |
| `/api/workflows/runs/:id` | GET | Get run status + step states |
| `/api/mentor/query` | POST | Query the Mentor |
| `/api/mentor/entries` | GET/POST/DELETE | Manage Mentor entries |

### WebSocket Messages

New message types for the existing Otto WebSocket protocol:

- `WORKFLOW_RUN_STARTED` — broadcast when a run begins
- `WORKFLOW_STEP_UPDATE` — broadcast per-step progress (running, completed, failed)
- `WORKFLOW_RUN_COMPLETE` — broadcast final result + verification outcome
- `WORKFLOW_CREATE` — Otto sends NL description, receives decomposed DAG for refinement
- `WORKFLOW_CONFIRM` — Otto confirms the refined DAG, persists the workflow
