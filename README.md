# Botto — Shared AI Code Review Backend for GitLab

<p align="center">
  <img src="botto.svg" alt="Botto" width="120" />
</p>

<p align="center">
  Shared orchestration backend for <a href="../otto">Otto</a> Chrome extensions. One review per MR, served to the entire team.
</p>

Botto is a single Rust binary that centralizes AI-powered code reviews, caching, and event coordination across a development team using GitLab. When multiple developers have the Otto browser extension installed, Botto replaces per-extension AI calls with a shared server — so one review per MR serves everyone.

All AI suggestions remain drafts. Nothing is auto-posted to GitLab.

Works with gitlab.com, self-hosted GitLab instances, and any OpenAI-compatible API endpoint (OpenRouter, Ollama, local models, etc.).

## Why Botto?

- **One review, whole team** — if User A triggers a review on MR !42, Users B–E get the same results instantly (cached or live-streamed)
- **No duplicate AI calls** — SQLite-backed, gzip-compressed, diff-hash-keyed cache eliminates redundant work
- **Real-time sync** — comment actions, presence, review progress broadcast to all viewers via WebSocket
- **Sandbox auto-fix** — Docker containers that clone the repo, apply a suggested fix, run tests, and push on success
- **Follow-up fix** — fixes triggered from comment follow-up analysis, not just Botto review findings
- **Admin settings page** — embedded web UI at `/admin` for live config changes (hot-swap, no restart needed for most settings)
- **Self-evolving prompts** — built-in harness that autonomously improves sandbox fix prompts through evolution loops
- **Priority queue** — reviews scored and executed in priority order, with pause/resume/cancel
- **Auto-review on push** — optionally enqueue reviews when commits land on open MRs, so the AI review is cached before a human even opens the page
- **Works with any model** — OpenAI, Anthropic, Mistral, Ollama, or any OpenAI-compatible endpoint

## Features

### Shared Review Pipeline

When any Otto extension triggers a review, Botto runs a 3-phase parallel AI pipeline and streams results to all connected viewers in real-time:

```
MR Diff → Parse Context → [Phase 1 — Parallel]
                           ├─ MR Summary (+ ticket context)
                           ├─ File Activity Analysis
                           └─ Context Enrichment
                                    ↓
                          [Phase 2 — Parallel, after context ready]
                           ├─ Code Review (batched, concurrent, incremental)
                           ├─ Edge Cases
                           ├─ Related Files
                           └─ AC Validation
                                    ↓
                          [Phase 3 — Verification]
                           ├─ Adversarial Tests
                           ├─ Contracts
                           └─ Behavioral Delta
                                    ↓
                           Trust Score Computation
                                    ↓
                           STREAM_ALL_COMPLETE → broadcast to all viewers
```

Late-joiners get a replay of buffered chunks plus live subscription. In-flight deduplication prevents duplicate reviews using atomic entry operations.

### Review Cache

SQLite-backed, gzip-compressed, diff-hash-keyed (djb2). Supports exact-match cache hits and incremental re-review — only changed files get re-processed, unchanged files reuse cached results instantly.

| Property | Value |
|----------|-------|
| Storage | SQLite with WAL mode |
| Compression | gzip (flate2) |
| Hash | djb2, base-36 encoded (matches Otto) |
| Invalidation | Per-file diff hash comparison |
| TTL | Configurable (default 7 days) |
| Max entries | Configurable per project (default 500) |
| Cleanup | Hourly background eviction task |

### Sandbox Auto-Fix

When a review comment includes a code suggestion, Botto can automatically apply and verify the fix in an isolated Docker container:

1. Detects the correct base image from `.otto.json` → `Dockerfile` → language heuristics (20 languages supported)
2. Creates an isolated Docker container with configurable resource limits
3. Clones the repo and checks out the source branch
4. Runs an AI-driven project setup loop (figures out deps, build tools, etc.)
5. Pre-validates the target file and code snippet exist
6. Applies the fix via generated script
7. Runs an AI-driven test-fix loop (timeout-bounded, not iteration-capped)
8. Commits and pushes on success (with GitLab API fallback for fork-based MRs)
9. Posts a reply to the original review discussion thread on GitLab

#### Language Detection

The sandbox auto-detects language and version from project files:

| Language | Version Source | Default Image |
|----------|---------------|---------------|
| Go | `go.mod` | `golang:{version}` |
| Node.js | `.node-version`, `.nvmrc`, `package.json` | `node:{version}-slim` |
| Python | `.python-version`, `pyproject.toml` | `python:{version}-slim` |
| Rust | `rust-toolchain.toml`, `Cargo.toml` | `rust:{version}-slim` |
| Java | `pom.xml`, `build.gradle` | `eclipse-temurin:{version}` |
| Ruby | `.ruby-version`, `Gemfile` | `ruby:{version}-slim` |
| PHP | `composer.json` | `php:{version}-cli` |
| C# | `*.csproj` | `mcr.microsoft.com/dotnet/sdk:{version}` |
| Swift | `Package.swift` | `swift:{version}` |
| Kotlin | `build.gradle.kts` | `eclipse-temurin:{version}` |
| Elixir | `mix.exs` | `elixir:{version}` |
| Scala | `build.sbt` | `sbtscala/scala-sbt` |
| Dart | `pubspec.yaml` | `dart:{version}` |
| Perl | `cpanfile` | `perl:{version}` |
| Lua | `*.rockspec` | `nickblah/lua:{version}` |
| R | `DESCRIPTION` | `r-base:{version}` |
| Haskell | `stack.yaml`, `*.cabal` | `haskell:{version}` |
| Clojure | `deps.edn`, `project.clj` | `clojure:{version}` |
| C/C++ | `CMakeLists.txt`, `Makefile` | `gcc:{version}` |
| Objective-C | `*.xcodeproj` | `swift:latest` |

Shared package cache volumes are mounted across containers to avoid re-downloading dependencies. Resource hints (CPU, memory, disk) are inferred per language.

You can also pin the image explicitly in `.otto.json`:

```json
{
  "sandbox": {
    "image": "node:22-slim"
  }
}
```

### Self-Evolving Prompt Harness

A CLI subcommand that autonomously improves sandbox fix prompts through an evolution loop:

```
botto harness run
```

The harness operates in rounds:

1. Discovers test cases from real GitLab MRs
2. An AI judge generates prompt mutations from the current best variant
3. Each variant runs against all test cases in parallel Docker containers
4. A grader scores results on a weighted rubric:

| Criterion | Weight | Description |
|-----------|--------|-------------|
| Pass/Fail | 50% | Did the fix pass tests? |
| Iteration efficiency | 25% | Fewer fix-test loops = better |
| Time | 15% | Faster completion = better |
| Token usage | 10% | Lower token consumption = better |

5. The judge analyzes results and extracts learnings
6. Best variant becomes the new baseline for the next round
7. Everything persists to the `harness/` directory

```
harness/
├── summary.md              # Overall harness status
├── prompts/
│   ├── v000.toml           # Baseline prompt variant
│   ├── v001.toml           # Mutation 1
│   └── v002.toml           # Mutation 2
├── learnings/
│   ├── round-001.md        # Insights from round 1
│   └── round-002.md        # Insights from round 2
└── test-cases/
    ├── gl-0001.toml        # Real MR test case
    └── gl-0002.toml        # Real MR test case
```

### Review Queue

Priority-scored queue with serial execution. Reviews are scored 0–100 based on:

| Factor | Signal |
|--------|--------|
| Size | File count, line count |
| Risk | Security/risk labels |
| Age | How long the MR has been open |
| Approvals | Approvals still needed |
| Draft | Draft MRs deprioritized |

Supports pause, resume, and cancel operations. Queue state is SQLite-persisted and rehydrated on restart.

### Verification Layer

Three AI-powered verification analyses run after the core review:

| Analysis | Description |
|----------|-------------|
| Adversarial Tests | Property-based tests that try to break changed code. Shows held properties, counterexamples, and generated test code |
| Inferred Contracts | Preconditions, postconditions, and invariants for changed functions with violation paths |
| Behavioral Delta | Identifies what behaviors changed, were preserved, or changed unexpectedly. Each behavior has a Given/When/Then scenario |

Trust score (0–100) is computed from weighted criteria:

| Criterion | Weight |
|-----------|--------|
| Mutation score | 40% |
| Coverage delta | 20% |
| Counterexample quality | 20% |
| Test independence | 10% |
| Non-tautological | 10% |

AI-only scores are capped at 65 — real execution (via sandbox or CI) is required to reach higher confidence.

### WebSocket Gateway

Multiplexed WebSocket protocol between Otto extensions and Botto:

- First-message auth with shared API key (10s timeout)
- Stream multiplexing via `stream_id` (avoids Chrome's 6-connection limit)
- Presence tracking (who's viewing which MR)
- Periodic pings (30s) for proxy keepalive
- Broadcast channel per connection (256 capacity, slow clients disconnected)
- Supports both Otto's camelCase nested payloads and Botto's flat snake_case

### GitLab Webhooks

Botto receives GitLab webhook events for real-time coordination:

- **MR events** — triggers cache invalidation, queue updates
- **Push events** — detects new commits on reviewed MRs; optionally auto-enqueues reviews (see `review.auto_review_on_push`)
- **Note events** — tracks discussion activity

Configure a webhook in GitLab pointing to `https://your-botto-host:7700/api/webhooks/gitlab` with a matching secret token.

### Jira Integration

Fetches ticket acceptance criteria from Jira for AC validation. Credentials are passed per-request from Otto (not stored server-side).

### Autonomous Workflow Engine

Botto includes a full autonomous workflow system that can discover work, plan execution, write code, and report results — all without human intervention unless it gets stuck.

#### Three Execution Modes

| Mode | Description | Use Case |
|------|-------------|----------|
| **Simple** | Static DAG executor. Steps run in topological order with parallel batching. | Automated pipelines with known steps |
| **Autonomous** | Session Manager drives a Planner → Generator → Evaluator loop with fresh context per agent. | Complex tasks like "fix this bug" |
| **Directive** | Standing orders that continuously discover and execute work. | "Watch this Jira board and fix easy tickets" |

#### Session Manager (Autonomous Mode)

Inspired by [Anthropic's harness design for long-running agents](https://www.anthropic.com/engineering/harness-design-long-running-apps). Three structurally separated AI agents prevent context degradation and self-evaluation bias:

```
User: "Fix the null pointer in UserService"
                    │
              ┌─────▼──────┐
              │   Planner   │  Reads context, queries Mentor, creates structured plan
              └─────┬──────┘
                    │
              ┌─────▼──────┐
              │  Generator  │  Executes one step at a time with fresh context
              └─────┬──────┘  Can request plan modifications or human help
                    │
              ┌─────▼──────┐
              │  Evaluator  │  Independently verifies output against success criteria
              └─────┬──────┘  Never sees Generator reasoning (prevents self-eval bias)
                    │
              Pass? ─── No → retry with feedback (up to N times) → escalate to human
                    │
                   Yes → next step (or complete)
```

Each agent gets fresh context per invocation — no accumulated memory. A workflow that runs for 2 hours has the same quality on step 50 as step 1.

**State persistence:** Every state transition is checkpointed to SQLite. If botto crashes mid-workflow, sessions resume from the last checkpoint on restart.

**Human-in-the-loop escalation:** When an agent hits a blocker (missing permissions, ambiguous requirement, repeated failures), the workflow pauses and notifies the user via WebSocket + configured webhooks. The user responds through the admin dashboard, GitLab comment, or Slack message. The workflow resumes autonomously.

#### Directives (Standing Orders)

A directive is a standing order that continuously discovers and executes work:

```
User: "Watch the PROJ Jira board, pick up tickets labeled 'easy', and fix them"
                    │
              ┌─────▼──────┐
              │  Directive  │  Parses intent, sources, constraints from NL
              │   Parser    │
              └─────┬──────┘
                    │
              ┌─────▼──────┐
              │  Directive  │  Polls sources every N minutes
              │   Runner    │  Discovers work items
              │   (loop)    │  Triages via AI (accept/reject with reasoning)
              └─────┬──────┘
                    │ spawns per-item
              ┌─────▼──────┐
              │  Session    │  Full Planner → Generator → Evaluator pipeline
              │  Manager    │  per accepted work item
              └────────────┘
```

- **Work discovery:** `WorkDiscoverer` trait with built-in implementations for GitLab (native client) and any HTTP API (via Connector Registry)
- **AI triage:** `WorkTriager` trait evaluates each item against the directive's intent
- **Priority-based resource sharing:** Multiple directives share botto's semaphores. Higher priority gets resources first.
- **Directive-level escalation:** If no work is found after N polls, or failure rate is high, the directive escalates to the user

#### Agent Types

| Type | Description |
|------|-------------|
| `gitlab` | GitLab API operations (list MRs, post comments, check pipelines) |
| `ai` | AI-powered analysis, summarization, decision-making |
| `http` | External API calls (Slack, Jira, webhooks) — SSRF-protected |
| `script` | Shell commands with restricted shells and env var filtering |
| `sandbox` | Docker-isolated code execution with resource limits |
| `coding` | Multi-turn AI coding loop (clone → understand → fix → test → iterate → push) |
| `composite` | Nested sub-workflows with recursion depth limits |

#### Mentor — Institutional Memory

A queryable knowledge store that gets smarter with every workflow run:

- **Execution patterns:** "GitLab rate-limits at 300 req/min — batch calls"
- **Domain knowledge:** "service-auth uses JWT stored in Redis"
- **Workflow learnings:** "step 3 fails 40% of the time — adding a health check wait fixes it"
- **User corrections:** Explicit "remember this" entries from agents or users

Scoped per-repo with explicit cross-project linking for microservices. Uses SQLite FTS5 for semantic search. Entries that are never queried decay in confidence and get auto-pruned.

```toml
[mentor]
enabled = true
prune_below_confidence = 0.1
prune_interval_secs = 86400
linked_repos = [
    { name = "payments", repos = ["service-auth", "service-users", "service-billing"] }
]
```

#### Connector Registry

When an agent needs a capability that doesn't exist (e.g., Jira API), it can build an HTTP connector, test it, and store it in Mentor for reuse:

1. Agent needs Jira access → checks Mentor for existing connector
2. Not found → AI generates a connector spec (URL patterns, auth, response mapping)
3. Validates the spec → stores in Mentor
4. Next time any workflow needs Jira → finds and reuses the connector

Auth tokens are never stored in connectors — only env var names. Connectors that fail repeatedly get confidence-decayed and pruned.

### Channel Adapters

Botto can receive commands from and send results to external platforms through a Ports & Adapters pattern:

```
GitLab comments  →  GitLabInputAdapter   →
Slack messages   →  SlackInputAdapter    →  MessageBus  →  Router  →  Core
Admin UI         →  AdminInputAdapter    →

GitLab comments  ←  GitLabOutputAdapter  ←
Slack messages   ←  SlackOutputAdapter   ←  MessageBus  ←  Core
Admin dashboard  ←  AdminOutputAdapter   ←
```

#### GitLab Bot

Users invoke botto from GitLab issue or MR comments:

| Command | Action |
|---------|--------|
| `@botto fix this` / `/botto fix` | Creates a session to fix the issue |
| `@botto review` / `/botto review` | Triggers a code review |
| `@botto create directive: <desc>` | Creates a standing order |
| `@botto status` | Reports current session status |
| `@botto help` | Lists available commands |
| `@botto <anything>` | AI interprets the intent |

Results are posted back to the same thread — progress updates, code fixes (as MRs), escalation questions, and completion summaries.

#### Slack Integration

Same command set via `@botto` mentions or `/botto` slash commands. Escalation options render as interactive Slack buttons. All replies go to the originating thread.

#### Channel Configuration

```toml
[channels]
enabled = true

[channels.gitlab]
enabled = true
allowed_users = []              # Empty = all users with project access
blocked_users = ["bot-account"]
allow_directives = true
allow_workflows = true
allow_review = true
allow_fix = true
max_requests_per_user_per_hour = 20
allowed_projects = []           # Empty = all accessible projects

[channels.slack]
enabled = false
bot_token_env = "BOTTO_SLACK_BOT_TOKEN"
signing_secret_env = "BOTTO_SLACK_SIGNING_SECRET"
allowed_channels = []
max_requests_per_user_per_hour = 20

[channels.output]
gitlab_post_comments = true
gitlab_create_mrs = true
slack_post_messages = true
```

Every inbound and outbound message is audit-logged to SQLite with full provenance (who, where, what thread, what action).

### Admin Dashboards

Three admin pages at `/admin`, `/admin/workflows`, and `/admin/directives`:

**Workflows Dashboard** (`/admin/workflows`):
- Active sessions with live step progress
- Waiting for human — escalated sessions with respond form
- Recent runs with drill-down to step-by-step results
- Workflow definitions with enable/disable toggle

**Directives Dashboard** (`/admin/directives`):
- Active directives with stats (sessions active/completed/failed)
- Work item feed per directive (discovered, accepted, rejected, in-progress)
- Create directive from NL description
- Pause/resume/retire controls
- Escalation response for blocked directives

## Quick Start

### Build from Source

```bash
cargo build --release
```

### Run

```bash
BOTTO_API_KEY=your-team-secret \
BOTTO_GITLAB_TOKEN=glpat-... \
BOTTO_GITLAB_URL=https://gitlab.com \
BOTTO_AI_URL=https://openrouter.ai/api/v1 \
BOTTO_AI_KEY=sk-... \
./target/release/botto
```

Botto auto-creates `./data/botto.db` on first run.

### Run with Docker

```bash
cp botto.example.toml data/botto.toml
# Edit data/botto.toml with your credentials
docker compose up -d
```

The Docker setup mounts the Docker socket so the sandbox can create sibling containers for auto-fix.

## Configuration

All config is optional — Botto auto-detects what it can. Priority: CLI flags > env vars > `botto.toml` > auto-detected defaults.

### Required

| Env Var | Config Key | Description |
|---------|-----------|-------------|
| `BOTTO_API_KEY` | `auth.api_key` | Shared secret for Otto authentication. Generate with `openssl rand -hex 32` |
| `BOTTO_GITLAB_TOKEN` | `gitlab.bot_token` | Bot PAT — needs `read_api` + `write_repository` (for sandbox push) |
| `BOTTO_GITLAB_URL` | `gitlab.url` | GitLab instance URL |
| `BOTTO_AI_KEY` | `ai.api_key` | OpenAI-compatible API key |
| `BOTTO_AI_URL` | `ai.base_url` | AI endpoint URL |

### Optional

| Env Var | Config Key | Description |
|---------|-----------|-------------|
| `BOTTO_WEBHOOK_SECRET` | `gitlab.webhook_secret` | GitLab webhook validation token |

### Review Configuration

```toml
[review]
auto_review_on_push = false  # default: off
```

When enabled, Botto automatically enqueues a review whenever new commits are pushed to a branch with an open MR. Draft MRs are skipped. By the time a human reviewer opens the MR, the full AI review is already cached and waiting. Connected Otto extensions are notified immediately so the UI can show review-in-progress status.

### Per-Task Model Configuration

Each AI task can use a different model. Defaults:

| Task | Default Model | Default Temp |
|------|--------------|-------------|
| Summary | claude-sonnet-4-5 | 0.3 |
| Code Review | claude-sonnet-4-5 | 0.2 |
| Edge Cases | claude-sonnet-4-5 | 0.3 |
| Related Files | claude-haiku-4-5 | 0.1 |
| Follow-Up | claude-sonnet-4-5 | 0.3 |
| Chat | claude-sonnet-4-5 | 0.4 |
| AC Validation | claude-sonnet-4-5 | 0.2 |
| Adversarial Tests | claude-sonnet-4-5 | 0.3 |
| Contracts | claude-sonnet-4-5 | 0.2 |
| Behavioral Delta | claude-sonnet-4-5 | 0.3 |
| Sandbox Fix | claude-opus-4-6 | — |
| Harness Judge | claude-opus-4-6 | — |

Override in `botto.toml`:

```toml
[ai.models]
summary = "gpt-4o"
code_review = "claude-sonnet-4-5"
```

### Sandbox Configuration

```toml
[sandbox]
enabled = true              # auto-detected from Docker availability
max_concurrent = 2          # auto-detected from CPU cores
timeout_seconds = 300
max_memory_mb = 2048        # auto-detected from system memory
max_disk_mb = 4096
fix_branch_mode = "same_branch"  # "same_branch" or "new_branch"
```

When `fix_branch_mode` is set to `"new_branch"`, Botto creates a dedicated branch (e.g., `botto/fix/mr-42-add-auth-abc123`) and opens a merge request targeting the original source branch, instead of pushing directly to the MR branch.

### Cache Configuration

```toml
[cache]
review_ttl_days = 7
max_cached_reviews = 500    # per project
```

### Harness Configuration

```toml
[harness]
enabled = false
max_rounds = 10             # evolution rounds per run
variants_per_round = 4      # prompt variants tested per round
concurrency = 3             # parallel sandbox instances
test_cases = 5              # test cases per variant
gitlab_seed_orgs = ["gitlab-org"]
memory_dir = "harness"      # directory for prompts, learnings, test cases
judge_model = "claude-opus-4-6"
```

See [botto.example.toml](botto.example.toml) for the full annotated config template.

### Workflow & Directive Configuration

```toml
[workflows]
enabled = true                  # Enable the autonomous workflow engine
max_concurrent_runs = 3         # Max simultaneous workflow sessions
default_step_timeout_secs = 300 # Per-step timeout

[mentor]
enabled = true                  # Enable institutional memory
prune_below_confidence = 0.1    # Auto-prune stale knowledge
prune_interval_secs = 86400     # Prune check interval (daily)
linked_repos = [                # Cross-project knowledge sharing
    { name = "payments", repos = ["service-auth", "service-users", "service-billing"] }
]
```

### Admin Settings Page

Botto includes an embedded web UI for managing configuration at runtime. Access it at:

```
http://your-botto-host:7700/admin?key=YOUR_API_KEY
```

The page is protected by the same API key used for Otto WebSocket auth. In dev mode (empty API key), no key is needed.

Settings are organized into collapsible sections: Server, Authentication, GitLab, AI, AI Models, Sandbox, Cache, and Harness. Changes are hot-swapped in memory and persisted to `botto.toml` immediately. Fields that require a server restart (host, port, concurrency limits) are marked with a badge.

The admin API is also available programmatically:

| Route | Method | Description |
|-------|--------|-------------|
| `/api/admin/config` | GET | Current config (secrets redacted) |
| `/api/admin/config` | PUT | Update config (partial, hot-swap + persist) |
| `/api/admin/status` | GET | Live server status (connections, reviews, Docker) |

Secrets (API keys, tokens) are never returned in full — the GET response shows masked values like `••••xxxx`. Sending a masked value back in a PUT preserves the existing secret.

## Connecting Otto

1. Open Otto settings (extension options page)
2. Scroll to "Botto Server"
3. Enter the server URL: `wss://your-botto-host:7700/ws`
4. Enter the team API key
5. Click Test, then Save

Or use auto-discovery: if Botto is accessible at the same domain as your GitLab instance, Otto finds it automatically via `/.well-known/botto`.

## Integration Setup Guides

Botto works out of the box as a shared review server for Otto extensions. The integrations below are optional — enable them to let users interact with Botto directly from GitLab comments and Slack messages.

### Step 0: Base Botto Setup (Required)

Before setting up any integration, make sure Botto is running with the core config:

```bash
# 1. Build
cargo build --release

# 2. Set required environment variables
export BOTTO_API_KEY=$(openssl rand -hex 32)
export BOTTO_GITLAB_TOKEN=glpat-xxxxxxxxxxxxxxxxxxxx  # Bot PAT with read_api + write_repository
export BOTTO_GITLAB_URL=https://gitlab.com             # Your GitLab instance
export BOTTO_AI_URL=https://openrouter.ai/api/v1       # Any OpenAI-compatible endpoint
export BOTTO_AI_KEY=sk-...                              # AI API key

# 3. Run
./target/release/botto
```

Verify it's running:
```bash
curl http://localhost:7700/health
# → {"status":"ok"}
```

Open the admin dashboard at `http://localhost:7700/admin?key=YOUR_API_KEY` to confirm everything is connected.

### Step 1: Enable the Workflow Engine

The workflow engine powers autonomous sessions, directives, and the GitLab/Slack bot commands. Enable it in `botto.toml`:

```toml
[workflows]
enabled = true
max_concurrent_runs = 3
default_step_timeout_secs = 300

[mentor]
enabled = true
```

Or via environment variables — there are no env var shortcuts for these, so use `botto.toml`.

Restart Botto. You should see in the logs:
```
INFO botto: workflow scheduler started
INFO botto: mentor pruner started
INFO botto: directive runner started
```

Verify at `http://localhost:7700/admin/workflows?key=YOUR_API_KEY` — the workflow dashboard should load.

### Step 2: Enable the Channel Adapter Layer

The channel layer is the master switch for GitLab bot and Slack bot functionality:

```toml
[channels]
enabled = true
```

Restart Botto. You should see:
```
INFO botto: channel router started
INFO botto: channel adapters started
```

### Step 3: GitLab Bot Setup

The GitLab bot lets users invoke Botto from issue and MR comments using `@botto` mentions or `/botto` commands.

#### 3a. Create a GitLab Bot User (Recommended)

While you can use any user's PAT, a dedicated bot user keeps things clean:

1. Create a new GitLab user (e.g., `botto-bot`) — or use a group bot if on GitLab Premium+
2. Add the bot user to your projects/groups with **Developer** role (needs comment + push access)
3. Create a Personal Access Token for the bot with scopes:
   - `read_api` — read MRs, issues, pipelines
   - `write_repository` — push fix commits, create branches
   - `api` — post comments, create MRs (if using `fix_branch_mode = "new_branch"`)
4. Set the token as `BOTTO_GITLAB_TOKEN`

The bot user's username is what users will `@mention` in comments. If the user is named `botto-bot`, users type `@botto-bot fix this`.

#### 3b. Configure the GitLab Webhook

Botto needs to receive GitLab webhook events to detect `@botto` commands in comments:

1. Go to your GitLab project (or group) → Settings → Webhooks
2. Add a new webhook:
   - **URL:** `https://your-botto-host:7700/api/webhooks/gitlab`
   - **Secret token:** Set a secret and add it to your config:
     ```bash
     export BOTTO_WEBHOOK_SECRET=your-webhook-secret
     ```
   - **Trigger events:** Check these:
     - **Push events** — for auto-review on push
     - **Merge request events** — for cache invalidation, queue updates
     - **Comments (Note events)** — for `@botto` command detection
   - **SSL verification:** Enable if using HTTPS (recommended)
3. Click "Add webhook"
4. Test it — click "Test" → "Note events". Check Botto logs for:
   ```
   INFO botto::api::webhooks: received gitlab webhook event_type="note"
   ```

#### 3c. Configure GitLab Channel Settings

In `botto.toml`:

```toml
[channels.gitlab]
enabled = true

# Who can use @botto commands (empty = everyone with project access)
allowed_users = []

# Block specific users (e.g., other bots to prevent loops)
blocked_users = []

# What actions are allowed via GitLab comments
allow_directives = true    # @botto create directive: ...
allow_workflows = true     # @botto trigger workflow ...
allow_review = true        # @botto review
allow_fix = true           # @botto fix this

# Rate limiting
max_requests_per_user_per_hour = 20

# Restrict to specific projects (empty = all accessible)
allowed_projects = []

[channels.output]
gitlab_post_comments = true    # Botto posts results as comments
gitlab_create_mrs = true       # Botto can create MRs for fixes
```

#### 3d. Test the GitLab Bot

1. Go to any issue or MR in a project where the webhook is configured
2. Post a comment: `@botto-bot help`
3. Botto should reply with a comment listing available commands
4. Try: `@botto-bot review` on an MR to trigger a code review
5. Try: `@botto-bot fix this null pointer` on an issue to trigger an autonomous fix session

If Botto doesn't respond, check:
- Is `channels.enabled = true` in config?
- Is `channels.gitlab.enabled = true`?
- Is the webhook configured with Note events?
- Is the bot user a member of the project?
- Check Botto logs for errors

### Step 4: Slack Bot Setup

The Slack bot lets users invoke Botto from Slack channels and DMs.

#### 4a. Create a Slack App

1. Go to [api.slack.com/apps](https://api.slack.com/apps) → "Create New App"
2. Choose "From scratch"
3. Name it "Botto" (or whatever you prefer), select your workspace
4. Click "Create App"

#### 4b. Configure Bot Scopes

1. In the app settings, go to **OAuth & Permissions**
2. Under "Bot Token Scopes", add:
   - `app_mentions:read` — detect @Botto mentions
   - `chat:write` — post messages and replies
   - `im:history` — read DM messages
   - `im:read` — access DM channels
   - `channels:history` — read channel messages (for mentions)
3. Click "Install to Workspace" and authorize
4. Copy the **Bot User OAuth Token** (starts with `xoxb-`)

#### 4c. Configure Event Subscriptions

1. Go to **Event Subscriptions** → toggle "Enable Events" ON
2. Set the **Request URL** to: `https://your-botto-host:7700/api/webhooks/slack/events`
   - Slack will send a verification challenge — Botto handles this automatically
   - The URL must be HTTPS and publicly accessible
3. Under "Subscribe to bot events", add:
   - `app_mention` — when someone @mentions the bot
   - `message.im` — when someone DMs the bot
4. Click "Save Changes"

#### 4d. Configure Interactive Components (for Escalation Buttons)

1. Go to **Interactivity & Shortcuts** → toggle "Interactivity" ON
2. Set the **Request URL** to: `https://your-botto-host:7700/api/webhooks/slack/interactions`
3. Click "Save Changes"

This enables the interactive buttons that appear when Botto escalates a question to the user (e.g., "Provide credentials" / "Skip" / "Cancel").

#### 4e. Configure Slash Commands (Optional)

1. Go to **Slash Commands** → "Create New Command"
2. Command: `/botto`
3. Request URL: `https://your-botto-host:7700/api/webhooks/slack/events`
4. Description: "Invoke Botto — autonomous AI assistant"
5. Usage hint: `fix | review | create directive: ... | status | help`
6. Click "Save"

#### 4f. Get the Signing Secret

1. Go to **Basic Information** → "App Credentials"
2. Copy the **Signing Secret**

#### 4g. Configure Botto for Slack

Set the environment variables:

```bash
export BOTTO_SLACK_BOT_TOKEN=xoxb-your-bot-token
export BOTTO_SLACK_SIGNING_SECRET=your-signing-secret
```

And in `botto.toml`:

```toml
[channels.slack]
enabled = true
bot_token_env = "BOTTO_SLACK_BOT_TOKEN"
signing_secret_env = "BOTTO_SLACK_SIGNING_SECRET"

# Which channels Botto listens in (empty = all channels it's invited to)
allowed_channels = []

# What actions are allowed
allow_directives = true
allow_workflows = true

# Rate limiting
max_requests_per_user_per_hour = 20

[channels.output]
slack_post_messages = true
```

Restart Botto. You should see:
```
INFO botto: slack output listener started
```

#### 4h. Invite the Bot to Channels

1. In Slack, go to the channel where you want Botto available
2. Type `/invite @Botto` (or whatever you named the app)
3. The bot is now listening in that channel

#### 4i. Test the Slack Bot

1. In a channel where Botto is invited, type: `@Botto help`
2. Botto should reply in a thread with available commands
3. Try: `@Botto create directive: watch the PROJ Jira board and fix easy tickets`
4. Try DM: send "status" directly to the Botto app

If Botto doesn't respond, check:
- Is `channels.slack.enabled = true`?
- Are the env vars set (`BOTTO_SLACK_BOT_TOKEN`, `BOTTO_SLACK_SIGNING_SECRET`)?
- Is the Event Subscriptions URL verified (green checkmark in Slack app settings)?
- Is the bot invited to the channel?
- Check Botto logs for `slack_input` entries

### Step 5: Setting Up Directives (Optional)

Once the GitLab or Slack bot is working, you can create standing orders:

**From GitLab:**
```
@botto-bot create directive: Watch the PROJ Jira board, pick up tickets labeled 'easy', and fix them
```

**From Slack:**
```
@Botto create directive: Review all open MRs in group/project every morning
```

**From the Admin Dashboard:**
1. Go to `http://localhost:7700/admin/directives?key=YOUR_API_KEY`
2. Enter a description in the "Create Directive" form
3. Click "Create"

**From the API:**
```bash
curl -X POST http://localhost:7700/api/directives \
  -H "Content-Type: application/json" \
  -d '{"description": "Watch PROJ Jira board, fix easy tickets"}'
```

The directive will start polling immediately. Monitor progress at `/admin/directives`.

### Integration Summary

| Feature | Requires | Config |
|---------|----------|--------|
| Otto extension reviews | Base setup | — |
| GitLab bot commands | Webhook + channel layer | `channels.gitlab.enabled = true` |
| Slack bot commands | Slack app + channel layer | `channels.slack.enabled = true` |
| Autonomous workflows | Workflow engine | `workflows.enabled = true` |
| Standing directives | Workflow engine + channel layer | Both enabled |
| Mentor knowledge | Mentor | `mentor.enabled = true` |
| Sandbox auto-fix | Docker | `sandbox.enabled = true` |
| Admin dashboards | Base setup | `/admin`, `/admin/workflows`, `/admin/directives` |

## Endpoints

| Route | Method | Description |
|-------|--------|-------------|
| `/ws` | WS | Otto WebSocket connections (primary communication) |
| `/health` | GET | Liveness probe (always 200) |
| `/ready` | GET | Readiness probe (DB, AI, GitLab, queue, sandbox status) |
| `/api/webhooks/gitlab` | POST | GitLab webhook receiver (MR, push, note events + @botto commands) |
| `/api/webhooks/slack/events` | POST | Slack Events API receiver |
| `/api/webhooks/slack/interactions` | POST | Slack interactive components (button clicks) |
| `/.well-known/botto` | GET | Auto-discovery for Otto extensions |
| `/admin` | GET | Admin settings page |
| `/admin/workflows` | GET | Workflow dashboard (sessions, escalations, definitions) |
| `/admin/directives` | GET | Directives dashboard (standing orders, work items) |
| `/api/admin/config` | GET/PUT | Config management (hot-swap) |
| `/api/admin/status` | GET | Live server status |
| `/api/workflows` | GET/POST | List or create workflows (NL → DAG decomposition) |
| `/api/workflows/{id}` | GET/PUT/DELETE | Manage a workflow definition |
| `/api/workflows/{id}/run` | POST | Manually trigger a workflow |
| `/api/workflows/{id}/runs` | GET | List runs for a workflow |
| `/api/workflows/runs/{id}` | GET | Get run status + step states |
| `/api/workflows/sessions/waiting` | GET | Sessions waiting for human input |
| `/api/workflows/sessions/active` | GET | Currently running sessions |
| `/api/workflows/sessions/recent` | GET | Recently completed sessions |
| `/api/workflows/sessions/{id}` | GET | Get session state |
| `/api/workflows/sessions/{id}/respond` | POST | Respond to an escalation |
| `/api/workflows/sessions/{id}/messages` | GET | Session conversation thread |
| `/api/mentor/query` | POST | Query the Mentor knowledge store |
| `/api/mentor/feedback` | POST | Mark a Mentor entry as helpful/unhelpful |
| `/api/directives` | GET/POST | List or create directives (NL → standing order) |
| `/api/directives/{id}` | GET/PUT/DELETE | Manage a directive |
| `/api/directives/{id}/items` | GET | Work item feed for a directive |

## Architecture

```
External Channels ←→ Channel Adapters ←→ MessageBus ←→ Router
                                                         │
Otto extensions ←→ WebSocket ←→ Botto server             │
                                    │                    │
                                    ├── Core Services ◄──┘
                                    │   ├── Directive Runner (standing orders, work discovery)
                                    │   ├── Session Manager (Planner → Generator → Evaluator)
                                    │   ├── Workflow Orchestrator (simple DAG execution)
                                    │   ├── Review Orchestrator (3-phase parallel AI pipeline)
                                    │   └── Escalation Protocol (pause, notify, resume)
                                    │
                                    ├── Agents
                                    │   ├── GitLab, AI, HTTP, Script, Sandbox, Coding, Composite
                                    │   ├── Connector Registry (self-building HTTP integrations)
                                    │   └── Agent Factory (creates agents by type)
                                    │
                                    ├── Knowledge & State
                                    │   ├── Mentor (institutional memory, FTS5 search)
                                    │   ├── SQLite (11 migrations, WAL mode, checkpointing)
                                    │   └── Event Bus (cross-component broadcast)
                                    │
                                    ├── Infrastructure
                                    │   ├── GitLab client (REST v4, 20+ endpoints)
                                    │   ├── AI client (OpenAI-compatible, SSE streaming)
                                    │   ├── Sandbox manager (Docker, warm pools, 20 languages)
                                    │   ├── Review queue (priority scoring, serial execution)
                                    │   └── Prompt harness (self-evolving, AI judge)
                                    │
                                    └── Integrations
                                        ├── GitLab webhooks + bot comments
                                        ├── Slack events + interactive buttons
                                        ├── Jira ticket fetching
                                        └── Conflict Radar + Cross-MR Clusters
```

### Database

SQLite with WAL mode, 64MB cache, in-memory temp store. 11 migrations.

| Table | Purpose |
|-------|---------|
| `connections` | Ephemeral presence tracking (cleared on startup) |
| `review_cache` | Gzip-compressed cached reviews with TTL |
| `comment_actions` | User accept/dismiss/edit actions on review comments |
| `team_settings` | Per-project shared triage toggle |
| `review_queue` | Priority-scored review queue |
| `reviewer_prefs` | Learned team accept/dismiss patterns |
| `sandbox_jobs` | Docker fix job tracking |
| `events` | Event log |
| `digests` | Cached team activity digests with TTL |
| `mr_changed_files` | File index for Conflict Radar and Cross-MR Clusters |
| `mr_clusters` | Cross-MR cluster detection results |
| `setup_recipes` | Cached sandbox setup commands per project |
| `project_knowledge` | AI-distilled project facts for sandbox context |
| `repo_configs` | Cached `.otto.json` per-repo configs |
| `mentor_entries` | Mentor knowledge store with FTS5 full-text search |
| `mentor_fts` | FTS5 virtual table (external content mode) |
| `mentor_repo_links` | Explicit cross-project repo linking |
| `workflows` | Workflow definitions (JSON DAG + metadata) |
| `workflow_runs` | v1 workflow run instances with step states |
| `workflow_run_log` | Step-level event log for workflow runs |
| `workflow_sessions` | v2 session state (plan, outputs, escalation, checkpoints) |
| `session_messages` | Human conversation thread per session |
| `directives` | Standing orders (intent, sources, constraints, priority) |
| `directive_work_items` | Discovered work items with triage status |
| `channel_messages` | Audit log for all inbound/outbound channel messages |
| `channel_rate_limits` | Per-user rate limiting for channel commands |

### WebSocket Protocol

Inbound (Otto → Botto):

| Message | Description |
|---------|-------------|
| `AUTH` | First message, shared API key |
| `VIEWING_MR` | User opened an MR page |
| `LEFT_MR` | User left an MR page |
| `REQUEST` | Request/response operation |
| `STREAM_START` | Start a streaming operation (review, chat, inquiry) |
| `STREAM_CANCEL` | Cancel an in-flight stream |
| `COMMENT_ACTION` | Accept/dismiss/edit a review comment |
| `REQUEST_FIX` | Trigger sandbox auto-fix |
| `VIEWING_FILES` | File-level presence (which files a user has open) |

Outbound (Botto → Otto):

| Message | Description |
|---------|-------------|
| `AUTH_OK` / `AUTH_ERROR` | Authentication result |
| `RESPONSE` | Request/response result |
| `STREAM_CHUNK` | Streaming review/chat delta |
| `STREAM_END` | Stream completed |
| `ERROR` | Error notification |
| `COMMENT_ACTION_BROADCAST` | Action synced to all viewers |
| `FIX_PROGRESS` / `FIX_COMPLETE` | Sandbox fix status updates |
| `CACHED_REVIEW` | Cached review delivered |
| `COMMENT_ACTIONS_SYNC` | Bulk action sync for late-joiners |
| `EVENT_NOTIFICATION` | General event broadcast |
| `PRESENCE_SNAPSHOT` | Initial presence state on MR join |
| `PRESENCE_UPDATE` | File-level presence delta |
| `CONFLICT_UPDATED` | Conflict radar results pushed on MR join |
| `CLUSTER_UPDATED` | Cluster data pushed on MR join |

Request types (via `REQUEST` message):

| Type | Description |
|------|-------------|
| `GET_SETTINGS` | Server capabilities |
| `GET_SERVER_STATUS` | Lightweight server health and stats |
| `GET_SERVER_CONFIG` | Full config (secrets redacted) |
| `UPDATE_SERVER_CONFIG` | Hot-swap config update |
| `PING` | Latency measurement (returns server timestamp) |
| `TEST_GITLAB_CONNECTION` | Verify GitLab bot token |
| `FETCH_PROJECT` | Fetch GitLab project metadata |
| `FETCH_MR_METADATA` | Fetch MR details |
| `FETCH_MR_CHANGES` | Fetch MR diff |
| `FETCH_FILE_CONTENT` | Fetch file from GitLab |
| `FETCH_FILE_TREE` | Fetch repository tree |
| `FETCH_MR_DISCUSSIONS` | Fetch MR discussions |
| `FETCH_TICKET` / `FETCH_TICKET_BATCH` | Fetch Jira tickets |
| `GET_CACHED_REVIEW` | Get cached review by diff hash |
| `GET_LATEST_CACHED_REVIEW` | Get latest cached review (no diff hash needed) |
| `GET_REVIEW_HISTORY` | List cached reviews for a project |
| `INVALIDATE_REVIEW_CACHE` | Delete cached reviews for an MR |
| `GET_COMMENT_ACTIONS` | Get comment actions for an MR |
| `GET_TEAM_SETTINGS` / `SET_TEAM_SETTINGS` | Shared triage toggle |
| `GET_TEAM_DIGEST` | Team activity digest (daily/weekly) |
| `GET_QUEUE_STATUS` | Review queue state |
| `ENQUEUE_REVIEW` / `PAUSE_REVIEW` / `RESUME_REVIEW` / `CANCEL_REVIEW` | Queue operations |
| `GET_SANDBOX_JOB` | Get a specific sandbox job |
| `GET_SANDBOX_JOBS` | List sandbox jobs for an MR |
| `GET_WARM_POOL_STATUS` | Warm container pool details |
| `GET_CONFLICTS` | Conflict radar results for an MR |
| `GET_CLUSTER` | Cross-MR cluster data for an MR |
| `GET_PRESENCE` | Who's viewing a specific MR |
| `GET_ACTIVE_REVIEWS` | Currently in-flight reviews |
| `GET_CONNECTED_USERS` | All connected users and their MRs |
| `GET_REPO_CONFIG` / `INVALIDATE_REPO_CONFIG` | Per-repo `.otto.json` config |
| `GET_REVIEWER_PREFS` | Learned team review preferences |
| `GET_FILE_INDEX_STATUS` | File index population status |
| `GET_EVENTS` | Recent activity events for a project/MR |
| `BATCH_PRESENCE` | Viewer counts for multiple MRs at once |
| `GET_WORKFLOW_RUNS` | Recent workflow execution history |
| `GET_WORKFLOWS` | List configured workflows |

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Language | Rust (edition 2024) |
| Async runtime | Tokio (full features) |
| HTTP server | Axum 0.8 + WebSocket |
| HTTP client | reqwest 0.12 (JSON, streaming, gzip) |
| Database | SQLite via sqlx 0.8 (WAL mode) |
| Docker API | bollard 0.18 |
| Serialization | serde + serde_json + toml |
| Logging | tracing + tracing-subscriber (env-filter, JSON) |
| CLI | clap 4 (derive) |
| Error handling | anyhow + thiserror |
| Concurrency | DashMap, tokio broadcast/watch/mpsc, Semaphore, arc-swap |
| Compression | flate2 (gzip) |
| SSE streaming | reqwest-eventsource + hand-rolled SSE parser |
| System info | sysinfo 0.33 |

## Project Structure

```
src/
├── main.rs                         # CLI (clap), server startup, background tasks
├── lib.rs                          # Library root, re-exports all modules
├── config.rs                       # Config loading: TOML + env vars + auto-detection
├── server.rs                       # Axum HTTP + WebSocket server, graceful shutdown
├── api/
│   ├── ws.rs                       # WebSocket gateway (auth, multiplexing, presence)
│   ├── health.rs                   # /health and /ready endpoints
│   ├── webhooks.rs                 # GitLab webhook receiver + @botto command detection
│   ├── discovery.rs                # /.well-known/botto auto-discovery
│   ├── admin.rs                    # Admin settings + workflow/directive dashboards
│   ├── workflows.rs                # Workflow + session REST API (CRUD, trigger, respond)
│   └── directives.rs               # Directive REST API (CRUD, work items)
├── router/
│   ├── mod.rs                      # Message dispatch (request/response + streaming)
│   └── handlers.rs                 # 20+ request handlers
├── db/
│   ├── mod.rs                      # SQLite init, WAL mode, 11 embedded migrations
│   └── queries.rs                  # Typed query wrappers
├── types/
│   ├── messages.rs                 # Wire protocol types
│   ├── review.rs                   # Core review types (MrContext, FileReview, etc.)
│   ├── verification.rs             # Adversarial tests, contracts, trust assessment
│   ├── settings.rs                 # AiTaskType enum, per-task temperature defaults
│   ├── state.rs                    # AppState (Arc-wrapped), Connection, MessageBus
│   ├── queue.rs                    # QueuedReview, QueueItemStatus, ReviewPriority
│   ├── sandbox.rs                  # SandboxJob, SandboxJobStatus, SandboxStrategy
│   └── workflow.rs                 # Workflow, session, directive, escalation types
├── services/
│   ├── ai/
│   │   ├── client.rs               # OpenAI-compatible HTTP client (streaming SSE)
│   │   ├── service.rs              # Per-task AI orchestration
│   │   └── prompts/                # 12 prompt builder modules
│   ├── gitlab/
│   │   └── client.rs               # GitLab REST v4 client (20+ endpoints)
│   ├── review/
│   │   ├── orchestrator.rs         # 3-phase review pipeline
│   │   └── cache.rs                # SQLite + gzip cache, incremental re-review
│   ├── queue/
│   │   ├── manager.rs              # Background queue manager
│   │   └── priority.rs             # Priority scoring 0–100
│   ├── sandbox/
│   │   ├── manager.rs              # Docker container lifecycle, warm pools, AI fix loop
│   │   └── detector.rs             # Language detection, version-aware image resolution
│   ├── verification/
│   │   └── trust.rs                # Trust calibrator (weighted scoring)
│   ├── events/
│   │   └── mod.rs                  # In-process broadcast event bus
│   ├── ticket/
│   │   └── jira.rs                 # Jira REST API client
│   ├── harness/
│   │   ├── orchestrator.rs         # Self-evolving prompt engineering loop
│   │   ├── runner.rs               # Runs variants against test cases
│   │   ├── grader.rs               # Scores results (weighted rubric)
│   │   ├── judge.rs                # AI judge (mutations + analysis)
│   │   ├── memory.rs               # Filesystem persistence
│   │   └── test_case.rs            # Test case discovery from real MRs
│   ├── mentor/                     # Institutional memory
│   │   ├── client.rs               # MentorClient: query (FTS5), remember, forget
│   │   ├── linker.rs               # Cross-project repo linking
│   │   └── pruner.rs               # Background confidence decay + cleanup
│   ├── workflow/                   # Autonomous workflow engine
│   │   ├── traits.rs               # WorkflowAgent trait
│   │   ├── factory.rs              # Agent factory (creates agents by type)
│   │   ├── orchestrator.rs         # v1 DAG executor (simple mode)
│   │   ├── session.rs              # v2 Session Manager (Planner/Generator/Evaluator)
│   │   ├── planner.rs              # Planner agent: NL → structured plan
│   │   ├── generator.rs            # Generator agent: step execution, fresh context
│   │   ├── evaluator.rs            # Evaluator agent: independent verification
│   │   ├── escalation.rs           # Human-in-the-loop: pause, notify, resume
│   │   ├── connector.rs            # Connector Registry: build/store/find HTTP integrations
│   │   ├── decomposer.rs           # NL → DAG decomposition (v1 path)
│   │   ├── scheduler.rs            # Cron + event triggers, mode routing
│   │   ├── crud.rs                 # SQLite CRUD for workflows, runs, sessions, messages
│   │   ├── filter.rs               # Event trigger filter expression evaluator
│   │   ├── verification.rs         # AI-powered final verification
│   │   ├── coding.rs               # CodingAgent: wraps SandboxManager for multi-turn coding
│   │   ├── gitlab.rs               # GitLab workflow agent
│   │   ├── ai.rs                   # AI workflow agent
│   │   ├── http.rs                 # HTTP agent (SSRF-protected)
│   │   ├── script.rs               # Script agent (restricted shells)
│   │   ├── sandbox.rs              # Sandbox agent (Docker, resource limits)
│   │   └── composite.rs            # Composite agent (nested sub-workflows)
│   ├── directive/                  # Standing orders
│   │   ├── types.rs                # Directive, WorkSource, WorkItem, TriageDecision
│   │   ├── runner.rs               # Poll/discover/triage/spawn loop
│   │   ├── discoverer.rs           # WorkDiscoverer trait + ConnectorDiscoverer
│   │   ├── triager.rs              # WorkTriager trait + AiTriager
│   │   ├── parser.rs               # NL → Directive parsing
│   │   └── crud.rs                 # SQLite CRUD for directives + work items
│   └── channels/                   # External platform adapters
│       ├── types.rs                # MessageContext, InboundMessage, OutboundMessage
│       ├── bus.rs                  # MessageBus (inbound + outbound broadcast)
│       ├── config.rs               # Permission checks, rate limiting
│       ├── audit.rs                # Audit logging to SQLite
│       ├── router.rs               # Routes inbound messages to core actions
│       ├── gitlab_input.rs         # @botto command parsing from webhook comments
│       ├── gitlab_output.rs        # Post comments, create MRs
│       ├── slack_input.rs          # Slack event + interaction parsing
│       └── slack_output.rs         # Slack message posting, Block Kit buttons
└── util/
    ├── hash.rs                     # djb2 hashing (base-36, matches Otto)
    ├── json_repair.rs              # Truncated JSON repair for AI responses
    └── retry.rs                    # Exponential backoff retry
```

## Development

```bash
cargo check                         # Type check
cargo test                          # Run all tests
cargo run                           # Dev server (auto-creates ./data/)
RUST_LOG=botto=debug cargo run      # Verbose logging
```

### Running the Harness

```bash
cargo run -- harness run            # Start prompt evolution
cargo run -- harness status         # View current harness state
```

### Tests

388 tests across unit and integration suites:

```bash
cargo test                          # All tests
cargo test --test integration       # Integration tests only (42 tests)
cargo test -- --nocapture           # With stdout
```

**Integration tests** spin up real Axum server instances with in-memory SQLite and ephemeral TCP ports — WebSocket auth, health endpoints, comment actions, presence tracking, queue operations, sandbox queries, payload compatibility.

**Unit tests** cover: language detection, version parsing, image inference, priority scoring, hashing, JSON repair, URL encoding, session CRUD (create/load/checkpoint/resume), escalation (escalate/respond/cancel/replan), planner (parse/validate/cycle detection), generator (response parsing), evaluator (verdict parsing, safe truncation), connector (auth roundtrips), scheduler (cron parsing, event matching), mentor (FTS5 queries, pruning), channel adapters (command parsing, output formatting).

## Repo-Level Configuration

Teams can place a `.otto.json` in their repository root to customize behavior:

```json
{
  "context": "E-commerce platform built with Vue 3 + Node.js.",
  "focus": ["security", "error-handling", "performance"],
  "ignore": ["style", "naming"],
  "reviewTemplate": "Check for SQL injection, validate all user inputs.",
  "acceptanceCriteriaField": "customfield_10042",
  "sandbox": {
    "image": "node:22-slim"
  }
}
```

| Field | Purpose |
|-------|---------|
| `context` | Project description injected into all AI prompts |
| `focus` | Review categories to prioritize |
| `ignore` | Categories to deprioritize |
| `reviewTemplate` | Custom review checklist |
| `acceptanceCriteriaField` | Jira custom field ID for acceptance criteria |
| `sandbox.image` | Pin the Docker image for sandbox auto-fix |

## License

MIT
