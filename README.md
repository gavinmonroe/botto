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
- **Self-evolving prompts** — built-in harness that autonomously improves sandbox fix prompts through evolution loops
- **Priority queue** — reviews scored and executed in priority order, with pause/resume/cancel
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
- **Push events** — detects new commits on reviewed MRs
- **Note events** — tracks discussion activity

Configure a webhook in GitLab pointing to `https://your-botto-host:7700/api/webhooks/gitlab` with a matching secret token.

### Jira Integration

Fetches ticket acceptance criteria from Jira for AC validation. Credentials are passed per-request from Otto (not stored server-side).

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
```

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

## Connecting Otto

1. Open Otto settings (extension options page)
2. Scroll to "Botto Server"
3. Enter the server URL: `wss://your-botto-host:7700/ws`
4. Enter the team API key
5. Click Test, then Save

Or use auto-discovery: if Botto is accessible at the same domain as your GitLab instance, Otto finds it automatically via `/.well-known/botto`.

## Endpoints

| Route | Method | Description |
|-------|--------|-------------|
| `/ws` | WS | Otto WebSocket connections (primary communication) |
| `/health` | GET | Liveness probe (always 200) |
| `/ready` | GET | Readiness probe (DB + capabilities check) |
| `/api/webhooks/gitlab` | POST | GitLab webhook receiver (MR, push, note events) |
| `/.well-known/botto` | GET | Auto-discovery for Otto extensions |

## Architecture

```
Otto extensions ←→ WebSocket ←→ Botto server
                                    ├── Review orchestrator (3-phase parallel AI pipeline)
                                    ├── GitLab client (bot PAT, REST v4, 20+ endpoints)
                                    ├── AI client (OpenAI-compatible, SSE streaming)
                                    ├── SQLite cache (WAL mode, gzip, incremental re-review)
                                    ├── Event bus (presence, actions, notifications)
                                    ├── Review queue (priority scoring, serial execution)
                                    ├── Verification layer (trust calibration, contracts)
                                    ├── Sandbox manager (Docker, auto-fix, 20 languages)
                                    ├── Prompt harness (self-evolving, AI judge)
                                    └── Jira client (ticket fetching, AC validation)
```

### Database

SQLite with WAL mode, 64MB cache, in-memory temp store. 8 tables:

| Table | Purpose |
|-------|---------|
| `connections` | Ephemeral presence tracking (cleared on startup) |
| `review_cache` | Gzip-compressed cached reviews with TTL |
| `comment_actions` | User accept/dismiss/edit actions on review comments |
| `team_settings` | Per-project shared triage toggle |
| `review_queue` | Priority-scored review queue |
| `reviewer_prefs` | Reserved for future accept/dismiss learning |
| `sandbox_jobs` | Docker fix job tracking |
| `events` | Event log |

### WebSocket Protocol

Inbound (Otto → Botto):

| Message | Description |
|---------|-------------|
| `AUTH` | First message, shared API key |
| `VIEWING_MR` | User opened an MR page |
| `LEFT_MR` | User left an MR page |
| `REQUEST` | Request/response operation |
| `STREAM_START` | Start a streaming review or chat |
| `STREAM_CANCEL` | Cancel an in-flight stream |
| `COMMENT_ACTION` | Accept/dismiss/edit a review comment |
| `REQUEST_FIX` | Trigger sandbox auto-fix |

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
| Concurrency | DashMap, tokio broadcast/watch/mpsc, Semaphore |
| Compression | flate2 (gzip) |
| SSE streaming | reqwest-eventsource + hand-rolled SSE parser |
| System info | sysinfo 0.33 |

## Project Structure

```
src/
├── main.rs                         # CLI (clap), server startup, harness subcommands
├── lib.rs                          # Library root, re-exports all modules
├── config.rs                       # Config loading: TOML + env vars + auto-detection
├── server.rs                       # Axum HTTP + WebSocket server, graceful shutdown
├── api/
│   ├── ws.rs                       # WebSocket gateway (auth, multiplexing, presence)
│   ├── health.rs                   # /health and /ready endpoints
│   ├── webhooks.rs                 # GitLab webhook receiver (MR, push, note events)
│   └── discovery.rs                # /.well-known/botto auto-discovery
├── router/
│   ├── mod.rs                      # Message dispatch (request/response + streaming)
│   └── handlers.rs                 # 20+ request handlers
├── db/
│   ├── mod.rs                      # SQLite init, WAL mode, embedded migrations
│   └── queries.rs                  # Typed query wrappers
├── types/
│   ├── messages.rs                 # Wire protocol types
│   ├── review.rs                   # Core review types (MrContext, FileReview, etc.)
│   ├── verification.rs             # Adversarial tests, contracts, trust assessment
│   ├── settings.rs                 # AiTaskType enum, per-task temperature defaults
│   ├── state.rs                    # AppState (Arc-wrapped), Connection, InFlightReview
│   ├── queue.rs                    # QueuedReview, QueueItemStatus, ReviewPriority
│   └── sandbox.rs                  # SandboxJob, SandboxJobStatus, SandboxStrategy
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
│   │   ├── manager.rs              # Docker container lifecycle for auto-fix
│   │   └── detector.rs             # Language detection, version-aware image resolution
│   ├── verification/
│   │   └── trust.rs                # Trust calibrator (weighted scoring)
│   ├── events/
│   │   └── mod.rs                  # In-process broadcast event bus
│   ├── ticket/
│   │   └── jira.rs                 # Jira REST API client
│   └── harness/
│       ├── orchestrator.rs         # Self-evolving prompt engineering loop
│       ├── runner.rs               # Runs variants against test cases
│       ├── grader.rs               # Scores results (weighted rubric)
│       ├── judge.rs                # AI judge (mutations + analysis)
│       ├── memory.rs               # Filesystem persistence
│       └── test_case.rs            # Test case discovery from real MRs
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

Integration tests spin up real Axum server instances with in-memory SQLite and ephemeral TCP ports:

- WebSocket auth flow and health endpoints
- Comment action CRUD via WebSocket
- Team settings persistence
- Presence tracking (VIEWING_MR / LEFT_MR)
- Queue operations (enqueue, pause, resume, cancel)
- Error handling for unknown request types
- Sandbox job queries
- Otto camelCase nested payload compatibility
- Botto flat snake_case format regression

Unit tests are embedded in source files covering language detection, version parsing, image inference, priority scoring, hashing, JSON repair, and URL encoding.

```bash
cargo test                          # All tests
cargo test --test integration       # Integration tests only
cargo test -- --nocapture           # With stdout
```

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
