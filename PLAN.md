# Botto — Execution Plan

Shared orchestration backend for Otto Chrome extensions. Single Rust binary,
SQLite (WAL), WebSocket gateway, Docker sandbox for auto-fix.

## Decisions Made

- **Auth (Otto→Botto):** Shared API key (team secret). Botto validates on WS connect.
- **Auth (Botto→GitLab):** Central bot PAT (`read_api` + `write_repository`).
- **Auth (Botto→AI):** Central API key, OpenAI-compatible endpoint.
- **Sandbox:** Docker containers. Base image auto-detected from Dockerfile or `.otto.json`.
- **Database:** SQLite with WAL mode. Single file, auto-migrated.
- **Shared triage:** Configurable per-project. Default off (per-user actions).
- **Discovery:** Manual URL in Otto settings + optional `.well-known/botto` endpoint.

## Phase 1 — Skeleton

- Cargo workspace, crate structure
- `main.rs`: CLI (clap), config loading, server startup
- `config.rs`: Auto-detection (Docker, CPU, memory, disk) + `botto.toml` parsing
- `db/`: SQLite setup, migrations, connection pool
- `server.rs`: Axum HTTP + WebSocket upgrade
- `api/ws.rs`: WebSocket gateway (connect, auth, disconnect, message routing)
- `api/health.rs`: Health + readiness endpoints
- `router/`: Message type → handler dispatch (stub handlers)

## Phase 2 — Core Types

Port all Otto types to Rust with serde:
- `types/messages.rs` — Request/Response/StreamChunk protocol
- `types/review.rs` — MrContext, FileReview, ReviewComment, EdgeCase, etc.
- `types/settings.rs` — AiConfig, server config
- `types/verification.rs` — Tests, contracts, behavioral delta, trust
- `types/queue.rs` — Queue items, priority
- `types/sandbox.rs` — Sandbox job types

## Phase 3 — GitLab + AI Clients

- `services/gitlab/client.rs` — REST v4, pagination, bot PAT auth
- `services/gitlab/diff_parser.rs` — Unified diff parsing
- `services/gitlab/repo_explorer.rs` — AI tool-use repo navigation
- `services/gitlab/context_enrichment.rs` — Import analysis, caller discovery
- `services/ai/client.rs` — OpenAI-compatible, SSE streaming, tool calling
- `services/ai/service.rs` — Per-task orchestration, JSON repair
- `util/json_repair.rs` — Truncated JSON repair
- `util/retry.rs` — Exponential backoff

## Phase 4 — Prompt Builders

Port all 11 prompt templates:
- shared, summary, code_review, edge_cases, related_files, followup,
  chat, ac_validation, adversarial_tests, contracts, behavioral_delta

## Phase 5 — Review Orchestrator + Cache

- `services/review/orchestrator.rs` — Pipeline (parallel phases, streaming)
- `services/review/cache.rs` — SQLite cache, diff hashing (djb2), TTL, incremental
- `services/review/prefs.rs` — Reviewer preference learning
- `services/verification/trust.rs` — Trust calibrator (weighted scoring)
- `services/verification/ci_bridge.rs` — GitLab CI trigger + poll

## Phase 6 — Event Bus + Shared State

- `services/events/bus.rs` — tokio::broadcast, event types
- Comment action persistence + broadcast
- Presence tracking (who's viewing what MR)
- Team settings (shared triage toggle)

## Phase 7 — Review Queue

- `services/queue/manager.rs` — Priority queue, serial execution, scheduling
- `services/queue/priority.rs` — Priority scoring (same algo as Otto)
- SQLite persistence, rehydration on restart

## Phase 8 — Sandbox Manager

- `services/sandbox/manager.rs` — Docker container lifecycle (bollard)
- `services/sandbox/detector.rs` — Auto-detect Docker, resources, capabilities
- `services/sandbox/repo_setup.rs` — Clone, detect runtime, install deps
- `services/sandbox/fix_runner.rs` — Apply fix, run tests, validate
- `services/sandbox/git_ops.rs` — Commit + push on success
- Base image detection: .otto.json → Dockerfile → language heuristics

## Phase 9 — GitLab Webhooks + Discovery

- `api/webhooks.rs` — Receive MR/push/note events, validate secret
- Cache invalidation on push events
- Auto-enqueue reviews on MR open/update
- `api/discovery.rs` — `.well-known/botto` endpoint

## Phase 10 — Otto Extension Modifications

- `src/lib/botto-client.ts` — WebSocket connection manager
- `src/lib/messaging.ts` — Transport abstraction (local vs botto)
- `src/types/settings.ts` — Add `botto` config section
- `src/components/settings/BottoConnectionForm.tsx` — Server URL + test
- `src/components/review/ReviewComment.tsx` — "Apply Fix" button
- `src/services/review/stream-dispatcher.ts` — New chunk types
