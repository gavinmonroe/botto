# Botto

Shared orchestration backend for [Otto](../otto) Chrome extensions. Single Rust binary that centralizes AI code reviews, caching, and event coordination across a team.

## What it does

When multiple developers have Otto installed, Botto replaces the per-extension service worker with a shared server:

- **One review per MR** — if User A triggers a review on MR !42, Users B-E get the same results instantly (cached or live-streamed)
- **Shared cache** — SQLite-backed, gzip-compressed, diff-hash-keyed. No duplicate AI calls
- **Shared events** — comment actions, presence, review progress broadcast to all viewers
- **Sandbox auto-fix** — Docker containers that clone the repo, apply a suggested fix, run tests, and push on success

## Quick start

```bash
# Build
cargo build --release

# Run (auto-configures, creates ./data/botto.db)
BOTTO_API_KEY=your-team-secret \
BOTTO_GITLAB_TOKEN=glpat-... \
BOTTO_GITLAB_URL=https://gitlab.com \
BOTTO_AI_URL=https://openrouter.ai/api/v1 \
BOTTO_AI_KEY=sk-... \
./target/release/botto
```

Or with Docker:

```bash
cp botto.example.toml data/botto.toml
# Edit data/botto.toml with your credentials
docker compose up -d
```

## Configuration

All config is optional — Botto auto-detects what it can. Priority: env vars > `botto.toml` > auto-detected defaults.

| Env var | Config key | Description |
|---|---|---|
| `BOTTO_API_KEY` | `auth.api_key` | Shared secret for Otto authentication |
| `BOTTO_GITLAB_TOKEN` | `gitlab.bot_token` | Bot PAT (`read_api` + `write_repository`) |
| `BOTTO_GITLAB_URL` | `gitlab.url` | GitLab instance URL |
| `BOTTO_AI_KEY` | `ai.api_key` | OpenAI-compatible API key |
| `BOTTO_AI_URL` | `ai.base_url` | AI endpoint URL |
| `BOTTO_WEBHOOK_SECRET` | `gitlab.webhook_secret` | GitLab webhook validation token |

See [botto.example.toml](botto.example.toml) for all options.

## Connecting Otto

1. Open Otto settings (extension options page)
2. Scroll to "Botto Server"
3. Enter the server URL: `wss://your-botto-host:7700/ws`
4. Enter the team API key
5. Click Test, then Save

Or use auto-discovery: if Botto is accessible at the same domain as your GitLab instance, Otto can find it automatically via `/.well-known/botto`.

## Architecture

```
Otto extensions ←→ WebSocket ←→ Botto server
                                    ├── Review orchestrator (parallel AI pipeline)
                                    ├── GitLab client (bot PAT, REST v4)
                                    ├── AI client (OpenAI-compatible, SSE streaming)
                                    ├── SQLite cache (WAL mode, gzip compressed)
                                    ├── Event bus (presence, actions, notifications)
                                    ├── Review queue (priority scoring, serial execution)
                                    └── Sandbox manager (Docker, auto-fix)
```

## Endpoints

| Route | Method | Description |
|---|---|---|
| `/ws` | WS | Otto WebSocket connections |
| `/health` | GET | Liveness probe |
| `/ready` | GET | Readiness probe (DB + capabilities) |
| `/api/webhooks/gitlab` | POST | GitLab webhook receiver |
| `/.well-known/botto` | GET | Auto-discovery for Otto |

## Sandbox (auto-fix)

When enabled (requires Docker), Botto can automatically apply suggested fixes:

1. Detects base image from `.otto.json` → `Dockerfile` → language heuristics
2. Creates an isolated Docker container with resource limits
3. Clones the repo, applies the fix, runs tests
4. Commits and pushes on success

Configure in `.otto.json` at the repo root:
```json
{
  "sandbox": {
    "image": "node:22-slim"
  }
}
```

## Development

```bash
cargo check          # Type check
cargo test           # Run tests
cargo run            # Dev server (auto-creates ./data/)
RUST_LOG=botto=debug cargo run  # Verbose logging
```
