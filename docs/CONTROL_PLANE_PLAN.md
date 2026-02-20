# ZeroClaw Control Plane - Master Plan

## Goal

Turn ZeroClaw CP from a migration tool into a full operator control plane: lifecycle management, monitoring, configuration, and eventually a web UI for managing all agent instances from one place.

## Non-Goals

- Multi-host / distributed deployment (single machine only)
- User authentication / multi-tenancy (single operator)
- Auto-scaling or dynamic instance creation (operator-driven only)

## Current State (Phase 13.5 - Complete)

- `zeroclaw-cp migrate from-openclaw` imports agents from OpenClaw config
- SQLite registry tracks instances (id, name, status, port, config_path, workspace_dir, archived_at, migration_run_id)
- Instance configs written to `~/.zeroclaw/cp/instances/<uuid>/config.toml`
- `zeroclaw-cp serve` acquires migration lock, runs reconciliation, lists instances, blocks
- `zeroclaw daemon --port <port>` runs a single agent as foreground daemon
- `zeroclaw-cp start/stop/restart/status/logs` manage instance lifecycles
- PID tracking via `daemon.pid` per instance with `/proc/<pid>/environ` ownership verification
- Per-instance flock prevents concurrent start/stop races
- Post-spawn survival check catches immediate crashes; rollback kills orphans on bookkeeping failure
- EPERM-aware liveness check; post-SIGKILL recheck before clearing state
- Log rotation on start

## Phase 7: Persistent Supervisor + HTTP API

**Goal**: Long-running CP server that monitors instance health, auto-detects crashes, and exposes lifecycle operations via HTTP.

**Scope**:
- Persistent supervisor loop with periodic health checks
- DB PID column + reconciliation (adopt orphans, detect crashes)
- HTTP API: GET /instances, POST /instances/:id/start, POST /instances/:id/stop, etc.
- Proper signal handling (SIGINT/SIGTERM) for serve
- Health aggregation across all instances

**Not in scope**: Web UI, config editing API, inter-agent messaging

**Exit gates**:
1. Server detects crashed instance within 30s and updates status
2. Server survives restart without losing track of running instances
3. HTTP API matches CLI functionality
4. Proper graceful shutdown on SIGTERM

**Dependencies**: Phase 6

---

## Phase 7.5: Agent Observability API

**Goal**: Provide complete per-agent visibility for operator workflows: what each agent is, what it is configured to do, what it recently did, and how much model usage it consumed.

**Scope**:
- Agent Details API (read-only):
  - Build/version metadata per instance
  - Effective runtime config snapshot (non-secret fields)
  - Persona/profile file inventory (which files are active)
  - Channel policy snapshot (e.g., Telegram allowed users, channel enablement)
  - Model/provider settings snapshot
- Task/Event History API:
  - Persist per-agent task/event records
  - Query "last X" by instance with filters (time range, channel, status)
  - Include summary/outcome/duration and correlation IDs when available
- Per-Agent Usage API:
  - Capture input/output/total tokens per request when provider data is available
  - Aggregate by instance and time window
  - Mark missing/unknown usage explicitly (no silent zeros)
- Logs API:
  - Tail endpoint (existing behavior)
  - Full-history pagination endpoint (line/offset or cursor based)
  - Download endpoint for full log file(s)
  - Retention + rotation policy exposed in API metadata

**Secret Handling Policy (mandatory for all read endpoints)**:
- Never return raw secret values
- Masked read format for secret fields
- One-way set semantics for secrets (when write APIs arrive in later phases)
- Redact secrets from error messages and server logs

**Not in scope**: Mutating config/workspace files (Phase 8), web UI rendering (Phase 9), cross-agent messaging/handoff (Phase 10)

**Exit gates**:
1. `GET /instances/:id/details` returns build/config/persona/channel/model snapshot with secrets masked
2. `GET /instances/:id/tasks?limit=X` returns stable "last X" results with ordering guarantees
3. `GET /instances/:id/usage?window=...` returns per-agent token totals and unknown-data markers
4. `GET /instances/:id/logs` supports both tail and paginated history; full download works
5. Automated tests prove raw secrets never appear in API responses or logs

**Dependencies**: Phase 7

---

## Phase 8: Config + Workspace Mutation API

**Goal**: Safe programmatic editing of instance configs without manual file manipulation.

**Scope**:
- REST API: read/write/diff instance configs
- Atomic config writes (temp + fsync + rename)
- ETag-based preconditions (no silent overwrites)
- Blocked fields (prevent editing secrets via API without explicit override)
- Config validation before write
- Restart-after-config-change workflow

**Not in scope**: Web UI for config editing, template/schema management

**Exit gates**:
1. Config read/write/diff via HTTP
2. Concurrent write detected and rejected (ETag mismatch)
3. Invalid config rejected before write
4. Instance restart picks up config changes

**Dependencies**: Phase 7.5

---

## Phase 9: Web Command Center UI (MVP)

**Goal**: Browser-based dashboard for managing all instances visually.

**Scope**:
- Instance list with live status (running/stopped/error)
- Start/stop/restart buttons
- Log viewer (tail + paginated history via Phase 7.5 API)
- Config editor (read/write via Phase 8 API)
- Basic responsive layout

**Not in scope**: Real-time WebSocket streaming, user auth, mobile optimization, inter-agent features

**Exit gates**:
1. All Phase 6 CLI operations reproducible from browser
2. Log viewer shows last 100 lines with auto-refresh
3. Config editor prevents concurrent overwrites
4. Works in Chrome/Firefox

**Dependencies**: Phase 8

---

## Phase 10: Inter-Agent Coordination

**Goal**: Agents can communicate, hand off tasks, and share context through the CP.

### Phase 10.1: Inter-Agent Messaging Core

**Architecture**: CP-routed messaging only (no direct agent-to-agent transport). All messages flow through the CP, which enforces routing policy, persists the audit trail, and manages delivery.

**Message envelope**:
- `id` (UUID)
- `from` (agent instance ID)
- `to` (agent instance ID)
- `type` (application-defined message type string)
- `payload` (JSON, max 64 KiB)
- `correlation_id` (optional, links request/response chains)
- `idempotency_key` (client-provided, prevents duplicate processing)
- `created_at` (ISO 8601 timestamp)
- `hop_count` (incremented on each forward/relay, hard limit of 8)

**Delivery semantics**: at-least-once with idempotent consumer handling.
- No "exactly-once" claims. Consumers must be prepared to see duplicates (deduplicated via `idempotency_key`).
- Producers receive a delivery receipt with the message `id` and initial state.

**Message states**: `queued` -> `delivered` | `failed` | `dead_letter`
- `delivered` -> `acknowledged` (consumer confirms processing)
- `failed` after max retry attempts -> `dead_letter`
- `dead_letter` is terminal; requires operator intervention to replay

**Reliability**:
- Retry with exponential backoff (initial 1s, max 60s)
- Max retry attempts: 5 (configurable per routing rule)
- TTL: messages expire after 1 hour if undelivered (configurable)
- Dead-letter state for messages that exhaust retries or exceed TTL

**Safety**:
- Max payload size: 64 KiB (reject at ingestion, not silently truncate)
- Hop limit: 8 (prevents infinite forwarding loops between agents)
- Duplicate suppression: `idempotency_key` deduplication window of 24 hours

**Routing policy**: explicit allowlist by `(from, to, type)` tuple, deny by default.
- Policy stored in CP SQLite as a routing rules table
- No wildcard `*` for `from` or `to` (explicit pairs only)
- `type` supports prefix matching (e.g., `task.*` matches `task.handoff`, `task.status`)
- Unauthorized routes rejected with clear error (no silent drop)

**Secret hygiene**: Redact known secret patterns (API keys, tokens, passwords) from payload before persistence. Redaction happens at the CP routing layer, not just in the UI.

**Scope**:
- Message routing between instances via CP
- Persistent message queue in CP SQLite
- Task handoff protocol (agent A delegates to agent B with correlation tracking)
- Routing rules CRUD API
- Message send/receive/acknowledge API

**Not in scope**: External API consumers, multi-host routing, agent auto-discovery, guaranteed ordering (messages may arrive out of order)

**Exit gates**:
1. Agent A can send message to Agent B via CP; Agent B receives and acknowledges
2. Duplicate message with same `idempotency_key` is suppressed (not double-processed)
3. Message exceeding hop limit is rejected with clear error
4. Unauthorized route (not in allowlist) rejected before queuing
5. Failed delivery after max retries moves message to `dead_letter`
6. Payload exceeding 64 KiB rejected at ingestion
7. Known secret patterns in payload are redacted before persistence

### Phase 10.2: Messaging Observability

**Goal**: Full visibility into inter-agent message flow for the operator.

**API endpoints**:
- `GET /instances/:id/messages?direction=in|out` -- inbox/outbox per agent
- `GET /messages/:id` -- single message with full state transition history
- `GET /messages?correlation_id=...` -- timeline of a correlated chain
- `GET /messages?from=...&to=...&type=...&status=...&after=...&before=...` -- filtered search
- `GET /messages/dead_letter` -- dead-letter queue for operator triage
- `POST /messages/:id/replay` -- replay a dead-letter message (resets state to `queued`)

**Timeline model**:
- Every state transition recorded with timestamp in a `message_events` table
- Events: `created`, `queued`, `delivery_attempted`, `delivered`, `acknowledged`, `failed`, `dead_lettered`, `replayed`
- Each event includes optional `detail` field (e.g., error message on failure)
- Immutable append-only log (events are never updated or deleted)

**Filters**: agent, status, type, correlation_id, time range, hop_count

**Secret hygiene**: Redact before store and before render. No raw secret values in API responses, logs, or error messages. Redaction is applied at the persistence boundary (Phase 10.1), so the observability layer never sees raw secrets.

**Not in scope**: Real-time WebSocket streaming of messages (Phase 9 web UI can poll), cross-CP federation

**Exit gates**:
1. `GET /instances/:id/messages` returns correct inbox/outbox with pagination
2. `GET /messages?correlation_id=X` returns full chain in chronological order
3. Dead-letter queue is browsable and messages can be replayed
4. Full state transition history visible for any message
5. Automated tests prove raw secrets never appear in message payloads, API responses, or logs
6. Audit log is append-only (no UPDATE/DELETE on `message_events`)

**Dependencies**: Phase 10.1; Phase 9 optional for visualization

---

## Phase 13.1: Agent CRUD API (Complete)

**Goal**: HTTP API to create, archive, unarchive, clone, and hard-delete agent instances -- making the CP a self-service platform without manual file manipulation.

**Scope**:
- `POST /api/instances` -- create with name, optional port/model/provider
- `POST /api/instances/:name/archive` -- soft-delete (stops if running via live_status truth)
- `POST /api/instances/:name/unarchive` -- restore archived instance (rejects active name conflict)
- `POST /api/instances/:name/clone` -- independent copy with new UUID/port, cleared auth state
- `DELETE /api/instances/:name` -- hard-delete archived instances only (two-step safety)
- Name validation: `^[a-zA-Z0-9][a-zA-Z0-9-]{0,63}$`, unique among active
- Port protection: unique index on active ports, auto-allocation 18801-18999 or explicit with 409 on conflict
- Messages/message_events preserved on delete (Phase 10.2 append-only contract)

**Exit gates** (all passed):
1. POST creates bootable agent (config.toml + DB row + workspace dirs)
2. Archive uses live_status() truth, stops if running, then soft-deletes
3. Unarchive restores; rejects if active name conflict
4. Clone produces independent copy with cleared auth
5. Delete hard-deletes only archived (two-step safety), preserves messages
6. Name validation enforced
7. Port uniqueness via DB index, 409 on conflict
8. 20 gate tests in `tests/phase13_gates.rs`
9. Full test suite passes

**Dependencies**: Phase 10.2 (for message preservation contract)

---

## Phase 13.5: CRUD UI (Complete)

**Goal**: Add all Phase 13.1 CRUD operations to the embedded SPA with proper UX (confirmations, error feedback, auto-refresh).

**Scope**:
- Dashboard toolbar: "New Instance" button + "Show Archived" toggle
- Create instance modal: name, port, provider, model fields
- Clone instance modal: new name, port fields
- Archive with confirmation dialog (warns about stopping running instance)
- Unarchive (direct action, no confirmation needed)
- Delete with "permanently delete?" confirmation dialog
- Clean error rendering for 400/404/409 responses
- `?include_archived=true` query param on `GET /api/instances`
- Archived instance detail views (route, status, banner, action buttons)
- Backend: `GET /instances/:name` and `/details` fall back to archived lookup
- Archived cards visually distinct (dashed border, dimmed opacity)

**Exit gates** (all passed):
1. All 5 CRUD actions work from browser with confirmations
2. 400/404/409 rendered as user-visible error notifications
3. List/detail views auto-refresh after mutations
4. 5 UI-flow gate tests (include_archived, get/details archived, full lifecycle, sort order)
5. Full test suite passes (2,396 tests)

**Dependencies**: Phase 13.1

---

## Phase 14: Real-Time WebSocket Streams

See `docs/PHASE14_WEBSOCKET_SPEC.md` for full specification.

---

## Risk Register

| Risk | Mitigation |
|------|-----------|
| PID reuse kills wrong process | Phase 6: /proc ownership check before any signal |
| Split-brain PID state | Deferred to Phase 7: single pidfile in Phase 6, DB+reconcile in Phase 7 |
| Stale state after CP crash | Phase 7: reconcile loop detects and corrects on restart |
| Config corruption | Phase 8: atomic writes + ETag preconditions |
| Missing per-agent traceability | Phase 7.5: details/tasks/usage/log APIs with per-instance queries |
| Secret leakage in observability endpoints | Phase 7.5+: masked reads, never-return-raw, redaction tests |
| Scope creep per phase | Strict in/out scope lists + exit gates |
| Inter-agent message loops | Phase 10.1: hop limit (8) + duplicate suppression via idempotency key |
| Message queue unbounded growth | Phase 10.1: TTL expiry + dead-letter terminal state |
| Secret leakage in message payloads | Phase 10.1: redact known patterns before persistence, not just at render |
| Accidental hard-delete of active instance | Phase 13.1: two-step safety (archive first), 409 on active delete |
| Port conflict on create/clone | Phase 13.1: unique active-port index, 409 on SQLite constraint violation |
| Orphaned files after failed create/clone | Phase 13.1: cleanup-on-error removes inst_dir on any failure after mkdir |
| Polling overhead on dashboard | Phase 14: WebSocket push replaces 5s polling intervals |
