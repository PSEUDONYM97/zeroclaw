# Changelog

All notable changes to ZeroClaw will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added (Phase 10.2 - Messaging Observability)

- **GET /api/messages** - Filtered search across all messages with pagination. Supports
  `correlation_id`, `from`, `to`, `type`, `status`, `after`, `before`, `limit`, `offset`.
  Returns `{ items, total, limit, offset }`.
- **GET /api/messages/:id** - Single message detail with full state transition history.
  Returns the sanitized message object plus an `events` array (chronological).
- **GET /api/messages/:id/events** - Event timeline for a message. Events sorted by
  `created_at ASC, id ASC`. Returns `{ message_id, events }`.
- **GET /api/messages/dead-letter** - Dead-letter queue browser. Sorted by `updated_at DESC`
  (most recently dead-lettered first). Supports `from`, `to`, `after`, `before` filters.
- **POST /api/messages/:id/replay** - Replay a dead-lettered message. Resets status to
  `queued`, retry_count to 0, clears lease/attempt timestamps, computes fresh TTL from
  original `expires_at - created_at` (clamped 5m..24h, fallback 1h). Appends `replayed`
  and `queued` events atomically. Returns 200 (success), 404 (not found), 409 (not dead_letter).
- **GET /api/instances/:name/messages** - Per-instance inbox/outbox. `direction` param
  controls filtering: `in` (to_instance), `out` (from_instance), `all` (either). Returns 404
  if instance does not exist.
- **Append-only audit enforcement** - DB triggers prevent UPDATE and DELETE on `message_events`.
  Any attempt raises "message_events is append-only" error.
- **Defense-in-depth secret redaction** - All 6 new read-path handlers pass payloads through
  `redact_payload_secrets()` before response, even though ingest already redacts. Verified
  end-to-end: raw secrets never appear in API responses or DB storage.
- **4 new DB indexes** - `idx_messages_from_status`, `idx_messages_created_at`,
  `idx_messages_status`, `idx_message_events_msg_created` for observability query performance.
- **Datetime validation** - All `after`/`before` query params require canonical
  `YYYY-MM-DD HH:MM:SS` format. Returns 400 with descriptive error on parse failure.

### Security
- **Legacy XOR cipher migration**: The `enc:` prefix (XOR cipher) is now deprecated. 
  Secrets using this format will be automatically migrated to `enc2:` (ChaCha20-Poly1305 AEAD)
  when decrypted via `decrypt_and_migrate()`. A `tracing::warn!` is emitted when legacy
  values are encountered. The XOR cipher will be removed in a future release.

### Added
- `SecretStore::decrypt_and_migrate()` — Decrypts secrets and returns a migrated `enc2:` 
  value if the input used the legacy `enc:` format
- `SecretStore::needs_migration()` — Check if a value uses the legacy `enc:` format
- `SecretStore::is_secure_encrypted()` — Check if a value uses the secure `enc2:` format

### Deprecated
- `enc:` prefix for encrypted secrets — Use `enc2:` (ChaCha20-Poly1305) instead.
  Legacy values are still decrypted for backward compatibility but should be migrated.

## [0.1.0] - 2026-02-13

### Added
- **Core Architecture**: Trait-based pluggable system for Provider, Channel, Observer, RuntimeAdapter, Tool
- **Provider**: OpenRouter implementation (access Claude, GPT-4, Llama, Gemini via single API)
- **Channels**: CLI channel with interactive and single-message modes
- **Observability**: NoopObserver (zero overhead), LogObserver (tracing), MultiObserver (fan-out)
- **Security**: Workspace sandboxing, command allowlisting, path traversal blocking, autonomy levels (ReadOnly/Supervised/Full), rate limiting
- **Tools**: Shell (sandboxed), FileRead (path-checked), FileWrite (path-checked)
- **Memory (Brain)**: SQLite persistent backend (searchable, survives restarts), Markdown backend (plain files, human-readable)
- **Heartbeat Engine**: Periodic task execution from HEARTBEAT.md
- **Runtime**: Native adapter for Mac/Linux/Raspberry Pi
- **Config**: TOML-based configuration with sensible defaults
- **Onboarding**: Interactive CLI wizard with workspace scaffolding
- **CLI Commands**: agent, gateway, status, cron, channel, tools, onboard
- **CI/CD**: GitHub Actions with cross-platform builds (Linux, macOS Intel/ARM, Windows)
- **Tests**: 159 inline tests covering all modules and edge cases
- **Binary**: 3.1MB optimized release build (includes bundled SQLite)

### Security
- Path traversal attack prevention
- Command injection blocking
- Workspace escape prevention
- Forbidden system path protection (`/etc`, `/root`, `~/.ssh`)

[0.1.0]: https://github.com/theonlyhennygod/zeroclaw/releases/tag/v0.1.0
