# Changelog

All notable changes to ZeroClaw will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **Phase 13.1 -- Agent CRUD API**: HTTP endpoints to create (`POST /api/instances`),
  archive, unarchive, clone, and hard-delete agent instances. Includes name validation,
  auto port allocation (18801-18999), unique active-port index, atomic config writes,
  two-step delete safety (must archive before delete), and message preservation on delete.
  20 gate tests in `tests/phase13_gates.rs`.
- **Phase 13.5 -- CRUD UI**: Full CRUD actions in the embedded SPA. Dashboard toolbar
  with "New Instance" button and "Show Archived" toggle. Create/clone modal dialogs,
  archive/delete confirmation dialogs, clean error handling for 400/404/409 responses.
  Archived instance detail views with status banner and appropriate action buttons.
  `?include_archived=true` query param on instance list. 5 UI-flow gate tests.
- `Registry::list_instances_filtered(include_archived)` -- list instances with optional
  archived inclusion, sorted active-first
- `GET /instances/:name` and `/details` now fall back to archived instance lookup

### Security
- **Legacy XOR cipher migration**: The `enc:` prefix (XOR cipher) is now deprecated.
  Secrets using this format will be automatically migrated to `enc2:` (ChaCha20-Poly1305 AEAD)
  when decrypted via `decrypt_and_migrate()`. A `tracing::warn!` is emitted when legacy
  values are encountered. The XOR cipher will be removed in a future release.

### Added
- `SecretStore::decrypt_and_migrate()` -- Decrypts secrets and returns a migrated `enc2:`
  value if the input used the legacy `enc:` format
- `SecretStore::needs_migration()` -- Check if a value uses the legacy `enc:` format
- `SecretStore::is_secure_encrypted()` -- Check if a value uses the secure `enc2:` format

### Deprecated
- `enc:` prefix for encrypted secrets -- Use `enc2:` (ChaCha20-Poly1305) instead.
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
