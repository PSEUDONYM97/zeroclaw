# Phase 14: Real-Time WebSocket Streams

## Goal

Replace polling with server-push WebSocket streams for instance status, logs, and message events. The dashboard currently polls every 3-15 seconds; WebSocket push eliminates wasted requests and delivers sub-second updates.

## Non-Goals

- Binary protocol or custom framing (use JSON text frames)
- Multi-tenant auth (single operator, same as rest of CP)
- Streaming to external consumers outside the SPA
- Guaranteed message ordering across streams (per-stream ordering only)

---

## Architecture

### Transport

All WebSocket endpoints live under `/ws/` prefix, separate from the REST `/api/` namespace. Each endpoint upgrades a standard HTTP request to a WebSocket connection using axum's built-in `WebSocket` extractor (axum 0.7 + `ws` feature).

### Dependency

axum 0.7 with `ws` feature flag (add to existing axum dependency in Cargo.toml). No new crate needed -- `tokio-tungstenite` is already present.

### Connection Lifecycle

```
Client                          Server
  |-- GET /ws/instances -------->|
  |   Upgrade: websocket         |
  |<-------- 101 Switching ------|
  |                              |
  |<--- JSON frame (snapshot) ---|  (initial state dump)
  |<--- JSON frame (delta) ------|  (on change)
  |<--- JSON frame (delta) ------|
  |                              |
  |--- { "type": "ping" } ----->|  (client keepalive)
  |<-- { "type": "pong" } ------|
  |                              |
  |--- close frame ------------->|
  |<-- close frame --------------|
```

---

## Endpoints

### 1. `GET /ws/instances` -- Instance Status Stream

Pushes instance status changes in real time. Replaces the 5-second dashboard poll.

**Initial frame** (sent immediately on connect):

```json
{
  "type": "snapshot",
  "instances": [
    {
      "name": "agent-1",
      "status": "running",
      "port": 18801,
      "pid": 12345,
      "archived_at": null
    }
  ],
  "include_archived": false
}
```

**Delta frames** (sent on status change):

```json
{
  "type": "status_changed",
  "name": "agent-1",
  "status": "stopped",
  "pid": null,
  "timestamp": "2026-02-20T01:23:45Z"
}
```

```json
{
  "type": "instance_created",
  "name": "agent-2",
  "port": 18802,
  "status": "stopped",
  "timestamp": "2026-02-20T01:24:00Z"
}
```

```json
{
  "type": "instance_archived",
  "name": "agent-1",
  "timestamp": "2026-02-20T01:25:00Z"
}
```

**Client messages** (optional):

```json
{ "type": "set_include_archived", "value": true }
```

Toggles whether archived instances appear in snapshots and deltas. Default: false (matches current SPA behavior).

**Server-side implementation**:
- A `tokio::sync::broadcast` channel carries instance events from lifecycle operations
- The supervisor health-check loop (existing) feeds status changes into the broadcast
- Each WebSocket connection subscribes to the broadcast and filters by `include_archived`
- Stale detection: if the channel lags (buffer full), send a full re-snapshot instead of catching up on missed deltas

### 2. `GET /ws/instances/:name/logs` -- Log Tail Stream

Pushes new log lines as they're written. Replaces the 3-second log poll.

**Query params**:
- `backlog=N` -- send last N lines on connect (default: 100, max: 1000)

**Initial frame**:

```json
{
  "type": "backlog",
  "lines": ["2026-02-20 01:00:00 INFO Starting...", "..."],
  "count": 100
}
```

**Subsequent frames** (new lines as they appear):

```json
{
  "type": "lines",
  "lines": ["2026-02-20 01:23:45 INFO Request handled"],
  "count": 1
}
```

**Server-side implementation**:
- `tokio::fs::File` + `tokio::io::AsyncSeekExt` to tail the log file
- Poll the file for new bytes every 500ms (inotify would be better but adds platform complexity)
- Batch new lines within a 100ms window before sending (avoid per-line frame overhead)
- If the log file rotates (detected by inode change or file shrinkage), send a `{ "type": "rotated" }` frame and re-open

### 3. `GET /ws/messages` -- Message Event Stream

Pushes inter-agent message state transitions in real time. Replaces polling for message observability.

**Query params**:
- `instance=<name>` -- filter to messages involving this instance (from or to)
- `correlation_id=<id>` -- filter to a specific correlation chain

**Initial frame**:

```json
{
  "type": "snapshot",
  "recent_events": [
    {
      "message_id": "uuid",
      "event": "delivered",
      "from": "agent-1",
      "to": "agent-2",
      "message_type": "task.handoff",
      "timestamp": "2026-02-20T01:20:00Z"
    }
  ]
}
```

**Delta frames**:

```json
{
  "type": "message_event",
  "message_id": "uuid",
  "event": "acknowledged",
  "from": "agent-1",
  "to": "agent-2",
  "message_type": "task.handoff",
  "detail": null,
  "timestamp": "2026-02-20T01:20:05Z"
}
```

**Server-side implementation**:
- Message send/ack/fail operations publish to a `tokio::sync::broadcast` channel
- Each WebSocket client subscribes and applies its instance/correlation_id filter
- Secret hygiene: payloads are NOT included in stream frames (only metadata). Clients fetch full message via REST `GET /api/messages/:id` if needed.

---

## Shared Protocol

### Frame Format

All frames are JSON text. No binary frames. Each frame has a `type` field as discriminator.

### Keepalive

- **Client** sends `{ "type": "ping" }` every 30 seconds
- **Server** responds with `{ "type": "pong" }` within 5 seconds
- If server receives no client frame for 90 seconds, close with code 1001 (Going Away)
- If client receives no server frame for 90 seconds, reconnect

### Error Frames

```json
{
  "type": "error",
  "code": "instance_not_found",
  "message": "No instance named 'nonexistent'"
}
```

After an error frame, the server closes the connection with WebSocket close code 4000 + HTTP-equivalent (e.g., 4404 for not found).

### Close Codes

| Code | Meaning |
|------|---------|
| 1000 | Normal closure (client or server) |
| 1001 | Going away (server shutdown) |
| 1008 | Policy violation (invalid frame) |
| 4400 | Bad request (invalid query params) |
| 4404 | Instance not found |
| 4500 | Internal server error |

---

## Auth Model

Phase 14 has no authentication (consistent with all existing CP endpoints -- single operator, non-goal per plan). The WebSocket upgrade request is a standard HTTP GET with no auth headers required.

**Future consideration** (not in Phase 14 scope): When auth is added in a later phase, WebSocket connections will authenticate via:
1. Token in query param: `/ws/instances?token=<jwt>` (since WebSocket API doesn't support custom headers in browser)
2. Validated on upgrade, rejected with 401 before protocol switch
3. Token expiry checked periodically; expired connections closed with code 4401

---

## Reconnection and Backoff

Client-side reconnection strategy (implemented in SPA):

```
attempt  delay
1        1s
2        2s
3        4s
4        8s
5        16s
6+       30s (cap)
```

- Exponential backoff with jitter: `delay * (0.5 + Math.random() * 0.5)`
- Reset backoff to 0 on successful connection (first server frame received)
- Max reconnection attempts: unlimited (WebSocket is the primary transport)
- On reconnect, server sends a fresh snapshot -- client does not need to track deltas across connections

### Graceful Degradation

If WebSocket connection fails 3 times consecutively, fall back to REST polling at original intervals (5s instances, 3s logs, 15s health). Display a subtle indicator in the SPA header: "Live updates unavailable -- polling".

When a WebSocket connection succeeds again, stop polling and switch back.

---

## Migration Path (Polling Fallback)

### Phase 14.0: Backend WebSocket Endpoints (no SPA changes)

1. Add `ws` feature to axum dependency
2. Implement broadcast channels in `CpState` for instance events and message events
3. Wire lifecycle operations (start/stop/restart/archive/create/etc) to publish events
4. Implement the 3 WebSocket endpoint handlers
5. Gate tests verify:
   - Connection upgrade works
   - Snapshot received on connect
   - Delta received after lifecycle operation
   - Keepalive ping/pong
   - Clean close on server shutdown

REST endpoints remain unchanged. SPA continues polling.

### Phase 14.5: SPA WebSocket Integration

1. Replace `setInterval` polling with WebSocket connections
2. Implement reconnection with backoff
3. Implement graceful degradation (fall back to polling on WS failure)
4. Remove polling code paths (keep as fallback only)
5. Add connection status indicator in header

**Key principle**: REST API is never removed. WebSocket is an optimization layer. Any client (curl, scripts, other tools) can still use REST polling.

---

## Server-Side Implementation Details

### Broadcast Architecture

```
                     ┌──────────────────┐
  lifecycle ops ────>│  broadcast::Sender│──┬──> WS client 1 (dashboard)
  supervisor loop ──>│  (instance_events)│  ├──> WS client 2 (dashboard)
  CRUD handlers ────>│                  │  └──> WS client 3 (detail view)
                     └──────────────────┘
                     ┌──────────────────┐
  message send ─────>│  broadcast::Sender│──┬──> WS client 4 (messages)
  message ack ──────>│  (message_events) │  └──> WS client 5 (messages)
                     └──────────────────┘
                     (log tailing is per-connection, no broadcast needed)
```

### CpState Changes

```rust
pub struct CpState {
    pub db_path: Arc<PathBuf>,
    pub instance_events: broadcast::Sender<InstanceEvent>,
    pub message_events: broadcast::Sender<MessageEvent>,
}
```

### Event Types

```rust
#[derive(Clone, Debug, Serialize)]
#[serde(tag = "type")]
enum InstanceEvent {
    StatusChanged { name: String, status: String, pid: Option<u32> },
    InstanceCreated { name: String, port: u16, status: String },
    InstanceArchived { name: String },
    InstanceUnarchived { name: String },
    InstanceDeleted { name: String },
}

#[derive(Clone, Debug, Serialize)]
struct MessageEvent {
    message_id: String,
    event: String,
    from: String,
    to: String,
    message_type: String,
    detail: Option<String>,
}
```

### Broadcast Buffer

`broadcast::channel(256)` -- 256-event buffer. If a slow WebSocket consumer lags behind 256 events, it receives a `RecvError::Lagged(n)`. The handler responds by sending a full snapshot to resynchronize.

---

## Exit Gates

1. `GET /ws/instances` upgrades to WebSocket; client receives snapshot frame with current instance list
2. Status change (start/stop/restart/archive/create/clone/delete) produces a delta frame within 1 second
3. `GET /ws/instances/:name/logs` streams new log lines within 1 second of write
4. Log rotation detected and signaled to client
5. `GET /ws/messages` streams message state transitions with correct filtering
6. Keepalive ping/pong works; idle connections closed after 90 seconds
7. Server graceful shutdown sends close frame to all connected clients
8. Slow consumer (lagged broadcast) receives re-snapshot instead of stale deltas
9. Secret payloads never appear in WebSocket message event frames
10. Automated gate tests for each endpoint (connect, snapshot, delta, close)
11. REST polling continues to work unchanged (backward compatible)
12. `cargo test` passes (all existing + new gates)

---

## Test Plan

### Unit Tests

| Test | What it verifies |
|------|------------------|
| `ws_instances_upgrade` | HTTP upgrade to WebSocket succeeds |
| `ws_instances_snapshot` | First frame is a complete instance snapshot |
| `ws_instances_delta_on_start` | Starting an instance produces a `status_changed` delta |
| `ws_instances_delta_on_create` | Creating an instance produces `instance_created` delta |
| `ws_instances_delta_on_archive` | Archiving produces `instance_archived` delta |
| `ws_instances_include_archived` | `set_include_archived` message toggles archived visibility |
| `ws_instances_lagged_resnapshot` | Slow consumer gets resynchronized with full snapshot |
| `ws_logs_backlog` | Backlog of N lines received on connect |
| `ws_logs_new_lines` | New log writes produce `lines` frames |
| `ws_logs_rotation` | Log file rotation produces `rotated` frame |
| `ws_logs_nonexistent_instance` | Connect with bad name gets error frame + 4404 close |
| `ws_messages_snapshot` | Recent message events received on connect |
| `ws_messages_delta` | New message send/ack produces delta frame |
| `ws_messages_filter_instance` | Instance filter limits events to matching messages |
| `ws_messages_filter_correlation` | Correlation filter limits events to matching chain |
| `ws_keepalive` | Ping produces pong within timeout |
| `ws_idle_disconnect` | No client frames for 90s results in server close |
| `ws_graceful_shutdown` | Server shutdown sends close frames to all clients |
| `ws_no_secrets_in_message_stream` | Message events contain metadata only, no payloads |

### Integration Tests

- Start CP, connect dashboard WS, create/start/stop/archive instances, verify all deltas arrive
- Open log WS, write to log file externally, verify lines stream
- Multiple simultaneous WS clients receive same events
- REST polling still works alongside active WS connections

### Manual Verification

```bash
# Connect to instance stream
websocat ws://localhost:18800/ws/instances

# Connect to log stream
websocat "ws://localhost:18800/ws/instances/my-agent/logs?backlog=50"

# Connect to message stream
websocat "ws://localhost:18800/ws/messages?instance=my-agent"
```

---

## Dependencies

- Phase 13.5 (CRUD UI -- provides the SPA to upgrade from polling)
- axum `ws` feature (add to Cargo.toml)
- No new external crates required
