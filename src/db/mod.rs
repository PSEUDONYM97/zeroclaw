use anyhow::{Context, Result};
use rusqlite::{params, Connection, OptionalExtension};
use std::path::Path;

// ── Messaging structs (Phase 10.1) ──────────────────────────────

/// A routing rule that authorizes messages between two instances.
#[derive(Debug, Clone)]
pub struct RoutingRule {
    pub id: String,
    pub from_instance: String,
    pub to_instance: String,
    pub type_pattern: String,
    pub max_retries: i64,
    pub ttl_secs: i64,
    pub auto_start: bool,
    pub created_at: String,
}

/// A queued inter-agent message.
#[derive(Debug, Clone)]
pub struct Message {
    pub id: String,
    pub from_instance: String,
    pub to_instance: String,
    pub message_type: String,
    pub payload: String,
    pub correlation_id: Option<String>,
    pub idempotency_key: Option<String>,
    pub hop_count: i64,
    pub status: String,
    pub retry_count: i64,
    pub max_retries: i64,
    pub next_attempt_at: Option<String>,
    pub lease_expires_at: Option<String>,
    pub expires_at: String,
    pub created_at: String,
    pub updated_at: String,
}

/// Parameters for creating a new message.
pub struct NewMessage {
    pub id: String,
    pub from_instance: String,
    pub to_instance: String,
    pub message_type: String,
    pub payload: String,
    pub correlation_id: Option<String>,
    pub idempotency_key: Option<String>,
    pub hop_count: i64,
    pub max_retries: i64,
    pub ttl_secs: i64,
}

/// An append-only audit event for a message.
#[derive(Debug, Clone)]
pub struct MessageEvent {
    pub id: i64,
    pub message_id: String,
    pub event_type: String,
    pub detail: Option<String>,
    pub created_at: String,
}

/// Filter parameters for querying messages.
pub struct MessageFilters {
    pub correlation_id: Option<String>,
    pub from_instance: Option<String>,
    pub to_instance: Option<String>,
    pub message_type: Option<String>,
    pub status: Option<String>,
    pub after: Option<String>,
    pub before: Option<String>,
    pub limit: usize,
    pub offset: usize,
}

/// Direction filter for per-instance message queries.
pub enum MessageDirection {
    In,
    Out,
    All,
}

/// Represents an agent lifecycle/task event.
#[derive(Debug, Clone)]
pub struct AgentEvent {
    pub id: String,
    pub instance_id: String,
    pub event_type: String,
    pub channel: Option<String>,
    pub summary: Option<String>,
    pub status: String,
    pub duration_ms: Option<i64>,
    pub correlation_id: Option<String>,
    pub metadata: Option<String>,
    pub created_at: String,
}

/// Represents a single model usage record.
#[derive(Debug, Clone)]
pub struct AgentUsageRecord {
    pub id: String,
    pub instance_id: String,
    pub input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub total_tokens: Option<i64>,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub request_id: Option<String>,
    pub created_at: String,
}

/// Aggregated usage summary for an instance within a time window.
#[derive(Debug, Clone)]
pub struct AgentUsageSummary {
    pub input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub total_tokens: Option<i64>,
    pub request_count: usize,
    pub unknown_count: usize,
}

/// Represents a managed ZeroClaw instance in the CP registry.
#[derive(Debug, Clone)]
pub struct Instance {
    pub id: String,
    pub name: String,
    pub status: String,
    pub port: u16,
    pub config_path: String,
    pub workspace_dir: Option<String>,
    pub archived_at: Option<String>,
    pub migration_run_id: Option<String>,
    /// Best-effort PID cache. The pidfile on disk is authoritative.
    pub pid: Option<u32>,
}

/// SQLite-backed registry for managing ZeroClaw instances.
pub struct Registry {
    conn: Connection,
}

impl Registry {
    /// Open (or create) the registry database at the given path.
    pub fn open(db_path: &Path) -> Result<Self> {
        let conn = Connection::open(db_path)
            .with_context(|| format!("Failed to open registry DB: {}", db_path.display()))?;

        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;")
            .context("Failed to set SQLite pragmas")?;

        Self::init_schema(&conn)?;
        Ok(Self { conn })
    }

    /// Open an in-memory registry (for testing).
    #[cfg(test)]
    pub fn open_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch("PRAGMA foreign_keys=ON;")?;
        Self::init_schema(&conn)?;
        Ok(Self { conn })
    }

    fn init_schema(conn: &Connection) -> Result<()> {
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS instances (
                id TEXT PRIMARY KEY NOT NULL,
                name TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'stopped',
                port INTEGER NOT NULL,
                config_path TEXT NOT NULL,
                workspace_dir TEXT,
                archived_at TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
            CREATE INDEX IF NOT EXISTS idx_instances_name ON instances(name);",
        )
        .context("Failed to initialize registry schema")?;

        // Migration: add migration_run_id column if missing (pre-phase5 DBs lack it).
        let has_column = conn
            .prepare("PRAGMA table_info(instances)")?
            .query_map([], |row| row.get::<_, String>(1))?
            .filter_map(|r| r.ok())
            .any(|col| col == "migration_run_id");

        if !has_column {
            conn.execute_batch("ALTER TABLE instances ADD COLUMN migration_run_id TEXT;")?;
        }

        conn.execute_batch(
            "CREATE INDEX IF NOT EXISTS idx_instances_migration_run_id ON instances(migration_run_id);",
        )?;

        // Migration: add pid column if missing (pre-phase7 DBs lack it).
        let has_pid_column = conn
            .prepare("PRAGMA table_info(instances)")?
            .query_map([], |row| row.get::<_, String>(1))?
            .filter_map(|r| r.ok())
            .any(|col| col == "pid");

        if !has_pid_column {
            conn.execute_batch("ALTER TABLE instances ADD COLUMN pid INTEGER;")?;
        }

        // Phase 7.5: unique active-name index (prevents duplicate active names)
        let dupes: Vec<(String, i64)> = conn
            .prepare("SELECT name, COUNT(*) as cnt FROM instances WHERE archived_at IS NULL GROUP BY name HAVING cnt > 1")?
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
            .filter_map(|r| r.ok())
            .collect();

        if !dupes.is_empty() {
            let names: Vec<&str> = dupes.iter().map(|(n, _)| n.as_str()).collect();
            anyhow::bail!(
                "Cannot create unique active-name index: duplicate active instance names found: {:?}. \
                 Resolve manually by archiving or renaming duplicates, then restart. \
                 SQL to inspect: SELECT id, name, status FROM instances WHERE name IN ({}) AND archived_at IS NULL",
                names,
                names.iter().map(|n| format!("'{n}'")).collect::<Vec<_>>().join(", ")
            );
        }

        conn.execute_batch(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_instances_active_name
             ON instances(name) WHERE archived_at IS NULL;",
        )?;

        // Phase 7.5: agent_events table
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS agent_events (
                id TEXT PRIMARY KEY NOT NULL,
                instance_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                channel TEXT,
                summary TEXT,
                status TEXT NOT NULL DEFAULT 'completed',
                duration_ms INTEGER,
                correlation_id TEXT,
                metadata TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                FOREIGN KEY (instance_id) REFERENCES instances(id)
            );
            CREATE INDEX IF NOT EXISTS idx_agent_events_instance_created
                ON agent_events(instance_id, created_at DESC);",
        )?;

        // Phase 7.5: agent_usage table
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS agent_usage (
                id TEXT PRIMARY KEY NOT NULL,
                instance_id TEXT NOT NULL,
                input_tokens INTEGER,
                output_tokens INTEGER,
                total_tokens INTEGER,
                provider TEXT,
                model TEXT,
                request_id TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                FOREIGN KEY (instance_id) REFERENCES instances(id)
            );
            CREATE INDEX IF NOT EXISTS idx_agent_usage_instance_created
                ON agent_usage(instance_id, created_at DESC);",
        )?;

        // Phase 10.1: routing_rules table
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS routing_rules (
                id TEXT PRIMARY KEY NOT NULL,
                from_instance TEXT NOT NULL,
                to_instance TEXT NOT NULL,
                type_pattern TEXT NOT NULL,
                max_retries INTEGER NOT NULL DEFAULT 5,
                ttl_secs INTEGER NOT NULL DEFAULT 3600,
                auto_start INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now'))
            );
            CREATE UNIQUE INDEX IF NOT EXISTS idx_routing_rules_tuple
                ON routing_rules(from_instance, to_instance, type_pattern);",
        )?;

        // Phase 10.1: messages table
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS messages (
                id TEXT PRIMARY KEY NOT NULL,
                from_instance TEXT NOT NULL,
                to_instance TEXT NOT NULL,
                message_type TEXT NOT NULL,
                payload TEXT NOT NULL,
                correlation_id TEXT,
                idempotency_key TEXT,
                hop_count INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'queued',
                retry_count INTEGER NOT NULL DEFAULT 0,
                max_retries INTEGER NOT NULL DEFAULT 5,
                next_attempt_at TEXT,
                lease_expires_at TEXT,
                expires_at TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now')),
                updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now'))
            );
            CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_idempotency
                ON messages(idempotency_key) WHERE idempotency_key IS NOT NULL;
            CREATE INDEX IF NOT EXISTS idx_messages_to_status
                ON messages(to_instance, status);
            CREATE INDEX IF NOT EXISTS idx_messages_status_lease
                ON messages(status, lease_expires_at);
            CREATE INDEX IF NOT EXISTS idx_messages_correlation
                ON messages(correlation_id) WHERE correlation_id IS NOT NULL;",
        )?;

        // Phase 10.1: message_events table (append-only audit log)
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS message_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message_id TEXT NOT NULL REFERENCES messages(id),
                event_type TEXT NOT NULL,
                detail TEXT,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now'))
            );
            CREATE INDEX IF NOT EXISTS idx_message_events_msg
                ON message_events(message_id);",
        )?;

        // Phase 10.2: additional indexes for observability queries
        conn.execute_batch(
            "CREATE INDEX IF NOT EXISTS idx_messages_from_status
                ON messages(from_instance, status);
            CREATE INDEX IF NOT EXISTS idx_messages_created_at
                ON messages(created_at);
            CREATE INDEX IF NOT EXISTS idx_messages_status
                ON messages(status);
            CREATE INDEX IF NOT EXISTS idx_message_events_msg_created
                ON message_events(message_id, created_at);",
        )?;

        // Phase 10.2: append-only triggers for message_events
        conn.execute_batch(
            "CREATE TRIGGER IF NOT EXISTS prevent_message_events_update
            BEFORE UPDATE ON message_events
            BEGIN
                SELECT RAISE(ABORT, 'message_events is append-only: UPDATE not allowed');
            END;

            CREATE TRIGGER IF NOT EXISTS prevent_message_events_delete
            BEFORE DELETE ON message_events
            BEGIN
                SELECT RAISE(ABORT, 'message_events is append-only: DELETE not allowed');
            END;",
        )?;

        Ok(())
    }

    /// Create a new instance in the registry.
    pub fn create_instance(
        &self,
        id: &str,
        name: &str,
        port: u16,
        config_path: &str,
        workspace_dir: Option<&str>,
        migration_run_id: Option<&str>,
    ) -> Result<()> {
        self.conn.execute(
            "INSERT INTO instances (id, name, status, port, config_path, workspace_dir, migration_run_id)
             VALUES (?1, ?2, 'stopped', ?3, ?4, ?5, ?6)",
            params![id, name, port as i64, config_path, workspace_dir, migration_run_id],
        ).with_context(|| format!("Failed to create instance '{name}'"))?;
        Ok(())
    }

    /// Get an instance by ID.
    pub fn get_instance(&self, id: &str) -> Result<Option<Instance>> {
        self.conn
            .query_row(
                "SELECT id, name, status, port, config_path, workspace_dir, archived_at, migration_run_id, pid
                 FROM instances WHERE id = ?1",
                params![id],
                |row| {
                    Ok(Instance {
                        id: row.get(0)?,
                        name: row.get(1)?,
                        status: row.get(2)?,
                        port: row.get::<_, i64>(3)? as u16,
                        config_path: row.get(4)?,
                        workspace_dir: row.get(5)?,
                        archived_at: row.get(6)?,
                        migration_run_id: row.get(7)?,
                        pid: row.get::<_, Option<i64>>(8)?.map(|p| p as u32),
                    })
                },
            )
            .optional()
            .context("Failed to query instance by ID")
    }

    /// Get a non-archived instance by name.
    pub fn get_instance_by_name(&self, name: &str) -> Result<Option<Instance>> {
        self.conn
            .query_row(
                "SELECT id, name, status, port, config_path, workspace_dir, archived_at, migration_run_id, pid
                 FROM instances WHERE name = ?1 AND archived_at IS NULL",
                params![name],
                |row| {
                    Ok(Instance {
                        id: row.get(0)?,
                        name: row.get(1)?,
                        status: row.get(2)?,
                        port: row.get::<_, i64>(3)? as u16,
                        config_path: row.get(4)?,
                        workspace_dir: row.get(5)?,
                        archived_at: row.get(6)?,
                        migration_run_id: row.get(7)?,
                        pid: row.get::<_, Option<i64>>(8)?.map(|p| p as u32),
                    })
                },
            )
            .optional()
            .context("Failed to query instance by name")
    }

    /// Find an archived instance by name.
    pub fn find_archived_instance_by_name(&self, name: &str) -> Result<Option<Instance>> {
        self.conn
            .query_row(
                "SELECT id, name, status, port, config_path, workspace_dir, archived_at, migration_run_id, pid
                 FROM instances WHERE name = ?1 AND archived_at IS NOT NULL",
                params![name],
                |row| {
                    Ok(Instance {
                        id: row.get(0)?,
                        name: row.get(1)?,
                        status: row.get(2)?,
                        port: row.get::<_, i64>(3)? as u16,
                        config_path: row.get(4)?,
                        workspace_dir: row.get(5)?,
                        archived_at: row.get(6)?,
                        migration_run_id: row.get(7)?,
                        pid: row.get::<_, Option<i64>>(8)?.map(|p| p as u32),
                    })
                },
            )
            .optional()
            .context("Failed to query archived instance by name")
    }

    /// Delete an instance only if its migration_run_id matches.
    /// Returns true if a row was deleted, false if no match.
    pub fn delete_instance_if_migration(&self, id: &str, run_id: &str) -> Result<bool> {
        let rows = self
            .conn
            .execute(
                "DELETE FROM instances WHERE id = ?1 AND migration_run_id = ?2",
                params![id, run_id],
            )
            .context("Failed to delete migration instance")?;
        Ok(rows > 0)
    }

    /// Allocate the next available port in [start, end], skipping ports already
    /// in the DB and any in the excludes list. Linear scan, deterministic.
    /// Returns None if no port is available.
    pub fn allocate_port_with_excludes(
        &self,
        start: u16,
        end: u16,
        excludes: &[u16],
    ) -> Result<Option<u16>> {
        let mut stmt = self
            .conn
            .prepare("SELECT port FROM instances WHERE archived_at IS NULL")?;
        let used: std::collections::HashSet<u16> = stmt
            .query_map([], |row| Ok(row.get::<_, i64>(0)? as u16))?
            .filter_map(|r| r.ok())
            .collect();

        for port in start..=end {
            if !used.contains(&port) && !excludes.contains(&port) {
                return Ok(Some(port));
            }
        }
        Ok(None)
    }

    /// Update the status of an instance by ID.
    pub fn update_status(&self, id: &str, status: &str) -> Result<()> {
        let rows = self
            .conn
            .execute(
                "UPDATE instances SET status = ?1 WHERE id = ?2",
                params![status, id],
            )
            .context("Failed to update instance status")?;
        if rows == 0 {
            anyhow::bail!("No instance with id '{id}'");
        }
        Ok(())
    }

    /// Update the cached PID for an instance (best-effort cache; pidfile is authoritative).
    pub fn update_pid(&self, id: &str, pid: Option<u32>) -> Result<()> {
        let rows = self
            .conn
            .execute(
                "UPDATE instances SET pid = ?1 WHERE id = ?2",
                params![pid.map(|p| p as i64), id],
            )
            .context("Failed to update instance PID")?;
        if rows == 0 {
            anyhow::bail!("No instance with id '{id}'");
        }
        Ok(())
    }

    /// Borrow the underlying connection (for rollback operations).
    pub fn conn(&self) -> &Connection {
        &self.conn
    }

    // ── Agent events (Phase 7.5) ──────────────────────────────────

    /// Insert an agent event record.
    pub fn insert_agent_event(&self, event: &AgentEvent) -> Result<()> {
        self.conn.execute(
            "INSERT INTO agent_events (id, instance_id, event_type, channel, summary, status, duration_ms, correlation_id, metadata, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                event.id,
                event.instance_id,
                event.event_type,
                event.channel,
                event.summary,
                event.status,
                event.duration_ms,
                event.correlation_id,
                event.metadata,
                event.created_at,
            ],
        ).context("Failed to insert agent event")?;
        Ok(())
    }

    /// List agent events for an instance with pagination and filtering.
    /// Returns (events, total_count).
    pub fn list_agent_events(
        &self,
        instance_id: &str,
        limit: usize,
        offset: usize,
        status_filter: Option<&str>,
        after: Option<&str>,
        before: Option<&str>,
    ) -> Result<(Vec<AgentEvent>, usize)> {
        let mut where_clauses = vec!["instance_id = ?1".to_string()];
        let mut param_idx = 2u32;
        let mut bind_values: Vec<Box<dyn rusqlite::types::ToSql>> =
            vec![Box::new(instance_id.to_string())];

        if let Some(status) = status_filter {
            where_clauses.push(format!("status = ?{param_idx}"));
            bind_values.push(Box::new(status.to_string()));
            param_idx += 1;
        }
        if let Some(after_ts) = after {
            where_clauses.push(format!("created_at > ?{param_idx}"));
            bind_values.push(Box::new(after_ts.to_string()));
            param_idx += 1;
        }
        if let Some(before_ts) = before {
            where_clauses.push(format!("created_at < ?{param_idx}"));
            bind_values.push(Box::new(before_ts.to_string()));
            param_idx += 1;
        }

        let where_sql = where_clauses.join(" AND ");

        // Count total
        let count_sql = format!("SELECT COUNT(*) FROM agent_events WHERE {where_sql}");
        let params_ref: Vec<&dyn rusqlite::types::ToSql> =
            bind_values.iter().map(|b| b.as_ref()).collect();
        let total: usize = self
            .conn
            .query_row(&count_sql, params_ref.as_slice(), |row| {
                row.get::<_, i64>(0).map(|v| v as usize)
            })?;

        // Query with pagination
        let query_sql = format!(
            "SELECT id, instance_id, event_type, channel, summary, status, duration_ms, correlation_id, metadata, created_at \
             FROM agent_events WHERE {where_sql} \
             ORDER BY created_at DESC, id DESC \
             LIMIT ?{param_idx} OFFSET ?{}",
            param_idx + 1
        );
        let mut all_params: Vec<Box<dyn rusqlite::types::ToSql>> = bind_values;
        all_params.push(Box::new(limit as i64));
        all_params.push(Box::new(offset as i64));

        let params_ref: Vec<&dyn rusqlite::types::ToSql> =
            all_params.iter().map(|b| b.as_ref()).collect();
        let mut stmt = self.conn.prepare(&query_sql)?;
        let rows = stmt.query_map(params_ref.as_slice(), |row| {
            Ok(AgentEvent {
                id: row.get(0)?,
                instance_id: row.get(1)?,
                event_type: row.get(2)?,
                channel: row.get(3)?,
                summary: row.get(4)?,
                status: row.get(5)?,
                duration_ms: row.get(6)?,
                correlation_id: row.get(7)?,
                metadata: row.get(8)?,
                created_at: row.get(9)?,
            })
        })?;

        let mut events = Vec::new();
        for row in rows {
            events.push(row?);
        }
        Ok((events, total))
    }

    // ── Agent usage (Phase 7.5) ─────────────────────────────────

    /// Insert an agent usage record.
    pub fn insert_agent_usage(&self, usage: &AgentUsageRecord) -> Result<()> {
        self.conn.execute(
            "INSERT INTO agent_usage (id, instance_id, input_tokens, output_tokens, total_tokens, provider, model, request_id, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                usage.id,
                usage.instance_id,
                usage.input_tokens,
                usage.output_tokens,
                usage.total_tokens,
                usage.provider,
                usage.model,
                usage.request_id,
                usage.created_at,
            ],
        ).context("Failed to insert agent usage")?;
        Ok(())
    }

    /// Get aggregated usage for an instance within a time window.
    pub fn get_agent_usage(
        &self,
        instance_id: &str,
        window_start: Option<&str>,
        window_end: Option<&str>,
    ) -> Result<AgentUsageSummary> {
        let mut where_clauses = vec!["instance_id = ?1".to_string()];
        let mut bind_values: Vec<Box<dyn rusqlite::types::ToSql>> =
            vec![Box::new(instance_id.to_string())];
        let mut param_idx = 2u32;

        if let Some(start) = window_start {
            where_clauses.push(format!("created_at >= ?{param_idx}"));
            bind_values.push(Box::new(start.to_string()));
            param_idx += 1;
        }
        if let Some(end) = window_end {
            where_clauses.push(format!("created_at <= ?{param_idx}"));
            bind_values.push(Box::new(end.to_string()));
        }

        let where_sql = where_clauses.join(" AND ");
        let sql = format!(
            "SELECT \
                SUM(input_tokens), \
                SUM(output_tokens), \
                SUM(total_tokens), \
                COUNT(*), \
                SUM(CASE WHEN total_tokens IS NULL THEN 1 ELSE 0 END) \
             FROM agent_usage WHERE {where_sql}"
        );

        let params_ref: Vec<&dyn rusqlite::types::ToSql> =
            bind_values.iter().map(|b| b.as_ref()).collect();
        self.conn
            .query_row(&sql, params_ref.as_slice(), |row| {
                Ok(AgentUsageSummary {
                    input_tokens: row.get(0)?,
                    output_tokens: row.get(1)?,
                    total_tokens: row.get(2)?,
                    request_count: row.get::<_, Option<i64>>(3)?.unwrap_or(0) as usize,
                    unknown_count: row.get::<_, Option<i64>>(4)?.unwrap_or(0) as usize,
                })
            })
            .context("Failed to query agent usage")
    }

    // ── Messaging (Phase 10.1) ─────────────────────────────────

    /// Create a routing rule. Returns the generated rule ID.
    pub fn create_routing_rule(
        &self,
        from: &str,
        to: &str,
        type_pattern: &str,
        max_retries: i64,
        ttl_secs: i64,
        auto_start: bool,
    ) -> Result<String> {
        let id = uuid::Uuid::new_v4().to_string();
        self.conn.execute(
            "INSERT INTO routing_rules (id, from_instance, to_instance, type_pattern, max_retries, ttl_secs, auto_start)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![id, from, to, type_pattern, max_retries, ttl_secs, auto_start as i64],
        ).context("Failed to create routing rule")?;
        Ok(id)
    }

    /// List all routing rules.
    pub fn list_routing_rules(&self) -> Result<Vec<RoutingRule>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, from_instance, to_instance, type_pattern, max_retries, ttl_secs, auto_start, created_at
             FROM routing_rules ORDER BY created_at",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(RoutingRule {
                id: row.get(0)?,
                from_instance: row.get(1)?,
                to_instance: row.get(2)?,
                type_pattern: row.get(3)?,
                max_retries: row.get(4)?,
                ttl_secs: row.get(5)?,
                auto_start: row.get::<_, i64>(6)? != 0,
                created_at: row.get(7)?,
            })
        })?;
        let mut rules = Vec::new();
        for row in rows {
            rules.push(row?);
        }
        Ok(rules)
    }

    /// Delete a routing rule by ID. Returns true if a row was deleted.
    pub fn delete_routing_rule(&self, id: &str) -> Result<bool> {
        let rows = self
            .conn
            .execute("DELETE FROM routing_rules WHERE id = ?1", params![id])
            .context("Failed to delete routing rule")?;
        Ok(rows > 0)
    }

    /// Check if a route is allowed. Matches exact from/to and prefix match on type.
    /// Returns the matching rule if found.
    pub fn check_route_allowed(
        &self,
        from: &str,
        to: &str,
        message_type: &str,
    ) -> Result<Option<RoutingRule>> {
        // Fetch all rules matching from/to, then check type_pattern in Rust
        // (prefix match: "task.*" matches "task.handoff", "*" matches everything)
        let mut stmt = self.conn.prepare(
            "SELECT id, from_instance, to_instance, type_pattern, max_retries, ttl_secs, auto_start, created_at
             FROM routing_rules WHERE from_instance = ?1 AND to_instance = ?2",
        )?;
        let rows = stmt.query_map(params![from, to], |row| {
            Ok(RoutingRule {
                id: row.get(0)?,
                from_instance: row.get(1)?,
                to_instance: row.get(2)?,
                type_pattern: row.get(3)?,
                max_retries: row.get(4)?,
                ttl_secs: row.get(5)?,
                auto_start: row.get::<_, i64>(6)? != 0,
                created_at: row.get(7)?,
            })
        })?;

        for row in rows {
            let rule = row?;
            if type_pattern_matches(&rule.type_pattern, message_type) {
                return Ok(Some(rule));
            }
        }
        Ok(None)
    }

    /// Check if an idempotency key already exists. Returns the existing message ID if so.
    pub fn check_idempotency_key(&self, key: &str) -> Result<Option<String>> {
        self.conn
            .query_row(
                "SELECT id FROM messages WHERE idempotency_key = ?1",
                params![key],
                |row| row.get(0),
            )
            .optional()
            .context("Failed to check idempotency key")
    }

    /// Enqueue a new message. Returns the created Message.
    pub fn enqueue_message(&self, msg: &NewMessage) -> Result<Message> {
        let now = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
        let expires_at = (chrono::Utc::now() + chrono::Duration::seconds(msg.ttl_secs))
            .format("%Y-%m-%d %H:%M:%S")
            .to_string();

        self.conn.execute(
            "INSERT INTO messages (id, from_instance, to_instance, message_type, payload, correlation_id, idempotency_key, hop_count, status, retry_count, max_retries, expires_at, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'queued', 0, ?9, ?10, ?11, ?12)",
            params![
                msg.id,
                msg.from_instance,
                msg.to_instance,
                msg.message_type,
                msg.payload,
                msg.correlation_id,
                msg.idempotency_key,
                msg.hop_count,
                msg.max_retries,
                expires_at,
                now,
                now,
            ],
        ).context("Failed to enqueue message")?;

        // Fetch back the full row
        self.get_message(&msg.id)?
            .ok_or_else(|| anyhow::anyhow!("Message {} not found after insert", msg.id))
    }

    /// Get a message by ID.
    pub fn get_message(&self, id: &str) -> Result<Option<Message>> {
        self.conn
            .query_row(
                "SELECT id, from_instance, to_instance, message_type, payload, correlation_id, idempotency_key, hop_count, status, retry_count, max_retries, next_attempt_at, lease_expires_at, expires_at, created_at, updated_at
                 FROM messages WHERE id = ?1",
                params![id],
                Self::row_to_message,
            )
            .optional()
            .context("Failed to query message")
    }

    /// Atomically lease the oldest queued message for an instance.
    /// Sets status to 'leased' and lease_expires_at to now + 90s.
    pub fn lease_pending_message(&self, to_instance: &str) -> Result<Option<Message>> {
        let now = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
        let lease_expires = (chrono::Utc::now() + chrono::Duration::seconds(90))
            .format("%Y-%m-%d %H:%M:%S")
            .to_string();

        // Find oldest queued message where next_attempt_at has passed (or is null)
        let msg_id: Option<String> = self
            .conn
            .query_row(
                "SELECT id FROM messages
                 WHERE to_instance = ?1 AND status = 'queued'
                 AND (next_attempt_at IS NULL OR next_attempt_at <= ?2)
                 ORDER BY created_at ASC LIMIT 1",
                params![to_instance, now],
                |row| row.get(0),
            )
            .optional()
            .context("Failed to query pending message")?;

        let msg_id = match msg_id {
            Some(id) => id,
            None => return Ok(None),
        };

        // Transition to leased
        self.conn.execute(
            "UPDATE messages SET status = 'leased', lease_expires_at = ?1, updated_at = ?2 WHERE id = ?3",
            params![lease_expires, now, msg_id],
        )?;

        self.get_message(&msg_id)
    }

    /// Acknowledge a message (mark as acknowledged).
    pub fn acknowledge_message(&self, id: &str) -> Result<bool> {
        let now = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
        let rows = self.conn.execute(
            "UPDATE messages SET status = 'acknowledged', updated_at = ?1 WHERE id = ?2 AND status = 'leased'",
            params![now, id],
        )?;
        Ok(rows > 0)
    }

    /// Get messages with expired leases (leased + lease_expires_at < now).
    pub fn get_expired_leases(&self) -> Result<Vec<Message>> {
        let now = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
        let mut stmt = self.conn.prepare(
            "SELECT id, from_instance, to_instance, message_type, payload, correlation_id, idempotency_key, hop_count, status, retry_count, max_retries, next_attempt_at, lease_expires_at, expires_at, created_at, updated_at
             FROM messages WHERE status = 'leased' AND lease_expires_at < ?1",
        )?;
        let rows = stmt.query_map(params![now], Self::row_to_message)?;
        let mut msgs = Vec::new();
        for row in rows {
            msgs.push(row?);
        }
        Ok(msgs)
    }

    /// Get messages with expired TTL (queued/leased + expires_at < now).
    pub fn get_ttl_expired_messages(&self) -> Result<Vec<Message>> {
        let now = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
        let mut stmt = self.conn.prepare(
            "SELECT id, from_instance, to_instance, message_type, payload, correlation_id, idempotency_key, hop_count, status, retry_count, max_retries, next_attempt_at, lease_expires_at, expires_at, created_at, updated_at
             FROM messages WHERE status IN ('queued', 'leased') AND expires_at < ?1",
        )?;
        let rows = stmt.query_map(params![now], Self::row_to_message)?;
        let mut msgs = Vec::new();
        for row in rows {
            msgs.push(row?);
        }
        Ok(msgs)
    }

    /// Retry a message: increment retry_count, set backoff, return to queued.
    pub fn retry_message(&self, id: &str) -> Result<()> {
        let now = chrono::Utc::now();
        let now_str = now.format("%Y-%m-%d %H:%M:%S").to_string();

        // Get current retry_count
        let retry_count: i64 = self
            .conn
            .query_row(
                "SELECT retry_count FROM messages WHERE id = ?1",
                params![id],
                |row| row.get(0),
            )
            .context("Failed to get retry count")?;

        let new_retry = retry_count + 1;
        // Backoff: min(1s * 2^retry_count, 60s) + jitter(0..500ms)
        let base_secs = (1i64 << retry_count.min(6)).min(60);
        let jitter_ms = (uuid::Uuid::new_v4().as_bytes()[0] as i64 % 500).abs();
        let delay = chrono::Duration::milliseconds(base_secs * 1000 + jitter_ms);
        let next_attempt = (now + delay).format("%Y-%m-%d %H:%M:%S").to_string();

        self.conn.execute(
            "UPDATE messages SET status = 'queued', retry_count = ?1, next_attempt_at = ?2, lease_expires_at = NULL, updated_at = ?3 WHERE id = ?4",
            params![new_retry, next_attempt, now_str, id],
        )?;
        Ok(())
    }

    /// Move a message to dead_letter status.
    pub fn dead_letter_message(&self, id: &str, reason: &str) -> Result<()> {
        let now = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
        self.conn.execute(
            "UPDATE messages SET status = 'dead_letter', updated_at = ?1 WHERE id = ?2",
            params![now, id],
        )?;
        self.append_message_event(id, "dead_lettered", Some(reason))?;
        Ok(())
    }

    /// Append an audit event for a message.
    pub fn append_message_event(
        &self,
        message_id: &str,
        event_type: &str,
        detail: Option<&str>,
    ) -> Result<()> {
        self.conn
            .execute(
                "INSERT INTO message_events (message_id, event_type, detail) VALUES (?1, ?2, ?3)",
                params![message_id, event_type, detail],
            )
            .context("Failed to insert message event")?;
        Ok(())
    }

    /// Get queued messages where recipient is stopped and routing rule has auto_start.
    pub fn get_instances_needing_autostart(&self) -> Result<Vec<(Message, String)>> {
        let mut stmt = self.conn.prepare(
            "SELECT m.id, m.from_instance, m.to_instance, m.message_type, m.payload, m.correlation_id, m.idempotency_key, m.hop_count, m.status, m.retry_count, m.max_retries, m.next_attempt_at, m.lease_expires_at, m.expires_at, m.created_at, m.updated_at, r.to_instance
             FROM messages m
             JOIN routing_rules r ON m.from_instance = r.from_instance AND m.to_instance = r.to_instance AND r.auto_start = 1
             JOIN instances i ON i.name = m.to_instance AND i.archived_at IS NULL AND i.status = 'stopped'
             WHERE m.status = 'queued'
             GROUP BY m.to_instance",
        )?;
        let rows = stmt.query_map([], |row| {
            let msg = Message {
                id: row.get(0)?,
                from_instance: row.get(1)?,
                to_instance: row.get(2)?,
                message_type: row.get(3)?,
                payload: row.get(4)?,
                correlation_id: row.get(5)?,
                idempotency_key: row.get(6)?,
                hop_count: row.get(7)?,
                status: row.get(8)?,
                retry_count: row.get(9)?,
                max_retries: row.get(10)?,
                next_attempt_at: row.get(11)?,
                lease_expires_at: row.get(12)?,
                expires_at: row.get(13)?,
                created_at: row.get(14)?,
                updated_at: row.get(15)?,
            };
            let instance_name: String = row.get(16)?;
            Ok((msg, instance_name))
        })?;
        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    // ── Messaging observability (Phase 10.2) ──────────────────

    /// Get all events for a message, ordered chronologically.
    pub fn get_message_events(&self, message_id: &str) -> Result<Vec<MessageEvent>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, message_id, event_type, detail, created_at
             FROM message_events WHERE message_id = ?1
             ORDER BY created_at ASC, id ASC",
        )?;
        let rows = stmt.query_map(params![message_id], |row| {
            Ok(MessageEvent {
                id: row.get(0)?,
                message_id: row.get(1)?,
                event_type: row.get(2)?,
                detail: row.get(3)?,
                created_at: row.get(4)?,
            })
        })?;
        let mut events = Vec::new();
        for row in rows {
            events.push(row?);
        }
        Ok(events)
    }

    /// List messages with dynamic filters. Returns (messages, total_count).
    pub fn list_messages(&self, filters: &MessageFilters) -> Result<(Vec<Message>, usize)> {
        let mut where_clauses: Vec<String> = Vec::new();
        let mut bind_values: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();
        let mut param_idx = 1u32;

        if let Some(ref cid) = filters.correlation_id {
            where_clauses.push(format!("correlation_id = ?{param_idx}"));
            bind_values.push(Box::new(cid.clone()));
            param_idx += 1;
        }
        if let Some(ref from) = filters.from_instance {
            where_clauses.push(format!("from_instance = ?{param_idx}"));
            bind_values.push(Box::new(from.clone()));
            param_idx += 1;
        }
        if let Some(ref to) = filters.to_instance {
            where_clauses.push(format!("to_instance = ?{param_idx}"));
            bind_values.push(Box::new(to.clone()));
            param_idx += 1;
        }
        if let Some(ref mt) = filters.message_type {
            where_clauses.push(format!("message_type = ?{param_idx}"));
            bind_values.push(Box::new(mt.clone()));
            param_idx += 1;
        }
        if let Some(ref status) = filters.status {
            where_clauses.push(format!("status = ?{param_idx}"));
            bind_values.push(Box::new(status.clone()));
            param_idx += 1;
        }
        if let Some(ref after) = filters.after {
            where_clauses.push(format!("created_at > ?{param_idx}"));
            bind_values.push(Box::new(after.clone()));
            param_idx += 1;
        }
        if let Some(ref before) = filters.before {
            where_clauses.push(format!("created_at < ?{param_idx}"));
            bind_values.push(Box::new(before.clone()));
            param_idx += 1;
        }

        let where_sql = if where_clauses.is_empty() {
            "1=1".to_string()
        } else {
            where_clauses.join(" AND ")
        };

        // Count total
        let count_sql = format!("SELECT COUNT(*) FROM messages WHERE {where_sql}");
        let params_ref: Vec<&dyn rusqlite::types::ToSql> =
            bind_values.iter().map(|b| b.as_ref()).collect();
        let total: usize = self
            .conn
            .query_row(&count_sql, params_ref.as_slice(), |row| {
                row.get::<_, i64>(0).map(|v| v as usize)
            })?;

        // Query with pagination
        let query_sql = format!(
            "SELECT id, from_instance, to_instance, message_type, payload, correlation_id, \
             idempotency_key, hop_count, status, retry_count, max_retries, next_attempt_at, \
             lease_expires_at, expires_at, created_at, updated_at \
             FROM messages WHERE {where_sql} \
             ORDER BY created_at DESC, id DESC \
             LIMIT ?{param_idx} OFFSET ?{}",
            param_idx + 1
        );
        let mut all_params: Vec<Box<dyn rusqlite::types::ToSql>> = bind_values;
        all_params.push(Box::new(filters.limit as i64));
        all_params.push(Box::new(filters.offset as i64));

        let params_ref: Vec<&dyn rusqlite::types::ToSql> =
            all_params.iter().map(|b| b.as_ref()).collect();
        let mut stmt = self.conn.prepare(&query_sql)?;
        let rows = stmt.query_map(params_ref.as_slice(), Self::row_to_message)?;
        let mut messages = Vec::new();
        for row in rows {
            messages.push(row?);
        }
        Ok((messages, total))
    }

    /// List messages for a specific instance with direction filter.
    /// Returns (messages, total_count).
    pub fn list_messages_for_instance(
        &self,
        name: &str,
        direction: MessageDirection,
        filters: &MessageFilters,
    ) -> Result<(Vec<Message>, usize)> {
        let mut where_clauses: Vec<String> = Vec::new();
        let mut bind_values: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();
        let mut param_idx = 1u32;

        // Direction clause
        match direction {
            MessageDirection::In => {
                where_clauses.push(format!("to_instance = ?{param_idx}"));
                bind_values.push(Box::new(name.to_string()));
                param_idx += 1;
            }
            MessageDirection::Out => {
                where_clauses.push(format!("from_instance = ?{param_idx}"));
                bind_values.push(Box::new(name.to_string()));
                param_idx += 1;
            }
            MessageDirection::All => {
                where_clauses.push(format!(
                    "(from_instance = ?{param_idx} OR to_instance = ?{})",
                    param_idx + 1
                ));
                bind_values.push(Box::new(name.to_string()));
                bind_values.push(Box::new(name.to_string()));
                param_idx += 2;
            }
        }

        if let Some(ref status) = filters.status {
            where_clauses.push(format!("status = ?{param_idx}"));
            bind_values.push(Box::new(status.clone()));
            param_idx += 1;
        }
        if let Some(ref mt) = filters.message_type {
            where_clauses.push(format!("message_type = ?{param_idx}"));
            bind_values.push(Box::new(mt.clone()));
            param_idx += 1;
        }
        if let Some(ref after) = filters.after {
            where_clauses.push(format!("created_at > ?{param_idx}"));
            bind_values.push(Box::new(after.clone()));
            param_idx += 1;
        }
        if let Some(ref before) = filters.before {
            where_clauses.push(format!("created_at < ?{param_idx}"));
            bind_values.push(Box::new(before.clone()));
            param_idx += 1;
        }

        let where_sql = where_clauses.join(" AND ");

        // Count total
        let count_sql = format!("SELECT COUNT(*) FROM messages WHERE {where_sql}");
        let params_ref: Vec<&dyn rusqlite::types::ToSql> =
            bind_values.iter().map(|b| b.as_ref()).collect();
        let total: usize = self
            .conn
            .query_row(&count_sql, params_ref.as_slice(), |row| {
                row.get::<_, i64>(0).map(|v| v as usize)
            })?;

        // Query with pagination
        let query_sql = format!(
            "SELECT id, from_instance, to_instance, message_type, payload, correlation_id, \
             idempotency_key, hop_count, status, retry_count, max_retries, next_attempt_at, \
             lease_expires_at, expires_at, created_at, updated_at \
             FROM messages WHERE {where_sql} \
             ORDER BY created_at DESC, id DESC \
             LIMIT ?{param_idx} OFFSET ?{}",
            param_idx + 1
        );
        let mut all_params: Vec<Box<dyn rusqlite::types::ToSql>> = bind_values;
        all_params.push(Box::new(filters.limit as i64));
        all_params.push(Box::new(filters.offset as i64));

        let params_ref: Vec<&dyn rusqlite::types::ToSql> =
            all_params.iter().map(|b| b.as_ref()).collect();
        let mut stmt = self.conn.prepare(&query_sql)?;
        let rows = stmt.query_map(params_ref.as_slice(), Self::row_to_message)?;
        let mut messages = Vec::new();
        for row in rows {
            messages.push(row?);
        }
        Ok((messages, total))
    }

    /// List dead-letter messages with optional filters.
    /// Sorts by updated_at DESC (most recently dead-lettered first).
    pub fn list_dead_letter_messages(
        &self,
        filters: &MessageFilters,
    ) -> Result<(Vec<Message>, usize)> {
        let mut where_clauses = vec!["status = 'dead_letter'".to_string()];
        let mut bind_values: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();
        let mut param_idx = 1u32;

        if let Some(ref from) = filters.from_instance {
            where_clauses.push(format!("from_instance = ?{param_idx}"));
            bind_values.push(Box::new(from.clone()));
            param_idx += 1;
        }
        if let Some(ref to) = filters.to_instance {
            where_clauses.push(format!("to_instance = ?{param_idx}"));
            bind_values.push(Box::new(to.clone()));
            param_idx += 1;
        }
        if let Some(ref after) = filters.after {
            where_clauses.push(format!("updated_at > ?{param_idx}"));
            bind_values.push(Box::new(after.clone()));
            param_idx += 1;
        }
        if let Some(ref before) = filters.before {
            where_clauses.push(format!("updated_at < ?{param_idx}"));
            bind_values.push(Box::new(before.clone()));
            param_idx += 1;
        }

        let where_sql = where_clauses.join(" AND ");

        // Count total
        let count_sql = format!("SELECT COUNT(*) FROM messages WHERE {where_sql}");
        let params_ref: Vec<&dyn rusqlite::types::ToSql> =
            bind_values.iter().map(|b| b.as_ref()).collect();
        let total: usize = self
            .conn
            .query_row(&count_sql, params_ref.as_slice(), |row| {
                row.get::<_, i64>(0).map(|v| v as usize)
            })?;

        // Query with pagination
        let query_sql = format!(
            "SELECT id, from_instance, to_instance, message_type, payload, correlation_id, \
             idempotency_key, hop_count, status, retry_count, max_retries, next_attempt_at, \
             lease_expires_at, expires_at, created_at, updated_at \
             FROM messages WHERE {where_sql} \
             ORDER BY updated_at DESC, id DESC \
             LIMIT ?{param_idx} OFFSET ?{}",
            param_idx + 1
        );
        let mut all_params: Vec<Box<dyn rusqlite::types::ToSql>> = bind_values;
        all_params.push(Box::new(filters.limit as i64));
        all_params.push(Box::new(filters.offset as i64));

        let params_ref: Vec<&dyn rusqlite::types::ToSql> =
            all_params.iter().map(|b| b.as_ref()).collect();
        let mut stmt = self.conn.prepare(&query_sql)?;
        let rows = stmt.query_map(params_ref.as_slice(), Self::row_to_message)?;
        let mut messages = Vec::new();
        for row in rows {
            messages.push(row?);
        }
        Ok((messages, total))
    }

    /// Replay a dead-letter message: reset to queued with fresh TTL.
    /// Wrapped in a transaction. Returns the updated message.
    pub fn replay_message(&self, id: &str) -> Result<Message> {
        self.conn
            .execute_batch("BEGIN")
            .context("Failed to begin replay transaction")?;

        let result = (|| -> Result<Message> {
            // Verify message exists and is dead_letter
            let msg = self
                .get_message(id)?
                .ok_or_else(|| anyhow::anyhow!("Message not found: {id}"))?;

            if msg.status != "dead_letter" {
                anyhow::bail!(
                    "Message {id} has status '{}', expected 'dead_letter'",
                    msg.status
                );
            }

            // Compute new TTL from original expires_at - created_at
            let fmt = "%Y-%m-%d %H:%M:%S";
            let ttl_secs = match (
                chrono::NaiveDateTime::parse_from_str(&msg.expires_at, fmt),
                chrono::NaiveDateTime::parse_from_str(&msg.created_at, fmt),
            ) {
                (Ok(exp), Ok(cre)) => {
                    let diff = (exp - cre).num_seconds();
                    diff.max(300).min(86400) // clamp 5m..24h
                }
                _ => 3600, // fallback: 1 hour
            };

            let now = chrono::Utc::now();
            let now_str = now.format(fmt).to_string();
            let new_expires = (now + chrono::Duration::seconds(ttl_secs))
                .format(fmt)
                .to_string();

            // Reset message fields
            self.conn.execute(
                "UPDATE messages SET status = 'queued', retry_count = 0, \
                 next_attempt_at = NULL, lease_expires_at = NULL, \
                 expires_at = ?1, updated_at = ?2 WHERE id = ?3",
                params![new_expires, now_str, id],
            )?;

            // Append replay events
            self.conn.execute(
                "INSERT INTO message_events (message_id, event_type, detail) VALUES (?1, ?2, ?3)",
                params![id, "replayed", "manually replayed via API"],
            )?;
            self.conn.execute(
                "INSERT INTO message_events (message_id, event_type, detail) VALUES (?1, ?2, ?3)",
                params![id, "queued", "status reset to queued after replay"],
            )?;

            Ok(self
                .get_message(id)?
                .ok_or_else(|| anyhow::anyhow!("Message {id} not found after replay"))?)
        })();

        match result {
            Ok(msg) => {
                self.conn
                    .execute_batch("COMMIT")
                    .context("Failed to commit replay transaction")?;
                Ok(msg)
            }
            Err(e) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(e)
            }
        }
    }

    fn row_to_message(row: &rusqlite::Row<'_>) -> rusqlite::Result<Message> {
        Ok(Message {
            id: row.get(0)?,
            from_instance: row.get(1)?,
            to_instance: row.get(2)?,
            message_type: row.get(3)?,
            payload: row.get(4)?,
            correlation_id: row.get(5)?,
            idempotency_key: row.get(6)?,
            hop_count: row.get(7)?,
            status: row.get(8)?,
            retry_count: row.get(9)?,
            max_retries: row.get(10)?,
            next_attempt_at: row.get(11)?,
            lease_expires_at: row.get(12)?,
            expires_at: row.get(13)?,
            created_at: row.get(14)?,
            updated_at: row.get(15)?,
        })
    }

    /// List all non-archived instances.
    pub fn list_instances(&self) -> Result<Vec<Instance>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, name, status, port, config_path, workspace_dir, archived_at, migration_run_id, pid
             FROM instances WHERE archived_at IS NULL ORDER BY name",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(Instance {
                id: row.get(0)?,
                name: row.get(1)?,
                status: row.get(2)?,
                port: row.get::<_, i64>(3)? as u16,
                config_path: row.get(4)?,
                workspace_dir: row.get(5)?,
                archived_at: row.get(6)?,
                migration_run_id: row.get(7)?,
                pid: row.get::<_, Option<i64>>(8)?.map(|p| p as u32),
            })
        })?;
        let mut instances = Vec::new();
        for row in rows {
            instances.push(row?);
        }
        Ok(instances)
    }
}

/// Check if a type pattern matches a message type.
/// "*" matches everything. "task.*" matches "task.handoff". Exact match otherwise.
fn type_pattern_matches(pattern: &str, message_type: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    if let Some(prefix) = pattern.strip_suffix(".*") {
        return message_type == prefix || message_type.starts_with(&format!("{prefix}."));
    }
    pattern == message_type
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn type_pattern_wildcard() {
        assert!(type_pattern_matches("*", "anything"));
        assert!(type_pattern_matches("*", ""));
    }

    #[test]
    fn type_pattern_prefix() {
        assert!(type_pattern_matches("task.*", "task.handoff"));
        assert!(type_pattern_matches("task.*", "task"));
        assert!(!type_pattern_matches("task.*", "other.thing"));
    }

    #[test]
    fn type_pattern_exact() {
        assert!(type_pattern_matches("task.handoff", "task.handoff"));
        assert!(!type_pattern_matches("task.handoff", "task.other"));
    }

    #[test]
    fn create_and_get_instance() {
        let reg = Registry::open_in_memory().unwrap();
        reg.create_instance(
            "id-1",
            "test-agent",
            18801,
            "/tmp/config.toml",
            Some("/tmp/ws"),
            None,
        )
        .unwrap();

        let inst = reg.get_instance("id-1").unwrap().unwrap();
        assert_eq!(inst.name, "test-agent");
        assert_eq!(inst.port, 18801);
        assert_eq!(inst.status, "stopped");
        assert!(inst.migration_run_id.is_none());
    }

    #[test]
    fn get_instance_by_name_excludes_archived() {
        let reg = Registry::open_in_memory().unwrap();
        reg.create_instance("id-1", "agent", 18801, "/tmp/c.toml", None, None)
            .unwrap();
        // Archive it
        reg.conn
            .execute(
                "UPDATE instances SET archived_at = datetime('now') WHERE id = 'id-1'",
                [],
            )
            .unwrap();

        assert!(reg.get_instance_by_name("agent").unwrap().is_none());
        assert!(reg
            .find_archived_instance_by_name("agent")
            .unwrap()
            .is_some());
    }

    #[test]
    fn delete_instance_if_migration_scoped() {
        let reg = Registry::open_in_memory().unwrap();
        reg.create_instance("id-1", "a1", 18801, "/c.toml", None, Some("run-abc"))
            .unwrap();

        // Wrong run_id: should not delete
        assert!(!reg.delete_instance_if_migration("id-1", "run-xyz").unwrap());
        assert!(reg.get_instance("id-1").unwrap().is_some());

        // Correct run_id: should delete
        assert!(reg.delete_instance_if_migration("id-1", "run-abc").unwrap());
        assert!(reg.get_instance("id-1").unwrap().is_none());
    }

    #[test]
    fn allocate_port_skips_used_and_excluded() {
        let reg = Registry::open_in_memory().unwrap();
        reg.create_instance("id-1", "a1", 18801, "/c.toml", None, None)
            .unwrap();

        let port = reg
            .allocate_port_with_excludes(18801, 18810, &[18802])
            .unwrap()
            .unwrap();
        assert_eq!(port, 18803);
    }

    #[test]
    fn allocate_port_returns_none_when_exhausted() {
        let reg = Registry::open_in_memory().unwrap();
        reg.create_instance("id-1", "a1", 18801, "/c.toml", None, None)
            .unwrap();
        reg.create_instance("id-2", "a2", 18802, "/c.toml", None, None)
            .unwrap();

        let port = reg.allocate_port_with_excludes(18801, 18802, &[]).unwrap();
        assert!(port.is_none());
    }

    #[test]
    fn schema_migration_adds_migration_run_id_column() {
        // Simulate a pre-phase5 DB: create table WITHOUT migration_run_id,
        // then open via Registry which should ALTER TABLE to add it.
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("PRAGMA foreign_keys=ON;").unwrap();
        conn.execute_batch(
            "CREATE TABLE instances (
                id TEXT PRIMARY KEY NOT NULL,
                name TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'stopped',
                port INTEGER NOT NULL,
                config_path TEXT NOT NULL,
                workspace_dir TEXT,
                archived_at TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );",
        )
        .unwrap();

        // Insert a row without migration_run_id (old schema)
        conn.execute(
            "INSERT INTO instances (id, name, status, port, config_path)
             VALUES ('old-1', 'legacy-agent', 'running', 18801, '/old/config.toml')",
            [],
        )
        .unwrap();

        // Now run init_schema which should add the column
        Registry::init_schema(&conn).unwrap();

        // Verify: column exists and old row is readable with NULL migration_run_id
        let reg = Registry { conn };
        let inst = reg.get_instance("old-1").unwrap().unwrap();
        assert_eq!(inst.name, "legacy-agent");
        assert_eq!(inst.status, "running");
        assert!(inst.migration_run_id.is_none());

        // Verify: new instances with migration_run_id work
        reg.create_instance(
            "new-1",
            "new-agent",
            18802,
            "/new/config.toml",
            None,
            Some("run-123"),
        )
        .unwrap();
        let new_inst = reg.get_instance("new-1").unwrap().unwrap();
        assert_eq!(new_inst.migration_run_id.as_deref(), Some("run-123"));
    }

    #[test]
    fn update_status_changes_instance_status() {
        let reg = Registry::open_in_memory().unwrap();
        reg.create_instance("id-1", "agent", 18801, "/c.toml", None, None)
            .unwrap();

        assert_eq!(reg.get_instance("id-1").unwrap().unwrap().status, "stopped");

        reg.update_status("id-1", "running").unwrap();
        assert_eq!(reg.get_instance("id-1").unwrap().unwrap().status, "running");

        reg.update_status("id-1", "stopped").unwrap();
        assert_eq!(reg.get_instance("id-1").unwrap().unwrap().status, "stopped");
    }

    #[test]
    fn update_status_errors_on_missing_instance() {
        let reg = Registry::open_in_memory().unwrap();
        let err = reg.update_status("nonexistent", "running").unwrap_err();
        assert!(err.to_string().contains("No instance"));
    }

    #[test]
    fn list_instances_excludes_archived() {
        let reg = Registry::open_in_memory().unwrap();
        reg.create_instance("id-1", "active", 18801, "/c.toml", None, None)
            .unwrap();
        reg.create_instance("id-2", "archived", 18802, "/c.toml", None, None)
            .unwrap();
        reg.conn
            .execute(
                "UPDATE instances SET archived_at = datetime('now') WHERE id = 'id-2'",
                [],
            )
            .unwrap();

        let list = reg.list_instances().unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].name, "active");
    }

    #[test]
    fn update_pid_roundtrip() {
        let reg = Registry::open_in_memory().unwrap();
        reg.create_instance("id-1", "agent", 18801, "/c.toml", None, None)
            .unwrap();

        // Initially None
        assert!(reg.get_instance("id-1").unwrap().unwrap().pid.is_none());

        // Set PID
        reg.update_pid("id-1", Some(12345)).unwrap();
        assert_eq!(reg.get_instance("id-1").unwrap().unwrap().pid, Some(12345));

        // Clear PID
        reg.update_pid("id-1", None).unwrap();
        assert!(reg.get_instance("id-1").unwrap().unwrap().pid.is_none());
    }

    #[test]
    fn update_pid_errors_on_missing_instance() {
        let reg = Registry::open_in_memory().unwrap();
        let err = reg.update_pid("nonexistent", Some(123)).unwrap_err();
        assert!(err.to_string().contains("No instance"));
    }
}
