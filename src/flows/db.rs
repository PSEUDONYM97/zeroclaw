use crate::flows::types::{FlowAuditRow, FlowVersionRow};
use rusqlite::{params, Connection, OpenFlags, TransactionBehavior};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// Row type for active flow instances.
#[derive(Debug, Clone)]
pub struct FlowInstanceRow {
    pub chat_id: String,
    pub flow_name: String,
    pub current_step: String,
    pub started_at: String,
    pub step_entered_at: String,
    pub anchor_message_id: Option<i64>,
    pub status: String,
}

/// Row type for flow history.
#[derive(Debug, Clone)]
pub struct FlowHistoryRow {
    pub id: i64,
    pub chat_id: String,
    pub flow_name: String,
    pub final_step: String,
    pub started_at: String,
    pub completed_at: String,
    pub status: String,
    pub anchor_message_id: Option<i64>,
}

/// SQLite-backed storage for durable flow state.
///
/// Thread safety: wraps `Connection` in `Mutex` (same pattern as `SqliteMemory`).
/// The daemon shares a single `FlowDb` across async tasks via `Arc<FlowDb>`.
/// The CP opens short-lived connections per request.
pub struct FlowDb {
    conn: Mutex<Connection>,
    db_path: PathBuf,
}

impl FlowDb {
    /// Open (or create) the flow state database at the given path.
    pub fn open(path: &Path) -> anyhow::Result<Self> {
        let conn = Connection::open(path)?;
        let db = Self {
            conn: Mutex::new(conn),
            db_path: path.to_path_buf(),
        };
        db.init_schema()?;
        Ok(db)
    }

    /// Open the database in read-only mode (for CP query handlers).
    pub fn open_read_only(path: &Path) -> anyhow::Result<Self> {
        let flags = OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX;
        let conn = Connection::open_with_flags(path, flags)?;
        let db = Self {
            conn: Mutex::new(conn),
            db_path: path.to_path_buf(),
        };
        // Schema should already exist; just set pragmas
        {
            let guard = db.conn.lock().unwrap_or_else(|e| e.into_inner());
            guard.execute_batch("PRAGMA busy_timeout=5000;")?;
        }
        Ok(db)
    }

    /// Create an in-memory database (for tests).
    pub fn open_in_memory() -> anyhow::Result<Self> {
        let conn = Connection::open_in_memory()?;
        let db = Self {
            conn: Mutex::new(conn),
            db_path: PathBuf::from(":memory:"),
        };
        db.init_schema()?;
        Ok(db)
    }

    pub fn db_path(&self) -> &Path {
        &self.db_path
    }

    fn init_schema(&self) -> anyhow::Result<()> {
        let guard = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        guard.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA foreign_keys=ON;
             PRAGMA busy_timeout=5000;

             CREATE TABLE IF NOT EXISTS flow_instances (
                 chat_id          TEXT PRIMARY KEY NOT NULL,
                 flow_name        TEXT NOT NULL,
                 current_step     TEXT NOT NULL,
                 started_at       TEXT NOT NULL,
                 step_entered_at  TEXT NOT NULL,
                 anchor_message_id INTEGER,
                 status           TEXT NOT NULL DEFAULT 'active',
                 created_at       TEXT NOT NULL DEFAULT (datetime('now'))
             );
             CREATE INDEX IF NOT EXISTS idx_flow_instances_status
                 ON flow_instances(status);

             CREATE TABLE IF NOT EXISTS flow_history (
                 id               INTEGER PRIMARY KEY AUTOINCREMENT,
                 chat_id          TEXT NOT NULL,
                 flow_name        TEXT NOT NULL,
                 final_step       TEXT NOT NULL,
                 started_at       TEXT NOT NULL,
                 completed_at     TEXT NOT NULL,
                 status           TEXT NOT NULL,
                 anchor_message_id INTEGER,
                 created_at       TEXT NOT NULL DEFAULT (datetime('now'))
             );
             CREATE INDEX IF NOT EXISTS idx_flow_history_completed
                 ON flow_history(completed_at DESC);
             CREATE INDEX IF NOT EXISTS idx_flow_history_flow_name
                 ON flow_history(flow_name);

             CREATE TABLE IF NOT EXISTS kv_state (
                 key              TEXT PRIMARY KEY NOT NULL,
                 value            TEXT NOT NULL,
                 updated_at       TEXT NOT NULL DEFAULT (datetime('now'))
             );

             -- Phase 17: Flow versioning (agent-authored flows)
             CREATE TABLE IF NOT EXISTS flow_versions (
                 id              INTEGER PRIMARY KEY AUTOINCREMENT,
                 flow_name       TEXT NOT NULL,
                 version         INTEGER NOT NULL,
                 source          TEXT NOT NULL DEFAULT 'operator',
                 status          TEXT NOT NULL DEFAULT 'draft',
                 definition_json TEXT NOT NULL,
                 created_at      TEXT NOT NULL DEFAULT (datetime('now')),
                 created_by      TEXT NOT NULL DEFAULT 'system',
                 review_note     TEXT,
                 UNIQUE(flow_name, version)
             );
             CREATE INDEX IF NOT EXISTS idx_fv_name_status ON flow_versions(flow_name, status);
             CREATE INDEX IF NOT EXISTS idx_fv_status ON flow_versions(status);

             CREATE TABLE IF NOT EXISTS flow_audit_log (
                 id              INTEGER PRIMARY KEY AUTOINCREMENT,
                 flow_name       TEXT NOT NULL,
                 version         INTEGER,
                 event           TEXT NOT NULL,
                 actor           TEXT NOT NULL,
                 detail          TEXT,
                 created_at      TEXT NOT NULL DEFAULT (datetime('now'))
             );
             CREATE INDEX IF NOT EXISTS idx_fal_name ON flow_audit_log(flow_name);
             CREATE INDEX IF NOT EXISTS idx_fal_created ON flow_audit_log(created_at DESC);

             -- Append-only protection: prevent tampering with audit trail
             CREATE TRIGGER IF NOT EXISTS prevent_audit_update
                 BEFORE UPDATE ON flow_audit_log
                 BEGIN SELECT RAISE(ABORT, 'flow_audit_log is append-only'); END;
             CREATE TRIGGER IF NOT EXISTS prevent_audit_delete
                 BEFORE DELETE ON flow_audit_log
                 BEGIN SELECT RAISE(ABORT, 'flow_audit_log is append-only'); END;",
        )?;
        Ok(())
    }

    // ── Active flows ─────────────────────────────────────────────

    /// Insert or replace an active flow instance.
    pub fn upsert_active(&self, row: &FlowInstanceRow) -> anyhow::Result<()> {
        let guard = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        guard.execute(
            "INSERT OR REPLACE INTO flow_instances
                (chat_id, flow_name, current_step, started_at, step_entered_at,
                 anchor_message_id, status)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                row.chat_id,
                row.flow_name,
                row.current_step,
                row.started_at,
                row.step_entered_at,
                row.anchor_message_id,
                row.status,
            ],
        )?;
        Ok(())
    }

    /// Get a single active flow by chat_id.
    pub fn get_active(&self, chat_id: &str) -> anyhow::Result<Option<FlowInstanceRow>> {
        let guard = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let mut stmt = guard.prepare(
            "SELECT chat_id, flow_name, current_step, started_at, step_entered_at,
                    anchor_message_id, status
             FROM flow_instances WHERE chat_id = ?1",
        )?;
        let mut rows = stmt.query_map(params![chat_id], |row| {
            Ok(FlowInstanceRow {
                chat_id: row.get(0)?,
                flow_name: row.get(1)?,
                current_step: row.get(2)?,
                started_at: row.get(3)?,
                step_entered_at: row.get(4)?,
                anchor_message_id: row.get(5)?,
                status: row.get(6)?,
            })
        })?;
        match rows.next() {
            Some(Ok(r)) => Ok(Some(r)),
            Some(Err(e)) => Err(e.into()),
            None => Ok(None),
        }
    }

    /// List all active flow instances.
    pub fn list_active(&self) -> anyhow::Result<Vec<FlowInstanceRow>> {
        let guard = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let mut stmt = guard.prepare(
            "SELECT chat_id, flow_name, current_step, started_at, step_entered_at,
                    anchor_message_id, status
             FROM flow_instances WHERE status = 'active'",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(FlowInstanceRow {
                chat_id: row.get(0)?,
                flow_name: row.get(1)?,
                current_step: row.get(2)?,
                started_at: row.get(3)?,
                step_entered_at: row.get(4)?,
                anchor_message_id: row.get(5)?,
                status: row.get(6)?,
            })
        })?;
        let mut result = Vec::new();
        for r in rows {
            result.push(r?);
        }
        Ok(result)
    }

    /// Update the current step of an active flow. Returns true if a row was updated.
    pub fn update_step(
        &self,
        chat_id: &str,
        step: &str,
        anchor: Option<i64>,
        step_entered_at: &str,
    ) -> anyhow::Result<bool> {
        let guard = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let changed = guard.execute(
            "UPDATE flow_instances
             SET current_step = ?2, anchor_message_id = ?3, step_entered_at = ?4
             WHERE chat_id = ?1 AND status = 'active'",
            params![chat_id, step, anchor, step_entered_at],
        )?;
        Ok(changed > 0)
    }

    /// Complete a flow: move from flow_instances to flow_history.
    /// Returns the removed row (if any).
    pub fn complete_flow(
        &self,
        chat_id: &str,
        status: &str,
    ) -> anyhow::Result<Option<FlowInstanceRow>> {
        let guard = self.conn.lock().unwrap_or_else(|e| e.into_inner());

        // Read the active row first
        let row = {
            let mut stmt = guard.prepare(
                "SELECT chat_id, flow_name, current_step, started_at, step_entered_at,
                        anchor_message_id, status
                 FROM flow_instances WHERE chat_id = ?1",
            )?;
            let mut rows = stmt.query_map(params![chat_id], |row| {
                Ok(FlowInstanceRow {
                    chat_id: row.get(0)?,
                    flow_name: row.get(1)?,
                    current_step: row.get(2)?,
                    started_at: row.get(3)?,
                    step_entered_at: row.get(4)?,
                    anchor_message_id: row.get(5)?,
                    status: row.get(6)?,
                })
            })?;
            match rows.next() {
                Some(Ok(r)) => Some(r),
                _ => None,
            }
        };

        let Some(ref active) = row else {
            return Ok(None);
        };

        // Insert into history + delete from active in one transaction
        guard.execute_batch("BEGIN")?;
        let result = (|| -> anyhow::Result<()> {
            guard.execute(
                "INSERT INTO flow_history
                    (chat_id, flow_name, final_step, started_at, completed_at,
                     status, anchor_message_id)
                 VALUES (?1, ?2, ?3, ?4, datetime('now'), ?5, ?6)",
                params![
                    active.chat_id,
                    active.flow_name,
                    active.current_step,
                    active.started_at,
                    status,
                    active.anchor_message_id,
                ],
            )?;
            guard.execute(
                "DELETE FROM flow_instances WHERE chat_id = ?1",
                params![chat_id],
            )?;
            Ok(())
        })();

        match result {
            Ok(()) => {
                guard.execute_batch("COMMIT")?;
                Ok(row)
            }
            Err(e) => {
                let _ = guard.execute_batch("ROLLBACK");
                Err(e)
            }
        }
    }

    // ── History ──────────────────────────────────────────────────

    /// List flow history with pagination and optional filters.
    /// Returns (rows, total_count).
    pub fn list_history(
        &self,
        limit: usize,
        offset: usize,
        flow_name: Option<&str>,
        status: Option<&str>,
        chat_id: Option<&str>,
    ) -> anyhow::Result<(Vec<FlowHistoryRow>, usize)> {
        let guard = self.conn.lock().unwrap_or_else(|e| e.into_inner());

        let mut where_clauses = Vec::new();
        let mut bind_values: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();

        if let Some(name) = flow_name {
            where_clauses.push("flow_name = ?");
            bind_values.push(Box::new(name.to_string()));
        }
        if let Some(s) = status {
            where_clauses.push("status = ?");
            bind_values.push(Box::new(s.to_string()));
        }
        if let Some(cid) = chat_id {
            where_clauses.push("chat_id = ?");
            bind_values.push(Box::new(cid.to_string()));
        }

        let where_sql = if where_clauses.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", where_clauses.join(" AND "))
        };

        // Count
        let count_sql = format!("SELECT COUNT(*) FROM flow_history {where_sql}");
        let total: usize = {
            let mut stmt = guard.prepare(&count_sql)?;
            let refs: Vec<&dyn rusqlite::types::ToSql> =
                bind_values.iter().map(|b| b.as_ref()).collect();
            stmt.query_row(refs.as_slice(), |row| row.get(0))?
        };

        // Rows
        let query_sql = format!(
            "SELECT id, chat_id, flow_name, final_step, started_at, completed_at,
                    status, anchor_message_id
             FROM flow_history {where_sql}
             ORDER BY completed_at DESC
             LIMIT ?{} OFFSET ?{}",
            bind_values.len() + 1,
            bind_values.len() + 2,
        );
        bind_values.push(Box::new(limit as i64));
        bind_values.push(Box::new(offset as i64));

        let mut stmt = guard.prepare(&query_sql)?;
        let refs: Vec<&dyn rusqlite::types::ToSql> =
            bind_values.iter().map(|b| b.as_ref()).collect();
        let rows = stmt.query_map(refs.as_slice(), |row| {
            Ok(FlowHistoryRow {
                id: row.get(0)?,
                chat_id: row.get(1)?,
                flow_name: row.get(2)?,
                final_step: row.get(3)?,
                started_at: row.get(4)?,
                completed_at: row.get(5)?,
                status: row.get(6)?,
                anchor_message_id: row.get(7)?,
            })
        })?;

        let mut result = Vec::new();
        for r in rows {
            result.push(r?);
        }
        Ok((result, total))
    }

    // ── Key-Value state ──────────────────────────────────────────

    /// Get a value from kv_state.
    pub fn get_kv(&self, key: &str) -> anyhow::Result<Option<String>> {
        let guard = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let mut stmt = guard.prepare("SELECT value FROM kv_state WHERE key = ?1")?;
        let mut rows = stmt.query_map(params![key], |row| row.get(0))?;
        match rows.next() {
            Some(Ok(v)) => Ok(Some(v)),
            Some(Err(e)) => Err(e.into()),
            None => Ok(None),
        }
    }

    /// Set a value in kv_state (upsert).
    pub fn set_kv(&self, key: &str, value: &str) -> anyhow::Result<()> {
        let guard = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        guard.execute(
            "INSERT OR REPLACE INTO kv_state (key, value, updated_at)
             VALUES (?1, ?2, datetime('now'))",
            params![key, value],
        )?;
        Ok(())
    }

    // ── Flow Versions (Phase 17) ────────────────────────────────

    /// Create a new flow version with race-safe auto-increment.
    /// Uses IMMEDIATE transaction to serialize concurrent writers.
    /// Returns the assigned version number.
    pub fn create_flow_version(
        &self,
        flow_name: &str,
        definition_json: &str,
        source: &str,
        created_by: &str,
    ) -> anyhow::Result<i64> {
        let mut guard = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let tx = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;

        let next_version: i64 = tx.query_row(
            "SELECT COALESCE(MAX(version), 0) + 1 FROM flow_versions WHERE flow_name = ?1",
            [flow_name],
            |row| row.get(0),
        )?;

        tx.execute(
            "INSERT INTO flow_versions (flow_name, version, source, status, definition_json, created_by)
             VALUES (?1, ?2, ?3, 'draft', ?4, ?5)",
            params![flow_name, next_version, source, definition_json, created_by],
        )?;

        tx.commit()?;
        Ok(next_version)
    }

    /// Get a specific version of a flow.
    pub fn get_flow_version(
        &self,
        flow_name: &str,
        version: i64,
    ) -> anyhow::Result<Option<FlowVersionRow>> {
        let guard = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let mut stmt = guard.prepare(
            "SELECT id, flow_name, version, source, status, definition_json,
                    created_at, created_by, review_note
             FROM flow_versions WHERE flow_name = ?1 AND version = ?2",
        )?;
        let mut rows = stmt.query_map(params![flow_name, version], Self::map_version_row)?;
        match rows.next() {
            Some(Ok(r)) => Ok(Some(r)),
            Some(Err(e)) => Err(e.into()),
            None => Ok(None),
        }
    }

    /// Get the currently active version of a flow (status = 'active').
    pub fn get_active_version(&self, flow_name: &str) -> anyhow::Result<Option<FlowVersionRow>> {
        let guard = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let mut stmt = guard.prepare(
            "SELECT id, flow_name, version, source, status, definition_json,
                    created_at, created_by, review_note
             FROM flow_versions WHERE flow_name = ?1 AND status = 'active'
             ORDER BY version DESC LIMIT 1",
        )?;
        let mut rows = stmt.query_map(params![flow_name], Self::map_version_row)?;
        match rows.next() {
            Some(Ok(r)) => Ok(Some(r)),
            Some(Err(e)) => Err(e.into()),
            None => Ok(None),
        }
    }

    /// List all versions of a flow, ordered by version descending.
    pub fn list_flow_versions(&self, flow_name: &str) -> anyhow::Result<Vec<FlowVersionRow>> {
        let guard = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let mut stmt = guard.prepare(
            "SELECT id, flow_name, version, source, status, definition_json,
                    created_at, created_by, review_note
             FROM flow_versions WHERE flow_name = ?1
             ORDER BY version DESC",
        )?;
        let rows = stmt.query_map(params![flow_name], Self::map_version_row)?;
        let mut result = Vec::new();
        for r in rows {
            result.push(r?);
        }
        Ok(result)
    }

    /// List all versions with status = 'pending_review'.
    pub fn list_pending_review(&self) -> anyhow::Result<Vec<FlowVersionRow>> {
        let guard = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let mut stmt = guard.prepare(
            "SELECT id, flow_name, version, source, status, definition_json,
                    created_at, created_by, review_note
             FROM flow_versions WHERE status = 'pending_review'
             ORDER BY created_at ASC",
        )?;
        let rows = stmt.query_map([], Self::map_version_row)?;
        let mut result = Vec::new();
        for r in rows {
            result.push(r?);
        }
        Ok(result)
    }

    /// Update the status of a specific flow version. Returns true if a row was updated.
    pub fn update_version_status(
        &self,
        flow_name: &str,
        version: i64,
        new_status: &str,
        review_note: Option<&str>,
    ) -> anyhow::Result<bool> {
        let guard = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let changed = guard.execute(
            "UPDATE flow_versions SET status = ?3, review_note = ?4
             WHERE flow_name = ?1 AND version = ?2",
            params![flow_name, version, new_status, review_note],
        )?;
        Ok(changed > 0)
    }

    /// Activate a specific version: deactivate the current active version (if any),
    /// then set the target version to 'active'. Uses IMMEDIATE transaction.
    pub fn activate_version(&self, flow_name: &str, version: i64) -> anyhow::Result<()> {
        let mut guard = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let tx = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;

        // Deactivate any currently active version
        tx.execute(
            "UPDATE flow_versions SET status = 'deactivated'
             WHERE flow_name = ?1 AND status = 'active'",
            params![flow_name],
        )?;

        // Activate the target version
        let changed = tx.execute(
            "UPDATE flow_versions SET status = 'active'
             WHERE flow_name = ?1 AND version = ?2",
            params![flow_name, version],
        )?;

        if changed == 0 {
            tx.rollback()?;
            anyhow::bail!(
                "flow version {flow_name} v{version} not found; cannot activate"
            );
        }

        tx.commit()?;
        Ok(())
    }

    /// Count distinct flow_names with source = 'agent'.
    pub fn count_agent_flows(&self) -> anyhow::Result<usize> {
        let guard = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let count: usize = guard.query_row(
            "SELECT COUNT(DISTINCT flow_name) FROM flow_versions WHERE source = 'agent'",
            [],
            |row| row.get(0),
        )?;
        Ok(count)
    }

    /// Check if any version exists with source = 'operator' for this flow name.
    pub fn has_operator_versions(&self, flow_name: &str) -> anyhow::Result<bool> {
        let guard = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let count: i64 = guard.query_row(
            "SELECT COUNT(*) FROM flow_versions WHERE flow_name = ?1 AND source = 'operator'",
            params![flow_name],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

    // ── Audit Log (Phase 17) ────────────────────────────────────

    /// Append an entry to the flow audit log.
    pub fn log_audit(
        &self,
        flow_name: &str,
        version: Option<i64>,
        event: &str,
        actor: &str,
        detail: Option<&str>,
    ) -> anyhow::Result<()> {
        let guard = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        guard.execute(
            "INSERT INTO flow_audit_log (flow_name, version, event, actor, detail)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![flow_name, version, event, actor, detail],
        )?;
        Ok(())
    }

    /// List audit log entries with pagination and optional flow_name filter.
    /// Returns (rows, total_count).
    pub fn list_audit_log(
        &self,
        limit: usize,
        offset: usize,
        flow_name_filter: Option<&str>,
    ) -> anyhow::Result<(Vec<FlowAuditRow>, usize)> {
        let guard = self.conn.lock().unwrap_or_else(|e| e.into_inner());

        let (where_sql, bind_name) = if let Some(name) = flow_name_filter {
            ("WHERE flow_name = ?1".to_string(), Some(name.to_string()))
        } else {
            (String::new(), None)
        };

        // Count
        let count_sql = format!("SELECT COUNT(*) FROM flow_audit_log {where_sql}");
        let total: usize = if let Some(ref name) = bind_name {
            let mut stmt = guard.prepare(&count_sql)?;
            stmt.query_row(params![name], |row| row.get(0))?
        } else {
            let mut stmt = guard.prepare(&count_sql)?;
            stmt.query_row([], |row| row.get(0))?
        };

        // Rows
        let query_sql = if bind_name.is_some() {
            format!(
                "SELECT id, flow_name, version, event, actor, detail, created_at
                 FROM flow_audit_log {where_sql}
                 ORDER BY created_at DESC LIMIT ?2 OFFSET ?3"
            )
        } else {
            format!(
                "SELECT id, flow_name, version, event, actor, detail, created_at
                 FROM flow_audit_log {where_sql}
                 ORDER BY created_at DESC LIMIT ?1 OFFSET ?2"
            )
        };

        let mut result = Vec::new();
        if let Some(ref name) = bind_name {
            let mut stmt = guard.prepare(&query_sql)?;
            let rows = stmt.query_map(
                params![name, limit as i64, offset as i64],
                Self::map_audit_row,
            )?;
            for r in rows {
                result.push(r?);
            }
        } else {
            let mut stmt = guard.prepare(&query_sql)?;
            let rows = stmt.query_map(
                params![limit as i64, offset as i64],
                Self::map_audit_row,
            )?;
            for r in rows {
                result.push(r?);
            }
        }

        Ok((result, total))
    }

    // ── Internal row mappers ────────────────────────────────────

    fn map_version_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<FlowVersionRow> {
        Ok(FlowVersionRow {
            id: row.get(0)?,
            flow_name: row.get(1)?,
            version: row.get(2)?,
            source: row.get(3)?,
            status: row.get(4)?,
            definition_json: row.get(5)?,
            created_at: row.get(6)?,
            created_by: row.get(7)?,
            review_note: row.get(8)?,
        })
    }

    fn map_audit_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<FlowAuditRow> {
        Ok(FlowAuditRow {
            id: row.get(0)?,
            flow_name: row.get(1)?,
            version: row.get(2)?,
            event: row.get(3)?,
            actor: row.get(4)?,
            detail: row.get(5)?,
            created_at: row.get(6)?,
        })
    }

    /// Expose the inner connection for direct SQL access.
    ///
    /// Intended for tests and advanced use cases (e.g., verifying trigger behavior).
    pub fn conn(&self) -> std::sync::MutexGuard<'_, Connection> {
        self.conn.lock().unwrap_or_else(|e| e.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flow_db_creates_tables() {
        let db = FlowDb::open_in_memory().unwrap();
        // Should be able to query all three tables
        let guard = db.conn.lock().unwrap();
        let _: i64 = guard
            .query_row("SELECT COUNT(*) FROM flow_instances", [], |r| r.get(0))
            .unwrap();
        let _: i64 = guard
            .query_row("SELECT COUNT(*) FROM flow_history", [], |r| r.get(0))
            .unwrap();
        let _: i64 = guard
            .query_row("SELECT COUNT(*) FROM kv_state", [], |r| r.get(0))
            .unwrap();
    }

    #[test]
    fn flow_db_upsert_and_get() {
        let db = FlowDb::open_in_memory().unwrap();
        let row = FlowInstanceRow {
            chat_id: "chat1".into(),
            flow_name: "greet".into(),
            current_step: "hello".into(),
            started_at: "2026-01-01T00:00:00Z".into(),
            step_entered_at: "2026-01-01T00:00:00Z".into(),
            anchor_message_id: Some(42),
            status: "active".into(),
        };
        db.upsert_active(&row).unwrap();
        let got = db.get_active("chat1").unwrap().unwrap();
        assert_eq!(got.flow_name, "greet");
        assert_eq!(got.anchor_message_id, Some(42));
    }

    #[test]
    fn flow_db_update_step() {
        let db = FlowDb::open_in_memory().unwrap();
        let row = FlowInstanceRow {
            chat_id: "chat1".into(),
            flow_name: "greet".into(),
            current_step: "hello".into(),
            started_at: "2026-01-01T00:00:00Z".into(),
            step_entered_at: "2026-01-01T00:00:00Z".into(),
            anchor_message_id: None,
            status: "active".into(),
        };
        db.upsert_active(&row).unwrap();
        let updated = db
            .update_step("chat1", "done", Some(99), "2026-01-01T00:01:00Z")
            .unwrap();
        assert!(updated);
        let got = db.get_active("chat1").unwrap().unwrap();
        assert_eq!(got.current_step, "done");
        assert_eq!(got.anchor_message_id, Some(99));
    }

    #[test]
    fn flow_db_complete_moves_to_history() {
        let db = FlowDb::open_in_memory().unwrap();
        let row = FlowInstanceRow {
            chat_id: "chat1".into(),
            flow_name: "greet".into(),
            current_step: "done".into(),
            started_at: "2026-01-01T00:00:00Z".into(),
            step_entered_at: "2026-01-01T00:00:00Z".into(),
            anchor_message_id: Some(10),
            status: "active".into(),
        };
        db.upsert_active(&row).unwrap();
        let completed = db.complete_flow("chat1", "completed").unwrap();
        assert!(completed.is_some());

        // Active should be empty
        assert!(db.get_active("chat1").unwrap().is_none());

        // History should have one entry
        let (history, total) = db.list_history(10, 0, None, None, None).unwrap();
        assert_eq!(total, 1);
        assert_eq!(history[0].status, "completed");
        assert_eq!(history[0].final_step, "done");
    }

    #[test]
    fn flow_db_list_active() {
        let db = FlowDb::open_in_memory().unwrap();
        for i in 0..3 {
            let row = FlowInstanceRow {
                chat_id: format!("chat{i}"),
                flow_name: "test".into(),
                current_step: "s1".into(),
                started_at: "2026-01-01T00:00:00Z".into(),
                step_entered_at: "2026-01-01T00:00:00Z".into(),
                anchor_message_id: None,
                status: "active".into(),
            };
            db.upsert_active(&row).unwrap();
        }
        let active = db.list_active().unwrap();
        assert_eq!(active.len(), 3);
    }

    #[test]
    fn flow_db_history_filters() {
        let db = FlowDb::open_in_memory().unwrap();
        // Insert some history directly
        for (name, status) in &[("flow_a", "completed"), ("flow_b", "timed_out"), ("flow_a", "force_completed")] {
            let row = FlowInstanceRow {
                chat_id: format!("chat_{name}_{status}"),
                flow_name: name.to_string(),
                current_step: "end".into(),
                started_at: "2026-01-01T00:00:00Z".into(),
                step_entered_at: "2026-01-01T00:00:00Z".into(),
                anchor_message_id: None,
                status: "active".into(),
            };
            db.upsert_active(&row).unwrap();
            db.complete_flow(&row.chat_id, status).unwrap();
        }

        // Filter by flow_name
        let (rows, total) = db.list_history(10, 0, Some("flow_a"), None, None).unwrap();
        assert_eq!(total, 2);
        assert_eq!(rows.len(), 2);

        // Filter by status
        let (rows, total) = db.list_history(10, 0, None, Some("timed_out"), None).unwrap();
        assert_eq!(total, 1);
        assert_eq!(rows[0].flow_name, "flow_b");
        let _ = total; // suppress warning
    }

    #[test]
    fn flow_db_kv_roundtrip() {
        let db = FlowDb::open_in_memory().unwrap();
        db.set_kv("telegram_offset", "42").unwrap();
        let val = db.get_kv("telegram_offset").unwrap();
        assert_eq!(val, Some("42".into()));
    }

    #[test]
    fn flow_db_kv_overwrite() {
        let db = FlowDb::open_in_memory().unwrap();
        db.set_kv("key1", "old").unwrap();
        db.set_kv("key1", "new").unwrap();
        let val = db.get_kv("key1").unwrap();
        assert_eq!(val, Some("new".into()));
    }

    #[test]
    fn flow_db_kv_missing_key() {
        let db = FlowDb::open_in_memory().unwrap();
        let val = db.get_kv("nonexistent").unwrap();
        assert!(val.is_none());
    }
}
