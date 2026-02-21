use rusqlite::{params, Connection, OpenFlags};
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
             );",
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
