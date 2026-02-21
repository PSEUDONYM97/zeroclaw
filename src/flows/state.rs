use super::db::{FlowDb, FlowInstanceRow};
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// A running flow instance for a specific chat.
#[derive(Debug)]
pub struct FlowInstance {
    pub flow_name: String,
    pub current_step: String,
    pub started_at: DateTime<Utc>,
    pub step_entered_at: DateTime<Utc>,
    pub anchor_message_id: Option<i64>,
    pub chat_id: String,
}

/// Error from flow operations.
#[derive(Debug, Clone)]
pub enum FlowError {
    FlowNotFound(String),
    StepNotFound(String),
    NoActiveFlow(String),
}

impl std::fmt::Display for FlowError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::FlowNotFound(name) => write!(f, "flow '{name}' not found"),
            Self::StepNotFound(step) => write!(f, "step '{step}' not found"),
            Self::NoActiveFlow(chat) => write!(f, "no active flow for chat '{chat}'"),
        }
    }
}

impl std::error::Error for FlowError {}

/// Durable store tracking one active flow per chat.
///
/// In-memory HashMap is the hot cache; every mutation is written through to
/// `FlowDb` when present.  `refresh_from_db()` reconciles the cache with
/// the database (so CP mutations propagate within one timeout tick).
pub struct FlowStore {
    active: Mutex<HashMap<String, FlowInstance>>,
    db: Option<Arc<FlowDb>>,
}

impl FlowStore {
    /// Create an in-memory-only store (backward compat, tests).
    pub fn new() -> Self {
        Self {
            active: Mutex::new(HashMap::new()),
            db: None,
        }
    }

    /// Get a reference to the backing FlowDb (if any).
    pub fn db(&self) -> Option<&Arc<FlowDb>> {
        self.db.as_ref()
    }

    /// Create a durable store backed by SQLite.
    pub fn with_db(db: Arc<FlowDb>) -> Self {
        Self {
            active: Mutex::new(HashMap::new()),
            db: Some(db),
        }
    }

    /// Restore in-memory cache from the database. Returns the number of flows loaded.
    pub fn load_from_db(&self) -> anyhow::Result<usize> {
        let Some(ref db) = self.db else {
            return Ok(0);
        };
        let rows = db.list_active()?;
        let mut guard = self.active.lock().unwrap_or_else(|e| e.into_inner());
        for row in &rows {
            guard.insert(row.chat_id.clone(), row_to_instance(row));
        }
        Ok(rows.len())
    }

    /// Sync in-memory cache with the database (called on each timeout tick).
    ///
    /// - Rows in DB but not in cache: add to cache (CP replayed a flow)
    /// - Rows in cache but not in DB: remove from cache (CP force-completed)
    /// - Rows in both: update cache from DB (CP may have changed step)
    pub fn refresh_from_db(&self) -> anyhow::Result<()> {
        let Some(ref db) = self.db else {
            return Ok(());
        };
        let db_rows = db.list_active()?;
        let db_map: HashMap<String, FlowInstanceRow> = db_rows
            .into_iter()
            .map(|r| (r.chat_id.clone(), r))
            .collect();

        let mut guard = self.active.lock().unwrap_or_else(|e| e.into_inner());

        // Remove flows that are no longer in DB
        guard.retain(|chat_id, _| db_map.contains_key(chat_id));

        // Add/update from DB
        for (chat_id, row) in &db_map {
            guard.insert(chat_id.clone(), row_to_instance(row));
        }

        Ok(())
    }

    /// Start a new flow for a chat. Replaces any existing active flow.
    pub fn start_flow(
        &self,
        chat_id: &str,
        flow_name: &str,
        start_step: &str,
        anchor_message_id: Option<i64>,
    ) {
        let now = Utc::now();
        let instance = FlowInstance {
            flow_name: flow_name.to_string(),
            current_step: start_step.to_string(),
            started_at: now,
            step_entered_at: now,
            anchor_message_id,
            chat_id: chat_id.to_string(),
        };

        // Persist first
        if let Some(ref db) = self.db {
            let row = instance_to_row(&instance);
            if let Err(e) = db.upsert_active(&row) {
                tracing::warn!("Failed to persist flow start: {e}");
            }
        }

        self.active
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(chat_id.to_string(), instance);
    }

    /// Get a reference to the active flow for a chat (via closure to avoid borrow issues).
    pub fn with_flow<F, R>(&self, chat_id: &str, f: F) -> Option<R>
    where
        F: FnOnce(&FlowInstance) -> R,
    {
        self.active
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(chat_id)
            .map(f)
    }

    /// Check if a chat has an active flow.
    pub fn has_flow(&self, chat_id: &str) -> bool {
        self.active
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains_key(chat_id)
    }

    /// Advance the flow to a new step.
    pub fn advance(
        &self,
        chat_id: &str,
        new_step: &str,
        anchor_message_id: Option<i64>,
    ) -> Result<(), FlowError> {
        let now = Utc::now();
        let mut guard = self.active.lock().unwrap_or_else(|e| e.into_inner());
        let instance = guard
            .get_mut(chat_id)
            .ok_or_else(|| FlowError::NoActiveFlow(chat_id.to_string()))?;
        instance.current_step = new_step.to_string();
        instance.step_entered_at = now;
        if let Some(mid) = anchor_message_id {
            instance.anchor_message_id = Some(mid);
        }

        // Persist
        if let Some(ref db) = self.db {
            let ts = now.to_rfc3339();
            if let Err(e) = db.update_step(chat_id, new_step, instance.anchor_message_id, &ts) {
                tracing::warn!("Failed to persist flow advance: {e}");
            }
        }

        Ok(())
    }

    /// Complete (remove) the active flow for a chat with the given terminal status.
    pub fn complete_flow(&self, chat_id: &str, status: &str) -> Option<FlowInstance> {
        let removed = self
            .active
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(chat_id);

        // Persist
        if let Some(ref db) = self.db {
            if let Err(e) = db.complete_flow(chat_id, status) {
                tracing::warn!("Failed to persist flow completion: {e}");
            }
        }

        removed
    }

    /// Check all active flows for timeouts. Returns list of (chat_id, flow_name, current_step)
    /// for flows whose current step has exceeded its effective timeout.
    pub fn check_timeouts(
        &self,
        flow_defs: &HashMap<String, super::types::FlowDefinition>,
    ) -> Vec<(String, String, String)> {
        let guard = self.active.lock().unwrap_or_else(|e| e.into_inner());
        let now = Utc::now();
        let mut timed_out = Vec::new();

        for (chat_id, instance) in guard.iter() {
            if let Some(def) = flow_defs.get(&instance.flow_name) {
                if let Some(step) = def.steps.get(&instance.current_step) {
                    let timeout = step.effective_timeout(def.default_timeout_secs);
                    if timeout > 0 {
                        let elapsed = (now - instance.step_entered_at).num_seconds().max(0) as u64;
                        if elapsed >= timeout {
                            timed_out.push((
                                chat_id.clone(),
                                instance.flow_name.clone(),
                                instance.current_step.clone(),
                            ));
                        }
                    }
                }
            }
        }

        timed_out
    }

    /// Get the flow name and current step for a chat (snapshot).
    pub fn get_flow_info(&self, chat_id: &str) -> Option<(String, String, Option<i64>)> {
        self.active
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(chat_id)
            .map(|inst| {
                (
                    inst.flow_name.clone(),
                    inst.current_step.clone(),
                    inst.anchor_message_id,
                )
            })
    }
}

/// Convert a DB row to an in-memory instance.
fn row_to_instance(row: &FlowInstanceRow) -> FlowInstance {
    FlowInstance {
        flow_name: row.flow_name.clone(),
        current_step: row.current_step.clone(),
        started_at: row
            .started_at
            .parse::<DateTime<Utc>>()
            .unwrap_or_else(|_| Utc::now()),
        step_entered_at: row
            .step_entered_at
            .parse::<DateTime<Utc>>()
            .unwrap_or_else(|_| Utc::now()),
        anchor_message_id: row.anchor_message_id,
        chat_id: row.chat_id.clone(),
    }
}

/// Convert an in-memory instance to a DB row.
fn instance_to_row(inst: &FlowInstance) -> FlowInstanceRow {
    FlowInstanceRow {
        chat_id: inst.chat_id.clone(),
        flow_name: inst.flow_name.clone(),
        current_step: inst.current_step.clone(),
        started_at: inst.started_at.to_rfc3339(),
        step_entered_at: inst.step_entered_at.to_rfc3339(),
        anchor_message_id: inst.anchor_message_id,
        status: "active".into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::flows::types::*;

    fn make_flow_def() -> FlowDefinition {
        let mut steps = HashMap::new();
        steps.insert(
            "ask".into(),
            Step {
                id: "ask".into(),
                kind: StepKind::Keyboard,
                text: "Sure?".into(),
                buttons: None,
                poll_options: None,
                poll_anonymous: true,
                timeout_secs: Some(5),
                agent_handoff: false,
                transitions: vec![
                    TransitionDef { on: "yes".into(), target: "done".into() },
                    TransitionDef { on: "_timeout".into(), target: "timeout".into() },
                ],
            },
        );
        steps.insert(
            "done".into(),
            Step {
                id: "done".into(),
                kind: StepKind::Message,
                text: "OK".into(),
                buttons: None,
                poll_options: None,
                poll_anonymous: true,
                timeout_secs: None,
                agent_handoff: false,
                transitions: vec![],
            },
        );
        steps.insert(
            "timeout".into(),
            Step {
                id: "timeout".into(),
                kind: StepKind::Edit,
                text: "Timed out".into(),
                buttons: None,
                poll_options: None,
                poll_anonymous: true,
                timeout_secs: None,
                agent_handoff: false,
                transitions: vec![],
            },
        );
        FlowDefinition {
            name: "test_flow".into(),
            description: None,
            start_step: "ask".into(),
            default_timeout_secs: 120,
            steps,
        }
    }

    #[test]
    fn start_and_get_flow() {
        let store = FlowStore::new();
        store.start_flow("chat1", "test_flow", "ask", Some(100));
        assert!(store.has_flow("chat1"));
        let info = store.get_flow_info("chat1").unwrap();
        assert_eq!(info.0, "test_flow");
        assert_eq!(info.1, "ask");
        assert_eq!(info.2, Some(100));
    }

    #[test]
    fn advance_flow() {
        let store = FlowStore::new();
        store.start_flow("chat1", "test_flow", "ask", None);
        store.advance("chat1", "done", Some(200)).unwrap();
        let info = store.get_flow_info("chat1").unwrap();
        assert_eq!(info.1, "done");
        assert_eq!(info.2, Some(200));
    }

    #[test]
    fn complete_flow_removes() {
        let store = FlowStore::new();
        store.start_flow("chat1", "test_flow", "ask", None);
        let removed = store.complete_flow("chat1", "completed");
        assert!(removed.is_some());
        assert!(!store.has_flow("chat1"));
    }

    #[test]
    fn advance_no_active_flow_errors() {
        let store = FlowStore::new();
        let result = store.advance("missing", "step", None);
        assert!(result.is_err());
    }

    #[test]
    fn timeout_detection() {
        let store = FlowStore::new();
        store.start_flow("chat1", "test_flow", "ask", None);

        // Manually set step_entered_at to the past
        {
            let mut guard = store.active.lock().unwrap();
            let inst = guard.get_mut("chat1").unwrap();
            inst.step_entered_at = Utc::now() - chrono::Duration::seconds(10);
        }

        let mut defs = HashMap::new();
        defs.insert("test_flow".into(), make_flow_def());

        let timed_out = store.check_timeouts(&defs);
        assert_eq!(timed_out.len(), 1);
        assert_eq!(timed_out[0].0, "chat1");
        assert_eq!(timed_out[0].2, "ask");
    }
}
