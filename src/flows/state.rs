use std::collections::HashMap;
use std::sync::Mutex;
use tokio::time::Instant;

/// A running flow instance for a specific chat.
#[derive(Debug)]
pub struct FlowInstance {
    pub flow_name: String,
    pub current_step: String,
    pub started_at: Instant,
    pub step_entered_at: Instant,
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

/// In-memory store tracking one active flow per chat.
pub struct FlowStore {
    active: Mutex<HashMap<String, FlowInstance>>,
}

impl FlowStore {
    pub fn new() -> Self {
        Self {
            active: Mutex::new(HashMap::new()),
        }
    }

    /// Start a new flow for a chat. Replaces any existing active flow.
    pub fn start_flow(
        &self,
        chat_id: &str,
        flow_name: &str,
        start_step: &str,
        anchor_message_id: Option<i64>,
    ) {
        let now = Instant::now();
        let instance = FlowInstance {
            flow_name: flow_name.to_string(),
            current_step: start_step.to_string(),
            started_at: now,
            step_entered_at: now,
            anchor_message_id,
            chat_id: chat_id.to_string(),
        };
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
        let mut guard = self.active.lock().unwrap_or_else(|e| e.into_inner());
        let instance = guard
            .get_mut(chat_id)
            .ok_or_else(|| FlowError::NoActiveFlow(chat_id.to_string()))?;
        instance.current_step = new_step.to_string();
        instance.step_entered_at = Instant::now();
        if let Some(mid) = anchor_message_id {
            instance.anchor_message_id = Some(mid);
        }
        Ok(())
    }

    /// Complete (remove) the active flow for a chat.
    pub fn complete_flow(&self, chat_id: &str) -> Option<FlowInstance> {
        self.active
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(chat_id)
    }

    /// Check all active flows for timeouts. Returns list of (chat_id, flow_name, current_step)
    /// for flows whose current step has exceeded its effective timeout.
    pub fn check_timeouts(
        &self,
        flow_defs: &HashMap<String, super::types::FlowDefinition>,
    ) -> Vec<(String, String, String)> {
        let guard = self.active.lock().unwrap_or_else(|e| e.into_inner());
        let now = Instant::now();
        let mut timed_out = Vec::new();

        for (chat_id, instance) in guard.iter() {
            if let Some(def) = flow_defs.get(&instance.flow_name) {
                if let Some(step) = def.steps.get(&instance.current_step) {
                    let timeout = step.effective_timeout(def.default_timeout_secs);
                    if timeout > 0 {
                        let elapsed = now.duration_since(instance.step_entered_at).as_secs();
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
        let removed = store.complete_flow("chat1");
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
        // Start a flow with a very short timeout (we'll test the logic, not actual waiting)
        store.start_flow("chat1", "test_flow", "ask", None);

        // Manually set step_entered_at to the past
        {
            let mut guard = store.active.lock().unwrap();
            let inst = guard.get_mut("chat1").unwrap();
            inst.step_entered_at = Instant::now() - std::time::Duration::from_secs(10);
        }

        let mut defs = HashMap::new();
        defs.insert("test_flow".into(), make_flow_def());

        let timed_out = store.check_timeouts(&defs);
        assert_eq!(timed_out.len(), 1);
        assert_eq!(timed_out[0].0, "chat1");
        assert_eq!(timed_out[0].2, "ask");
    }
}
