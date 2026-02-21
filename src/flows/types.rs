use serde::Deserialize;
use std::collections::HashMap;

// ── TOML-parsed types ───────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
pub struct FlowDefinitionToml {
    pub flow: FlowMeta,
    #[serde(default)]
    pub steps: Vec<StepToml>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FlowMeta {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    pub start: String,
    #[serde(default)]
    pub default_timeout_secs: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StepToml {
    pub id: String,
    pub kind: StepKind,
    #[serde(default)]
    pub text: Option<String>,
    #[serde(default)]
    pub buttons: Option<Vec<Vec<ButtonDef>>>,
    #[serde(default)]
    pub poll_options: Option<Vec<String>>,
    #[serde(default)]
    pub poll_anonymous: Option<bool>,
    #[serde(default)]
    pub timeout_secs: Option<u64>,
    #[serde(default)]
    pub agent_handoff: bool,
    #[serde(default)]
    pub transitions: Vec<TransitionDef>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum StepKind {
    Keyboard,
    Poll,
    Message,
    Edit,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ButtonDef {
    pub text: String,
    pub callback_data: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TransitionDef {
    pub on: String,
    pub target: String,
}

// ── Validated runtime types ─────────────────────────────────────

#[derive(Debug, Clone)]
pub struct FlowDefinition {
    pub name: String,
    pub description: Option<String>,
    pub start_step: String,
    pub default_timeout_secs: u64,
    pub steps: HashMap<String, Step>,
}

#[derive(Debug, Clone)]
pub struct Step {
    pub id: String,
    pub kind: StepKind,
    pub text: String,
    pub buttons: Option<Vec<Vec<ButtonDef>>>,
    pub poll_options: Option<Vec<String>>,
    pub poll_anonymous: bool,
    pub timeout_secs: Option<u64>,
    pub agent_handoff: bool,
    pub transitions: Vec<TransitionDef>,
}

impl Step {
    /// A terminal step has no transitions -- the flow completes here.
    pub fn is_terminal(&self) -> bool {
        self.transitions.is_empty()
    }

    /// The effective timeout for this step: per-step override, or the flow default.
    /// Returns 0 if no timeout is configured.
    pub fn effective_timeout(&self, flow_default: u64) -> u64 {
        self.timeout_secs.unwrap_or(flow_default)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn step_kind_serde_roundtrip() {
        let toml_str = r#"kind = "keyboard""#;
        #[derive(Deserialize)]
        struct W {
            kind: StepKind,
        }
        let w: W = toml::from_str(toml_str).unwrap();
        assert_eq!(w.kind, StepKind::Keyboard);

        let toml_str = r#"kind = "poll""#;
        let w: W = toml::from_str(toml_str).unwrap();
        assert_eq!(w.kind, StepKind::Poll);

        let toml_str = r#"kind = "message""#;
        let w: W = toml::from_str(toml_str).unwrap();
        assert_eq!(w.kind, StepKind::Message);

        let toml_str = r#"kind = "edit""#;
        let w: W = toml::from_str(toml_str).unwrap();
        assert_eq!(w.kind, StepKind::Edit);
    }

    #[test]
    fn is_terminal_with_no_transitions() {
        let step = Step {
            id: "end".into(),
            kind: StepKind::Message,
            text: "Done".into(),
            buttons: None,
            poll_options: None,
            poll_anonymous: true,
            timeout_secs: None,
            agent_handoff: false,
            transitions: vec![],
        };
        assert!(step.is_terminal());
    }

    #[test]
    fn effective_timeout_uses_step_override() {
        let step = Step {
            id: "s".into(),
            kind: StepKind::Keyboard,
            text: "Q?".into(),
            buttons: None,
            poll_options: None,
            poll_anonymous: true,
            timeout_secs: Some(30),
            agent_handoff: false,
            transitions: vec![TransitionDef {
                on: "yes".into(),
                target: "done".into(),
            }],
        };
        assert_eq!(step.effective_timeout(120), 30);
        // Without override, uses flow default
        let step2 = Step {
            timeout_secs: None,
            ..step
        };
        assert_eq!(step2.effective_timeout(120), 120);
    }
}
