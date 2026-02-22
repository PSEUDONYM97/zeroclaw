use super::types::{FlowDefinitionToml, FlowDefinition, StepKind};
use crate::config::FlowPolicyConfig;
use std::collections::HashMap;

/// A single policy violation found during check_policy.
#[derive(Debug, Clone)]
pub struct PolicyViolation {
    pub message: String,
}

impl std::fmt::Display for PolicyViolation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

/// Validate an agent-authored flow definition against policy constraints.
///
/// Returns `Ok(())` if the flow passes all checks, or `Err(violations)` listing
/// every policy rule that was broken.
pub fn check_policy(
    toml: &FlowDefinitionToml,
    policy: &FlowPolicyConfig,
    current_agent_flow_count: usize,
) -> Result<(), Vec<PolicyViolation>> {
    let mut violations = Vec::new();

    // 1. Agent authoring must be enabled
    if !policy.agent_authoring_enabled {
        violations.push(PolicyViolation {
            message: "agent flow authoring is disabled".into(),
        });
    }

    // 2. Step count within limit
    if toml.steps.len() > policy.max_steps {
        violations.push(PolicyViolation {
            message: format!(
                "flow has {} steps, exceeds max_steps ({})",
                toml.steps.len(),
                policy.max_steps
            ),
        });
    }

    // 3. No denied step kinds
    for step in &toml.steps {
        let kind_str = step_kind_to_string(&step.kind);
        if policy
            .denied_step_kinds
            .iter()
            .any(|d: &String| d.to_lowercase() == kind_str)
        {
            violations.push(PolicyViolation {
                message: format!(
                    "step '{}' uses denied kind '{}'",
                    step.id, kind_str
                ),
            });
        }
    }

    // 4. Require handoff on keyboard steps
    if policy.require_handoff_on_keyboard {
        for step in &toml.steps {
            if step.kind == StepKind::Keyboard && !step.agent_handoff {
                violations.push(PolicyViolation {
                    message: format!(
                        "step '{}': keyboard step requires agent_handoff = true (policy: require_handoff_on_keyboard)",
                        step.id
                    ),
                });
            }
        }
    }

    // 5. Denied text patterns (case-insensitive substring)
    if !policy.denied_text_patterns.is_empty() {
        for step in &toml.steps {
            if let Some(ref text) = step.text {
                let text_lower = text.to_lowercase();
                for pattern in &policy.denied_text_patterns {
                    let pattern_lower: String = pattern.to_lowercase();
                    if text_lower.contains(pattern_lower.as_str()) {
                        violations.push(PolicyViolation {
                            message: format!(
                                "step '{}': text contains denied pattern '{}'",
                                step.id, pattern
                            ),
                        });
                    }
                }
            }
        }
    }

    // 6. Agent flow count within limit
    if current_agent_flow_count >= policy.max_agent_flows {
        violations.push(PolicyViolation {
            message: format!(
                "agent flow count ({}) has reached max_agent_flows ({})",
                current_agent_flow_count, policy.max_agent_flows
            ),
        });
    }

    if violations.is_empty() {
        Ok(())
    } else {
        Err(violations)
    }
}

/// Check if a flow qualifies for auto-approval.
///
/// Returns true iff: auto_approve is enabled AND all steps are Message kind
/// AND step count <= auto_approve_max_steps.
pub fn qualifies_for_auto_approve(
    toml: &FlowDefinitionToml,
    policy: &FlowPolicyConfig,
) -> bool {
    if !policy.auto_approve {
        return false;
    }
    if toml.steps.len() > policy.auto_approve_max_steps {
        return false;
    }
    toml.steps.iter().all(|s| s.kind == StepKind::Message)
}

/// Check if a flow name is owned by operator TOML definitions.
///
/// Pure function check against the in-memory operator flow_defs HashMap.
/// Agent cannot shadow operator-defined flow names.
pub fn is_operator_owned(
    flow_name: &str,
    operator_flow_defs: &HashMap<String, FlowDefinition>,
) -> bool {
    operator_flow_defs.contains_key(flow_name)
}

fn step_kind_to_string(kind: &StepKind) -> String {
    match kind {
        StepKind::Keyboard => "keyboard".into(),
        StepKind::Poll => "poll".into(),
        StepKind::Message => "message".into(),
        StepKind::Edit => "edit".into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::flows::types::*;

    fn make_policy() -> FlowPolicyConfig {
        FlowPolicyConfig {
            agent_authoring_enabled: true,
            denied_step_kinds: vec!["edit".into()],
            max_steps: 10,
            max_agent_flows: 50,
            require_handoff_on_keyboard: true,
            auto_approve: false,
            auto_approve_max_steps: 5,
            denied_text_patterns: vec!["<script".into(), "javascript:".into()],
        }
    }

    fn minimal_toml() -> FlowDefinitionToml {
        FlowDefinitionToml {
            flow: FlowMeta {
                name: "test".into(),
                description: None,
                start: "s1".into(),
                default_timeout_secs: 60,
            },
            steps: vec![StepToml {
                id: "s1".into(),
                kind: StepKind::Message,
                text: Some("Hello!".into()),
                buttons: None,
                poll_options: None,
                poll_anonymous: None,
                timeout_secs: None,
                agent_handoff: false,
                transitions: vec![],
            }],
        }
    }

    #[test]
    fn policy_passes_valid_flow() {
        let toml = minimal_toml();
        let policy = make_policy();
        assert!(check_policy(&toml, &policy, 0).is_ok());
    }

    #[test]
    fn policy_rejects_disabled_authoring() {
        let toml = minimal_toml();
        let mut policy = make_policy();
        policy.agent_authoring_enabled = false;
        let err = check_policy(&toml, &policy, 0).unwrap_err();
        assert!(err.iter().any(|v| v.message.contains("disabled")));
    }

    #[test]
    fn policy_rejects_denied_kind() {
        let mut toml = minimal_toml();
        toml.steps[0].kind = StepKind::Edit;
        toml.steps[0].text = Some("Editing".into());
        let policy = make_policy();
        let err = check_policy(&toml, &policy, 0).unwrap_err();
        assert!(err.iter().any(|v| v.message.contains("denied kind")));
    }

    #[test]
    fn policy_rejects_too_many_steps() {
        let mut toml = minimal_toml();
        for i in 0..11 {
            toml.steps.push(StepToml {
                id: format!("s{i}"),
                kind: StepKind::Message,
                text: Some("Hi".into()),
                buttons: None,
                poll_options: None,
                poll_anonymous: None,
                timeout_secs: None,
                agent_handoff: false,
                transitions: vec![],
            });
        }
        let policy = make_policy();
        let err = check_policy(&toml, &policy, 0).unwrap_err();
        assert!(err.iter().any(|v| v.message.contains("exceeds max_steps")));
    }

    #[test]
    fn policy_requires_handoff_on_keyboard() {
        let mut toml = minimal_toml();
        toml.steps[0].kind = StepKind::Keyboard;
        toml.steps[0].agent_handoff = false;
        toml.steps[0].buttons = Some(vec![vec![ButtonDef {
            text: "Yes".into(),
            callback_data: "yes".into(),
        }]]);
        let policy = make_policy();
        let err = check_policy(&toml, &policy, 0).unwrap_err();
        assert!(err
            .iter()
            .any(|v| v.message.contains("require_handoff_on_keyboard")));
    }

    #[test]
    fn policy_rejects_denied_text() {
        let mut toml = minimal_toml();
        toml.steps[0].text = Some("Click <script>alert(1)</script>".into());
        let policy = make_policy();
        let err = check_policy(&toml, &policy, 0).unwrap_err();
        assert!(err
            .iter()
            .any(|v| v.message.contains("denied pattern")));
    }

    #[test]
    fn policy_rejects_over_max_agent_flows() {
        let toml = minimal_toml();
        let policy = make_policy();
        let err = check_policy(&toml, &policy, 50).unwrap_err();
        assert!(err
            .iter()
            .any(|v| v.message.contains("max_agent_flows")));
    }

    #[test]
    fn auto_approve_message_only() {
        let toml = minimal_toml();
        let mut policy = make_policy();
        policy.auto_approve = true;
        assert!(qualifies_for_auto_approve(&toml, &policy));
    }

    #[test]
    fn auto_approve_denied_for_keyboard() {
        let mut toml = minimal_toml();
        toml.steps[0].kind = StepKind::Keyboard;
        let mut policy = make_policy();
        policy.auto_approve = true;
        assert!(!qualifies_for_auto_approve(&toml, &policy));
    }

    #[test]
    fn auto_approve_denied_when_disabled() {
        let toml = minimal_toml();
        let policy = make_policy(); // auto_approve = false
        assert!(!qualifies_for_auto_approve(&toml, &policy));
    }

    #[test]
    fn auto_approve_denied_over_max_steps() {
        let mut toml = minimal_toml();
        for i in 0..6 {
            toml.steps.push(StepToml {
                id: format!("extra{i}"),
                kind: StepKind::Message,
                text: Some("Hi".into()),
                buttons: None,
                poll_options: None,
                poll_anonymous: None,
                timeout_secs: None,
                agent_handoff: false,
                transitions: vec![],
            });
        }
        let mut policy = make_policy();
        policy.auto_approve = true;
        // 7 steps > auto_approve_max_steps (5)
        assert!(!qualifies_for_auto_approve(&toml, &policy));
    }

    #[test]
    fn operator_owned_check() {
        let mut defs = HashMap::new();
        defs.insert(
            "onboarding".into(),
            FlowDefinition {
                name: "onboarding".into(),
                description: None,
                start_step: "s1".into(),
                default_timeout_secs: 60,
                steps: HashMap::new(),
            },
        );
        assert!(is_operator_owned("onboarding", &defs));
        assert!(!is_operator_owned("agent_custom", &defs));
    }
}
