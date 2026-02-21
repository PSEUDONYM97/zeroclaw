use super::types::*;
use std::collections::{HashMap, HashSet, VecDeque};

/// Validation errors that prevent a flow from loading.
#[derive(Debug, Clone)]
pub struct FlowValidationError {
    pub flow_name: String,
    pub message: String,
}

impl std::fmt::Display for FlowValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "flow '{}': {}", self.flow_name, self.message)
    }
}

/// Build a validated `FlowDefinition` from a parsed TOML definition.
/// Returns validation errors if the flow is structurally invalid.
pub fn build_flow_definition(toml: &FlowDefinitionToml) -> Result<FlowDefinition, Vec<FlowValidationError>> {
    let mut errors = Vec::new();
    let name = &toml.flow.name;

    // Check for duplicate step IDs
    let mut seen_ids = HashSet::new();
    for step in &toml.steps {
        if !seen_ids.insert(&step.id) {
            errors.push(FlowValidationError {
                flow_name: name.clone(),
                message: format!("duplicate step id '{}'", step.id),
            });
        }
    }

    // Build step map for lookups
    let step_ids: HashSet<&str> = toml.steps.iter().map(|s| s.id.as_str()).collect();

    // Check start step exists
    if !step_ids.contains(toml.flow.start.as_str()) {
        errors.push(FlowValidationError {
            flow_name: name.clone(),
            message: format!("start step '{}' does not exist", toml.flow.start),
        });
    }

    // Validate each step
    for step in &toml.steps {
        // Kind/field validation
        match step.kind {
            StepKind::Keyboard => {
                if step.buttons.as_ref().map_or(true, |b| b.is_empty()) {
                    errors.push(FlowValidationError {
                        flow_name: name.clone(),
                        message: format!("step '{}': keyboard step requires non-empty buttons", step.id),
                    });
                }
            }
            StepKind::Poll => {
                let opt_count = step.poll_options.as_ref().map_or(0, |o| o.len());
                if opt_count < 2 {
                    errors.push(FlowValidationError {
                        flow_name: name.clone(),
                        message: format!(
                            "step '{}': poll step requires at least 2 options (found {opt_count})",
                            step.id
                        ),
                    });
                }
            }
            StepKind::Message | StepKind::Edit => {
                // text is required but defaults to None in TOML
                if step.text.as_ref().map_or(true, |t| t.is_empty()) {
                    errors.push(FlowValidationError {
                        flow_name: name.clone(),
                        message: format!("step '{}': {} step requires non-empty text", step.id, match step.kind {
                            StepKind::Message => "message",
                            StepKind::Edit => "edit",
                            _ => unreachable!(),
                        }),
                    });
                }
            }
        }

        // Validate transition targets exist
        for tr in &step.transitions {
            if !step_ids.contains(tr.target.as_str()) {
                errors.push(FlowValidationError {
                    flow_name: name.clone(),
                    message: format!(
                        "step '{}': transition target '{}' does not exist",
                        step.id, tr.target
                    ),
                });
            }
        }

        // Warning: button callback_data with no matching transition
        if let Some(ref buttons) = step.buttons {
            for row in buttons {
                for btn in row {
                    let has_matching = step.transitions.iter().any(|t| {
                        t.on == btn.callback_data || t.on == "_any"
                    });
                    if !has_matching {
                        tracing::warn!(
                            "flow '{}', step '{}': button callback_data '{}' has no matching transition",
                            name, step.id, btn.callback_data
                        );
                    }
                }
            }
        }
    }

    // Warning: unreachable steps from start
    if step_ids.contains(toml.flow.start.as_str()) {
        let reachable = find_reachable_steps(&toml.steps, &toml.flow.start);
        for step in &toml.steps {
            if !reachable.contains(step.id.as_str()) {
                tracing::warn!(
                    "flow '{}': step '{}' is unreachable from start",
                    name, step.id
                );
            }
        }
    }

    // Info: cycle detection (cycles are valid, just log for awareness)
    if has_cycles(&toml.steps, &toml.flow.start) {
        tracing::info!("flow '{}': contains cycles (valid for retry loops)", name);
    }

    if !errors.is_empty() {
        return Err(errors);
    }

    // Build validated definition
    let mut steps = HashMap::new();
    for s in &toml.steps {
        steps.insert(
            s.id.clone(),
            Step {
                id: s.id.clone(),
                kind: s.kind.clone(),
                text: s.text.clone().unwrap_or_default(),
                buttons: s.buttons.clone(),
                poll_options: s.poll_options.clone(),
                poll_anonymous: s.poll_anonymous.unwrap_or(true),
                timeout_secs: s.timeout_secs,
                agent_handoff: s.agent_handoff,
                transitions: s.transitions.clone(),
            },
        );
    }

    Ok(FlowDefinition {
        name: toml.flow.name.clone(),
        description: toml.flow.description.clone(),
        start_step: toml.flow.start.clone(),
        default_timeout_secs: toml.flow.default_timeout_secs,
        steps,
    })
}

/// BFS from start to find all reachable step IDs.
fn find_reachable_steps<'a>(steps: &'a [StepToml], start: &'a str) -> HashSet<&'a str> {
    let mut reachable = HashSet::new();
    let mut queue = VecDeque::new();
    queue.push_back(start);

    while let Some(current) = queue.pop_front() {
        if !reachable.insert(current) {
            continue;
        }
        for step in steps {
            if step.id == current {
                for tr in &step.transitions {
                    queue.push_back(&tr.target);
                }
                break;
            }
        }
    }

    reachable
}

/// Check if there are any cycles reachable from start.
fn has_cycles(steps: &[StepToml], start: &str) -> bool {
    let mut visited = HashSet::new();
    let mut stack = HashSet::new();
    let step_map: HashMap<&str, &StepToml> = steps.iter().map(|s| (s.id.as_str(), s)).collect();
    dfs_cycle(&step_map, start, &mut visited, &mut stack)
}

fn dfs_cycle<'a>(
    map: &HashMap<&str, &'a StepToml>,
    node: &'a str,
    visited: &mut HashSet<&'a str>,
    stack: &mut HashSet<&'a str>,
) -> bool {
    if stack.contains(node) {
        return true;
    }
    if visited.contains(node) {
        return false;
    }
    visited.insert(node);
    stack.insert(node);

    if let Some(step) = map.get(node) {
        for tr in &step.transitions {
            if dfs_cycle(map, &tr.target, visited, stack) {
                return true;
            }
        }
    }

    stack.remove(node);
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    fn minimal_flow() -> FlowDefinitionToml {
        toml::from_str(
            r#"
[flow]
name = "test"
start = "s1"

[[steps]]
id = "s1"
kind = "message"
text = "Hello"
"#,
        )
        .unwrap()
    }

    #[test]
    fn valid_flow_passes() {
        let toml = minimal_flow();
        let result = build_flow_definition(&toml);
        assert!(result.is_ok());
        let def = result.unwrap();
        assert_eq!(def.name, "test");
        assert_eq!(def.start_step, "s1");
        assert!(def.steps.contains_key("s1"));
    }

    #[test]
    fn missing_start_step_errors() {
        let mut toml = minimal_flow();
        toml.flow.start = "nonexistent".into();
        let result = build_flow_definition(&toml);
        assert!(result.is_err());
        let errs = result.unwrap_err();
        assert!(errs.iter().any(|e| e.message.contains("start step")));
    }

    #[test]
    fn duplicate_step_ids_errors() {
        let toml: FlowDefinitionToml = toml::from_str(
            r#"
[flow]
name = "dup"
start = "s1"

[[steps]]
id = "s1"
kind = "message"
text = "A"

[[steps]]
id = "s1"
kind = "message"
text = "B"
"#,
        )
        .unwrap();
        let result = build_flow_definition(&toml);
        assert!(result.is_err());
        let errs = result.unwrap_err();
        assert!(errs.iter().any(|e| e.message.contains("duplicate step id")));
    }

    #[test]
    fn orphan_transition_target_errors() {
        let toml: FlowDefinitionToml = toml::from_str(
            r#"
[flow]
name = "orphan"
start = "s1"

[[steps]]
id = "s1"
kind = "message"
text = "go"

[[steps.transitions]]
on = "next"
target = "nonexistent"
"#,
        )
        .unwrap();
        let result = build_flow_definition(&toml);
        assert!(result.is_err());
        let errs = result.unwrap_err();
        assert!(errs.iter().any(|e| e.message.contains("does not exist")));
    }

    #[test]
    fn keyboard_without_buttons_errors() {
        let toml: FlowDefinitionToml = toml::from_str(
            r#"
[flow]
name = "kb"
start = "s1"

[[steps]]
id = "s1"
kind = "keyboard"
text = "Pick one"
"#,
        )
        .unwrap();
        let result = build_flow_definition(&toml);
        assert!(result.is_err());
        let errs = result.unwrap_err();
        assert!(errs.iter().any(|e| e.message.contains("buttons")));
    }

    #[test]
    fn poll_too_few_options_errors() {
        let toml: FlowDefinitionToml = toml::from_str(
            r#"
[flow]
name = "poll"
start = "s1"

[[steps]]
id = "s1"
kind = "poll"
text = "Vote"
poll_options = ["only_one"]
"#,
        )
        .unwrap();
        let result = build_flow_definition(&toml);
        assert!(result.is_err());
        let errs = result.unwrap_err();
        assert!(errs.iter().any(|e| e.message.contains("at least 2 options")));
    }

    #[test]
    fn reachability_detects_unreachable() {
        // This test verifies the reachability helper; unreachable steps
        // produce warnings but don't block validation.
        let steps = vec![
            StepToml {
                id: "s1".into(),
                kind: StepKind::Message,
                text: Some("A".into()),
                buttons: None,
                poll_options: None,
                poll_anonymous: None,
                timeout_secs: None,
                agent_handoff: false,
                transitions: vec![],
            },
            StepToml {
                id: "orphan".into(),
                kind: StepKind::Message,
                text: Some("B".into()),
                buttons: None,
                poll_options: None,
                poll_anonymous: None,
                timeout_secs: None,
                agent_handoff: false,
                transitions: vec![],
            },
        ];
        let reachable = find_reachable_steps(&steps, "s1");
        assert!(reachable.contains("s1"));
        assert!(!reachable.contains("orphan"));
    }

    #[test]
    fn cycle_detection() {
        let steps = vec![
            StepToml {
                id: "a".into(),
                kind: StepKind::Message,
                text: Some("A".into()),
                buttons: None,
                poll_options: None,
                poll_anonymous: None,
                timeout_secs: None,
                agent_handoff: false,
                transitions: vec![TransitionDef {
                    on: "next".into(),
                    target: "b".into(),
                }],
            },
            StepToml {
                id: "b".into(),
                kind: StepKind::Message,
                text: Some("B".into()),
                buttons: None,
                poll_options: None,
                poll_anonymous: None,
                timeout_secs: None,
                agent_handoff: false,
                transitions: vec![TransitionDef {
                    on: "back".into(),
                    target: "a".into(),
                }],
            },
        ];
        assert!(has_cycles(&steps, "a"));
    }
}
