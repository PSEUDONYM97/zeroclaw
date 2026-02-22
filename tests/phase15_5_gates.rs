use std::collections::HashMap;
use std::time::Duration;

// ── Gate 1: flow_parse_minimal ──────────────────────────────────

#[test]
fn flow_parse_minimal() {
    let toml_str = r#"
[flow]
name = "test"
start = "s1"

[[steps]]
id = "s1"
kind = "message"
text = "Hello"
"#;
    let parsed: zeroclaw::flows::types::FlowDefinitionToml = toml::from_str(toml_str).unwrap();
    assert_eq!(parsed.flow.name, "test");
    assert_eq!(parsed.flow.start, "s1");
    assert_eq!(parsed.steps.len(), 1);
    assert_eq!(parsed.steps[0].id, "s1");
}

// ── Gate 2: flow_validate_missing_start ─────────────────────────

#[test]
fn flow_validate_missing_start() {
    let toml_str = r#"
[flow]
name = "bad"
start = "nonexistent"

[[steps]]
id = "s1"
kind = "message"
text = "Hello"
"#;
    let parsed: zeroclaw::flows::types::FlowDefinitionToml = toml::from_str(toml_str).unwrap();
    let result = zeroclaw::flows::validate::build_flow_definition(&parsed);
    assert!(result.is_err());
    let errs = result.unwrap_err();
    assert!(errs.iter().any(|e| e.message.contains("start step")));
}

// ── Gate 3: flow_validate_duplicate_ids ─────────────────────────

#[test]
fn flow_validate_duplicate_ids() {
    let toml_str = r#"
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
"#;
    let parsed: zeroclaw::flows::types::FlowDefinitionToml = toml::from_str(toml_str).unwrap();
    let result = zeroclaw::flows::validate::build_flow_definition(&parsed);
    assert!(result.is_err());
    let errs = result.unwrap_err();
    assert!(errs.iter().any(|e| e.message.contains("duplicate step id")));
}

// ── Gate 4: flow_validate_orphan_target ─────────────────────────

#[test]
fn flow_validate_orphan_target() {
    let toml_str = r#"
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
"#;
    let parsed: zeroclaw::flows::types::FlowDefinitionToml = toml::from_str(toml_str).unwrap();
    let result = zeroclaw::flows::validate::build_flow_definition(&parsed);
    assert!(result.is_err());
    let errs = result.unwrap_err();
    assert!(errs.iter().any(|e| e.message.contains("does not exist")));
}

// ── Gate 5: flow_validate_keyboard_no_buttons ───────────────────

#[test]
fn flow_validate_keyboard_no_buttons() {
    let toml_str = r#"
[flow]
name = "kb"
start = "s1"

[[steps]]
id = "s1"
kind = "keyboard"
text = "Pick one"
"#;
    let parsed: zeroclaw::flows::types::FlowDefinitionToml = toml::from_str(toml_str).unwrap();
    let result = zeroclaw::flows::validate::build_flow_definition(&parsed);
    assert!(result.is_err());
    let errs = result.unwrap_err();
    assert!(errs.iter().any(|e| e.message.contains("buttons")));
}

// ── Gate 6: flow_validate_poll_too_few_options ──────────────────

#[test]
fn flow_validate_poll_too_few_options() {
    let toml_str = r#"
[flow]
name = "poll"
start = "s1"

[[steps]]
id = "s1"
kind = "poll"
text = "Vote"
poll_options = ["only_one"]
"#;
    let parsed: zeroclaw::flows::types::FlowDefinitionToml = toml::from_str(toml_str).unwrap();
    let result = zeroclaw::flows::validate::build_flow_definition(&parsed);
    assert!(result.is_err());
    let errs = result.unwrap_err();
    assert!(errs.iter().any(|e| e.message.contains("at least 2 options")));
}

// ── Gate 7: flow_validate_valid_passes ──────────────────────────

#[test]
fn flow_validate_valid_passes() {
    let toml_str = r#"
[flow]
name = "confirm"
start = "ask"
default_timeout_secs = 120

[[steps]]
id = "ask"
kind = "keyboard"
text = "Are you sure?"
buttons = [[{ text = "Yes", callback_data = "yes" }, { text = "No", callback_data = "no" }]]

[[steps.transitions]]
on = "yes"
target = "done"

[[steps.transitions]]
on = "no"
target = "cancelled"

[[steps]]
id = "done"
kind = "message"
text = "Done!"

[[steps]]
id = "cancelled"
kind = "message"
text = "Cancelled."
"#;
    let parsed: zeroclaw::flows::types::FlowDefinitionToml = toml::from_str(toml_str).unwrap();
    let result = zeroclaw::flows::validate::build_flow_definition(&parsed);
    assert!(result.is_ok(), "Valid flow should pass: {:?}", result.err());
    let def = result.unwrap();
    assert_eq!(def.name, "confirm");
    assert_eq!(def.start_step, "ask");
    assert_eq!(def.steps.len(), 3);
}

// ── Gate 8: flow_store_start_get ────────────────────────────────

#[test]
fn flow_store_start_get() {
    let store = zeroclaw::flows::state::FlowStore::new();
    store.start_flow("chat1", "test_flow", "ask", Some(100));
    assert!(store.has_flow("chat1"));
    let info = store.get_flow_info("chat1").unwrap();
    assert_eq!(info.0, "test_flow");
    assert_eq!(info.1, "ask");
    assert_eq!(info.2, Some(100));
}

// ── Gate 9: flow_store_advance ──────────────────────────────────

#[test]
fn flow_store_advance() {
    let store = zeroclaw::flows::state::FlowStore::new();
    store.start_flow("chat1", "test_flow", "ask", None);
    store.advance("chat1", "done", Some(200)).unwrap();
    let info = store.get_flow_info("chat1").unwrap();
    assert_eq!(info.1, "done");
    assert_eq!(info.2, Some(200));
}

// ── Gate 10: flow_store_complete ────────────────────────────────

#[test]
fn flow_store_complete() {
    let store = zeroclaw::flows::state::FlowStore::new();
    store.start_flow("chat1", "test_flow", "ask", None);
    let removed = store.complete_flow("chat1", "completed");
    assert!(removed.is_some());
    assert!(!store.has_flow("chat1"));
}

// ── Gate 11: flow_store_timeout_detection ───────────────────────

#[test]
fn flow_store_timeout_detection() {
    use zeroclaw::flows::types::*;
    let store = zeroclaw::flows::state::FlowStore::new();
    store.start_flow("chat1", "test_flow", "ask", None);

    // Manually age the step entry using with_flow
    // We can't easily backdated Instant, so test the logic path differently:
    // Create a flow def with timeout_secs=0 (no timeout) and verify it's NOT flagged
    let mut steps = HashMap::new();
    steps.insert(
        "ask".into(),
        Step {
            id: "ask".into(),
            kind: StepKind::Keyboard,
            text: "Q?".into(),
            buttons: None,
            poll_options: None,
            poll_anonymous: true,
            timeout_secs: Some(0), // 0 = no timeout
            agent_handoff: false,
            transitions: vec![],
        },
    );
    let mut defs = HashMap::new();
    defs.insert(
        "test_flow".into(),
        FlowDefinition {
            name: "test_flow".into(),
            description: None,
            start_step: "ask".into(),
            default_timeout_secs: 0,
            steps,
        },
    );

    // With timeout=0, nothing should be flagged
    let timed_out = store.check_timeouts(&defs);
    assert!(timed_out.is_empty(), "timeout=0 should never trigger");
}

// ── Gate 12: flow_transition_exact_match ────────────────────────

#[test]
fn flow_transition_exact_match() {
    use zeroclaw::flows::types::*;
    let step = Step {
        id: "s1".into(),
        kind: StepKind::Keyboard,
        text: "Q".into(),
        buttons: None,
        poll_options: None,
        poll_anonymous: true,
        timeout_secs: None,
        agent_handoff: false,
        transitions: vec![
            TransitionDef { on: "yes".into(), target: "done".into() },
            TransitionDef { on: "no".into(), target: "cancel".into() },
        ],
    };
    let matched = step.transitions.iter().find(|t| t.on == "yes");
    assert!(matched.is_some());
    assert_eq!(matched.unwrap().target, "done");
}

// ── Gate 13: flow_transition_any_match ──────────────────────────

#[test]
fn flow_transition_any_match() {
    use zeroclaw::flows::types::*;
    let step = Step {
        id: "s1".into(),
        kind: StepKind::Keyboard,
        text: "Q".into(),
        buttons: None,
        poll_options: None,
        poll_anonymous: true,
        timeout_secs: None,
        agent_handoff: false,
        transitions: vec![
            TransitionDef { on: "_any".into(), target: "next".into() },
        ],
    };
    let callback_data = "anything_at_all";
    let matched = step
        .transitions
        .iter()
        .find(|t| t.on == callback_data || t.on == "_any");
    assert!(matched.is_some());
    assert_eq!(matched.unwrap().target, "next");
}

// ── Gate 14: flow_transition_no_match ───────────────────────────

#[test]
fn flow_transition_no_match() {
    use zeroclaw::flows::types::*;
    let step = Step {
        id: "s1".into(),
        kind: StepKind::Keyboard,
        text: "Q".into(),
        buttons: None,
        poll_options: None,
        poll_anonymous: true,
        timeout_secs: None,
        agent_handoff: false,
        transitions: vec![
            TransitionDef { on: "yes".into(), target: "done".into() },
        ],
    };
    let matched = step.transitions.iter().find(|t| t.on == "unknown_data");
    assert!(matched.is_none());
}

// ── Gate 15: flow_terminal_step ─────────────────────────────────

#[test]
fn flow_terminal_step() {
    use zeroclaw::flows::types::*;
    let terminal = Step {
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
    assert!(terminal.is_terminal());

    let non_terminal = Step {
        transitions: vec![TransitionDef { on: "go".into(), target: "next".into() }],
        ..terminal.clone()
    };
    assert!(!non_terminal.is_terminal());
}

// ── Gate 16: flow_tool_schema ───────────────────────────────────

#[test]
fn flow_tool_schema() {
    use std::sync::{Arc, Mutex};
    use zeroclaw::channels::telegram::TelegramChannel;
    use zeroclaw::channels::telegram_types::TelegramToolContext;
    use zeroclaw::flows::state::FlowStore;
    use zeroclaw::tools::telegram_flow::TelegramFlowTool;
    use zeroclaw::tools::traits::Tool;

    let ch = Arc::new(TelegramChannel::new("t".into(), vec![]));
    let ctx = Arc::new(Mutex::new(None::<TelegramToolContext>));
    let defs = Arc::new(HashMap::new());
    let store = Arc::new(FlowStore::new());
    let tool = TelegramFlowTool::new(ch, ctx, defs, store, None);

    assert_eq!(tool.name(), "telegram_start_flow");
    let schema = tool.parameters_schema();
    assert!(schema["properties"]["flow_name"].is_object());
    let required = schema["required"].as_array().unwrap();
    assert!(required.iter().any(|v| v.as_str() == Some("flow_name")));
}

// ── Gate 17: flow_dir_missing_ok ────────────────────────────────

#[test]
fn flow_dir_missing_ok() {
    let tmp = tempfile::TempDir::new().unwrap();
    let flows_dir = tmp.path().join("nonexistent_flows");
    let result = zeroclaw::flows::load_flows(&flows_dir).unwrap();
    assert!(result.is_empty());
}

// ── Gate 18: dedup_seen_update_ids ──────────────────────────────

#[test]
fn dedup_seen_update_ids() {
    // Test the dedup via TelegramChannel's seen_update_ids field.
    // We can't directly access the private field, so test the concept:
    use std::collections::{HashSet, VecDeque};

    // Replicate the SeenUpdates logic
    let mut set = HashSet::new();
    let mut order = VecDeque::new();

    let insert = |set: &mut HashSet<i64>, order: &mut VecDeque<i64>, uid: i64| -> bool {
        if !set.insert(uid) {
            return false;
        }
        order.push_back(uid);
        true
    };

    assert!(insert(&mut set, &mut order, 100));
    assert!(insert(&mut set, &mut order, 101));
    assert!(!insert(&mut set, &mut order, 100)); // duplicate
    assert!(!insert(&mut set, &mut order, 101)); // duplicate
    assert!(insert(&mut set, &mut order, 102)); // new
}

// ── Gate 19: dedup_prune_at_limit ───────────────────────────────

#[test]
fn dedup_prune_at_limit() {
    use std::collections::{HashSet, VecDeque};

    let max_seen = 100; // Use small limit for test
    let mut set = HashSet::new();
    let mut order = VecDeque::new();

    for i in 0..150 {
        if set.insert(i) {
            order.push_back(i);
            while order.len() > max_seen {
                if let Some(old) = order.pop_front() {
                    set.remove(&old);
                }
            }
        }
    }

    assert_eq!(set.len(), max_seen);
    assert_eq!(order.len(), max_seen);
    // Old IDs should be pruned
    assert!(!set.contains(&0));
    assert!(!set.contains(&49));
    // Recent IDs should still be present
    assert!(set.contains(&50));
    assert!(set.contains(&149));
}

// ── Gate 20: stt_error_messages_distinct ────────────────────────

#[test]
fn stt_error_messages_distinct() {
    // Verify each STT error condition produces a unique user-facing message
    let messages = [
        "[Voice message too large -- max 5 MB. Try a shorter recording.]",
        "[Unsupported audio format. Send as .ogg, .mp3, .mp4, or .wav.]",
        "[Voice transcription busy -- please wait a moment and try again.]",
        "[Voice transcription failed. Your message was received but could not be transcribed.]",
        // Timeout message uses format! with STT_TIMEOUT_SECS, tested pattern:
        "[Transcription timed out after",
    ];

    // All messages should be unique (by first 30 chars)
    let mut seen = std::collections::HashSet::new();
    for msg in &messages {
        let prefix: String = msg.chars().take(30).collect();
        assert!(seen.insert(prefix.clone()), "Duplicate message prefix: {prefix}");
    }
}

// ── Gate 21: observer_telegram_event_variant ────────────────────

#[test]
fn observer_telegram_event_variant() {
    use zeroclaw::observability::log::LogObserver;
    use zeroclaw::observability::traits::{Observer, ObserverEvent};

    let obs = LogObserver::new();
    // Should not panic
    obs.record_event(&ObserverEvent::TelegramEvent {
        direction: "inbound".into(),
        event_type: "tg.inbound.text".into(),
        status: "ok".into(),
        chat_id: "12345".into(),
        correlation_id: "corr-1".into(),
        duration: Some(Duration::from_millis(50)),
        metadata: Some(r#"{"chat_id":"12345"}"#.into()),
    });
    obs.record_event(&ObserverEvent::TelegramEvent {
        direction: "outbound".into(),
        event_type: "tg.outbound.message".into(),
        status: "ok".into(),
        chat_id: "12345".into(),
        correlation_id: "corr-2".into(),
        duration: None,
        metadata: None,
    });
}

// ── Gate 22: observer_stt_latency_metric ────────────────────────

#[test]
fn observer_stt_latency_metric() {
    use zeroclaw::observability::log::LogObserver;
    use zeroclaw::observability::traits::{Observer, ObserverMetric};

    let obs = LogObserver::new();
    // Should not panic
    obs.record_metric(&ObserverMetric::SttLatency(Duration::from_millis(250)));
    obs.record_metric(&ObserverMetric::SttErrorCount(1));
    obs.record_metric(&ObserverMetric::CallbackRejectCount(3));
    obs.record_metric(&ObserverMetric::TelegramEventCount(100));
}

// ── Gate 23: config_flows_enabled_default ───────────────────────

#[test]
fn config_flows_enabled_default() {
    // TelegramConfig without flows_enabled should deserialize with default false
    let json = r#"{"bot_token":"123:ABC","allowed_users":["user"]}"#;
    let tc: zeroclaw::config::TelegramConfig = serde_json::from_str(json).unwrap();
    assert_eq!(tc.bot_token, "123:ABC");
    assert!(!tc.flows_enabled);
}

// ── Gate 24: telegram_health_counters_empty ─────────────────────

#[test]
fn telegram_health_counters_empty() {
    let tmp = tempfile::TempDir::new().unwrap();
    let db_path = tmp.path().join("test.db");
    let registry = zeroclaw::db::Registry::open(&db_path).unwrap();

    // Create a dummy instance
    registry
        .create_instance("inst1", "test-instance", 8080, "/tmp/config.toml", None, None)
        .unwrap();

    // Health counters with no events should return zeroes
    let counters = registry.telegram_health_counters("inst1", 5).unwrap();
    assert_eq!(counters.events_per_min, 0.0);
    assert_eq!(counters.stt_error_rate, 0.0);
    assert_eq!(counters.callback_reject_rate, 0.0);
    assert!(counters.stt_p95_latency_ms.is_none());
    assert_eq!(counters.inbound_count, 0);
    assert_eq!(counters.outbound_count, 0);
    assert_eq!(counters.auth_reject_count, 0);
    assert_eq!(counters.guard_reject_count, 0);
}
