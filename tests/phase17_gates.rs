// Phase 17.0 Gate Tests: Controlled Adaptive Flows -- Core
//
// 28 gates covering: DB schema, flow versioning, policy engine, compose tool,
// config backward compatibility, serde roundtrip, DB fallback, concurrency,
// operator-name collision, timeout cache refresh, and audit log integrity.

use std::collections::HashMap;
use std::sync::Arc;
use zeroclaw::config::FlowPolicyConfig;
use zeroclaw::flows::db::FlowDb;
use zeroclaw::flows::policy;
use zeroclaw::flows::types::*;
use zeroclaw::flows::validate::build_flow_definition;
use zeroclaw::tools::traits::Tool;

// ── Helpers ──────────────────────────────────────────────────────

fn make_toml_def(name: &str, steps: Vec<StepToml>) -> FlowDefinitionToml {
    FlowDefinitionToml {
        flow: FlowMeta {
            name: name.into(),
            description: Some("test flow".into()),
            start: steps.first().map(|s| s.id.clone()).unwrap_or_default(),
            default_timeout_secs: 60,
        },
        steps,
    }
}

fn message_step(id: &str, text: &str) -> StepToml {
    StepToml {
        id: id.into(),
        kind: StepKind::Message,
        text: Some(text.into()),
        buttons: None,
        poll_options: None,
        poll_anonymous: None,
        timeout_secs: None,
        agent_handoff: false,
        transitions: vec![],
    }
}

fn keyboard_step(id: &str, text: &str, handoff: bool) -> StepToml {
    StepToml {
        id: id.into(),
        kind: StepKind::Keyboard,
        text: Some(text.into()),
        buttons: Some(vec![vec![ButtonDef {
            text: "OK".into(),
            callback_data: "ok".into(),
        }]]),
        poll_options: None,
        poll_anonymous: None,
        timeout_secs: None,
        agent_handoff: handoff,
        transitions: vec![TransitionDef {
            on: "ok".into(),
            target: "end".into(),
        }],
    }
}

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

fn make_flow_def(name: &str) -> FlowDefinition {
    let mut steps = HashMap::new();
    steps.insert(
        "s1".into(),
        Step {
            id: "s1".into(),
            kind: StepKind::Message,
            text: "Hello".into(),
            buttons: None,
            poll_options: None,
            poll_anonymous: true,
            timeout_secs: None,
            agent_handoff: false,
            transitions: vec![],
        },
    );
    FlowDefinition {
        name: name.into(),
        description: Some("test".into()),
        start_step: "s1".into(),
        default_timeout_secs: 60,
        steps,
    }
}

// ── DB Gates (1-10, 25, 28) ─────────────────────────────────────

// Gate 1: flow_versions table created
#[test]
fn gate1_flow_versions_table_created() {
    let db = FlowDb::open_in_memory().unwrap();
    // Verify the table exists by successfully creating a version
    let v = db
        .create_flow_version("test_flow", "{}", "agent", "agent")
        .unwrap();
    assert_eq!(v, 1, "flow_versions table should exist and accept inserts");
    // Also verify list works
    let versions = db.list_flow_versions("test_flow").unwrap();
    assert_eq!(versions.len(), 1);
}

// Gate 2: flow_audit_log table created
#[test]
fn gate2_flow_audit_log_table_created() {
    let db = FlowDb::open_in_memory().unwrap();
    // Verify the table exists by successfully logging an audit entry
    db.log_audit("test_flow", Some(1), "created", "agent", None)
        .unwrap();
    let (logs, total) = db.list_audit_log(10, 0, None).unwrap();
    assert_eq!(total, 1, "flow_audit_log table should exist and accept inserts");
    assert_eq!(logs[0].event, "created");
}

// Gate 3: create_version returns v1
#[test]
fn gate3_create_version_returns_v1() {
    let db = FlowDb::open_in_memory().unwrap();
    let v = db
        .create_flow_version("test_flow", "{}", "agent", "agent")
        .unwrap();
    assert_eq!(v, 1);
}

// Gate 4: create_second_version returns v2
#[test]
fn gate4_create_second_version_returns_v2() {
    let db = FlowDb::open_in_memory().unwrap();
    let v1 = db
        .create_flow_version("test_flow", "{}", "agent", "agent")
        .unwrap();
    let v2 = db
        .create_flow_version("test_flow", "{}", "agent", "agent")
        .unwrap();
    assert_eq!(v1, 1);
    assert_eq!(v2, 2);
}

// Gate 5: activate_deactivates_previous
#[test]
fn gate5_activate_deactivates_previous() {
    let db = FlowDb::open_in_memory().unwrap();
    let v1 = db
        .create_flow_version("test_flow", "{}", "agent", "agent")
        .unwrap();
    db.activate_version("test_flow", v1).unwrap();

    let v2 = db
        .create_flow_version("test_flow", "{}", "agent", "agent")
        .unwrap();
    db.activate_version("test_flow", v2).unwrap();

    // v1 should now be deactivated
    let row = db.get_flow_version("test_flow", v1).unwrap().unwrap();
    assert_eq!(row.status, "deactivated");

    // v2 should be active
    let row = db.get_flow_version("test_flow", v2).unwrap().unwrap();
    assert_eq!(row.status, "active");
}

// Gate 6: get_active_version returns correct row
#[test]
fn gate6_get_active_version_correct() {
    let db = FlowDb::open_in_memory().unwrap();
    let v1 = db
        .create_flow_version("test_flow", r#"{"hello":"world"}"#, "agent", "agent")
        .unwrap();
    db.activate_version("test_flow", v1).unwrap();

    let active = db.get_active_version("test_flow").unwrap().unwrap();
    assert_eq!(active.version, v1);
    assert_eq!(active.status, "active");
    assert_eq!(active.definition_json, r#"{"hello":"world"}"#);
}

// Gate 7: list_pending_review only returns pending_review rows
#[test]
fn gate7_list_pending_review() {
    let db = FlowDb::open_in_memory().unwrap();
    let v1 = db
        .create_flow_version("flow_a", "{}", "agent", "agent")
        .unwrap();
    db.update_version_status("flow_a", v1, "pending_review", None)
        .unwrap();

    let v2 = db
        .create_flow_version("flow_b", "{}", "agent", "agent")
        .unwrap();
    db.activate_version("flow_b", v2).unwrap();

    let pending = db.list_pending_review().unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].flow_name, "flow_a");
}

// Gate 8: count_agent_flows
#[test]
fn gate8_count_agent_flows() {
    let db = FlowDb::open_in_memory().unwrap();
    db.create_flow_version("flow_a", "{}", "agent", "agent")
        .unwrap();
    db.create_flow_version("flow_a", "{}", "agent", "agent")
        .unwrap(); // v2 same name
    db.create_flow_version("flow_b", "{}", "agent", "agent")
        .unwrap();
    db.create_flow_version("flow_c", "{}", "operator", "system")
        .unwrap();

    let count = db.count_agent_flows().unwrap();
    assert_eq!(count, 2, "Should count 2 distinct agent flow_names (flow_a, flow_b)");
}

// Gate 9: audit_log_on_create roundtrip
#[test]
fn gate9_audit_log_on_create() {
    let db = FlowDb::open_in_memory().unwrap();
    db.log_audit("test_flow", Some(1), "created", "agent", None)
        .unwrap();
    db.log_audit("test_flow", Some(1), "validated", "agent", None)
        .unwrap();

    let (logs, total) = db.list_audit_log(10, 0, None).unwrap();
    assert_eq!(total, 2);
    assert_eq!(logs.len(), 2);
    // Both events should be present (order may vary with same-second timestamps)
    let events: Vec<&str> = logs.iter().map(|l| l.event.as_str()).collect();
    assert!(events.contains(&"created"));
    assert!(events.contains(&"validated"));
}

// Gate 10: audit_log filter by name
#[test]
fn gate10_audit_log_filter_by_name() {
    let db = FlowDb::open_in_memory().unwrap();
    db.log_audit("flow_a", Some(1), "created", "agent", None)
        .unwrap();
    db.log_audit("flow_b", Some(1), "created", "agent", None)
        .unwrap();
    db.log_audit("flow_a", Some(1), "activated", "agent", None)
        .unwrap();

    let (logs, total) = db.list_audit_log(10, 0, Some("flow_a")).unwrap();
    assert_eq!(total, 2);
    assert!(logs.iter().all(|l| l.flow_name == "flow_a"));
}

// ── Policy Gates (11-18, 26) ────────────────────────────────────

// Gate 11: policy rejects denied kind
#[test]
fn gate11_policy_rejects_denied_kind() {
    let mut step = message_step("s1", "Hello");
    step.kind = StepKind::Edit;
    let toml = make_toml_def("test", vec![step]);
    let pol = make_policy();
    let err = policy::check_policy(&toml, &pol, 0).unwrap_err();
    assert!(err.iter().any(|v| v.message.contains("denied kind")));
}

// Gate 12: policy rejects too many steps
#[test]
fn gate12_policy_rejects_too_many_steps() {
    let steps: Vec<StepToml> = (0..12)
        .map(|i| message_step(&format!("s{i}"), "Hi"))
        .collect();
    let toml = make_toml_def("test", steps);
    let pol = make_policy();
    let err = policy::check_policy(&toml, &pol, 0).unwrap_err();
    assert!(err.iter().any(|v| v.message.contains("exceeds max_steps")));
}

// Gate 13: policy requires handoff on keyboard
#[test]
fn gate13_policy_requires_handoff_on_keyboard() {
    let toml = make_toml_def(
        "test",
        vec![
            keyboard_step("s1", "Choose:", false), // handoff = false
            message_step("end", "Done"),
        ],
    );
    let pol = make_policy();
    let err = policy::check_policy(&toml, &pol, 0).unwrap_err();
    assert!(err
        .iter()
        .any(|v| v.message.contains("require_handoff_on_keyboard")));
}

// Gate 14: policy rejects denied text
#[test]
fn gate14_policy_rejects_denied_text() {
    let toml = make_toml_def(
        "test",
        vec![message_step("s1", "Click <script>alert(1)</script>")],
    );
    let pol = make_policy();
    let err = policy::check_policy(&toml, &pol, 0).unwrap_err();
    assert!(err.iter().any(|v| v.message.contains("denied pattern")));
}

// Gate 15: policy passes valid flow
#[test]
fn gate15_policy_passes_valid_flow() {
    let toml = make_toml_def("test", vec![message_step("s1", "Hello!")]);
    let pol = make_policy();
    assert!(policy::check_policy(&toml, &pol, 0).is_ok());
}

// Gate 16: policy rejects over max_agent_flows
#[test]
fn gate16_policy_rejects_over_max_agent_flows() {
    let toml = make_toml_def("test", vec![message_step("s1", "Hello!")]);
    let pol = make_policy();
    let err = policy::check_policy(&toml, &pol, 50).unwrap_err();
    assert!(err.iter().any(|v| v.message.contains("max_agent_flows")));
}

// Gate 17: auto_approve message-only
#[test]
fn gate17_auto_approve_message_only() {
    let toml = make_toml_def("test", vec![message_step("s1", "Hi")]);
    let mut pol = make_policy();
    pol.auto_approve = true;
    assert!(policy::qualifies_for_auto_approve(&toml, &pol));
}

// Gate 18: auto_approve denied for keyboard
#[test]
fn gate18_auto_approve_denied_for_keyboard() {
    let toml = make_toml_def(
        "test",
        vec![
            keyboard_step("s1", "Choose:", true),
            message_step("end", "Done"),
        ],
    );
    let mut pol = make_policy();
    pol.auto_approve = true;
    assert!(!policy::qualifies_for_auto_approve(&toml, &pol));
}

// ── Tool Gates (19-20) ─────────────────────────────────────────

// Gate 19: compose tool schema valid
#[test]
fn gate19_compose_tool_schema_valid() {
    let db = Arc::new(FlowDb::open_in_memory().unwrap());
    let policy = FlowPolicyConfig::default();
    let defs = Arc::new(HashMap::new());
    let tool = zeroclaw::tools::telegram_compose::TelegramComposeFlowTool::new(db, policy, defs);
    let schema = tool.parameters_schema();
    assert!(schema["properties"]["flow_name"].is_object());
    assert!(schema["properties"]["steps"].is_object());
    assert!(schema["properties"]["start_step"].is_object());
    assert_eq!(tool.name(), "telegram_compose_flow");

    let required = schema["required"].as_array().unwrap();
    assert!(required.iter().any(|v| v.as_str() == Some("flow_name")));
    assert!(required.iter().any(|v| v.as_str() == Some("steps")));
    assert!(required.iter().any(|v| v.as_str() == Some("start_step")));
}

// Gate 20: compose tool rejects when authoring disabled
#[tokio::test]
async fn gate20_compose_tool_rejects_disabled() {
    let db = Arc::new(FlowDb::open_in_memory().unwrap());
    let policy = FlowPolicyConfig::default(); // agent_authoring_enabled = false
    let defs = Arc::new(HashMap::new());
    let tool = zeroclaw::tools::telegram_compose::TelegramComposeFlowTool::new(db, policy, defs);

    let args = serde_json::json!({
        "flow_name": "test_flow",
        "start_step": "s1",
        "steps": [{
            "id": "s1",
            "kind": "message",
            "text": "Hello"
        }]
    });

    let result = tool.execute(args).await.unwrap();
    assert!(!result.success);
    assert!(result.error.as_deref().unwrap_or("").contains("disabled"));
}

// ── Serde Gate (21) ─────────────────────────────────────────────

// Gate 21: FlowDefinitionToml JSON roundtrip
#[test]
fn gate21_flow_def_toml_json_roundtrip() {
    let toml = make_toml_def(
        "roundtrip_test",
        vec![
            message_step("s1", "Hello"),
            keyboard_step("s2", "Choose:", true),
            message_step("end", "Done"),
        ],
    );

    let json = serde_json::to_string(&toml).unwrap();
    let parsed: FlowDefinitionToml = serde_json::from_str(&json).unwrap();

    assert_eq!(parsed.flow.name, "roundtrip_test");
    assert_eq!(parsed.steps.len(), 3);
    assert_eq!(parsed.steps[0].id, "s1");
    assert_eq!(parsed.steps[1].kind, StepKind::Keyboard);
}

// ── Config Gates (22-23) ────────────────────────────────────────

// Gate 22: FlowPolicyConfig defaults secure
#[test]
fn gate22_flow_policy_config_defaults_secure() {
    let policy = FlowPolicyConfig::default();
    assert!(
        !policy.agent_authoring_enabled,
        "Default policy should have agent_authoring_enabled = false"
    );
    assert!(
        policy.require_handoff_on_keyboard,
        "Default policy should require handoff on keyboard"
    );
    assert!(
        !policy.auto_approve,
        "Default policy should have auto_approve = false"
    );
}

// Gate 23: Config backward compatibility -- TOML without [flow_policy] parses
#[test]
fn gate23_flow_policy_config_backward_compat() {
    let toml_str = r#"
        bot_token = "123:test"
        allowed_users = ["user1"]
    "#;

    let config: zeroclaw::config::TelegramConfig = toml::from_str(toml_str).unwrap();
    assert_eq!(config.bot_token, "123:test");
    // flow_policy should default to secure values
    assert!(!config.flow_policy.agent_authoring_enabled);
    assert!(config.flow_policy.require_handoff_on_keyboard);
    assert_eq!(config.flow_policy.max_steps, 10);
}

// ── Integration Gate (24) ───────────────────────────────────────

// Gate 24: DB fallback finds active version for start_flow
#[test]
fn gate24_db_fallback_finds_active_version() {
    let db = Arc::new(FlowDb::open_in_memory().unwrap());

    // Create and activate a flow definition in the DB
    let toml_def = make_toml_def("agent_flow", vec![message_step("s1", "Hi from DB")]);
    let json = serde_json::to_string(&toml_def).unwrap();
    let v = db
        .create_flow_version("agent_flow", &json, "agent", "agent")
        .unwrap();
    db.activate_version("agent_flow", v).unwrap();

    // Verify the DB fallback lookup chain works
    let active = db.get_active_version("agent_flow").unwrap().unwrap();
    assert_eq!(active.version, v);
    assert_eq!(active.status, "active");

    // Parse the stored definition
    let parsed: FlowDefinitionToml = serde_json::from_str(&active.definition_json).unwrap();
    assert_eq!(parsed.flow.name, "agent_flow");

    // Build the flow definition (validates structure)
    let flow_def = build_flow_definition(&parsed).unwrap();
    assert_eq!(flow_def.start_step, "s1");
    assert!(flow_def.steps.contains_key("s1"));
}

// ── Concurrency Gate (25) ───────────────────────────────────────

// Gate 25: Concurrent version creation produces sequential versions
#[test]
fn gate25_concurrent_version_creation() {
    let db = Arc::new(FlowDb::open_in_memory().unwrap());
    let num_threads = 10;

    let mut handles = Vec::new();
    for _ in 0..num_threads {
        let db_clone = db.clone();
        handles.push(std::thread::spawn(move || {
            db_clone
                .create_flow_version("test_flow", "{}", "agent", "agent")
                .unwrap()
        }));
    }

    let mut versions: Vec<i64> = handles
        .into_iter()
        .map(|h| h.join().unwrap())
        .collect();
    versions.sort();

    // All versions should be unique and sequential 1..=N
    let expected: Vec<i64> = (1..=num_threads as i64).collect();
    assert_eq!(
        versions, expected,
        "Concurrent version creation should produce sequential unique versions"
    );

    // Verify all exist in DB
    let all = db.list_flow_versions("test_flow").unwrap();
    assert_eq!(all.len(), num_threads);
}

// ── Operator Name Collision Gate (26) ───────────────────────────

// Gate 26: operator_name_collision_rejected
#[test]
fn gate26_operator_name_collision_rejected() {
    let mut operator_defs = HashMap::new();
    operator_defs.insert("onboarding".to_string(), make_flow_def("onboarding"));

    assert!(
        policy::is_operator_owned("onboarding", &operator_defs),
        "onboarding should be operator-owned"
    );
    assert!(
        !policy::is_operator_owned("agent_custom_flow", &operator_defs),
        "agent_custom_flow should not be operator-owned"
    );
}

// ── Timeout Cache Refresh Gate (27) ─────────────────────────────

// Gate 27: Timeout cache refresh on activation
//
// Tests the version-aware cache invalidation logic: create v1 (active),
// cache it, activate v2, verify lookup sees v2's definition.
#[test]
fn gate27_timeout_cache_refresh_on_activation() {
    let db = Arc::new(FlowDb::open_in_memory().unwrap());

    // Create v1 with "Hello v1" text
    let toml_v1 = make_toml_def("cache_test", vec![message_step("s1", "Hello v1")]);
    let json_v1 = serde_json::to_string(&toml_v1).unwrap();
    let v1 = db
        .create_flow_version("cache_test", &json_v1, "agent", "agent")
        .unwrap();
    db.activate_version("cache_test", v1).unwrap();

    // Simulate initial cache population
    let active_v1 = db.get_active_version("cache_test").unwrap().unwrap();
    let parsed_v1: FlowDefinitionToml =
        serde_json::from_str(&active_v1.definition_json).unwrap();
    let def_v1 = build_flow_definition(&parsed_v1).unwrap();

    let mut cache: HashMap<String, (i64, FlowDefinition)> = HashMap::new();
    cache.insert("cache_test".into(), (active_v1.id, def_v1));

    // Verify cache has v1
    assert_eq!(cache["cache_test"].1.steps["s1"].text, "Hello v1");

    // Create and activate v2 with different text
    let toml_v2 = make_toml_def("cache_test", vec![message_step("s1", "Hello v2")]);
    let json_v2 = serde_json::to_string(&toml_v2).unwrap();
    let v2 = db
        .create_flow_version("cache_test", &json_v2, "agent", "agent")
        .unwrap();
    db.activate_version("cache_test", v2).unwrap();

    // Simulate cache check (what the timeout checker does each tick)
    let active_now = db.get_active_version("cache_test").unwrap().unwrap();
    let cached_valid = cache
        .get("cache_test")
        .map(|(cid, _)| *cid == active_now.id)
        .unwrap_or(false);

    assert!(!cached_valid, "Cache should be stale after activation change");

    // Re-parse and update cache
    let parsed_v2: FlowDefinitionToml =
        serde_json::from_str(&active_now.definition_json).unwrap();
    let def_v2 = build_flow_definition(&parsed_v2).unwrap();
    cache.insert("cache_test".into(), (active_now.id, def_v2));

    // Verify cache now has v2
    assert_eq!(cache["cache_test"].1.steps["s1"].text, "Hello v2");
}

// ── Audit Log Append-Only Gate (28) ─────────────────────────────

// Gate 28: audit_log_append_only -- UPDATE and DELETE are blocked by triggers
#[test]
fn gate28_audit_log_append_only() {
    let db = FlowDb::open_in_memory().unwrap();
    db.log_audit("test", Some(1), "created", "agent", None)
        .unwrap();

    let conn = db.conn();

    // UPDATE should fail
    let update_result = conn.execute(
        "UPDATE flow_audit_log SET event = 'tampered' WHERE id = 1",
        [],
    );
    assert!(
        update_result.is_err(),
        "UPDATE on flow_audit_log should be blocked by trigger"
    );
    let err_msg = update_result.unwrap_err().to_string();
    assert!(
        err_msg.contains("append-only"),
        "Error should mention append-only: {err_msg}"
    );

    // DELETE should fail
    let delete_result = conn.execute("DELETE FROM flow_audit_log WHERE id = 1", []);
    assert!(
        delete_result.is_err(),
        "DELETE on flow_audit_log should be blocked by trigger"
    );
    let err_msg = delete_result.unwrap_err().to_string();
    assert!(
        err_msg.contains("append-only"),
        "Error should mention append-only: {err_msg}"
    );

    // INSERT should still work (verify append-only doesn't block inserts)
    drop(conn);
    db.log_audit("test", Some(1), "validated", "agent", None)
        .unwrap();
    let (logs, _) = db.list_audit_log(10, 0, None).unwrap();
    assert_eq!(logs.len(), 2, "Both audit entries should exist");
}
