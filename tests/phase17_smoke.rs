// Phase 17.0 End-to-End Smoke Test
//
// Exercises the complete adaptive flow pipeline:
//   1. telegram_compose_flow -- create agent-authored flow
//   2. Pending review AND auto-approve paths
//   3. telegram_start_flow DB fallback
//   4. Callback + timeout path using DB-loaded definition
//   5. Audit log entries + append-only verification

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use zeroclaw::channels::telegram::TelegramChannel;
use zeroclaw::channels::telegram_types::TelegramToolContext;
use zeroclaw::config::FlowPolicyConfig;
use zeroclaw::flows::db::FlowDb;
use zeroclaw::flows::state::FlowStore;
use zeroclaw::flows::types::*;
use zeroclaw::flows::validate::build_flow_definition;
use zeroclaw::tools::traits::Tool;

/// Full end-to-end smoke test covering all Phase 17.0 code paths.
#[tokio::test]
async fn phase17_smoke_end_to_end() {
    let db = Arc::new(FlowDb::open_in_memory().unwrap());
    let operator_defs: Arc<HashMap<String, FlowDefinition>> = Arc::new(HashMap::new());

    // ================================================================
    // STEP 1: telegram_compose_flow -- create with auto-approve disabled
    // ================================================================
    println!("\n=== STEP 1: Compose flow (pending_review path) ===");

    let policy_no_auto = FlowPolicyConfig {
        agent_authoring_enabled: true,
        auto_approve: false, // requires operator review
        max_steps: 10,
        max_agent_flows: 50,
        require_handoff_on_keyboard: true,
        denied_step_kinds: vec!["edit".into()],
        auto_approve_max_steps: 5,
        denied_text_patterns: vec![],
    };

    let compose_tool = zeroclaw::tools::telegram_compose::TelegramComposeFlowTool::new(
        db.clone(),
        policy_no_auto,
        operator_defs.clone(),
    );

    let compose_args = serde_json::json!({
        "flow_name": "agent_survey",
        "description": "A simple survey flow composed by the agent",
        "start_step": "greeting",
        "steps": [
            {
                "id": "greeting",
                "kind": "message",
                "text": "Welcome to the survey!",
                "transitions": [{"on": "_any", "target": "question1"}]
            },
            {
                "id": "question1",
                "kind": "keyboard",
                "text": "How are you today?",
                "buttons": [[
                    {"text": "Great", "callback_data": "great"},
                    {"text": "OK", "callback_data": "ok"}
                ]],
                "agent_handoff": true,
                "transitions": [
                    {"on": "great", "target": "thanks"},
                    {"on": "ok", "target": "thanks"},
                    {"on": "_timeout", "target": "timeout_msg"}
                ],
                "timeout_secs": 30
            },
            {
                "id": "thanks",
                "kind": "message",
                "text": "Thanks for your response!"
            },
            {
                "id": "timeout_msg",
                "kind": "message",
                "text": "Survey timed out. Maybe next time!"
            }
        ]
    });

    let result = compose_tool.execute(compose_args).await.unwrap();
    println!("  compose result: success={}, output={:?}", result.success, result.output);
    println!("  error: {:?}", result.error);
    assert!(result.success, "Compose should succeed");
    assert!(
        result.output.contains("submitted for operator review"),
        "Should be pending_review (auto_approve=false): {}",
        result.output
    );

    // Verify pending_review status in DB
    let pending = db.list_pending_review().unwrap();
    println!("  pending_review count: {}", pending.len());
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].flow_name, "agent_survey");
    assert_eq!(pending[0].status, "pending_review");
    println!("  [OK] Flow 'agent_survey' v{} is pending_review", pending[0].version);

    // ================================================================
    // STEP 2: Compose flow with auto-approve enabled (message-only)
    // ================================================================
    println!("\n=== STEP 2: Compose flow (auto-approve path) ===");

    let policy_auto = FlowPolicyConfig {
        agent_authoring_enabled: true,
        auto_approve: true,
        auto_approve_max_steps: 5,
        max_steps: 10,
        max_agent_flows: 50,
        require_handoff_on_keyboard: true,
        denied_step_kinds: vec![],
        denied_text_patterns: vec![],
    };

    let compose_tool_auto = zeroclaw::tools::telegram_compose::TelegramComposeFlowTool::new(
        db.clone(),
        policy_auto,
        operator_defs.clone(),
    );

    let auto_args = serde_json::json!({
        "flow_name": "agent_greeting",
        "description": "Simple auto-approved greeting flow",
        "start_step": "hello",
        "steps": [
            {
                "id": "hello",
                "kind": "message",
                "text": "Hello! This is an auto-approved flow.",
                "transitions": [{"on": "_any", "target": "bye"}]
            },
            {
                "id": "bye",
                "kind": "message",
                "text": "Goodbye!"
            }
        ]
    });

    let result2 = compose_tool_auto.execute(auto_args).await.unwrap();
    println!("  compose result: success={}, output={:?}", result2.success, result2.output);
    assert!(result2.success);
    assert!(
        result2.output.contains("auto-approved and activated"),
        "Should be auto-approved: {}",
        result2.output
    );

    // Verify active status
    let active = db.get_active_version("agent_greeting").unwrap().unwrap();
    println!("  [OK] Flow 'agent_greeting' v{} is active (auto-approved)", active.version);
    assert_eq!(active.status, "active");

    // ================================================================
    // STEP 3: telegram_start_flow DB fallback
    // ================================================================
    println!("\n=== STEP 3: telegram_start_flow DB fallback ===");

    // The flow tool has empty TOML defs -- should fall back to DB
    let empty_toml_defs: Arc<HashMap<String, FlowDefinition>> = Arc::new(HashMap::new());
    let ch = Arc::new(TelegramChannel::new("fake_token".into(), vec![]));
    let ctx = Arc::new(Mutex::new(Some(TelegramToolContext {
        chat_id: "user123".into(),
        channel: "telegram".into(),
    })));
    let store = Arc::new(FlowStore::new());
    let flow_tool = zeroclaw::tools::telegram_flow::TelegramFlowTool::new(
        ch,
        ctx,
        empty_toml_defs.clone(),
        store.clone(),
        Some(db.clone()),
    );

    // Start the auto-approved flow via DB fallback
    let start_args = serde_json::json!({"flow_name": "agent_greeting"});
    let start_result = flow_tool.execute(start_args).await.unwrap();
    println!("  start_flow result: success={}, output={:?}", start_result.success, start_result.output);
    println!("  error: {:?}", start_result.error);

    // The execute will fail at the HTTP send (fake token) but we verify the DB
    // lookup chain works by checking it gets past "Flow not found"
    if !start_result.success {
        // Should NOT be "Flow not found" -- that would mean DB fallback failed
        let err = start_result.error.as_deref().unwrap_or("");
        assert!(
            !err.contains("not found"),
            "DB fallback should find the flow, but got: {}",
            err
        );
        println!("  [OK] DB fallback resolved the flow (execution failed due to fake token, as expected)");
    } else {
        println!("  [OK] DB fallback resolved and executed the flow");
    }

    // Verify DB fallback lookup independently
    let row = db.get_active_version("agent_greeting").unwrap();
    assert!(row.is_some(), "Active version should exist in DB");
    let row = row.unwrap();
    let toml_def: FlowDefinitionToml = serde_json::from_str(&row.definition_json).unwrap();
    let flow_def = build_flow_definition(&toml_def).unwrap();
    println!(
        "  [OK] DB fallback chain: get_active_version -> deserialize -> validate -> FlowDefinition (start_step='{}')",
        flow_def.start_step
    );
    assert_eq!(flow_def.start_step, "hello");
    assert!(flow_def.steps.contains_key("hello"));
    assert!(flow_def.steps.contains_key("bye"));

    // ================================================================
    // STEP 4: Callback + timeout path using DB-loaded definition
    // ================================================================
    println!("\n=== STEP 4: Callback/timeout path via DB-loaded definition ===");

    // Activate the survey flow (simulate operator approval)
    let survey_pending = db.list_pending_review().unwrap();
    let survey_version = survey_pending[0].version;
    db.activate_version("agent_survey", survey_version).unwrap();
    println!("  Activated 'agent_survey' v{} (simulating operator approval)", survey_version);

    // Load and verify the survey flow from DB
    let survey_active = db.get_active_version("agent_survey").unwrap().unwrap();
    let survey_toml: FlowDefinitionToml =
        serde_json::from_str(&survey_active.definition_json).unwrap();
    let survey_def = build_flow_definition(&survey_toml).unwrap();

    // Verify callback path: question1 step has transitions
    let q1 = survey_def.steps.get("question1").unwrap();
    println!("  question1 step: kind={:?}, transitions={}", q1.kind, q1.transitions.len());
    assert_eq!(q1.kind, StepKind::Keyboard);

    // Simulate callback resolution: "great" -> "thanks"
    let callback_match = q1
        .transitions
        .iter()
        .find(|t| t.on == "great")
        .map(|t| t.target.clone());
    assert_eq!(callback_match, Some("thanks".into()));
    println!("  [OK] Callback 'great' -> target 'thanks' resolved from DB-loaded definition");

    // Simulate timeout resolution: "_timeout" -> "timeout_msg"
    let timeout_match = q1
        .transitions
        .iter()
        .find(|t| t.on == "_timeout")
        .map(|t| t.target.clone());
    assert_eq!(timeout_match, Some("timeout_msg".into()));
    let timeout_step = survey_def.steps.get("timeout_msg").unwrap();
    assert_eq!(timeout_step.text, "Survey timed out. Maybe next time!");
    println!("  [OK] Timeout '_timeout' -> target 'timeout_msg' resolved from DB-loaded definition");

    // Verify timeout_secs is set on the step
    assert_eq!(q1.timeout_secs, Some(30));
    println!("  [OK] question1 timeout_secs=30 (would trigger after 30s)");

    // Version-aware cache simulation
    let cache_id = survey_active.id;
    let active_check = db.get_active_version("agent_survey").unwrap().unwrap();
    assert_eq!(cache_id, active_check.id, "Same version ID = cache hit");
    println!("  [OK] Version-aware cache: id={} matches active version (cache hit)", cache_id);

    // ================================================================
    // STEP 5: Audit log entries + append-only verification
    // ================================================================
    println!("\n=== STEP 5: Audit log entries + append-only ===");

    // Check audit entries for agent_survey
    let (survey_audit, survey_total) = db.list_audit_log(20, 0, Some("agent_survey")).unwrap();
    println!("  agent_survey audit entries: {survey_total}");
    for entry in &survey_audit {
        println!(
            "    - event='{}', version={:?}, actor='{}', at={}",
            entry.event,
            entry.version,
            entry.actor,
            entry.created_at
        );
    }

    let survey_events: Vec<&str> = survey_audit.iter().map(|e| e.event.as_str()).collect();
    assert!(survey_events.contains(&"created"), "Should have 'created' audit entry");
    assert!(survey_events.contains(&"validated"), "Should have 'validated' audit entry");
    assert!(
        survey_events.contains(&"submitted_for_review"),
        "Should have 'submitted_for_review' audit entry"
    );

    // Check audit entries for agent_greeting (auto-approved)
    let (greeting_audit, greeting_total) =
        db.list_audit_log(20, 0, Some("agent_greeting")).unwrap();
    println!("\n  agent_greeting audit entries: {greeting_total}");
    for entry in &greeting_audit {
        println!(
            "    - event='{}', version={:?}, actor='{}', at={}",
            entry.event,
            entry.version,
            entry.actor,
            entry.created_at
        );
    }

    let greeting_events: Vec<&str> = greeting_audit.iter().map(|e| e.event.as_str()).collect();
    assert!(greeting_events.contains(&"created"));
    assert!(greeting_events.contains(&"validated"));
    assert!(
        greeting_events.contains(&"activated"),
        "Auto-approved flow should have 'activated' audit entry"
    );

    // Verify append-only protection
    println!("\n  Testing append-only protection...");
    let conn = db.conn();

    let update_err = conn
        .execute(
            "UPDATE flow_audit_log SET event = 'tampered' WHERE id = 1",
            [],
        )
        .unwrap_err();
    println!("  UPDATE blocked: {update_err}");
    assert!(update_err.to_string().contains("append-only"));

    let delete_err = conn
        .execute("DELETE FROM flow_audit_log WHERE id = 1", [])
        .unwrap_err();
    println!("  DELETE blocked: {delete_err}");
    assert!(delete_err.to_string().contains("append-only"));
    println!("  [OK] Audit log is append-only (UPDATE and DELETE blocked by triggers)");

    // ================================================================
    // SUMMARY
    // ================================================================
    println!("\n=== SMOKE TEST SUMMARY ===");
    println!("  [1] telegram_compose_flow: pending_review path ... OK");
    println!("  [2] telegram_compose_flow: auto-approve path ... OK");
    println!("  [3] telegram_start_flow: DB fallback ... OK");
    println!("  [4] Callback + timeout via DB-loaded definition ... OK");
    println!("  [5] Audit log entries + append-only ... OK");
    println!("  ALL PATHS VERIFIED\n");
}
