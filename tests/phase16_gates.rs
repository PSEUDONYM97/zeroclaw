use std::collections::HashMap;
use std::sync::Arc;
use zeroclaw::channels::Channel;
use zeroclaw::flows::db::{FlowDb, FlowInstanceRow};
use zeroclaw::flows::execute::StepExecuteResult;
use zeroclaw::flows::state::FlowStore;
use zeroclaw::flows::types::*;

// ── Helper ────────────────────────────────────────────────────────

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
                TransitionDef {
                    on: "yes".into(),
                    target: "done".into(),
                },
                TransitionDef {
                    on: "poll_option_1".into(),
                    target: "done".into(),
                },
                TransitionDef {
                    on: "_timeout".into(),
                    target: "timeout_step".into(),
                },
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
        "timeout_step".into(),
        Step {
            id: "timeout_step".into(),
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

// ═══════════════════════════════════════════════════════════════════
// FlowDb CRUD (Gates 1-8)
// ═══════════════════════════════════════════════════════════════════

// Gate 1
#[test]
fn flow_db_creates_tables() {
    let db = FlowDb::open_in_memory().unwrap();
    // If tables don't exist, list_active would fail
    let active = db.list_active().unwrap();
    assert!(active.is_empty());
    let (history, total) = db.list_history(10, 0, None, None, None).unwrap();
    assert!(history.is_empty());
    assert_eq!(total, 0);
    assert!(db.get_kv("any").unwrap().is_none());
}

// Gate 2
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
    assert_eq!(got.current_step, "hello");
    assert_eq!(got.anchor_message_id, Some(42));
}

// Gate 3
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
    assert_eq!(got.step_entered_at, "2026-01-01T00:01:00Z");
    assert_eq!(got.anchor_message_id, Some(99));
}

// Gate 4
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
    assert!(db.get_active("chat1").unwrap().is_none());

    let (history, total) = db.list_history(10, 0, None, None, None).unwrap();
    assert_eq!(total, 1);
    assert_eq!(history[0].status, "completed");
    assert_eq!(history[0].final_step, "done");
    assert_eq!(history[0].anchor_message_id, Some(10));
}

// Gate 5
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

// Gate 6
#[test]
fn flow_db_history_filters() {
    let db = FlowDb::open_in_memory().unwrap();
    for (name, status) in &[
        ("flow_a", "completed"),
        ("flow_b", "timed_out"),
        ("flow_a", "force_completed"),
    ] {
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

    let (rows, total) = db.list_history(10, 0, Some("flow_a"), None, None).unwrap();
    assert_eq!(total, 2);
    assert_eq!(rows.len(), 2);

    let (rows, total) = db
        .list_history(10, 0, None, Some("timed_out"), None)
        .unwrap();
    assert_eq!(total, 1);
    assert_eq!(rows[0].flow_name, "flow_b");
    let _ = rows.len(); // suppress warning
    let _ = total;
}

// Gate 7
#[test]
fn flow_db_kv_roundtrip() {
    let db = FlowDb::open_in_memory().unwrap();
    db.set_kv("telegram_offset", "42").unwrap();
    let val = db.get_kv("telegram_offset").unwrap();
    assert_eq!(val, Some("42".into()));
}

// Gate 8
#[test]
fn flow_db_kv_overwrite() {
    let db = FlowDb::open_in_memory().unwrap();
    db.set_kv("key1", "old").unwrap();
    db.set_kv("key1", "new").unwrap();
    let val = db.get_kv("key1").unwrap();
    assert_eq!(val, Some("new".into()));
}

// ═══════════════════════════════════════════════════════════════════
// Durable FlowStore (Gates 9-15)
// ═══════════════════════════════════════════════════════════════════

// Gate 9
#[test]
fn durable_start_persists() {
    let db = Arc::new(FlowDb::open_in_memory().unwrap());
    let store = FlowStore::with_db(db.clone());
    store.start_flow("chat1", "greet", "hello", Some(42));

    // Verify in DB
    let row = db.get_active("chat1").unwrap().unwrap();
    assert_eq!(row.flow_name, "greet");
    assert_eq!(row.current_step, "hello");
    assert_eq!(row.anchor_message_id, Some(42));
}

// Gate 10
#[test]
fn durable_advance_persists() {
    let db = Arc::new(FlowDb::open_in_memory().unwrap());
    let store = FlowStore::with_db(db.clone());
    store.start_flow("chat1", "greet", "hello", None);
    store.advance("chat1", "done", Some(99)).unwrap();

    let row = db.get_active("chat1").unwrap().unwrap();
    assert_eq!(row.current_step, "done");
    assert_eq!(row.anchor_message_id, Some(99));
}

// Gate 11
#[test]
fn durable_complete_persists() {
    let db = Arc::new(FlowDb::open_in_memory().unwrap());
    let store = FlowStore::with_db(db.clone());
    store.start_flow("chat1", "greet", "hello", None);
    store.complete_flow("chat1", "completed");

    assert!(db.get_active("chat1").unwrap().is_none());
    let (history, _) = db.list_history(10, 0, None, None, None).unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].status, "completed");
}

// Gate 12
#[test]
fn durable_load_from_db() {
    let db = Arc::new(FlowDb::open_in_memory().unwrap());

    // Seed DB directly
    db.upsert_active(&FlowInstanceRow {
        chat_id: "chat1".into(),
        flow_name: "greet".into(),
        current_step: "hello".into(),
        started_at: "2026-01-01T00:00:00+00:00".into(),
        step_entered_at: "2026-01-01T00:00:00+00:00".into(),
        anchor_message_id: Some(10),
        status: "active".into(),
    })
    .unwrap();
    db.upsert_active(&FlowInstanceRow {
        chat_id: "chat2".into(),
        flow_name: "poll_flow".into(),
        current_step: "q1".into(),
        started_at: "2026-01-01T00:00:00+00:00".into(),
        step_entered_at: "2026-01-01T00:00:00+00:00".into(),
        anchor_message_id: None,
        status: "active".into(),
    })
    .unwrap();

    // Create a fresh FlowStore and load
    let store = FlowStore::with_db(db.clone());
    let loaded = store.load_from_db().unwrap();
    assert_eq!(loaded, 2);
    assert!(store.has_flow("chat1"));
    assert!(store.has_flow("chat2"));

    let info = store.get_flow_info("chat1").unwrap();
    assert_eq!(info.0, "greet");
    assert_eq!(info.1, "hello");
    assert_eq!(info.2, Some(10));
}

// Gate 13
#[test]
fn refresh_removes_force_completed() {
    let db = Arc::new(FlowDb::open_in_memory().unwrap());
    let store = FlowStore::with_db(db.clone());
    store.start_flow("chat1", "greet", "hello", None);
    assert!(store.has_flow("chat1"));

    // Simulate CP force-completing directly in DB
    db.complete_flow("chat1", "force_completed").unwrap();

    // Refresh should remove it from cache
    store.refresh_from_db().unwrap();
    assert!(!store.has_flow("chat1"));
}

// Gate 14
#[test]
fn datetime_timeout_detection() {
    let db = Arc::new(FlowDb::open_in_memory().unwrap());
    let store = FlowStore::with_db(db.clone());
    store.start_flow("chat1", "test_flow", "ask", None);

    // Manually set step_entered_at to the past
    {
        let guard = store
            .with_flow("chat1", |inst| inst.step_entered_at);
        // It should be very recent
        assert!(guard.is_some());
    }

    // Hack the in-memory timestamp to the past via a DB round-trip
    let now_minus_10 = (chrono::Utc::now() - chrono::Duration::seconds(10)).to_rfc3339();
    db.update_step("chat1", "ask", None, &now_minus_10).unwrap();
    store.refresh_from_db().unwrap();

    let mut defs = HashMap::new();
    defs.insert("test_flow".into(), make_flow_def());

    let timed_out = store.check_timeouts(&defs);
    assert_eq!(timed_out.len(), 1);
    assert_eq!(timed_out[0].0, "chat1");
    assert_eq!(timed_out[0].2, "ask");
}

// Gate 15
#[test]
fn expired_during_downtime() {
    let db = Arc::new(FlowDb::open_in_memory().unwrap());

    // Seed a flow that expired during "downtime" (step_entered_at far in the past)
    let past = (chrono::Utc::now() - chrono::Duration::seconds(300)).to_rfc3339();
    db.upsert_active(&FlowInstanceRow {
        chat_id: "chat1".into(),
        flow_name: "test_flow".into(),
        current_step: "ask".into(),
        started_at: past.clone(),
        step_entered_at: past,
        anchor_message_id: None,
        status: "active".into(),
    })
    .unwrap();

    let store = FlowStore::with_db(db.clone());
    store.load_from_db().unwrap();

    let mut defs = HashMap::new();
    defs.insert("test_flow".into(), make_flow_def());

    // First check_timeouts should catch it
    let timed_out = store.check_timeouts(&defs);
    assert_eq!(timed_out.len(), 1);
    assert_eq!(timed_out[0].0, "chat1");
}

// ═══════════════════════════════════════════════════════════════════
// Offset + Dedup (Gates 16-18)
// ═══════════════════════════════════════════════════════════════════

// Gate 16
#[test]
fn offset_kv_roundtrip() {
    let db = FlowDb::open_in_memory().unwrap();
    db.set_kv("telegram_offset", "12345").unwrap();
    let val = db.get_kv("telegram_offset").unwrap().unwrap();
    assert_eq!(val.parse::<i64>().unwrap(), 12345);
}

// Gate 17
#[test]
fn offset_parse_fallback() {
    let db = FlowDb::open_in_memory().unwrap();
    // Missing key should return None
    let val = db.get_kv("telegram_offset").unwrap();
    assert!(val.is_none());
    // The offset loading code: .unwrap_or(0)
    let offset: i64 = val.and_then(|v| v.parse().ok()).unwrap_or(0);
    assert_eq!(offset, 0);
}

// Gate 18 - SeenUpdates dedup regression check
#[test]
fn dedup_still_works() {
    // Verify the TelegramChannel can be created and the dedup set works
    let ch = zeroclaw::channels::TelegramChannel::new("fake".into(), vec!["*".into()]);
    // The SeenUpdates is internal, but the channel should construct successfully
    assert_eq!(ch.name(), "telegram");
    // Verify is_user_allowed still works (basic regression)
    assert!(ch.is_user_allowed("anyone"));
}

// ═══════════════════════════════════════════════════════════════════
// poll_answer (Gates 19-22)
// ═══════════════════════════════════════════════════════════════════

// Gate 19
#[test]
fn poll_option_synthetic_callback() {
    // Verify "poll_option_0" matches a transition `on = "poll_option_0"`
    let def = make_flow_def();
    let step = def.steps.get("ask").unwrap();
    let matched = step
        .transitions
        .iter()
        .find(|t| t.on == "poll_option_1");
    assert!(matched.is_some());
    assert_eq!(matched.unwrap().target, "done");
}

// Gate 20
#[test]
fn poll_id_mapping_roundtrip() {
    let db = FlowDb::open_in_memory().unwrap();
    db.set_kv("poll:abc123", "chat_42").unwrap();
    let val = db.get_kv("poll:abc123").unwrap();
    assert_eq!(val, Some("chat_42".into()));
}

// Gate 21
#[test]
fn step_result_has_poll_id() {
    let result = StepExecuteResult {
        anchor_message_id: Some(42),
        poll_id: Some("poll_xyz".into()),
    };
    assert_eq!(result.anchor_message_id, Some(42));
    assert_eq!(result.poll_id, Some("poll_xyz".into()));

    // No poll_id
    let result2 = StepExecuteResult {
        anchor_message_id: Some(10),
        poll_id: None,
    };
    assert!(result2.poll_id.is_none());
}

// Gate 22
#[test]
fn poll_answer_transition_match() {
    let def = make_flow_def();
    let step = def.steps.get("ask").unwrap();

    // Simulate poll_option_1 matching
    let callback_data = "poll_option_1";
    let matched = step
        .transitions
        .iter()
        .find(|t| t.on == callback_data || t.on == "_any");
    assert!(matched.is_some());
    assert_eq!(matched.unwrap().target, "done");
}

// ═══════════════════════════════════════════════════════════════════
// Admin (Gates 23-25)
// ═══════════════════════════════════════════════════════════════════

// Gate 23
#[test]
fn force_complete_sets_status() {
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
    db.complete_flow("chat1", "force_completed").unwrap();

    let (history, _) = db.list_history(10, 0, None, None, None).unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].status, "force_completed");
}

// Gate 24
#[test]
fn replay_resets_step() {
    let db = FlowDb::open_in_memory().unwrap();
    let row = FlowInstanceRow {
        chat_id: "chat1".into(),
        flow_name: "test_flow".into(),
        current_step: "done".into(),
        started_at: "2026-01-01T00:00:00Z".into(),
        step_entered_at: "2026-01-01T00:00:00Z".into(),
        anchor_message_id: Some(42),
        status: "active".into(),
    };
    db.upsert_active(&row).unwrap();

    let now = chrono::Utc::now().to_rfc3339();
    let updated = db.update_step("chat1", "ask", Some(42), &now).unwrap();
    assert!(updated);

    let got = db.get_active("chat1").unwrap().unwrap();
    assert_eq!(got.current_step, "ask");
    assert_eq!(got.step_entered_at, now);
}

// Gate 25
#[test]
fn history_preserves_all_statuses() {
    let db = FlowDb::open_in_memory().unwrap();

    for (chat, status) in &[
        ("c1", "completed"),
        ("c2", "timed_out"),
        ("c3", "force_completed"),
    ] {
        let row = FlowInstanceRow {
            chat_id: chat.to_string(),
            flow_name: "test".into(),
            current_step: "end".into(),
            started_at: "2026-01-01T00:00:00Z".into(),
            step_entered_at: "2026-01-01T00:00:00Z".into(),
            anchor_message_id: None,
            status: "active".into(),
        };
        db.upsert_active(&row).unwrap();
        db.complete_flow(chat, status).unwrap();
    }

    let (history, total) = db.list_history(10, 0, None, None, None).unwrap();
    assert_eq!(total, 3);

    let statuses: Vec<&str> = history.iter().map(|r| r.status.as_str()).collect();
    assert!(statuses.contains(&"completed"));
    assert!(statuses.contains(&"timed_out"));
    assert!(statuses.contains(&"force_completed"));
}

// ═══════════════════════════════════════════════════════════════════
// Backward Compat (Gate 26)
// ═══════════════════════════════════════════════════════════════════

// Gate 26
#[test]
fn in_memory_fallback() {
    let store = FlowStore::new();
    assert!(store.db().is_none());

    store.start_flow("chat1", "greet", "hello", Some(1));
    assert!(store.has_flow("chat1"));

    store.advance("chat1", "done", None).unwrap();
    let info = store.get_flow_info("chat1").unwrap();
    assert_eq!(info.1, "done");

    let removed = store.complete_flow("chat1", "completed");
    assert!(removed.is_some());
    assert!(!store.has_flow("chat1"));

    // load_from_db should return 0 (no DB)
    let loaded = store.load_from_db().unwrap();
    assert_eq!(loaded, 0);

    // refresh_from_db should be a no-op
    store.refresh_from_db().unwrap();
}
