// Phase 17.5 Gate Tests: CP API + Operator UI for Flow Version Review
//
// 18 gates covering: FlowDb::list_all_flow_versions method (5 DB gates),
// API endpoints for pending/list/detail/approve/reject/activate/audit (11 API gates),
// and integration tests for approve-deactivates-previous + full lifecycle (2 gates).

use std::path::PathBuf;
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt; // for `oneshot`
use zeroclaw::cp::server::{build_router, CpState};
use zeroclaw::db::Registry;
use zeroclaw::flows::db::FlowDb;

// ── Test Helpers ────────────────────────────────────────────────

/// Create a temp directory, registry DB, an instance pointing at a workspace
/// with a state.db, and return (CpState, workspace_path).
fn setup_test_env() -> (CpState, PathBuf) {
    let tmp = tempfile::tempdir().unwrap();
    let registry_path = tmp.path().join("registry.db");
    let workspace = tmp.path().join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();

    // Create registry and instance
    let registry = Registry::open(&registry_path).unwrap();
    registry
        .create_instance(
            "test-id",
            "test",
            9999,
            "/dev/null",
            Some(workspace.to_str().unwrap()),
            None,
        )
        .unwrap();

    // Create state.db (FlowDb) in the workspace
    let state_db_path = workspace.join("state.db");
    let _flow_db = FlowDb::open(&state_db_path).unwrap();

    let state = CpState {
        db_path: Arc::new(registry_path),
    };

    // Leak the tempdir so it doesn't get cleaned up while the test runs
    std::mem::forget(tmp);

    (state, workspace)
}

/// Seed a flow version in the state.db and return the version number.
fn seed_version(workspace: &std::path::Path, flow_name: &str, source: &str, status: &str) -> i64 {
    let state_db_path = workspace.join("state.db");
    let db = FlowDb::open(&state_db_path).unwrap();
    let def = serde_json::json!({
        "flow": { "name": flow_name, "start": "s1", "default_timeout_secs": 60 },
        "steps": [{ "id": "s1", "kind": "message", "text": "Hello" }]
    });
    let v = db
        .create_flow_version(flow_name, &def.to_string(), source, "test")
        .unwrap();
    if status != "draft" {
        db.update_version_status(flow_name, v, status, None)
            .unwrap();
    }
    v
}

async fn get_json(app: axum::Router, uri: &str) -> (StatusCode, serde_json::Value) {
    let resp = app
        .oneshot(
            Request::builder()
                .uri(uri)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    (status, json)
}

async fn post_json(
    app: axum::Router,
    uri: &str,
    body: Option<serde_json::Value>,
) -> (StatusCode, serde_json::Value) {
    let req = if let Some(b) = body {
        Request::builder()
            .method("POST")
            .uri(uri)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&b).unwrap()))
            .unwrap()
    } else {
        Request::builder()
            .method("POST")
            .uri(uri)
            .header("content-type", "application/json")
            .body(Body::from("{}"))
            .unwrap()
    };
    let resp = app.oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    (status, json)
}

// ── DB Gates (1-5): list_all_flow_versions ──────────────────────

// Gate 1: list_all_flow_versions_unfiltered
#[test]
fn gate1_list_all_flow_versions_unfiltered() {
    let db = FlowDb::open_in_memory().unwrap();
    db.create_flow_version("flow_a", "{}", "agent", "agent")
        .unwrap();
    db.create_flow_version("flow_b", "{}", "operator", "system")
        .unwrap();
    db.create_flow_version("flow_a", "{}", "agent", "agent")
        .unwrap();

    let (rows, total) = db.list_all_flow_versions(50, 0, None, None, None).unwrap();
    assert_eq!(total, 3);
    assert_eq!(rows.len(), 3);
}

// Gate 2: list_all_flow_versions_filter_by_status
#[test]
fn gate2_list_all_flow_versions_filter_by_status() {
    let db = FlowDb::open_in_memory().unwrap();
    let v1 = db
        .create_flow_version("flow_a", "{}", "agent", "agent")
        .unwrap();
    db.update_version_status("flow_a", v1, "pending_review", None)
        .unwrap();
    db.create_flow_version("flow_b", "{}", "agent", "agent")
        .unwrap(); // stays draft

    let (rows, total) = db
        .list_all_flow_versions(50, 0, None, Some("pending_review"), None)
        .unwrap();
    assert_eq!(total, 1);
    assert_eq!(rows[0].flow_name, "flow_a");
    assert_eq!(rows[0].status, "pending_review");
}

// Gate 3: list_all_flow_versions_filter_by_source
#[test]
fn gate3_list_all_flow_versions_filter_by_source() {
    let db = FlowDb::open_in_memory().unwrap();
    db.create_flow_version("flow_a", "{}", "agent", "agent")
        .unwrap();
    db.create_flow_version("flow_b", "{}", "operator", "system")
        .unwrap();

    let (rows, total) = db
        .list_all_flow_versions(50, 0, None, None, Some("operator"))
        .unwrap();
    assert_eq!(total, 1);
    assert_eq!(rows[0].source, "operator");
}

// Gate 4: list_all_flow_versions_filter_by_name
#[test]
fn gate4_list_all_flow_versions_filter_by_name() {
    let db = FlowDb::open_in_memory().unwrap();
    db.create_flow_version("flow_a", "{}", "agent", "agent")
        .unwrap();
    db.create_flow_version("flow_b", "{}", "agent", "agent")
        .unwrap();
    db.create_flow_version("flow_a", "{}", "agent", "agent")
        .unwrap();

    let (rows, total) = db
        .list_all_flow_versions(50, 0, Some("flow_a"), None, None)
        .unwrap();
    assert_eq!(total, 2);
    assert!(rows.iter().all(|r| r.flow_name == "flow_a"));
}

// Gate 5: list_all_flow_versions_pagination
#[test]
fn gate5_list_all_flow_versions_pagination() {
    let db = FlowDb::open_in_memory().unwrap();
    for i in 0..5 {
        db.create_flow_version(&format!("flow_{i}"), "{}", "agent", "agent")
            .unwrap();
    }

    // First page
    let (rows, total) = db.list_all_flow_versions(2, 0, None, None, None).unwrap();
    assert_eq!(total, 5);
    assert_eq!(rows.len(), 2);

    // Second page
    let (rows2, total2) = db.list_all_flow_versions(2, 2, None, None, None).unwrap();
    assert_eq!(total2, 5);
    assert_eq!(rows2.len(), 2);

    // Last page
    let (rows3, total3) = db.list_all_flow_versions(2, 4, None, None, None).unwrap();
    assert_eq!(total3, 5);
    assert_eq!(rows3.len(), 1);
}

// ── API Gates (6-16) ────────────────────────────────────────────

// Gate 6: api_pending_returns_pending_only
#[tokio::test]
async fn gate6_api_pending_returns_pending_only() {
    let (state, workspace) = setup_test_env();
    seed_version(&workspace, "flow_a", "agent", "pending_review");
    seed_version(&workspace, "flow_b", "agent", "draft");

    let app = build_router(state);
    let (status, json) = get_json(app, "/api/instances/test/flows/versions/pending").await;

    assert_eq!(status, StatusCode::OK);
    let versions = json["versions"].as_array().unwrap();
    assert_eq!(versions.len(), 1);
    assert_eq!(versions[0]["flow_name"], "flow_a");
    assert_eq!(json["total"], 1);
}

// Gate 7: api_versions_list_with_filters
#[tokio::test]
async fn gate7_api_versions_list_with_filters() {
    let (state, workspace) = setup_test_env();
    seed_version(&workspace, "flow_a", "agent", "pending_review");
    seed_version(&workspace, "flow_a", "agent", "draft");
    seed_version(&workspace, "flow_b", "operator", "draft");

    let app = build_router(state);
    let (status, json) =
        get_json(app, "/api/instances/test/flows/versions?source=agent").await;

    assert_eq!(status, StatusCode::OK);
    let versions = json["versions"].as_array().unwrap();
    assert_eq!(versions.len(), 2);
    assert!(versions.iter().all(|v| v["source"] == "agent"));
}

// Gate 8: api_version_detail_found
#[tokio::test]
async fn gate8_api_version_detail_found() {
    let (state, workspace) = setup_test_env();
    seed_version(&workspace, "flow_a", "agent", "draft");

    let app = build_router(state);
    let (status, json) =
        get_json(app, "/api/instances/test/flows/versions/flow_a/1").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["flow_name"], "flow_a");
    assert_eq!(json["version"], 1);
}

// Gate 9: api_version_detail_not_found
#[tokio::test]
async fn gate9_api_version_detail_not_found() {
    let (state, _workspace) = setup_test_env();

    let app = build_router(state);
    let (status, json) =
        get_json(app, "/api/instances/test/flows/versions/nonexistent/99").await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(json["error"].as_str().unwrap().contains("not found"));
}

// Gate 10: api_approve_pending_version
#[tokio::test]
async fn gate10_api_approve_pending_version() {
    let (state, workspace) = setup_test_env();
    seed_version(&workspace, "flow_a", "agent", "pending_review");

    let app = build_router(state.clone());
    let (status, json) = post_json(
        app,
        "/api/instances/test/flows/versions/flow_a/1/approve",
        None,
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["status"], "approved");

    // Verify state change in DB
    let state_db = workspace.join("state.db");
    let db = FlowDb::open(&state_db).unwrap();
    let row = db.get_flow_version("flow_a", 1).unwrap().unwrap();
    assert_eq!(row.status, "active");

    // Verify audit entries
    let (audit, _) = db.list_audit_log(10, 0, Some("flow_a")).unwrap();
    let events: Vec<&str> = audit.iter().map(|a| a.event.as_str()).collect();
    assert!(events.contains(&"approved"));
    assert!(events.contains(&"activated"));
}

// Gate 11: api_approve_non_pending_rejected
#[tokio::test]
async fn gate11_api_approve_non_pending_rejected() {
    let (state, workspace) = setup_test_env();
    seed_version(&workspace, "flow_a", "agent", "draft");

    let app = build_router(state);
    let (status, json) = post_json(
        app,
        "/api/instances/test/flows/versions/flow_a/1/approve",
        None,
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(json["error"].as_str().unwrap().contains("Cannot approve"));
}

// Gate 12: api_reject_with_note
#[tokio::test]
async fn gate12_api_reject_with_note() {
    let (state, workspace) = setup_test_env();
    seed_version(&workspace, "flow_a", "agent", "pending_review");

    let app = build_router(state);
    let (status, json) = post_json(
        app,
        "/api/instances/test/flows/versions/flow_a/1/reject",
        Some(serde_json::json!({ "note": "Too complex" })),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["status"], "rejected");

    // Verify note stored
    let state_db = workspace.join("state.db");
    let db = FlowDb::open(&state_db).unwrap();
    let row = db.get_flow_version("flow_a", 1).unwrap().unwrap();
    assert_eq!(row.status, "rejected");
    assert_eq!(row.review_note.as_deref(), Some("Too complex"));

    // Verify audit
    let (audit, _) = db.list_audit_log(10, 0, Some("flow_a")).unwrap();
    assert!(audit.iter().any(|a| a.event == "rejected" && a.detail.as_deref() == Some("Too complex")));
}

// Gate 13: api_activate_deactivated_version
#[tokio::test]
async fn gate13_api_activate_deactivated_version() {
    let (state, workspace) = setup_test_env();
    // Create v1 and activate it
    let state_db = workspace.join("state.db");
    {
        let db = FlowDb::open(&state_db).unwrap();
        let def = serde_json::json!({
            "flow": { "name": "flow_a", "start": "s1", "default_timeout_secs": 60 },
            "steps": [{ "id": "s1", "kind": "message", "text": "v1" }]
        });
        let v1 = db
            .create_flow_version("flow_a", &def.to_string(), "agent", "test")
            .unwrap();
        db.activate_version("flow_a", v1).unwrap();

        // Create v2 and activate it (deactivates v1)
        let v2 = db
            .create_flow_version("flow_a", &def.to_string(), "agent", "test")
            .unwrap();
        db.activate_version("flow_a", v2).unwrap();
    }

    // Now activate v1 (deactivated) via API
    let app = build_router(state);
    let (status, json) = post_json(
        app,
        "/api/instances/test/flows/versions/flow_a/1/activate",
        None,
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["status"], "activated");

    // Verify v1 is now active, v2 is deactivated
    let db = FlowDb::open(&state_db).unwrap();
    let v1 = db.get_flow_version("flow_a", 1).unwrap().unwrap();
    assert_eq!(v1.status, "active");
    let v2 = db.get_flow_version("flow_a", 2).unwrap().unwrap();
    assert_eq!(v2.status, "deactivated");
}

// Gate 14: api_activate_rejected_blocked
#[tokio::test]
async fn gate14_api_activate_rejected_blocked() {
    let (state, workspace) = setup_test_env();
    seed_version(&workspace, "flow_a", "agent", "rejected");

    let app = build_router(state);
    let (status, json) = post_json(
        app,
        "/api/instances/test/flows/versions/flow_a/1/activate",
        None,
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(json["error"].as_str().unwrap().contains("Cannot activate"));
}

// Gate 15: api_audit_log_returns_entries
#[tokio::test]
async fn gate15_api_audit_log_returns_entries() {
    let (state, workspace) = setup_test_env();
    let state_db = workspace.join("state.db");
    {
        let db = FlowDb::open(&state_db).unwrap();
        db.log_audit("flow_a", Some(1), "created", "agent", None)
            .unwrap();
        db.log_audit("flow_a", Some(1), "validated", "agent", None)
            .unwrap();
        db.log_audit("flow_b", Some(1), "created", "agent", None)
            .unwrap();
    }

    let app = build_router(state);
    let (status, json) =
        get_json(app, "/api/instances/test/flows/audit?limit=10&offset=0").await;

    assert_eq!(status, StatusCode::OK);
    let entries = json["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 3);
    assert_eq!(json["total"], 3);
}

// Gate 16: api_audit_log_filter_by_name
#[tokio::test]
async fn gate16_api_audit_log_filter_by_name() {
    let (state, workspace) = setup_test_env();
    let state_db = workspace.join("state.db");
    {
        let db = FlowDb::open(&state_db).unwrap();
        db.log_audit("flow_a", Some(1), "created", "agent", None)
            .unwrap();
        db.log_audit("flow_b", Some(1), "created", "agent", None)
            .unwrap();
        db.log_audit("flow_a", Some(1), "approved", "operator", None)
            .unwrap();
    }

    let app = build_router(state);
    let (status, json) =
        get_json(app, "/api/instances/test/flows/audit?flow_name=flow_a").await;

    assert_eq!(status, StatusCode::OK);
    let entries = json["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 2);
    assert!(entries.iter().all(|e| e["flow_name"] == "flow_a"));
}

// Gate 16b: api_reject_non_pending_blocked
#[tokio::test]
async fn gate16b_api_reject_non_pending_blocked() {
    let (state, workspace) = setup_test_env();
    seed_version(&workspace, "flow_a", "agent", "draft"); // not pending_review

    let app = build_router(state);
    let (status, json) = post_json(
        app,
        "/api/instances/test/flows/versions/flow_a/1/reject",
        Some(serde_json::json!({ "note": "nope" })),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(json["error"].as_str().unwrap().contains("Cannot reject"));
}

// Gate 16c: list_all_flow_versions_pagination_stability
#[test]
fn gate16c_list_all_flow_versions_pagination_stability() {
    let db = FlowDb::open_in_memory().unwrap();

    // Insert several versions that will share the same created_at (same second)
    for i in 0..6 {
        db.create_flow_version(&format!("flow_{}", i % 2), "{}", "agent", "agent")
            .unwrap();
    }

    // Fetch page 1 and page 2
    let (page1, total1) = db.list_all_flow_versions(3, 0, None, None, None).unwrap();
    let (page2, total2) = db.list_all_flow_versions(3, 3, None, None, None).unwrap();

    assert_eq!(total1, 6);
    assert_eq!(total2, 6);
    assert_eq!(page1.len(), 3);
    assert_eq!(page2.len(), 3);

    // No ID should appear on both pages
    let page1_ids: Vec<i64> = page1.iter().map(|r| r.id).collect();
    let page2_ids: Vec<i64> = page2.iter().map(|r| r.id).collect();
    for id in &page1_ids {
        assert!(
            !page2_ids.contains(id),
            "ID {id} appears on both page 1 and page 2 -- pagination is unstable"
        );
    }

    // IDs within each page should be in descending order (tie-breaker)
    for window in page1_ids.windows(2) {
        assert!(window[0] > window[1], "Page 1 IDs not in DESC order: {:?}", page1_ids);
    }
    for window in page2_ids.windows(2) {
        assert!(window[0] > window[1], "Page 2 IDs not in DESC order: {:?}", page2_ids);
    }
}

// ── Integration Gates (17-18) ───────────────────────────────────

// Gate 17: approve_deactivates_previous_active
#[tokio::test]
async fn gate17_approve_deactivates_previous_active() {
    let (state, workspace) = setup_test_env();
    let state_db = workspace.join("state.db");

    // Create v1 as active
    {
        let db = FlowDb::open(&state_db).unwrap();
        let def = serde_json::json!({
            "flow": { "name": "flow_a", "start": "s1", "default_timeout_secs": 60 },
            "steps": [{ "id": "s1", "kind": "message", "text": "v1" }]
        });
        let v1 = db
            .create_flow_version("flow_a", &def.to_string(), "agent", "test")
            .unwrap();
        db.activate_version("flow_a", v1).unwrap();

        // Create v2 as pending_review
        let v2 = db
            .create_flow_version("flow_a", &def.to_string(), "agent", "test")
            .unwrap();
        db.update_version_status("flow_a", v2, "pending_review", None)
            .unwrap();
    }

    // Approve v2
    let app = build_router(state);
    let (status, _) = post_json(
        app,
        "/api/instances/test/flows/versions/flow_a/2/approve",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // Verify v1 is deactivated, v2 is active
    let db = FlowDb::open(&state_db).unwrap();
    let v1 = db.get_flow_version("flow_a", 1).unwrap().unwrap();
    assert_eq!(v1.status, "deactivated");
    let v2 = db.get_flow_version("flow_a", 2).unwrap().unwrap();
    assert_eq!(v2.status, "active");
}

// Gate 18: full_lifecycle_compose_review_activate
#[tokio::test]
async fn gate18_full_lifecycle_compose_review_activate() {
    let (state, workspace) = setup_test_env();
    let state_db = workspace.join("state.db");

    // Step 1: Simulate compose -> create v1 as pending_review
    {
        let db = FlowDb::open(&state_db).unwrap();
        let def = serde_json::json!({
            "flow": { "name": "onboard", "start": "welcome", "default_timeout_secs": 60 },
            "steps": [
                { "id": "welcome", "kind": "message", "text": "Welcome!" },
            ]
        });
        let v1 = db
            .create_flow_version("onboard", &def.to_string(), "agent", "agent")
            .unwrap();
        db.update_version_status("onboard", v1, "pending_review", None)
            .unwrap();
        db.log_audit("onboard", Some(v1), "created", "agent", None)
            .unwrap();
        db.log_audit("onboard", Some(v1), "pending_review", "agent", None)
            .unwrap();
    }

    // Step 2: Approve v1 via API
    let app = build_router(state.clone());
    let (status, json) = post_json(
        app,
        "/api/instances/test/flows/versions/onboard/1/approve",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["status"], "approved");

    // Verify v1 is active
    {
        let db = FlowDb::open(&state_db).unwrap();
        let v1 = db.get_flow_version("onboard", 1).unwrap().unwrap();
        assert_eq!(v1.status, "active");
    }

    // Step 3: Simulate compose -> create v2 as pending_review
    {
        let db = FlowDb::open(&state_db).unwrap();
        let def2 = serde_json::json!({
            "flow": { "name": "onboard", "start": "welcome", "default_timeout_secs": 60 },
            "steps": [
                { "id": "welcome", "kind": "message", "text": "Welcome v2!" },
            ]
        });
        let v2 = db
            .create_flow_version("onboard", &def2.to_string(), "agent", "agent")
            .unwrap();
        db.update_version_status("onboard", v2, "pending_review", None)
            .unwrap();
    }

    // Step 4: Approve v2 -> v1 should be deactivated
    let app2 = build_router(state);
    let (status2, json2) = post_json(
        app2,
        "/api/instances/test/flows/versions/onboard/2/approve",
        None,
    )
    .await;
    assert_eq!(status2, StatusCode::OK);
    assert_eq!(json2["status"], "approved");

    // Verify v1 deactivated, v2 active
    let db = FlowDb::open(&state_db).unwrap();
    let v1 = db.get_flow_version("onboard", 1).unwrap().unwrap();
    assert_eq!(v1.status, "deactivated");
    let v2 = db.get_flow_version("onboard", 2).unwrap().unwrap();
    assert_eq!(v2.status, "active");

    // Verify audit trail has all events
    let (audit, total) = db.list_audit_log(50, 0, Some("onboard")).unwrap();
    assert!(total >= 4, "Should have at least 4 audit entries, got {total}");
    let events: Vec<&str> = audit.iter().map(|a| a.event.as_str()).collect();
    assert!(events.contains(&"approved"));
    assert!(events.contains(&"activated"));
}
