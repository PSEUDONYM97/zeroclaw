use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::TempDir;
use zeroclaw::cp;
use zeroclaw::db::Registry;

/// Helper: create a registry with a registered instance in a temp dir.
fn setup_instance(name: &str, port: u16, config_toml: &str) -> (TempDir, PathBuf, String, PathBuf) {
    let tmp = TempDir::new().unwrap();
    let cp_dir = tmp.path().join("cp");
    let instances_dir = cp_dir.join("instances");
    fs::create_dir_all(&instances_dir).unwrap();

    let db_path = cp_dir.join("registry.db");
    let registry = Registry::open(&db_path).unwrap();

    let id = uuid::Uuid::new_v4().to_string();
    let inst_dir = instances_dir.join(&id);
    fs::create_dir_all(&inst_dir).unwrap();

    // Create workspace dir alongside instance
    let ws_dir = inst_dir.join("workspace");
    fs::create_dir_all(&ws_dir).unwrap();

    let config_path = inst_dir.join("config.toml");
    fs::write(&config_path, config_toml).unwrap();

    registry
        .create_instance(
            &id,
            name,
            port,
            config_path.to_str().unwrap(),
            Some(ws_dir.to_str().unwrap()),
            None,
        )
        .unwrap();

    (tmp, db_path, id, inst_dir)
}

/// Helper: start an in-process axum server on a random port.
async fn start_test_server(db_path: PathBuf) -> (String, tokio::sync::watch::Sender<bool>) {
    let state = cp::server::CpState {
        db_path: Arc::new(db_path),
    };
    let app = cp::server::build_router(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://127.0.0.1:{}", addr.port());

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
    tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                let _ = shutdown_rx.changed().await;
            })
            .await
            .unwrap();
    });

    (base_url, shutdown_tx)
}

fn minimal_config() -> String {
    r#"api_key = "sk-test"
default_temperature = 0.7

[heartbeat]
enabled = false
interval_minutes = 30

[channels_config]
cli = true
"#
    .to_string()
}

// ── Scaffold endpoint tests ──────────────────────────────────────

#[tokio::test]
async fn scaffold_creates_workspace_files() {
    let (_tmp, db_path, _id, inst_dir) = setup_instance("scout", 18801, &minimal_config());
    let (base, _shutdown) = start_test_server(db_path).await;

    let client = reqwest::Client::new();
    let res = client
        .post(format!("{base}/api/instances/scout/scaffold"))
        .json(&serde_json::json!({
            "agent_name": "Scout",
            "user_name": "Tester",
            "timezone": "UTC",
            "personality": {
                "warmth": 8,
                "verbosity": 6,
                "formality": 3,
                "playfulness": 6,
                "creativity": 5
            }
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(res.status(), 200);
    let body: serde_json::Value = res.json().await.unwrap();
    assert!(body["files_created"].as_u64().unwrap() >= 8);

    let ws_dir = inst_dir.join("workspace");
    assert!(ws_dir.join("SOUL.md").exists());
    assert!(ws_dir.join("IDENTITY.md").exists());
    assert!(ws_dir.join("USER.md").exists());
    assert!(ws_dir.join("AGENTS.md").exists());
    assert!(ws_dir.join("BOOTSTRAP.md").exists());
    assert!(ws_dir.join("MEMORY.md").exists());

    // Verify content references the agent name
    let soul = fs::read_to_string(ws_dir.join("SOUL.md")).unwrap();
    assert!(soul.contains("Scout"), "SOUL.md should reference agent name");

    let user_md = fs::read_to_string(ws_dir.join("USER.md")).unwrap();
    assert!(user_md.contains("Tester"), "USER.md should reference user name");
}

#[tokio::test]
async fn scaffold_idempotent() {
    let (_tmp, db_path, _id, inst_dir) = setup_instance("idempotent", 18802, &minimal_config());
    let (base, _shutdown) = start_test_server(db_path).await;

    let client = reqwest::Client::new();
    let payload = serde_json::json!({
        "agent_name": "Dupe",
        "personality": { "warmth": 5, "verbosity": 5, "formality": 5, "playfulness": 5, "creativity": 5 }
    });

    // First call
    let res1 = client
        .post(format!("{base}/api/instances/idempotent/scaffold"))
        .json(&payload)
        .send()
        .await
        .unwrap();
    assert_eq!(res1.status(), 200);
    let body1: serde_json::Value = res1.json().await.unwrap();
    let created1 = body1["files_created"].as_u64().unwrap();
    assert!(created1 >= 8);

    // Capture SOUL.md content
    let soul_before = fs::read_to_string(inst_dir.join("workspace/SOUL.md")).unwrap();

    // Second call
    let res2 = client
        .post(format!("{base}/api/instances/idempotent/scaffold"))
        .json(&payload)
        .send()
        .await
        .unwrap();
    assert_eq!(res2.status(), 200);
    let body2: serde_json::Value = res2.json().await.unwrap();
    assert!(
        body2["files_skipped"].as_u64().unwrap() > 0,
        "Second scaffold call should skip existing files"
    );

    // Content should be unchanged
    let soul_after = fs::read_to_string(inst_dir.join("workspace/SOUL.md")).unwrap();
    assert_eq!(soul_before, soul_after, "Existing files should not be overwritten");
}

#[tokio::test]
async fn scaffold_clamps_out_of_range_sliders() {
    let (_tmp, db_path, _id, inst_dir) = setup_instance("clamper", 18803, &minimal_config());
    let (base, _shutdown) = start_test_server(db_path).await;

    let client = reqwest::Client::new();
    let res = client
        .post(format!("{base}/api/instances/clamper/scaffold"))
        .json(&serde_json::json!({
            "agent_name": "Clampy",
            "personality": {
                "warmth": 15,
                "verbosity": 255,
                "formality": 10,
                "playfulness": 0,
                "creativity": 10
            }
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(res.status(), 200, "Out-of-range sliders should be clamped, not rejected");

    // The generated SOUL.md should exist and contain warmth=10 level text (warm/friendly)
    let soul = fs::read_to_string(inst_dir.join("workspace/SOUL.md")).unwrap();
    assert!(
        soul.contains("warm") || soul.contains("friendly") || soul.contains("personable"),
        "Clamped warmth=10 should produce warm tone in SOUL.md: {soul}"
    );
}

#[tokio::test]
async fn scaffold_rejects_missing_instance() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("registry.db");
    let _registry = Registry::open(&db_path).unwrap();
    let (base, _shutdown) = start_test_server(db_path).await;

    let client = reqwest::Client::new();
    let res = client
        .post(format!("{base}/api/instances/nonexistent/scaffold"))
        .json(&serde_json::json!({ "agent_name": "Ghost" }))
        .send()
        .await
        .unwrap();

    assert_eq!(res.status(), 404);
}

#[tokio::test]
async fn scaffold_truncates_oversized_names() {
    let (_tmp, db_path, _id, inst_dir) = setup_instance("truncator", 18804, &minimal_config());
    let (base, _shutdown) = start_test_server(db_path).await;

    let long_name = "A".repeat(500);
    let client = reqwest::Client::new();
    let res = client
        .post(format!("{base}/api/instances/truncator/scaffold"))
        .json(&serde_json::json!({
            "agent_name": long_name,
            "user_name": "B".repeat(500),
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(res.status(), 200);

    // Verify the agent name in SOUL.md is truncated
    let soul = fs::read_to_string(inst_dir.join("workspace/SOUL.md")).unwrap();
    // Should not contain the full 500-char name
    assert!(
        !soul.contains(&"A".repeat(500)),
        "500-char name should be truncated"
    );
    // But should contain the truncated 64-char version
    assert!(
        soul.contains(&"A".repeat(64)),
        "Truncated 64-char name should appear in SOUL.md"
    );
}

#[tokio::test]
async fn scaffold_handles_multibyte_utf8_names() {
    let (_tmp, db_path, _id, inst_dir) = setup_instance("unicode", 18810, &minimal_config());
    let (base, _shutdown) = start_test_server(db_path).await;

    // Build a name with multibyte chars that would panic with byte-slicing at 64
    // Each emoji is 4 bytes, so 20 emojis = 80 bytes but only 20 chars
    let emoji_name = "\u{1f980}".repeat(20); // 20 crab emojis
    let mixed_name = format!("{}{}", "\u{00e9}".repeat(60), "ABCDEFGHIJ"); // 60 accented + 10 ASCII = 70 chars

    let client = reqwest::Client::new();
    let res = client
        .post(format!("{base}/api/instances/unicode/scaffold"))
        .json(&serde_json::json!({
            "agent_name": emoji_name,
            "user_name": mixed_name,
        }))
        .send()
        .await
        .unwrap();

    // Should not panic, should return 200
    assert_eq!(res.status(), 200, "Multibyte UTF-8 names should not panic");

    let soul = fs::read_to_string(inst_dir.join("workspace/SOUL.md")).unwrap();
    // Emoji name is only 20 chars, should not be truncated
    assert!(soul.contains(&emoji_name), "20-char emoji name should appear in full");

    let user_md = fs::read_to_string(inst_dir.join("workspace/USER.md")).unwrap();
    // 70-char mixed name should be truncated to 64 chars
    assert!(
        !user_md.contains("ABCDEFGHIJ"),
        "70-char name should be truncated, losing the trailing ASCII"
    );
}

#[tokio::test]
async fn scaffold_default_personality() {
    let (_tmp, db_path, _id, inst_dir) = setup_instance("defaulter", 18805, &minimal_config());
    let (base, _shutdown) = start_test_server(db_path).await;

    // Send empty body -- no personality field at all
    let client = reqwest::Client::new();
    let res = client
        .post(format!("{base}/api/instances/defaulter/scaffold"))
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();

    assert_eq!(res.status(), 200);

    // Should still create files with Balanced defaults (all 5s)
    let soul = fs::read_to_string(inst_dir.join("workspace/SOUL.md")).unwrap();
    assert!(!soul.is_empty(), "SOUL.md should be created with default personality");
    // Balanced (5,5,5,5,5) produces mid-range text
    assert!(
        soul.contains("approachable") || soul.contains("balanced") || soul.contains("genuine"),
        "Default personality should produce balanced tone: {soul}"
    );
}

// ── Details endpoint scaffolded flag test ─────────────────────────

#[tokio::test]
async fn details_shows_scaffolded_false_before_scaffold() {
    let (_tmp, db_path, _id, _inst_dir) = setup_instance("unscaffolded", 18806, &minimal_config());
    let (base, _shutdown) = start_test_server(db_path).await;

    let client = reqwest::Client::new();
    let res = client
        .get(format!("{base}/api/instances/unscaffolded/details"))
        .send()
        .await
        .unwrap();

    assert_eq!(res.status(), 200);
    let body: serde_json::Value = res.json().await.unwrap();
    assert_eq!(body["scaffolded"], false, "Instance without SOUL.md should report scaffolded=false");
}

#[tokio::test]
async fn details_shows_scaffolded_true_after_scaffold() {
    let (_tmp, db_path, _id, _inst_dir) = setup_instance("scaffolded", 18807, &minimal_config());
    let (base, _shutdown) = start_test_server(db_path).await;

    let client = reqwest::Client::new();

    // Scaffold first
    let res = client
        .post(format!("{base}/api/instances/scaffolded/scaffold"))
        .json(&serde_json::json!({ "agent_name": "Test" }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);

    // Now check details
    let res = client
        .get(format!("{base}/api/instances/scaffolded/details"))
        .send()
        .await
        .unwrap();

    assert_eq!(res.status(), 200);
    let body: serde_json::Value = res.json().await.unwrap();
    assert_eq!(body["scaffolded"], true, "Instance with SOUL.md should report scaffolded=true");
}

// ── Config PATCH safety contract tests ───────────────────────────

#[tokio::test]
async fn config_patch_rejects_secret_without_header() {
    let (_tmp, db_path, _id, _inst_dir) = setup_instance("secretguard", 18808, &minimal_config());
    let (base, _shutdown) = start_test_server(db_path).await;

    let client = reqwest::Client::new();

    // First get the ETag
    let config_res = client
        .get(format!("{base}/api/instances/secretguard/config"))
        .send()
        .await
        .unwrap();
    assert_eq!(config_res.status(), 200);
    let config_body: serde_json::Value = config_res.json().await.unwrap();
    let etag = config_body["etag"].as_str().unwrap().to_string();

    // PATCH with api_key but NO X-Allow-Secret-Write header
    let res = client
        .patch(format!("{base}/api/instances/secretguard/config"))
        .json(&serde_json::json!({
            "patch": { "api_key": "sk-new-secret-key" },
            "etag": etag
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(res.status(), 400, "Secret write without header should be blocked");
    let body: serde_json::Value = res.json().await.unwrap();
    assert!(
        body["blocked_fields"].is_array(),
        "Response should include blocked_fields hint"
    );
}

#[tokio::test]
async fn config_patch_rejects_stale_etag() {
    let (_tmp, db_path, _id, _inst_dir) = setup_instance("etagcheck", 18809, &minimal_config());
    let (base, _shutdown) = start_test_server(db_path).await;

    let client = reqwest::Client::new();

    // PATCH with a fabricated stale ETag
    let res = client
        .patch(format!("{base}/api/instances/etagcheck/config"))
        .json(&serde_json::json!({
            "patch": { "default_temperature": 0.9 },
            "etag": "0000000000000000000000000000000000000000000000000000000000000000"
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(res.status(), 409, "Stale ETag should return 409 Conflict");
}
