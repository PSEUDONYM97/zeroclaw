use anyhow::Result;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::TempDir;
use zeroclaw::cp;
use zeroclaw::db::Registry;

// ── Test helpers ─────────────────────────────────────────────────

/// Setup a CP temp dir with an instances directory and registry DB.
/// Returns (TempDir, db_path).
fn setup_cp() -> (TempDir, PathBuf) {
    let tmp = TempDir::new().unwrap();
    let cp_dir = tmp.path().join("cp");
    let instances_dir = cp_dir.join("instances");
    fs::create_dir_all(&instances_dir).unwrap();

    let db_path = cp_dir.join("registry.db");
    let _registry = Registry::open(&db_path).unwrap();

    (tmp, db_path)
}

/// Setup a CP temp dir with a pre-registered instance.
fn setup_with_instance(name: &str, port: u16) -> (TempDir, PathBuf) {
    let tmp = TempDir::new().unwrap();
    let cp_dir = tmp.path().join("cp");
    let instances_dir = cp_dir.join("instances");
    fs::create_dir_all(&instances_dir).unwrap();

    let db_path = cp_dir.join("registry.db");
    let registry = Registry::open(&db_path).unwrap();

    let id = uuid::Uuid::new_v4().to_string();
    let inst_dir = instances_dir.join(&id);
    let workspace_dir = inst_dir.join("workspace");
    for subdir in &["skills", "memory", "sessions", "state", "cron"] {
        fs::create_dir_all(workspace_dir.join(subdir)).unwrap();
    }

    let config_toml = format!(
        r#"default_temperature = 0.7

[gateway]
port = {port}
host = "127.0.0.1"
"#
    );
    let config_path = inst_dir.join("config.toml");
    fs::write(&config_path, &config_toml).unwrap();

    registry
        .create_instance(
            &id,
            name,
            port,
            config_path.to_str().unwrap(),
            Some(workspace_dir.to_str().unwrap()),
            None,
        )
        .unwrap();

    (tmp, db_path)
}

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

// ══════════════════════════════════════════════════════════════════
// Gate 1: Create instance
// ══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn create_instance_basic() -> Result<()> {
    let (_tmp, db_path) = setup_cp();
    let (base_url, _shutdown) = start_test_server(db_path).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base_url}/api/instances"))
        .json(&serde_json::json!({ "name": "test-agent" }))
        .send()
        .await?;

    assert_eq!(resp.status(), 201);
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["name"], "test-agent");
    assert_eq!(body["status"], "stopped");
    assert!(body["id"].as_str().unwrap().len() > 10); // UUID
    assert!(body["port"].as_u64().unwrap() >= 18801);

    Ok(())
}

#[tokio::test]
async fn create_instance_with_port() -> Result<()> {
    let (_tmp, db_path) = setup_cp();
    let (base_url, _shutdown) = start_test_server(db_path).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base_url}/api/instances"))
        .json(&serde_json::json!({ "name": "agent-explicit", "port": 18850 }))
        .send()
        .await?;

    assert_eq!(resp.status(), 201);
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["port"], 18850);

    Ok(())
}

#[tokio::test]
async fn create_instance_auto_port() -> Result<()> {
    let (_tmp, db_path) = setup_cp();
    let (base_url, _shutdown) = start_test_server(db_path.clone()).await;

    let client = reqwest::Client::new();

    // Create first instance
    let resp = client
        .post(format!("{base_url}/api/instances"))
        .json(&serde_json::json!({ "name": "agent-1" }))
        .send()
        .await?;
    assert_eq!(resp.status(), 201);
    let body1: serde_json::Value = resp.json().await?;

    // Create second instance
    let resp = client
        .post(format!("{base_url}/api/instances"))
        .json(&serde_json::json!({ "name": "agent-2" }))
        .send()
        .await?;
    assert_eq!(resp.status(), 201);
    let body2: serde_json::Value = resp.json().await?;

    // Ports should be different
    assert_ne!(body1["port"], body2["port"]);

    Ok(())
}

#[tokio::test]
async fn create_instance_duplicate_name() -> Result<()> {
    let (_tmp, db_path) = setup_with_instance("existing", 18801);
    let (base_url, _shutdown) = start_test_server(db_path).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base_url}/api/instances"))
        .json(&serde_json::json!({ "name": "existing" }))
        .send()
        .await?;

    assert_eq!(resp.status(), 409);

    Ok(())
}

#[tokio::test]
async fn create_instance_invalid_names() -> Result<()> {
    let (_tmp, db_path) = setup_cp();
    let (base_url, _shutdown) = start_test_server(db_path).await;

    let client = reqwest::Client::new();

    // Empty name
    let resp = client
        .post(format!("{base_url}/api/instances"))
        .json(&serde_json::json!({ "name": "" }))
        .send()
        .await?;
    assert_eq!(resp.status(), 400);

    // Starts with hyphen
    let resp = client
        .post(format!("{base_url}/api/instances"))
        .json(&serde_json::json!({ "name": "-bad" }))
        .send()
        .await?;
    assert_eq!(resp.status(), 400);

    // Contains spaces
    let resp = client
        .post(format!("{base_url}/api/instances"))
        .json(&serde_json::json!({ "name": "has spaces" }))
        .send()
        .await?;
    assert_eq!(resp.status(), 400);

    // Too long (65 chars)
    let long_name = "a".repeat(65);
    let resp = client
        .post(format!("{base_url}/api/instances"))
        .json(&serde_json::json!({ "name": long_name }))
        .send()
        .await?;
    assert_eq!(resp.status(), 400);

    Ok(())
}

#[tokio::test]
async fn create_instance_duplicate_port() -> Result<()> {
    let (_tmp, db_path) = setup_with_instance("first", 18850);
    let (base_url, _shutdown) = start_test_server(db_path).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base_url}/api/instances"))
        .json(&serde_json::json!({ "name": "second", "port": 18850 }))
        .send()
        .await?;

    assert_eq!(resp.status(), 409);

    Ok(())
}

#[tokio::test]
async fn create_produces_bootable_config() -> Result<()> {
    let (_tmp, db_path) = setup_cp();
    let (base_url, _shutdown) = start_test_server(db_path.clone()).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base_url}/api/instances"))
        .json(&serde_json::json!({ "name": "bootable-test" }))
        .send()
        .await?;

    assert_eq!(resp.status(), 201);
    let body: serde_json::Value = resp.json().await?;
    let id = body["id"].as_str().unwrap();

    // Verify config.toml exists and parses
    let instances_dir = db_path.parent().unwrap().join("instances");
    let config_path = instances_dir.join(id).join("config.toml");
    assert!(config_path.exists());

    let config_str = fs::read_to_string(&config_path)?;
    let _config: zeroclaw::Config = toml::from_str(&config_str)?;

    Ok(())
}

#[tokio::test]
async fn create_produces_workspace() -> Result<()> {
    let (_tmp, db_path) = setup_cp();
    let (base_url, _shutdown) = start_test_server(db_path.clone()).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base_url}/api/instances"))
        .json(&serde_json::json!({ "name": "workspace-test" }))
        .send()
        .await?;

    assert_eq!(resp.status(), 201);
    let body: serde_json::Value = resp.json().await?;
    let id = body["id"].as_str().unwrap();

    let instances_dir = db_path.parent().unwrap().join("instances");
    let workspace_dir = instances_dir.join(id).join("workspace");

    for subdir in &["skills", "memory", "sessions", "state", "cron"] {
        assert!(
            workspace_dir.join(subdir).is_dir(),
            "workspace/{subdir} should exist"
        );
    }

    Ok(())
}

// ══════════════════════════════════════════════════════════════════
// Gate 2: Archive
// ══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn archive_instance_basic() -> Result<()> {
    let (_tmp, db_path) = setup_with_instance("archivable", 18801);
    let (base_url, _shutdown) = start_test_server(db_path).await;

    let client = reqwest::Client::new();

    // Archive
    let resp = client
        .post(format!("{base_url}/api/instances/archivable/archive"))
        .send()
        .await?;
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["status"], "archived");

    // Should no longer appear in list
    let resp = client
        .get(format!("{base_url}/api/instances"))
        .send()
        .await?;
    let list: serde_json::Value = resp.json().await?;
    let arr = list.as_array().unwrap();
    assert!(
        !arr.iter().any(|i| i["name"] == "archivable"),
        "Archived instance should not appear in list"
    );

    Ok(())
}

#[tokio::test]
async fn archive_nonexistent() -> Result<()> {
    let (_tmp, db_path) = setup_cp();
    let (base_url, _shutdown) = start_test_server(db_path).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base_url}/api/instances/nonexistent/archive"))
        .send()
        .await?;

    assert_eq!(resp.status(), 404);

    Ok(())
}

#[tokio::test]
async fn archive_already_archived() -> Result<()> {
    let (_tmp, db_path) = setup_with_instance("double-archive", 18801);
    let (base_url, _shutdown) = start_test_server(db_path).await;

    let client = reqwest::Client::new();

    // Archive once
    let resp = client
        .post(format!("{base_url}/api/instances/double-archive/archive"))
        .send()
        .await?;
    assert_eq!(resp.status(), 200);

    // Try to archive again (should 404 since it's no longer active)
    let resp = client
        .post(format!("{base_url}/api/instances/double-archive/archive"))
        .send()
        .await?;
    assert_eq!(resp.status(), 404);

    Ok(())
}

// ══════════════════════════════════════════════════════════════════
// Gate 3: Unarchive
// ══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn unarchive_instance_basic() -> Result<()> {
    let (_tmp, db_path) = setup_with_instance("restorable", 18801);
    let (base_url, _shutdown) = start_test_server(db_path).await;

    let client = reqwest::Client::new();

    // Archive then unarchive
    client
        .post(format!("{base_url}/api/instances/restorable/archive"))
        .send()
        .await?;

    let resp = client
        .post(format!("{base_url}/api/instances/restorable/unarchive"))
        .send()
        .await?;
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["status"], "active");

    // Should be back in list
    let resp = client
        .get(format!("{base_url}/api/instances"))
        .send()
        .await?;
    let list: serde_json::Value = resp.json().await?;
    let arr = list.as_array().unwrap();
    assert!(arr.iter().any(|i| i["name"] == "restorable"));

    Ok(())
}

#[tokio::test]
async fn unarchive_name_conflict() -> Result<()> {
    let (_tmp, db_path) = setup_cp();
    let (base_url, _shutdown) = start_test_server(db_path.clone()).await;

    let client = reqwest::Client::new();

    // Create and archive an instance
    client
        .post(format!("{base_url}/api/instances"))
        .json(&serde_json::json!({ "name": "conflicted" }))
        .send()
        .await?;

    client
        .post(format!("{base_url}/api/instances/conflicted/archive"))
        .send()
        .await?;

    // Create another instance with the same name
    client
        .post(format!("{base_url}/api/instances"))
        .json(&serde_json::json!({ "name": "conflicted" }))
        .send()
        .await?;

    // Try to unarchive the first one -- should conflict
    let resp = client
        .post(format!("{base_url}/api/instances/conflicted/unarchive"))
        .send()
        .await?;

    assert_eq!(resp.status(), 409);

    Ok(())
}

// ══════════════════════════════════════════════════════════════════
// Gate 4: Clone
// ══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn clone_instance_basic() -> Result<()> {
    let (_tmp, db_path) = setup_with_instance("original", 18801);
    let (base_url, _shutdown) = start_test_server(db_path).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base_url}/api/instances/original/clone"))
        .json(&serde_json::json!({ "new_name": "cloned" }))
        .send()
        .await?;

    assert_eq!(resp.status(), 201);
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["name"], "cloned");
    assert_eq!(body["cloned_from"], "original");
    assert_ne!(body["port"], 18801); // Should get a different port

    // Both should be in the list
    let resp = client
        .get(format!("{base_url}/api/instances"))
        .send()
        .await?;
    let list: serde_json::Value = resp.json().await?;
    let arr = list.as_array().unwrap();
    assert_eq!(arr.len(), 2);

    Ok(())
}

#[tokio::test]
async fn clone_clears_auth() -> Result<()> {
    let (_tmp, db_path) = setup_cp();
    let (base_url, _shutdown) = start_test_server(db_path.clone()).await;

    let client = reqwest::Client::new();

    // Create source with paired_tokens in config
    let resp = client
        .post(format!("{base_url}/api/instances"))
        .json(&serde_json::json!({ "name": "auth-source" }))
        .send()
        .await?;
    assert_eq!(resp.status(), 201);
    let body: serde_json::Value = resp.json().await?;
    let source_id = body["id"].as_str().unwrap().to_string();

    // Manually inject a paired_token into the source config
    let instances_dir = db_path.parent().unwrap().join("instances");
    let source_config = instances_dir.join(&source_id).join("config.toml");
    let mut config_str = fs::read_to_string(&source_config)?;
    config_str.push_str("\n[gateway]\npaired_tokens = [\"secret-token-123\"]\n");
    // Need to re-write without duplicate gateway section -- just overwrite properly
    let mut config: zeroclaw::Config = toml::from_str(&fs::read_to_string(&source_config)?)?;
    config.gateway.paired_tokens = vec!["secret-token-123".to_string()];
    let toml_out = toml::to_string_pretty(&config)?;
    fs::write(&source_config, &toml_out)?;

    // Clone it
    let resp = client
        .post(format!("{base_url}/api/instances/auth-source/clone"))
        .json(&serde_json::json!({ "new_name": "auth-clone" }))
        .send()
        .await?;
    assert_eq!(resp.status(), 201);
    let clone_body: serde_json::Value = resp.json().await?;
    let clone_id = clone_body["id"].as_str().unwrap();

    // Read clone config and verify paired_tokens is empty
    let clone_config_path = instances_dir.join(clone_id).join("config.toml");
    let clone_config: zeroclaw::Config = toml::from_str(&fs::read_to_string(&clone_config_path)?)?;
    assert!(
        clone_config.gateway.paired_tokens.is_empty(),
        "Cloned config should have empty paired_tokens"
    );

    Ok(())
}

// ══════════════════════════════════════════════════════════════════
// Gate 5: Delete
// ══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn delete_archived() -> Result<()> {
    let (_tmp, db_path) = setup_with_instance("deletable", 18801);
    let (base_url, _shutdown) = start_test_server(db_path.clone()).await;

    let client = reqwest::Client::new();

    // Archive first
    client
        .post(format!("{base_url}/api/instances/deletable/archive"))
        .send()
        .await?;

    // Delete
    let resp = client
        .delete(format!("{base_url}/api/instances/deletable"))
        .send()
        .await?;
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["status"], "deleted");

    Ok(())
}

#[tokio::test]
async fn delete_active_rejected() -> Result<()> {
    let (_tmp, db_path) = setup_with_instance("active-no-delete", 18801);
    let (base_url, _shutdown) = start_test_server(db_path).await;

    let client = reqwest::Client::new();
    let resp = client
        .delete(format!("{base_url}/api/instances/active-no-delete"))
        .send()
        .await?;

    assert_eq!(resp.status(), 409);
    let body: serde_json::Value = resp.json().await?;
    assert!(body["error"]
        .as_str()
        .unwrap()
        .contains("archived before deletion"));

    Ok(())
}

#[tokio::test]
async fn delete_nonexistent() -> Result<()> {
    let (_tmp, db_path) = setup_cp();
    let (base_url, _shutdown) = start_test_server(db_path).await;

    let client = reqwest::Client::new();
    let resp = client
        .delete(format!("{base_url}/api/instances/ghost"))
        .send()
        .await?;

    assert_eq!(resp.status(), 404);

    Ok(())
}

#[tokio::test]
async fn delete_preserves_messages() -> Result<()> {
    let (_tmp, db_path) = setup_with_instance("msg-source", 18801);
    let (base_url, _shutdown) = start_test_server(db_path.clone()).await;

    let client = reqwest::Client::new();

    // Create a second instance to be the message target
    client
        .post(format!("{base_url}/api/instances"))
        .json(&serde_json::json!({ "name": "msg-target" }))
        .send()
        .await?;

    // Create a routing rule
    client
        .post(format!("{base_url}/api/routing-rules"))
        .json(&serde_json::json!({
            "from_instance": "msg-source",
            "to_instance": "msg-target",
            "type_pattern": "*"
        }))
        .send()
        .await?;

    // Send a message (field is "type" not "message_type", payload is JSON value)
    let msg_resp = client
        .post(format!("{base_url}/api/messages"))
        .json(&serde_json::json!({
            "from_instance": "msg-source",
            "to_instance": "msg-target",
            "type": "test",
            "payload": {"text": "hello"}
        }))
        .send()
        .await?;
    assert_eq!(msg_resp.status(), 201);
    let msg_body: serde_json::Value = msg_resp.json().await?;
    let msg_id = msg_body["id"].as_str().unwrap().to_string();

    // Archive and delete the source
    client
        .post(format!("{base_url}/api/instances/msg-source/archive"))
        .send()
        .await?;

    let resp = client
        .delete(format!("{base_url}/api/instances/msg-source"))
        .send()
        .await?;
    assert_eq!(resp.status(), 200);

    // Message should still exist -- verify via direct DB query
    let registry = Registry::open(&db_path)?;
    let msg = registry.get_message(&msg_id)?;
    assert!(msg.is_some(), "Messages must be preserved after instance deletion (append-only contract)");

    Ok(())
}

// ══════════════════════════════════════════════════════════════════
// Gate 6: Two-step delete flow
// ══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn two_step_delete_flow() -> Result<()> {
    let (_tmp, db_path) = setup_cp();
    let (base_url, _shutdown) = start_test_server(db_path.clone()).await;

    let client = reqwest::Client::new();

    // Step 1: Create
    let resp = client
        .post(format!("{base_url}/api/instances"))
        .json(&serde_json::json!({ "name": "lifecycle-test" }))
        .send()
        .await?;
    assert_eq!(resp.status(), 201);

    // Step 2: Attempt delete without archive -> 409
    let resp = client
        .delete(format!("{base_url}/api/instances/lifecycle-test"))
        .send()
        .await?;
    assert_eq!(resp.status(), 409);

    // Step 3: Archive
    let resp = client
        .post(format!("{base_url}/api/instances/lifecycle-test/archive"))
        .send()
        .await?;
    assert_eq!(resp.status(), 200);

    // Step 4: Delete
    let resp = client
        .delete(format!("{base_url}/api/instances/lifecycle-test"))
        .send()
        .await?;
    assert_eq!(resp.status(), 200);

    // Verify instance is gone from list
    let resp = client
        .get(format!("{base_url}/api/instances"))
        .send()
        .await?;
    let list: serde_json::Value = resp.json().await?;
    let arr = list.as_array().unwrap();
    assert!(arr.is_empty());

    Ok(())
}
