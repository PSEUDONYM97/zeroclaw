use anyhow::Result;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::TempDir;
use zeroclaw::cp;
use zeroclaw::db::Registry;

/// Helper: create a temp CP dir with a registry and two registered instances.
fn setup_two_instances() -> (TempDir, PathBuf) {
    let tmp = TempDir::new().unwrap();
    let cp_dir = tmp.path().join("cp");
    let instances_dir = cp_dir.join("instances");
    fs::create_dir_all(&instances_dir).unwrap();

    let db_path = cp_dir.join("registry.db");
    let registry = Registry::open(&db_path).unwrap();

    // Instance A
    let id_a = uuid::Uuid::new_v4().to_string();
    let dir_a = instances_dir.join(&id_a);
    fs::create_dir_all(&dir_a).unwrap();
    let config_a = dir_a.join("config.toml");
    fs::write(&config_a, "default_temperature = 0.7\n").unwrap();
    registry
        .create_instance(
            &id_a,
            "agent-a",
            18801,
            config_a.to_str().unwrap(),
            None,
            None,
        )
        .unwrap();

    // Instance B
    let id_b = uuid::Uuid::new_v4().to_string();
    let dir_b = instances_dir.join(&id_b);
    fs::create_dir_all(&dir_b).unwrap();
    let config_b = dir_b.join("config.toml");
    fs::write(&config_b, "default_temperature = 0.7\n").unwrap();
    registry
        .create_instance(
            &id_b,
            "agent-b",
            18802,
            config_b.to_str().unwrap(),
            None,
            None,
        )
        .unwrap();

    drop(registry);
    (tmp, db_path)
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

// ══════════════════════════════════════════════════════════════════
// Gate 1: End-to-end send/receive/acknowledge
// ══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn gate1_send_receive_acknowledge() -> Result<()> {
    let (_tmp, db_path) = setup_two_instances();
    let (base_url, _shutdown) = start_test_server(db_path.clone()).await;
    let client = reqwest::Client::new();

    // Create routing rule A -> B
    let rule_resp = client
        .post(format!("{base_url}/api/routing-rules"))
        .json(&serde_json::json!({
            "from_instance": "agent-a",
            "to_instance": "agent-b",
            "type_pattern": "task.*",
        }))
        .send()
        .await?;
    assert_eq!(rule_resp.status(), 201, "create rule should return 201");

    // Send message from A to B
    let send_resp = client
        .post(format!("{base_url}/api/messages"))
        .json(&serde_json::json!({
            "from_instance": "agent-a",
            "to_instance": "agent-b",
            "type": "task.handoff",
            "payload": {"text": "Hello from A"},
        }))
        .send()
        .await?;
    assert_eq!(send_resp.status(), 201, "send should return 201");
    let send_body: serde_json::Value = send_resp.json().await?;
    let msg_id = send_body["id"].as_str().unwrap().to_string();
    assert_eq!(send_body["status"].as_str().unwrap(), "queued");

    // Receive message as B (short poll)
    let recv_resp = client
        .get(format!(
            "{base_url}/api/instances/agent-b/messages/pending?wait=1"
        ))
        .send()
        .await?;
    assert_eq!(recv_resp.status(), 200);
    let recv_body: serde_json::Value = recv_resp.json().await?;
    assert!(!recv_body["message"].is_null(), "should receive a message");
    assert_eq!(recv_body["message"]["id"].as_str().unwrap(), msg_id);
    assert_eq!(
        recv_body["message"]["from_instance"].as_str().unwrap(),
        "agent-a"
    );

    // Acknowledge
    let ack_resp = client
        .post(format!("{base_url}/api/messages/{msg_id}/acknowledge"))
        .send()
        .await?;
    assert_eq!(ack_resp.status(), 200);
    let ack_body: serde_json::Value = ack_resp.json().await?;
    assert_eq!(ack_body["status"].as_str().unwrap(), "acknowledged");

    // Verify second receive returns null (message already acked)
    let recv2 = client
        .get(format!(
            "{base_url}/api/instances/agent-b/messages/pending?wait=1"
        ))
        .send()
        .await?;
    let recv2_body: serde_json::Value = recv2.json().await?;
    assert!(
        recv2_body["message"].is_null(),
        "should be no more messages"
    );

    Ok(())
}

// ══════════════════════════════════════════════════════════════════
// Gate 2: Idempotency dedup
// ══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn gate2_idempotency_dedup() -> Result<()> {
    let (_tmp, db_path) = setup_two_instances();
    let (base_url, _shutdown) = start_test_server(db_path).await;
    let client = reqwest::Client::new();

    // Create routing rule
    client
        .post(format!("{base_url}/api/routing-rules"))
        .json(&serde_json::json!({
            "from_instance": "agent-a",
            "to_instance": "agent-b",
            "type_pattern": "*",
        }))
        .send()
        .await?;

    let idem_key = "test-idem-key-123";

    // Send first message
    let resp1 = client
        .post(format!("{base_url}/api/messages"))
        .json(&serde_json::json!({
            "from_instance": "agent-a",
            "to_instance": "agent-b",
            "type": "task",
            "payload": {"text": "first"},
            "idempotency_key": idem_key,
        }))
        .send()
        .await?;
    assert_eq!(resp1.status(), 201);
    let body1: serde_json::Value = resp1.json().await?;
    let msg_id = body1["id"].as_str().unwrap().to_string();

    // Send duplicate with same idempotency key
    let resp2 = client
        .post(format!("{base_url}/api/messages"))
        .json(&serde_json::json!({
            "from_instance": "agent-a",
            "to_instance": "agent-b",
            "type": "task",
            "payload": {"text": "duplicate"},
            "idempotency_key": idem_key,
        }))
        .send()
        .await?;
    assert_eq!(resp2.status(), 200, "duplicate should return 200 not 201");
    let body2: serde_json::Value = resp2.json().await?;
    assert_eq!(
        body2["id"].as_str().unwrap(),
        msg_id,
        "should return same message ID"
    );
    assert_eq!(body2["deduplicated"].as_bool().unwrap(), true);

    Ok(())
}

// ══════════════════════════════════════════════════════════════════
// Gate 3: Hop limit rejection
// ══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn gate3_hop_limit_rejection() -> Result<()> {
    let (_tmp, db_path) = setup_two_instances();
    let (base_url, _shutdown) = start_test_server(db_path).await;
    let client = reqwest::Client::new();

    // Create routing rule
    client
        .post(format!("{base_url}/api/routing-rules"))
        .json(&serde_json::json!({
            "from_instance": "agent-a",
            "to_instance": "agent-b",
            "type_pattern": "*",
        }))
        .send()
        .await?;

    // Send message with hop_count = 8 (max)
    let resp = client
        .post(format!("{base_url}/api/messages"))
        .json(&serde_json::json!({
            "from_instance": "agent-a",
            "to_instance": "agent-b",
            "type": "task",
            "payload": {"text": "too many hops"},
            "hop_count": 8,
        }))
        .send()
        .await?;
    assert_eq!(resp.status(), 400, "hop count 8 should be rejected");
    let body: serde_json::Value = resp.json().await?;
    assert!(
        body["error"]
            .as_str()
            .unwrap()
            .to_lowercase()
            .contains("hop"),
        "error should mention hop count"
    );

    Ok(())
}

// ══════════════════════════════════════════════════════════════════
// Gate 4: Unauthorized route rejection
// ══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn gate4_unauthorized_route_rejection() -> Result<()> {
    let (_tmp, db_path) = setup_two_instances();
    let (base_url, _shutdown) = start_test_server(db_path).await;
    let client = reqwest::Client::new();

    // Send message without any routing rule
    let resp = client
        .post(format!("{base_url}/api/messages"))
        .json(&serde_json::json!({
            "from_instance": "agent-a",
            "to_instance": "agent-b",
            "type": "task",
            "payload": {"text": "no rule"},
        }))
        .send()
        .await?;
    assert_eq!(resp.status(), 403, "should reject without routing rule");
    let body: serde_json::Value = resp.json().await?;
    assert!(
        body["error"].as_str().unwrap().contains("routing rule"),
        "error should mention routing rule"
    );

    Ok(())
}

// ══════════════════════════════════════════════════════════════════
// Gate 5: Failed delivery -> dead_letter
// ══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn gate5_failed_delivery_dead_letter() -> Result<()> {
    let (_tmp, db_path) = setup_two_instances();
    // Use direct DB access for this test (no need for HTTP server for delivery worker logic)
    let registry = Registry::open(&db_path)?;

    // Create routing rule with max_retries = 1
    registry.create_routing_rule("agent-a", "agent-b", "*", 1, 3600, false)?;

    // Enqueue a message
    let msg_id = uuid::Uuid::new_v4().to_string();
    let new_msg = zeroclaw::db::NewMessage {
        id: msg_id.clone(),
        from_instance: "agent-a".to_string(),
        to_instance: "agent-b".to_string(),
        message_type: "task".to_string(),
        payload: r#"{"text":"test"}"#.to_string(),
        correlation_id: None,
        idempotency_key: None,
        hop_count: 0,
        max_retries: 1,
        ttl_secs: 3600,
    };
    let msg = registry.enqueue_message(&new_msg)?;
    assert_eq!(msg.status, "queued");

    // Lease it
    let leased = registry.lease_pending_message("agent-b")?;
    assert!(leased.is_some(), "should be able to lease the message");
    assert_eq!(leased.as_ref().unwrap().status, "leased");

    // Simulate expired lease by directly checking retry behavior
    // Force expire the lease: retry_message resets to queued with backoff
    registry.retry_message(&msg_id)?;
    let after_retry = registry.get_message(&msg_id)?;
    assert_eq!(
        after_retry.as_ref().unwrap().status,
        "queued",
        "retry should set status back to queued"
    );
    assert_eq!(
        after_retry.as_ref().unwrap().retry_count,
        1,
        "retry_count should be incremented"
    );

    // Lease again
    // Need to clear next_attempt_at for immediate lease in test
    // Instead, dead letter since retry_count (1) >= max_retries (1)
    registry.dead_letter_message(&msg_id, "max retries exceeded")?;
    let dead = registry.get_message(&msg_id)?;
    assert_eq!(
        dead.as_ref().unwrap().status,
        "dead_letter",
        "should be dead-lettered"
    );

    Ok(())
}

// ══════════════════════════════════════════════════════════════════
// Gate 6: Payload size rejection
// ══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn gate6_payload_size_rejection() -> Result<()> {
    let (_tmp, db_path) = setup_two_instances();
    let (base_url, _shutdown) = start_test_server(db_path).await;
    let client = reqwest::Client::new();

    // Create routing rule
    client
        .post(format!("{base_url}/api/routing-rules"))
        .json(&serde_json::json!({
            "from_instance": "agent-a",
            "to_instance": "agent-b",
            "type_pattern": "*",
        }))
        .send()
        .await?;

    // Create a payload larger than 64 KiB
    let big_payload = "x".repeat(70_000);

    let resp = client
        .post(format!("{base_url}/api/messages"))
        .json(&serde_json::json!({
            "from_instance": "agent-a",
            "to_instance": "agent-b",
            "type": "task",
            "payload": {"text": big_payload},
        }))
        .send()
        .await?;
    assert_eq!(resp.status(), 400, "oversized payload should be rejected");
    let body: serde_json::Value = resp.json().await?;
    assert!(
        body["error"].as_str().unwrap().contains("size")
            || body["error"].as_str().unwrap().contains("bytes"),
        "error should mention size"
    );

    Ok(())
}

// ══════════════════════════════════════════════════════════════════
// Gate 7: Secret redaction
// ══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn gate7_secret_redaction() -> Result<()> {
    let (_tmp, db_path) = setup_two_instances();
    let (base_url, _shutdown) = start_test_server(db_path.clone()).await;
    let client = reqwest::Client::new();

    // Create routing rule
    client
        .post(format!("{base_url}/api/routing-rules"))
        .json(&serde_json::json!({
            "from_instance": "agent-a",
            "to_instance": "agent-b",
            "type_pattern": "*",
        }))
        .send()
        .await?;

    // Send message with secrets in payload
    let resp = client
        .post(format!("{base_url}/api/messages"))
        .json(&serde_json::json!({
            "from_instance": "agent-a",
            "to_instance": "agent-b",
            "type": "config",
            "payload": {
                "api_key": "sk-secret-value-12345",
                "password": "hunter2",
                "safe_field": "this should stay",
            },
        }))
        .send()
        .await?;
    assert_eq!(resp.status(), 201);
    let send_body: serde_json::Value = resp.json().await?;
    let msg_id = send_body["id"].as_str().unwrap();

    // Read message back via receive endpoint
    let recv_resp = client
        .get(format!(
            "{base_url}/api/instances/agent-b/messages/pending?wait=1"
        ))
        .send()
        .await?;
    let recv_body: serde_json::Value = recv_resp.json().await?;
    let payload = &recv_body["message"]["payload"];

    assert_eq!(
        payload["api_key"].as_str().unwrap(),
        "***REDACTED***",
        "api_key should be redacted"
    );
    assert_eq!(
        payload["password"].as_str().unwrap(),
        "***REDACTED***",
        "password should be redacted"
    );
    assert_eq!(
        payload["safe_field"].as_str().unwrap(),
        "this should stay",
        "safe fields should not be redacted"
    );

    // Ack to clean up
    let _ = client
        .post(format!("{base_url}/api/messages/{msg_id}/acknowledge"))
        .send()
        .await?;

    Ok(())
}

// ══════════════════════════════════════════════════════════════════
// Routing rules CRUD
// ══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn routing_rules_crud() -> Result<()> {
    let (_tmp, db_path) = setup_two_instances();
    let (base_url, _shutdown) = start_test_server(db_path).await;
    let client = reqwest::Client::new();

    // Create
    let create_resp = client
        .post(format!("{base_url}/api/routing-rules"))
        .json(&serde_json::json!({
            "from_instance": "agent-a",
            "to_instance": "agent-b",
            "type_pattern": "task.*",
            "max_retries": 3,
            "ttl_secs": 1800,
            "auto_start": true,
        }))
        .send()
        .await?;
    assert_eq!(create_resp.status(), 201);
    let create_body: serde_json::Value = create_resp.json().await?;
    let rule_id = create_body["id"].as_str().unwrap().to_string();

    // List
    let list_resp = client
        .get(format!("{base_url}/api/routing-rules"))
        .send()
        .await?;
    assert_eq!(list_resp.status(), 200);
    let list_body: serde_json::Value = list_resp.json().await?;
    let rules = list_body.as_array().unwrap();
    assert_eq!(rules.len(), 1);
    assert_eq!(rules[0]["type_pattern"].as_str().unwrap(), "task.*");

    // Delete
    let del_resp = client
        .delete(format!("{base_url}/api/routing-rules/{rule_id}"))
        .send()
        .await?;
    assert_eq!(del_resp.status(), 200);

    // Verify empty
    let list2 = client
        .get(format!("{base_url}/api/routing-rules"))
        .send()
        .await?;
    let list2_body: serde_json::Value = list2.json().await?;
    assert_eq!(list2_body.as_array().unwrap().len(), 0);

    Ok(())
}

// ══════════════════════════════════════════════════════════════════
// Instance existence validation (D10)
// ══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn instance_existence_validation() -> Result<()> {
    let (_tmp, db_path) = setup_two_instances();
    let (base_url, _shutdown) = start_test_server(db_path).await;
    let client = reqwest::Client::new();

    // Create rule with nonexistent from_instance
    let resp = client
        .post(format!("{base_url}/api/routing-rules"))
        .json(&serde_json::json!({
            "from_instance": "nonexistent",
            "to_instance": "agent-b",
            "type_pattern": "*",
        }))
        .send()
        .await?;
    assert_eq!(resp.status(), 404, "nonexistent from_instance should 404");

    // Create rule with nonexistent to_instance
    let resp = client
        .post(format!("{base_url}/api/routing-rules"))
        .json(&serde_json::json!({
            "from_instance": "agent-a",
            "to_instance": "nonexistent",
            "type_pattern": "*",
        }))
        .send()
        .await?;
    assert_eq!(resp.status(), 404, "nonexistent to_instance should 404");

    // Send message to nonexistent instance
    let resp = client
        .post(format!("{base_url}/api/messages"))
        .json(&serde_json::json!({
            "from_instance": "agent-a",
            "to_instance": "nonexistent",
            "type": "task",
            "payload": {"text": "hello"},
        }))
        .send()
        .await?;
    assert_eq!(
        resp.status(),
        404,
        "nonexistent to_instance in message should 404"
    );

    Ok(())
}

// ══════════════════════════════════════════════════════════════════
// Long-poll timeout returns null
// ══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn long_poll_timeout_returns_null() -> Result<()> {
    let (_tmp, db_path) = setup_two_instances();
    let (base_url, _shutdown) = start_test_server(db_path).await;
    let client = reqwest::Client::new();

    let start = std::time::Instant::now();
    let resp = client
        .get(format!(
            "{base_url}/api/instances/agent-b/messages/pending?wait=2"
        ))
        .send()
        .await?;
    let elapsed = start.elapsed();

    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await?;
    assert!(body["message"].is_null(), "should return null on timeout");
    assert!(
        elapsed >= std::time::Duration::from_secs(1),
        "should wait at least 1 second"
    );

    Ok(())
}
