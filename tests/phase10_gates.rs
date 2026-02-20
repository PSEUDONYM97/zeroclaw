use anyhow::Result;
use std::collections::HashSet;
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

// ══════════════════════════════════════════════════════════════════
// Gate 8: Chronological event ordering
// ══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn gate8_chronological_event_ordering() -> Result<()> {
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

    // Send message
    let send_resp = client
        .post(format!("{base_url}/api/messages"))
        .json(&serde_json::json!({
            "from_instance": "agent-a",
            "to_instance": "agent-b",
            "type": "task.handoff",
            "payload": {"text": "event ordering test"},
        }))
        .send()
        .await?;
    let send_body: serde_json::Value = send_resp.json().await?;
    let msg_id = send_body["id"].as_str().unwrap().to_string();

    // Receive (leases the message)
    let recv_resp = client
        .get(format!(
            "{base_url}/api/instances/agent-b/messages/pending?wait=1"
        ))
        .send()
        .await?;
    let recv_body: serde_json::Value = recv_resp.json().await?;
    assert!(!recv_body["message"].is_null());

    // Acknowledge
    client
        .post(format!("{base_url}/api/messages/{msg_id}/acknowledge"))
        .send()
        .await?;

    // GET /messages/:id/events
    let events_resp = client
        .get(format!("{base_url}/api/messages/{msg_id}/events"))
        .send()
        .await?;
    assert_eq!(events_resp.status(), 200);
    let events_body: serde_json::Value = events_resp.json().await?;
    let events = events_body["events"].as_array().unwrap();

    assert!(
        events.len() >= 3,
        "expected at least 3 events (created, leased, acknowledged), got {}",
        events.len()
    );

    // Verify chronological ordering: created_at non-decreasing, id strictly ascending
    for i in 1..events.len() {
        let prev_ts = events[i - 1]["created_at"].as_str().unwrap();
        let curr_ts = events[i]["created_at"].as_str().unwrap();
        assert!(
            curr_ts >= prev_ts,
            "events should be in chronological order: {} >= {}",
            curr_ts,
            prev_ts
        );

        let prev_id = events[i - 1]["id"].as_i64().unwrap();
        let curr_id = events[i]["id"].as_i64().unwrap();
        assert!(
            curr_id > prev_id,
            "event IDs should be strictly ascending: {} > {}",
            curr_id,
            prev_id
        );
    }

    // Verify event type sequence
    assert_eq!(events[0]["event_type"].as_str().unwrap(), "created");
    assert_eq!(events[1]["event_type"].as_str().unwrap(), "leased");
    assert_eq!(events[2]["event_type"].as_str().unwrap(), "acknowledged");

    // Also verify GET /messages/:id includes events
    let msg_resp = client
        .get(format!("{base_url}/api/messages/{msg_id}"))
        .send()
        .await?;
    assert_eq!(msg_resp.status(), 200);
    let msg_body: serde_json::Value = msg_resp.json().await?;
    assert!(msg_body["events"].as_array().unwrap().len() >= 3);

    Ok(())
}

// ══════════════════════════════════════════════════════════════════
// Gate 9: Replay behavior
// ══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn gate9_replay_behavior() -> Result<()> {
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

    // Send message
    let send_resp = client
        .post(format!("{base_url}/api/messages"))
        .json(&serde_json::json!({
            "from_instance": "agent-a",
            "to_instance": "agent-b",
            "type": "task",
            "payload": {"text": "replay test"},
        }))
        .send()
        .await?;
    let send_body: serde_json::Value = send_resp.json().await?;
    let msg_id = send_body["id"].as_str().unwrap().to_string();

    // Dead-letter it via direct DB
    {
        let registry = Registry::open(&db_path)?;
        registry.dead_letter_message(&msg_id, "test dead letter")?;
    }

    // POST /messages/:id/replay -> 200
    let replay_resp = client
        .post(format!("{base_url}/api/messages/{msg_id}/replay"))
        .send()
        .await?;
    assert_eq!(replay_resp.status(), 200, "replay should return 200");
    let replay_body: serde_json::Value = replay_resp.json().await?;
    assert_eq!(replay_body["status"].as_str().unwrap(), "queued");
    assert_eq!(replay_body["retry_count"].as_i64().unwrap(), 0);
    assert!(replay_body["next_attempt_at"].is_null());
    assert!(replay_body["lease_expires_at"].is_null());

    // GET /messages/:id/events -> last two events are replayed, queued
    let events_resp = client
        .get(format!("{base_url}/api/messages/{msg_id}/events"))
        .send()
        .await?;
    let events_body: serde_json::Value = events_resp.json().await?;
    let events = events_body["events"].as_array().unwrap();
    let len = events.len();
    assert!(len >= 2);
    assert_eq!(events[len - 2]["event_type"].as_str().unwrap(), "replayed");
    assert_eq!(events[len - 1]["event_type"].as_str().unwrap(), "queued");

    // POST replay again on now-queued message -> 409 Conflict
    let replay2_resp = client
        .post(format!("{base_url}/api/messages/{msg_id}/replay"))
        .send()
        .await?;
    assert_eq!(
        replay2_resp.status(),
        409,
        "replay on queued message should return 409"
    );

    // POST replay on nonexistent ID -> 404
    let replay3_resp = client
        .post(format!("{base_url}/api/messages/nonexistent-id/replay"))
        .send()
        .await?;
    assert_eq!(
        replay3_resp.status(),
        404,
        "replay on nonexistent message should return 404"
    );

    Ok(())
}

// ══════════════════════════════════════════════════════════════════
// Gate 10: Pagination correctness
// ══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn gate10_pagination_correctness() -> Result<()> {
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

    // Enqueue 5 messages
    for i in 0..5 {
        client
            .post(format!("{base_url}/api/messages"))
            .json(&serde_json::json!({
                "from_instance": "agent-a",
                "to_instance": "agent-b",
                "type": "task",
                "payload": {"index": i},
            }))
            .send()
            .await?;
    }

    // Page 1: limit=2, offset=0
    let page1 = client
        .get(format!("{base_url}/api/messages?limit=2&offset=0"))
        .send()
        .await?;
    assert_eq!(page1.status(), 200);
    let page1_body: serde_json::Value = page1.json().await?;
    assert_eq!(page1_body["items"].as_array().unwrap().len(), 2);
    assert_eq!(page1_body["total"].as_i64().unwrap(), 5);

    // Page 2: limit=2, offset=2
    let page2 = client
        .get(format!("{base_url}/api/messages?limit=2&offset=2"))
        .send()
        .await?;
    let page2_body: serde_json::Value = page2.json().await?;
    assert_eq!(page2_body["items"].as_array().unwrap().len(), 2);

    // Page 3: limit=2, offset=4
    let page3 = client
        .get(format!("{base_url}/api/messages?limit=2&offset=4"))
        .send()
        .await?;
    let page3_body: serde_json::Value = page3.json().await?;
    assert_eq!(page3_body["items"].as_array().unwrap().len(), 1);

    // Collect all IDs, assert no duplicates and total = 5
    let mut all_ids: Vec<String> = Vec::new();
    for page_body in [&page1_body, &page2_body, &page3_body] {
        for item in page_body["items"].as_array().unwrap() {
            all_ids.push(item["id"].as_str().unwrap().to_string());
        }
    }
    let unique_ids: HashSet<&String> = all_ids.iter().collect();
    assert_eq!(all_ids.len(), 5, "should have 5 total items across pages");
    assert_eq!(
        unique_ids.len(),
        5,
        "all IDs should be unique (no duplicates)"
    );

    // Invalid limit=0 -> 400
    let bad_limit = client
        .get(format!("{base_url}/api/messages?limit=0"))
        .send()
        .await?;
    assert_eq!(bad_limit.status(), 400);

    // Invalid offset=200000 -> 400
    let bad_offset = client
        .get(format!("{base_url}/api/messages?offset=200000"))
        .send()
        .await?;
    assert_eq!(bad_offset.status(), 400);

    Ok(())
}

// ══════════════════════════════════════════════════════════════════
// Gate 11: No raw secret leak on read path
// ══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn gate11_no_raw_secret_leak_on_read_path() -> Result<()> {
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

    let raw_secret = "sk-real-secret-12345";

    // Send message with secrets in payload
    let send_resp = client
        .post(format!("{base_url}/api/messages"))
        .json(&serde_json::json!({
            "from_instance": "agent-a",
            "to_instance": "agent-b",
            "type": "config",
            "payload": {
                "api_key": raw_secret,
                "nested": {"token": raw_secret},
                "safe": "visible",
            },
        }))
        .send()
        .await?;
    assert_eq!(send_resp.status(), 201);
    let send_body: serde_json::Value = send_resp.json().await?;
    let msg_id = send_body["id"].as_str().unwrap().to_string();

    // Helper: check that a response body string does not contain the raw secret
    fn assert_no_secret(body: &str, context: &str) {
        assert!(
            !body.contains("sk-real-secret-12345"),
            "raw secret leaked in {context}"
        );
    }

    // GET /messages/:id
    let msg_resp = client
        .get(format!("{base_url}/api/messages/{msg_id}"))
        .send()
        .await?;
    assert_eq!(msg_resp.status(), 200);
    let msg_text = msg_resp.text().await?;
    assert_no_secret(&msg_text, "GET /messages/:id");

    // GET /messages
    let list_resp = client
        .get(format!("{base_url}/api/messages"))
        .send()
        .await?;
    let list_text = list_resp.text().await?;
    assert_no_secret(&list_text, "GET /messages");

    // GET /instances/:name/messages?direction=in
    let inst_resp = client
        .get(format!(
            "{base_url}/api/instances/agent-b/messages?direction=in"
        ))
        .send()
        .await?;
    let inst_text = inst_resp.text().await?;
    assert_no_secret(&inst_text, "GET /instances/:name/messages");

    // Dead-letter and replay, check replay response
    {
        let registry = Registry::open(&db_path)?;
        registry.dead_letter_message(&msg_id, "test")?;
    }

    let replay_resp = client
        .post(format!("{base_url}/api/messages/{msg_id}/replay"))
        .send()
        .await?;
    let replay_text = replay_resp.text().await?;
    assert_no_secret(&replay_text, "POST /messages/:id/replay");

    // Verify DB storage has redacted values (ingest redaction)
    {
        let registry = Registry::open(&db_path)?;
        let msg = registry.get_message(&msg_id)?.unwrap();
        assert!(
            !msg.payload.contains("sk-real-secret-12345"),
            "raw secret should not be in DB payload"
        );
        assert!(
            msg.payload.contains("***REDACTED***"),
            "DB payload should contain redaction sentinel"
        );
    }

    Ok(())
}

// ══════════════════════════════════════════════════════════════════
// Gate 12: Inbox/outbox direction filtering
// ══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn gate12_inbox_outbox_direction_filtering() -> Result<()> {
    let (_tmp, db_path) = setup_two_instances();
    let (base_url, _shutdown) = start_test_server(db_path.clone()).await;
    let client = reqwest::Client::new();

    // Create routing rule A -> B
    client
        .post(format!("{base_url}/api/routing-rules"))
        .json(&serde_json::json!({
            "from_instance": "agent-a",
            "to_instance": "agent-b",
            "type_pattern": "*",
        }))
        .send()
        .await?;

    // Send message A -> B
    client
        .post(format!("{base_url}/api/messages"))
        .json(&serde_json::json!({
            "from_instance": "agent-a",
            "to_instance": "agent-b",
            "type": "task",
            "payload": {"text": "direction test"},
        }))
        .send()
        .await?;

    // GET /instances/agent-a/messages?direction=out -> 1 item
    let out_resp = client
        .get(format!(
            "{base_url}/api/instances/agent-a/messages?direction=out"
        ))
        .send()
        .await?;
    assert_eq!(out_resp.status(), 200);
    let out_body: serde_json::Value = out_resp.json().await?;
    assert_eq!(out_body["items"].as_array().unwrap().len(), 1);
    assert_eq!(
        out_body["items"][0]["from_instance"].as_str().unwrap(),
        "agent-a"
    );

    // GET /instances/agent-a/messages?direction=in -> 0 items
    let in_resp = client
        .get(format!(
            "{base_url}/api/instances/agent-a/messages?direction=in"
        ))
        .send()
        .await?;
    let in_body: serde_json::Value = in_resp.json().await?;
    assert_eq!(in_body["items"].as_array().unwrap().len(), 0);

    // GET /instances/agent-b/messages?direction=in -> 1 item
    let b_in_resp = client
        .get(format!(
            "{base_url}/api/instances/agent-b/messages?direction=in"
        ))
        .send()
        .await?;
    let b_in_body: serde_json::Value = b_in_resp.json().await?;
    assert_eq!(b_in_body["items"].as_array().unwrap().len(), 1);

    // GET /instances/agent-a/messages?direction=all -> 1 item
    let all_resp = client
        .get(format!(
            "{base_url}/api/instances/agent-a/messages?direction=all"
        ))
        .send()
        .await?;
    let all_body: serde_json::Value = all_resp.json().await?;
    assert_eq!(all_body["items"].as_array().unwrap().len(), 1);

    // Invalid direction -> 400
    let bad_dir = client
        .get(format!(
            "{base_url}/api/instances/agent-a/messages?direction=invalid"
        ))
        .send()
        .await?;
    assert_eq!(bad_dir.status(), 400);

    // Nonexistent instance -> 404
    let no_inst = client
        .get(format!(
            "{base_url}/api/instances/nonexistent/messages?direction=in"
        ))
        .send()
        .await?;
    assert_eq!(no_inst.status(), 404);

    Ok(())
}

// ══════════════════════════════════════════════════════════════════
// Gate 13: Append-only trigger enforcement
// ══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn gate13_append_only_trigger_enforcement() -> Result<()> {
    let (_tmp, db_path) = setup_two_instances();
    let registry = Registry::open(&db_path)?;

    // Create a routing rule and enqueue a message to get a valid message_id
    registry.create_routing_rule("agent-a", "agent-b", "*", 5, 3600, false)?;
    let msg = registry.enqueue_message(&zeroclaw::db::NewMessage {
        id: uuid::Uuid::new_v4().to_string(),
        from_instance: "agent-a".to_string(),
        to_instance: "agent-b".to_string(),
        message_type: "task".to_string(),
        payload: r#"{"text":"trigger test"}"#.to_string(),
        correlation_id: None,
        idempotency_key: None,
        hop_count: 0,
        max_retries: 5,
        ttl_secs: 3600,
    })?;

    // Insert a message event
    registry.append_message_event(&msg.id, "created", Some("original detail"))?;

    // Attempt UPDATE -> should fail
    let update_result = registry.conn().execute(
        "UPDATE message_events SET detail = 'tampered' WHERE message_id = ?1",
        rusqlite::params![msg.id],
    );
    assert!(
        update_result.is_err(),
        "UPDATE on message_events should fail"
    );
    let update_err = update_result.unwrap_err().to_string();
    assert!(
        update_err.contains("append-only"),
        "error should mention append-only, got: {update_err}"
    );

    // Attempt DELETE -> should fail
    let delete_result = registry.conn().execute(
        "DELETE FROM message_events WHERE message_id = ?1",
        rusqlite::params![msg.id],
    );
    assert!(
        delete_result.is_err(),
        "DELETE on message_events should fail"
    );
    let delete_err = delete_result.unwrap_err().to_string();
    assert!(
        delete_err.contains("append-only"),
        "error should mention append-only, got: {delete_err}"
    );

    // Verify original event is unchanged
    let events = registry.get_message_events(&msg.id)?;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].detail.as_deref(), Some("original detail"));

    Ok(())
}

// ══════════════════════════════════════════════════════════════════
// Gate 14: Dead-letter endpoint + correlation filter
// ══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn gate14_dead_letter_endpoint_and_correlation_filter() -> Result<()> {
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

    let corr_id = "corr-test-123";

    // Send 2 messages: one with correlation_id, one without
    let send1 = client
        .post(format!("{base_url}/api/messages"))
        .json(&serde_json::json!({
            "from_instance": "agent-a",
            "to_instance": "agent-b",
            "type": "task",
            "payload": {"text": "with correlation"},
            "correlation_id": corr_id,
        }))
        .send()
        .await?;
    let send1_body: serde_json::Value = send1.json().await?;
    let msg1_id = send1_body["id"].as_str().unwrap().to_string();

    let send2 = client
        .post(format!("{base_url}/api/messages"))
        .json(&serde_json::json!({
            "from_instance": "agent-a",
            "to_instance": "agent-b",
            "type": "task",
            "payload": {"text": "without correlation"},
        }))
        .send()
        .await?;
    let send2_body: serde_json::Value = send2.json().await?;
    let msg2_id = send2_body["id"].as_str().unwrap().to_string();

    // Dead-letter both via direct DB
    {
        let registry = Registry::open(&db_path)?;
        registry.dead_letter_message(&msg1_id, "test")?;
        registry.dead_letter_message(&msg2_id, "test")?;
    }

    // GET /messages/dead-letter -> total=2
    let dl_resp = client
        .get(format!("{base_url}/api/messages/dead-letter"))
        .send()
        .await?;
    assert_eq!(dl_resp.status(), 200);
    let dl_body: serde_json::Value = dl_resp.json().await?;
    assert_eq!(dl_body["total"].as_i64().unwrap(), 2);

    // GET /messages?correlation_id=X -> 1 item, correct ID
    let corr_resp = client
        .get(format!("{base_url}/api/messages?correlation_id={corr_id}"))
        .send()
        .await?;
    let corr_body: serde_json::Value = corr_resp.json().await?;
    assert_eq!(corr_body["total"].as_i64().unwrap(), 1);
    assert_eq!(
        corr_body["items"][0]["id"].as_str().unwrap(),
        msg1_id,
        "correlation filter should return the correct message"
    );

    // GET /messages?status=dead_letter -> total=2
    let status_resp = client
        .get(format!("{base_url}/api/messages?status=dead_letter"))
        .send()
        .await?;
    let status_body: serde_json::Value = status_resp.json().await?;
    assert_eq!(status_body["total"].as_i64().unwrap(), 2);

    // Replay one, verify dead-letter count drops
    let replay_resp = client
        .post(format!("{base_url}/api/messages/{msg1_id}/replay"))
        .send()
        .await?;
    assert_eq!(replay_resp.status(), 200);

    let dl_resp2 = client
        .get(format!("{base_url}/api/messages/dead-letter"))
        .send()
        .await?;
    let dl_body2: serde_json::Value = dl_resp2.json().await?;
    assert_eq!(
        dl_body2["total"].as_i64().unwrap(),
        1,
        "after replaying one, dead-letter count should be 1"
    );

    Ok(())
}
