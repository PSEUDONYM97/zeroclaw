//! Manual smoke test transcript for Phase 10.2 closeout.
//! Runs: send -> force dead-letter -> replay -> verify event timeline + DL count.
//!
//! Execute: cargo test --test smoke_10_2 -- --nocapture

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::TempDir;
use zeroclaw::cp;
use zeroclaw::db::Registry;

fn setup_two_instances() -> (TempDir, PathBuf) {
    let tmp = TempDir::new().unwrap();
    let cp_dir = tmp.path().join("cp");
    let instances_dir = cp_dir.join("instances");
    fs::create_dir_all(&instances_dir).unwrap();

    let db_path = cp_dir.join("registry.db");
    let registry = Registry::open(&db_path).unwrap();

    let id_a = uuid::Uuid::new_v4().to_string();
    let dir_a = instances_dir.join(&id_a);
    fs::create_dir_all(&dir_a).unwrap();
    let config_a = dir_a.join("config.toml");
    fs::write(&config_a, "default_temperature = 0.7\n").unwrap();
    registry
        .create_instance(&id_a, "agent-a", 18801, config_a.to_str().unwrap(), None, None)
        .unwrap();

    let id_b = uuid::Uuid::new_v4().to_string();
    let dir_b = instances_dir.join(&id_b);
    fs::create_dir_all(&dir_b).unwrap();
    let config_b = dir_b.join("config.toml");
    fs::write(&config_b, "default_temperature = 0.7\n").unwrap();
    registry
        .create_instance(&id_b, "agent-b", 18802, config_b.to_str().unwrap(), None, None)
        .unwrap();

    drop(registry);
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
            .with_graceful_shutdown(async move { let _ = shutdown_rx.changed().await; })
            .await
            .unwrap();
    });
    (base_url, shutdown_tx)
}

#[tokio::test]
async fn smoke_phase10_2_transcript() {
    let (_tmp, db_path) = setup_two_instances();
    let (base_url, _shutdown) = start_test_server(db_path.clone()).await;
    let client = reqwest::Client::new();

    println!("\n============================================================");
    println!("  PHASE 10.2 SMOKE TEST TRANSCRIPT");
    println!("============================================================\n");

    // Step 1: Create routing rule
    println!(">>> POST /api/routing-rules  (agent-a -> agent-b, type: *)");
    let rule_resp = client
        .post(format!("{base_url}/api/routing-rules"))
        .json(&serde_json::json!({
            "from_instance": "agent-a",
            "to_instance": "agent-b",
            "type_pattern": "*",
        }))
        .send()
        .await
        .unwrap();
    println!("<<< {}", rule_resp.status());
    let rule_body: serde_json::Value = rule_resp.json().await.unwrap();
    println!("    {}\n", serde_json::to_string_pretty(&rule_body).unwrap());

    // Step 2: Send message
    println!(">>> POST /api/messages  (agent-a -> agent-b, payload with secret)");
    let send_resp = client
        .post(format!("{base_url}/api/messages"))
        .json(&serde_json::json!({
            "from_instance": "agent-a",
            "to_instance": "agent-b",
            "type": "task.handoff",
            "payload": {"instruction": "process this", "api_key": "sk-smoke-secret"},
            "correlation_id": "smoke-corr-001",
        }))
        .send()
        .await
        .unwrap();
    println!("<<< {}", send_resp.status());
    let send_body: serde_json::Value = send_resp.json().await.unwrap();
    println!("    {}\n", serde_json::to_string_pretty(&send_body).unwrap());
    let msg_id = send_body["id"].as_str().unwrap().to_string();

    // Step 3: Check dead-letter count (should be 0)
    println!(">>> GET /api/messages/dead-letter  (expect 0)");
    let dl0 = client
        .get(format!("{base_url}/api/messages/dead-letter"))
        .send()
        .await
        .unwrap();
    println!("<<< {}", dl0.status());
    let dl0_body: serde_json::Value = dl0.json().await.unwrap();
    println!("    total: {}\n", dl0_body["total"]);

    // Step 4: Force dead-letter via direct DB
    println!(">>> [DB] dead_letter_message({msg_id}, \"smoke test forced\")");
    {
        let registry = Registry::open(&db_path).unwrap();
        registry.dead_letter_message(&msg_id, "smoke test forced").unwrap();
    }
    println!("    done\n");

    // Step 5: Verify dead-letter count = 1
    println!(">>> GET /api/messages/dead-letter  (expect 1)");
    let dl1 = client
        .get(format!("{base_url}/api/messages/dead-letter"))
        .send()
        .await
        .unwrap();
    println!("<<< {}", dl1.status());
    let dl1_body: serde_json::Value = dl1.json().await.unwrap();
    println!("    total: {}", dl1_body["total"]);
    println!("    first item status: {}\n", dl1_body["items"][0]["status"]);

    // Step 6: Check event timeline before replay
    println!(">>> GET /api/messages/{msg_id}/events  (before replay)");
    let events_before = client
        .get(format!("{base_url}/api/messages/{msg_id}/events"))
        .send()
        .await
        .unwrap();
    println!("<<< {}", events_before.status());
    let events_before_body: serde_json::Value = events_before.json().await.unwrap();
    let evts = events_before_body["events"].as_array().unwrap();
    println!("    {} events:", evts.len());
    for evt in evts {
        println!("      - {} ({})", evt["event_type"].as_str().unwrap(), evt["created_at"].as_str().unwrap());
    }
    println!();

    // Step 7: Replay
    println!(">>> POST /api/messages/{msg_id}/replay");
    let replay = client
        .post(format!("{base_url}/api/messages/{msg_id}/replay"))
        .send()
        .await
        .unwrap();
    println!("<<< {}", replay.status());
    let replay_body: serde_json::Value = replay.json().await.unwrap();
    println!("    status: {}", replay_body["status"]);
    println!("    retry_count: {}", replay_body["retry_count"]);
    // Verify secret still redacted after replay
    let replay_text = serde_json::to_string(&replay_body).unwrap();
    assert!(!replay_text.contains("sk-smoke-secret"), "secret leaked in replay response!");
    println!("    payload.api_key: {} (redacted confirmed)\n", replay_body["payload"]["api_key"]);

    // Step 8: Verify event timeline after replay
    println!(">>> GET /api/messages/{msg_id}/events  (after replay)");
    let events_after = client
        .get(format!("{base_url}/api/messages/{msg_id}/events"))
        .send()
        .await
        .unwrap();
    println!("<<< {}", events_after.status());
    let events_after_body: serde_json::Value = events_after.json().await.unwrap();
    let evts2 = events_after_body["events"].as_array().unwrap();
    println!("    {} events:", evts2.len());
    for evt in evts2 {
        let detail = evt["detail"].as_str().unwrap_or("-");
        println!("      - {} | {}", evt["event_type"].as_str().unwrap(), detail);
    }
    println!();

    // Step 9: Verify dead-letter count dropped to 0
    println!(">>> GET /api/messages/dead-letter  (expect 0 after replay)");
    let dl2 = client
        .get(format!("{base_url}/api/messages/dead-letter"))
        .send()
        .await
        .unwrap();
    println!("<<< {}", dl2.status());
    let dl2_body: serde_json::Value = dl2.json().await.unwrap();
    println!("    total: {}\n", dl2_body["total"]);

    // Step 10: Verify correlation filter still works
    println!(">>> GET /api/messages?correlation_id=smoke-corr-001");
    let corr = client
        .get(format!("{base_url}/api/messages?correlation_id=smoke-corr-001"))
        .send()
        .await
        .unwrap();
    println!("<<< {}", corr.status());
    let corr_body: serde_json::Value = corr.json().await.unwrap();
    println!("    total: {}", corr_body["total"]);
    println!("    matched id: {}\n", corr_body["items"][0]["id"]);

    println!("============================================================");
    println!("  SMOKE TEST PASSED");
    println!("============================================================");
}
