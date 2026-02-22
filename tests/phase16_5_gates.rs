use anyhow::Result;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::TempDir;
use zeroclaw::cp;
use zeroclaw::db::Registry;

/// Helper: create a registry with a registered instance in a temp dir.
/// When workspace_dir is true, sets workspace_dir to the instance directory.
fn setup_instance(
    name: &str,
    port: u16,
    config_toml: &str,
    with_workspace: bool,
) -> (TempDir, PathBuf, String, PathBuf) {
    let tmp = TempDir::new().unwrap();
    let cp_dir = tmp.path().join("cp");
    let instances_dir = cp_dir.join("instances");
    fs::create_dir_all(&instances_dir).unwrap();

    let db_path = cp_dir.join("registry.db");
    let registry = Registry::open(&db_path).unwrap();

    let id = uuid::Uuid::new_v4().to_string();
    let inst_dir = instances_dir.join(&id);
    fs::create_dir_all(&inst_dir).unwrap();

    let config_path = inst_dir.join("config.toml");
    fs::write(&config_path, config_toml).unwrap();

    let ws = if with_workspace {
        Some(inst_dir.to_str().unwrap())
    } else {
        None
    };

    registry
        .create_instance(&id, name, port, config_path.to_str().unwrap(), ws, None)
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

const MINIMAL_CONFIG: &str = "default_temperature = 0.7\n";

// ══════════════════════════════════════════════════════════════════
// Gate 1: SPA contains Telegram and Flows tabs
// ══════════════════════════════════════════════════════════════════

#[test]
fn spa_contains_telegram_and_flows_tabs() {
    let html = include_str!("../static/index.html");

    // Tab registration
    assert!(
        html.contains("'telegram'"),
        "SPA should contain 'telegram' tab"
    );
    assert!(
        html.contains("'flows'"),
        "SPA should contain 'flows' tab"
    );

    // Render functions
    assert!(
        html.contains("renderTelegram"),
        "SPA should contain renderTelegram function"
    );
    assert!(
        html.contains("renderFlows"),
        "SPA should contain renderFlows function"
    );
}

// ══════════════════════════════════════════════════════════════════
// Gate 2: SPA contains action functions and UI elements
// ══════════════════════════════════════════════════════════════════

#[test]
fn spa_contains_action_functions() {
    let html = include_str!("../static/index.html");

    assert!(
        html.contains("doForceComplete"),
        "SPA should contain doForceComplete function"
    );
    assert!(
        html.contains("doReplay"),
        "SPA should contain doReplay function"
    );
    assert!(
        html.contains("filter-bar"),
        "SPA should contain filter-bar CSS class"
    );
    assert!(
        html.contains("subtab"),
        "SPA should contain subtab CSS class"
    );
}

// ══════════════════════════════════════════════════════════════════
// Gate 3: Active flows returns empty when no state.db
// ══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn flows_active_no_state_db() -> Result<()> {
    let (_tmp, db_path, _id, _inst_dir) =
        setup_instance("flow-no-db", 19001, MINIMAL_CONFIG, true);

    let (base_url, shutdown) = start_test_server(db_path).await;
    let client = reqwest::Client::new();

    let resp = client
        .get(format!("{base_url}/api/instances/flow-no-db/flows/active"))
        .send()
        .await?;
    assert_eq!(resp.status(), 200);

    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["flows"], serde_json::json!([]));
    assert_eq!(body["total"], 0);

    let _ = shutdown.send(true);
    Ok(())
}

// ══════════════════════════════════════════════════════════════════
// Gate 4: Flow history returns empty when no state.db
// ══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn flows_history_no_state_db() -> Result<()> {
    let (_tmp, db_path, _id, _inst_dir) =
        setup_instance("flow-hist-no-db", 19002, MINIMAL_CONFIG, true);

    let (base_url, shutdown) = start_test_server(db_path).await;
    let client = reqwest::Client::new();

    let resp = client
        .get(format!(
            "{base_url}/api/instances/flow-hist-no-db/flows/history"
        ))
        .send()
        .await?;
    assert_eq!(resp.status(), 200);

    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["history"], serde_json::json!([]));
    assert_eq!(body["total"], 0);

    let _ = shutdown.send(true);
    Ok(())
}

// ══════════════════════════════════════════════════════════════════
// Gate 5: Force-complete returns 404 when no state.db
// ══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn flow_force_complete_no_db() -> Result<()> {
    let (_tmp, db_path, _id, _inst_dir) =
        setup_instance("flow-fc-no-db", 19003, MINIMAL_CONFIG, true);

    let (base_url, shutdown) = start_test_server(db_path).await;
    let client = reqwest::Client::new();

    let resp = client
        .delete(format!(
            "{base_url}/api/instances/flow-fc-no-db/flows/active/12345"
        ))
        .send()
        .await?;
    assert_eq!(resp.status(), 404);

    let _ = shutdown.send(true);
    Ok(())
}

// ══════════════════════════════════════════════════════════════════
// Gate 6: Replay returns 404 when no state.db
// ══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn flow_replay_no_db() -> Result<()> {
    let (_tmp, db_path, _id, _inst_dir) =
        setup_instance("flow-rp-no-db", 19004, MINIMAL_CONFIG, true);

    let (base_url, shutdown) = start_test_server(db_path).await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!(
            "{base_url}/api/instances/flow-rp-no-db/flows/active/12345/replay"
        ))
        .header("Content-Type", "application/json")
        .body(r#"{"step":"some_step"}"#)
        .send()
        .await?;
    assert_eq!(resp.status(), 404);

    let _ = shutdown.send(true);
    Ok(())
}

// ══════════════════════════════════════════════════════════════════
// Gate 7: Telegram events returns empty for fresh instance
// ══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn telegram_events_empty() -> Result<()> {
    let (_tmp, db_path, _id, _inst_dir) =
        setup_instance("tg-empty", 19005, MINIMAL_CONFIG, false);

    let (base_url, shutdown) = start_test_server(db_path).await;
    let client = reqwest::Client::new();

    let resp = client
        .get(format!(
            "{base_url}/api/instances/tg-empty/telegram/events"
        ))
        .send()
        .await?;
    assert_eq!(resp.status(), 200);

    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["events"], serde_json::json!([]));
    assert_eq!(body["total"], 0);

    let _ = shutdown.send(true);
    Ok(())
}

// ══════════════════════════════════════════════════════════════════
// Gate 8: Telegram health - valid window returns 200, invalid -> 400
// ══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn telegram_health_windows() -> Result<()> {
    let (_tmp, db_path, _id, _inst_dir) =
        setup_instance("tg-health", 19006, MINIMAL_CONFIG, false);

    let (base_url, shutdown) = start_test_server(db_path).await;
    let client = reqwest::Client::new();

    // Valid windows
    for win in &["1m", "5m", "15m", "1h"] {
        let resp = client
            .get(format!(
                "{base_url}/api/instances/tg-health/telegram/health?window={win}"
            ))
            .send()
            .await?;
        assert_eq!(
            resp.status(),
            200,
            "Window '{win}' should return 200"
        );

        let body: serde_json::Value = resp.json().await?;
        assert_eq!(body["window"], *win);
        assert!(body["counters"].is_object(), "Should have counters object");
    }

    // Invalid window
    let resp = client
        .get(format!(
            "{base_url}/api/instances/tg-health/telegram/health?window=99m"
        ))
        .send()
        .await?;
    assert_eq!(resp.status(), 400, "Invalid window should return 400");

    let _ = shutdown.send(true);
    Ok(())
}
