use anyhow::Result;
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

    let config_path = inst_dir.join("config.toml");
    fs::write(&config_path, config_toml).unwrap();

    registry
        .create_instance(&id, name, port, config_path.to_str().unwrap(), None, None)
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
    "default_temperature = 0.7\n".to_string()
}

fn config_with_secret() -> String {
    r#"
api_key = "SECRET_TOP_LEVEL_KEY"
default_temperature = 0.7
"#
    .to_string()
}

// ══════════════════════════════════════════════════════════════════
// Gate 1: UI serving + API/UI separation
// ══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn gate1_root_serves_html() -> Result<()> {
    let tmp = TempDir::new()?;
    let cp_dir = tmp.path().join("cp");
    fs::create_dir_all(cp_dir.join("instances"))?;
    let db_path = cp_dir.join("registry.db");
    let _registry = Registry::open(&db_path)?;

    let (base_url, shutdown) = start_test_server(db_path).await;
    let client = reqwest::Client::new();

    let resp = client.get(&base_url).send().await?;
    assert_eq!(resp.status(), 200);

    let ct = resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    assert!(
        ct.contains("text/html"),
        "Root should return text/html, got: {ct}"
    );

    let body = resp.text().await?;
    assert!(body.contains("<!DOCTYPE html>"), "Should contain DOCTYPE");
    assert!(
        body.contains("ZeroClaw"),
        "Should contain ZeroClaw in title"
    );

    let _ = shutdown.send(true);
    Ok(())
}

#[tokio::test]
async fn gate1_unknown_ui_route_serves_html() -> Result<()> {
    let tmp = TempDir::new()?;
    let cp_dir = tmp.path().join("cp");
    fs::create_dir_all(cp_dir.join("instances"))?;
    let db_path = cp_dir.join("registry.db");
    let _registry = Registry::open(&db_path)?;

    let (base_url, shutdown) = start_test_server(db_path).await;
    let client = reqwest::Client::new();

    let resp = client.get(format!("{base_url}/dashboard")).send().await?;
    assert_eq!(resp.status(), 200);

    let ct = resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    assert!(
        ct.contains("text/html"),
        "SPA fallback should return text/html, got: {ct}"
    );

    let _ = shutdown.send(true);
    Ok(())
}

#[tokio::test]
async fn gate1_unknown_api_route_returns_json_404() -> Result<()> {
    let tmp = TempDir::new()?;
    let cp_dir = tmp.path().join("cp");
    fs::create_dir_all(cp_dir.join("instances"))?;
    let db_path = cp_dir.join("registry.db");
    let _registry = Registry::open(&db_path)?;

    let (base_url, shutdown) = start_test_server(db_path).await;
    let client = reqwest::Client::new();

    let resp = client
        .get(format!("{base_url}/api/nonexistent"))
        .send()
        .await?;
    assert_eq!(resp.status(), 404);

    let ct = resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    assert!(
        ct.contains("application/json"),
        "API 404 should return JSON, got: {ct}"
    );

    let body: serde_json::Value = resp.json().await?;
    assert!(
        body["error"].is_string(),
        "API 404 body should contain error field"
    );

    let _ = shutdown.send(true);
    Ok(())
}

#[tokio::test]
async fn gate1_unknown_api_nested_returns_json_404() -> Result<()> {
    let tmp = TempDir::new()?;
    let cp_dir = tmp.path().join("cp");
    fs::create_dir_all(cp_dir.join("instances"))?;
    let db_path = cp_dir.join("registry.db");
    let _registry = Registry::open(&db_path)?;

    let (base_url, shutdown) = start_test_server(db_path).await;
    let client = reqwest::Client::new();

    let resp = client
        .get(format!("{base_url}/api/instances/fake/nonexistent"))
        .send()
        .await?;
    assert_eq!(resp.status(), 404);

    let ct = resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    assert!(
        ct.contains("application/json"),
        "Nested API 404 should return JSON, got: {ct}"
    );

    let _ = shutdown.send(true);
    Ok(())
}

// ══════════════════════════════════════════════════════════════════
// Gate 2: Content-type separation
// ══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn gate2_html_for_ui_json_for_api() -> Result<()> {
    let (_tmp, db_path, _id, _inst_dir) = setup_instance("ct-test", 19100, &minimal_config());

    let (base_url, shutdown) = start_test_server(db_path).await;
    let client = reqwest::Client::new();

    // UI route returns HTML
    let ui_resp = client.get(&base_url).send().await?;
    let ui_ct = ui_resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()?
        .to_string();
    assert!(ui_ct.contains("text/html"), "UI should be HTML");

    // API routes return JSON
    let health_resp = client.get(format!("{base_url}/api/health")).send().await?;
    let health_ct = health_resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()?
        .to_string();
    assert!(
        health_ct.contains("application/json"),
        "Health should be JSON, got: {health_ct}"
    );

    let list_resp = client
        .get(format!("{base_url}/api/instances"))
        .send()
        .await?;
    let list_ct = list_resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()?
        .to_string();
    assert!(
        list_ct.contains("application/json"),
        "Instances list should be JSON, got: {list_ct}"
    );

    let inst_resp = client
        .get(format!("{base_url}/api/instances/ct-test"))
        .send()
        .await?;
    let inst_ct = inst_resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()?
        .to_string();
    assert!(
        inst_ct.contains("application/json"),
        "Instance GET should be JSON, got: {inst_ct}"
    );

    let _ = shutdown.send(true);
    Ok(())
}

// ══════════════════════════════════════════════════════════════════
// Gate 3: HTML structure
// ══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn gate3_html_contains_required_elements() -> Result<()> {
    let tmp = TempDir::new()?;
    let cp_dir = tmp.path().join("cp");
    fs::create_dir_all(cp_dir.join("instances"))?;
    let db_path = cp_dir.join("registry.db");
    let _registry = Registry::open(&db_path)?;

    let (base_url, shutdown) = start_test_server(db_path).await;
    let client = reqwest::Client::new();

    let resp = client.get(&base_url).send().await?;
    let body = resp.text().await?;

    assert!(body.contains("<style>"), "Should contain inline styles");
    assert!(body.contains("<script>"), "Should contain inline script");
    assert!(
        body.contains("id=\"app\""),
        "Should contain app mount point"
    );
    assert!(body.contains("hashchange"), "Should use hash-based routing");
    assert!(
        body.contains("/api/instances"),
        "Should reference API endpoints"
    );
    // Behavioral XSS safety check: the SPA must not use dangerous
    // innerHTML assignment patterns that could inject API data as HTML.
    let has_dangerous_innerhtml = body.contains(".innerHTML =");
    assert!(
        !has_dangerous_innerhtml,
        "SPA should not use innerHTML assignment (XSS risk)"
    );

    let _ = shutdown.send(true);
    Ok(())
}

// ══════════════════════════════════════════════════════════════════
// Gate 4: API regression + conflict path
// ══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn gate4_existing_api_unaffected() -> Result<()> {
    let (_tmp, db_path, _id, _inst_dir) = setup_instance("api-reg", 19110, &config_with_secret());

    let (base_url, shutdown) = start_test_server(db_path).await;
    let client = reqwest::Client::new();

    // Health
    let health = client.get(format!("{base_url}/api/health")).send().await?;
    assert_eq!(health.status(), 200);
    let health_body: serde_json::Value = health.json().await?;
    assert_eq!(health_body["status"], "ok");

    // List instances
    let list = client
        .get(format!("{base_url}/api/instances"))
        .send()
        .await?;
    assert_eq!(list.status(), 200);
    let list_body: serde_json::Value = list.json().await?;
    assert!(list_body.as_array().unwrap().len() >= 1);

    // Config GET
    let config = client
        .get(format!("{base_url}/api/instances/api-reg/config"))
        .send()
        .await?;
    assert_eq!(config.status(), 200);
    let config_body: serde_json::Value = config.json().await?;
    assert_eq!(config_body["name"], "api-reg");
    assert!(config_body["etag"].is_string());

    let _ = shutdown.send(true);
    Ok(())
}

#[tokio::test]
async fn gate4_config_conflict_returns_409_json() -> Result<()> {
    let (_tmp, db_path, _id, _inst_dir) =
        setup_instance("conflict-test", 19111, &config_with_secret());

    let (base_url, shutdown) = start_test_server(db_path).await;
    let client = reqwest::Client::new();

    // PUT with wrong ETag
    let resp = client
        .put(format!("{base_url}/api/instances/conflict-test/config"))
        .json(&serde_json::json!({
            "config": "default_temperature = 0.9\n",
            "etag": "0000000000000000000000000000000000000000000000000000000000000000",
        }))
        .send()
        .await?;
    assert_eq!(resp.status(), 409);

    let ct = resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()?
        .to_string();
    assert!(
        ct.contains("application/json"),
        "409 should be JSON, got: {ct}"
    );

    let body: serde_json::Value = resp.json().await?;
    assert!(
        body["error"].as_str().unwrap().contains("ETag mismatch"),
        "Should mention ETag mismatch"
    );
    assert!(
        body["current_etag"].is_string(),
        "Should include current_etag"
    );

    let _ = shutdown.send(true);
    Ok(())
}
