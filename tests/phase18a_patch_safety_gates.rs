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

/// Config with real secrets for testing PATCH behavior.
fn config_with_secrets() -> String {
    r#"api_key = "sk-real-api-key-12345678"
default_temperature = 0.7

[heartbeat]
enabled = false
interval_minutes = 30

[gateway]
port = 3000
host = "127.0.0.1"
require_pairing = true
paired_tokens = ["real-paired-token-1", "real-paired-token-2"]

[channels_config]
cli = true

[channels_config.telegram]
bot_token = "8256947227:AAEqtest_secret_token"
allowed_users = ["alice", "bob"]
flows_enabled = false

[channels_config.telegram.flow_policy]
agent_authoring_enabled = false
max_steps = 10
max_agent_flows = 50
require_handoff_on_keyboard = true
auto_approve = false
auto_approve_max_steps = 5

[[model_routes]]
hint = "fast"
provider = "groq"
model = "llama-3"
api_key = "sk-groq-secret-key-abcd"

[[model_routes]]
hint = "reason"
provider = "openai"
model = "gpt-4o"
"#
    .to_string()
}

/// Get current ETag for an instance config.
async fn get_etag(base_url: &str, name: &str) -> String {
    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{base_url}/api/instances/{name}/config"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    body["etag"].as_str().unwrap().to_string()
}

// ── Gate 1: Non-object root rejected ────────────────────────────

#[tokio::test]
async fn patch_non_object_root_rejected() {
    let (_tmp, db_path, _id, _dir) = setup_instance("gate1", 19001, &config_with_secrets());
    let (base_url, _shutdown) = start_test_server(db_path).await;
    let client = reqwest::Client::new();
    let etag = get_etag(&base_url, "gate1").await;

    // String root
    let resp = client
        .patch(format!("{base_url}/api/instances/gate1/config"))
        .json(&serde_json::json!({
            "patch": "not an object",
            "etag": etag,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(body["error"].as_str().unwrap().contains("object"));

    // Array root
    let resp = client
        .patch(format!("{base_url}/api/instances/gate1/config"))
        .json(&serde_json::json!({
            "patch": [1, 2, 3],
            "etag": etag,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
}

// ── Gate 2: Unknown path rejected ───────────────────────────────

#[tokio::test]
async fn patch_unknown_path_rejected() {
    let (_tmp, db_path, _id, _dir) = setup_instance("gate2", 19002, &config_with_secrets());
    let (base_url, _shutdown) = start_test_server(db_path).await;
    let client = reqwest::Client::new();
    let etag = get_etag(&base_url, "gate2").await;

    let resp = client
        .patch(format!("{base_url}/api/instances/gate2/config"))
        .json(&serde_json::json!({
            "patch": { "nonexistent_field": 42 },
            "etag": etag,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    let body: serde_json::Value = resp.json().await.unwrap();
    let invalid = body["invalid_paths"].as_array().unwrap();
    assert!(invalid.iter().any(|p| p.as_str() == Some("nonexistent_field")));
}

// ── Gate 3: Masked sentinel rejected ────────────────────────────

#[tokio::test]
async fn patch_masked_sentinel_rejected() {
    let (_tmp, db_path, _id, _dir) = setup_instance("gate3", 19003, &config_with_secrets());
    let (base_url, _shutdown) = start_test_server(db_path).await;
    let client = reqwest::Client::new();
    let etag = get_etag(&base_url, "gate3").await;

    // Even with secret-write header, MASKED is always rejected
    let resp = client
        .patch(format!("{base_url}/api/instances/gate3/config"))
        .header("X-Allow-Secret-Write", "true")
        .json(&serde_json::json!({
            "patch": { "api_key": "***MASKED***" },
            "etag": etag,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(body["error"].as_str().unwrap().contains("MASKED"));
}

// ── Gate 4: Null on required rejected ───────────────────────────

#[tokio::test]
async fn patch_null_on_required_rejected() {
    let (_tmp, db_path, _id, _dir) = setup_instance("gate4", 19004, &config_with_secrets());
    let (base_url, _shutdown) = start_test_server(db_path).await;
    let client = reqwest::Client::new();
    let etag = get_etag(&base_url, "gate4").await;

    // default_temperature is f64, not Option -- null should be rejected
    let resp = client
        .patch(format!("{base_url}/api/instances/gate4/config"))
        .json(&serde_json::json!({
            "patch": { "default_temperature": null },
            "etag": etag,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(body["invalid_null_paths"]
        .as_array()
        .unwrap()
        .iter()
        .any(|p| p.as_str() == Some("default_temperature")));

    // email.password is String, not Option -- null should also be rejected
    let resp = client
        .patch(format!("{base_url}/api/instances/gate4/config"))
        .json(&serde_json::json!({
            "patch": {
                "channels_config": {
                    "email": {
                        "password": null
                    }
                }
            },
            "etag": etag,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
}

// ── Gate 5: Nonsecret succeeds with existing secrets ────────────

#[tokio::test]
async fn patch_nonsecret_succeeds_with_existing_secrets() {
    let (_tmp, db_path, _id, dir) = setup_instance("gate5", 19005, &config_with_secrets());
    let (base_url, _shutdown) = start_test_server(db_path).await;
    let client = reqwest::Client::new();
    let etag = get_etag(&base_url, "gate5").await;

    // PATCH only changes default_temperature -- no secret-write header needed
    let resp = client
        .patch(format!("{base_url}/api/instances/gate5/config"))
        .json(&serde_json::json!({
            "patch": { "default_temperature": 0.9 },
            "etag": etag,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Verify api_key is still intact on disk
    let config_path = dir.join("config.toml");
    let on_disk = fs::read_to_string(&config_path).unwrap();
    assert!(on_disk.contains("sk-real-api-key-12345678"));
    assert!(on_disk.contains("0.9"));
}

// ── Gate 6: Array secret requires header ────────────────────────

#[tokio::test]
async fn patch_array_secret_requires_header() {
    let (_tmp, db_path, _id, _dir) = setup_instance("gate6", 19006, &config_with_secrets());
    let (base_url, _shutdown) = start_test_server(db_path).await;
    let client = reqwest::Client::new();

    // (a) model_routes with api_key -- without header -> 400
    let etag = get_etag(&base_url, "gate6").await;
    let resp = client
        .patch(format!("{base_url}/api/instances/gate6/config"))
        .json(&serde_json::json!({
            "patch": {
                "model_routes": [
                    { "hint": "fast", "provider": "groq", "model": "llama", "api_key": "sk-new" }
                ]
            },
            "etag": etag,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(body["blocked_fields"]
        .as_array()
        .unwrap()
        .iter()
        .any(|p| p.as_str().unwrap().contains("model_routes")));

    // With header -> succeeds
    let etag = get_etag(&base_url, "gate6").await;
    let resp = client
        .patch(format!("{base_url}/api/instances/gate6/config"))
        .header("X-Allow-Secret-Write", "true")
        .json(&serde_json::json!({
            "patch": {
                "model_routes": [
                    { "hint": "fast", "provider": "groq", "model": "llama", "api_key": "sk-new" }
                ]
            },
            "etag": etag,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // (b) paired_tokens without header -> 400
    let etag = get_etag(&base_url, "gate6").await;
    let resp = client
        .patch(format!("{base_url}/api/instances/gate6/config"))
        .json(&serde_json::json!({
            "patch": {
                "gateway": { "paired_tokens": ["new-tok"] }
            },
            "etag": etag,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);

    // With header -> succeeds
    let etag = get_etag(&base_url, "gate6").await;
    let resp = client
        .patch(format!("{base_url}/api/instances/gate6/config"))
        .header("X-Allow-Secret-Write", "true")
        .json(&serde_json::json!({
            "patch": {
                "gateway": { "paired_tokens": ["new-tok"] }
            },
            "etag": etag,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // (c) Empty paired_tokens (clear-all) without header -> 400
    let etag = get_etag(&base_url, "gate6").await;
    let resp = client
        .patch(format!("{base_url}/api/instances/gate6/config"))
        .json(&serde_json::json!({
            "patch": {
                "gateway": { "paired_tokens": [] }
            },
            "etag": etag,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);

    // With header -> succeeds
    let etag = get_etag(&base_url, "gate6").await;
    let resp = client
        .patch(format!("{base_url}/api/instances/gate6/config"))
        .header("X-Allow-Secret-Write", "true")
        .json(&serde_json::json!({
            "patch": {
                "gateway": { "paired_tokens": [] }
            },
            "etag": etag,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

// ── Gate 7: Scalar preserves untouched sections ─────────────────

#[tokio::test]
async fn patch_scalar_preserves_untouched_sections() {
    // Config with a comment and heartbeat section
    let config_toml = r#"api_key = "sk-real-api-key-12345678"
default_temperature = 0.7

# Heartbeat configuration
[heartbeat]
enabled = false
interval_minutes = 30
"#;

    let (_tmp, db_path, _id, dir) = setup_instance("gate7", 19007, config_toml);
    let (base_url, _shutdown) = start_test_server(db_path).await;
    let client = reqwest::Client::new();
    let etag = get_etag(&base_url, "gate7").await;

    // PATCH only changes default_temperature
    let resp = client
        .patch(format!("{base_url}/api/instances/gate7/config"))
        .json(&serde_json::json!({
            "patch": { "default_temperature": 0.9 },
            "etag": etag,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Read back and verify preservation
    let config_path = dir.join("config.toml");
    let on_disk = fs::read_to_string(&config_path).unwrap();

    // Heartbeat block should be byte-identical
    assert!(on_disk.contains("[heartbeat]"));
    assert!(on_disk.contains("enabled = false"));
    assert!(on_disk.contains("interval_minutes = 30"));
    // Comment should be preserved
    assert!(on_disk.contains("# Heartbeat configuration"));
    // Only the temperature line differs
    assert!(on_disk.contains("0.9"));
    assert!(!on_disk.contains("0.7"));
}

// ── Gate 8: Nested optional path validated ──────────────────────

#[tokio::test]
async fn patch_nested_optional_path_validated() {
    let (_tmp, db_path, _id, _dir) = setup_instance("gate8", 19008, &config_with_secrets());
    let (base_url, _shutdown) = start_test_server(db_path).await;
    let client = reqwest::Client::new();

    // (a) Valid nested path succeeds
    let etag = get_etag(&base_url, "gate8").await;
    let resp = client
        .patch(format!("{base_url}/api/instances/gate8/config"))
        .json(&serde_json::json!({
            "patch": {
                "channels_config": { "telegram": { "flows_enabled": true } }
            },
            "etag": etag,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // (b) Invalid nested path -> 400
    let etag = get_etag(&base_url, "gate8").await;
    let resp = client
        .patch(format!("{base_url}/api/instances/gate8/config"))
        .json(&serde_json::json!({
            "patch": {
                "channels_config": { "telegram": { "nonexistent": 42 } }
            },
            "etag": etag,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);

    // (c) Nullable Option<String> can be set and nulled
    let etag = get_etag(&base_url, "gate8").await;
    let resp = client
        .patch(format!("{base_url}/api/instances/gate8/config"))
        .json(&serde_json::json!({
            "patch": { "browser": { "session_name": "test" } },
            "etag": etag,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let etag = get_etag(&base_url, "gate8").await;
    let resp = client
        .patch(format!("{base_url}/api/instances/gate8/config"))
        .json(&serde_json::json!({
            "patch": { "browser": { "session_name": null } },
            "etag": etag,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

// ── Bonus: GET includes fingerprints and secret_paths ───────────

#[tokio::test]
async fn get_config_includes_fingerprints_and_secret_paths() {
    let (_tmp, db_path, _id, _dir) = setup_instance("getext", 19009, &config_with_secrets());
    let (base_url, _shutdown) = start_test_server(db_path).await;
    let client = reqwest::Client::new();

    let resp = client
        .get(format!("{base_url}/api/instances/getext/config"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();

    // secret_fingerprints should be present
    let fps = &body["secret_fingerprints"];
    assert!(fps.is_object());
    // api_key fingerprint should show last 4
    assert!(fps["api_key"].as_str().unwrap().starts_with("..."));
    // telegram bot_token should show bot ID
    assert!(fps["channels_config.telegram.bot_token"]
        .as_str()
        .unwrap()
        .starts_with("bot "));
    // paired_tokens should show count
    assert!(fps["gateway.paired_tokens"]
        .as_str()
        .unwrap()
        .contains("token"));

    // secret_paths should be present and contain expected entries
    let paths = body["secret_paths"].as_array().unwrap();
    let path_strs: Vec<&str> = paths.iter().filter_map(|p| p.as_str()).collect();
    assert!(path_strs.contains(&"api_key"));
    assert!(path_strs.contains(&"channels_config.telegram.bot_token"));
    assert!(path_strs.contains(&"gateway.paired_tokens"));
    assert!(path_strs.contains(&"model_routes[*].api_key"));
}
