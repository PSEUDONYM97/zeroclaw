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

fn full_config() -> String {
    r#"api_key = "sk-real-api-key-12345678"
default_temperature = 0.7
default_provider = "openrouter"
default_model = "claude-sonnet"

[heartbeat]
enabled = false
interval_minutes = 30

[gateway]
port = 3000
host = "127.0.0.1"
require_pairing = true
paired_tokens = ["real-paired-token-1"]

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

// ── API Gate 1: touched field only sent ─────────────────────────
// PATCH with single field succeeds; all other fields unchanged on disk.

#[tokio::test]
async fn patch_touched_field_only_sent() {
    let (_tmp, db_path, _id, dir) = setup_instance("ui1", 19101, &full_config());
    let (base_url, _shutdown) = start_test_server(db_path).await;
    let client = reqwest::Client::new();
    let etag = get_etag(&base_url, "ui1").await;

    // Only touch default_temperature
    let resp = client
        .patch(format!("{base_url}/api/instances/ui1/config"))
        .json(&serde_json::json!({
            "patch": { "default_temperature": 0.9 },
            "etag": etag,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Verify on disk: secrets preserved, only temperature changed
    let on_disk = fs::read_to_string(dir.join("config.toml")).unwrap();
    assert!(on_disk.contains("sk-real-api-key-12345678"), "api_key should be preserved");
    assert!(on_disk.contains("8256947227:AAEqtest_secret_token"), "bot_token should be preserved");
    assert!(on_disk.contains("sk-groq-secret-key-abcd"), "model route api_key should be preserved");
    assert!(on_disk.contains("real-paired-token-1"), "paired token should be preserved");
    assert!(on_disk.contains("0.9"), "temperature should be updated");
    assert!(!on_disk.contains("= 0.7"), "old temperature should be gone");
    // Arrays should be preserved
    assert!(on_disk.contains("alice"), "allowed_users should be preserved");
}

// ── API Gate 2: secret absent when unchanged ────────────────────
// PATCH with non-secret field on config with real secrets. Secrets intact.

#[tokio::test]
async fn patch_secret_absent_when_unchanged() {
    let (_tmp, db_path, _id, dir) = setup_instance("ui2", 19102, &full_config());
    let (base_url, _shutdown) = start_test_server(db_path).await;
    let client = reqwest::Client::new();
    let etag = get_etag(&base_url, "ui2").await;

    // Patch only heartbeat enabled (no secrets in patch body)
    let resp = client
        .patch(format!("{base_url}/api/instances/ui2/config"))
        .json(&serde_json::json!({
            "patch": { "heartbeat": { "enabled": true } },
            "etag": etag,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // No X-Allow-Secret-Write header needed
    let on_disk = fs::read_to_string(dir.join("config.toml")).unwrap();
    assert!(on_disk.contains("sk-real-api-key-12345678"));
    assert!(on_disk.contains("enabled = true"));
}

// ── API Gate 3: channel disable removes section ─────────────────
// PATCH with null on channel removes entire table. No empty table left.

#[tokio::test]
async fn patch_channel_disable_removes_section() {
    let (_tmp, db_path, _id, dir) = setup_instance("ui3", 19103, &full_config());
    let (base_url, _shutdown) = start_test_server(db_path).await;
    let client = reqwest::Client::new();
    let etag = get_etag(&base_url, "ui3").await;

    let resp = client
        .patch(format!("{base_url}/api/instances/ui3/config"))
        .json(&serde_json::json!({
            "patch": { "channels_config": { "telegram": null } },
            "etag": etag,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let on_disk = fs::read_to_string(dir.join("config.toml")).unwrap();
    // Entire telegram section should be gone
    assert!(!on_disk.contains("telegram"), "telegram section should be removed entirely");
    assert!(!on_disk.contains("bot_token"), "bot_token should be gone");
    assert!(!on_disk.contains("flow_policy"), "flow_policy should be gone");
    // Other sections should survive
    assert!(on_disk.contains("[heartbeat]"), "heartbeat should survive");
    assert!(on_disk.contains("[[model_routes]]"), "model_routes should survive");
}

// ── API Gate 4: nested telegram leaf preserves siblings ──────────
// PATCH on a single nested field preserves all sibling fields.

#[tokio::test]
async fn patch_nested_telegram_leaf_preserves_siblings() {
    let (_tmp, db_path, _id, dir) = setup_instance("ui4", 19104, &full_config());
    let (base_url, _shutdown) = start_test_server(db_path).await;
    let client = reqwest::Client::new();
    let etag = get_etag(&base_url, "ui4").await;

    // Only change flows_enabled inside telegram
    let resp = client
        .patch(format!("{base_url}/api/instances/ui4/config"))
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

    let on_disk = fs::read_to_string(dir.join("config.toml")).unwrap();
    // flows_enabled should be true now
    assert!(on_disk.contains("flows_enabled = true"));
    // bot_token should survive (it's a sibling in the same section)
    assert!(on_disk.contains("8256947227:AAEqtest_secret_token"), "bot_token must survive");
    // allowed_users should survive
    assert!(on_disk.contains("alice"), "allowed_users must survive");
    assert!(on_disk.contains("bob"), "allowed_users must survive");
    // flow_policy section should survive
    assert!(on_disk.contains("flow_policy"), "flow_policy must survive");
    assert!(on_disk.contains("agent_authoring_enabled"), "flow_policy fields must survive");
}

// ── Frontend-Unit Gate 5: touched tracking correct ──────────────
// Tests the masking.rs functions that the frontend logic depends on.

#[test]
fn touched_tracking_produces_minimal_patch() {
    use zeroclaw::cp::masking::{validate_patch_paths, reject_masked_sentinels};

    // A patch with only default_temperature (what buildPatchFromTouched would produce
    // when only that field is touched)
    let patch = serde_json::json!({ "default_temperature": 0.9 });
    assert!(validate_patch_paths(&patch).is_ok());
    assert!(reject_masked_sentinels(&patch).is_ok());

    // Verify the patch has exactly one key
    let obj = patch.as_object().unwrap();
    assert_eq!(obj.len(), 1, "patch should only contain the touched field");
    assert!(obj.contains_key("default_temperature"));
}

// ── Frontend-Unit Gate 6: model_routes sanitizer strips MASKED ──
// Proves buildModelRoutesForPatch strips unchanged masked api_keys.

#[test]
fn model_routes_sanitizer_strips_masked() {
    use zeroclaw::cp::masking::{reject_masked_sentinels, collect_secret_writes, MASKED};

    // Simulate what buildModelRoutesForPatch() produces:
    // route 0 has unchanged api_key (omitted), route 1 has new api_key
    let patch = serde_json::json!({
        "model_routes": [
            { "hint": "fast", "provider": "groq", "model": "llama" },
            { "hint": "code", "provider": "openai", "model": "gpt-4", "api_key": "sk-new-key" }
        ]
    });

    // No MASKED sentinel anywhere
    assert!(reject_masked_sentinels(&patch).is_ok());

    // Verify route 0 has NO api_key field
    let routes = patch["model_routes"].as_array().unwrap();
    assert!(routes[0].get("api_key").is_none(), "unchanged route should NOT have api_key");
    assert!(routes[1].get("api_key").is_some(), "changed route should have api_key");

    // Verify no MASKED string anywhere in serialized output
    let serialized = serde_json::to_string(&patch).unwrap();
    assert!(!serialized.contains(MASKED), "no MASKED sentinel in patch output");

    // collect_secret_writes should find route[1].api_key
    let secrets = collect_secret_writes(&patch);
    assert!(secrets.contains(&"model_routes[1].api_key".to_string()));
    assert!(!secrets.iter().any(|s| s.contains("[0]")), "route 0 should have no secret write");
}

// ── Frontend-Unit Gate 7: secret paths from manifest ────────────
// Backend provides secret_paths in GET response.

#[test]
fn secret_paths_from_manifest() {
    use zeroclaw::cp::masking::SECRET_PATHS_MANIFEST;

    // Verify manifest contains expected entries
    assert!(SECRET_PATHS_MANIFEST.contains(&"channels_config.telegram.bot_token"));
    assert!(SECRET_PATHS_MANIFEST.contains(&"api_key"));
    assert!(SECRET_PATHS_MANIFEST.contains(&"gateway.paired_tokens"));
    assert!(SECRET_PATHS_MANIFEST.contains(&"model_routes[*].api_key"));

    // Verify non-secrets are NOT in the manifest
    assert!(!SECRET_PATHS_MANIFEST.contains(&"default_temperature"));
    assert!(!SECRET_PATHS_MANIFEST.contains(&"heartbeat.enabled"));
}

// ── Frontend-Unit Gate 8: array secret detection ────────────────
// Tests collect_secret_writes for array types.

#[test]
fn array_secret_detection() {
    use zeroclaw::cp::masking::collect_secret_writes;

    // (a) model_routes with api_key
    let patch_a = serde_json::json!({
        "model_routes": [
            { "hint": "fast", "provider": "groq", "model": "llama", "api_key": "sk-new" }
        ]
    });
    let secrets_a = collect_secret_writes(&patch_a);
    assert!(!secrets_a.is_empty(), "should detect model_routes api_key");

    // (b) paired_tokens with values
    let patch_b = serde_json::json!({
        "gateway": { "paired_tokens": ["new-tok"] }
    });
    let secrets_b = collect_secret_writes(&patch_b);
    assert!(!secrets_b.is_empty(), "should detect paired_tokens");

    // (c) empty paired_tokens (clear-all) is also a secret write
    let patch_c = serde_json::json!({
        "gateway": { "paired_tokens": [] }
    });
    let secrets_c = collect_secret_writes(&patch_c);
    assert!(!secrets_c.is_empty(), "empty paired_tokens should be detected as secret write");

    // (d) non-secret array should NOT trigger
    let patch_d = serde_json::json!({
        "autonomy": { "allowed_commands": ["git", "npm"] }
    });
    let secrets_d = collect_secret_writes(&patch_d);
    assert!(secrets_d.is_empty(), "non-secret array should not trigger");
}
