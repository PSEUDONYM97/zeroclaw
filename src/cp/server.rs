use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio_util::io::ReaderStream;

use crate::cp::masking::{
    collect_key_paths, diff_json, mask_config_secrets, preserve_masked_secrets,
};
use crate::cp::messaging;
use crate::db::Registry;
use crate::lifecycle;
use crate::lifecycle::LifecycleError;

/// Maximum bytes to read from the tail of a log file.
/// Bounds memory usage regardless of total file size.
const MAX_TAIL_BYTES: u64 = 4 * 1024 * 1024; // 4 MiB

/// Embedded SPA HTML served at `/` and as a fallback for non-API paths.
const INDEX_HTML: &str = include_str!("../../static/index.html");

/// Read the last `n` lines from a file without loading the entire file.
/// Reads at most `MAX_TAIL_BYTES` from the end of the file.
fn read_last_n_lines(path: &Path, n: usize) -> std::io::Result<Vec<String>> {
    let mut file = std::fs::File::open(path)?;
    let file_len = file.metadata()?.len();

    let read_from = file_len.saturating_sub(MAX_TAIL_BYTES);
    file.seek(SeekFrom::Start(read_from))?;

    let to_read = (file_len - read_from) as usize;
    let mut buf = vec![0u8; to_read];
    file.read_exact(&mut buf)?;

    let text = String::from_utf8_lossy(&buf);
    let all_lines: Vec<&str> = text.lines().collect();

    // If we seeked past the start, the first "line" may be partial -- skip it
    let skip = if read_from > 0 && !all_lines.is_empty() {
        1
    } else {
        0
    };

    let usable = &all_lines[skip..];
    let start = usable.len().saturating_sub(n);
    Ok(usable[start..].iter().map(|s| s.to_string()).collect())
}

/// Read a tail window of `MAX_TAIL_BYTES` from a file and paginate within it.
/// Returns (lines, window_lines, has_more, truncated).
fn read_lines_paginated(
    path: &Path,
    offset: usize,
    count: usize,
) -> std::io::Result<(Vec<String>, usize, bool, bool)> {
    let mut file = std::fs::File::open(path)?;
    let file_len = file.metadata()?.len();

    let read_from = file_len.saturating_sub(MAX_TAIL_BYTES);
    let truncated = read_from > 0;
    file.seek(SeekFrom::Start(read_from))?;

    let to_read = (file_len - read_from) as usize;
    let mut buf = vec![0u8; to_read];
    file.read_exact(&mut buf)?;

    let text = String::from_utf8_lossy(&buf);
    let all_lines: Vec<&str> = text.lines().collect();

    let skip = if truncated && !all_lines.is_empty() {
        1
    } else {
        0
    };
    let usable = &all_lines[skip..];
    let window_lines = usable.len();

    let start = offset.min(window_lines);
    let end = (start + count).min(window_lines);
    let lines: Vec<String> = usable[start..end].iter().map(|s| s.to_string()).collect();
    let has_more = end < window_lines;

    Ok((lines, window_lines, has_more, truncated))
}

/// Shared state: just the DB path. Each request opens its own connection.
#[derive(Clone)]
pub struct CpState {
    pub db_path: Arc<PathBuf>,
}

/// Serve the embedded SPA HTML.
async fn handle_ui() -> Response<Body> {
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/html; charset=utf-8")
        .body(Body::from(INDEX_HTML))
        .unwrap()
}

/// JSON 404 for unknown API paths. Keeps API errors as JSON, never HTML.
async fn handle_api_fallback() -> ApiResponse {
    err_json(StatusCode::NOT_FOUND, "Unknown API endpoint")
}

/// Build the axum router with all CP API routes and embedded UI.
pub fn build_router(state: CpState) -> Router {
    let api_router = Router::new()
        .route("/health", get(handle_health))
        .route("/instances", get(handle_list_instances).post(handle_create_instance))
        .route("/instances/:name", get(handle_get_instance).delete(handle_delete_instance))
        .route("/instances/:name/archive", post(handle_archive))
        .route("/instances/:name/unarchive", post(handle_unarchive))
        .route("/instances/:name/clone", post(handle_clone_instance))
        .route("/instances/:name/start", post(handle_start))
        .route("/instances/:name/stop", post(handle_stop))
        .route("/instances/:name/restart", post(handle_restart))
        .route("/instances/:name/logs", get(handle_logs))
        .route("/instances/:name/details", get(handle_details))
        .route("/instances/:name/tasks", get(handle_tasks))
        .route("/instances/:name/usage", get(handle_usage))
        .route("/instances/:name/logs/download", get(handle_logs_download))
        .route(
            "/instances/:name/config",
            get(handle_config_get).put(handle_config_put),
        )
        .route(
            "/instances/:name/config/validate",
            post(handle_config_validate),
        )
        .route("/instances/:name/config/diff", post(handle_config_diff))
        .route(
            "/routing-rules",
            get(messaging::handle_list_rules).post(messaging::handle_create_rule),
        )
        .route("/routing-rules/:id", delete(messaging::handle_delete_rule))
        .route("/messages", post(messaging::handle_send_message))
        .route(
            "/instances/:name/messages/pending",
            get(messaging::handle_receive_message),
        )
        .route(
            "/messages/:id/acknowledge",
            post(messaging::handle_acknowledge_message),
        )
        .fallback(handle_api_fallback);

    Router::new()
        .route("/", get(handle_ui))
        .nest("/api", api_router)
        .fallback(handle_ui)
        .with_state(state)
}

// ── Response helpers ─────────────────────────────────────────────

type ApiResponse = (StatusCode, Json<serde_json::Value>);

fn ok_json(value: serde_json::Value) -> ApiResponse {
    (StatusCode::OK, Json(value))
}

fn err_json(status: StatusCode, message: &str) -> ApiResponse {
    (status, Json(serde_json::json!({ "error": message })))
}

fn lifecycle_err_to_response(e: LifecycleError) -> ApiResponse {
    match &e {
        LifecycleError::NotFound(_) => err_json(StatusCode::NOT_FOUND, &e.to_string()),
        LifecycleError::AlreadyRunning(_) => err_json(StatusCode::CONFLICT, &e.to_string()),
        LifecycleError::NotRunning(_) => err_json(StatusCode::CONFLICT, &e.to_string()),
        LifecycleError::LockHeld => err_json(StatusCode::SERVICE_UNAVAILABLE, &e.to_string()),
        LifecycleError::Internal(_) => err_json(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    }
}

fn open_registry(db_path: &Path) -> Result<Registry, ApiResponse> {
    Registry::open(db_path).map_err(|e| {
        tracing::error!("Failed to open registry: {e:#}");
        err_json(StatusCode::INTERNAL_SERVER_ERROR, "Failed to open registry")
    })
}

// ── Phase 13.1: CRUD request bodies ─────────────────────────────

#[derive(Deserialize)]
struct CreateInstanceBody {
    name: String,
    port: Option<u16>,
    model_provider: Option<String>,
    model_name: Option<String>,
}

#[derive(Deserialize)]
struct CloneInstanceBody {
    new_name: String,
    port: Option<u16>,
}

// ── Phase 13.1: helpers ─────────────────────────────────────────

/// Validate instance name: alphanumeric + hyphens, 1-64 chars, starts with alphanum.
fn validate_instance_name(name: &str) -> Result<(), String> {
    if name.is_empty() || name.len() > 64 {
        return Err("Name must be 1-64 characters".into());
    }
    if !name.chars().next().unwrap().is_ascii_alphanumeric() {
        return Err("Name must start with a letter or digit".into());
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-')
    {
        return Err("Name may only contain letters, digits, and hyphens".into());
    }
    Ok(())
}

/// Derive instances_dir from db_path (sibling directory).
fn instances_dir_from_db(db_path: &Path) -> PathBuf {
    db_path
        .parent()
        .unwrap_or(Path::new("."))
        .join("instances")
}

/// Atomic config write with fsync (replicates openclaw.rs:773-805 pattern).
fn write_config_atomic(path: &Path, content: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    let dir = path
        .parent()
        .ok_or_else(|| std::io::Error::other("no parent dir"))?;
    let temp = dir.join(format!(".tmp-{}", uuid::Uuid::new_v4()));
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        f.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    }
    f.write_all(content)?;
    f.sync_all()?;
    std::fs::rename(&temp, path)?;
    std::fs::File::open(dir)?.sync_all()?;
    Ok(())
}

/// Build a Config struct with sane defaults, applying overrides.
fn build_default_config(
    port: u16,
    provider: Option<&str>,
    model: Option<&str>,
) -> crate::config::Config {
    let mut config = crate::config::Config::default();
    config.gateway.port = port;
    config.gateway.host = "127.0.0.1".to_string();
    if let Some(p) = provider {
        config.default_provider = Some(p.to_string());
    }
    if let Some(m) = model {
        config.default_model = Some(m.to_string());
    }
    config
}

// ── Instance serialization ───────────────────────────────────────

fn instance_to_json(
    inst: &crate::db::Instance,
    live_status: &str,
    live_pid: Option<u32>,
) -> serde_json::Value {
    serde_json::json!({
        "id": inst.id,
        "name": inst.name,
        "port": inst.port,
        "status": live_status,
        "pid": live_pid,
        "config_path": inst.config_path,
        "workspace_dir": inst.workspace_dir,
    })
}

// ── Handlers ─────────────────────────────────────────────────────

async fn handle_health(State(state): State<CpState>) -> impl IntoResponse {
    let db_path = state.db_path.clone();
    let result = tokio::task::spawn_blocking(move || -> Result<serde_json::Value, String> {
        let registry = Registry::open(&db_path).map_err(|e| format!("{e:#}"))?;
        let instances = registry.list_instances().map_err(|e| format!("{e:#}"))?;

        let mut instance_map = serde_json::Map::new();
        for inst in &instances {
            let inst_dir = lifecycle::instance_dir_from(inst);
            let (status, pid) =
                lifecycle::live_status(&inst_dir).unwrap_or(("unknown".to_string(), None));
            instance_map.insert(
                inst.name.clone(),
                serde_json::json!({ "status": status, "pid": pid }),
            );
        }

        Ok(serde_json::json!({
            "status": "ok",
            "instances": instance_map,
        }))
    })
    .await;

    match result {
        Ok(Ok(value)) => ok_json(value),
        Ok(Err(msg)) => err_json(StatusCode::INTERNAL_SERVER_ERROR, &msg),
        Err(e) => err_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("Task join error: {e}"),
        ),
    }
}

async fn handle_list_instances(State(state): State<CpState>) -> impl IntoResponse {
    let db_path = state.db_path.clone();
    let result = tokio::task::spawn_blocking(move || -> Result<serde_json::Value, String> {
        let registry = Registry::open(&db_path).map_err(|e| format!("{e:#}"))?;
        let instances = registry.list_instances().map_err(|e| format!("{e:#}"))?;

        let mut list = Vec::new();
        for inst in &instances {
            let inst_dir = lifecycle::instance_dir_from(inst);
            let (status, pid) =
                lifecycle::live_status(&inst_dir).unwrap_or(("unknown".to_string(), None));
            list.push(instance_to_json(inst, &status, pid));
        }

        Ok(serde_json::json!(list))
    })
    .await;

    match result {
        Ok(Ok(value)) => ok_json(value),
        Ok(Err(msg)) => err_json(StatusCode::INTERNAL_SERVER_ERROR, &msg),
        Err(e) => err_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("Task join error: {e}"),
        ),
    }
}

async fn handle_get_instance(
    State(state): State<CpState>,
    AxumPath(name): AxumPath<String>,
) -> impl IntoResponse {
    let db_path = state.db_path.clone();
    let result = tokio::task::spawn_blocking(move || -> Result<ApiResponse, String> {
        let registry = Registry::open(&db_path).map_err(|e| format!("{e:#}"))?;
        match registry.get_instance_by_name(&name) {
            Ok(Some(inst)) => {
                let inst_dir = lifecycle::instance_dir_from(&inst);
                let (status, pid) =
                    lifecycle::live_status(&inst_dir).unwrap_or(("unknown".to_string(), None));
                Ok(ok_json(instance_to_json(&inst, &status, pid)))
            }
            Ok(None) => Ok(err_json(
                StatusCode::NOT_FOUND,
                &format!("No instance named '{name}'"),
            )),
            Err(e) => Err(format!("{e:#}")),
        }
    })
    .await;

    match result {
        Ok(Ok(resp)) => resp,
        Ok(Err(msg)) => err_json(StatusCode::INTERNAL_SERVER_ERROR, &msg),
        Err(e) => err_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("Task join error: {e}"),
        ),
    }
}

async fn handle_start(
    State(state): State<CpState>,
    AxumPath(name): AxumPath<String>,
) -> impl IntoResponse {
    let db_path = state.db_path.clone();
    let result = tokio::task::spawn_blocking(move || -> ApiResponse {
        let registry = match open_registry(&db_path) {
            Ok(r) => r,
            Err(resp) => return resp,
        };
        match lifecycle::start_instance(&registry, &name) {
            Ok(()) => ok_json(serde_json::json!({ "status": "started", "name": name })),
            Err(e) => lifecycle_err_to_response(e),
        }
    })
    .await;

    match result {
        Ok(resp) => resp,
        Err(e) => err_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("Task join error: {e}"),
        ),
    }
}

async fn handle_stop(
    State(state): State<CpState>,
    AxumPath(name): AxumPath<String>,
) -> impl IntoResponse {
    let db_path = state.db_path.clone();
    let result = tokio::task::spawn_blocking(move || -> ApiResponse {
        let registry = match open_registry(&db_path) {
            Ok(r) => r,
            Err(resp) => return resp,
        };
        match lifecycle::stop_instance(&registry, &name) {
            Ok(()) => ok_json(serde_json::json!({ "status": "stopped", "name": name })),
            Err(e) => lifecycle_err_to_response(e),
        }
    })
    .await;

    match result {
        Ok(resp) => resp,
        Err(e) => err_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("Task join error: {e}"),
        ),
    }
}

async fn handle_restart(
    State(state): State<CpState>,
    AxumPath(name): AxumPath<String>,
) -> impl IntoResponse {
    let db_path = state.db_path.clone();
    let result = tokio::task::spawn_blocking(move || -> ApiResponse {
        let registry = match open_registry(&db_path) {
            Ok(r) => r,
            Err(resp) => return resp,
        };
        match lifecycle::restart_instance(&registry, &name) {
            Ok(()) => ok_json(serde_json::json!({ "status": "restarted", "name": name })),
            Err(e) => lifecycle_err_to_response(e),
        }
    })
    .await;

    match result {
        Ok(resp) => resp,
        Err(e) => err_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("Task join error: {e}"),
        ),
    }
}

// ── Phase 13.1: CRUD handlers ───────────────────────────────────

async fn handle_create_instance(
    State(state): State<CpState>,
    Json(body): Json<CreateInstanceBody>,
) -> impl IntoResponse {
    if let Err(msg) = validate_instance_name(&body.name) {
        return err_json(StatusCode::BAD_REQUEST, &msg);
    }

    let db_path = state.db_path.clone();
    let result = tokio::task::spawn_blocking(move || -> ApiResponse {
        let registry = match open_registry(&db_path) {
            Ok(r) => r,
            Err(resp) => return resp,
        };

        // Check for name conflict
        match registry.get_instance_by_name(&body.name) {
            Ok(Some(_)) => {
                return err_json(
                    StatusCode::CONFLICT,
                    &format!("Instance '{}' already exists", body.name),
                )
            }
            Ok(None) => {}
            Err(e) => {
                tracing::error!("Failed to query instance: {e:#}");
                return err_json(StatusCode::INTERNAL_SERVER_ERROR, "Failed to query instance");
            }
        }

        // Allocate port
        let port = if let Some(p) = body.port {
            p
        } else {
            match registry.allocate_port_with_excludes(18801, 18999, &[]) {
                Ok(Some(p)) => p,
                Ok(None) => {
                    return err_json(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "No ports available in range 18801-18999",
                    )
                }
                Err(e) => {
                    tracing::error!("Port allocation failed: {e:#}");
                    return err_json(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "Failed to allocate port",
                    );
                }
            }
        };

        let id = uuid::Uuid::new_v4().to_string();
        let instances_dir = instances_dir_from_db(&db_path);
        let inst_dir = instances_dir.join(&id);

        // Create instance directory + workspace subdirs
        let workspace_dir = inst_dir.join("workspace");
        for subdir in &["skills", "memory", "sessions", "state", "cron"] {
            if let Err(e) = std::fs::create_dir_all(workspace_dir.join(subdir)) {
                tracing::error!("Failed to create workspace dir: {e}");
                let _ = std::fs::remove_dir_all(&inst_dir);
                return err_json(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to create workspace directories",
                );
            }
        }

        // Build and write config
        let mut config = build_default_config(
            port,
            body.model_provider.as_deref(),
            body.model_name.as_deref(),
        );
        let config_path = inst_dir.join("config.toml");
        config.config_path = config_path.clone();
        config.workspace_dir = workspace_dir.clone();

        let toml_str = match toml::to_string_pretty(&config) {
            Ok(s) => s,
            Err(e) => {
                let _ = std::fs::remove_dir_all(&inst_dir);
                tracing::error!("Failed to serialize config: {e}");
                return err_json(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to serialize config",
                );
            }
        };

        if let Err(e) = write_config_atomic(&config_path, toml_str.as_bytes()) {
            let _ = std::fs::remove_dir_all(&inst_dir);
            tracing::error!("Failed to write config: {e}");
            return err_json(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to write config file",
            );
        }

        // Register in DB
        match registry.create_instance(
            &id,
            &body.name,
            port,
            config_path.to_str().unwrap_or(""),
            Some(workspace_dir.to_str().unwrap_or("")),
            None,
        ) {
            Ok(()) => {}
            Err(e) => {
                let _ = std::fs::remove_dir_all(&inst_dir);
                let msg = format!("{e:#}");
                if msg.contains("UNIQUE constraint failed") {
                    return err_json(
                        StatusCode::CONFLICT,
                        &format!("Port {} is already in use by another instance", port),
                    );
                }
                tracing::error!("Failed to register instance: {msg}");
                return err_json(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to register instance",
                );
            }
        }

        (
            StatusCode::CREATED,
            Json(serde_json::json!({
                "id": id,
                "name": body.name,
                "port": port,
                "status": "stopped",
            })),
        )
    })
    .await;

    match result {
        Ok(resp) => resp,
        Err(e) => err_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("Task join error: {e}"),
        ),
    }
}

async fn handle_archive(
    State(state): State<CpState>,
    AxumPath(name): AxumPath<String>,
) -> impl IntoResponse {
    let db_path = state.db_path.clone();
    let result = tokio::task::spawn_blocking(move || -> ApiResponse {
        let registry = match open_registry(&db_path) {
            Ok(r) => r,
            Err(resp) => return resp,
        };

        let instance = match registry.get_instance_by_name(&name) {
            Ok(Some(inst)) => inst,
            Ok(None) => {
                return err_json(
                    StatusCode::NOT_FOUND,
                    &format!("No active instance named '{name}'"),
                )
            }
            Err(e) => {
                tracing::error!("Failed to query instance: {e:#}");
                return err_json(StatusCode::INTERNAL_SERVER_ERROR, "Failed to query instance");
            }
        };

        // Check live truth and stop if running
        let inst_dir = lifecycle::instance_dir_from(&instance);
        let (live_status, _) =
            lifecycle::live_status(&inst_dir).unwrap_or(("unknown".to_string(), None));

        if live_status == "running" {
            if let Err(e) = lifecycle::stop_instance(&registry, &name) {
                return lifecycle_err_to_response(e);
            }
        }

        // Archive
        match registry.archive_instance(&instance.id) {
            Ok(true) => ok_json(serde_json::json!({ "status": "archived", "name": name })),
            Ok(false) => err_json(StatusCode::NOT_FOUND, &format!("Instance '{name}' not found")),
            Err(e) => {
                tracing::error!("Failed to archive instance: {e:#}");
                err_json(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to archive instance",
                )
            }
        }
    })
    .await;

    match result {
        Ok(resp) => resp,
        Err(e) => err_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("Task join error: {e}"),
        ),
    }
}

async fn handle_unarchive(
    State(state): State<CpState>,
    AxumPath(name): AxumPath<String>,
) -> impl IntoResponse {
    let db_path = state.db_path.clone();
    let result = tokio::task::spawn_blocking(move || -> ApiResponse {
        let registry = match open_registry(&db_path) {
            Ok(r) => r,
            Err(resp) => return resp,
        };

        // Check no active instance with this name
        match registry.get_instance_by_name(&name) {
            Ok(Some(_)) => {
                return err_json(
                    StatusCode::CONFLICT,
                    &format!("Active instance named '{name}' already exists"),
                )
            }
            Ok(None) => {}
            Err(e) => {
                tracing::error!("Failed to query instance: {e:#}");
                return err_json(StatusCode::INTERNAL_SERVER_ERROR, "Failed to query instance");
            }
        }

        match registry.unarchive_instance(&name) {
            Ok(true) => ok_json(serde_json::json!({ "status": "active", "name": name })),
            Ok(false) => err_json(
                StatusCode::NOT_FOUND,
                &format!("No archived instance named '{name}'"),
            ),
            Err(e) => {
                tracing::error!("Failed to unarchive instance: {e:#}");
                err_json(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to unarchive instance",
                )
            }
        }
    })
    .await;

    match result {
        Ok(resp) => resp,
        Err(e) => err_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("Task join error: {e}"),
        ),
    }
}

async fn handle_clone_instance(
    State(state): State<CpState>,
    AxumPath(name): AxumPath<String>,
    Json(body): Json<CloneInstanceBody>,
) -> impl IntoResponse {
    if let Err(msg) = validate_instance_name(&body.new_name) {
        return err_json(StatusCode::BAD_REQUEST, &msg);
    }

    let db_path = state.db_path.clone();
    let result = tokio::task::spawn_blocking(move || -> ApiResponse {
        let registry = match open_registry(&db_path) {
            Ok(r) => r,
            Err(resp) => return resp,
        };

        // Get source instance
        let source = match registry.get_instance_by_name(&name) {
            Ok(Some(inst)) => inst,
            Ok(None) => {
                return err_json(
                    StatusCode::NOT_FOUND,
                    &format!("No instance named '{name}'"),
                )
            }
            Err(e) => {
                tracing::error!("Failed to query instance: {e:#}");
                return err_json(StatusCode::INTERNAL_SERVER_ERROR, "Failed to query instance");
            }
        };

        // Check new name doesn't conflict
        match registry.get_instance_by_name(&body.new_name) {
            Ok(Some(_)) => {
                return err_json(
                    StatusCode::CONFLICT,
                    &format!("Instance '{}' already exists", body.new_name),
                )
            }
            Ok(None) => {}
            Err(e) => {
                tracing::error!("Failed to query instance: {e:#}");
                return err_json(StatusCode::INTERNAL_SERVER_ERROR, "Failed to query instance");
            }
        }

        // Allocate port
        let port = if let Some(p) = body.port {
            p
        } else {
            match registry.allocate_port_with_excludes(18801, 18999, &[]) {
                Ok(Some(p)) => p,
                Ok(None) => {
                    return err_json(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "No ports available in range 18801-18999",
                    )
                }
                Err(e) => {
                    tracing::error!("Port allocation failed: {e:#}");
                    return err_json(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "Failed to allocate port",
                    );
                }
            }
        };

        let new_id = uuid::Uuid::new_v4().to_string();
        let instances_dir = instances_dir_from_db(&db_path);
        let new_inst_dir = instances_dir.join(&new_id);

        // Read and parse source config
        let source_config_str = match std::fs::read_to_string(&source.config_path) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!("Failed to read source config: {e}");
                return err_json(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to read source config",
                );
            }
        };

        let mut config: crate::config::Config = match toml::from_str(&source_config_str) {
            Ok(c) => c,
            Err(e) => {
                tracing::error!("Failed to parse source config: {e}");
                return err_json(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to parse source config",
                );
            }
        };

        // Mutate clone
        config.gateway.port = port;
        config.gateway.paired_tokens = vec![]; // clear auth state

        let new_config_path = new_inst_dir.join("config.toml");
        config.config_path = new_config_path.clone();
        let new_workspace = new_inst_dir.join("workspace");
        config.workspace_dir = new_workspace.clone();

        // Create workspace subdirs
        for subdir in &["skills", "memory", "sessions", "state", "cron"] {
            if let Err(e) = std::fs::create_dir_all(new_workspace.join(subdir)) {
                tracing::error!("Failed to create workspace dir: {e}");
                let _ = std::fs::remove_dir_all(&new_inst_dir);
                return err_json(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to create workspace directories",
                );
            }
        }

        // Copy skills from source if they exist
        if let Some(ref src_ws) = source.workspace_dir {
            let src_skills = PathBuf::from(src_ws).join("skills");
            let dst_skills = new_workspace.join("skills");
            if src_skills.is_dir() {
                if let Ok(entries) = std::fs::read_dir(&src_skills) {
                    for entry in entries.flatten() {
                        let src_path = entry.path();
                        let dst_path = dst_skills.join(entry.file_name());
                        if src_path.is_dir() {
                            let _ = copy_dir_recursive(&src_path, &dst_path);
                        } else {
                            let _ = std::fs::copy(&src_path, &dst_path);
                        }
                    }
                }
            }
        }

        // Write config
        let toml_str = match toml::to_string_pretty(&config) {
            Ok(s) => s,
            Err(e) => {
                let _ = std::fs::remove_dir_all(&new_inst_dir);
                tracing::error!("Failed to serialize config: {e}");
                return err_json(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to serialize config",
                );
            }
        };

        if let Err(e) = write_config_atomic(&new_config_path, toml_str.as_bytes()) {
            let _ = std::fs::remove_dir_all(&new_inst_dir);
            tracing::error!("Failed to write config: {e}");
            return err_json(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to write config file",
            );
        }

        // Register in DB
        match registry.create_instance(
            &new_id,
            &body.new_name,
            port,
            new_config_path.to_str().unwrap_or(""),
            Some(new_workspace.to_str().unwrap_or("")),
            None,
        ) {
            Ok(()) => {}
            Err(e) => {
                let _ = std::fs::remove_dir_all(&new_inst_dir);
                let msg = format!("{e:#}");
                if msg.contains("UNIQUE constraint failed") {
                    return err_json(
                        StatusCode::CONFLICT,
                        &format!("Port {} is already in use by another instance", port),
                    );
                }
                tracing::error!("Failed to register cloned instance: {msg}");
                return err_json(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to register instance",
                );
            }
        }

        (
            StatusCode::CREATED,
            Json(serde_json::json!({
                "id": new_id,
                "name": body.new_name,
                "port": port,
                "cloned_from": name,
                "status": "stopped",
            })),
        )
    })
    .await;

    match result {
        Ok(resp) => resp,
        Err(e) => err_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("Task join error: {e}"),
        ),
    }
}

async fn handle_delete_instance(
    State(state): State<CpState>,
    AxumPath(name): AxumPath<String>,
) -> impl IntoResponse {
    let db_path = state.db_path.clone();
    let result = tokio::task::spawn_blocking(move || -> ApiResponse {
        let registry = match open_registry(&db_path) {
            Ok(r) => r,
            Err(resp) => return resp,
        };

        match registry.delete_archived_instance(&name) {
            Ok(Some(inst)) => {
                // Remove filesystem artifacts
                let inst_dir = lifecycle::instance_dir_from(&inst);
                if inst_dir.exists() {
                    if let Err(e) = std::fs::remove_dir_all(&inst_dir) {
                        tracing::warn!("Failed to remove instance dir {}: {e}", inst_dir.display());
                    }
                }
                ok_json(serde_json::json!({ "status": "deleted", "name": name }))
            }
            Ok(None) => {
                // Not archived -- check if it's active
                match registry.get_instance_by_name(&name) {
                    Ok(Some(_)) => err_json(
                        StatusCode::CONFLICT,
                        "Instance must be archived before deletion",
                    ),
                    _ => err_json(
                        StatusCode::NOT_FOUND,
                        &format!("No instance named '{name}'"),
                    ),
                }
            }
            Err(e) => {
                tracing::error!("Failed to delete instance: {e:#}");
                err_json(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to delete instance",
                )
            }
        }
    })
    .await;

    match result {
        Ok(resp) => resp,
        Err(e) => err_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("Task join error: {e}"),
        ),
    }
}

/// Recursively copy a directory.
fn copy_dir_recursive(src: &Path, dst: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());
        if src_path.is_dir() {
            copy_dir_recursive(&src_path, &dst_path)?;
        } else {
            std::fs::copy(&src_path, &dst_path)?;
        }
    }
    Ok(())
}

// ── Logs (enhanced with pagination modes) ────────────────────────

/// Query params for the logs endpoint.
#[derive(Deserialize)]
struct LogsQuery {
    lines: Option<usize>,
    offset: Option<usize>,
    mode: Option<String>,
}

/// Maximum number of log lines returnable.
const MAX_LOG_LINES: usize = 10_000;

async fn handle_logs(
    State(state): State<CpState>,
    AxumPath(name): AxumPath<String>,
    Query(query): Query<LogsQuery>,
) -> impl IntoResponse {
    let mode = query.mode.as_deref().unwrap_or("tail");

    // Validate mode
    if mode != "tail" && mode != "page" {
        return err_json(
            StatusCode::BAD_REQUEST,
            &format!("Invalid mode: '{mode}'. Valid values: tail, page"),
        );
    }

    let db_path = state.db_path.clone();
    let lines_count = query
        .lines
        .unwrap_or(lifecycle::DEFAULT_LOG_LINES)
        .min(MAX_LOG_LINES);
    let offset = query.offset.unwrap_or(0);
    let mode = mode.to_string();

    let result = tokio::task::spawn_blocking(move || -> ApiResponse {
        let registry = match open_registry(&db_path) {
            Ok(r) => r,
            Err(resp) => return resp,
        };

        let instance = match registry.get_instance_by_name(&name) {
            Ok(Some(inst)) => inst,
            Ok(None) => {
                return err_json(
                    StatusCode::NOT_FOUND,
                    &format!("No instance named '{name}'"),
                )
            }
            Err(e) => {
                tracing::error!("Failed to query instance: {e:#}");
                return err_json(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to query instance",
                );
            }
        };

        let inst_dir = lifecycle::instance_dir_from(&instance);
        let log_file = lifecycle::log_path(&inst_dir);

        if !log_file.exists() {
            return ok_json(serde_json::json!({
                "lines": [],
                "name": name,
                "mode": mode,
            }));
        }

        if mode == "page" {
            match read_lines_paginated(&log_file, offset, lines_count) {
                Ok((lines, window_lines, has_more, truncated)) => ok_json(serde_json::json!({
                    "lines": lines,
                    "name": name,
                    "mode": "page",
                    "offset": offset,
                    "window_lines": window_lines,
                    "has_more": has_more,
                    "truncated": truncated,
                })),
                Err(e) => {
                    tracing::error!("Failed to read log file: {e}");
                    err_json(StatusCode::INTERNAL_SERVER_ERROR, "Failed to read log file")
                }
            }
        } else {
            match read_last_n_lines(&log_file, lines_count) {
                Ok(tail) => ok_json(serde_json::json!({
                    "lines": tail,
                    "name": name,
                    "mode": "tail",
                })),
                Err(e) => {
                    tracing::error!("Failed to read log file: {e}");
                    err_json(StatusCode::INTERNAL_SERVER_ERROR, "Failed to read log file")
                }
            }
        }
    })
    .await;

    match result {
        Ok(resp) => resp,
        Err(e) => err_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("Task join error: {e}"),
        ),
    }
}

// ── Logs download (streamed) ─────────────────────────────────────

async fn handle_logs_download(
    State(state): State<CpState>,
    AxumPath(name): AxumPath<String>,
) -> Response {
    use futures_util::StreamExt;

    let db_path = state.db_path.clone();

    // Look up instance dir in a blocking task
    let lookup = tokio::task::spawn_blocking(move || -> Result<PathBuf, ApiResponse> {
        let registry = match open_registry(&db_path) {
            Ok(r) => r,
            Err(resp) => return Err(resp),
        };
        match registry.get_instance_by_name(&name) {
            Ok(Some(inst)) => Ok(lifecycle::instance_dir_from(&inst)),
            Ok(None) => Err(err_json(
                StatusCode::NOT_FOUND,
                &format!("No instance named '{name}'"),
            )),
            Err(e) => {
                tracing::error!("Failed to query instance: {e:#}");
                Err(err_json(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to query instance",
                ))
            }
        }
    })
    .await;

    let inst_dir = match lookup {
        Ok(Ok(p)) => p,
        Ok(Err((status, json))) => return (status, json).into_response(),
        Err(e) => {
            return err_json(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("Task join error: {e}"),
            )
            .into_response()
        }
    };

    let log_path = lifecycle::log_path(&inst_dir);
    let rotated_path = lifecycle::rotated_log_path(&inst_dir);
    let has_current = log_path.exists();
    let has_rotated = rotated_path.exists();

    if !has_current && !has_rotated {
        return Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "text/plain")
            .header(
                header::CONTENT_DISPOSITION,
                "attachment; filename=\"daemon.log\"",
            )
            .body(Body::empty())
            .unwrap();
    }

    // Build a chained stream: rotated (older) first, then current (newer).
    // This gives chronological order in the downloaded file.
    let body = match (has_rotated, has_current) {
        (true, true) => {
            let rotated_file = match tokio::fs::File::open(&rotated_path).await {
                Ok(f) => f,
                Err(e) => {
                    tracing::error!("Failed to open rotated log: {e}");
                    return err_json(StatusCode::INTERNAL_SERVER_ERROR, "Failed to open log file")
                        .into_response();
                }
            };
            let current_file = match tokio::fs::File::open(&log_path).await {
                Ok(f) => f,
                Err(e) => {
                    tracing::error!("Failed to open current log: {e}");
                    return err_json(StatusCode::INTERNAL_SERVER_ERROR, "Failed to open log file")
                        .into_response();
                }
            };
            let rotated_stream = ReaderStream::new(rotated_file);
            let current_stream = ReaderStream::new(current_file);
            Body::from_stream(rotated_stream.chain(current_stream))
        }
        (true, false) => {
            let file = match tokio::fs::File::open(&rotated_path).await {
                Ok(f) => f,
                Err(e) => {
                    tracing::error!("Failed to open rotated log: {e}");
                    return err_json(StatusCode::INTERNAL_SERVER_ERROR, "Failed to open log file")
                        .into_response();
                }
            };
            Body::from_stream(ReaderStream::new(file))
        }
        (false, true) => {
            let file = match tokio::fs::File::open(&log_path).await {
                Ok(f) => f,
                Err(e) => {
                    tracing::error!("Failed to open current log: {e}");
                    return err_json(StatusCode::INTERNAL_SERVER_ERROR, "Failed to open log file")
                        .into_response();
                }
            };
            Body::from_stream(ReaderStream::new(file))
        }
        (false, false) => unreachable!(), // guarded above
    };

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/plain")
        .header(
            header::CONTENT_DISPOSITION,
            "attachment; filename=\"daemon.log\"",
        )
        .body(body)
        .unwrap()
}

// ── Details endpoint ─────────────────────────────────────────────

async fn handle_details(
    State(state): State<CpState>,
    AxumPath(name): AxumPath<String>,
) -> impl IntoResponse {
    let db_path = state.db_path.clone();
    let result = tokio::task::spawn_blocking(move || -> ApiResponse {
        let registry = match open_registry(&db_path) {
            Ok(r) => r,
            Err(resp) => return resp,
        };

        let instance = match registry.get_instance_by_name(&name) {
            Ok(Some(inst)) => inst,
            Ok(None) => {
                return err_json(
                    StatusCode::NOT_FOUND,
                    &format!("No instance named '{name}'"),
                )
            }
            Err(e) => {
                tracing::error!("Failed to query instance: {e:#}");
                return err_json(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to query instance",
                );
            }
        };

        let inst_dir = lifecycle::instance_dir_from(&instance);
        let (live_status, live_pid) =
            lifecycle::live_status(&inst_dir).unwrap_or(("unknown".to_string(), None));

        // Load and parse config
        let config_path = Path::new(&instance.config_path);
        let (config_json, config_error, config_unknown_fields) = if config_path.exists() {
            match std::fs::read_to_string(config_path) {
                Ok(raw_toml) => {
                    // Parse as raw TOML value
                    let raw_value: Result<toml::Value, _> = toml::from_str(&raw_toml);
                    // Parse as typed Config
                    let typed_result: Result<crate::config::schema::Config, _> =
                        toml::from_str(&raw_toml);

                    match typed_result {
                        Ok(typed_config) => {
                            let mut config_val =
                                serde_json::to_value(&typed_config).unwrap_or_default();
                            mask_config_secrets(&mut config_val);

                            // Compute unknown fields
                            let unknown_fields = if let Ok(raw_val) = raw_value {
                                let raw_json = serde_json::to_value(&raw_val).unwrap_or_default();
                                let raw_paths = collect_key_paths(&raw_json, "");
                                let typed_paths = collect_key_paths(&config_val, "");
                                let diff: Vec<String> =
                                    raw_paths.difference(&typed_paths).cloned().collect();
                                if diff.is_empty() {
                                    serde_json::Value::Array(vec![])
                                } else {
                                    serde_json::json!(diff)
                                }
                            } else {
                                serde_json::Value::Array(vec![])
                            };

                            (Some(config_val), None, unknown_fields)
                        }
                        Err(e) => (None, Some(format!("{e}")), serde_json::Value::Array(vec![])),
                    }
                }
                Err(e) => (
                    None,
                    Some(format!("Failed to read config: {e}")),
                    serde_json::Value::Array(vec![]),
                ),
            }
        } else {
            (
                None,
                Some("Config file not found".to_string()),
                serde_json::Value::Array(vec![]),
            )
        };

        // Extract identity info from config
        let identity = if let Some(ref cfg) = config_json {
            let format = cfg
                .get("identity")
                .and_then(|i| i.get("format"))
                .and_then(|f| f.as_str())
                .unwrap_or("openclaw");
            let configured = cfg.get("identity").is_some();
            serde_json::json!({
                "format": format,
                "configured": configured,
            })
        } else {
            serde_json::json!({ "format": "unknown", "configured": false })
        };

        // Extract channel info
        let channels = if let Some(ref cfg) = config_json {
            if let Some(cc) = cfg.get("channels_config") {
                let cli = cc.get("cli").and_then(|v| v.as_bool()).unwrap_or(false);
                let mut ch = serde_json::json!({ "cli": cli });
                for channel_name in &[
                    "telegram", "discord", "slack", "webhook", "imessage", "matrix", "whatsapp",
                    "email", "irc",
                ] {
                    if let Some(channel_val) = cc.get(*channel_name) {
                        if !channel_val.is_null() {
                            ch[channel_name] = serde_json::json!({ "configured": true });
                        }
                    }
                }
                ch
            } else {
                serde_json::json!({ "cli": true })
            }
        } else {
            serde_json::json!({ "cli": false })
        };

        // Extract model info
        let model = if let Some(ref cfg) = config_json {
            let provider = cfg
                .get("default_provider")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");
            let model_name = cfg
                .get("default_model")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");
            let routes_count = cfg
                .get("model_routes")
                .and_then(|v| v.as_array())
                .map(|a| a.len())
                .unwrap_or(0);
            serde_json::json!({
                "provider": provider,
                "model": model_name,
                "routes_count": routes_count,
            })
        } else {
            serde_json::json!({ "provider": "unknown", "model": "unknown", "routes_count": 0 })
        };

        // Runtime info
        let runtime = if let Some(ref cfg) = config_json {
            let kind = cfg
                .get("runtime")
                .and_then(|r| r.get("kind"))
                .and_then(|v| v.as_str())
                .unwrap_or("native");
            let docker_image = cfg
                .get("runtime")
                .and_then(|r| r.get("docker"))
                .and_then(|d| d.get("image"))
                .and_then(|v| v.as_str());
            serde_json::json!({
                "kind": kind,
                "docker_image": docker_image,
            })
        } else {
            serde_json::json!({ "kind": "unknown", "docker_image": null })
        };

        let mut response = serde_json::json!({
            "instance": {
                "id": instance.id,
                "name": instance.name,
                "port": instance.port,
                "status": live_status,
                "pid": live_pid,
            },
            "config": config_json,
            "config_error": config_error,
            "config_unknown_fields": config_unknown_fields,
            "identity": identity,
            "channels": channels,
            "model": model,
            "runtime": runtime,
        });

        // Remove null config_error for cleaner output
        if response
            .get("config_error")
            .and_then(|v| v.as_null())
            .is_some()
        {
            if let Some(obj) = response.as_object_mut() {
                obj.remove("config_error");
            }
        }

        ok_json(response)
    })
    .await;

    match result {
        Ok(resp) => resp,
        Err(e) => err_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("Task join error: {e}"),
        ),
    }
}

// ── Tasks endpoint ───────────────────────────────────────────────

#[derive(Deserialize)]
struct TasksQuery {
    limit: Option<usize>,
    offset: Option<usize>,
    status: Option<String>,
    after: Option<String>,
    before: Option<String>,
}

async fn handle_tasks(
    State(state): State<CpState>,
    AxumPath(name): AxumPath<String>,
    Query(query): Query<TasksQuery>,
) -> impl IntoResponse {
    // Validate params
    let limit = query.limit.unwrap_or(20);
    if limit < 1 || limit > 1000 {
        return err_json(
            StatusCode::BAD_REQUEST,
            "Invalid limit: must be between 1 and 1000",
        );
    }
    let offset = query.offset.unwrap_or(0);
    if offset > 100_000 {
        return err_json(
            StatusCode::BAD_REQUEST,
            "Invalid offset: must be at most 100000",
        );
    }
    if let Some(ref status) = query.status {
        if !["started", "completed", "failed"].contains(&status.as_str()) {
            return err_json(
                StatusCode::BAD_REQUEST,
                &format!("Invalid status: '{status}'. Valid values: started, completed, failed"),
            );
        }
    }

    let db_path = state.db_path.clone();
    let status_filter = query.status.clone();
    let after = query.after.clone();
    let before = query.before.clone();

    let result = tokio::task::spawn_blocking(move || -> ApiResponse {
        let registry = match open_registry(&db_path) {
            Ok(r) => r,
            Err(resp) => return resp,
        };

        let instance = match registry.get_instance_by_name(&name) {
            Ok(Some(inst)) => inst,
            Ok(None) => {
                return err_json(
                    StatusCode::NOT_FOUND,
                    &format!("No instance named '{name}'"),
                )
            }
            Err(e) => {
                tracing::error!("Failed to query instance: {e:#}");
                return err_json(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to query instance",
                );
            }
        };

        match registry.list_agent_events(
            &instance.id,
            limit,
            offset,
            status_filter.as_deref(),
            after.as_deref(),
            before.as_deref(),
        ) {
            Ok((events, total)) => {
                let data_available = !events.is_empty() || total > 0;
                let tasks: Vec<serde_json::Value> = events
                    .iter()
                    .map(|e| {
                        serde_json::json!({
                            "id": e.id,
                            "instance_id": e.instance_id,
                            "event_type": e.event_type,
                            "channel": e.channel,
                            "summary": e.summary,
                            "status": e.status,
                            "duration_ms": e.duration_ms,
                            "correlation_id": e.correlation_id,
                            "created_at": e.created_at,
                        })
                    })
                    .collect();

                let mut resp = serde_json::json!({
                    "tasks": tasks,
                    "total": total,
                    "data_available": data_available,
                    "limit": limit,
                    "offset": offset,
                });
                if !data_available {
                    resp["message"] = serde_json::json!("No event data available.");
                }
                ok_json(resp)
            }
            Err(e) => {
                tracing::error!("Failed to list agent events: {e:#}");
                err_json(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to list agent events",
                )
            }
        }
    })
    .await;

    match result {
        Ok(resp) => resp,
        Err(e) => err_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("Task join error: {e}"),
        ),
    }
}

// ── Usage endpoint ───────────────────────────────────────────────

#[derive(Deserialize)]
struct UsageQuery {
    window: Option<String>,
}

async fn handle_usage(
    State(state): State<CpState>,
    AxumPath(name): AxumPath<String>,
    Query(query): Query<UsageQuery>,
) -> impl IntoResponse {
    let window = query.window.as_deref().unwrap_or("24h");
    if !["1h", "24h", "7d", "30d"].contains(&window) {
        return err_json(
            StatusCode::BAD_REQUEST,
            &format!("Invalid window: '{window}'. Valid values: 1h, 24h, 7d, 30d"),
        );
    }

    let db_path = state.db_path.clone();
    let window = window.to_string();

    let result = tokio::task::spawn_blocking(move || -> ApiResponse {
        let registry = match open_registry(&db_path) {
            Ok(r) => r,
            Err(resp) => return resp,
        };

        let instance = match registry.get_instance_by_name(&name) {
            Ok(Some(inst)) => inst,
            Ok(None) => {
                return err_json(
                    StatusCode::NOT_FOUND,
                    &format!("No instance named '{name}'"),
                )
            }
            Err(e) => {
                tracing::error!("Failed to query instance: {e:#}");
                return err_json(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to query instance",
                );
            }
        };

        // Compute window start from now
        let now = chrono::Utc::now();
        let duration = match window.as_str() {
            "1h" => chrono::Duration::hours(1),
            "24h" => chrono::Duration::hours(24),
            "7d" => chrono::Duration::days(7),
            "30d" => chrono::Duration::days(30),
            _ => unreachable!(),
        };
        let window_start = (now - duration).format("%Y-%m-%d %H:%M:%S").to_string();
        let window_end = now.format("%Y-%m-%d %H:%M:%S").to_string();

        match registry.get_agent_usage(&instance.id, Some(&window_start), Some(&window_end)) {
            Ok(summary) => {
                let data_available = summary.request_count > 0;
                ok_json(serde_json::json!({
                    "instance_name": name,
                    "window": window,
                    "data_available": data_available,
                    "usage": {
                        "input_tokens": summary.input_tokens,
                        "output_tokens": summary.output_tokens,
                        "total_tokens": summary.total_tokens,
                        "request_count": summary.request_count,
                        "unknown_count": summary.unknown_count,
                    },
                }))
            }
            Err(e) => {
                tracing::error!("Failed to query usage: {e:#}");
                err_json(StatusCode::INTERNAL_SERVER_ERROR, "Failed to query usage")
            }
        }
    })
    .await;

    match result {
        Ok(resp) => resp,
        Err(e) => err_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("Task join error: {e}"),
        ),
    }
}

// ── Config API ──────────────────────────────────────────────────

#[derive(Deserialize)]
struct ConfigPutBody {
    config: String, // TOML string
    etag: String,   // SHA-256 hex of previous file bytes
}

#[derive(Deserialize)]
struct ConfigBody {
    config: String, // TOML string (used by validate + diff)
}

fn compute_config_etag(raw_bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(raw_bytes))
}

/// Produce masked TOML and masked JSON from a typed Config.
fn masked_config_outputs(
    config: &crate::config::schema::Config,
) -> Result<(String, serde_json::Value), String> {
    let mut config_json = serde_json::to_value(config).map_err(|e| format!("{e}"))?;
    mask_config_secrets(&mut config_json);
    // Deserialize masked JSON back to Config, then serialize to pretty TOML
    let masked_config: crate::config::schema::Config =
        serde_json::from_value(config_json.clone()).map_err(|e| format!("{e}"))?;
    let toml_str = toml::to_string_pretty(&masked_config).map_err(|e| format!("{e}"))?;
    Ok((toml_str, config_json))
}

// ── GET /api/instances/:name/config ─────────────────────────────

async fn handle_config_get(
    State(state): State<CpState>,
    AxumPath(name): AxumPath<String>,
) -> impl IntoResponse {
    let db_path = state.db_path.clone();
    let result = tokio::task::spawn_blocking(move || -> ApiResponse {
        let registry = match open_registry(&db_path) {
            Ok(r) => r,
            Err(resp) => return resp,
        };

        let instance = match registry.get_instance_by_name(&name) {
            Ok(Some(inst)) => inst,
            Ok(None) => {
                return err_json(
                    StatusCode::NOT_FOUND,
                    &format!("No instance named '{name}'"),
                )
            }
            Err(e) => {
                tracing::error!("Failed to query instance: {e:#}");
                return err_json(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to query instance",
                );
            }
        };

        let config_path = Path::new(&instance.config_path);
        let raw_bytes = match std::fs::read(config_path) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return err_json(StatusCode::NOT_FOUND, "Config file not found")
            }
            Err(e) => {
                tracing::error!("Failed to read config: {e}");
                return err_json(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to read config file",
                );
            }
        };

        let etag = compute_config_etag(&raw_bytes);

        let raw_str = match String::from_utf8(raw_bytes) {
            Ok(s) => s,
            Err(_) => {
                return err_json(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Config file is not valid UTF-8",
                )
            }
        };

        let config: crate::config::schema::Config = match toml::from_str(&raw_str) {
            Ok(c) => c,
            Err(e) => {
                return err_json(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &format!("Config parse error: {e}"),
                )
            }
        };

        let (masked_toml, masked_json) = match masked_config_outputs(&config) {
            Ok(v) => v,
            Err(msg) => return err_json(StatusCode::INTERNAL_SERVER_ERROR, &msg),
        };

        ok_json(serde_json::json!({
            "name": name,
            "config_toml": masked_toml,
            "config_masked": masked_json,
            "etag": etag,
        }))
    })
    .await;

    match result {
        Ok(resp) => resp,
        Err(e) => err_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("Task join error: {e}"),
        ),
    }
}

// ── PUT /api/instances/:name/config ─────────────────────────────

async fn handle_config_put(
    State(state): State<CpState>,
    AxumPath(name): AxumPath<String>,
    headers: HeaderMap,
    Json(body): Json<ConfigPutBody>,
) -> impl IntoResponse {
    let db_path = state.db_path.clone();
    let allow_secret_write = headers
        .get("x-allow-secret-write")
        .and_then(|v| v.to_str().ok())
        .map_or(false, |v| v.eq_ignore_ascii_case("true"));

    let result = tokio::task::spawn_blocking(move || -> ApiResponse {
        let registry = match open_registry(&db_path) {
            Ok(r) => r,
            Err(resp) => return resp,
        };

        let instance = match registry.get_instance_by_name(&name) {
            Ok(Some(inst)) => inst,
            Ok(None) => {
                return err_json(
                    StatusCode::NOT_FOUND,
                    &format!("No instance named '{name}'"),
                )
            }
            Err(e) => {
                tracing::error!("Failed to query instance: {e:#}");
                return err_json(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to query instance",
                );
            }
        };

        // Validate etag is non-empty
        if body.etag.is_empty() {
            return err_json(StatusCode::BAD_REQUEST, "ETag is required");
        }

        let config_path_str = instance.config_path.clone();
        let config_path = Path::new(&config_path_str);

        // Read current file, compute current ETag
        let current_bytes = match std::fs::read(config_path) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return err_json(StatusCode::NOT_FOUND, "Config file not found")
            }
            Err(e) => {
                tracing::error!("Failed to read config: {e}");
                return err_json(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to read config file",
                );
            }
        };

        let current_etag = compute_config_etag(&current_bytes);
        if body.etag != current_etag {
            return (
                StatusCode::CONFLICT,
                Json(serde_json::json!({
                    "error": "ETag mismatch: config modified since last read",
                    "current_etag": current_etag,
                })),
            );
        }

        // Parse incoming TOML as Config
        let incoming_config: crate::config::schema::Config = match toml::from_str(&body.config) {
            Ok(c) => c,
            Err(e) => return err_json(StatusCode::BAD_REQUEST, &format!("Invalid config: {e}")),
        };

        // Parse current config for sentinel preservation
        let current_str = String::from_utf8_lossy(&current_bytes);
        let current_config: crate::config::schema::Config = match toml::from_str(&current_str) {
            Ok(c) => c,
            Err(e) => {
                return err_json(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &format!("Current config parse error: {e}"),
                )
            }
        };

        let mut incoming_json = serde_json::to_value(&incoming_config).unwrap_or_default();
        let current_json = serde_json::to_value(&current_config).unwrap_or_default();

        // Preserve masked sentinels
        match preserve_masked_secrets(&mut incoming_json, &current_json) {
            Err((_path, msg)) => {
                return err_json(StatusCode::BAD_REQUEST, &msg);
            }
            Ok(new_secret_paths) => {
                // Block new secrets unless header allows
                if !new_secret_paths.is_empty() && !allow_secret_write {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(serde_json::json!({
                            "error": "Config contains secret fields blocked by default",
                            "blocked_fields": new_secret_paths,
                            "hint": "Set header X-Allow-Secret-Write: true to allow",
                        })),
                    );
                }
            }
        }

        // Deserialize merged JSON back to Config
        let mut final_config: crate::config::schema::Config =
            match serde_json::from_value(incoming_json) {
                Ok(c) => c,
                Err(e) => {
                    return err_json(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        &format!("Failed to reconstruct config: {e}"),
                    )
                }
            };

        // Acquire lifecycle lock
        let inst_dir = lifecycle::instance_dir_from(&instance);
        let _lock = match lifecycle::try_lifecycle_lock(&inst_dir) {
            Ok(lifecycle::LockOutcome::Acquired(f)) => f,
            Ok(lifecycle::LockOutcome::Contended) => {
                return err_json(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "Lifecycle lock held (concurrent operation in progress)",
                )
            }
            Err(e) => {
                tracing::error!("Failed to acquire lifecycle lock: {e:#}");
                return err_json(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to acquire lifecycle lock",
                );
            }
        };

        // Double-check ETag after lock acquisition
        let recheck_bytes = match std::fs::read(config_path) {
            Ok(b) => b,
            Err(e) => {
                tracing::error!("Failed to re-read config: {e}");
                return err_json(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to re-read config file",
                );
            }
        };
        let recheck_etag = compute_config_etag(&recheck_bytes);
        if body.etag != recheck_etag {
            return (
                StatusCode::CONFLICT,
                Json(serde_json::json!({
                    "error": "ETag mismatch: config modified since last read",
                    "current_etag": recheck_etag,
                })),
            );
        }

        // Set skip fields
        final_config.config_path = PathBuf::from(&config_path_str);
        final_config.workspace_dir = inst_dir.join("workspace");

        // Atomic write
        if let Err(e) = final_config.save() {
            tracing::error!("Failed to save config: {e:#}");
            return err_json(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("Failed to save config: {e}"),
            );
        }

        // Compute new ETag
        let new_bytes = match std::fs::read(config_path) {
            Ok(b) => b,
            Err(e) => {
                tracing::error!("Failed to read saved config: {e}");
                return err_json(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Config saved but failed to compute new ETag",
                );
            }
        };
        let new_etag = compute_config_etag(&new_bytes);

        // Check if instance is running
        let (live_status, _live_pid) =
            lifecycle::live_status(&inst_dir).unwrap_or(("unknown".to_string(), None));
        let restart_recommended = live_status == "running";

        ok_json(serde_json::json!({
            "status": "saved",
            "name": name,
            "etag": new_etag,
            "restart_recommended": restart_recommended,
        }))
    })
    .await;

    match result {
        Ok(resp) => resp,
        Err(e) => err_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("Task join error: {e}"),
        ),
    }
}

// ── POST /api/instances/:name/config/validate ───────────────────

async fn handle_config_validate(
    State(state): State<CpState>,
    AxumPath(name): AxumPath<String>,
    Json(body): Json<ConfigBody>,
) -> impl IntoResponse {
    let db_path = state.db_path.clone();
    let result = tokio::task::spawn_blocking(move || -> ApiResponse {
        let registry = match open_registry(&db_path) {
            Ok(r) => r,
            Err(resp) => return resp,
        };

        // Verify instance exists
        match registry.get_instance_by_name(&name) {
            Ok(Some(_)) => {}
            Ok(None) => {
                return err_json(
                    StatusCode::NOT_FOUND,
                    &format!("No instance named '{name}'"),
                )
            }
            Err(e) => {
                tracing::error!("Failed to query instance: {e:#}");
                return err_json(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to query instance",
                );
            }
        };

        match toml::from_str::<crate::config::schema::Config>(&body.config) {
            Ok(_) => ok_json(serde_json::json!({ "valid": true })),
            Err(e) => (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "valid": false,
                    "error": format!("{e}"),
                })),
            ),
        }
    })
    .await;

    match result {
        Ok(resp) => resp,
        Err(e) => err_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("Task join error: {e}"),
        ),
    }
}

// ── POST /api/instances/:name/config/diff ───────────────────────

async fn handle_config_diff(
    State(state): State<CpState>,
    AxumPath(name): AxumPath<String>,
    Json(body): Json<ConfigBody>,
) -> impl IntoResponse {
    let db_path = state.db_path.clone();
    let result = tokio::task::spawn_blocking(move || -> ApiResponse {
        let registry = match open_registry(&db_path) {
            Ok(r) => r,
            Err(resp) => return resp,
        };

        let instance = match registry.get_instance_by_name(&name) {
            Ok(Some(inst)) => inst,
            Ok(None) => {
                return err_json(
                    StatusCode::NOT_FOUND,
                    &format!("No instance named '{name}'"),
                )
            }
            Err(e) => {
                tracing::error!("Failed to query instance: {e:#}");
                return err_json(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to query instance",
                );
            }
        };

        // Read + parse current config
        let config_path = Path::new(&instance.config_path);
        let current_bytes = match std::fs::read(config_path) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return err_json(StatusCode::NOT_FOUND, "Config file not found")
            }
            Err(e) => {
                tracing::error!("Failed to read config: {e}");
                return err_json(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to read config file",
                );
            }
        };

        let current_str = String::from_utf8_lossy(&current_bytes);

        // Parse raw TOML for unknown field detection
        let raw_toml_value: Result<toml::Value, _> = toml::from_str(&current_str);

        let current_config: crate::config::schema::Config = match toml::from_str(&current_str) {
            Ok(c) => c,
            Err(e) => {
                return err_json(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &format!("Current config parse error: {e}"),
                )
            }
        };

        // Parse proposed config
        let proposed_config: crate::config::schema::Config = match toml::from_str(&body.config) {
            Ok(c) => c,
            Err(e) => return err_json(StatusCode::BAD_REQUEST, &format!("Invalid config: {e}")),
        };

        // Mask both sides
        let mut current_json = serde_json::to_value(&current_config).unwrap_or_default();
        mask_config_secrets(&mut current_json);

        let mut proposed_json = serde_json::to_value(&proposed_config).unwrap_or_default();
        mask_config_secrets(&mut proposed_json);

        // Diff
        let diff = diff_json(&current_json, &proposed_json);

        // Unknown fields warning
        let unknown_fields_warning: Vec<String> = if let Ok(raw_val) = raw_toml_value {
            let raw_json = serde_json::to_value(&raw_val).unwrap_or_default();
            let raw_paths = collect_key_paths(&raw_json, "");
            let typed_paths = collect_key_paths(&current_json, "");
            raw_paths.difference(&typed_paths).cloned().collect()
        } else {
            Vec::new()
        };

        ok_json(serde_json::json!({
            "changes": diff.changes,
            "added": diff.added,
            "removed": diff.removed,
            "unchanged_count": diff.unchanged_count,
            "unknown_fields_warning": unknown_fields_warning,
        }))
    })
    .await;

    match result {
        Ok(resp) => resp,
        Err(e) => err_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("Task join error: {e}"),
        ),
    }
}
