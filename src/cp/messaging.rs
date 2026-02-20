use std::path::{Path, PathBuf};
use std::sync::Arc;

use axum::extract::{Path as AxumPath, Query, State};
use axum::http::StatusCode;
use axum::Json;
use serde::Deserialize;

use crate::cp::masking::redact_payload_secrets;
use crate::cp::server::CpState;
use crate::db::{MessageDirection, MessageFilters, NewMessage, Registry};
use crate::lifecycle;

type ApiResponse = (StatusCode, Json<serde_json::Value>);

fn ok_json(value: serde_json::Value) -> ApiResponse {
    (StatusCode::OK, Json(value))
}

fn err_json(status: StatusCode, message: &str) -> ApiResponse {
    (status, Json(serde_json::json!({ "error": message })))
}

const MAX_PAYLOAD_BYTES: usize = 65536; // 64 KiB
const MAX_HOP_COUNT: i64 = 8;

// ── Routing rules ────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct CreateRuleBody {
    pub from_instance: String,
    pub to_instance: String,
    pub type_pattern: String,
    #[serde(default = "default_max_retries")]
    pub max_retries: i64,
    #[serde(default = "default_ttl_secs")]
    pub ttl_secs: i64,
    #[serde(default)]
    pub auto_start: bool,
}

fn default_max_retries() -> i64 {
    5
}
fn default_ttl_secs() -> i64 {
    3600
}

pub async fn handle_create_rule(
    State(state): State<CpState>,
    Json(body): Json<CreateRuleBody>,
) -> ApiResponse {
    let db_path = state.db_path.clone();
    let result =
        tokio::task::spawn_blocking(move || -> Result<serde_json::Value, (StatusCode, String)> {
            let registry = Registry::open(&db_path)
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}")))?;

            // Validate both instances exist (D10)
            if registry
                .get_instance_by_name(&body.from_instance)
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}")))?
                .is_none()
            {
                return Err((
                    StatusCode::NOT_FOUND,
                    format!("No instance named '{}'", body.from_instance),
                ));
            }
            if registry
                .get_instance_by_name(&body.to_instance)
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}")))?
                .is_none()
            {
                return Err((
                    StatusCode::NOT_FOUND,
                    format!("No instance named '{}'", body.to_instance),
                ));
            }

            let id = registry
                .create_routing_rule(
                    &body.from_instance,
                    &body.to_instance,
                    &body.type_pattern,
                    body.max_retries,
                    body.ttl_secs,
                    body.auto_start,
                )
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}")))?;

            Ok(serde_json::json!({
                "id": id,
                "from_instance": body.from_instance,
                "to_instance": body.to_instance,
                "type_pattern": body.type_pattern,
            }))
        })
        .await;

    match result {
        Ok(Ok(value)) => (StatusCode::CREATED, Json(value)),
        Ok(Err((status, msg))) => err_json(status, &msg),
        Err(e) => err_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("Task join error: {e}"),
        ),
    }
}

pub async fn handle_list_rules(State(state): State<CpState>) -> ApiResponse {
    let db_path = state.db_path.clone();
    let result = tokio::task::spawn_blocking(move || -> Result<serde_json::Value, String> {
        let registry = Registry::open(&db_path).map_err(|e| format!("{e:#}"))?;
        let rules = registry
            .list_routing_rules()
            .map_err(|e| format!("{e:#}"))?;
        let json: Vec<serde_json::Value> = rules
            .iter()
            .map(|r| {
                serde_json::json!({
                    "id": r.id,
                    "from_instance": r.from_instance,
                    "to_instance": r.to_instance,
                    "type_pattern": r.type_pattern,
                    "max_retries": r.max_retries,
                    "ttl_secs": r.ttl_secs,
                    "auto_start": r.auto_start,
                    "created_at": r.created_at,
                })
            })
            .collect();
        Ok(serde_json::json!(json))
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

pub async fn handle_delete_rule(
    State(state): State<CpState>,
    AxumPath(id): AxumPath<String>,
) -> ApiResponse {
    let db_path = state.db_path.clone();
    let result =
        tokio::task::spawn_blocking(move || -> Result<serde_json::Value, (StatusCode, String)> {
            let registry = Registry::open(&db_path)
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}")))?;
            let deleted = registry
                .delete_routing_rule(&id)
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}")))?;
            if deleted {
                Ok(serde_json::json!({ "deleted": true, "id": id }))
            } else {
                Err((
                    StatusCode::NOT_FOUND,
                    format!("No routing rule with id '{id}'"),
                ))
            }
        })
        .await;

    match result {
        Ok(Ok(value)) => ok_json(value),
        Ok(Err((status, msg))) => err_json(status, &msg),
        Err(e) => err_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("Task join error: {e}"),
        ),
    }
}

// ── Send message ─────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct SendMessageBody {
    pub from_instance: String,
    pub to_instance: String,
    #[serde(rename = "type")]
    pub message_type: String,
    pub payload: serde_json::Value,
    pub correlation_id: Option<String>,
    pub idempotency_key: Option<String>,
    #[serde(default)]
    pub hop_count: i64,
}

pub async fn handle_send_message(
    State(state): State<CpState>,
    Json(body): Json<SendMessageBody>,
) -> ApiResponse {
    let db_path = state.db_path.clone();
    let result = tokio::task::spawn_blocking(
        move || -> Result<(StatusCode, serde_json::Value), (StatusCode, String)> {
            validate_and_enqueue(&db_path, body)
        },
    )
    .await;

    match result {
        Ok(Ok((status, value))) => (status, Json(value)),
        Ok(Err((status, msg))) => err_json(status, &msg),
        Err(e) => err_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("Task join error: {e}"),
        ),
    }
}

#[allow(clippy::too_many_lines)]
fn validate_and_enqueue(
    db_path: &Path,
    mut body: SendMessageBody,
) -> Result<(StatusCode, serde_json::Value), (StatusCode, String)> {
    let registry = Registry::open(db_path)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}")))?;

    // 1. Instance existence (D10)
    if registry
        .get_instance_by_name(&body.from_instance)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}")))?
        .is_none()
    {
        return Err((
            StatusCode::NOT_FOUND,
            format!("No instance named '{}'", body.from_instance),
        ));
    }
    if registry
        .get_instance_by_name(&body.to_instance)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}")))?
        .is_none()
    {
        return Err((
            StatusCode::NOT_FOUND,
            format!("No instance named '{}'", body.to_instance),
        ));
    }

    // 2. Payload size check
    let payload_str = body.payload.to_string();
    if payload_str.len() > MAX_PAYLOAD_BYTES {
        return Err((
            StatusCode::BAD_REQUEST,
            format!(
                "Payload exceeds maximum size of {} bytes ({} bytes)",
                MAX_PAYLOAD_BYTES,
                payload_str.len()
            ),
        ));
    }

    // 3. Hop count check
    if body.hop_count >= MAX_HOP_COUNT {
        return Err((
            StatusCode::BAD_REQUEST,
            format!(
                "Hop count {} exceeds maximum of {}",
                body.hop_count, MAX_HOP_COUNT
            ),
        ));
    }

    // 4. Routing allowlist check
    let rule = registry
        .check_route_allowed(&body.from_instance, &body.to_instance, &body.message_type)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}")))?;
    let Some(rule) = rule else {
        return Err((
            StatusCode::FORBIDDEN,
            format!(
                "No routing rule allows {} -> {} for type '{}'",
                body.from_instance, body.to_instance, body.message_type
            ),
        ));
    };

    // 5. Idempotency check
    if let Some(ref key) = body.idempotency_key {
        if let Some(existing_id) = registry
            .check_idempotency_key(key)
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}")))?
        {
            // Return existing message ID (not an error)
            return Ok((
                StatusCode::OK,
                serde_json::json!({
                    "id": existing_id,
                    "deduplicated": true,
                }),
            ));
        }
    }

    // 6. Secret redaction
    redact_payload_secrets(&mut body.payload);

    // 7. Enqueue
    let msg_id = uuid::Uuid::new_v4().to_string();
    let new_msg = NewMessage {
        id: msg_id.clone(),
        from_instance: body.from_instance.clone(),
        to_instance: body.to_instance.clone(),
        message_type: body.message_type.clone(),
        payload: body.payload.to_string(),
        correlation_id: body.correlation_id.clone(),
        idempotency_key: body.idempotency_key.clone(),
        hop_count: body.hop_count,
        max_retries: rule.max_retries,
        ttl_secs: rule.ttl_secs,
    };

    let msg = registry
        .enqueue_message(&new_msg)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}")))?;
    registry
        .append_message_event(&msg.id, "created", None)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}")))?;

    // 8. Auto-start check
    if rule.auto_start {
        if let Ok(Some(inst)) = registry.get_instance_by_name(&body.to_instance) {
            let inst_dir = lifecycle::instance_dir_from(&inst);
            let (live_status, _) =
                lifecycle::live_status(&inst_dir).unwrap_or(("unknown".into(), None));
            if live_status == "stopped" || live_status == "dead" {
                tracing::info!(
                    "Auto-starting instance '{}' for pending message",
                    body.to_instance
                );
                if let Err(e) = lifecycle::start_instance(&registry, &body.to_instance) {
                    tracing::warn!("Auto-start failed for '{}': {e}", body.to_instance);
                }
            }
        }
    }

    Ok((
        StatusCode::CREATED,
        serde_json::json!({
            "id": msg.id,
            "status": msg.status,
            "expires_at": msg.expires_at,
        }),
    ))
}

// ── Receive message (long-poll) ──────────────────────────────────

#[derive(Deserialize)]
pub struct ReceiveQuery {
    #[serde(default = "default_wait")]
    pub wait: u64,
}

fn default_wait() -> u64 {
    30
}

pub async fn handle_receive_message(
    State(state): State<CpState>,
    AxumPath(name): AxumPath<String>,
    Query(query): Query<ReceiveQuery>,
) -> ApiResponse {
    let wait_secs = query.wait.min(60); // Cap at 60s
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(wait_secs);

    loop {
        let db_path = state.db_path.clone();
        let instance_name = name.clone();

        let result = tokio::task::spawn_blocking(move || -> Result<Option<serde_json::Value>, String> {
            let registry = Registry::open(&db_path).map_err(|e| format!("{e:#}"))?;
            let msg = registry.lease_pending_message(&instance_name).map_err(|e| format!("{e:#}"))?;
            match msg {
                Some(m) => {
                    let _ = registry.append_message_event(&m.id, "leased", None);
                    Ok(Some(serde_json::json!({
                        "id": m.id,
                        "from_instance": m.from_instance,
                        "to_instance": m.to_instance,
                        "message_type": m.message_type,
                        "payload": serde_json::from_str::<serde_json::Value>(&m.payload).unwrap_or(serde_json::Value::String(m.payload.clone())),
                        "correlation_id": m.correlation_id,
                        "hop_count": m.hop_count,
                        "created_at": m.created_at,
                    })))
                }
                None => Ok(None),
            }
        }).await;

        match result {
            Ok(Ok(Some(msg_json))) => {
                return ok_json(serde_json::json!({ "message": msg_json }));
            }
            Ok(Ok(None)) => {
                // No message available, check if we should keep waiting
                if tokio::time::Instant::now() >= deadline {
                    return ok_json(serde_json::json!({ "message": null }));
                }
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            }
            Ok(Err(msg)) => {
                return err_json(StatusCode::INTERNAL_SERVER_ERROR, &msg);
            }
            Err(e) => {
                return err_json(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &format!("Task join error: {e}"),
                );
            }
        }
    }
}

// ── Acknowledge message ──────────────────────────────────────────

pub async fn handle_acknowledge_message(
    State(state): State<CpState>,
    AxumPath(id): AxumPath<String>,
) -> ApiResponse {
    let db_path = state.db_path.clone();
    let result =
        tokio::task::spawn_blocking(move || -> Result<serde_json::Value, (StatusCode, String)> {
            let registry = Registry::open(&db_path)
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}")))?;
            let acked = registry
                .acknowledge_message(&id)
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}")))?;
            if acked {
                let _ = registry.append_message_event(&id, "acknowledged", None);
                Ok(serde_json::json!({ "id": id, "status": "acknowledged" }))
            } else {
                Err((
                    StatusCode::NOT_FOUND,
                    format!("No leased message with id '{id}'"),
                ))
            }
        })
        .await;

    match result {
        Ok(Ok(value)) => ok_json(value),
        Ok(Err((status, msg))) => err_json(status, &msg),
        Err(e) => err_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("Task join error: {e}"),
        ),
    }
}

// ── Delivery worker ──────────────────────────────────────────────

pub async fn run_delivery_worker(
    db_path: Arc<PathBuf>,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) {
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(2));

    loop {
        tokio::select! {
            _ = interval.tick() => {},
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    tracing::info!("Delivery worker shutting down");
                    return;
                }
            }
        }

        let db = db_path.clone();
        let result = tokio::task::spawn_blocking(move || delivery_tick(&db)).await;

        if let Ok(Err(e)) = result {
            tracing::error!("Delivery worker tick error: {e:#}");
        }
    }
}

// ── Sanitization helpers (Phase 10.2) ─────────────────────────

fn sanitize_message_for_response(msg: &crate::db::Message) -> serde_json::Value {
    let mut payload: serde_json::Value = serde_json::from_str(&msg.payload)
        .unwrap_or(serde_json::Value::String(msg.payload.clone()));
    redact_payload_secrets(&mut payload);
    serde_json::json!({
        "id": msg.id,
        "from_instance": msg.from_instance,
        "to_instance": msg.to_instance,
        "type": msg.message_type,
        "payload": payload,
        "correlation_id": msg.correlation_id,
        "hop_count": msg.hop_count,
        "status": msg.status,
        "retry_count": msg.retry_count,
        "max_retries": msg.max_retries,
        "next_attempt_at": msg.next_attempt_at,
        "lease_expires_at": msg.lease_expires_at,
        "expires_at": msg.expires_at,
        "created_at": msg.created_at,
        "updated_at": msg.updated_at,
    })
}

fn sanitize_event_for_response(evt: &crate::db::MessageEvent) -> serde_json::Value {
    serde_json::json!({
        "id": evt.id,
        "message_id": evt.message_id,
        "event_type": evt.event_type,
        "detail": evt.detail,
        "created_at": evt.created_at,
    })
}

/// Validate a datetime string matches the canonical format.
fn validate_datetime(value: &str, param_name: &str) -> Result<(), (StatusCode, String)> {
    chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S").map_err(|_| {
        (
            StatusCode::BAD_REQUEST,
            format!(
                "Invalid datetime format for '{}': expected YYYY-MM-DD HH:MM:SS",
                param_name
            ),
        )
    })?;
    Ok(())
}

// ── Observability query param structs ─────────────────────────

#[derive(Deserialize)]
pub struct MessagesQuery {
    pub correlation_id: Option<String>,
    pub from: Option<String>,
    pub to: Option<String>,
    #[serde(rename = "type")]
    pub message_type: Option<String>,
    pub status: Option<String>,
    pub after: Option<String>,
    pub before: Option<String>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[derive(Deserialize)]
pub struct InstanceMessagesQuery {
    pub direction: Option<String>,
    pub status: Option<String>,
    #[serde(rename = "type")]
    pub message_type: Option<String>,
    pub after: Option<String>,
    pub before: Option<String>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[derive(Deserialize)]
pub struct DeadLetterQuery {
    pub from: Option<String>,
    pub to: Option<String>,
    pub after: Option<String>,
    pub before: Option<String>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

const VALID_STATUSES: &[&str] = &["queued", "leased", "acknowledged", "dead_letter"];

// ── GET /api/messages/:id ─────────────────────────────────────

pub async fn handle_get_message(
    State(state): State<CpState>,
    AxumPath(id): AxumPath<String>,
) -> ApiResponse {
    let db_path = state.db_path.clone();
    let result =
        tokio::task::spawn_blocking(move || -> Result<serde_json::Value, (StatusCode, String)> {
            let registry = Registry::open(&db_path)
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}")))?;

            let msg = registry
                .get_message(&id)
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}")))?
                .ok_or_else(|| (StatusCode::NOT_FOUND, format!("No message with id '{id}'")))?;

            let events = registry
                .get_message_events(&id)
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}")))?;

            let mut msg_json = sanitize_message_for_response(&msg);
            msg_json["events"] =
                serde_json::Value::Array(events.iter().map(sanitize_event_for_response).collect());

            Ok(msg_json)
        })
        .await;

    match result {
        Ok(Ok(value)) => ok_json(value),
        Ok(Err((status, msg))) => err_json(status, &msg),
        Err(e) => err_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("Task join error: {e}"),
        ),
    }
}

// ── GET /api/messages/:id/events ──────────────────────────────

pub async fn handle_get_message_events(
    State(state): State<CpState>,
    AxumPath(id): AxumPath<String>,
) -> ApiResponse {
    let db_path = state.db_path.clone();
    let result =
        tokio::task::spawn_blocking(move || -> Result<serde_json::Value, (StatusCode, String)> {
            let registry = Registry::open(&db_path)
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}")))?;

            // Verify message exists
            registry
                .get_message(&id)
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}")))?
                .ok_or_else(|| (StatusCode::NOT_FOUND, format!("No message with id '{id}'")))?;

            let events = registry
                .get_message_events(&id)
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}")))?;

            Ok(serde_json::json!({
                "message_id": id,
                "events": events.iter().map(sanitize_event_for_response).collect::<Vec<_>>(),
            }))
        })
        .await;

    match result {
        Ok(Ok(value)) => ok_json(value),
        Ok(Err((status, msg))) => err_json(status, &msg),
        Err(e) => err_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("Task join error: {e}"),
        ),
    }
}

// ── GET /api/messages ─────────────────────────────────────────

pub async fn handle_list_messages(
    State(state): State<CpState>,
    Query(query): Query<MessagesQuery>,
) -> ApiResponse {
    // Validate params
    let limit = query.limit.unwrap_or(20);
    if !(1..=1000).contains(&limit) {
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
        if !VALID_STATUSES.contains(&status.as_str()) {
            return err_json(
                StatusCode::BAD_REQUEST,
                &format!(
                    "Invalid status: '{}'. Valid values: queued, leased, acknowledged, dead_letter",
                    status
                ),
            );
        }
    }
    if let Some(ref after) = query.after {
        if let Err((status, msg)) = validate_datetime(after, "after") {
            return err_json(status, &msg);
        }
    }
    if let Some(ref before) = query.before {
        if let Err((status, msg)) = validate_datetime(before, "before") {
            return err_json(status, &msg);
        }
    }

    let db_path = state.db_path.clone();
    let filters = MessageFilters {
        correlation_id: query.correlation_id,
        from_instance: query.from,
        to_instance: query.to,
        message_type: query.message_type,
        status: query.status,
        after: query.after,
        before: query.before,
        limit,
        offset,
    };

    let result =
        tokio::task::spawn_blocking(move || -> Result<serde_json::Value, (StatusCode, String)> {
            let registry = Registry::open(&db_path)
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}")))?;

            let (messages, total) = registry
                .list_messages(&filters)
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}")))?;

            let items: Vec<serde_json::Value> =
                messages.iter().map(sanitize_message_for_response).collect();

            Ok(serde_json::json!({
                "items": items,
                "total": total,
                "limit": filters.limit,
                "offset": filters.offset,
            }))
        })
        .await;

    match result {
        Ok(Ok(value)) => ok_json(value),
        Ok(Err((status, msg))) => err_json(status, &msg),
        Err(e) => err_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("Task join error: {e}"),
        ),
    }
}

// ── GET /api/instances/:name/messages ─────────────────────────

pub async fn handle_list_instance_messages(
    State(state): State<CpState>,
    AxumPath(name): AxumPath<String>,
    Query(query): Query<InstanceMessagesQuery>,
) -> ApiResponse {
    // Validate params
    let limit = query.limit.unwrap_or(20);
    if !(1..=1000).contains(&limit) {
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

    let direction_str = query.direction.as_deref().unwrap_or("all").to_string();
    let direction = match direction_str.as_str() {
        "in" => MessageDirection::In,
        "out" => MessageDirection::Out,
        "all" => MessageDirection::All,
        _ => {
            return err_json(
                StatusCode::BAD_REQUEST,
                &format!(
                    "Invalid direction: '{}'. Valid values: in, out, all",
                    direction_str
                ),
            );
        }
    };

    if let Some(ref status) = query.status {
        if !VALID_STATUSES.contains(&status.as_str()) {
            return err_json(
                StatusCode::BAD_REQUEST,
                &format!(
                    "Invalid status: '{}'. Valid values: queued, leased, acknowledged, dead_letter",
                    status
                ),
            );
        }
    }
    if let Some(ref after) = query.after {
        if let Err((status, msg)) = validate_datetime(after, "after") {
            return err_json(status, &msg);
        }
    }
    if let Some(ref before) = query.before {
        if let Err((status, msg)) = validate_datetime(before, "before") {
            return err_json(status, &msg);
        }
    }

    let db_path = state.db_path.clone();
    let filters = MessageFilters {
        correlation_id: None,
        from_instance: None,
        to_instance: None,
        message_type: query.message_type,
        status: query.status,
        after: query.after,
        before: query.before,
        limit,
        offset,
    };

    let result =
        tokio::task::spawn_blocking(move || -> Result<serde_json::Value, (StatusCode, String)> {
            let registry = Registry::open(&db_path)
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}")))?;

            // Verify instance exists
            registry
                .get_instance_by_name(&name)
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}")))?
                .ok_or_else(|| (StatusCode::NOT_FOUND, format!("No instance named '{name}'")))?;

            let (messages, total) = registry
                .list_messages_for_instance(&name, direction, &filters)
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}")))?;

            let items: Vec<serde_json::Value> =
                messages.iter().map(sanitize_message_for_response).collect();

            Ok(serde_json::json!({
                "instance": name,
                "direction": direction_str,
                "items": items,
                "total": total,
                "limit": filters.limit,
                "offset": filters.offset,
            }))
        })
        .await;

    match result {
        Ok(Ok(value)) => ok_json(value),
        Ok(Err((status, msg))) => err_json(status, &msg),
        Err(e) => err_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("Task join error: {e}"),
        ),
    }
}

// ── GET /api/messages/dead-letter ─────────────────────────────

pub async fn handle_list_dead_letter(
    State(state): State<CpState>,
    Query(query): Query<DeadLetterQuery>,
) -> ApiResponse {
    let limit = query.limit.unwrap_or(20);
    if !(1..=1000).contains(&limit) {
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
    if let Some(ref after) = query.after {
        if let Err((status, msg)) = validate_datetime(after, "after") {
            return err_json(status, &msg);
        }
    }
    if let Some(ref before) = query.before {
        if let Err((status, msg)) = validate_datetime(before, "before") {
            return err_json(status, &msg);
        }
    }

    let db_path = state.db_path.clone();
    let filters = MessageFilters {
        correlation_id: None,
        from_instance: query.from,
        to_instance: query.to,
        message_type: None,
        status: None,
        after: query.after,
        before: query.before,
        limit,
        offset,
    };

    let result =
        tokio::task::spawn_blocking(move || -> Result<serde_json::Value, (StatusCode, String)> {
            let registry = Registry::open(&db_path)
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}")))?;

            let (messages, total) = registry
                .list_dead_letter_messages(&filters)
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}")))?;

            let items: Vec<serde_json::Value> =
                messages.iter().map(sanitize_message_for_response).collect();

            Ok(serde_json::json!({
                "items": items,
                "total": total,
                "limit": filters.limit,
                "offset": filters.offset,
            }))
        })
        .await;

    match result {
        Ok(Ok(value)) => ok_json(value),
        Ok(Err((status, msg))) => err_json(status, &msg),
        Err(e) => err_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("Task join error: {e}"),
        ),
    }
}

// ── POST /api/messages/:id/replay ─────────────────────────────

pub async fn handle_replay_message(
    State(state): State<CpState>,
    AxumPath(id): AxumPath<String>,
) -> ApiResponse {
    let db_path = state.db_path.clone();
    let result =
        tokio::task::spawn_blocking(move || -> Result<serde_json::Value, (StatusCode, String)> {
            let registry = Registry::open(&db_path)
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}")))?;

            match registry.replay_message(&id) {
                Ok(msg) => Ok(sanitize_message_for_response(&msg)),
                Err(e) => {
                    let msg = format!("{e:#}");
                    if msg.contains("not found") || msg.contains("Message not found") {
                        Err((StatusCode::NOT_FOUND, format!("No message with id '{id}'")))
                    } else if msg.contains("expected 'dead_letter'") {
                        Err((
                            StatusCode::CONFLICT,
                            format!("Message '{id}' is not in dead_letter status"),
                        ))
                    } else {
                        Err((StatusCode::INTERNAL_SERVER_ERROR, msg))
                    }
                }
            }
        })
        .await;

    match result {
        Ok(Ok(value)) => ok_json(value),
        Ok(Err((status, msg))) => err_json(status, &msg),
        Err(e) => err_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("Task join error: {e}"),
        ),
    }
}

// ── Delivery worker ──────────────────────────────────────────────

fn delivery_tick(db_path: &Path) -> anyhow::Result<()> {
    let registry = Registry::open(db_path)?;

    // Process expired leases
    let expired_leases = registry.get_expired_leases()?;
    for msg in expired_leases {
        registry.append_message_event(&msg.id, "lease_expired", None)?;
        if msg.retry_count + 1 >= msg.max_retries {
            registry.dead_letter_message(&msg.id, "max retries exceeded")?;
            tracing::info!("Message {} dead-lettered (max retries)", msg.id);
        } else {
            registry.retry_message(&msg.id)?;
            registry.append_message_event(&msg.id, "retry_scheduled", None)?;
            tracing::debug!(
                "Message {} retried (attempt {})",
                msg.id,
                msg.retry_count + 1
            );
        }
    }

    // Process TTL-expired messages
    let ttl_expired = registry.get_ttl_expired_messages()?;
    for msg in ttl_expired {
        registry.dead_letter_message(&msg.id, "TTL expired")?;
        tracing::info!("Message {} dead-lettered (TTL expired)", msg.id);
    }

    // Process auto-starts
    let autostart_needed = registry.get_instances_needing_autostart()?;
    for (_msg, instance_name) in autostart_needed {
        tracing::info!("Auto-starting instance '{instance_name}' for pending messages");
        if let Err(e) = lifecycle::start_instance(&registry, &instance_name) {
            tracing::warn!("Auto-start failed for '{instance_name}': {e}");
        }
    }

    Ok(())
}
