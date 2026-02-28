use serde_json::Value;

pub const MASKED: &str = "***MASKED***";

/// All known scalar secret paths (segments from root).
const SCALAR_SECRET_PATHS: &[&[&str]] = &[
    &["api_key"],
    &["channels_config", "telegram", "bot_token"],
    &["channels_config", "discord", "bot_token"],
    &["channels_config", "slack", "bot_token"],
    &["channels_config", "slack", "app_token"],
    &["channels_config", "webhook", "secret"],
    &["channels_config", "matrix", "access_token"],
    &["channels_config", "whatsapp", "access_token"],
    &["channels_config", "whatsapp", "verify_token"],
    &["channels_config", "whatsapp", "app_secret"],
    &["channels_config", "irc", "server_password"],
    &["channels_config", "irc", "nickserv_password"],
    &["channels_config", "irc", "sasl_password"],
    &["channels_config", "email", "password"],
    &["composio", "api_key"],
    &["tunnel", "ngrok", "auth_token"],
    &["tunnel", "cloudflare", "token"],
];

/// Replace known secret fields in a serialized config JSON with `"***MASKED***"`.
///
/// Walks enumerated paths corresponding to every secret field in `Config` and
/// its nested channel/tunnel/composio/model_routes structs. Null or missing
/// values are left as-is (they are not a leak).
pub fn mask_config_secrets(value: &mut Value) {
    // Top-level scalar secrets
    mask_path(value, &["api_key"]);

    // Channels
    mask_path(value, &["channels_config", "telegram", "bot_token"]);
    mask_path(value, &["channels_config", "discord", "bot_token"]);
    mask_path(value, &["channels_config", "slack", "bot_token"]);
    mask_path(value, &["channels_config", "slack", "app_token"]);
    mask_path(value, &["channels_config", "webhook", "secret"]);
    mask_path(value, &["channels_config", "matrix", "access_token"]);
    mask_path(value, &["channels_config", "whatsapp", "access_token"]);
    mask_path(value, &["channels_config", "whatsapp", "verify_token"]);
    mask_path(value, &["channels_config", "whatsapp", "app_secret"]);
    mask_path(value, &["channels_config", "irc", "server_password"]);
    mask_path(value, &["channels_config", "irc", "nickserv_password"]);
    mask_path(value, &["channels_config", "irc", "sasl_password"]);
    mask_path(value, &["channels_config", "email", "password"]);

    // Gateway paired tokens (array of strings)
    mask_array_elements(value, &["gateway", "paired_tokens"]);

    // Composio
    mask_path(value, &["composio", "api_key"]);

    // Tunnel
    mask_path(value, &["tunnel", "ngrok", "auth_token"]);
    mask_path(value, &["tunnel", "cloudflare", "token"]);

    // Model routes (array of objects, each may have api_key)
    if let Some(routes) = value.pointer_mut("/model_routes") {
        if let Some(arr) = routes.as_array_mut() {
            for route in arr.iter_mut() {
                mask_path(route, &["api_key"]);
            }
        }
    }
}

/// Walk a dotted path into a JSON value and replace the leaf with MASKED,
/// but only if the leaf is a non-null string.
fn mask_path(value: &mut Value, segments: &[&str]) {
    if segments.is_empty() {
        return;
    }

    let mut current = value;
    // Navigate to the parent of the final segment
    for &seg in &segments[..segments.len() - 1] {
        match current.get_mut(seg) {
            Some(child) if child.is_object() => current = child,
            _ => return, // Path doesn't exist or isn't an object -- nothing to mask
        }
    }

    let leaf_key = segments[segments.len() - 1];
    if let Some(leaf) = current.get(leaf_key) {
        if leaf.is_string() {
            current[leaf_key] = Value::String(MASKED.to_string());
        }
        // null / missing: leave as-is
    }
}

/// Mask every string element in an array at the given path.
fn mask_array_elements(value: &mut Value, segments: &[&str]) {
    if segments.is_empty() {
        return;
    }

    let mut current = value as &mut Value;
    for &seg in &segments[..segments.len() - 1] {
        match current.get_mut(seg) {
            Some(child) if child.is_object() => current = child,
            _ => return,
        }
    }

    let leaf_key = segments[segments.len() - 1];
    if let Some(arr_val) = current.get_mut(leaf_key) {
        if let Some(arr) = arr_val.as_array_mut() {
            for elem in arr.iter_mut() {
                if elem.is_string() {
                    *elem = Value::String(MASKED.to_string());
                }
            }
        }
    }
}

// ── Config diff structs ────────────────────────────────────────────

/// A single field change between two config JSON values.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ConfigChange {
    pub path: String,
    pub from: Value,
    pub to: Value,
}

/// Result of diffing two masked config JSON values.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ConfigDiff {
    pub changes: Vec<ConfigChange>,
    pub added: Vec<String>,
    pub removed: Vec<String>,
    pub unchanged_count: usize,
}

// ── Secret detection and preservation ─────────────────────────────

/// Read a value at a dotted path of segments (immutable).
fn get_at_path<'a>(value: &'a Value, segments: &[&str]) -> Option<&'a Value> {
    let mut current = value;
    for &seg in segments {
        current = current.get(seg)?;
    }
    Some(current)
}

/// Set a value at a dotted path of segments, creating intermediate objects.
fn set_at_path(value: &mut Value, segments: &[&str], new_val: Value) {
    if segments.is_empty() {
        return;
    }
    let mut current = value;
    for &seg in &segments[..segments.len() - 1] {
        if !current.get(seg).map_or(false, Value::is_object) {
            current[seg] = Value::Object(serde_json::Map::new());
        }
        current = current.get_mut(seg).unwrap();
    }
    current[segments[segments.len() - 1]] = new_val;
}

/// Dotted path string from segments.
fn dotted(segments: &[&str]) -> String {
    segments.join(".")
}

/// Detect non-null secret values in a config JSON.
/// Returns dotted paths of secret fields that contain real (non-masked, non-null) values.
pub fn detect_secret_fields(value: &Value) -> Vec<String> {
    let mut paths = Vec::new();

    // Scalar secret paths
    for segs in SCALAR_SECRET_PATHS {
        if let Some(leaf) = get_at_path(value, segs) {
            if let Some(s) = leaf.as_str() {
                if s != MASKED {
                    paths.push(dotted(segs));
                }
            }
        }
    }

    // gateway.paired_tokens (array of strings)
    if let Some(arr) = get_at_path(value, &["gateway", "paired_tokens"]).and_then(Value::as_array) {
        for (i, elem) in arr.iter().enumerate() {
            if let Some(s) = elem.as_str() {
                if s != MASKED {
                    paths.push(format!("gateway.paired_tokens[{i}]"));
                }
            }
        }
    }

    // model_routes[*].api_key
    if let Some(routes) = value.get("model_routes").and_then(Value::as_array) {
        for (i, route) in routes.iter().enumerate() {
            if let Some(s) = route.get("api_key").and_then(Value::as_str) {
                if s != MASKED {
                    paths.push(format!("model_routes[{i}].api_key"));
                }
            }
        }
    }

    paths
}

/// Preserve masked sentinel values by copying real values from `current` config.
///
/// For each secret path where `incoming` has `"***MASKED***"`, copies the value from `current`.
/// Returns `Ok(Vec<String>)` with paths that have genuinely NEW secret values (for blocking check).
/// Returns `Err((path, message))` if a sentinel exists on a path with no current secret to preserve.
pub fn preserve_masked_secrets(
    incoming: &mut Value,
    current: &Value,
) -> Result<Vec<String>, (String, String)> {
    let mut new_secret_paths = Vec::new();

    // Scalar secret paths
    for segs in SCALAR_SECRET_PATHS {
        let path_str = dotted(segs);
        if let Some(incoming_val) = get_at_path(incoming, segs) {
            if let Some(s) = incoming_val.as_str() {
                if s == MASKED {
                    // Sentinel -- try to preserve from current
                    match get_at_path(current, segs).and_then(Value::as_str) {
                        Some(current_str) if current_str != MASKED => {
                            set_at_path(incoming, segs, Value::String(current_str.to_string()));
                        }
                        _ => {
                            return Err((
                                path_str.clone(),
                                format!("Cannot preserve masked value for '{path_str}': no existing secret to preserve"),
                            ));
                        }
                    }
                } else {
                    // Non-masked, non-null string -- this is a genuinely new secret
                    new_secret_paths.push(path_str);
                }
            }
            // null values: not a secret, nothing to do
        }
    }

    // gateway.paired_tokens (array of strings)
    if let Some(arr) =
        get_at_path(incoming, &["gateway", "paired_tokens"]).and_then(Value::as_array)
    {
        let all_masked = !arr.is_empty() && arr.iter().all(|e| e.as_str() == Some(MASKED));
        if all_masked {
            // Copy entire array from current
            match get_at_path(current, &["gateway", "paired_tokens"]).and_then(Value::as_array) {
                Some(current_arr) if !current_arr.is_empty() => {
                    set_at_path(
                        incoming,
                        &["gateway", "paired_tokens"],
                        Value::Array(current_arr.clone()),
                    );
                }
                _ => {
                    return Err((
                        "gateway.paired_tokens".to_string(),
                        "Cannot preserve masked value for 'gateway.paired_tokens': no existing secret to preserve".to_string(),
                    ));
                }
            }
        } else {
            // Check for individual new secrets
            for (i, elem) in arr.iter().enumerate() {
                if let Some(s) = elem.as_str() {
                    if s != MASKED {
                        new_secret_paths.push(format!("gateway.paired_tokens[{i}]"));
                    }
                }
            }
        }
    }

    // model_routes[*].api_key
    if let Some(routes) = incoming.get("model_routes").and_then(Value::as_array) {
        let routes_snapshot: Vec<Value> = routes.clone();
        let current_routes = current.get("model_routes").and_then(Value::as_array);

        for (i, route) in routes_snapshot.iter().enumerate() {
            if let Some(s) = route.get("api_key").and_then(Value::as_str) {
                if s == MASKED {
                    // Try to preserve from current route at same index
                    let current_key = current_routes
                        .and_then(|r| r.get(i))
                        .and_then(|r| r.get("api_key"))
                        .and_then(Value::as_str);
                    match current_key {
                        Some(k) if k != MASKED => {
                            if let Some(route_mut) = incoming
                                .get_mut("model_routes")
                                .and_then(Value::as_array_mut)
                                .and_then(|arr| arr.get_mut(i))
                            {
                                route_mut["api_key"] = Value::String(k.to_string());
                            }
                        }
                        _ => {
                            let path = format!("model_routes[{i}].api_key");
                            return Err((
                                path.clone(),
                                format!("Cannot preserve masked value for '{path}': no existing secret to preserve"),
                            ));
                        }
                    }
                } else {
                    new_secret_paths.push(format!("model_routes[{i}].api_key"));
                }
            }
        }
    }

    Ok(new_secret_paths)
}

/// Compute a field-by-field diff between two JSON values.
/// Both inputs should already be masked.
pub fn diff_json(old: &Value, new: &Value) -> ConfigDiff {
    let mut changes = Vec::new();
    let mut added = Vec::new();
    let mut removed = Vec::new();
    let mut unchanged_count: usize = 0;

    diff_recursive(
        old,
        new,
        "",
        &mut changes,
        &mut added,
        &mut removed,
        &mut unchanged_count,
    );

    ConfigDiff {
        changes,
        added,
        removed,
        unchanged_count,
    }
}

fn diff_recursive(
    old: &Value,
    new: &Value,
    prefix: &str,
    changes: &mut Vec<ConfigChange>,
    added: &mut Vec<String>,
    removed: &mut Vec<String>,
    unchanged: &mut usize,
) {
    match (old, new) {
        (Value::Object(old_map), Value::Object(new_map)) => {
            // Keys in both
            for (key, old_val) in old_map {
                let path = if prefix.is_empty() {
                    key.clone()
                } else {
                    format!("{prefix}.{key}")
                };
                match new_map.get(key) {
                    Some(new_val) => {
                        diff_recursive(old_val, new_val, &path, changes, added, removed, unchanged);
                    }
                    None => {
                        removed.push(path);
                    }
                }
            }
            // Keys only in new
            for key in new_map.keys() {
                if !old_map.contains_key(key) {
                    let path = if prefix.is_empty() {
                        key.clone()
                    } else {
                        format!("{prefix}.{key}")
                    };
                    added.push(path);
                }
            }
        }
        (Value::Array(old_arr), Value::Array(new_arr)) => {
            if old_arr.len() == new_arr.len() {
                for (i, (o, n)) in old_arr.iter().zip(new_arr.iter()).enumerate() {
                    let path = format!("{prefix}[{i}]");
                    diff_recursive(o, n, &path, changes, added, removed, unchanged);
                }
            } else {
                // Different lengths: treat as an opaque change
                if old != new {
                    changes.push(ConfigChange {
                        path: prefix.to_string(),
                        from: old.clone(),
                        to: new.clone(),
                    });
                } else {
                    *unchanged += 1;
                }
            }
        }
        _ => {
            // Scalar comparison
            if old == new {
                *unchanged += 1;
            } else {
                changes.push(ConfigChange {
                    path: prefix.to_string(),
                    from: old.clone(),
                    to: new.clone(),
                });
            }
        }
    }
}

// ── Payload secret redaction (Phase 10.1) ───────────────────────

const REDACTED: &str = "***REDACTED***";

/// Keys whose string values should be redacted in message payloads.
const SECRET_KEY_PATTERNS: &[&str] = &[
    "api_key",
    "apikey",
    "token",
    "secret",
    "password",
    "authorization",
    "auth_token",
    "access_token",
    "private_key",
    "credentials",
];

/// Scan a JSON payload for known secret patterns and replace string values with `***REDACTED***`.
/// Checks JSON object keys (case-insensitive) against known secret-like patterns.
pub fn redact_payload_secrets(value: &mut Value) {
    match value {
        Value::Object(map) => {
            let keys: Vec<String> = map.keys().cloned().collect();
            for key in keys {
                let key_lower = key.to_lowercase();
                let is_secret = SECRET_KEY_PATTERNS
                    .iter()
                    .any(|pattern| key_lower.contains(pattern));
                if is_secret {
                    if let Some(val) = map.get_mut(&key) {
                        if val.is_string() {
                            *val = Value::String(REDACTED.to_string());
                        }
                    }
                } else if let Some(val) = map.get_mut(&key) {
                    redact_payload_secrets(val);
                }
            }
        }
        Value::Array(arr) => {
            for item in arr.iter_mut() {
                redact_payload_secrets(item);
            }
        }
        _ => {}
    }
}

/// Test helper: recursively check whether any string value in `json` exactly
/// matches one of the provided `secrets`. Returns true if a leak is found.
#[cfg(test)]
pub fn contains_raw_secrets(json: &Value, secrets: &[&str]) -> bool {
    match json {
        Value::String(s) => secrets.contains(&s.as_str()),
        Value::Array(arr) => arr.iter().any(|v| contains_raw_secrets(v, secrets)),
        Value::Object(map) => map.values().any(|v| contains_raw_secrets(v, secrets)),
        _ => false,
    }
}

/// Collect all dotted key paths to leaf nodes in a JSON value.
/// Used to diff raw TOML keys against typed Config keys.
pub fn collect_key_paths(value: &Value, prefix: &str) -> std::collections::HashSet<String> {
    let mut paths = std::collections::HashSet::new();
    match value {
        Value::Object(map) => {
            for (k, v) in map {
                let path = if prefix.is_empty() {
                    k.clone()
                } else {
                    format!("{prefix}.{k}")
                };
                match v {
                    Value::Object(_) => {
                        paths.extend(collect_key_paths(v, &path));
                    }
                    Value::Array(arr) => {
                        // For arrays of objects, recurse into each element
                        let mut has_object = false;
                        for (i, elem) in arr.iter().enumerate() {
                            if elem.is_object() {
                                has_object = true;
                                let elem_prefix = format!("{path}[{i}]");
                                paths.extend(collect_key_paths(elem, &elem_prefix));
                            }
                        }
                        if !has_object {
                            // Array of scalars -- the path itself is a leaf
                            paths.insert(path);
                        }
                    }
                    _ => {
                        paths.insert(path);
                    }
                }
            }
        }
        _ => {
            if !prefix.is_empty() {
                paths.insert(prefix.to_string());
            }
        }
    }
    paths
}

// ── PATCH validation constants ────────────────────────────────

/// Every valid leaf path in Config, including paths inside Option<T> structs.
/// Used by validate_patch_paths to reject unknown keys in PATCH payloads.
pub const VALID_CONFIG_PATHS: &[&str] = &[
    // Top-level
    "api_key",
    "default_provider",
    "default_model",
    "default_temperature",
    // Observability
    "observability.backend",
    "observability.otel_endpoint",
    "observability.otel_service_name",
    // Autonomy
    "autonomy.level",
    "autonomy.workspace_only",
    "autonomy.allowed_commands",
    "autonomy.forbidden_paths",
    "autonomy.max_actions_per_hour",
    "autonomy.max_cost_per_day_cents",
    "autonomy.require_approval_for_medium_risk",
    "autonomy.block_high_risk_commands",
    // Approval Policy
    "approval_policy.enabled",
    "approval_policy.timeout_secs",
    "approval_policy.medium_risk_approver",
    "approval_policy.high_risk_approver",
    "approval_policy.origin_mode",
    "approval_policy.admin_users",
    // Runtime
    "runtime.kind",
    "runtime.docker.image",
    "runtime.docker.network",
    "runtime.docker.memory_limit_mb",
    "runtime.docker.cpu_limit",
    "runtime.docker.read_only_rootfs",
    "runtime.docker.mount_workspace",
    "runtime.docker.allowed_workspace_roots",
    // Reliability
    "reliability.provider_retries",
    "reliability.provider_backoff_ms",
    "reliability.fallback_providers",
    "reliability.channel_initial_backoff_secs",
    "reliability.channel_max_backoff_secs",
    "reliability.scheduler_poll_secs",
    "reliability.scheduler_retries",
    // Heartbeat
    "heartbeat.enabled",
    "heartbeat.interval_minutes",
    // Gateway
    "gateway.port",
    "gateway.host",
    "gateway.require_pairing",
    "gateway.allow_public_bind",
    "gateway.paired_tokens",
    "gateway.pair_rate_limit_per_minute",
    "gateway.webhook_rate_limit_per_minute",
    "gateway.idempotency_ttl_secs",
    // Memory
    "memory.backend",
    "memory.auto_save",
    "memory.hygiene_enabled",
    "memory.archive_after_days",
    "memory.purge_after_days",
    "memory.conversation_retention_days",
    "memory.embedding_provider",
    "memory.embedding_model",
    "memory.embedding_dimensions",
    "memory.vector_weight",
    "memory.keyword_weight",
    "memory.embedding_cache_size",
    "memory.chunk_max_tokens",
    // Tunnel
    "tunnel.provider",
    "tunnel.cloudflare.token",
    "tunnel.tailscale.funnel",
    "tunnel.tailscale.hostname",
    "tunnel.ngrok.auth_token",
    "tunnel.ngrok.domain",
    "tunnel.custom.start_command",
    "tunnel.custom.health_url",
    "tunnel.custom.url_pattern",
    // Composio
    "composio.enabled",
    "composio.api_key",
    "composio.entity_id",
    // Secrets
    "secrets.encrypt",
    // Browser
    "browser.enabled",
    "browser.allowed_domains",
    "browser.session_name",
    // Identity
    "identity.format",
    "identity.aieos_path",
    "identity.aieos_inline",
    // Channels
    "channels_config.cli",
    // Telegram
    "channels_config.telegram.bot_token",
    "channels_config.telegram.allowed_users",
    "channels_config.telegram.stt_endpoint",
    "channels_config.telegram.flows_enabled",
    "channels_config.telegram.flow_policy.agent_authoring_enabled",
    "channels_config.telegram.flow_policy.denied_step_kinds",
    "channels_config.telegram.flow_policy.max_steps",
    "channels_config.telegram.flow_policy.max_agent_flows",
    "channels_config.telegram.flow_policy.require_handoff_on_keyboard",
    "channels_config.telegram.flow_policy.auto_approve",
    "channels_config.telegram.flow_policy.auto_approve_max_steps",
    "channels_config.telegram.flow_policy.denied_text_patterns",
    // Discord
    "channels_config.discord.bot_token",
    "channels_config.discord.guild_id",
    "channels_config.discord.allowed_users",
    // Slack
    "channels_config.slack.bot_token",
    "channels_config.slack.app_token",
    "channels_config.slack.channel_id",
    "channels_config.slack.allowed_users",
    // Webhook
    "channels_config.webhook.port",
    "channels_config.webhook.secret",
    // iMessage
    "channels_config.imessage.allowed_contacts",
    // Matrix
    "channels_config.matrix.homeserver",
    "channels_config.matrix.access_token",
    "channels_config.matrix.room_id",
    "channels_config.matrix.allowed_users",
    // WhatsApp
    "channels_config.whatsapp.access_token",
    "channels_config.whatsapp.phone_number_id",
    "channels_config.whatsapp.verify_token",
    "channels_config.whatsapp.app_secret",
    "channels_config.whatsapp.allowed_numbers",
    // IRC
    "channels_config.irc.server",
    "channels_config.irc.port",
    "channels_config.irc.nickname",
    "channels_config.irc.username",
    "channels_config.irc.channels",
    "channels_config.irc.allowed_users",
    "channels_config.irc.server_password",
    "channels_config.irc.nickserv_password",
    "channels_config.irc.sasl_password",
    "channels_config.irc.verify_tls",
    // Email
    "channels_config.email.imap_host",
    "channels_config.email.imap_port",
    "channels_config.email.imap_folder",
    "channels_config.email.smtp_host",
    "channels_config.email.smtp_port",
    "channels_config.email.smtp_tls",
    "channels_config.email.username",
    "channels_config.email.password",
    "channels_config.email.from_address",
    "channels_config.email.poll_interval_secs",
    "channels_config.email.allowed_senders",
    // CP Relay
    "channels_config.cp_relay.cp_url",
    // Model routes (array-of-tables, validated separately)
    "model_routes",
];

/// Valid keys within a model_routes element.
pub const MODEL_ROUTE_ELEMENT_KEYS: &[&str] = &["hint", "provider", "model", "api_key"];

/// Paths where null is allowed (Option<T> fields and section-level Option<ChannelConfig>).
pub const NULLABLE_PATHS: &[&str] = &[
    // Top-level Option<String>
    "api_key",
    "default_provider",
    "default_model",
    // Channel sections (Option<ChannelConfig>)
    "channels_config.telegram",
    "channels_config.discord",
    "channels_config.slack",
    "channels_config.webhook",
    "channels_config.matrix",
    "channels_config.whatsapp",
    "channels_config.email",
    "channels_config.irc",
    "channels_config.imessage",
    "channels_config.cp_relay",
    // Tunnel sub-configs (Option<TunnelSubConfig>)
    "tunnel.cloudflare",
    "tunnel.tailscale",
    "tunnel.ngrok",
    "tunnel.custom",
    // Option<String> leaves within channel/tunnel structs
    "channels_config.telegram.stt_endpoint",
    "channels_config.discord.guild_id",
    "channels_config.slack.app_token",
    "channels_config.slack.channel_id",
    "channels_config.webhook.secret",
    "channels_config.whatsapp.app_secret",
    "channels_config.irc.username",
    "channels_config.irc.server_password",
    "channels_config.irc.nickserv_password",
    "channels_config.irc.sasl_password",
    "channels_config.irc.verify_tls",
    "tunnel.ngrok.domain",
    "tunnel.tailscale.hostname",
    "tunnel.custom.health_url",
    "tunnel.custom.url_pattern",
    "composio.api_key",
    "identity.aieos_path",
    "identity.aieos_inline",
    "browser.session_name",
    "observability.otel_endpoint",
    "observability.otel_service_name",
    // Docker optional fields
    "runtime.docker.memory_limit_mb",
    "runtime.docker.cpu_limit",
];

/// The full list of secret paths exposed in the GET response manifest.
pub const SECRET_PATHS_MANIFEST: &[&str] = &[
    "api_key",
    "channels_config.telegram.bot_token",
    "channels_config.discord.bot_token",
    "channels_config.slack.bot_token",
    "channels_config.slack.app_token",
    "channels_config.webhook.secret",
    "channels_config.matrix.access_token",
    "channels_config.whatsapp.access_token",
    "channels_config.whatsapp.verify_token",
    "channels_config.whatsapp.app_secret",
    "channels_config.irc.server_password",
    "channels_config.irc.nickserv_password",
    "channels_config.irc.sasl_password",
    "channels_config.email.password",
    "composio.api_key",
    "tunnel.ngrok.auth_token",
    "tunnel.cloudflare.token",
    "gateway.paired_tokens",
    "model_routes[*].api_key",
];

// ── PATCH validation functions ───────────────────────────────

/// Reject dotted literal keys in a patch. Keys containing '.' must be
/// expressed as nested objects, not flat dotted strings.
pub fn reject_dotted_keys(patch: &Value, prefix: &str) -> Result<(), String> {
    if let Some(obj) = patch.as_object() {
        for (key, val) in obj {
            if key.contains('.') {
                let full = if prefix.is_empty() {
                    key.clone()
                } else {
                    format!("{prefix}.{key}")
                };
                return Err(format!(
                    "Dotted key '{key}' at '{full}' is not allowed. Use nested objects instead."
                ));
            }
            if val.is_object() {
                let child_prefix = if prefix.is_empty() {
                    key.clone()
                } else {
                    format!("{prefix}.{key}")
                };
                reject_dotted_keys(val, &child_prefix)?;
            }
        }
    }
    Ok(())
}

/// Validate that all leaf paths in a patch exist in VALID_CONFIG_PATHS.
/// For model_routes array elements, checks keys against MODEL_ROUTE_ELEMENT_KEYS.
pub fn validate_patch_paths(patch: &Value) -> Result<(), Vec<String>> {
    let mut invalid = Vec::new();
    validate_paths_recursive(patch, "", &mut invalid);
    if invalid.is_empty() {
        Ok(())
    } else {
        Err(invalid)
    }
}

fn validate_paths_recursive(value: &Value, prefix: &str, invalid: &mut Vec<String>) {
    if let Some(obj) = value.as_object() {
        for (key, val) in obj {
            let path = if prefix.is_empty() {
                key.clone()
            } else {
                format!("{prefix}.{key}")
            };

            // Special case: model_routes is an array of objects
            if path == "model_routes" {
                if let Some(arr) = val.as_array() {
                    for (i, elem) in arr.iter().enumerate() {
                        if let Some(elem_obj) = elem.as_object() {
                            for elem_key in elem_obj.keys() {
                                if !MODEL_ROUTE_ELEMENT_KEYS.contains(&elem_key.as_str()) {
                                    invalid.push(format!("model_routes[{i}].{elem_key}"));
                                }
                            }
                        }
                    }
                }
                // model_routes itself is valid (it's in VALID_CONFIG_PATHS)
                continue;
            }

            if val.is_object() {
                // Check if this intermediate path is a valid section-level nullable
                // (e.g. channels_config.telegram set to null for removal)
                // Recurse into nested objects
                validate_paths_recursive(val, &path, invalid);
            } else {
                // Leaf node: check if the path is valid
                if !VALID_CONFIG_PATHS.contains(&path.as_str()) {
                    // Also allow section-level paths (e.g. "channels_config.telegram")
                    // that are valid as nullable section removals
                    let is_valid_section = VALID_CONFIG_PATHS.iter().any(|p| {
                        // Must start with path + "." to prevent partial matches
                        p.starts_with(&path) && p.as_bytes().get(path.len()) == Some(&b'.')
                    });
                    if !is_valid_section {
                        invalid.push(path);
                    }
                }
            }
        }
    }
}

/// Validate that null values only target Option<T> paths.
pub fn validate_null_targets(patch: &Value) -> Result<(), Vec<String>> {
    let mut invalid = Vec::new();
    collect_null_paths(patch, "", &mut invalid);
    if invalid.is_empty() {
        Ok(())
    } else {
        Err(invalid)
    }
}

fn collect_null_paths(value: &Value, prefix: &str, invalid: &mut Vec<String>) {
    if let Some(obj) = value.as_object() {
        for (key, val) in obj {
            let path = if prefix.is_empty() {
                key.clone()
            } else {
                format!("{prefix}.{key}")
            };

            if val.is_null() {
                if !NULLABLE_PATHS.contains(&path.as_str()) {
                    invalid.push(path);
                }
            } else if val.is_object() {
                collect_null_paths(val, &path, invalid);
            }
        }
    }
}

/// Reject any string value equal to the MASKED sentinel anywhere in the patch.
pub fn reject_masked_sentinels(patch: &Value) -> Result<(), String> {
    reject_masked_recursive(patch, "")
}

fn reject_masked_recursive(value: &Value, prefix: &str) -> Result<(), String> {
    match value {
        Value::String(s) if s == MASKED => {
            Err(format!(
                "Masked sentinel '***MASKED***' at '{prefix}' is not allowed in PATCH. \
                 Omit unchanged secrets from the patch entirely."
            ))
        }
        Value::Object(obj) => {
            for (key, val) in obj {
                let path = if prefix.is_empty() {
                    key.clone()
                } else {
                    format!("{prefix}.{key}")
                };
                reject_masked_recursive(val, &path)?;
            }
            Ok(())
        }
        Value::Array(arr) => {
            for (i, elem) in arr.iter().enumerate() {
                let path = format!("{prefix}[{i}]");
                reject_masked_recursive(elem, &path)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

// ── Secret write detection for PATCH ─────────────────────────

/// Check if a dotted path is a known secret path.
pub fn is_secret_path(path: &str) -> bool {
    SCALAR_SECRET_PATHS
        .iter()
        .any(|segs| segs.join(".") == path)
        || path == "gateway.paired_tokens"
}

/// Collect all secret paths present in a PATCH payload.
/// Covers scalar secrets, gateway.paired_tokens, and model_routes[*].api_key.
pub fn collect_secret_writes(patch: &Value) -> Vec<String> {
    let mut paths = Vec::new();
    collect_secrets_recursive(patch, "", &mut paths);

    // model_routes array elements
    if let Some(routes) = patch.get("model_routes").and_then(Value::as_array) {
        for (i, route) in routes.iter().enumerate() {
            if route.get("api_key").is_some() {
                paths.push(format!("model_routes[{i}].api_key"));
            }
        }
    }

    paths
}

fn collect_secrets_recursive(value: &Value, prefix: &str, paths: &mut Vec<String>) {
    if let Some(obj) = value.as_object() {
        for (key, val) in obj {
            let path = if prefix.is_empty() {
                key.clone()
            } else {
                format!("{prefix}.{key}")
            };

            // Skip model_routes -- handled separately by caller
            if path == "model_routes" {
                continue;
            }

            if is_secret_path(&path) {
                paths.push(path);
            } else if val.is_object() {
                collect_secrets_recursive(val, &path, paths);
            }
        }
    }
}

// ── TOML patch application ───────────────────────────────────

/// Apply a JSON patch object to a TOML document string using toml_edit
/// for targeted writes that preserve formatting of untouched sections.
pub fn apply_json_patch_to_toml(
    toml_text: &str,
    patch: &Value,
) -> Result<String, String> {
    let mut doc: toml_edit::DocumentMut = toml_text
        .parse()
        .map_err(|e| format!("Failed to parse TOML: {e}"))?;

    apply_patch_to_table(doc.as_table_mut(), patch)?;
    Ok(doc.to_string())
}

fn apply_patch_to_table(
    table: &mut toml_edit::Table,
    patch: &Value,
) -> Result<(), String> {
    let obj = patch
        .as_object()
        .ok_or_else(|| "Patch must be a JSON object".to_string())?;

    for (key, val) in obj {
        match val {
            Value::Null => {
                // Remove key/table from document
                table.remove(key);
            }
            Value::Object(_) => {
                // Special case: model_routes is an array-of-tables but appears
                // as a JSON array, not a JSON object. Skip recursion for it here;
                // it's handled by the Array branch if the value were an array.
                // For genuine nested objects, recurse into subtable.
                if !table.contains_key(key) {
                    table.insert(key, toml_edit::Item::Table(toml_edit::Table::new()));
                }
                match table.get_mut(key) {
                    Some(toml_edit::Item::Table(sub)) => {
                        apply_patch_to_table(sub, val)?;
                    }
                    Some(item) => {
                        // Key exists but isn't a table -- replace with table
                        *item = toml_edit::Item::Table(toml_edit::Table::new());
                        if let toml_edit::Item::Table(sub) = item {
                            apply_patch_to_table(sub, val)?;
                        }
                    }
                    None => unreachable!(), // we just inserted it
                }
            }
            Value::Array(arr) => {
                // Replace entire array at this key
                let toml_arr = json_array_to_toml(arr)?;
                table.insert(key, toml_arr);
            }
            _ => {
                // Scalar leaf value
                let toml_val = json_scalar_to_toml_value(val)?;
                table.insert(key, toml_edit::Item::Value(toml_val));
            }
        }
    }
    Ok(())
}

fn json_scalar_to_toml_value(v: &Value) -> Result<toml_edit::Value, String> {
    match v {
        Value::Bool(b) => Ok(toml_edit::Value::from(*b)),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(toml_edit::Value::from(i))
            } else if let Some(f) = n.as_f64() {
                Ok(toml_edit::Value::from(f))
            } else {
                Err(format!("Unsupported number: {n}"))
            }
        }
        Value::String(s) => Ok(toml_edit::Value::from(s.as_str())),
        _ => Err(format!("Cannot convert {v} to TOML scalar")),
    }
}

fn json_array_to_toml(arr: &[Value]) -> Result<toml_edit::Item, String> {
    // Check if this is an array of objects (array-of-tables in TOML)
    if arr.iter().all(|v| v.is_object()) && !arr.is_empty() {
        let mut aot = toml_edit::ArrayOfTables::new();
        for elem in arr {
            let mut tbl = toml_edit::Table::new();
            if let Some(obj) = elem.as_object() {
                for (k, v) in obj {
                    match v {
                        Value::Null => {} // skip null fields in array elements
                        Value::Array(inner) => {
                            tbl.insert(k, json_array_to_toml(inner)?);
                        }
                        Value::Object(_) => {
                            let mut sub = toml_edit::Table::new();
                            apply_patch_to_table(&mut sub, v)?;
                            tbl.insert(k, toml_edit::Item::Table(sub));
                        }
                        _ => {
                            tbl.insert(k, toml_edit::Item::Value(json_scalar_to_toml_value(v)?));
                        }
                    }
                }
            }
            aot.push(tbl);
        }
        Ok(toml_edit::Item::ArrayOfTables(aot))
    } else {
        // Array of scalars
        let mut toml_arr = toml_edit::Array::new();
        for elem in arr {
            match elem {
                Value::Null => {} // skip nulls
                Value::Array(_) => {
                    return Err("Nested arrays not supported in TOML".to_string());
                }
                _ => {
                    toml_arr.push(json_scalar_to_toml_value(elem)?);
                }
            }
        }
        Ok(toml_edit::Item::Value(toml_edit::Value::Array(toml_arr)))
    }
}

// ── Secret fingerprints ──────────────────────────────────────

/// Compute human-readable fingerprints for all configured secrets.
pub fn compute_secret_fingerprints(
    config: &crate::config::schema::Config,
) -> Value {
    let json = match serde_json::to_value(config) {
        Ok(v) => v,
        Err(_) => return Value::Object(serde_json::Map::new()),
    };

    let mut fps = serde_json::Map::new();

    // Scalar secrets
    for segs in SCALAR_SECRET_PATHS {
        if let Some(leaf) = get_at_path(&json, segs) {
            if let Some(s) = leaf.as_str() {
                let fp = fingerprint_value(s, &segs.join("."));
                fps.insert(segs.join("."), Value::String(fp));
            }
            // null/missing: omit
        }
    }

    // gateway.paired_tokens
    if let Some(arr) = get_at_path(&json, &["gateway", "paired_tokens"]).and_then(Value::as_array) {
        if !arr.is_empty() {
            fps.insert(
                "gateway.paired_tokens".to_string(),
                Value::String(format!("{} token(s) configured", arr.len())),
            );
        }
    }

    // model_routes[*].api_key
    if let Some(routes) = json.get("model_routes").and_then(Value::as_array) {
        for (i, route) in routes.iter().enumerate() {
            if let Some(s) = route.get("api_key").and_then(Value::as_str) {
                let fp = fingerprint_value(s, "api_key");
                fps.insert(format!("model_routes[{i}].api_key"), Value::String(fp));
            }
        }
    }

    Value::Object(fps)
}

/// Produce a fingerprint for a secret value.
/// Telegram bot tokens: "bot {id}" (chars before ':').
/// Secrets >= 8 chars: "...{last4}".
/// Short secrets: "configured".
fn fingerprint_value(value: &str, path_hint: &str) -> String {
    // Telegram bot token format: {bot_id}:{secret}
    if path_hint.contains("bot_token") || path_hint.ends_with("telegram.bot_token") {
        if let Some(colon_pos) = value.find(':') {
            let bot_id = &value[..colon_pos];
            return format!("bot {bot_id}");
        }
    }

    if value.len() >= 8 {
        let last4 = &value[value.len() - 4..];
        format!("...{last4}")
    } else {
        "configured".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn mask_top_level_api_key() {
        let mut v = json!({ "api_key": "sk-secret-123", "default_temperature": 0.7 });
        mask_config_secrets(&mut v);
        assert_eq!(v["api_key"], MASKED);
        assert_eq!(v["default_temperature"], 0.7);
    }

    #[test]
    fn mask_nested_channel_tokens() {
        let mut v = json!({
            "channels_config": {
                "telegram": { "bot_token": "SECRET_TG" },
                "discord": { "bot_token": "SECRET_DC" },
                "slack": { "bot_token": "SECRET_SL", "app_token": "SECRET_SL_APP" },
            }
        });
        mask_config_secrets(&mut v);
        assert_eq!(v["channels_config"]["telegram"]["bot_token"], MASKED);
        assert_eq!(v["channels_config"]["discord"]["bot_token"], MASKED);
        assert_eq!(v["channels_config"]["slack"]["bot_token"], MASKED);
        assert_eq!(v["channels_config"]["slack"]["app_token"], MASKED);
    }

    #[test]
    fn mask_null_stays_null() {
        let mut v = json!({ "api_key": null, "composio": { "api_key": null } });
        mask_config_secrets(&mut v);
        assert!(v["api_key"].is_null());
        assert!(v["composio"]["api_key"].is_null());
    }

    #[test]
    fn mask_missing_paths_no_panic() {
        let mut v = json!({ "default_temperature": 0.7 });
        mask_config_secrets(&mut v); // Should not panic
        assert_eq!(v["default_temperature"], 0.7);
    }

    #[test]
    fn mask_gateway_paired_tokens_array() {
        let mut v = json!({
            "gateway": { "paired_tokens": ["tok-a", "tok-b", "tok-c"] }
        });
        mask_config_secrets(&mut v);
        let tokens = v["gateway"]["paired_tokens"].as_array().unwrap();
        for t in tokens {
            assert_eq!(t, MASKED);
        }
    }

    #[test]
    fn mask_model_routes_api_keys() {
        let mut v = json!({
            "model_routes": [
                { "hint": "fast", "provider": "groq", "model": "llama", "api_key": "SECRET_ROUTE" },
                { "hint": "reason", "provider": "openai", "model": "gpt4" }
            ]
        });
        mask_config_secrets(&mut v);
        assert_eq!(v["model_routes"][0]["api_key"], MASKED);
        assert_eq!(v["model_routes"][0]["hint"], "fast"); // non-secret preserved
                                                          // Second route has no api_key -- should not be added
        assert!(v["model_routes"][1].get("api_key").is_none());
    }

    #[test]
    fn mask_tunnel_secrets() {
        let mut v = json!({
            "tunnel": {
                "ngrok": { "auth_token": "SECRET_NGROK" },
                "cloudflare": { "token": "SECRET_CF" }
            }
        });
        mask_config_secrets(&mut v);
        assert_eq!(v["tunnel"]["ngrok"]["auth_token"], MASKED);
        assert_eq!(v["tunnel"]["cloudflare"]["token"], MASKED);
    }

    #[test]
    fn mask_irc_passwords() {
        let mut v = json!({
            "channels_config": {
                "irc": {
                    "server_password": "SECRET_IRC_SRV",
                    "nickserv_password": "SECRET_IRC_NS",
                    "sasl_password": "SECRET_IRC_SASL",
                    "server": "irc.example.com"
                }
            }
        });
        mask_config_secrets(&mut v);
        assert_eq!(v["channels_config"]["irc"]["server_password"], MASKED);
        assert_eq!(v["channels_config"]["irc"]["nickserv_password"], MASKED);
        assert_eq!(v["channels_config"]["irc"]["sasl_password"], MASKED);
        assert_eq!(v["channels_config"]["irc"]["server"], "irc.example.com");
    }

    #[test]
    fn mask_whatsapp_secrets() {
        let mut v = json!({
            "channels_config": {
                "whatsapp": {
                    "access_token": "SECRET_WA_AT",
                    "verify_token": "SECRET_WA_VT",
                    "app_secret": "SECRET_WA_AS",
                    "phone_number_id": "12345"
                }
            }
        });
        mask_config_secrets(&mut v);
        assert_eq!(v["channels_config"]["whatsapp"]["access_token"], MASKED);
        assert_eq!(v["channels_config"]["whatsapp"]["verify_token"], MASKED);
        assert_eq!(v["channels_config"]["whatsapp"]["app_secret"], MASKED);
        assert_eq!(v["channels_config"]["whatsapp"]["phone_number_id"], "12345");
    }

    #[test]
    fn mask_email_password() {
        let mut v = json!({
            "channels_config": {
                "email": { "password": "SECRET_EMAIL", "username": "user@test.com" }
            }
        });
        mask_config_secrets(&mut v);
        assert_eq!(v["channels_config"]["email"]["password"], MASKED);
        assert_eq!(v["channels_config"]["email"]["username"], "user@test.com");
    }

    #[test]
    fn contains_raw_secrets_detects_leak() {
        let v = json!({ "config": { "key": "my-secret" } });
        assert!(contains_raw_secrets(&v, &["my-secret"]));
    }

    #[test]
    fn contains_raw_secrets_no_false_positive() {
        let v = json!({ "config": { "key": "***MASKED***" } });
        assert!(!contains_raw_secrets(&v, &["my-secret"]));
    }

    #[test]
    fn collect_key_paths_simple() {
        let v = json!({ "a": 1, "b": { "c": 2, "d": 3 } });
        let paths = collect_key_paths(&v, "");
        assert!(paths.contains("a"));
        assert!(paths.contains("b.c"));
        assert!(paths.contains("b.d"));
        assert!(!paths.contains("b")); // not a leaf
    }

    // ── detect_secret_fields tests ──────────────────────────────

    #[test]
    fn detect_finds_top_level_api_key() {
        let v = json!({ "api_key": "sk-real", "default_temperature": 0.7 });
        let paths = detect_secret_fields(&v);
        assert!(paths.contains(&"api_key".to_string()));
    }

    #[test]
    fn detect_skips_masked_values() {
        let v = json!({ "api_key": MASKED });
        let paths = detect_secret_fields(&v);
        assert!(paths.is_empty());
    }

    #[test]
    fn detect_skips_null_values() {
        let v = json!({ "api_key": null });
        let paths = detect_secret_fields(&v);
        assert!(paths.is_empty());
    }

    #[test]
    fn detect_finds_nested_and_array_secrets() {
        let v = json!({
            "channels_config": { "telegram": { "bot_token": "real-tok" } },
            "gateway": { "paired_tokens": ["tok1", "tok2"] },
            "model_routes": [
                { "hint": "fast", "api_key": "route-key" },
                { "hint": "slow" }
            ]
        });
        let paths = detect_secret_fields(&v);
        assert!(paths.contains(&"channels_config.telegram.bot_token".to_string()));
        assert!(paths.contains(&"gateway.paired_tokens[0]".to_string()));
        assert!(paths.contains(&"gateway.paired_tokens[1]".to_string()));
        assert!(paths.contains(&"model_routes[0].api_key".to_string()));
        assert_eq!(paths.len(), 4);
    }

    // ── preserve_masked_secrets tests ───────────────────────────

    #[test]
    fn preserve_copies_masked_sentinel_from_current() {
        let current = json!({ "api_key": "sk-real-secret" });
        let mut incoming = json!({ "api_key": MASKED });
        let result = preserve_masked_secrets(&mut incoming, &current);
        assert!(result.is_ok());
        assert_eq!(incoming["api_key"], "sk-real-secret");
        assert!(result.unwrap().is_empty()); // no NEW secrets
    }

    #[test]
    fn preserve_detects_new_secret() {
        let current = json!({ "api_key": null });
        let mut incoming = json!({ "api_key": "sk-brand-new" });
        let result = preserve_masked_secrets(&mut incoming, &current);
        assert!(result.is_ok());
        let new_paths = result.unwrap();
        assert!(new_paths.contains(&"api_key".to_string()));
    }

    #[test]
    fn preserve_errors_on_dangling_sentinel() {
        let current = json!({ "api_key": null });
        let mut incoming = json!({ "api_key": MASKED });
        let result = preserve_masked_secrets(&mut incoming, &current);
        assert!(result.is_err());
        let (path, _msg) = result.unwrap_err();
        assert_eq!(path, "api_key");
    }

    #[test]
    fn preserve_paired_tokens_array() {
        let current = json!({ "gateway": { "paired_tokens": ["real-tok-1", "real-tok-2"] } });
        let mut incoming = json!({ "gateway": { "paired_tokens": [MASKED, MASKED] } });
        let result = preserve_masked_secrets(&mut incoming, &current);
        assert!(result.is_ok());
        let arr = incoming["gateway"]["paired_tokens"].as_array().unwrap();
        assert_eq!(arr[0], "real-tok-1");
        assert_eq!(arr[1], "real-tok-2");
    }

    #[test]
    fn preserve_model_route_api_key() {
        let current = json!({
            "model_routes": [{ "hint": "fast", "api_key": "route-secret" }]
        });
        let mut incoming = json!({
            "model_routes": [{ "hint": "fast", "api_key": MASKED }]
        });
        let result = preserve_masked_secrets(&mut incoming, &current);
        assert!(result.is_ok());
        assert_eq!(incoming["model_routes"][0]["api_key"], "route-secret");
    }

    // ── diff_json tests ─────────────────────────────────────────

    #[test]
    fn diff_detects_scalar_change() {
        let old = json!({ "temperature": 0.7, "model": "gpt-4" });
        let new = json!({ "temperature": 0.9, "model": "gpt-4" });
        let diff = diff_json(&old, &new);
        assert_eq!(diff.changes.len(), 1);
        assert_eq!(diff.changes[0].path, "temperature");
        assert_eq!(diff.unchanged_count, 1); // model
    }

    #[test]
    fn diff_detects_added_and_removed() {
        let old = json!({ "a": 1, "b": 2 });
        let new = json!({ "b": 2, "c": 3 });
        let diff = diff_json(&old, &new);
        assert!(diff.removed.contains(&"a".to_string()));
        assert!(diff.added.contains(&"c".to_string()));
        assert_eq!(diff.unchanged_count, 1); // b
    }

    #[test]
    fn diff_nested_change() {
        let old = json!({ "heartbeat": { "enabled": false, "interval": 30 } });
        let new = json!({ "heartbeat": { "enabled": true, "interval": 30 } });
        let diff = diff_json(&old, &new);
        assert_eq!(diff.changes.len(), 1);
        assert_eq!(diff.changes[0].path, "heartbeat.enabled");
    }

    #[test]
    fn diff_identical_values() {
        let v = json!({ "a": 1, "b": { "c": 2 } });
        let diff = diff_json(&v, &v);
        assert!(diff.changes.is_empty());
        assert!(diff.added.is_empty());
        assert!(diff.removed.is_empty());
        assert_eq!(diff.unchanged_count, 2);
    }

    // ── PATCH validation tests ──────────────────────────────────

    #[test]
    fn reject_dotted_keys_catches_flat_path() {
        let patch = json!({ "channels_config.telegram.flows_enabled": true });
        let result = reject_dotted_keys(&patch, "");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Dotted key"));
    }

    #[test]
    fn reject_dotted_keys_allows_nested() {
        let patch = json!({ "channels_config": { "telegram": { "flows_enabled": true } } });
        let result = reject_dotted_keys(&patch, "");
        assert!(result.is_ok());
    }

    #[test]
    fn validate_paths_rejects_unknown() {
        let patch = json!({ "nonexistent_field": 42 });
        let result = validate_patch_paths(&patch);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains(&"nonexistent_field".to_string()));
    }

    #[test]
    fn validate_paths_accepts_valid_leaf() {
        let patch = json!({ "default_temperature": 0.9 });
        assert!(validate_patch_paths(&patch).is_ok());
    }

    #[test]
    fn validate_paths_accepts_nested_valid() {
        let patch = json!({
            "channels_config": { "telegram": { "flows_enabled": true } }
        });
        assert!(validate_patch_paths(&patch).is_ok());
    }

    #[test]
    fn validate_paths_accepts_model_routes() {
        let patch = json!({
            "model_routes": [
                { "hint": "fast", "provider": "groq", "model": "llama" }
            ]
        });
        assert!(validate_patch_paths(&patch).is_ok());
    }

    #[test]
    fn validate_paths_rejects_unknown_model_route_key() {
        let patch = json!({
            "model_routes": [
                { "hint": "fast", "provider": "groq", "model": "llama", "bogus": true }
            ]
        });
        let result = validate_patch_paths(&patch);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains(&"model_routes[0].bogus".to_string()));
    }

    #[test]
    fn validate_null_rejects_required_field() {
        let patch = json!({ "default_temperature": null });
        let result = validate_null_targets(&patch);
        assert!(result.is_err());
    }

    #[test]
    fn validate_null_accepts_optional_field() {
        let patch = json!({ "api_key": null });
        assert!(validate_null_targets(&patch).is_ok());
    }

    #[test]
    fn validate_null_accepts_channel_section() {
        let patch = json!({
            "channels_config": { "telegram": null }
        });
        assert!(validate_null_targets(&patch).is_ok());
    }

    #[test]
    fn reject_masked_sentinel_catches_scalar() {
        let patch = json!({ "api_key": MASKED });
        let result = reject_masked_sentinels(&patch);
        assert!(result.is_err());
    }

    #[test]
    fn reject_masked_sentinel_catches_in_array() {
        let patch = json!({
            "model_routes": [{ "api_key": MASKED }]
        });
        let result = reject_masked_sentinels(&patch);
        assert!(result.is_err());
    }

    #[test]
    fn reject_masked_sentinel_passes_clean() {
        let patch = json!({ "default_temperature": 0.9 });
        assert!(reject_masked_sentinels(&patch).is_ok());
    }

    #[test]
    fn collect_secret_writes_finds_scalar() {
        let patch = json!({ "api_key": "sk-new" });
        let paths = collect_secret_writes(&patch);
        assert!(paths.contains(&"api_key".to_string()));
    }

    #[test]
    fn collect_secret_writes_finds_paired_tokens() {
        let patch = json!({ "gateway": { "paired_tokens": ["tok1"] } });
        let paths = collect_secret_writes(&patch);
        assert!(paths.contains(&"gateway.paired_tokens".to_string()));
    }

    #[test]
    fn collect_secret_writes_finds_model_route_api_key() {
        let patch = json!({
            "model_routes": [{ "hint": "fast", "api_key": "sk-new" }]
        });
        let paths = collect_secret_writes(&patch);
        assert!(paths.contains(&"model_routes[0].api_key".to_string()));
    }

    #[test]
    fn collect_secret_writes_empty_paired_tokens_is_secret() {
        let patch = json!({ "gateway": { "paired_tokens": [] } });
        let paths = collect_secret_writes(&patch);
        assert!(paths.contains(&"gateway.paired_tokens".to_string()));
    }

    #[test]
    fn collect_secret_writes_ignores_nonsecret() {
        let patch = json!({ "default_temperature": 0.9 });
        let paths = collect_secret_writes(&patch);
        assert!(paths.is_empty());
    }

    // ── TOML patch application tests ────────────────────────────

    #[test]
    fn apply_patch_scalar() {
        let toml = "default_temperature = 0.7\n";
        let patch = json!({ "default_temperature": 0.9 });
        let result = apply_json_patch_to_toml(toml, &patch).unwrap();
        assert!(result.contains("0.9"));
        assert!(!result.contains("0.7"));
    }

    #[test]
    fn apply_patch_preserves_untouched() {
        let toml = "default_temperature = 0.7\n\n[heartbeat]\nenabled = false\ninterval_minutes = 30\n";
        let patch = json!({ "default_temperature": 0.9 });
        let result = apply_json_patch_to_toml(toml, &patch).unwrap();
        // heartbeat section should be preserved
        assert!(result.contains("[heartbeat]"));
        assert!(result.contains("enabled = false"));
        assert!(result.contains("interval_minutes = 30"));
    }

    #[test]
    fn apply_patch_null_removes_key() {
        let toml = "api_key = \"test\"\ndefault_temperature = 0.7\n";
        let patch = json!({ "api_key": null });
        let result = apply_json_patch_to_toml(toml, &patch).unwrap();
        assert!(!result.contains("api_key"));
        assert!(result.contains("default_temperature"));
    }

    #[test]
    fn apply_patch_null_removes_table() {
        let toml = "[channels_config.telegram]\nbot_token = \"tok\"\nflows_enabled = false\n\n[heartbeat]\nenabled = true\n";
        let patch = json!({ "channels_config": { "telegram": null } });
        let result = apply_json_patch_to_toml(toml, &patch).unwrap();
        assert!(!result.contains("telegram"));
        assert!(result.contains("[heartbeat]"));
    }

    #[test]
    fn apply_patch_nested_object() {
        let toml = "[heartbeat]\nenabled = false\ninterval_minutes = 30\n";
        let patch = json!({ "heartbeat": { "enabled": true } });
        let result = apply_json_patch_to_toml(toml, &patch).unwrap();
        assert!(result.contains("enabled = true"));
        assert!(result.contains("interval_minutes = 30"));
    }

    #[test]
    fn apply_patch_array_of_tables() {
        let toml = "default_temperature = 0.7\n";
        let patch = json!({
            "model_routes": [
                { "hint": "fast", "provider": "groq", "model": "llama" }
            ]
        });
        let result = apply_json_patch_to_toml(toml, &patch).unwrap();
        assert!(result.contains("[[model_routes]]"));
        assert!(result.contains("hint = \"fast\""));
    }

    // ── Fingerprint tests ───────────────────────────────────────

    #[test]
    fn fingerprint_telegram_bot_token() {
        let fp = fingerprint_value("8256947227:AAEq_test_secret", "telegram.bot_token");
        assert_eq!(fp, "bot 8256947227");
    }

    #[test]
    fn fingerprint_long_secret() {
        let fp = fingerprint_value("sk-abcdefgh1234", "api_key");
        assert_eq!(fp, "...1234");
    }

    #[test]
    fn fingerprint_short_secret() {
        let fp = fingerprint_value("abc", "api_key");
        assert_eq!(fp, "configured");
    }

    // ── Drift test ──────────────────────────────────────────────

    /// Build a fully-populated Config for schema coverage testing.
    /// Every Option<T> is Some, every Vec has at least one entry.
    fn build_fully_populated_config() -> crate::config::schema::Config {
        use crate::config::schema::*;
        Config {
            workspace_dir: std::path::PathBuf::new(),
            config_path: std::path::PathBuf::new(),
            api_key: Some("test-key".into()),
            default_provider: Some("openrouter".into()),
            default_model: Some("test-model".into()),
            default_temperature: 0.7,
            observability: ObservabilityConfig {
                backend: "none".into(),
                otel_endpoint: Some("http://localhost:4318".into()),
                otel_service_name: Some("test".into()),
            },
            autonomy: AutonomyConfig {
                level: crate::security::AutonomyLevel::Supervised,
                workspace_only: true,
                allowed_commands: vec!["git".into()],
                forbidden_paths: vec!["/tmp".into()],
                max_actions_per_hour: 20,
                max_cost_per_day_cents: 500,
                require_approval_for_medium_risk: true,
                block_high_risk_commands: true,
            },
            approval_policy: ApprovalPolicyConfig {
                enabled: false,
                timeout_secs: 90,
                medium_risk_approver: "origin".into(),
                high_risk_approver: "admin".into(),
                origin_mode: "requester_only".into(),
                admin_users: vec!["12345".into()],
            },
            runtime: RuntimeConfig {
                kind: "native".into(),
                docker: DockerRuntimeConfig {
                    image: "alpine:3.20".into(),
                    network: "none".into(),
                    memory_limit_mb: Some(512),
                    cpu_limit: Some(1.0),
                    read_only_rootfs: true,
                    mount_workspace: true,
                    allowed_workspace_roots: vec!["/home".into()],
                },
            },
            reliability: ReliabilityConfig {
                provider_retries: 2,
                provider_backoff_ms: 500,
                fallback_providers: vec!["openai".into()],
                channel_initial_backoff_secs: 2,
                channel_max_backoff_secs: 60,
                scheduler_poll_secs: 15,
                scheduler_retries: 2,
            },
            model_routes: vec![ModelRouteConfig {
                hint: "fast".into(),
                provider: "groq".into(),
                model: "llama".into(),
                api_key: Some("route-key".into()),
            }],
            heartbeat: HeartbeatConfig {
                enabled: true,
                interval_minutes: 30,
            },
            channels_config: ChannelsConfig {
                cli: true,
                telegram: Some(TelegramConfig {
                    bot_token: "123:ABC".into(),
                    allowed_users: vec!["user1".into()],
                    stt_endpoint: Some("http://localhost:9000".into()),
                    flows_enabled: true,
                    flow_policy: FlowPolicyConfig {
                        agent_authoring_enabled: true,
                        denied_step_kinds: vec!["edit".into()],
                        max_steps: 10,
                        max_agent_flows: 50,
                        require_handoff_on_keyboard: true,
                        auto_approve: false,
                        auto_approve_max_steps: 5,
                        denied_text_patterns: vec!["blocked".into()],
                    },
                }),
                discord: Some(DiscordConfig {
                    bot_token: "discord-tok".into(),
                    guild_id: Some("guild-123".into()),
                    allowed_users: vec!["user1".into()],
                }),
                slack: Some(SlackConfig {
                    bot_token: "slack-tok".into(),
                    app_token: Some("app-tok".into()),
                    channel_id: Some("chan-123".into()),
                    allowed_users: vec!["user1".into()],
                }),
                webhook: Some(WebhookConfig {
                    port: 8080,
                    secret: Some("wh-secret".into()),
                }),
                imessage: Some(IMessageConfig {
                    allowed_contacts: vec!["contact1".into()],
                }),
                matrix: Some(MatrixConfig {
                    homeserver: "https://matrix.test".into(),
                    access_token: "matrix-tok".into(),
                    room_id: "!room:test".into(),
                    allowed_users: vec!["@user:test".into()],
                }),
                whatsapp: Some(WhatsAppConfig {
                    access_token: "wa-tok".into(),
                    phone_number_id: "12345".into(),
                    verify_token: "wa-verify".into(),
                    app_secret: Some("wa-secret".into()),
                    allowed_numbers: vec!["+1234567890".into()],
                }),
                email: Some(crate::channels::email_channel::EmailConfig {
                    imap_host: "imap.test.com".into(),
                    imap_port: 993,
                    imap_folder: "INBOX".into(),
                    smtp_host: "smtp.test.com".into(),
                    smtp_port: 587,
                    smtp_tls: true,
                    username: "user@test.com".into(),
                    password: "email-pass".into(),
                    from_address: "from@test.com".into(),
                    poll_interval_secs: 60,
                    allowed_senders: vec!["sender@test.com".into()],
                }),
                irc: Some(IrcConfig {
                    server: "irc.test.com".into(),
                    port: 6697,
                    nickname: "bot".into(),
                    username: Some("botuser".into()),
                    channels: vec!["#test".into()],
                    allowed_users: vec!["user1".into()],
                    server_password: Some("irc-pass".into()),
                    nickserv_password: Some("ns-pass".into()),
                    sasl_password: Some("sasl-pass".into()),
                    verify_tls: Some(true),
                }),
                cp_relay: Some(CpRelayConfig {
                    cp_url: "http://localhost:18800".into(),
                }),
            },
            memory: MemoryConfig {
                backend: "sqlite".into(),
                auto_save: true,
                hygiene_enabled: true,
                archive_after_days: 7,
                purge_after_days: 30,
                conversation_retention_days: 30,
                embedding_provider: "none".into(),
                embedding_model: "text-embedding-3-small".into(),
                embedding_dimensions: 1536,
                vector_weight: 0.7,
                keyword_weight: 0.3,
                embedding_cache_size: 10_000,
                chunk_max_tokens: 512,
            },
            tunnel: TunnelConfig {
                provider: "none".into(),
                cloudflare: Some(CloudflareTunnelConfig {
                    token: "cf-token".into(),
                }),
                tailscale: Some(TailscaleTunnelConfig {
                    funnel: true,
                    hostname: Some("ts-host".into()),
                }),
                ngrok: Some(NgrokTunnelConfig {
                    auth_token: "ngrok-tok".into(),
                    domain: Some("ngrok.test".into()),
                }),
                custom: Some(CustomTunnelConfig {
                    start_command: "bore local {port}".into(),
                    health_url: Some("http://localhost:8080/health".into()),
                    url_pattern: Some("https://(.+)\\.bore\\.pub".into()),
                }),
            },
            gateway: GatewayConfig {
                port: 3000,
                host: "127.0.0.1".into(),
                require_pairing: true,
                allow_public_bind: false,
                paired_tokens: vec!["tok1".into()],
                pair_rate_limit_per_minute: 10,
                webhook_rate_limit_per_minute: 60,
                idempotency_ttl_secs: 300,
            },
            composio: ComposioConfig {
                enabled: true,
                api_key: Some("composio-key".into()),
                entity_id: "default".into(),
            },
            secrets: SecretsConfig { encrypt: true },
            browser: BrowserConfig {
                enabled: true,
                allowed_domains: vec!["example.com".into()],
                session_name: Some("test-session".into()),
            },
            identity: IdentityConfig {
                format: "openclaw".into(),
                aieos_path: Some("/path/to/aieos.json".into()),
                aieos_inline: Some("{\"name\":\"test\"}".into()),
            },
        }
    }

    /// Collect all leaf paths from a JSON value, skipping array-of-objects internals.
    fn collect_config_paths(value: &Value, prefix: &str) -> Vec<String> {
        let mut paths = Vec::new();
        match value {
            Value::Object(map) => {
                for (k, v) in map {
                    let path = if prefix.is_empty() {
                        k.clone()
                    } else {
                        format!("{prefix}.{k}")
                    };
                    match v {
                        Value::Object(_) => {
                            paths.extend(collect_config_paths(v, &path));
                        }
                        Value::Array(arr) => {
                            // Check if array-of-objects (model_routes) vs array-of-scalars
                            if arr.iter().any(|e| e.is_object()) {
                                // model_routes: just record the array path
                                paths.push(path);
                            } else {
                                paths.push(path);
                            }
                        }
                        _ => {
                            paths.push(path);
                        }
                    }
                }
            }
            _ => {}
        }
        paths
    }

    #[test]
    fn valid_config_paths_covers_schema() {
        let config = build_fully_populated_config();
        let json = serde_json::to_value(&config).unwrap();
        let leaf_paths = collect_config_paths(&json, "");

        for path in &leaf_paths {
            assert!(
                VALID_CONFIG_PATHS.contains(&path.as_str()),
                "Schema path '{}' not in VALID_CONFIG_PATHS -- add it",
                path
            );
        }
    }

    #[test]
    fn model_route_element_keys_match_schema() {
        let config = build_fully_populated_config();
        let json = serde_json::to_value(&config).unwrap();

        if let Some(routes) = json.get("model_routes").and_then(|v| v.as_array()) {
            if let Some(first) = routes.first().and_then(|v| v.as_object()) {
                let mut actual: Vec<&str> = first.keys().map(|k| k.as_str()).collect();
                actual.sort();
                let mut expected: Vec<&str> = MODEL_ROUTE_ELEMENT_KEYS.to_vec();
                expected.sort();
                assert_eq!(
                    actual, expected,
                    "model_routes element keys drifted. Actual: {:?}, Expected: {:?}",
                    actual, expected
                );
            } else {
                panic!("model_routes[0] is not an object");
            }
        } else {
            panic!("model_routes is missing or not an array");
        }
    }

    #[test]
    fn validate_paths_accepts_section_null() {
        // Setting an entire channel section to null (to disable it)
        let patch = json!({
            "channels_config": { "telegram": null }
        });
        assert!(validate_patch_paths(&patch).is_ok());
    }

    #[test]
    fn validate_paths_rejects_partial_section_name() {
        // "channels_config.te" should NOT match "channels_config.telegram"
        let patch = json!({
            "channels_config": { "te": null }
        });
        let result = validate_patch_paths(&patch);
        assert!(result.is_err());
    }
}
