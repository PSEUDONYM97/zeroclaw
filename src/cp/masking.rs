use serde_json::Value;

const MASKED: &str = "***MASKED***";

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
}
