pub mod execute;
pub mod state;
pub mod types;
pub mod validate;

use std::collections::HashMap;
use std::path::Path;
use types::FlowDefinition;

/// Load and validate all flow TOML files from the given directory.
/// Returns a map of flow_name -> FlowDefinition.
///
/// If the directory does not exist, returns an empty map (not an error).
/// If any flow file fails to parse or validate, returns an error.
pub fn load_flows(flows_dir: &Path) -> anyhow::Result<HashMap<String, FlowDefinition>> {
    if !flows_dir.exists() {
        tracing::debug!("flows directory does not exist: {}", flows_dir.display());
        return Ok(HashMap::new());
    }

    let mut definitions = HashMap::new();
    let mut all_errors = Vec::new();

    let entries = std::fs::read_dir(flows_dir)
        .map_err(|e| anyhow::anyhow!("failed to read flows directory: {e}"))?;

    for entry in entries {
        let entry = entry?;
        let path = entry.path();

        if path.extension().and_then(|e| e.to_str()) != Some("toml") {
            continue;
        }

        let content = std::fs::read_to_string(&path)
            .map_err(|e| anyhow::anyhow!("failed to read {}: {e}", path.display()))?;

        let toml_def: types::FlowDefinitionToml = toml::from_str(&content)
            .map_err(|e| anyhow::anyhow!("failed to parse {}: {e}", path.display()))?;

        match validate::build_flow_definition(&toml_def) {
            Ok(def) => {
                if definitions.contains_key(&def.name) {
                    all_errors.push(format!(
                        "duplicate flow name '{}' in {}",
                        def.name,
                        path.display()
                    ));
                } else {
                    tracing::info!("loaded flow '{}' from {}", def.name, path.display());
                    definitions.insert(def.name.clone(), def);
                }
            }
            Err(errors) => {
                for e in errors {
                    all_errors.push(format!("{} ({})", e, path.display()));
                }
            }
        }
    }

    if !all_errors.is_empty() {
        anyhow::bail!(
            "flow validation errors:\n  {}",
            all_errors.join("\n  ")
        );
    }

    Ok(definitions)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn load_flows_missing_dir_returns_empty() {
        let tmp = TempDir::new().unwrap();
        let flows_dir = tmp.path().join("nonexistent_flows");
        let result = load_flows(&flows_dir).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn load_flows_valid_file() {
        let tmp = TempDir::new().unwrap();
        let flows_dir = tmp.path().join("flows");
        std::fs::create_dir(&flows_dir).unwrap();
        std::fs::write(
            flows_dir.join("greet.toml"),
            r#"
[flow]
name = "greet"
start = "hello"

[[steps]]
id = "hello"
kind = "message"
text = "Hello there!"
"#,
        )
        .unwrap();

        let result = load_flows(&flows_dir).unwrap();
        assert_eq!(result.len(), 1);
        assert!(result.contains_key("greet"));
    }

    #[test]
    fn load_flows_invalid_file_errors() {
        let tmp = TempDir::new().unwrap();
        let flows_dir = tmp.path().join("flows");
        std::fs::create_dir(&flows_dir).unwrap();
        std::fs::write(
            flows_dir.join("bad.toml"),
            r#"
[flow]
name = "bad"
start = "nonexistent"
"#,
        )
        .unwrap();

        let result = load_flows(&flows_dir);
        assert!(result.is_err());
    }

    #[test]
    fn load_flows_duplicate_name_errors() {
        let tmp = TempDir::new().unwrap();
        let flows_dir = tmp.path().join("flows");
        std::fs::create_dir(&flows_dir).unwrap();

        let flow_content = r#"
[flow]
name = "same"
start = "s1"

[[steps]]
id = "s1"
kind = "message"
text = "Hi"
"#;
        std::fs::write(flows_dir.join("a.toml"), flow_content).unwrap();
        std::fs::write(flows_dir.join("b.toml"), flow_content).unwrap();

        let result = load_flows(&flows_dir);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("duplicate flow name"));
    }
}
