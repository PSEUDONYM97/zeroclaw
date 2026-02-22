use crate::config::FlowPolicyConfig;
use crate::flows::db::FlowDb;
use crate::flows::policy;
use crate::flows::types::{FlowDefinition, FlowDefinitionToml, FlowMeta, StepToml};
use crate::flows::validate::build_flow_definition;
use crate::tools::traits::{Tool, ToolResult};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;

/// Tool: `telegram_compose_flow` -- agent composes a new flow definition.
///
/// The flow goes through: draft -> validated -> pending_review (or auto-approved -> active).
/// Does NOT need TelegramChannel -- it only writes to DB, doesn't execute the flow.
pub struct TelegramComposeFlowTool {
    flow_db: Arc<FlowDb>,
    policy: FlowPolicyConfig,
    operator_flow_defs: Arc<HashMap<String, FlowDefinition>>,
}

impl TelegramComposeFlowTool {
    pub fn new(
        flow_db: Arc<FlowDb>,
        policy: FlowPolicyConfig,
        operator_flow_defs: Arc<HashMap<String, FlowDefinition>>,
    ) -> Self {
        Self {
            flow_db,
            policy,
            operator_flow_defs,
        }
    }
}

#[async_trait]
impl Tool for TelegramComposeFlowTool {
    fn name(&self) -> &str {
        "telegram_compose_flow"
    }

    fn description(&self) -> &str {
        "Compose a new declarative conversation flow. The flow will be validated against policy \
         and submitted for operator review (or auto-approved if eligible). Agent cannot override \
         operator-defined flow names."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        serde_json::json!({
            "type": "object",
            "properties": {
                "flow_name": {
                    "type": "string",
                    "description": "Unique name for this flow (must not conflict with operator flows)"
                },
                "description": {
                    "type": "string",
                    "description": "Human-readable description of what this flow does"
                },
                "start_step": {
                    "type": "string",
                    "description": "ID of the first step in the flow"
                },
                "default_timeout_secs": {
                    "type": "integer",
                    "description": "Default timeout in seconds for steps without explicit timeout (0 = no timeout)"
                },
                "steps": {
                    "type": "array",
                    "description": "Array of step definitions",
                    "items": {
                        "type": "object",
                        "properties": {
                            "id": { "type": "string" },
                            "kind": { "type": "string", "enum": ["message", "keyboard", "poll", "edit"] },
                            "text": { "type": "string" },
                            "buttons": {
                                "type": "array",
                                "items": {
                                    "type": "array",
                                    "items": {
                                        "type": "object",
                                        "properties": {
                                            "text": { "type": "string" },
                                            "callback_data": { "type": "string" }
                                        }
                                    }
                                }
                            },
                            "poll_options": { "type": "array", "items": { "type": "string" } },
                            "poll_anonymous": { "type": "boolean" },
                            "timeout_secs": { "type": "integer" },
                            "agent_handoff": { "type": "boolean" },
                            "transitions": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "on": { "type": "string" },
                                        "target": { "type": "string" }
                                    }
                                }
                            }
                        },
                        "required": ["id", "kind"]
                    }
                }
            },
            "required": ["flow_name", "start_step", "steps"]
        })
    }

    async fn execute(&self, args: serde_json::Value) -> anyhow::Result<ToolResult> {
        // Parse required fields
        let flow_name = match args.get("flow_name").and_then(|v| v.as_str()) {
            Some(name) => name.to_string(),
            None => {
                return Ok(ToolResult {
                    success: false,
                    output: String::new(),
                    error: Some("Missing required parameter: flow_name".into()),
                });
            }
        };

        let start_step = match args.get("start_step").and_then(|v| v.as_str()) {
            Some(s) => s.to_string(),
            None => {
                return Ok(ToolResult {
                    success: false,
                    output: String::new(),
                    error: Some("Missing required parameter: start_step".into()),
                });
            }
        };

        let steps_val = match args.get("steps").and_then(|v| v.as_array()) {
            Some(arr) => arr,
            None => {
                return Ok(ToolResult {
                    success: false,
                    output: String::new(),
                    error: Some("Missing required parameter: steps (array)".into()),
                });
            }
        };

        // 1. Operator-name collision check
        if policy::is_operator_owned(&flow_name, &self.operator_flow_defs) {
            return Ok(ToolResult {
                success: false,
                output: String::new(),
                error: Some(format!(
                    "Flow '{}' is operator-defined and cannot be overridden by agent",
                    flow_name
                )),
            });
        }

        // 2. Parse steps from JSON into FlowDefinitionToml
        let steps: Vec<StepToml> = match serde_json::from_value(serde_json::Value::Array(steps_val.clone())) {
            Ok(s) => s,
            Err(e) => {
                return Ok(ToolResult {
                    success: false,
                    output: String::new(),
                    error: Some(format!("Failed to parse steps: {e}")),
                });
            }
        };

        let description = args
            .get("description")
            .and_then(|v| v.as_str())
            .map(String::from);
        let default_timeout = args
            .get("default_timeout_secs")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);

        let toml_def = FlowDefinitionToml {
            flow: FlowMeta {
                name: flow_name.clone(),
                description,
                start: start_step,
                default_timeout_secs: default_timeout,
            },
            steps,
        };

        // 3. Structural validation
        if let Err(errors) = build_flow_definition(&toml_def) {
            let msgs: Vec<String> = errors.iter().map(|e| e.message.clone()).collect();
            return Ok(ToolResult {
                success: false,
                output: String::new(),
                error: Some(format!("Flow validation failed: {}", msgs.join("; "))),
            });
        }

        // 4. Policy validation
        let agent_count = self.flow_db.count_agent_flows().unwrap_or(0);
        if let Err(violations) = policy::check_policy(&toml_def, &self.policy, agent_count) {
            let msgs: Vec<String> = violations.iter().map(|v| v.message.clone()).collect();
            return Ok(ToolResult {
                success: false,
                output: String::new(),
                error: Some(format!("Policy violations: {}", msgs.join("; "))),
            });
        }

        // 5. Serialize and create version
        let definition_json = match serde_json::to_string(&toml_def) {
            Ok(j) => j,
            Err(e) => {
                return Ok(ToolResult {
                    success: false,
                    output: String::new(),
                    error: Some(format!("Failed to serialize flow definition: {e}")),
                });
            }
        };

        let version = match self
            .flow_db
            .create_flow_version(&flow_name, &definition_json, "agent", "agent")
        {
            Ok(v) => v,
            Err(e) => {
                return Ok(ToolResult {
                    success: false,
                    output: String::new(),
                    error: Some(format!("Failed to create flow version: {e}")),
                });
            }
        };

        // 6. Audit: created + validated
        let _ = self
            .flow_db
            .log_audit(&flow_name, Some(version), "created", "agent", None);
        let _ = self
            .flow_db
            .log_audit(&flow_name, Some(version), "validated", "agent", None);

        // 7. Auto-approve or submit for review
        if policy::qualifies_for_auto_approve(&toml_def, &self.policy) {
            if let Err(e) = self.flow_db.activate_version(&flow_name, version) {
                return Ok(ToolResult {
                    success: false,
                    output: String::new(),
                    error: Some(format!("Failed to activate flow: {e}")),
                });
            }
            let _ = self
                .flow_db
                .log_audit(&flow_name, Some(version), "activated", "agent", None);
            Ok(ToolResult {
                success: true,
                output: format!(
                    "Flow '{}' v{} auto-approved and activated (message-only, {} steps)",
                    flow_name,
                    version,
                    toml_def.steps.len()
                ),
                error: None,
            })
        } else {
            let _ = self.flow_db.update_version_status(
                &flow_name,
                version,
                "pending_review",
                None,
            );
            let _ = self.flow_db.log_audit(
                &flow_name,
                Some(version),
                "submitted_for_review",
                "agent",
                None,
            );
            Ok(ToolResult {
                success: true,
                output: format!(
                    "Flow '{}' v{} submitted for operator review ({} steps)",
                    flow_name,
                    version,
                    toml_def.steps.len()
                ),
                error: None,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compose_tool_schema_valid() {
        let db = Arc::new(FlowDb::open_in_memory().unwrap());
        let policy = FlowPolicyConfig::default();
        let defs = Arc::new(HashMap::new());
        let tool = TelegramComposeFlowTool::new(db, policy, defs);
        let schema = tool.parameters_schema();
        assert!(schema["properties"]["flow_name"].is_object());
        assert!(schema["properties"]["steps"].is_object());
        assert!(schema["properties"]["start_step"].is_object());
        assert_eq!(tool.name(), "telegram_compose_flow");

        let required = schema["required"].as_array().unwrap();
        assert!(required.iter().any(|v| v.as_str() == Some("flow_name")));
        assert!(required.iter().any(|v| v.as_str() == Some("steps")));
        assert!(required.iter().any(|v| v.as_str() == Some("start_step")));
    }

    #[tokio::test]
    async fn compose_tool_rejects_disabled() {
        let db = Arc::new(FlowDb::open_in_memory().unwrap());
        let policy = FlowPolicyConfig::default(); // agent_authoring_enabled = false
        let defs = Arc::new(HashMap::new());
        let tool = TelegramComposeFlowTool::new(db, policy, defs);

        let args = serde_json::json!({
            "flow_name": "test_flow",
            "start_step": "s1",
            "steps": [{
                "id": "s1",
                "kind": "message",
                "text": "Hello"
            }]
        });

        let result = tool.execute(args).await.unwrap();
        assert!(!result.success);
        assert!(result.error.as_deref().unwrap_or("").contains("disabled"));
    }
}
