use crate::channels::telegram::TelegramChannel;
use crate::channels::telegram_types::TelegramToolContext;
use crate::flows::db::FlowDb;
use crate::flows::execute::execute_step;
use crate::flows::state::FlowStore;
use crate::flows::types::FlowDefinition;
use crate::tools::traits::{Tool, ToolResult};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Resolve chat_id from explicit param or from the shared tool context.
fn resolve_chat_id(
    args: &serde_json::Value,
    context: &Arc<Mutex<Option<TelegramToolContext>>>,
) -> Option<String> {
    if let Some(id) = args.get("chat_id").and_then(|v| v.as_str()) {
        if !id.is_empty() {
            return Some(id.to_string());
        }
    }
    context
        .lock()
        .ok()
        .and_then(|guard| guard.as_ref().map(|ctx| ctx.chat_id.clone()))
}

/// Tool: `telegram_start_flow` -- triggers a validated declarative flow.
///
/// Looks up flow definitions in the operator TOML map first, then falls back
/// to the DB for agent-authored active versions.
pub struct TelegramFlowTool {
    channel: Arc<TelegramChannel>,
    context: Arc<Mutex<Option<TelegramToolContext>>>,
    flow_defs: Arc<HashMap<String, FlowDefinition>>,
    flow_store: Arc<FlowStore>,
    flow_db: Option<Arc<FlowDb>>,
}

impl TelegramFlowTool {
    pub fn new(
        channel: Arc<TelegramChannel>,
        context: Arc<Mutex<Option<TelegramToolContext>>>,
        flow_defs: Arc<HashMap<String, FlowDefinition>>,
        flow_store: Arc<FlowStore>,
        flow_db: Option<Arc<FlowDb>>,
    ) -> Self {
        Self {
            channel,
            context,
            flow_defs,
            flow_store,
            flow_db,
        }
    }
}

#[async_trait]
impl Tool for TelegramFlowTool {
    fn name(&self) -> &str {
        "telegram_start_flow"
    }

    fn description(&self) -> &str {
        "Start a declarative Telegram conversation flow. Available flows are loaded from TOML definitions."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        serde_json::json!({
            "type": "object",
            "properties": {
                "flow_name": {
                    "type": "string",
                    "description": "Name of the flow to start (must match a loaded flow definition)"
                },
                "chat_id": {
                    "type": "string",
                    "description": "Target chat ID (auto-resolved from context if omitted)"
                }
            },
            "required": ["flow_name"]
        })
    }

    async fn execute(&self, args: serde_json::Value) -> anyhow::Result<ToolResult> {
        let chat_id = match resolve_chat_id(&args, &self.context) {
            Some(id) => id,
            None => {
                return Ok(ToolResult {
                    success: false,
                    output: String::new(),
                    error: Some("No chat_id provided and no context available".into()),
                });
            }
        };

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

        // Look up flow definition: operator TOML first, then DB fallback
        let db_flow_def: Option<FlowDefinition>;
        let flow_def = if let Some(def) = self.flow_defs.get(&flow_name) {
            def
        } else if let Some(ref db) = self.flow_db {
            // DB fallback for agent-authored active flows
            match db.get_active_version(&flow_name) {
                Ok(Some(row)) => {
                    let toml_def: crate::flows::types::FlowDefinitionToml =
                        match serde_json::from_str(&row.definition_json) {
                            Ok(d) => d,
                            Err(e) => {
                                return Ok(ToolResult {
                                    success: false,
                                    output: String::new(),
                                    error: Some(format!(
                                        "Flow '{}' has invalid definition in DB: {e}",
                                        flow_name
                                    )),
                                });
                            }
                        };
                    match crate::flows::validate::build_flow_definition(&toml_def) {
                        Ok(def) => {
                            db_flow_def = Some(def);
                            db_flow_def.as_ref().unwrap()
                        }
                        Err(errors) => {
                            let msgs: Vec<String> =
                                errors.iter().map(|e| e.message.clone()).collect();
                            return Ok(ToolResult {
                                success: false,
                                output: String::new(),
                                error: Some(format!(
                                    "Flow '{}' DB definition failed validation: {}",
                                    flow_name,
                                    msgs.join("; ")
                                )),
                            });
                        }
                    }
                }
                _ => {
                    let available: Vec<&str> =
                        self.flow_defs.keys().map(|k| k.as_str()).collect();
                    return Ok(ToolResult {
                        success: false,
                        output: String::new(),
                        error: Some(format!(
                            "Flow '{}' not found. Available flows: {}",
                            flow_name,
                            if available.is_empty() {
                                "none".to_string()
                            } else {
                                available.join(", ")
                            }
                        )),
                    });
                }
            }
        } else {
            let available: Vec<&str> = self.flow_defs.keys().map(|k| k.as_str()).collect();
            return Ok(ToolResult {
                success: false,
                output: String::new(),
                error: Some(format!(
                    "Flow '{}' not found. Available flows: {}",
                    flow_name,
                    if available.is_empty() {
                        "none".to_string()
                    } else {
                        available.join(", ")
                    }
                )),
            });
        };

        let start_step = match flow_def.steps.get(&flow_def.start_step) {
            Some(step) => step,
            None => {
                return Ok(ToolResult {
                    success: false,
                    output: String::new(),
                    error: Some(format!(
                        "Flow '{}' has invalid start step '{}'",
                        flow_name, flow_def.start_step
                    )),
                });
            }
        };

        // Execute the start step
        match execute_step(&self.channel, &chat_id, start_step, None).await {
            Ok(result) => {
                // Register the flow in the store
                self.flow_store.start_flow(
                    &chat_id,
                    &flow_name,
                    &flow_def.start_step,
                    result.anchor_message_id,
                );

                // Store poll_id mapping if present
                if let Some(ref poll_id) = result.poll_id {
                    if let Some(ref db) = self.flow_store.db() {
                        let key = format!("poll:{poll_id}");
                        if let Err(e) = db.set_kv(&key, &chat_id) {
                            tracing::warn!("Failed to store poll_id mapping: {e}");
                        }
                    }
                }

                Ok(ToolResult {
                    success: true,
                    output: format!(
                        "Flow '{}' started at step '{}', message_id={}",
                        flow_name,
                        flow_def.start_step,
                        result.anchor_message_id.unwrap_or(-1),
                    ),
                    error: None,
                })
            }
            Err(e) => Ok(ToolResult {
                success: false,
                output: String::new(),
                error: Some(format!("Failed to execute start step: {e}")),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flow_tool_schema_valid() {
        let ch = Arc::new(TelegramChannel::new("t".into(), vec![]));
        let ctx = Arc::new(Mutex::new(None));
        let defs = Arc::new(HashMap::new());
        let store = Arc::new(FlowStore::new());
        let tool = TelegramFlowTool::new(ch, ctx, defs, store, None);
        let schema = tool.parameters_schema();
        assert!(schema["properties"]["flow_name"].is_object());
        assert_eq!(tool.name(), "telegram_start_flow");
    }

    #[test]
    fn flow_tool_schema_requires_flow_name() {
        let ch = Arc::new(TelegramChannel::new("t".into(), vec![]));
        let ctx = Arc::new(Mutex::new(None));
        let defs = Arc::new(HashMap::new());
        let store = Arc::new(FlowStore::new());
        let tool = TelegramFlowTool::new(ch, ctx, defs, store, None);
        let schema = tool.parameters_schema();
        let required = schema["required"].as_array().unwrap();
        assert!(required.iter().any(|v| v.as_str() == Some("flow_name")));
    }
}
