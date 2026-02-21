use crate::channels::telegram::TelegramChannel;
use crate::channels::telegram_types::TelegramToolContext;
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
pub struct TelegramFlowTool {
    channel: Arc<TelegramChannel>,
    context: Arc<Mutex<Option<TelegramToolContext>>>,
    flow_defs: Arc<HashMap<String, FlowDefinition>>,
    flow_store: Arc<FlowStore>,
}

impl TelegramFlowTool {
    pub fn new(
        channel: Arc<TelegramChannel>,
        context: Arc<Mutex<Option<TelegramToolContext>>>,
        flow_defs: Arc<HashMap<String, FlowDefinition>>,
        flow_store: Arc<FlowStore>,
    ) -> Self {
        Self {
            channel,
            context,
            flow_defs,
            flow_store,
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

        let flow_def = match self.flow_defs.get(&flow_name) {
            Some(def) => def,
            None => {
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
            }
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
            Ok(anchor_msg_id) => {
                // Register the flow in the store
                self.flow_store.start_flow(
                    &chat_id,
                    &flow_name,
                    &flow_def.start_step,
                    anchor_msg_id,
                );

                Ok(ToolResult {
                    success: true,
                    output: format!(
                        "Flow '{}' started at step '{}', message_id={}",
                        flow_name,
                        flow_def.start_step,
                        anchor_msg_id.unwrap_or(-1),
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
        let tool = TelegramFlowTool::new(ch, ctx, defs, store);
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
        let tool = TelegramFlowTool::new(ch, ctx, defs, store);
        let schema = tool.parameters_schema();
        let required = schema["required"].as_array().unwrap();
        assert!(required.iter().any(|v| v.as_str() == Some("flow_name")));
    }
}
