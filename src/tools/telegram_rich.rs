use crate::channels::telegram::TelegramChannel;
use crate::channels::telegram_types::{InlineButton, TelegramToolContext};
use crate::tools::traits::{Tool, ToolResult};
use async_trait::async_trait;
use std::sync::{Arc, Mutex};

/// Resolve chat_id from explicit param or from the shared tool context.
fn resolve_chat_id(
    args: &serde_json::Value,
    context: &Arc<Mutex<Option<TelegramToolContext>>>,
) -> Option<String> {
    // Explicit param takes priority
    if let Some(id) = args.get("chat_id").and_then(|v| v.as_str()) {
        if !id.is_empty() {
            return Some(id.to_string());
        }
    }
    // Fall back to context
    context
        .lock()
        .ok()
        .and_then(|guard| guard.as_ref().map(|ctx| ctx.chat_id.clone()))
}

// ── Telegram Keyboard Tool ──────────────────────────────────────

pub struct TelegramKeyboardTool {
    channel: Arc<TelegramChannel>,
    context: Arc<Mutex<Option<TelegramToolContext>>>,
}

impl TelegramKeyboardTool {
    pub fn new(
        channel: Arc<TelegramChannel>,
        context: Arc<Mutex<Option<TelegramToolContext>>>,
    ) -> Self {
        Self { channel, context }
    }
}

#[async_trait]
impl Tool for TelegramKeyboardTool {
    fn name(&self) -> &str {
        "telegram_send_keyboard"
    }

    fn description(&self) -> &str {
        "Send a Telegram message with inline keyboard buttons. Returns the message_id for later editing."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        serde_json::json!({
            "type": "object",
            "properties": {
                "text": {
                    "type": "string",
                    "description": "Message text to display above the keyboard"
                },
                "buttons": {
                    "type": "array",
                    "description": "Rows of buttons. Each row is an array of {text, callback_data} objects.",
                    "items": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "text": { "type": "string" },
                                "callback_data": { "type": "string" }
                            },
                            "required": ["text", "callback_data"]
                        }
                    }
                },
                "chat_id": {
                    "type": "string",
                    "description": "Target chat ID (auto-resolved from context if omitted)"
                }
            },
            "required": ["text", "buttons"]
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

        let text = args["text"].as_str().unwrap_or("");
        let buttons_raw = args["buttons"].as_array();

        let buttons: Vec<Vec<InlineButton>> = match buttons_raw {
            Some(rows) => rows
                .iter()
                .map(|row| {
                    row.as_array()
                        .unwrap_or(&vec![])
                        .iter()
                        .map(|b| InlineButton {
                            text: b["text"].as_str().unwrap_or("").to_string(),
                            callback_data: b["callback_data"].as_str().unwrap_or("").to_string(),
                        })
                        .collect()
                })
                .collect(),
            None => vec![],
        };

        match self
            .channel
            .send_with_keyboard(&chat_id, text, &buttons)
            .await
        {
            Ok(msg_id) => Ok(ToolResult {
                success: true,
                output: format!("Keyboard sent. message_id={msg_id}"),
                error: None,
            }),
            Err(e) => Ok(ToolResult {
                success: false,
                output: String::new(),
                error: Some(e.to_string()),
            }),
        }
    }
}

// ── Telegram Poll Tool ──────────────────────────────────────────

pub struct TelegramPollTool {
    channel: Arc<TelegramChannel>,
    context: Arc<Mutex<Option<TelegramToolContext>>>,
}

impl TelegramPollTool {
    pub fn new(
        channel: Arc<TelegramChannel>,
        context: Arc<Mutex<Option<TelegramToolContext>>>,
    ) -> Self {
        Self { channel, context }
    }
}

#[async_trait]
impl Tool for TelegramPollTool {
    fn name(&self) -> &str {
        "telegram_send_poll"
    }

    fn description(&self) -> &str {
        "Send a poll to a Telegram chat. Returns the message_id."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        serde_json::json!({
            "type": "object",
            "properties": {
                "question": {
                    "type": "string",
                    "description": "Poll question"
                },
                "options": {
                    "type": "array",
                    "items": { "type": "string" },
                    "description": "Poll answer options (2-10)"
                },
                "is_anonymous": {
                    "type": "boolean",
                    "description": "Whether the poll is anonymous (default: true)"
                },
                "chat_id": {
                    "type": "string",
                    "description": "Target chat ID (auto-resolved from context if omitted)"
                }
            },
            "required": ["question", "options"]
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

        let question = args["question"].as_str().unwrap_or("");
        let options: Vec<String> = args["options"]
            .as_array()
            .unwrap_or(&vec![])
            .iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect();
        let is_anonymous = args["is_anonymous"].as_bool().unwrap_or(true);

        match self
            .channel
            .send_poll(&chat_id, question, &options, is_anonymous)
            .await
        {
            Ok(msg_id) => Ok(ToolResult {
                success: true,
                output: format!("Poll sent. message_id={msg_id}"),
                error: None,
            }),
            Err(e) => Ok(ToolResult {
                success: false,
                output: String::new(),
                error: Some(e.to_string()),
            }),
        }
    }
}

// ── Telegram Edit Message Tool ──────────────────────────────────

pub struct TelegramEditTool {
    channel: Arc<TelegramChannel>,
    context: Arc<Mutex<Option<TelegramToolContext>>>,
}

impl TelegramEditTool {
    pub fn new(
        channel: Arc<TelegramChannel>,
        context: Arc<Mutex<Option<TelegramToolContext>>>,
    ) -> Self {
        Self { channel, context }
    }
}

#[async_trait]
impl Tool for TelegramEditTool {
    fn name(&self) -> &str {
        "telegram_edit_message"
    }

    fn description(&self) -> &str {
        "Edit an existing Telegram message's text and optionally its inline keyboard."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        serde_json::json!({
            "type": "object",
            "properties": {
                "message_id": {
                    "type": "integer",
                    "description": "ID of the message to edit"
                },
                "text": {
                    "type": "string",
                    "description": "New message text"
                },
                "buttons": {
                    "type": "array",
                    "description": "Optional new inline keyboard buttons",
                    "items": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "text": { "type": "string" },
                                "callback_data": { "type": "string" }
                            },
                            "required": ["text", "callback_data"]
                        }
                    }
                },
                "chat_id": {
                    "type": "string",
                    "description": "Target chat ID (auto-resolved from context if omitted)"
                }
            },
            "required": ["message_id", "text"]
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

        let message_id = args["message_id"].as_i64().unwrap_or_default();
        let text = args["text"].as_str().unwrap_or("");

        let buttons: Option<Vec<Vec<InlineButton>>> = args.get("buttons").and_then(|b| {
            b.as_array().map(|rows| {
                rows.iter()
                    .map(|row| {
                        row.as_array()
                            .unwrap_or(&vec![])
                            .iter()
                            .map(|btn| InlineButton {
                                text: btn["text"].as_str().unwrap_or("").to_string(),
                                callback_data: btn["callback_data"]
                                    .as_str()
                                    .unwrap_or("")
                                    .to_string(),
                            })
                            .collect()
                    })
                    .collect()
            })
        });

        match self
            .channel
            .edit_message_text(
                &chat_id,
                message_id,
                text,
                buttons.as_deref(),
            )
            .await
        {
            Ok(()) => Ok(ToolResult {
                success: true,
                output: "Message edited successfully.".into(),
                error: None,
            }),
            Err(e) => Ok(ToolResult {
                success: false,
                output: String::new(),
                error: Some(e.to_string()),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_context(chat_id: &str) -> Arc<Mutex<Option<TelegramToolContext>>> {
        Arc::new(Mutex::new(Some(TelegramToolContext {
            chat_id: chat_id.into(),
            channel: "telegram".into(),
        })))
    }

    #[test]
    fn resolve_chat_id_from_args() {
        let ctx = Arc::new(Mutex::new(None));
        let args = serde_json::json!({"chat_id": "12345"});
        assert_eq!(resolve_chat_id(&args, &ctx), Some("12345".into()));
    }

    #[test]
    fn resolve_chat_id_from_context() {
        let ctx = make_context("99999");
        let args = serde_json::json!({});
        assert_eq!(resolve_chat_id(&args, &ctx), Some("99999".into()));
    }

    #[test]
    fn resolve_chat_id_args_over_context() {
        let ctx = make_context("99999");
        let args = serde_json::json!({"chat_id": "12345"});
        assert_eq!(resolve_chat_id(&args, &ctx), Some("12345".into()));
    }

    #[test]
    fn resolve_chat_id_none() {
        let ctx = Arc::new(Mutex::new(None));
        let args = serde_json::json!({});
        assert_eq!(resolve_chat_id(&args, &ctx), None);
    }

    #[test]
    fn keyboard_tool_schema_valid() {
        let ch = Arc::new(TelegramChannel::new("t".into(), vec![]));
        let ctx = Arc::new(Mutex::new(None));
        let tool = TelegramKeyboardTool::new(ch, ctx);
        let schema = tool.parameters_schema();
        assert!(schema["properties"]["text"].is_object());
        assert!(schema["properties"]["buttons"].is_object());
        assert_eq!(tool.name(), "telegram_send_keyboard");
    }

    #[test]
    fn poll_tool_schema_valid() {
        let ch = Arc::new(TelegramChannel::new("t".into(), vec![]));
        let ctx = Arc::new(Mutex::new(None));
        let tool = TelegramPollTool::new(ch, ctx);
        let schema = tool.parameters_schema();
        assert!(schema["properties"]["question"].is_object());
        assert!(schema["properties"]["options"].is_object());
        assert_eq!(tool.name(), "telegram_send_poll");
    }

    #[test]
    fn edit_tool_schema_valid() {
        let ch = Arc::new(TelegramChannel::new("t".into(), vec![]));
        let ctx = Arc::new(Mutex::new(None));
        let tool = TelegramEditTool::new(ch, ctx);
        let schema = tool.parameters_schema();
        assert!(schema["properties"]["message_id"].is_object());
        assert!(schema["properties"]["text"].is_object());
        assert_eq!(tool.name(), "telegram_edit_message");
    }
}
