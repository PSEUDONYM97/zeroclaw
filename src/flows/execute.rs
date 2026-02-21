use super::types::{ButtonDef, Step, StepKind};
use crate::channels::telegram::TelegramChannel;
use crate::channels::telegram_types::InlineButton;

/// Execute a flow step against the Telegram API.
/// Returns the message_id of the sent/edited message (for anchor tracking).
pub async fn execute_step(
    channel: &TelegramChannel,
    chat_id: &str,
    step: &Step,
    anchor_message_id: Option<i64>,
) -> anyhow::Result<Option<i64>> {
    match step.kind {
        StepKind::Keyboard => {
            let buttons = step
                .buttons
                .as_ref()
                .map(|rows| {
                    rows.iter()
                        .map(|row| row.iter().map(|b| button_def_to_inline(b)).collect())
                        .collect::<Vec<Vec<InlineButton>>>()
                })
                .unwrap_or_default();
            let msg_id = channel
                .send_with_keyboard(chat_id, &step.text, &buttons)
                .await?;
            Ok(Some(msg_id))
        }
        StepKind::Poll => {
            let options: Vec<String> = step
                .poll_options
                .as_ref()
                .cloned()
                .unwrap_or_default();
            let msg_id = channel
                .send_poll(chat_id, &step.text, &options, step.poll_anonymous)
                .await?;
            Ok(Some(msg_id))
        }
        StepKind::Message => {
            // Send plain text -- use the Channel::send method by building a minimal
            // ChannelMessage for context, or just call sendMessage directly.
            let url = channel.api_url("sendMessage");
            let body = serde_json::json!({
                "chat_id": chat_id,
                "text": &step.text,
                "parse_mode": "Markdown",
            });
            let resp = channel.http_client().post(&url).json(&body).send().await?;
            let data: serde_json::Value = resp.json().await?;
            let msg_id = data["result"]["message_id"].as_i64();
            Ok(msg_id)
        }
        StepKind::Edit => {
            if let Some(anchor_id) = anchor_message_id {
                let buttons: Option<Vec<Vec<InlineButton>>> = step.buttons.as_ref().map(|rows| {
                    rows.iter()
                        .map(|row| row.iter().map(|b| button_def_to_inline(b)).collect())
                        .collect()
                });
                channel
                    .edit_message_text(chat_id, anchor_id, &step.text, buttons.as_deref())
                    .await?;
                Ok(Some(anchor_id))
            } else {
                // No anchor -- fall back to sending a new message
                tracing::warn!("edit step '{}' has no anchor message_id, sending new message", step.id);
                let url = channel.api_url("sendMessage");
                let body = serde_json::json!({
                    "chat_id": chat_id,
                    "text": &step.text,
                    "parse_mode": "Markdown",
                });
                let resp = channel.http_client().post(&url).json(&body).send().await?;
                let data: serde_json::Value = resp.json().await?;
                let msg_id = data["result"]["message_id"].as_i64();
                Ok(msg_id)
            }
        }
    }
}

fn button_def_to_inline(b: &ButtonDef) -> InlineButton {
    InlineButton {
        text: b.text.clone(),
        callback_data: b.callback_data.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn button_def_conversion() {
        let def = ButtonDef {
            text: "OK".into(),
            callback_data: "ok_data".into(),
        };
        let inline = button_def_to_inline(&def);
        assert_eq!(inline.text, "OK");
        assert_eq!(inline.callback_data, "ok_data");
    }
}
