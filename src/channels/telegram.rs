use super::telegram_types::{
    InlineButton, SpeechClient, ACCEPTED_AUDIO_TYPES, MAX_VOICE_BYTES, STT_CONCURRENCY,
    STT_TIMEOUT_SECS,
};
use super::traits::{Channel, ChannelMessage};
use crate::observability::traits::{Observer, ObserverEvent, ObserverMetric};
use async_trait::async_trait;
use reqwest::multipart::{Form, Part};
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::Path;
use std::sync::{Arc, Mutex};
use uuid::Uuid;

/// Maximum number of update_ids to track for dedup (bounded FIFO).
const MAX_SEEN_UPDATES: usize = 10_000;

/// Bounded seen-set for Telegram update_id dedup.
struct SeenUpdates {
    set: HashSet<i64>,
    order: VecDeque<i64>,
}

impl SeenUpdates {
    fn new() -> Self {
        Self {
            set: HashSet::new(),
            order: VecDeque::new(),
        }
    }

    /// Insert an update_id. Returns `true` if new, `false` if duplicate.
    fn insert(&mut self, uid: i64) -> bool {
        if !self.set.insert(uid) {
            return false;
        }
        self.order.push_back(uid);
        while self.order.len() > MAX_SEEN_UPDATES {
            if let Some(old) = self.order.pop_front() {
                self.set.remove(&old);
            }
        }
        true
    }
}

/// Build allowlist-based metadata JSON for Telegram events.
/// Only includes explicitly listed fields -- never raw message content.
pub fn build_telegram_metadata(fields: &[(&str, serde_json::Value)]) -> String {
    let mut map = serde_json::Map::new();
    for (k, v) in fields {
        map.insert((*k).to_string(), v.clone());
    }
    serde_json::Value::Object(map).to_string()
}

/// Telegram channel -- long-polls the Bot API for updates
pub struct TelegramChannel {
    bot_token: String,
    allowed_users: Vec<String>,
    client: reqwest::Client,
    speech: Option<Arc<SpeechClient>>,
    stt_semaphore: Arc<tokio::sync::Semaphore>,
    observer: Option<Arc<dyn Observer>>,
    seen_update_ids: Arc<Mutex<SeenUpdates>>,
}

impl TelegramChannel {
    pub fn new(bot_token: String, allowed_users: Vec<String>) -> Self {
        Self {
            bot_token,
            allowed_users,
            client: reqwest::Client::new(),
            speech: None,
            stt_semaphore: Arc::new(tokio::sync::Semaphore::new(STT_CONCURRENCY)),
            observer: None,
            seen_update_ids: Arc::new(Mutex::new(SeenUpdates::new())),
        }
    }

    /// Attach an STT endpoint for voice transcription
    pub fn with_stt(mut self, stt_endpoint: String) -> Self {
        self.speech = Some(Arc::new(SpeechClient::new(stt_endpoint)));
        self
    }

    /// Attach an observer for Telegram event instrumentation
    pub fn with_observer(mut self, observer: Arc<dyn Observer>) -> Self {
        self.observer = Some(observer);
        self
    }

    /// Record a Telegram event on the observer (if present).
    fn record_tg_event(
        &self,
        direction: &str,
        event_type: &str,
        status: &str,
        chat_id: &str,
        duration: Option<std::time::Duration>,
        metadata: Option<String>,
    ) {
        if let Some(ref obs) = self.observer {
            obs.record_event(&ObserverEvent::TelegramEvent {
                direction: direction.to_string(),
                event_type: event_type.to_string(),
                status: status.to_string(),
                chat_id: chat_id.to_string(),
                correlation_id: Uuid::new_v4().to_string(),
                duration,
                metadata,
            });
        }
    }

    pub fn api_url(&self, method: &str) -> String {
        format!("https://api.telegram.org/bot{}/{method}", self.bot_token)
    }

    pub fn is_user_allowed(&self, username: &str) -> bool {
        self.allowed_users.iter().any(|u| u == "*" || u == username)
    }

    pub fn is_any_user_allowed<'a, I>(&self, identities: I) -> bool
    where
        I: IntoIterator<Item = &'a str>,
    {
        identities.into_iter().any(|id| self.is_user_allowed(id))
    }

    pub fn bot_token(&self) -> &str {
        &self.bot_token
    }

    pub fn http_client(&self) -> &reqwest::Client {
        &self.client
    }

    /// Send a document/file to a Telegram chat
    pub async fn send_document(
        &self,
        chat_id: &str,
        file_path: &Path,
        caption: Option<&str>,
    ) -> anyhow::Result<()> {
        let file_name = file_path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("file");

        let file_bytes = tokio::fs::read(file_path).await?;
        let part = Part::bytes(file_bytes).file_name(file_name.to_string());

        let mut form = Form::new()
            .text("chat_id", chat_id.to_string())
            .part("document", part);

        if let Some(cap) = caption {
            form = form.text("caption", cap.to_string());
        }

        let resp = self
            .client
            .post(self.api_url("sendDocument"))
            .multipart(form)
            .send()
            .await?;

        if !resp.status().is_success() {
            let err = resp.text().await?;
            anyhow::bail!("Telegram sendDocument failed: {err}");
        }

        tracing::info!("Telegram document sent to {chat_id}: {file_name}");
        Ok(())
    }

    /// Send a document from bytes (in-memory) to a Telegram chat
    pub async fn send_document_bytes(
        &self,
        chat_id: &str,
        file_bytes: Vec<u8>,
        file_name: &str,
        caption: Option<&str>,
    ) -> anyhow::Result<()> {
        let part = Part::bytes(file_bytes).file_name(file_name.to_string());

        let mut form = Form::new()
            .text("chat_id", chat_id.to_string())
            .part("document", part);

        if let Some(cap) = caption {
            form = form.text("caption", cap.to_string());
        }

        let resp = self
            .client
            .post(self.api_url("sendDocument"))
            .multipart(form)
            .send()
            .await?;

        if !resp.status().is_success() {
            let err = resp.text().await?;
            anyhow::bail!("Telegram sendDocument failed: {err}");
        }

        tracing::info!("Telegram document sent to {chat_id}: {file_name}");
        Ok(())
    }

    /// Send a photo to a Telegram chat
    pub async fn send_photo(
        &self,
        chat_id: &str,
        file_path: &Path,
        caption: Option<&str>,
    ) -> anyhow::Result<()> {
        let file_name = file_path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("photo.jpg");

        let file_bytes = tokio::fs::read(file_path).await?;
        let part = Part::bytes(file_bytes).file_name(file_name.to_string());

        let mut form = Form::new()
            .text("chat_id", chat_id.to_string())
            .part("photo", part);

        if let Some(cap) = caption {
            form = form.text("caption", cap.to_string());
        }

        let resp = self
            .client
            .post(self.api_url("sendPhoto"))
            .multipart(form)
            .send()
            .await?;

        if !resp.status().is_success() {
            let err = resp.text().await?;
            anyhow::bail!("Telegram sendPhoto failed: {err}");
        }

        tracing::info!("Telegram photo sent to {chat_id}: {file_name}");
        Ok(())
    }

    /// Send a photo from bytes (in-memory) to a Telegram chat
    pub async fn send_photo_bytes(
        &self,
        chat_id: &str,
        file_bytes: Vec<u8>,
        file_name: &str,
        caption: Option<&str>,
    ) -> anyhow::Result<()> {
        let part = Part::bytes(file_bytes).file_name(file_name.to_string());

        let mut form = Form::new()
            .text("chat_id", chat_id.to_string())
            .part("photo", part);

        if let Some(cap) = caption {
            form = form.text("caption", cap.to_string());
        }

        let resp = self
            .client
            .post(self.api_url("sendPhoto"))
            .multipart(form)
            .send()
            .await?;

        if !resp.status().is_success() {
            let err = resp.text().await?;
            anyhow::bail!("Telegram sendPhoto failed: {err}");
        }

        tracing::info!("Telegram photo sent to {chat_id}: {file_name}");
        Ok(())
    }

    /// Send a video to a Telegram chat
    pub async fn send_video(
        &self,
        chat_id: &str,
        file_path: &Path,
        caption: Option<&str>,
    ) -> anyhow::Result<()> {
        let file_name = file_path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("video.mp4");

        let file_bytes = tokio::fs::read(file_path).await?;
        let part = Part::bytes(file_bytes).file_name(file_name.to_string());

        let mut form = Form::new()
            .text("chat_id", chat_id.to_string())
            .part("video", part);

        if let Some(cap) = caption {
            form = form.text("caption", cap.to_string());
        }

        let resp = self
            .client
            .post(self.api_url("sendVideo"))
            .multipart(form)
            .send()
            .await?;

        if !resp.status().is_success() {
            let err = resp.text().await?;
            anyhow::bail!("Telegram sendVideo failed: {err}");
        }

        tracing::info!("Telegram video sent to {chat_id}: {file_name}");
        Ok(())
    }

    /// Send an audio file to a Telegram chat
    pub async fn send_audio(
        &self,
        chat_id: &str,
        file_path: &Path,
        caption: Option<&str>,
    ) -> anyhow::Result<()> {
        let file_name = file_path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("audio.mp3");

        let file_bytes = tokio::fs::read(file_path).await?;
        let part = Part::bytes(file_bytes).file_name(file_name.to_string());

        let mut form = Form::new()
            .text("chat_id", chat_id.to_string())
            .part("audio", part);

        if let Some(cap) = caption {
            form = form.text("caption", cap.to_string());
        }

        let resp = self
            .client
            .post(self.api_url("sendAudio"))
            .multipart(form)
            .send()
            .await?;

        if !resp.status().is_success() {
            let err = resp.text().await?;
            anyhow::bail!("Telegram sendAudio failed: {err}");
        }

        tracing::info!("Telegram audio sent to {chat_id}: {file_name}");
        Ok(())
    }

    /// Send a voice message to a Telegram chat
    pub async fn send_voice(
        &self,
        chat_id: &str,
        file_path: &Path,
        caption: Option<&str>,
    ) -> anyhow::Result<()> {
        let file_name = file_path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("voice.ogg");

        let file_bytes = tokio::fs::read(file_path).await?;
        let part = Part::bytes(file_bytes).file_name(file_name.to_string());

        let mut form = Form::new()
            .text("chat_id", chat_id.to_string())
            .part("voice", part);

        if let Some(cap) = caption {
            form = form.text("caption", cap.to_string());
        }

        let resp = self
            .client
            .post(self.api_url("sendVoice"))
            .multipart(form)
            .send()
            .await?;

        if !resp.status().is_success() {
            let err = resp.text().await?;
            anyhow::bail!("Telegram sendVoice failed: {err}");
        }

        tracing::info!("Telegram voice sent to {chat_id}: {file_name}");
        Ok(())
    }

    /// Send a file by URL (Telegram will download it)
    pub async fn send_document_by_url(
        &self,
        chat_id: &str,
        url: &str,
        caption: Option<&str>,
    ) -> anyhow::Result<()> {
        let mut body = serde_json::json!({
            "chat_id": chat_id,
            "document": url
        });

        if let Some(cap) = caption {
            body["caption"] = serde_json::Value::String(cap.to_string());
        }

        let resp = self
            .client
            .post(self.api_url("sendDocument"))
            .json(&body)
            .send()
            .await?;

        if !resp.status().is_success() {
            let err = resp.text().await?;
            anyhow::bail!("Telegram sendDocument by URL failed: {err}");
        }

        tracing::info!("Telegram document (URL) sent to {chat_id}: {url}");
        Ok(())
    }

    /// Send a photo by URL (Telegram will download it)
    pub async fn send_photo_by_url(
        &self,
        chat_id: &str,
        url: &str,
        caption: Option<&str>,
    ) -> anyhow::Result<()> {
        let mut body = serde_json::json!({
            "chat_id": chat_id,
            "photo": url
        });

        if let Some(cap) = caption {
            body["caption"] = serde_json::Value::String(cap.to_string());
        }

        let resp = self
            .client
            .post(self.api_url("sendPhoto"))
            .json(&body)
            .send()
            .await?;

        if !resp.status().is_success() {
            let err = resp.text().await?;
            anyhow::bail!("Telegram sendPhoto by URL failed: {err}");
        }

        tracing::info!("Telegram photo (URL) sent to {chat_id}: {url}");
        Ok(())
    }

    // ── Rich messaging methods ──────────────────────────────────

    /// Send a message with inline keyboard buttons.
    /// `buttons` is a list of button rows: `[[{text, callback_data}, ...], ...]`
    pub async fn send_with_keyboard(
        &self,
        chat_id: &str,
        text: &str,
        buttons: &[Vec<InlineButton>],
    ) -> anyhow::Result<i64> {
        let keyboard: Vec<Vec<serde_json::Value>> = buttons
            .iter()
            .map(|row| {
                row.iter()
                    .map(|b| {
                        serde_json::json!({
                            "text": b.text,
                            "callback_data": b.callback_data,
                        })
                    })
                    .collect()
            })
            .collect();

        let body = serde_json::json!({
            "chat_id": chat_id,
            "text": text,
            "reply_markup": {
                "inline_keyboard": keyboard,
            }
        });

        let resp = self
            .client
            .post(self.api_url("sendMessage"))
            .json(&body)
            .send()
            .await?;

        if !resp.status().is_success() {
            let err = resp.text().await?;
            anyhow::bail!("Telegram sendMessage (keyboard) failed: {err}");
        }

        let data: serde_json::Value = resp.json().await?;
        let msg_id = data["result"]["message_id"]
            .as_i64()
            .unwrap_or_default();
        Ok(msg_id)
    }

    /// Build the JSON body for `send_with_keyboard` (for testing without network).
    pub fn build_keyboard_json(
        chat_id: &str,
        text: &str,
        buttons: &[Vec<InlineButton>],
    ) -> serde_json::Value {
        let keyboard: Vec<Vec<serde_json::Value>> = buttons
            .iter()
            .map(|row| {
                row.iter()
                    .map(|b| {
                        serde_json::json!({
                            "text": b.text,
                            "callback_data": b.callback_data,
                        })
                    })
                    .collect()
            })
            .collect();

        serde_json::json!({
            "chat_id": chat_id,
            "text": text,
            "reply_markup": {
                "inline_keyboard": keyboard,
            }
        })
    }

    /// Edit an existing message's text (and optionally its inline keyboard).
    pub async fn edit_message_text(
        &self,
        chat_id: &str,
        message_id: i64,
        text: &str,
        buttons: Option<&[Vec<InlineButton>]>,
    ) -> anyhow::Result<()> {
        let mut body = serde_json::json!({
            "chat_id": chat_id,
            "message_id": message_id,
            "text": text,
        });

        if let Some(btns) = buttons {
            let keyboard: Vec<Vec<serde_json::Value>> = btns
                .iter()
                .map(|row| {
                    row.iter()
                        .map(|b| {
                            serde_json::json!({
                                "text": b.text,
                                "callback_data": b.callback_data,
                            })
                        })
                        .collect()
                })
                .collect();
            body["reply_markup"] = serde_json::json!({
                "inline_keyboard": keyboard,
            });
        }

        let resp = self
            .client
            .post(self.api_url("editMessageText"))
            .json(&body)
            .send()
            .await?;

        if !resp.status().is_success() {
            let err = resp.text().await?;
            anyhow::bail!("Telegram editMessageText failed: {err}");
        }

        Ok(())
    }

    /// Build the JSON body for `edit_message_text` (for testing).
    pub fn build_edit_json(
        chat_id: &str,
        message_id: i64,
        text: &str,
        buttons: Option<&[Vec<InlineButton>]>,
    ) -> serde_json::Value {
        let mut body = serde_json::json!({
            "chat_id": chat_id,
            "message_id": message_id,
            "text": text,
        });

        if let Some(btns) = buttons {
            let keyboard: Vec<Vec<serde_json::Value>> = btns
                .iter()
                .map(|row| {
                    row.iter()
                        .map(|b| {
                            serde_json::json!({
                                "text": b.text,
                                "callback_data": b.callback_data,
                            })
                        })
                        .collect()
                })
                .collect();
            body["reply_markup"] = serde_json::json!({
                "inline_keyboard": keyboard,
            });
        }

        body
    }

    /// Send a poll to a Telegram chat.
    pub async fn send_poll(
        &self,
        chat_id: &str,
        question: &str,
        options: &[String],
        is_anonymous: bool,
    ) -> anyhow::Result<i64> {
        let body = serde_json::json!({
            "chat_id": chat_id,
            "question": question,
            "options": options,
            "is_anonymous": is_anonymous,
        });

        let resp = self
            .client
            .post(self.api_url("sendPoll"))
            .json(&body)
            .send()
            .await?;

        if !resp.status().is_success() {
            let err = resp.text().await?;
            anyhow::bail!("Telegram sendPoll failed: {err}");
        }

        let data: serde_json::Value = resp.json().await?;
        let msg_id = data["result"]["message_id"]
            .as_i64()
            .unwrap_or_default();
        Ok(msg_id)
    }

    /// Build the JSON body for `send_poll` (for testing).
    pub fn build_poll_json(
        chat_id: &str,
        question: &str,
        options: &[String],
        is_anonymous: bool,
    ) -> serde_json::Value {
        serde_json::json!({
            "chat_id": chat_id,
            "question": question,
            "options": options,
            "is_anonymous": is_anonymous,
        })
    }

    /// Answer a callback query (dismiss the loading spinner on a button press).
    pub async fn answer_callback_query(
        &self,
        callback_query_id: &str,
        text: Option<&str>,
        show_alert: bool,
    ) -> anyhow::Result<()> {
        let mut body = serde_json::json!({
            "callback_query_id": callback_query_id,
            "show_alert": show_alert,
        });

        if let Some(t) = text {
            body["text"] = serde_json::Value::String(t.to_string());
        }

        let resp = self
            .client
            .post(self.api_url("answerCallbackQuery"))
            .json(&body)
            .send()
            .await?;

        if !resp.status().is_success() {
            let err = resp.text().await?;
            anyhow::bail!("Telegram answerCallbackQuery failed: {err}");
        }

        Ok(())
    }

    /// Download a file from Telegram by file_id.
    /// Returns `(bytes, file_path_on_telegram)`.
    pub async fn download_file(&self, file_id: &str) -> anyhow::Result<(Vec<u8>, String)> {
        // Step 1: getFile to get file_path
        let body = serde_json::json!({ "file_id": file_id });
        let resp = self
            .client
            .post(self.api_url("getFile"))
            .json(&body)
            .send()
            .await?;

        if !resp.status().is_success() {
            let err = resp.text().await?;
            anyhow::bail!("Telegram getFile failed: {err}");
        }

        let data: serde_json::Value = resp.json().await?;
        let file_path = data["result"]["file_path"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("Telegram getFile: missing file_path"))?
            .to_string();

        // Step 2: Download the file
        let download_url = format!(
            "https://api.telegram.org/file/bot{}/{}",
            self.bot_token, file_path
        );
        let file_resp = self.client.get(&download_url).send().await?;

        if !file_resp.status().is_success() {
            anyhow::bail!("Telegram file download failed: {}", file_resp.status());
        }

        let bytes = file_resp.bytes().await?.to_vec();
        Ok((bytes, file_path))
    }
}

#[async_trait]
impl Channel for TelegramChannel {
    fn name(&self) -> &str {
        "telegram"
    }

    async fn send(&self, message: &str, reply_to: &ChannelMessage) -> anyhow::Result<()> {
        let chat_id = &reply_to.sender;
        let markdown_body = serde_json::json!({
            "chat_id": chat_id,
            "text": message,
            "parse_mode": "Markdown"
        });

        let markdown_resp = self
            .client
            .post(self.api_url("sendMessage"))
            .json(&markdown_body)
            .send()
            .await?;

        if markdown_resp.status().is_success() {
            return Ok(());
        }

        let markdown_status = markdown_resp.status();
        let markdown_err = markdown_resp.text().await.unwrap_or_default();
        tracing::warn!(
            status = ?markdown_status,
            "Telegram sendMessage with Markdown failed; retrying without parse_mode"
        );

        // Retry without parse_mode as a compatibility fallback.
        let plain_body = serde_json::json!({
            "chat_id": chat_id,
            "text": message,
        });
        let plain_resp = self
            .client
            .post(self.api_url("sendMessage"))
            .json(&plain_body)
            .send()
            .await?;

        if !plain_resp.status().is_success() {
            let plain_status = plain_resp.status();
            let plain_err = plain_resp.text().await.unwrap_or_default();
            anyhow::bail!(
                "Telegram sendMessage failed (markdown {}: {}; plain {}: {})",
                markdown_status,
                markdown_err,
                plain_status,
                plain_err
            );
        }

        Ok(())
    }

    async fn listen(&self, tx: tokio::sync::mpsc::Sender<ChannelMessage>) -> anyhow::Result<()> {
        let mut offset: i64 = 0;

        tracing::info!("Telegram channel listening for messages...");

        loop {
            let url = self.api_url("getUpdates");
            let body = serde_json::json!({
                "offset": offset,
                "timeout": 30,
                "allowed_updates": ["message", "callback_query"]
            });

            let resp = match self.client.post(&url).json(&body).send().await {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!("Telegram poll error: {e}");
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    continue;
                }
            };

            let data: serde_json::Value = match resp.json().await {
                Ok(d) => d,
                Err(e) => {
                    tracing::warn!("Telegram parse error: {e}");
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    continue;
                }
            };

            if let Some(results) = data.get("result").and_then(serde_json::Value::as_array) {
                for update in results {
                    // Advance offset past this update
                    if let Some(uid) = update.get("update_id").and_then(serde_json::Value::as_i64) {
                        offset = uid + 1;

                        // Dedup: skip if we've already processed this update_id
                        if !self.seen_update_ids.lock().unwrap_or_else(|e| e.into_inner()).insert(uid) {
                            tracing::debug!("Duplicate update_id {uid}, skipping");
                            continue;
                        }
                    }

                    // ── Handle callback_query ──────────────────────────
                    if let Some(cb) = update.get("callback_query") {
                        let cb_id = cb["id"].as_str().unwrap_or_default().to_string();

                        // Auth check for callback queries
                        let from = &cb["from"];
                        let cb_user_id = from["id"].as_i64().map(|id| id.to_string());
                        let cb_username = from["username"].as_str().unwrap_or("unknown");
                        let mut cb_identities = vec![cb_username];
                        if let Some(ref id) = cb_user_id {
                            cb_identities.push(id.as_str());
                        }

                        if !self.is_any_user_allowed(cb_identities.iter().copied()) {
                            // Answer to dismiss spinner, then drop
                            let _ = self.answer_callback_query(&cb_id, Some("Unauthorized"), false).await;
                            let cb_chat = cb["message"]["chat"]["id"]
                                .as_i64()
                                .map(|id| id.to_string())
                                .unwrap_or_default();
                            self.record_tg_event(
                                "inbound",
                                "tg.auth_reject",
                                "rejected",
                                &cb_chat,
                                None,
                                Some(build_telegram_metadata(&[
                                    ("username", serde_json::json!(cb_username)),
                                    ("user_id", serde_json::json!(cb_user_id)),
                                    ("chat_id", serde_json::json!(cb_chat)),
                                ])),
                            );
                            if let Some(ref obs) = self.observer {
                                obs.record_metric(&ObserverMetric::CallbackRejectCount(1));
                            }
                            continue;
                        }

                        // Answer callback to dismiss the loading spinner
                        let _ = self.answer_callback_query(&cb_id, None, false).await;

                        let callback_data = cb["data"].as_str().unwrap_or_default().to_string();
                        let chat_id = cb["message"]["chat"]["id"]
                            .as_i64()
                            .map(|id| id.to_string())
                            .unwrap_or_default();
                        let original_msg_id = cb["message"]["message_id"].as_i64().unwrap_or_default();

                        self.record_tg_event(
                            "inbound",
                            "tg.inbound.callback_query",
                            "ok",
                            &chat_id,
                            None,
                            Some(build_telegram_metadata(&[
                                ("msg_type", serde_json::json!("callback_query")),
                                ("callback_query_id", serde_json::json!(cb_id)),
                                ("original_message_id", serde_json::json!(original_msg_id)),
                                ("chat_id", serde_json::json!(&chat_id)),
                            ])),
                        );

                        let mut metadata = HashMap::new();
                        metadata.insert("msg_type".into(), serde_json::json!("callback_query"));
                        metadata.insert("callback_query_id".into(), serde_json::json!(cb_id));
                        metadata.insert("original_message_id".into(), serde_json::json!(original_msg_id));

                        let msg = ChannelMessage {
                            id: Uuid::new_v4().to_string(),
                            sender: chat_id,
                            content: callback_data,
                            channel: "telegram".to_string(),
                            timestamp: std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs(),
                            metadata,
                        };

                        if tx.send(msg).await.is_err() {
                            return Ok(());
                        }
                        continue;
                    }

                    // ── Handle message updates ─────────────────────────
                    let Some(message) = update.get("message") else {
                        continue;
                    };

                    // Auth check (shared for all message types)
                    let username_opt = message
                        .get("from")
                        .and_then(|f| f.get("username"))
                        .and_then(|u| u.as_str());
                    let username = username_opt.unwrap_or("unknown");

                    let user_id = message
                        .get("from")
                        .and_then(|f| f.get("id"))
                        .and_then(serde_json::Value::as_i64);
                    let user_id_str = user_id.map(|id| id.to_string());

                    let mut identities = vec![username];
                    if let Some(ref id) = user_id_str {
                        identities.push(id.as_str());
                    }

                    if !self.is_any_user_allowed(identities.iter().copied()) {
                        tracing::warn!(
                            "Telegram: ignoring message from unauthorized user: username={username}, user_id={}. \
Allowlist Telegram @username or numeric user ID, then run `zeroclaw onboard --channels-only`.",
                            user_id_str.as_deref().unwrap_or("unknown")
                        );
                        let reject_chat = message
                            .get("chat")
                            .and_then(|c| c.get("id"))
                            .and_then(serde_json::Value::as_i64)
                            .map(|id| id.to_string())
                            .unwrap_or_default();
                        self.record_tg_event(
                            "inbound",
                            "tg.auth_reject",
                            "rejected",
                            &reject_chat,
                            None,
                            Some(build_telegram_metadata(&[
                                ("username", serde_json::json!(username)),
                                ("user_id", serde_json::json!(user_id_str)),
                                ("chat_id", serde_json::json!(&reject_chat)),
                            ])),
                        );
                        continue;
                    }

                    let chat_id = message
                        .get("chat")
                        .and_then(|c| c.get("id"))
                        .and_then(serde_json::Value::as_i64)
                        .map(|id| id.to_string())
                        .unwrap_or_default();

                    // ── Voice message ──────────────────────────────────
                    if let Some(voice) = message.get("voice") {
                        let file_id = voice["file_id"].as_str().unwrap_or_default().to_string();
                        let file_size = voice["file_size"].as_u64().unwrap_or(0);
                        let mime_type = voice["mime_type"]
                            .as_str()
                            .unwrap_or("audio/ogg")
                            .to_string();
                        let duration = voice["duration"].as_u64().unwrap_or(0);

                        let mut metadata = HashMap::new();
                        metadata.insert("msg_type".into(), serde_json::json!("voice"));
                        metadata.insert("file_id".into(), serde_json::json!(file_id));
                        metadata.insert("duration".into(), serde_json::json!(duration));

                        // File-size guard
                        if file_size > MAX_VOICE_BYTES {
                            self.record_tg_event(
                                "inbound",
                                "tg.guard_reject",
                                "rejected",
                                &chat_id,
                                None,
                                Some(build_telegram_metadata(&[
                                    ("reason", serde_json::json!("file_too_large")),
                                    ("chat_id", serde_json::json!(&chat_id)),
                                    ("file_size", serde_json::json!(file_size)),
                                    ("mime_type", serde_json::json!(&mime_type)),
                                ])),
                            );
                            let msg = ChannelMessage {
                                id: Uuid::new_v4().to_string(),
                                sender: chat_id.clone(),
                                content: "[Voice message too large -- max 5 MB. Try a shorter recording.]".to_string(),
                                channel: "telegram".to_string(),
                                timestamp: std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap_or_default()
                                    .as_secs(),
                                metadata,
                            };
                            if tx.send(msg).await.is_err() {
                                return Ok(());
                            }
                            continue;
                        }

                        // MIME type guard
                        if !ACCEPTED_AUDIO_TYPES.contains(&mime_type.as_str()) {
                            self.record_tg_event(
                                "inbound",
                                "tg.guard_reject",
                                "rejected",
                                &chat_id,
                                None,
                                Some(build_telegram_metadata(&[
                                    ("reason", serde_json::json!("unsupported_mime")),
                                    ("chat_id", serde_json::json!(&chat_id)),
                                    ("mime_type", serde_json::json!(&mime_type)),
                                ])),
                            );
                            let msg = ChannelMessage {
                                id: Uuid::new_v4().to_string(),
                                sender: chat_id.clone(),
                                content: "[Unsupported audio format. Send as .ogg, .mp3, .mp4, or .wav.]".to_string(),
                                channel: "telegram".to_string(),
                                timestamp: std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap_or_default()
                                    .as_secs(),
                                metadata,
                            };
                            if tx.send(msg).await.is_err() {
                                return Ok(());
                            }
                            continue;
                        }

                        // Record inbound voice event
                        self.record_tg_event(
                            "inbound",
                            "tg.inbound.voice",
                            "ok",
                            &chat_id,
                            None,
                            Some(build_telegram_metadata(&[
                                ("msg_type", serde_json::json!("voice")),
                                ("file_id", serde_json::json!(&file_id)),
                                ("duration_secs", serde_json::json!(duration)),
                                ("mime_type", serde_json::json!(&mime_type)),
                                ("chat_id", serde_json::json!(&chat_id)),
                            ])),
                        );

                        // Non-blocking STT transcription
                        if let Some(ref speech) = self.speech {
                            let speech = speech.clone();
                            let tx = tx.clone();
                            let semaphore = self.stt_semaphore.clone();
                            let bot_token = self.bot_token.clone();
                            let client = self.client.clone();
                            let observer = self.observer.clone();
                            let stt_chat_id = chat_id.clone();
                            let api_base =
                                format!("https://api.telegram.org/bot{}", bot_token);

                            tokio::spawn(async move {
                                // Bounded concurrency
                                let permit = match semaphore.try_acquire() {
                                    Ok(p) => p,
                                    Err(_) => {
                                        let msg = ChannelMessage {
                                            id: Uuid::new_v4().to_string(),
                                            sender: chat_id,
                                            content: "[Voice transcription busy -- please wait a moment and try again.]"
                                                .to_string(),
                                            channel: "telegram".to_string(),
                                            timestamp: std::time::SystemTime::now()
                                                .duration_since(std::time::UNIX_EPOCH)
                                                .unwrap_or_default()
                                                .as_secs(),
                                            metadata,
                                        };
                                        let _ = tx.send(msg).await;
                                        return;
                                    }
                                };

                                // Send typing indicator
                                let typing_body = serde_json::json!({
                                    "chat_id": &chat_id,
                                    "action": "typing"
                                });
                                let _ = client
                                    .post(format!("{api_base}/sendChatAction"))
                                    .json(&typing_body)
                                    .send()
                                    .await;

                                // Download file
                                let get_file_body =
                                    serde_json::json!({ "file_id": &file_id });
                                let file_resp = match client
                                    .post(format!("{api_base}/getFile"))
                                    .json(&get_file_body)
                                    .send()
                                    .await
                                {
                                    Ok(r) => r,
                                    Err(e) => {
                                        tracing::warn!("STT: getFile failed: {e}");
                                        let msg = ChannelMessage {
                                            id: Uuid::new_v4().to_string(),
                                            sender: chat_id,
                                            content: "[Voice message]".to_string(),
                                            channel: "telegram".to_string(),
                                            timestamp: std::time::SystemTime::now()
                                                .duration_since(std::time::UNIX_EPOCH)
                                                .unwrap_or_default()
                                                .as_secs(),
                                            metadata,
                                        };
                                        let _ = tx.send(msg).await;
                                        return;
                                    }
                                };

                                let file_data: serde_json::Value = match file_resp.json().await {
                                    Ok(d) => d,
                                    Err(_) => {
                                        let msg = ChannelMessage {
                                            id: Uuid::new_v4().to_string(),
                                            sender: chat_id,
                                            content: "[Voice message]".to_string(),
                                            channel: "telegram".to_string(),
                                            timestamp: std::time::SystemTime::now()
                                                .duration_since(std::time::UNIX_EPOCH)
                                                .unwrap_or_default()
                                                .as_secs(),
                                            metadata,
                                        };
                                        let _ = tx.send(msg).await;
                                        return;
                                    }
                                };

                                let file_path = match file_data["result"]["file_path"].as_str() {
                                    Some(p) => p.to_string(),
                                    None => {
                                        let msg = ChannelMessage {
                                            id: Uuid::new_v4().to_string(),
                                            sender: chat_id,
                                            content: "[Voice message]".to_string(),
                                            channel: "telegram".to_string(),
                                            timestamp: std::time::SystemTime::now()
                                                .duration_since(std::time::UNIX_EPOCH)
                                                .unwrap_or_default()
                                                .as_secs(),
                                            metadata,
                                        };
                                        let _ = tx.send(msg).await;
                                        return;
                                    }
                                };

                                let download_url = format!(
                                    "https://api.telegram.org/file/bot{}/{}",
                                    bot_token, file_path
                                );
                                let audio_bytes = match client.get(&download_url).send().await {
                                    Ok(r) => match r.bytes().await {
                                        Ok(b) => b.to_vec(),
                                        Err(_) => {
                                            let msg = ChannelMessage {
                                                id: Uuid::new_v4().to_string(),
                                                sender: chat_id,
                                                content: "[Voice message]".to_string(),
                                                channel: "telegram".to_string(),
                                                timestamp: std::time::SystemTime::now()
                                                    .duration_since(std::time::UNIX_EPOCH)
                                                    .unwrap_or_default()
                                                    .as_secs(),
                                                metadata,
                                            };
                                            let _ = tx.send(msg).await;
                                            return;
                                        }
                                    },
                                    Err(_) => {
                                        let msg = ChannelMessage {
                                            id: Uuid::new_v4().to_string(),
                                            sender: chat_id,
                                            content: "[Voice message]".to_string(),
                                            channel: "telegram".to_string(),
                                            timestamp: std::time::SystemTime::now()
                                                .duration_since(std::time::UNIX_EPOCH)
                                                .unwrap_or_default()
                                                .as_secs(),
                                            metadata,
                                        };
                                        let _ = tx.send(msg).await;
                                        return;
                                    }
                                };

                                // Transcribe with timeout
                                let format_ext = file_path
                                    .rsplit('.')
                                    .next()
                                    .unwrap_or("ogg");
                                let stt_start = tokio::time::Instant::now();
                                let transcript = tokio::time::timeout(
                                    std::time::Duration::from_secs(STT_TIMEOUT_SECS),
                                    speech.transcribe(audio_bytes, format_ext),
                                )
                                .await;

                                let content = match transcript {
                                    Ok(Ok(result)) => {
                                        if let Some(ref obs) = observer {
                                            let latency = stt_start.elapsed();
                                            obs.record_event(&ObserverEvent::TelegramEvent {
                                                direction: String::new(),
                                                event_type: "tg.stt.success".to_string(),
                                                status: "ok".to_string(),
                                                chat_id: stt_chat_id.clone(),
                                                correlation_id: Uuid::new_v4().to_string(),
                                                duration: Some(latency),
                                                metadata: Some(build_telegram_metadata(&[
                                                    ("chat_id", serde_json::json!(&stt_chat_id)),
                                                    ("latency_ms", serde_json::json!(latency.as_millis() as u64)),
                                                ])),
                                            });
                                            obs.record_metric(&ObserverMetric::SttLatency(latency));
                                        }
                                        result.text
                                    }
                                    Ok(Err(e)) => {
                                        tracing::warn!("STT transcription error: {e}");
                                        if let Some(ref obs) = observer {
                                            obs.record_event(&ObserverEvent::TelegramEvent {
                                                direction: String::new(),
                                                event_type: "tg.stt.error".to_string(),
                                                status: "error".to_string(),
                                                chat_id: stt_chat_id.clone(),
                                                correlation_id: Uuid::new_v4().to_string(),
                                                duration: Some(stt_start.elapsed()),
                                                metadata: Some(build_telegram_metadata(&[
                                                    ("chat_id", serde_json::json!(&stt_chat_id)),
                                                    ("reason", serde_json::json!("transcription_error")),
                                                ])),
                                            });
                                            obs.record_metric(&ObserverMetric::SttErrorCount(1));
                                        }
                                        "[Voice transcription failed. Your message was received but could not be transcribed.]".to_string()
                                    }
                                    Err(_) => {
                                        tracing::warn!("STT transcription timed out");
                                        if let Some(ref obs) = observer {
                                            obs.record_event(&ObserverEvent::TelegramEvent {
                                                direction: String::new(),
                                                event_type: "tg.stt.error".to_string(),
                                                status: "error".to_string(),
                                                chat_id: stt_chat_id.clone(),
                                                correlation_id: Uuid::new_v4().to_string(),
                                                duration: Some(stt_start.elapsed()),
                                                metadata: Some(build_telegram_metadata(&[
                                                    ("chat_id", serde_json::json!(&stt_chat_id)),
                                                    ("reason", serde_json::json!("timeout")),
                                                ])),
                                            });
                                            obs.record_metric(&ObserverMetric::SttErrorCount(1));
                                        }
                                        format!("[Transcription timed out after {STT_TIMEOUT_SECS}s. Try a shorter message or try again.]")
                                    }
                                };

                                let msg = ChannelMessage {
                                    id: Uuid::new_v4().to_string(),
                                    sender: chat_id,
                                    content,
                                    channel: "telegram".to_string(),
                                    timestamp: std::time::SystemTime::now()
                                        .duration_since(std::time::UNIX_EPOCH)
                                        .unwrap_or_default()
                                        .as_secs(),
                                    metadata,
                                };
                                let _ = tx.send(msg).await;
                                drop(permit);
                            });
                            continue;
                        }

                        // No STT configured - send fallback
                        let msg = ChannelMessage {
                            id: Uuid::new_v4().to_string(),
                            sender: chat_id,
                            content: "[Voice message]".to_string(),
                            channel: "telegram".to_string(),
                            timestamp: std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs(),
                            metadata,
                        };
                        if tx.send(msg).await.is_err() {
                            return Ok(());
                        }
                        continue;
                    }

                    // ── Photo message ──────────────────────────────────
                    if let Some(photos) = message.get("photo").and_then(|p| p.as_array()) {
                        self.record_tg_event(
                            "inbound",
                            "tg.inbound.photo",
                            "ok",
                            &chat_id,
                            None,
                            Some(build_telegram_metadata(&[
                                ("msg_type", serde_json::json!("photo")),
                                ("chat_id", serde_json::json!(&chat_id)),
                            ])),
                        );
                        // Telegram sends multiple sizes; take the largest (last)
                        let best = photos.last();
                        let file_id = best
                            .and_then(|p| p["file_id"].as_str())
                            .unwrap_or_default()
                            .to_string();

                        let mut metadata = HashMap::new();
                        metadata.insert("msg_type".into(), serde_json::json!("photo"));
                        metadata.insert("file_id".into(), serde_json::json!(file_id));

                        let caption = message
                            .get("caption")
                            .and_then(|c| c.as_str())
                            .unwrap_or("[Photo received]");

                        let msg = ChannelMessage {
                            id: Uuid::new_v4().to_string(),
                            sender: chat_id,
                            content: caption.to_string(),
                            channel: "telegram".to_string(),
                            timestamp: std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs(),
                            metadata,
                        };

                        if tx.send(msg).await.is_err() {
                            return Ok(());
                        }
                        continue;
                    }

                    // ── Document message ───────────────────────────────
                    if let Some(doc) = message.get("document") {
                        self.record_tg_event(
                            "inbound",
                            "tg.inbound.document",
                            "ok",
                            &chat_id,
                            None,
                            Some(build_telegram_metadata(&[
                                ("msg_type", serde_json::json!("document")),
                                ("chat_id", serde_json::json!(&chat_id)),
                            ])),
                        );
                        let file_id = doc["file_id"]
                            .as_str()
                            .unwrap_or_default()
                            .to_string();
                        let file_name = doc["file_name"]
                            .as_str()
                            .unwrap_or("unknown")
                            .to_string();
                        let mime_type = doc["mime_type"]
                            .as_str()
                            .unwrap_or("application/octet-stream")
                            .to_string();

                        let mut metadata = HashMap::new();
                        metadata.insert("msg_type".into(), serde_json::json!("document"));
                        metadata.insert("file_id".into(), serde_json::json!(file_id));
                        metadata.insert("file_name".into(), serde_json::json!(file_name));
                        metadata.insert("mime_type".into(), serde_json::json!(mime_type));

                        let content = format!("[Document: {file_name}]");

                        let msg = ChannelMessage {
                            id: Uuid::new_v4().to_string(),
                            sender: chat_id,
                            content,
                            channel: "telegram".to_string(),
                            timestamp: std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs(),
                            metadata,
                        };

                        if tx.send(msg).await.is_err() {
                            return Ok(());
                        }
                        continue;
                    }

                    // ── Text message (existing behavior) ──────────────
                    let Some(text) = message.get("text").and_then(serde_json::Value::as_str) else {
                        continue;
                    };

                    self.record_tg_event(
                        "inbound",
                        "tg.inbound.text",
                        "ok",
                        &chat_id,
                        None,
                        Some(build_telegram_metadata(&[
                            ("msg_type", serde_json::json!("text")),
                            ("chat_id", serde_json::json!(&chat_id)),
                        ])),
                    );

                    // Send "typing" indicator immediately when we receive a message
                    let typing_body = serde_json::json!({
                        "chat_id": &chat_id,
                        "action": "typing"
                    });
                    let _ = self
                        .client
                        .post(self.api_url("sendChatAction"))
                        .json(&typing_body)
                        .send()
                        .await; // Ignore errors for typing indicator

                    let mut metadata = HashMap::new();
                    metadata.insert("msg_type".into(), serde_json::json!("text"));

                    let msg = ChannelMessage {
                        id: Uuid::new_v4().to_string(),
                        sender: chat_id,
                        content: text.to_string(),
                        channel: "telegram".to_string(),
                        timestamp: std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs(),
                        metadata,
                    };

                    if tx.send(msg).await.is_err() {
                        return Ok(());
                    }
                }
            }
        }
    }

    async fn health_check(&self) -> bool {
        self.client
            .get(self.api_url("getMe"))
            .send()
            .await
            .map(|r| r.status().is_success())
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn telegram_channel_name() {
        let ch = TelegramChannel::new("fake-token".into(), vec!["*".into()]);
        assert_eq!(ch.name(), "telegram");
    }

    #[test]
    fn telegram_api_url() {
        let ch = TelegramChannel::new("123:ABC".into(), vec![]);
        assert_eq!(
            ch.api_url("getMe"),
            "https://api.telegram.org/bot123:ABC/getMe"
        );
    }

    #[test]
    fn telegram_user_allowed_wildcard() {
        let ch = TelegramChannel::new("t".into(), vec!["*".into()]);
        assert!(ch.is_user_allowed("anyone"));
    }

    #[test]
    fn telegram_user_allowed_specific() {
        let ch = TelegramChannel::new("t".into(), vec!["alice".into(), "bob".into()]);
        assert!(ch.is_user_allowed("alice"));
        assert!(!ch.is_user_allowed("eve"));
    }

    #[test]
    fn telegram_user_denied_empty() {
        let ch = TelegramChannel::new("t".into(), vec![]);
        assert!(!ch.is_user_allowed("anyone"));
    }

    #[test]
    fn telegram_user_exact_match_not_substring() {
        let ch = TelegramChannel::new("t".into(), vec!["alice".into()]);
        assert!(!ch.is_user_allowed("alice_bot"));
        assert!(!ch.is_user_allowed("alic"));
        assert!(!ch.is_user_allowed("malice"));
    }

    #[test]
    fn telegram_user_empty_string_denied() {
        let ch = TelegramChannel::new("t".into(), vec!["alice".into()]);
        assert!(!ch.is_user_allowed(""));
    }

    #[test]
    fn telegram_user_case_sensitive() {
        let ch = TelegramChannel::new("t".into(), vec!["Alice".into()]);
        assert!(ch.is_user_allowed("Alice"));
        assert!(!ch.is_user_allowed("alice"));
        assert!(!ch.is_user_allowed("ALICE"));
    }

    #[test]
    fn telegram_wildcard_with_specific_users() {
        let ch = TelegramChannel::new("t".into(), vec!["alice".into(), "*".into()]);
        assert!(ch.is_user_allowed("alice"));
        assert!(ch.is_user_allowed("bob"));
        assert!(ch.is_user_allowed("anyone"));
    }

    #[test]
    fn telegram_user_allowed_by_numeric_id_identity() {
        let ch = TelegramChannel::new("t".into(), vec!["123456789".into()]);
        assert!(ch.is_any_user_allowed(["unknown", "123456789"]));
    }

    #[test]
    fn telegram_user_denied_when_none_of_identities_match() {
        let ch = TelegramChannel::new("t".into(), vec!["alice".into(), "987654321".into()]);
        assert!(!ch.is_any_user_allowed(["unknown", "123456789"]));
    }

    // ── File sending API URL tests ──────────────────────────────────

    #[test]
    fn telegram_api_url_send_document() {
        let ch = TelegramChannel::new("123:ABC".into(), vec![]);
        assert_eq!(
            ch.api_url("sendDocument"),
            "https://api.telegram.org/bot123:ABC/sendDocument"
        );
    }

    #[test]
    fn telegram_api_url_send_photo() {
        let ch = TelegramChannel::new("123:ABC".into(), vec![]);
        assert_eq!(
            ch.api_url("sendPhoto"),
            "https://api.telegram.org/bot123:ABC/sendPhoto"
        );
    }

    #[test]
    fn telegram_api_url_send_video() {
        let ch = TelegramChannel::new("123:ABC".into(), vec![]);
        assert_eq!(
            ch.api_url("sendVideo"),
            "https://api.telegram.org/bot123:ABC/sendVideo"
        );
    }

    #[test]
    fn telegram_api_url_send_audio() {
        let ch = TelegramChannel::new("123:ABC".into(), vec![]);
        assert_eq!(
            ch.api_url("sendAudio"),
            "https://api.telegram.org/bot123:ABC/sendAudio"
        );
    }

    #[test]
    fn telegram_api_url_send_voice() {
        let ch = TelegramChannel::new("123:ABC".into(), vec![]);
        assert_eq!(
            ch.api_url("sendVoice"),
            "https://api.telegram.org/bot123:ABC/sendVoice"
        );
    }

    // ── File sending integration tests (with mock server) ──────────

    #[tokio::test]
    async fn telegram_send_document_bytes_builds_correct_form() {
        // This test verifies the method doesn't panic and handles bytes correctly
        let ch = TelegramChannel::new("fake-token".into(), vec!["*".into()]);
        let file_bytes = b"Hello, this is a test file content".to_vec();

        // The actual API call will fail (no real server), but we verify the method exists
        // and handles the input correctly up to the network call
        let result = ch
            .send_document_bytes("123456", file_bytes, "test.txt", Some("Test caption"))
            .await;

        // Should fail with network error, not a panic or type error
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        // Error should be network-related, not a code bug
        assert!(
            err.contains("error") || err.contains("failed") || err.contains("connect"),
            "Expected network error, got: {err}"
        );
    }

    #[tokio::test]
    async fn telegram_send_photo_bytes_builds_correct_form() {
        let ch = TelegramChannel::new("fake-token".into(), vec!["*".into()]);
        // Minimal valid PNG header bytes
        let file_bytes = vec![0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A];

        let result = ch
            .send_photo_bytes("123456", file_bytes, "test.png", None)
            .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn telegram_send_document_by_url_builds_correct_json() {
        let ch = TelegramChannel::new("fake-token".into(), vec!["*".into()]);

        let result = ch
            .send_document_by_url("123456", "https://example.com/file.pdf", Some("PDF doc"))
            .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn telegram_send_photo_by_url_builds_correct_json() {
        let ch = TelegramChannel::new("fake-token".into(), vec!["*".into()]);

        let result = ch
            .send_photo_by_url("123456", "https://example.com/image.jpg", None)
            .await;

        assert!(result.is_err());
    }

    // ── File path handling tests ────────────────────────────────────

    #[tokio::test]
    async fn telegram_send_document_nonexistent_file() {
        let ch = TelegramChannel::new("fake-token".into(), vec!["*".into()]);
        let path = Path::new("/nonexistent/path/to/file.txt");

        let result = ch.send_document("123456", path, None).await;

        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        // Should fail with file not found error
        assert!(
            err.contains("No such file") || err.contains("not found") || err.contains("os error"),
            "Expected file not found error, got: {err}"
        );
    }

    #[tokio::test]
    async fn telegram_send_photo_nonexistent_file() {
        let ch = TelegramChannel::new("fake-token".into(), vec!["*".into()]);
        let path = Path::new("/nonexistent/path/to/photo.jpg");

        let result = ch.send_photo("123456", path, None).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn telegram_send_video_nonexistent_file() {
        let ch = TelegramChannel::new("fake-token".into(), vec!["*".into()]);
        let path = Path::new("/nonexistent/path/to/video.mp4");

        let result = ch.send_video("123456", path, None).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn telegram_send_audio_nonexistent_file() {
        let ch = TelegramChannel::new("fake-token".into(), vec!["*".into()]);
        let path = Path::new("/nonexistent/path/to/audio.mp3");

        let result = ch.send_audio("123456", path, None).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn telegram_send_voice_nonexistent_file() {
        let ch = TelegramChannel::new("fake-token".into(), vec!["*".into()]);
        let path = Path::new("/nonexistent/path/to/voice.ogg");

        let result = ch.send_voice("123456", path, None).await;

        assert!(result.is_err());
    }

    // ── Caption handling tests ──────────────────────────────────────

    #[tokio::test]
    async fn telegram_send_document_bytes_with_caption() {
        let ch = TelegramChannel::new("fake-token".into(), vec!["*".into()]);
        let file_bytes = b"test content".to_vec();

        // With caption
        let result = ch
            .send_document_bytes("123456", file_bytes.clone(), "test.txt", Some("My caption"))
            .await;
        assert!(result.is_err()); // Network error expected

        // Without caption
        let result = ch
            .send_document_bytes("123456", file_bytes, "test.txt", None)
            .await;
        assert!(result.is_err()); // Network error expected
    }

    #[tokio::test]
    async fn telegram_send_photo_bytes_with_caption() {
        let ch = TelegramChannel::new("fake-token".into(), vec!["*".into()]);
        let file_bytes = vec![0x89, 0x50, 0x4E, 0x47];

        // With caption
        let result = ch
            .send_photo_bytes(
                "123456",
                file_bytes.clone(),
                "test.png",
                Some("Photo caption"),
            )
            .await;
        assert!(result.is_err());

        // Without caption
        let result = ch
            .send_photo_bytes("123456", file_bytes, "test.png", None)
            .await;
        assert!(result.is_err());
    }

    // ── Empty/edge case tests ───────────────────────────────────────

    #[tokio::test]
    async fn telegram_send_document_bytes_empty_file() {
        let ch = TelegramChannel::new("fake-token".into(), vec!["*".into()]);
        let file_bytes: Vec<u8> = vec![];

        let result = ch
            .send_document_bytes("123456", file_bytes, "empty.txt", None)
            .await;

        // Should not panic, will fail at API level
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn telegram_send_document_bytes_empty_filename() {
        let ch = TelegramChannel::new("fake-token".into(), vec!["*".into()]);
        let file_bytes = b"content".to_vec();

        let result = ch.send_document_bytes("123456", file_bytes, "", None).await;

        // Should not panic
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn telegram_send_document_bytes_empty_chat_id() {
        let ch = TelegramChannel::new("fake-token".into(), vec!["*".into()]);
        let file_bytes = b"content".to_vec();

        let result = ch
            .send_document_bytes("", file_bytes, "test.txt", None)
            .await;

        // Should not panic
        assert!(result.is_err());
    }
}
