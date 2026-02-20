use anyhow::Result;
use reqwest::multipart::{Form, Part};
use serde::{Deserialize, Serialize};

/// A single inline keyboard button
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InlineButton {
    pub text: String,
    pub callback_data: String,
}

/// Max voice file size to download for STT (5 MB)
pub const MAX_VOICE_BYTES: u64 = 5 * 1024 * 1024;

/// Accepted audio MIME types for STT
pub const ACCEPTED_AUDIO_TYPES: &[&str] = &["audio/ogg", "audio/mpeg", "audio/mp4", "audio/wav"];

/// Concurrency limit for STT transcriptions
pub const STT_CONCURRENCY: usize = 3;

/// STT transcription timeout
pub const STT_TIMEOUT_SECS: u64 = 15;

/// Result from the external STT service
#[derive(Debug, Clone, Deserialize)]
pub struct TranscriptResult {
    pub text: String,
    pub language: String,
    #[allow(dead_code)]
    pub duration_ms: Option<u64>,
    pub confidence: f64,
    #[allow(dead_code)]
    pub processing_time_ms: Option<u64>,
}

/// Client for the external speech-to-text service
#[derive(Clone)]
pub struct SpeechClient {
    client: reqwest::Client,
    stt_endpoint: String,
}

impl SpeechClient {
    pub fn new(stt_endpoint: String) -> Self {
        Self {
            client: reqwest::Client::new(),
            stt_endpoint,
        }
    }

    /// Transcribe audio bytes via the external STT endpoint.
    ///
    /// Sends a multipart POST to `{stt_endpoint}/transcribe` with the audio file.
    pub async fn transcribe(&self, audio: Vec<u8>, format: &str) -> Result<TranscriptResult> {
        let mime = match format {
            "ogg" | "oga" => "audio/ogg",
            "mp3" => "audio/mpeg",
            "mp4" | "m4a" => "audio/mp4",
            "wav" => "audio/wav",
            _ => "application/octet-stream",
        };

        let part = Part::bytes(audio)
            .file_name(format!("audio.{format}"))
            .mime_str(mime)?;

        let form = Form::new().part("file", part);

        let url = format!("{}/transcribe", self.stt_endpoint.trim_end_matches('/'));
        let resp = self.client.post(&url).multipart(form).send().await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("STT transcribe failed ({status}): {body}");
        }

        let result: TranscriptResult = resp.json().await?;
        Ok(result)
    }

    pub fn endpoint(&self) -> &str {
        &self.stt_endpoint
    }
}

/// Per-message context for Telegram tools to resolve chat_id automatically
#[derive(Debug, Clone)]
pub struct TelegramToolContext {
    pub chat_id: String,
    pub channel: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inline_button_serde() {
        let btn = InlineButton {
            text: "Yes".into(),
            callback_data: "confirm_yes".into(),
        };
        let json = serde_json::to_string(&btn).unwrap();
        let parsed: InlineButton = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.text, "Yes");
        assert_eq!(parsed.callback_data, "confirm_yes");
    }

    #[test]
    fn transcript_result_deserialize() {
        let json = r#"{
            "text": "hello world",
            "language": "en",
            "duration_ms": 12340,
            "confidence": 0.95,
            "processing_time_ms": 850
        }"#;
        let result: TranscriptResult = serde_json::from_str(json).unwrap();
        assert_eq!(result.text, "hello world");
        assert_eq!(result.language, "en");
        assert!((result.confidence - 0.95).abs() < f64::EPSILON);
    }

    #[test]
    fn transcript_result_minimal() {
        // Without optional fields
        let json = r#"{
            "text": "test",
            "language": "en",
            "confidence": 0.8
        }"#;
        let result: TranscriptResult = serde_json::from_str(json).unwrap();
        assert_eq!(result.text, "test");
        assert!(result.duration_ms.is_none());
        assert!(result.processing_time_ms.is_none());
    }

    #[test]
    fn speech_client_new() {
        let client = SpeechClient::new("http://localhost:9000".into());
        assert_eq!(client.endpoint(), "http://localhost:9000");
    }

    #[test]
    fn max_voice_bytes_is_5mb() {
        assert_eq!(MAX_VOICE_BYTES, 5 * 1024 * 1024);
    }

    #[test]
    fn accepted_audio_types_not_empty() {
        assert!(!ACCEPTED_AUDIO_TYPES.is_empty());
        assert!(ACCEPTED_AUDIO_TYPES.contains(&"audio/ogg"));
    }

    #[test]
    fn tool_context_clone() {
        let ctx = TelegramToolContext {
            chat_id: "12345".into(),
            channel: "telegram".into(),
        };
        let cloned = ctx.clone();
        assert_eq!(cloned.chat_id, "12345");
        assert_eq!(cloned.channel, "telegram");
    }
}
