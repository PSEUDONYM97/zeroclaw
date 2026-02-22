use async_trait::async_trait;
use std::time::Duration;

use super::traits::{Channel, ChannelMessage};

/// Channel implementation that communicates with the `ZeroClaw` Control Plane.
///
/// Each agent instance runs a [`CpRelayChannel`] that long-polls the CP for
/// incoming messages, processes them through the normal agent loop, and
/// sends replies back through the CP.
pub struct CpRelayChannel {
    instance_name: String,
    cp_url: String,
    client: reqwest::Client,
}

impl CpRelayChannel {
    pub fn new(instance_name: String, cp_url: String) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(35)) // slightly longer than max long-poll wait
            .build()
            .expect("Failed to build HTTP client for CpRelayChannel");

        Self {
            instance_name,
            cp_url,
            client,
        }
    }

    /// Create from environment variables set by the CP lifecycle manager.
    pub fn from_env() -> Option<Self> {
        let instance_name = std::env::var("ZEROCLAW_INSTANCE_NAME").ok()?;
        let cp_url = std::env::var("ZEROCLAW_CP_URL")
            .unwrap_or_else(|_| "http://127.0.0.1:18800".to_string());

        Some(Self::new(instance_name, cp_url))
    }
}

#[async_trait]
impl Channel for CpRelayChannel {
    fn name(&self) -> &str {
        "cp"
    }

    async fn send(&self, message: &str, reply_to: &ChannelMessage) -> anyhow::Result<()> {
        let original_msg_id = &reply_to.id;

        // 1. Send reply message with deterministic idempotency key (D9)
        let reply_body = serde_json::json!({
            "from_instance": self.instance_name,
            "to_instance": reply_to.sender,
            "type": "response",
            "payload": { "text": message },
            "correlation_id": original_msg_id,
            "idempotency_key": format!("reply:{}", original_msg_id),
        });

        let reply_resp = self
            .client
            .post(format!("{}/api/messages", self.cp_url))
            .json(&reply_body)
            .send()
            .await;

        match reply_resp {
            Ok(resp) if resp.status().is_success() => {
                tracing::debug!("CP relay: sent reply for message {}", original_msg_id);
            }
            Ok(resp) => {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                tracing::warn!("CP relay: reply send returned {}: {}", status, body);
            }
            Err(e) => {
                tracing::error!("CP relay: failed to send reply: {e}");
            }
        }

        // 2. Acknowledge original message (D1: ack AFTER processing + reply)
        let ack_resp = self
            .client
            .post(format!(
                "{}/api/messages/{}/acknowledge",
                self.cp_url, original_msg_id
            ))
            .send()
            .await;

        match ack_resp {
            Ok(resp) if resp.status().is_success() => {
                tracing::debug!("CP relay: acknowledged message {}", original_msg_id);
            }
            Ok(resp) => {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                tracing::warn!(
                    "CP relay: ack returned {}: {} (message may be retried)",
                    status,
                    body
                );
            }
            Err(e) => {
                // If ack fails but reply succeeded, the message will be retried
                // but the reply will be deduplicated via idempotency_key (D9)
                tracing::error!(
                    "CP relay: failed to ack message {} (will be retried): {e}",
                    original_msg_id
                );
            }
        }

        Ok(())
    }

    async fn listen(&self, tx: tokio::sync::mpsc::Sender<ChannelMessage>) -> anyhow::Result<()> {
        tracing::info!(
            "CP relay channel starting for instance '{}' (CP: {})",
            self.instance_name,
            self.cp_url
        );

        loop {
            let url = format!(
                "{}/api/instances/{}/messages/pending?wait=30",
                self.cp_url, self.instance_name
            );

            let resp = match self.client.get(&url).send().await {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!("CP relay: poll error: {e}");
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    continue;
                }
            };

            if !resp.status().is_success() {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                tracing::warn!("CP relay: poll returned {}: {}", status, body);
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }

            let body: serde_json::Value = match resp.json().await {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!("CP relay: failed to parse poll response: {e}");
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    continue;
                }
            };

            // Check if a message was returned (vs null timeout)
            if body["message"].is_null() {
                // No message available, loop back to poll
                continue;
            }

            let msg = &body["message"];
            let cp_msg_id = msg["id"].as_str().unwrap_or("unknown").to_string();
            let from_instance = msg["from_instance"]
                .as_str()
                .unwrap_or("unknown")
                .to_string();

            // Extract payload text
            let payload_text = if let Some(text) = msg["payload"]["text"].as_str() {
                text.to_string()
            } else {
                msg["payload"].to_string()
            };

            let channel_msg = ChannelMessage {
                id: cp_msg_id.clone(),
                channel: "cp".to_string(),
                sender: from_instance,
                content: payload_text,
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                metadata: std::collections::HashMap::new(),
            };

            tracing::info!(
                "CP relay: received message {} from {}",
                cp_msg_id,
                channel_msg.sender
            );

            // Push into the channel bus -- does NOT ack here (D1)
            if tx.send(channel_msg).await.is_err() {
                tracing::info!("CP relay: channel receiver dropped, shutting down");
                return Ok(());
            }
        }
    }

    async fn health_check(&self) -> bool {
        match self
            .client
            .get(format!("{}/api/health", self.cp_url))
            .timeout(Duration::from_secs(5))
            .send()
            .await
        {
            Ok(resp) => resp.status().is_success(),
            Err(_) => false,
        }
    }
}
