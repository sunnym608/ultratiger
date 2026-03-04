use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone)]
pub struct BridgeConfig {
    pub telegram_bot_token: String,
    pub telegram_signing_secret: String,
    pub whatsapp_api_url: String,
    pub whatsapp_access_token: String,
    pub whatsapp_signing_secret: String,
    pub rate_limit_per_minute: u32,
    pub outbound_max_retries: u32,
}

#[derive(Debug, thiserror::Error)]
pub enum BridgeError {
    #[error("bridge not configured: {0}")]
    NotConfigured(&'static str),
    #[error("signature verification failed")]
    InvalidSignature,
    #[error("rate limit exceeded for bridge {0}")]
    RateLimited(String),
    #[error("http error: {0}")]
    Http(String),
    #[error("payload error: {0}")]
    Payload(String),
}

#[derive(Debug, Clone, Serialize)]
pub struct BridgeHealth {
    pub bridge: String,
    pub inbound_events: u64,
    pub outbound_sent: u64,
    pub outbound_failed: u64,
    pub consecutive_failures: u32,
    pub status: String,
    pub last_error: Option<String>,
    pub last_event_unix: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct BridgesHealthResponse {
    pub telegram: BridgeHealth,
    pub whatsapp: BridgeHealth,
}

#[derive(Debug, Clone)]
pub struct NormalizedBridgeEvent {
    pub bridge: String,
    pub user_id: String,
    pub channel_id: String,
    pub text: String,
    pub received_at_unix: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutboundReply {
    pub bridge: String,
    pub channel_id: String,
    pub text: String,
}

#[derive(Debug, Clone, Default)]
struct BridgeMetrics {
    inbound_events: u64,
    outbound_sent: u64,
    outbound_failed: u64,
    consecutive_failures: u32,
    last_error: Option<String>,
    last_event_unix: Option<u64>,
}

#[derive(Debug, Clone, Default)]
struct WindowRateCounter {
    window_epoch_minute: u64,
    count: u32,
}

pub struct BridgeHub {
    config: BridgeConfig,
    client: Client,
    telegram_metrics: BridgeMetrics,
    whatsapp_metrics: BridgeMetrics,
    rate_counters: HashMap<String, WindowRateCounter>,
}

impl BridgeHub {
    pub fn new(config: BridgeConfig) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap_or_else(|_| Client::new());

        Self {
            config,
            client,
            telegram_metrics: BridgeMetrics::default(),
            whatsapp_metrics: BridgeMetrics::default(),
            rate_counters: HashMap::new(),
        }
    }

    pub fn ingest_telegram_webhook(
        &mut self,
        payload: &str,
        signature_header: Option<&str>,
    ) -> Result<NormalizedBridgeEvent, BridgeError> {
        self.verify_signature(
            payload,
            signature_header,
            &self.config.telegram_signing_secret,
        )?;
        self.check_rate_limit("telegram")?;

        let value: serde_json::Value = serde_json::from_str(payload)
            .map_err(|err| BridgeError::Payload(format!("invalid telegram payload: {err}")))?;

        let text = value
            .pointer("/message/text")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_owned();
        let user_id = value
            .pointer("/message/from/id")
            .map(|v| v.to_string())
            .unwrap_or_else(|| "unknown".to_owned());
        let channel_id = value
            .pointer("/message/chat/id")
            .map(|v| v.to_string())
            .unwrap_or_else(|| "unknown".to_owned());

        let now = now_unix();
        self.telegram_metrics.inbound_events += 1;
        self.telegram_metrics.last_event_unix = Some(now);

        Ok(NormalizedBridgeEvent {
            bridge: "telegram".to_owned(),
            user_id,
            channel_id,
            text,
            received_at_unix: now,
        })
    }

    pub fn ingest_whatsapp_webhook(
        &mut self,
        payload: &str,
        signature_header: Option<&str>,
    ) -> Result<NormalizedBridgeEvent, BridgeError> {
        self.verify_signature(
            payload,
            signature_header,
            &self.config.whatsapp_signing_secret,
        )?;
        self.check_rate_limit("whatsapp")?;

        let value: serde_json::Value = serde_json::from_str(payload)
            .map_err(|err| BridgeError::Payload(format!("invalid whatsapp payload: {err}")))?;

        let text = value
            .pointer("/entry/0/changes/0/value/messages/0/text/body")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_owned();
        let user_id = value
            .pointer("/entry/0/changes/0/value/messages/0/from")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_owned();
        let channel_id = value
            .pointer("/entry/0/id")
            .map(|v| v.to_string())
            .unwrap_or_else(|| "unknown".to_owned());

        let now = now_unix();
        self.whatsapp_metrics.inbound_events += 1;
        self.whatsapp_metrics.last_event_unix = Some(now);

        Ok(NormalizedBridgeEvent {
            bridge: "whatsapp".to_owned(),
            user_id,
            channel_id,
            text,
            received_at_unix: now,
        })
    }

    pub fn send_outbound_with_retry(
        &mut self,
        mut reply: OutboundReply,
    ) -> Result<(), BridgeError> {
        reply.text = reply.text.trim().to_owned();
        if reply.text.is_empty() {
            return Err(BridgeError::Payload("empty outbound text".to_owned()));
        }

        let max_retries = self.config.outbound_max_retries.max(1);
        let mut last_err: Option<BridgeError> = None;

        for attempt in 0..max_retries {
            let result = self.send_outbound_once(&reply);
            match result {
                Ok(()) => {
                    self.record_success(&reply.bridge);
                    return Ok(());
                }
                Err(err) => {
                    self.record_failure(&reply.bridge, &err.to_string());
                    last_err = Some(err);

                    let backoff_ms = 150_u64.saturating_mul(2_u64.saturating_pow(attempt));
                    std::thread::sleep(Duration::from_millis(backoff_ms));
                }
            }
        }

        Err(last_err.unwrap_or_else(|| BridgeError::Http("unknown outbound failure".to_owned())))
    }

    pub fn health(&self) -> BridgesHealthResponse {
        BridgesHealthResponse {
            telegram: to_health("telegram", &self.telegram_metrics),
            whatsapp: to_health("whatsapp", &self.whatsapp_metrics),
        }
    }

    fn send_outbound_once(&self, reply: &OutboundReply) -> Result<(), BridgeError> {
        match reply.bridge.as_str() {
            "telegram" => self.send_telegram(reply),
            "whatsapp" => self.send_whatsapp(reply),
            other => Err(BridgeError::Payload(format!(
                "unsupported bridge '{other}'"
            ))),
        }
    }

    fn send_telegram(&self, reply: &OutboundReply) -> Result<(), BridgeError> {
        if self.config.telegram_bot_token.is_empty() {
            return Err(BridgeError::NotConfigured("telegram token missing"));
        }

        let url = format!(
            "https://api.telegram.org/bot{}/sendMessage",
            self.config.telegram_bot_token
        );

        let body = serde_json::json!({
            "chat_id": reply.channel_id,
            "text": reply.text,
        });

        self.client
            .post(url)
            .json(&body)
            .send()
            .map_err(|err| BridgeError::Http(err.to_string()))?
            .error_for_status()
            .map(|_| ())
            .map_err(|err| BridgeError::Http(err.to_string()))
    }

    fn send_whatsapp(&self, reply: &OutboundReply) -> Result<(), BridgeError> {
        if self.config.whatsapp_access_token.is_empty() {
            return Err(BridgeError::NotConfigured("whatsapp access token missing"));
        }

        let url = format!(
            "{}/messages",
            self.config.whatsapp_api_url.trim_end_matches('/')
        );
        let body = serde_json::json!({
            "to": reply.channel_id,
            "type": "text",
            "text": { "body": reply.text },
        });

        self.client
            .post(url)
            .bearer_auth(&self.config.whatsapp_access_token)
            .json(&body)
            .send()
            .map_err(|err| BridgeError::Http(err.to_string()))?
            .error_for_status()
            .map(|_| ())
            .map_err(|err| BridgeError::Http(err.to_string()))
    }

    fn verify_signature(
        &self,
        payload: &str,
        signature_header: Option<&str>,
        secret: &str,
    ) -> Result<(), BridgeError> {
        if secret.is_empty() {
            return Ok(());
        }

        let Some(signature) = signature_header else {
            return Err(BridgeError::InvalidSignature);
        };

        let expected = signature_for(secret, payload);
        let got = signature.trim().to_lowercase();
        if expected != got {
            return Err(BridgeError::InvalidSignature);
        }
        Ok(())
    }

    fn check_rate_limit(&mut self, bridge: &str) -> Result<(), BridgeError> {
        let now_minute = now_unix() / 60;
        let counter = self
            .rate_counters
            .entry(bridge.to_owned())
            .or_insert_with(WindowRateCounter::default);

        if counter.window_epoch_minute != now_minute {
            counter.window_epoch_minute = now_minute;
            counter.count = 0;
        }

        counter.count = counter.count.saturating_add(1);
        if counter.count > self.config.rate_limit_per_minute {
            return Err(BridgeError::RateLimited(bridge.to_owned()));
        }
        Ok(())
    }

    fn record_success(&mut self, bridge: &str) {
        let metrics = select_metrics_mut(
            bridge,
            &mut self.telegram_metrics,
            &mut self.whatsapp_metrics,
        );
        metrics.outbound_sent = metrics.outbound_sent.saturating_add(1);
        metrics.consecutive_failures = 0;
        metrics.last_error = None;
        metrics.last_event_unix = Some(now_unix());
    }

    fn record_failure(&mut self, bridge: &str, error: &str) {
        let metrics = select_metrics_mut(
            bridge,
            &mut self.telegram_metrics,
            &mut self.whatsapp_metrics,
        );
        metrics.outbound_failed = metrics.outbound_failed.saturating_add(1);
        metrics.consecutive_failures = metrics.consecutive_failures.saturating_add(1);
        metrics.last_error = Some(error.to_owned());
        metrics.last_event_unix = Some(now_unix());
    }
}

fn select_metrics_mut<'a>(
    bridge: &str,
    telegram: &'a mut BridgeMetrics,
    whatsapp: &'a mut BridgeMetrics,
) -> &'a mut BridgeMetrics {
    if bridge == "telegram" {
        telegram
    } else {
        whatsapp
    }
}

fn to_health(bridge: &str, metrics: &BridgeMetrics) -> BridgeHealth {
    let status = if metrics.consecutive_failures >= 3 {
        "degraded"
    } else {
        "healthy"
    }
    .to_owned();

    BridgeHealth {
        bridge: bridge.to_owned(),
        inbound_events: metrics.inbound_events,
        outbound_sent: metrics.outbound_sent,
        outbound_failed: metrics.outbound_failed,
        consecutive_failures: metrics.consecutive_failures,
        status,
        last_error: metrics.last_error.clone(),
        last_event_unix: metrics.last_event_unix,
    }
}

pub fn signature_for(secret: &str, payload: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(secret.as_bytes());
    hasher.update(b":");
    hasher.update(payload.as_bytes());
    hex::encode(hasher.finalize())
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signature_matches() {
        let payload = "{\"x\":1}";
        let sig = signature_for("abc", payload);
        assert_eq!(sig, signature_for("abc", payload));
    }

    #[test]
    fn rate_limit_blocks_after_threshold() {
        let mut hub = BridgeHub::new(BridgeConfig {
            telegram_bot_token: String::new(),
            telegram_signing_secret: String::new(),
            whatsapp_api_url: "https://example.com".to_owned(),
            whatsapp_access_token: String::new(),
            whatsapp_signing_secret: String::new(),
            rate_limit_per_minute: 1,
            outbound_max_retries: 1,
        });

        let p = r#"{"message":{"text":"hi"}}"#;
        let _ = hub.ingest_telegram_webhook(p, None);
        let second = hub.ingest_telegram_webhook(p, None);
        assert!(matches!(second, Err(BridgeError::RateLimited(_))));
    }
}
