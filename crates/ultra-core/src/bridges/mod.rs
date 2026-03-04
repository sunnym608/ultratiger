#[derive(Debug, thiserror::Error)]
pub enum BridgeError {
    #[error("bridge not configured: {0}")]
    NotConfigured(&'static str),
}

pub trait BridgeAdapter {
    fn name(&self) -> &'static str;
    fn ingest_event(&self, payload: &str) -> Result<(), BridgeError>;
}

#[derive(Debug, Default)]
pub struct TelegramBridge;

impl BridgeAdapter for TelegramBridge {
    fn name(&self) -> &'static str {
        "telegram"
    }

    fn ingest_event(&self, _payload: &str) -> Result<(), BridgeError> {
        Err(BridgeError::NotConfigured("telegram token missing"))
    }
}

#[derive(Debug, Default)]
pub struct WhatsappBridge;

impl BridgeAdapter for WhatsappBridge {
    fn name(&self) -> &'static str {
        "whatsapp"
    }

    fn ingest_event(&self, _payload: &str) -> Result<(), BridgeError> {
        Err(BridgeError::NotConfigured(
            "whatsapp sidecar endpoint missing",
        ))
    }
}
