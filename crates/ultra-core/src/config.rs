#[derive(Debug, Clone)]
pub struct AppConfig {
    pub daily_budget_limit_usd: f64,
    pub preflight_authorize_threshold_usd: f64,
    pub worker_concurrency: usize,
    pub retry_base_delay_seconds: u64,
    pub retry_jitter_seconds: u64,
    pub telegram_bot_token: String,
    pub telegram_signing_secret: String,
    pub whatsapp_api_url: String,
    pub whatsapp_access_token: String,
    pub whatsapp_signing_secret: String,
    pub bridge_rate_limit_per_minute: u32,
    pub bridge_outbound_max_retries: u32,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            daily_budget_limit_usd: 10.0,
            preflight_authorize_threshold_usd: 0.10,
            worker_concurrency: 2,
            retry_base_delay_seconds: 5,
            retry_jitter_seconds: 3,
            telegram_bot_token: std::env::var("ULTRA_TELEGRAM_BOT_TOKEN").unwrap_or_default(),
            telegram_signing_secret: std::env::var("ULTRA_TELEGRAM_SIGNING_SECRET")
                .unwrap_or_default(),
            whatsapp_api_url: std::env::var("ULTRA_WHATSAPP_API_URL")
                .unwrap_or_else(|_| "https://graph.facebook.com/v20.0".to_owned()),
            whatsapp_access_token: std::env::var("ULTRA_WHATSAPP_ACCESS_TOKEN").unwrap_or_default(),
            whatsapp_signing_secret: std::env::var("ULTRA_WHATSAPP_SIGNING_SECRET")
                .unwrap_or_default(),
            bridge_rate_limit_per_minute: std::env::var("ULTRA_BRIDGE_RATE_LIMIT_PER_MINUTE")
                .ok()
                .and_then(|v| v.parse::<u32>().ok())
                .unwrap_or(120),
            bridge_outbound_max_retries: std::env::var("ULTRA_BRIDGE_OUTBOUND_MAX_RETRIES")
                .ok()
                .and_then(|v| v.parse::<u32>().ok())
                .unwrap_or(3),
        }
    }
}
