#[derive(Debug, Clone)]
pub struct AppConfig {
    pub daily_budget_limit_usd: f64,
    pub preflight_authorize_threshold_usd: f64,
    pub worker_concurrency: usize,
    pub retry_base_delay_seconds: u64,
    pub retry_jitter_seconds: u64,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            daily_budget_limit_usd: 10.0,
            preflight_authorize_threshold_usd: 0.10,
            worker_concurrency: 2,
            retry_base_delay_seconds: 5,
            retry_jitter_seconds: 3,
        }
    }
}
