#[derive(Debug, Clone)]
pub struct AppConfig {
    pub daily_budget_limit_usd: f64,
    pub preflight_authorize_threshold_usd: f64,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            daily_budget_limit_usd: 10.0,
            preflight_authorize_threshold_usd: 0.10,
        }
    }
}
