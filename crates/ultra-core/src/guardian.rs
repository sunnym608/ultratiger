use crate::config::AppConfig;
use crate::models::{PreflightRequest, PreflightResponse};

#[derive(Debug, Clone)]
pub struct Guardian {
    config: AppConfig,
    total_spent_today_usd: f64,
    keys_revoked: bool,
}

impl Guardian {
    pub fn new(config: AppConfig) -> Self {
        Self {
            config,
            total_spent_today_usd: 0.0,
            keys_revoked: false,
        }
    }

    pub fn preflight_check(&self, req: &PreflightRequest) -> PreflightResponse {
        if self.keys_revoked {
            return PreflightResponse {
                allowed: false,
                requires_authorization: false,
                reason: "API keys are revoked due to daily budget limit".to_owned(),
                projected_daily_total_usd: self.total_spent_today_usd,
            };
        }

        let projected = self.total_spent_today_usd + req.estimated_cost_usd;
        if projected > self.config.daily_budget_limit_usd {
            return PreflightResponse {
                allowed: false,
                requires_authorization: false,
                reason: "Projected spend exceeds daily budget limit".to_owned(),
                projected_daily_total_usd: projected,
            };
        }

        let requires_authorization =
            req.estimated_cost_usd >= self.config.preflight_authorize_threshold_usd;

        PreflightResponse {
            allowed: true,
            requires_authorization,
            reason: if requires_authorization {
                format!("Task '{}' exceeds authorization threshold", req.task_name)
            } else {
                "Within allowed budget".to_owned()
            },
            projected_daily_total_usd: projected,
        }
    }

    pub fn register_spend(&mut self, amount_usd: f64) {
        self.total_spent_today_usd += amount_usd.max(0.0);
        if self.total_spent_today_usd > self.config.daily_budget_limit_usd {
            self.keys_revoked = true;
        }
    }

    pub fn total_spent_today_usd(&self) -> f64 {
        self.total_spent_today_usd
    }

    pub fn keys_revoked(&self) -> bool {
        self.keys_revoked
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requests_over_threshold_require_authorization() {
        let guardian = Guardian::new(AppConfig::default());
        let response = guardian.preflight_check(&PreflightRequest {
            task_name: "summary".to_owned(),
            estimated_tokens: 30_000,
            estimated_cost_usd: 0.10,
        });
        assert!(response.allowed);
        assert!(response.requires_authorization);
    }

    #[test]
    fn daily_limit_revokes_keys() {
        let mut guardian = Guardian::new(AppConfig {
            daily_budget_limit_usd: 1.0,
            preflight_authorize_threshold_usd: 0.10,
        });
        guardian.register_spend(1.2);
        assert!(guardian.keys_revoked());
    }
}
