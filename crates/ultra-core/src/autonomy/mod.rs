#[derive(Debug, Clone)]
pub struct TaskItem {
    pub id: String,
    pub task_type: String,
    pub payload: String,
    pub attempts: u32,
    pub max_attempts: u32,
    pub available_at_unix: u64,
}

#[derive(Debug, Clone)]
pub enum SchedulerTrigger {
    EverySeconds(u64),
    Cron(String),
}

pub fn compute_retry_delay_seconds(
    attempts: u32,
    base_delay_seconds: u64,
    jitter_seconds: u64,
    now_unix: u64,
) -> u64 {
    let exponent = attempts.saturating_sub(1).min(16);
    let exp_delay = base_delay_seconds.saturating_mul(2_u64.saturating_pow(exponent));
    let jitter = if jitter_seconds == 0 {
        0
    } else {
        now_unix % (jitter_seconds + 1)
    };
    exp_delay.saturating_add(jitter)
}

pub fn next_schedule_run_unix(trigger: &SchedulerTrigger, now_unix: u64) -> Option<u64> {
    match trigger {
        SchedulerTrigger::EverySeconds(seconds) => {
            if *seconds == 0 {
                None
            } else {
                Some(now_unix.saturating_add(*seconds))
            }
        }
        SchedulerTrigger::Cron(expr) => parse_simple_cron_seconds(expr, now_unix),
    }
}

fn parse_simple_cron_seconds(expr: &str, now_unix: u64) -> Option<u64> {
    // Supported minimal format:
    // - "*/N * * * * *" (every N seconds)
    // - "N * * * * *" (at second N every minute)
    let fields = expr.split_whitespace().collect::<Vec<_>>();
    if fields.len() != 6 {
        return None;
    }

    let seconds_field = fields[0];
    if let Some(step) = seconds_field.strip_prefix("*/") {
        let step = step.parse::<u64>().ok()?;
        if step == 0 {
            return None;
        }
        let rem = now_unix % step;
        return Some(if rem == 0 {
            now_unix.saturating_add(step)
        } else {
            now_unix.saturating_add(step - rem)
        });
    }

    let sec = seconds_field.parse::<u64>().ok()?;
    if sec > 59 {
        return None;
    }

    let current_sec = now_unix % 60;
    let delta = if sec > current_sec {
        sec - current_sec
    } else {
        60 - (current_sec - sec)
    };
    Some(now_unix.saturating_add(delta.max(1)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_delay_grows_exponentially() {
        let a1 = compute_retry_delay_seconds(1, 5, 0, 100);
        let a2 = compute_retry_delay_seconds(2, 5, 0, 100);
        let a3 = compute_retry_delay_seconds(3, 5, 0, 100);
        assert!(a2 > a1);
        assert!(a3 > a2);
    }

    #[test]
    fn parse_every_seconds_cron() {
        let next = next_schedule_run_unix(&SchedulerTrigger::Cron("*/15 * * * * *".to_owned()), 61)
            .expect("cron should parse");
        assert_eq!(next, 75);
    }
}
