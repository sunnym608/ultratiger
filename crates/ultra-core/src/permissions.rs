const SENSITIVE_ACTION_KEYWORDS: &[&str] = &[
    "delete",
    "revoke",
    "write",
    "send_money",
    "network:public",
    "filesystem:/",
];

pub fn requires_human_approval(action: &str) -> bool {
    let normalized = action.to_lowercase();
    SENSITIVE_ACTION_KEYWORDS
        .iter()
        .any(|keyword| normalized.contains(keyword))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delete_requires_approval() {
        assert!(requires_human_approval("Delete file /tmp/data.txt"));
    }

    #[test]
    fn read_only_action_is_safe() {
        assert!(!requires_human_approval("read calendar events"));
    }
}
