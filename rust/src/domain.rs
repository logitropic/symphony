use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Issue {
    pub id: Option<String>,
    pub identifier: Option<String>,
    pub title: Option<String>,
    pub description: Option<String>,
    pub priority: Option<i32>,
    pub state: Option<String>,
    pub branch_name: Option<String>,
    pub url: Option<String>,
    pub assignee_id: Option<String>,
    #[serde(default)]
    pub labels: Vec<String>,
    #[serde(default)]
    pub blocked_by: Vec<Issue>,
    #[serde(default)]
    pub assigned_to_worker: bool,
    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockerRef {
    pub id: Option<String>,
    pub identifier: Option<String>,
    pub state: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Workspace {
    pub path: PathBuf,
    pub workspace_key: String,
    pub created_now: bool,
}

pub fn normalize_issue_state(state: &str) -> String {
    state.trim().to_lowercase()
}

pub fn sanitize_identifier(identifier: &str) -> String {
    let sanitized: String = identifier
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '-'
            }
        })
        .collect::<String>()
        .trim_matches('-')
        .to_lowercase();

    // Append a short hash suffix to prevent collisions from different
    // special characters that map to the same dash-separated key.
    // Use first 8 chars of a simple hash to keep it readable.
    let hash = {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        identifier.hash(&mut hasher);
        format!("{:08x}", hasher.finish())
    };

    format!("{}-{}", sanitized, &hash[..6.min(hash.len())])
}

/// Token usage totals across all sessions.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TokenTotals {
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub total_tokens: i64,
    #[serde(default)]
    pub seconds_running: i64,
}

/// Entry for a running agent session.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunningEntry {
    pub pid: u32,
    pub identifier: String,
    pub issue: Issue,
    #[serde(default)]
    pub worker_host: Option<String>,
    #[serde(default)]
    pub workspace_path: Option<String>,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub last_codex_message: Option<String>,
    #[serde(default)]
    pub last_codex_timestamp: Option<DateTime<Utc>>,
    #[serde(default)]
    pub last_codex_event: Option<String>,
    #[serde(default)]
    pub codex_app_server_pid: Option<String>,
    pub codex_input_tokens: i64,
    pub codex_output_tokens: i64,
    pub codex_total_tokens: i64,
    #[serde(default)]
    pub codex_last_reported_input_tokens: i64,
    #[serde(default)]
    pub codex_last_reported_output_tokens: i64,
    #[serde(default)]
    pub codex_last_reported_total_tokens: i64,
    pub turn_count: i32,
    pub retry_attempt: i32,
    pub started_at: DateTime<Utc>,
}

/// Entry for a pending retry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryEntry {
    pub attempt: i32,
    pub due_at_ms: i64,
    pub identifier: String,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub worker_host: Option<String>,
    #[serde(default)]
    pub workspace_path: Option<String>,
}

/// Runtime state for the orchestrator polling loop.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct OrchestratorRuntimeState {
    #[serde(default)]
    pub running: HashMap<String, RunningEntry>,
    #[serde(default)]
    pub claimed: HashSet<String>,
    #[serde(default)]
    pub retry_attempts: HashMap<String, RetryEntry>,
    #[serde(default)]
    pub codex_totals: TokenTotals,
    #[serde(default)]
    pub codex_rate_limits: Option<serde_json::Value>,
    #[serde(default)]
    pub poll_interval_ms: i64,
    #[serde(default)]
    pub max_concurrent_agents: i32,
}

/// Return a sort priority rank for an issue priority value.
/// Linear priorities: 0 (No priority), 1 (Urgent), 2 (High), 3 (Medium), 4 (Low)
pub fn priority_rank(priority: Option<i32>) -> i32 {
    match priority {
        Some(p) if (1..=4).contains(&p) => p,
        _ => 5,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanitize_identifier_special_chars() {
        // Special chars become dashes, then trailing dashes are trimmed
        // Result includes hash suffix for collision prevention
        let result = sanitize_identifier("ABC-123!@#");
        assert!(result.starts_with("abc-123-"), "got: {}", result);
    }

    #[test]
    fn test_sanitize_identifier_trim_dashes() {
        let result = sanitize_identifier("...hello...");
        assert!(result.starts_with("hello-"), "got: {}", result);
    }

    #[test]
    fn test_sanitize_identifier_lowercase() {
        let result = sanitize_identifier("SYM-42");
        assert!(result.starts_with("sym-42-"), "got: {}", result);
    }

    #[test]
    fn test_sanitize_identifier_spaces_become_dashes() {
        let result = sanitize_identifier("hello world");
        assert!(result.starts_with("hello-world-"), "got: {}", result);
    }

    #[test]
    fn test_sanitize_identifier_preserves_valid() {
        let result = sanitize_identifier("my-valid_issue-1");
        assert!(result.starts_with("my-valid_issue-1-"), "got: {}", result);
    }

    #[test]
    fn test_sanitize_identifier_all_special() {
        // All special chars become dashes, then trim, then hash suffix
        let result = sanitize_identifier("!@#$%");
        // Should not be empty since we add a hash suffix
        assert!(!result.is_empty(), "got: {}", result);
        assert!(result.starts_with("-"), "got: {}", result);
    }

    #[test]
    fn test_sanitize_identifier_different_inputs_produce_different_outputs() {
        // Two different inputs should produce different outputs (no collision)
        let id1 = sanitize_identifier("ABC-123!@#");
        let id2 = sanitize_identifier("ABC-123#$%");
        assert_ne!(id1, id2, "different inputs should not collide");
    }

    #[test]
    fn test_normalize_issue_state_trim() {
        assert_eq!(normalize_issue_state("  Done  "), "done");
    }

    #[test]
    fn test_normalize_issue_state_lowercase() {
        assert_eq!(normalize_issue_state("In Progress"), "in progress");
    }

    #[test]
    fn test_normalize_issue_state_both() {
        assert_eq!(normalize_issue_state("  CANCELLED  "), "cancelled");
    }

    #[test]
    fn test_priority_rank_urgent() {
        assert_eq!(priority_rank(Some(1)), 1);
    }

    #[test]
    fn test_priority_rank_high() {
        assert_eq!(priority_rank(Some(2)), 2);
    }

    #[test]
    fn test_priority_rank_medium() {
        assert_eq!(priority_rank(Some(3)), 3);
    }

    #[test]
    fn test_priority_rank_low() {
        assert_eq!(priority_rank(Some(4)), 4);
    }

    #[test]
    fn test_priority_rank_no_priority() {
        assert_eq!(priority_rank(Some(0)), 5);
    }

    #[test]
    fn test_priority_rank_none() {
        assert_eq!(priority_rank(None), 5);
    }

    #[test]
    fn test_priority_rank_out_of_range() {
        assert_eq!(priority_rank(Some(99)), 5);
        assert_eq!(priority_rank(Some(-1)), 5);
    }

    #[test]
    fn test_blocker_rule_todo_with_terminal_blocker() {
        // If issue is in Todo state but all blockers are terminal, it should be dispatchable
        let terminal_states: HashSet<String> = vec!["done".to_string(), "closed".to_string()]
            .into_iter()
            .collect();

        let blocker = Issue {
            id: Some("BLOCK-1".to_string()),
            state: Some("Done".to_string()),
            ..Default::default()
        };

        let blocker_state = blocker.state.as_ref().unwrap();
        let normalized_blocker_state = normalize_issue_state(blocker_state);
        let is_terminal = terminal_states.contains(&normalized_blocker_state);
        assert!(is_terminal, "Done should be terminal");
    }

    #[test]
    fn test_blocker_rule_todo_with_active_blocker() {
        // If issue is in Todo state and has an active blocker, it should NOT be dispatchable
        let terminal_states: HashSet<String> = vec!["done".to_string(), "closed".to_string()]
            .into_iter()
            .collect();

        let blocker = Issue {
            id: Some("BLOCK-1".to_string()),
            state: Some("In Progress".to_string()),
            ..Default::default()
        };

        let blocker_state = blocker.state.as_ref().unwrap();
        let normalized_blocker_state = normalize_issue_state(blocker_state);
        let is_terminal = terminal_states.contains(&normalized_blocker_state);
        assert!(!is_terminal, "In Progress should not be terminal");
    }

    #[test]
    fn test_blocker_rule_todo_with_no_blockers() {
        // Empty blocked_by list means no blocking
        let blocked_by: Vec<Issue> = vec![];
        assert!(blocked_by.is_empty());
    }

    #[test]
    fn test_blocker_rule_todo_with_missing_blocker_state() {
        // If blocker has no state, treat it as non-terminal (blocking)
        let terminal_states: HashSet<String> = vec!["done".to_string(), "closed".to_string()]
            .into_iter()
            .collect();

        let blocker = Issue {
            id: Some("BLOCK-1".to_string()),
            state: None,
            ..Default::default()
        };

        // When state is None, we can't determine if it's terminal, so it blocks
        let blocker_state = blocker.state.as_ref();
        let is_blocking = blocker_state
            .map(|s| !terminal_states.contains(&normalize_issue_state(s)))
            .unwrap_or(true);
        assert!(is_blocking, "Issue with no state should block");
    }
}
