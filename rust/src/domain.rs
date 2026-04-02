use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
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
    identifier
        .chars()
        .map(|c| if c.is_alphanumeric() || c == '-' || c == '_' { c } else { '-' })
        .collect::<String>()
        .trim_matches('-')
        .to_lowercase()
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
