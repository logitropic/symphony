use crate::config::ConfigSchema;
use crate::domain::{normalize_issue_state, priority_rank, OrchestratorRuntimeState, RunningEntry};
use crate::execution::WorkspaceManager;
use crate::integration::LinearClient;
use crate::protocol::CodexClient;
use chrono::Utc;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};
use tracing::{error, info, warn};

#[derive(Error, Debug)]
pub enum OrchestratorError {
    #[error("Config validation failed: {0}")]
    ConfigValidation(String),
    #[error("Tracker error: {0}")]
    TrackerError(String),
    #[error("Workspace error: {0}")]
    WorkspaceError(String),
    #[error("Codex error: {0}")]
    CodexError(String),
}

impl From<crate::execution::WorkspaceError> for OrchestratorError {
    fn from(e: crate::execution::WorkspaceError) -> Self {
        OrchestratorError::WorkspaceError(e.to_string())
    }
}

impl From<crate::protocol::CodexError> for OrchestratorError {
    fn from(e: crate::protocol::CodexError) -> Self {
        OrchestratorError::CodexError(e.to_string())
    }
}

impl From<crate::integration::LinearError> for OrchestratorError {
    fn from(e: crate::integration::LinearError) -> Self {
        OrchestratorError::TrackerError(e.to_string())
    }
}

impl From<crate::config::ConfigError> for OrchestratorError {
    fn from(e: crate::config::ConfigError) -> Self {
        OrchestratorError::ConfigValidation(e.to_string())
    }
}

#[derive(Clone)]
pub struct Orchestrator {
    state: Arc<Mutex<OrchestratorRuntimeState>>,
    config: ConfigSchema,
    tracker: LinearClient,
    workspace_manager: WorkspaceManager,
    codex_client: CodexClient,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
}

impl Orchestrator {
    pub fn new(
        config: ConfigSchema,
        tracker: LinearClient,
        workspace_manager: WorkspaceManager,
        codex_client: CodexClient,
        shutdown_rx: tokio::sync::watch::Receiver<bool>,
    ) -> Self {
        Self {
            state: Arc::new(Mutex::new(OrchestratorRuntimeState {
                poll_interval_ms: config.polling.interval_ms(),
                max_concurrent_agents: config.agent.max_concurrent_agents(),
                ..Default::default()
            })),
            config,
            tracker,
            workspace_manager,
            codex_client,
            shutdown_rx,
        }
    }

    pub async fn run(&mut self) -> Result<(), OrchestratorError> {
        info!("Orchestrator starting");

        let mut next_poll_time = tokio::time::Instant::now();

        loop {
            let poll_interval = {
                let state = self.state.lock().await;
                state.poll_interval_ms
            };

            // Wait until the next poll time, but only if we're not already past it
            let now = tokio::time::Instant::now();
            if now < next_poll_time {
                tokio::select! {
                    _ = sleep(next_poll_time - now) => {}
                    _ = self.shutdown_rx.changed() => {
                        if *self.shutdown_rx.borrow() {
                            info!("Orchestrator shutting down");
                            break;
                        }
                    }
                }
            }

            // Run poll
            if let Err(e) = self.poll_tick().await {
                error!(error = %e, "Poll tick failed");
            }

            // Schedule next poll interval from now
            next_poll_time =
                tokio::time::Instant::now() + Duration::from_millis(poll_interval as u64);
        }

        Ok(())
    }

    async fn poll_tick(&self) -> Result<(), OrchestratorError> {
        // Step 1: Reconcile running issues
        self.reconcile_running_issues().await?;

        // Step 2: Clean up expired retry entries
        let now_ms = Utc::now().timestamp_millis();
        let max_retry_backoff_ms = self.config.agent.max_retry_backoff_ms();
        {
            let mut state = self.state.lock().await;
            // Remove retries that have passed their due time by a significant margin
            // (they've either been dispatched already by should_dispatch, or should be discarded)
            state.retry_attempts.retain(|_, retry| {
                // Keep if not yet due, or if due but within the last max_backoff period
                // This handles the case where should_dispatch removed the entry for dispatch
                now_ms < retry.due_at_ms || (now_ms - retry.due_at_ms) < max_retry_backoff_ms
            });
        }

        // Step 3: Validate dispatch config
        if let Err(e) = self.config.validate() {
            error!(error = %e, "Config validation failed, skipping dispatch");
            return Ok(());
        }

        // Step 3: Fetch candidate issues
        let active_states = self.config.tracker.active_states();
        let issues = self
            .tracker
            .fetch_candidate_issues(&active_states)
            .await
            .map_err(|e| OrchestratorError::TrackerError(e.to_string()))?;

        // Step 4: Sort and dispatch
        let mut sorted_issues = issues.clone();
        sorted_issues.sort_by(|a, b| {
            let a_rank = priority_rank(a.priority);
            let b_rank = priority_rank(b.priority);
            if a_rank != b_rank {
                return a_rank.cmp(&b_rank);
            }
            // Created at oldest first
            let a_time = a.created_at.map(|dt| dt.timestamp()).unwrap_or(i64::MAX);
            let b_time = b.created_at.map(|dt| dt.timestamp()).unwrap_or(i64::MAX);
            if a_time != b_time {
                return a_time.cmp(&b_time);
            }
            // Identifier tiebreaker
            a.identifier.cmp(&b.identifier)
        });

        // Step 5: Dispatch eligible issues
        let state_arc = Arc::new(self.state.clone());
        let orchestrator_arc: Arc<Self> = Arc::new(self.clone());
        for issue in sorted_issues {
            let slots_available = {
                let state = state_arc.lock().await;
                let running_count = state.running.len() as i32;
                let max = state.max_concurrent_agents;
                max - running_count > 0
            };

            if !slots_available {
                break;
            }

            if self.should_dispatch(&issue).await {
                let identifier = issue
                    .identifier
                    .clone()
                    .unwrap_or_else(|| issue.id.clone().unwrap_or_default());
                match self.workspace_manager.create_for_issue(&identifier) {
                    Ok(workspace) => {
                        if let Err(e) = self.workspace_manager.run_before_run_hook(&workspace.path)
                        {
                            warn!(error = %e, "Before_run hook failed");
                        }
                        if let Err(e) = orchestrator_arc
                            .clone()
                            .dispatch_issue(issue, workspace.path)
                            .await
                        {
                            warn!(error = %e, "Failed to dispatch issue");
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, "Failed to create workspace");
                    }
                }
            }
        }

        Ok(())
    }

    async fn reconcile_running_issues(&self) -> Result<(), OrchestratorError> {
        let running_ids: Vec<String> = {
            let state = self.state.lock().await;
            state.running.keys().cloned().collect()
        };

        if running_ids.is_empty() {
            return Ok(());
        }

        // Check for stalled runs
        let stall_timeout_ms = self.config.codex.stall_timeout_ms();
        let max_retry_backoff_ms = self.config.agent.max_retry_backoff_ms();
        let now = Utc::now();

        // Collect stalled issues to process after releasing lock
        let stalled_entries: Vec<(String, RunningEntry, i32)> = {
            let state = self.state.lock().await;
            state
                .running
                .iter()
                .filter_map(|(issue_id, entry)| {
                    if stall_timeout_ms <= 0 {
                        return None;
                    }
                    let elapsed = entry
                        .last_codex_timestamp
                        .map(|ts| (now - ts).num_milliseconds())
                        .unwrap_or_else(|| (now - entry.started_at).num_milliseconds());

                    if elapsed > stall_timeout_ms {
                        Some((issue_id.clone(), entry.clone(), entry.retry_attempt))
                    } else {
                        None
                    }
                })
                .collect()
        };

        // Process stalled issues outside the lock
        for (issue_id, entry, retry_attempt) in stalled_entries {
            warn!(issue_id = %issue_id, elapsed_ms = ?stall_timeout_ms, retry_attempt = %retry_attempt, "Issue stalled, scheduling retry");
            let backoff_ms = calculate_backoff(retry_attempt, max_retry_backoff_ms);
            let retry_entry = crate::domain::RetryEntry {
                attempt: retry_attempt + 1,
                due_at_ms: now.timestamp_millis() + backoff_ms,
                identifier: entry.identifier.clone(),
                error: Some("Stalled (no activity)".to_string()),
                worker_host: entry.worker_host.clone(),
                workspace_path: entry.workspace_path.clone(),
            };
            let mut state = self.state.lock().await;
            state.running.remove(&issue_id);
            state.claimed.remove(&issue_id);
            state.retry_attempts.insert(issue_id.clone(), retry_entry);
        }

        // Fetch current issue states
        match self.tracker.fetch_issue_states_by_ids(&running_ids).await {
            Ok(refreshed_issues) => {
                let terminal_states: HashSet<String> = self
                    .config
                    .tracker
                    .terminal_states()
                    .iter()
                    .map(|s| normalize_issue_state(s))
                    .collect();
                let active_states: HashSet<String> = self
                    .config
                    .tracker
                    .active_states()
                    .iter()
                    .map(|s| normalize_issue_state(s))
                    .collect();

                let refreshed_map: HashMap<String, crate::domain::Issue> = refreshed_issues
                    .iter()
                    .filter_map(|i| i.id.clone().map(|id| (id, i.clone())))
                    .collect();

                let mut state = self.state.lock().await;

                for issue_id in running_ids {
                    if let Some(refreshed) = refreshed_map.get(&issue_id) {
                        let normalized_state = normalize_issue_state(
                            refreshed.state.as_ref().unwrap_or(&"".to_string()),
                        );

                        if terminal_states.contains(&normalized_state) {
                            // Terminal state - cleanup workspace
                            info!(issue_id = %issue_id, state = %normalized_state, "Issue terminal, cleaning workspace");
                            let _ = self.workspace_manager.remove_issue_workspaces(
                                refreshed.identifier.as_ref().unwrap_or(&issue_id),
                            );
                            state.running.remove(&issue_id);
                            state.claimed.remove(&issue_id);
                        } else if active_states.contains(&normalized_state) {
                            // Still active - update state
                            if let Some(entry) = state.running.get_mut(&issue_id) {
                                entry.issue = refreshed.clone();
                            }
                        } else {
                            // Non-active, non-terminal - stop without cleanup
                            info!(issue_id = %issue_id, state = %normalized_state, "Issue non-active, stopping");
                            state.running.remove(&issue_id);
                            state.claimed.remove(&issue_id);
                        }
                    } else {
                        // Issue not found - stop without cleanup
                        info!(issue_id = %issue_id, "Issue not found in tracker, stopping");
                        state.running.remove(&issue_id);
                        state.claimed.remove(&issue_id);
                    }
                }
            }
            Err(e) => {
                warn!(error = %e, "Failed to refresh issue states, keeping workers running");
            }
        }

        Ok(())
    }

    async fn should_dispatch(&self, issue: &crate::domain::Issue) -> bool {
        // Check basic eligibility
        if issue.id.is_none()
            || issue.identifier.is_none()
            || issue.title.is_none()
            || issue.state.is_none()
        {
            return false;
        }

        let mut state = self.state.lock().await;

        // Not already running
        if state.running.contains_key(issue.id.as_ref().unwrap()) {
            return false;
        }

        // Not already claimed
        if state.claimed.contains(issue.id.as_ref().unwrap()) {
            return false;
        }

        // Check if issue is in retry backoff period
        let issue_id_str = issue.id.as_ref().unwrap();
        if let Some(due_at_ms) = state.retry_attempts.get(issue_id_str).map(|r| r.due_at_ms) {
            let now_ms = Utc::now().timestamp_millis();
            if now_ms < due_at_ms {
                // Still in backoff period
                return false;
            }
            // Backoff elapsed - remove from retry_attempts so it can be re-dispatched
            state.retry_attempts.remove(issue_id_str);
        }

        // Check active/terminal states
        let issue_state = normalize_issue_state(issue.state.as_ref().unwrap());
        let terminal_states: HashSet<String> = self
            .config
            .tracker
            .terminal_states()
            .iter()
            .map(|s| normalize_issue_state(s))
            .collect();
        let active_states: HashSet<String> = self
            .config
            .tracker
            .active_states()
            .iter()
            .map(|s| normalize_issue_state(s))
            .collect();

        if terminal_states.contains(&issue_state) {
            return false;
        }

        if !active_states.contains(&issue_state) {
            return false;
        }

        // Check blocker rule for Todo state
        if issue_state == "todo" && !issue.blocked_by.is_empty() {
            let any_non_terminal_blocker = issue.blocked_by.iter().any(|b| {
                b.state
                    .as_ref()
                    .map(|s| !terminal_states.contains(&normalize_issue_state(s)))
                    .unwrap_or(true)
            });
            if any_non_terminal_blocker {
                return false;
            }
        }

        // Check per-state concurrency
        let max_for_state = self
            .config
            .agent
            .max_concurrent_agents_by_state
            .as_ref()
            .and_then(|m| m.get(&issue_state))
            .copied()
            .unwrap_or(self.config.agent.max_concurrent_agents());

        let running_in_state = state
            .running
            .values()
            .filter(|e| {
                normalize_issue_state(e.issue.state.as_ref().unwrap_or(&"".to_string()))
                    == issue_state
            })
            .count() as i32;

        if running_in_state >= max_for_state {
            return false;
        }

        true
    }

    async fn dispatch_issue(
        self: Arc<Self>,
        issue: crate::domain::Issue,
        workspace: std::path::PathBuf,
    ) -> Result<(), OrchestratorError> {
        let issue_id = issue.id.clone().unwrap();
        let identifier = issue.identifier.clone().unwrap_or_else(|| issue_id.clone());
        let workspace_path_str = workspace.to_string_lossy().to_string();

        info!(issue_id = %issue_id, issue_identifier = %identifier, "Dispatching issue");

        // Insert initial entry (worker will update session_id, pid, etc.)
        let entry = RunningEntry {
            pid: 0,
            identifier: identifier.clone(),
            issue: issue.clone(),
            worker_host: None,
            workspace_path: Some(workspace_path_str.clone()),
            session_id: None,
            last_codex_message: None,
            last_codex_timestamp: None,
            last_codex_event: None,
            codex_app_server_pid: None,
            codex_input_tokens: 0,
            codex_output_tokens: 0,
            codex_total_tokens: 0,
            codex_last_reported_input_tokens: 0,
            codex_last_reported_output_tokens: 0,
            codex_last_reported_total_tokens: 0,
            turn_count: 0,
            retry_attempt: 0,
            started_at: Utc::now(),
        };

        {
            let mut state = self.state.lock().await;
            state.running.insert(issue_id.clone(), entry);
            state.claimed.insert(issue_id.clone());
        }

        // Spawn worker task
        let worker_state = self.state.clone();
        let worker_config = self.config.clone();
        let worker_workspace_manager = self.workspace_manager.clone();
        let worker_codex_client = self.codex_client.clone();
        let worker_workspace_path = workspace.clone();
        let worker_issue_id = issue_id.clone();
        let worker_identifier = identifier.clone();
        let worker_issue = issue.clone();

        tokio::spawn(async move {
            run_worker(
                worker_state,
                worker_config,
                worker_workspace_manager,
                worker_codex_client,
                worker_workspace_path,
                worker_issue_id,
                worker_identifier,
                worker_issue,
            )
            .await;
        });

        Ok(())
    }

    pub async fn get_state(&self) -> OrchestratorRuntimeState {
        self.state.lock().await.clone()
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_worker(
    state: Arc<Mutex<OrchestratorRuntimeState>>,
    config: ConfigSchema,
    workspace_manager: WorkspaceManager,
    codex_client: CodexClient,
    workspace_path: std::path::PathBuf,
    issue_id: String,
    identifier: String,
    issue: crate::domain::Issue,
) {
    let turn_timeout_ms = config.codex.turn_timeout_ms();

    // Start Codex session
    let session_result = codex_client
        .start_session(workspace_path.to_str().unwrap_or(""))
        .await;

    let session = match session_result {
        Ok(s) => {
            // Update state with session_id
            let mut state_guard = state.lock().await;
            if let Some(entry) = state_guard.running.get_mut(&issue_id) {
                entry.session_id = Some(s.thread_id.clone());
                entry.last_codex_timestamp = Some(Utc::now());
                entry.last_codex_message = Some("Session started".to_string());
            }
            s
        }
        Err(e) => {
            error!(issue_id = %issue_id, error = %e, "Failed to start Codex session");
            let mut state_guard = state.lock().await;
            if let Some(entry) = state_guard.running.get_mut(&issue_id) {
                entry.last_codex_message = Some(format!("Session start failed: {}", e));
                entry.last_codex_timestamp = Some(Utc::now());
            }
            // Remove from running - workspace cleanup will be handled by caller
            state_guard.running.remove(&issue_id);
            state_guard.claimed.remove(&issue_id);
            return;
        }
    };

    // Build the turn prompt from the workflow template
    let prompt = build_turn_prompt(&issue);

    // Clone values needed for the callback
    let cb_issue_id = issue_id.clone();
    let cb_state = state.clone();

    // Run a turn with timeout
    let turn_result = tokio::time::timeout(
        std::time::Duration::from_millis(turn_timeout_ms as u64),
        codex_client.run_turn(&session, &prompt, &issue, move |msg| {
            // SAFETY: We use blocking_lock() (std::sync::Mutex) instead of async Mutex
            // because this callback runs synchronously within a spawned tokio task.
            // The callback is invoked by codex_client.run_turn() which is itself called
            // from a spawned worker task (line 485-497). The worker task runs to completion
            // (including this callback) before cleanup_worker is called.
            // Since Arc<Mutex<OrchestratorRuntimeState>> is only accessed here and the
            // JoinHandle is awaited before cleanup, there's no risk of concurrent access.
            // std::sync::Mutex won't poison on panic, unlike tokio's async Mutex.
            if let Some(content) = msg.get("content").and_then(|c| c.as_str()) {
                let mut state_guard = cb_state.blocking_lock();
                if let Some(entry) = state_guard.running.get_mut(&cb_issue_id) {
                    entry.last_codex_message = Some(content.to_string());
                    entry.last_codex_timestamp = Some(Utc::now());
                    entry.turn_count += 1;
                }
            }
            if let Some(tokens) = msg.get("usage") {
                let mut state_guard = cb_state.blocking_lock();
                if let Some(entry) = state_guard.running.get_mut(&cb_issue_id) {
                    if let Some(input) = tokens.get("inputTokens").and_then(|v| v.as_i64()) {
                        entry.codex_input_tokens += input;
                        entry.codex_last_reported_input_tokens = input;
                    }
                    if let Some(output) = tokens.get("outputTokens").and_then(|v| v.as_i64()) {
                        entry.codex_output_tokens += output;
                        entry.codex_last_reported_output_tokens = output;
                    }
                    if let Some(total) = tokens.get("totalTokens").and_then(|v| v.as_i64()) {
                        entry.codex_total_tokens += total;
                        entry.codex_last_reported_total_tokens = total;
                    }
                }
            }
        }),
    )
    .await;

    match turn_result {
        Ok(Ok(())) => {
            // Turn succeeded
            let mut state_guard = state.lock().await;
            if let Some(entry) = state_guard.running.get_mut(&issue_id) {
                entry.last_codex_message = Some("Turn completed".to_string());
                entry.last_codex_timestamp = Some(Utc::now());
            }
        }
        Ok(Err(e)) => {
            // Turn error
            warn!(issue_id = %issue_id, error = %e, "Turn failed");
            handle_turn_error(&state, &issue_id, &e).await;
        }
        Err(_) => {
            // Timeout
            warn!(issue_id = %issue_id, "Turn timed out after {}ms", turn_timeout_ms);
            let mut state_guard = state.lock().await;
            if let Some(entry) = state_guard.running.get_mut(&issue_id) {
                entry.last_codex_message =
                    Some(format!("Turn timed out after {}ms", turn_timeout_ms));
                entry.last_codex_timestamp = Some(Utc::now());
            }
        }
    }

    // Worker cleanup
    cleanup_worker(&state, &workspace_manager, &issue_id, &identifier).await;
}

fn build_turn_prompt(issue: &crate::domain::Issue) -> String {
    let title = issue.title.as_deref().unwrap_or("(no title)");
    let description = issue.description.as_deref().unwrap_or("(no description)");
    let identifier = issue.identifier.as_deref().unwrap_or("(no identifier)");
    let branch = issue.branch_name.as_deref().unwrap_or_else(|| {
        static DEFAULT: std::sync::OnceLock<String> = std::sync::OnceLock::new();
        DEFAULT.get_or_init(|| format!("issue/{}", identifier))
    });

    let mut prompt = format!(
        "Issue: {} - {}\n\nDescription:\n{}\n\n",
        identifier, title, description
    );

    let non_empty_labels: Vec<_> = issue.labels.iter().filter(|l| !l.is_empty()).collect();
    if !non_empty_labels.is_empty() {
        prompt.push_str(&format!(
            "Labels: {}\n",
            non_empty_labels
                .iter()
                .map(|l| l.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }

    prompt.push_str(&format!("\nBranch: {}\n", branch));
    prompt
}

async fn handle_turn_error(
    state: &Arc<Mutex<OrchestratorRuntimeState>>,
    issue_id: &str,
    error: &crate::protocol::CodexError,
) {
    let mut state_guard = state.lock().await;
    if let Some(entry) = state_guard.running.get_mut(issue_id) {
        entry.last_codex_message = Some(format!("Turn error: {}", error));
        entry.last_codex_timestamp = Some(Utc::now());
        entry.retry_attempt += 1;
    }
}

async fn cleanup_worker(
    state: &Arc<Mutex<OrchestratorRuntimeState>>,
    workspace_manager: &WorkspaceManager,
    issue_id: &str,
    identifier: &str,
) {
    // Update state
    {
        let mut state_guard = state.lock().await;
        state_guard.running.remove(issue_id);
        state_guard.claimed.remove(issue_id);
    }

    // Cleanup workspace
    if let Err(e) = workspace_manager.remove_issue_workspaces(identifier) {
        warn!(issue_id = %issue_id, error = %e, "Failed to cleanup workspace");
    }

    // Run after_run hook
    let workspace_key = crate::domain::sanitize_identifier(identifier);
    let workspace_root = &workspace_manager.workspace_root();
    let workspace_path = workspace_root.join(&workspace_key);
    if let Err(e) = workspace_manager.run_after_run_hook(&workspace_path) {
        error!(issue_id = %issue_id, error = %e, "After_run hook failed");
    }

    info!(issue_id = %issue_id, "Worker cleanup complete");
}

/// Calculate exponential backoff with capping.
fn calculate_backoff(retry_attempt: i32, max_backoff_ms: i64) -> i64 {
    (60_000_i64)
        .saturating_mul(2_i64.saturating_pow(retry_attempt as u32))
        .min(max_backoff_ms)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_backoff_exponential() {
        let max = i64::MAX;
        assert_eq!(calculate_backoff(0, max), 60_000);
        assert_eq!(calculate_backoff(1, max), 120_000);
        assert_eq!(calculate_backoff(2, max), 240_000);
        assert_eq!(calculate_backoff(3, max), 480_000);
    }

    #[test]
    fn test_calculate_backoff_caps_at_max() {
        let max = 300_000;
        assert_eq!(calculate_backoff(0, max), 60_000);
        assert_eq!(calculate_backoff(1, max), 120_000);
        assert_eq!(calculate_backoff(2, max), 240_000);
        assert_eq!(calculate_backoff(3, max), 300_000); // capped
        assert_eq!(calculate_backoff(4, max), 300_000); // still capped
    }

    #[test]
    fn test_calculate_backoff_overflow_safety() {
        // With saturating_mul and saturating_pow, large values shouldn't overflow
        let max = i64::MAX;
        // 2^30 * 60000 = 64 trillion, well under i64::MAX
        assert_eq!(calculate_backoff(30, max), 64_424_509_440_000);
        // Even 2^62 * 60000 would overflow, but saturating_pow caps at u32::MAX
        // 2_i64.saturating_pow(u32::MAX) would saturate to i64::MAX
        let result = calculate_backoff(u32::MAX as i32, max);
        assert_eq!(result, max); // saturates to max
    }

    #[test]
    fn test_calculate_backoff_zero_attempt() {
        assert_eq!(calculate_backoff(0, i64::MAX), 60_000);
    }

    #[test]
    fn test_calculate_backoff_large_attempt_saturates() {
        // Very large retry counts should saturate, not overflow
        let max = 300_000;
        // Even with high attempts, backoff is capped by max
        assert_eq!(calculate_backoff(100, max), max);
    }
}
