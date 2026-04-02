use crate::config::ConfigSchema;
use crate::domain::{
    normalize_issue_state, priority_rank, OrchestratorRuntimeState,
    RunningEntry,
};
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

pub struct Orchestrator {
    state: Arc<Mutex<OrchestratorRuntimeState>>,
    config: ConfigSchema,
    tracker: LinearClient,
    workspace_manager: WorkspaceManager,
    #[allow(dead_code)]
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

        // Initial poll immediately
        self.poll_tick().await?;

        // Run poll loop
        loop {
            let poll_interval = {
                let state = self.state.lock().await;
                state.poll_interval_ms
            };

            tokio::select! {
                _ = sleep(Duration::from_millis(poll_interval as u64)) => {
                    if let Err(e) = self.poll_tick().await {
                        error!(error = %e, "Poll tick failed");
                    }
                }
                _ = self.shutdown_rx.changed() => {
                    if *self.shutdown_rx.borrow() {
                        info!("Orchestrator shutting down");
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    async fn poll_tick(&self) -> Result<(), OrchestratorError> {
        // Step 1: Reconcile running issues
        self.reconcile_running_issues().await?;

        // Step 2: Validate dispatch config
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
            let a_time = a
                .created_at
                .map(|dt| dt.timestamp())
                .unwrap_or(i64::MAX);
            let b_time = b
                .created_at
                .map(|dt| dt.timestamp())
                .unwrap_or(i64::MAX);
            if a_time != b_time {
                return a_time.cmp(&b_time);
            }
            // Identifier tiebreaker
            a.identifier.cmp(&b.identifier)
        });

        // Step 5: Dispatch eligible issues
        for issue in sorted_issues {
            let slots_available = {
                let state = self.state.lock().await;
                let running_count = state.running.len() as i32;
                let max = state.max_concurrent_agents;
                max - running_count > 0
            };

            if !slots_available {
                break;
            }

            if self.should_dispatch(&issue).await {
                if let Err(e) = self.dispatch_issue(issue).await {
                    warn!(error = %e, "Failed to dispatch issue");
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
        let now = Utc::now();

        {
            let state = self.state.lock().await;
            for (issue_id, entry) in state.running.iter() {
                if stall_timeout_ms <= 0 {
                    continue;
                }

                let elapsed = entry
                    .last_codex_timestamp
                    .map(|ts| (now - ts).num_milliseconds())
                    .unwrap_or_else(|| (now - entry.started_at).num_milliseconds());

                if elapsed > stall_timeout_ms {
                    warn!(issue_id = %issue_id, elapsed_ms = elapsed, "Issue stalled, will retry");
                }
            }
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
                        let normalized_state =
                            normalize_issue_state(refreshed.state.as_ref().unwrap_or(&"".to_string()));

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

        let state = self.state.lock().await;

        // Not already running
        if state
            .running
            .contains_key(issue.id.as_ref().unwrap())
        {
            return false;
        }

        // Not already claimed
        if state.claimed.contains(issue.id.as_ref().unwrap()) {
            return false;
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
                normalize_issue_state(
                    &e.issue.state.as_ref().unwrap_or(&"".to_string()),
                ) == issue_state
            })
            .count() as i32;

        if running_in_state >= max_for_state {
            return false;
        }

        true
    }

    async fn dispatch_issue(&self, issue: crate::domain::Issue) -> Result<(), OrchestratorError> {
        let issue_id = issue.id.clone().unwrap();
        let identifier = issue.identifier.clone().unwrap_or_else(|| issue_id.clone());

        info!(issue_id = %issue_id, issue_identifier = %identifier, "Dispatching issue");

        // Create workspace
        let workspace = self
            .workspace_manager
            .create_for_issue(&identifier)
            .map_err(|e| OrchestratorError::WorkspaceError(e.to_string()))?;

        // Run before_run hook
        self.workspace_manager
            .run_before_run_hook(&workspace.path)
            .map_err(|e| OrchestratorError::WorkspaceError(e.to_string()))?;

        // Update state
        let entry = RunningEntry {
            pid: 0, // Would be actual PID in real implementation
            identifier: identifier.clone(),
            issue: issue.clone(),
            worker_host: None,
            workspace_path: Some(workspace.path.to_string_lossy().to_string()),
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
            state
                .running
                .insert(issue_id.clone(), entry);
            state.claimed.insert(issue_id);
        }

        // In real implementation, would spawn worker task here

        Ok(())
    }

    pub async fn get_state(&self) -> OrchestratorRuntimeState {
        self.state.lock().await.clone()
    }
}
