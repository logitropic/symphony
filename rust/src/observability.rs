use crate::domain::{OrchestratorRuntimeState, TokenTotals};
use axum::{
    extract::State,
    response::{Html, Json},
    routing::{get, post},
    Router,
};
use serde::Serialize;
use serde_json::Value;
use std::sync::Arc;
use thiserror::Error;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tower_http::trace::TraceLayer;
use tracing::{info, warn};

#[derive(Error, Debug)]
pub enum HttpError {
    #[error("IO error: {0}")]
    IoError(String),
    #[error("JSON error: {0}")]
    JsonError(String),
    #[error("Not found: {0}")]
    NotFound(String),
}

#[derive(Debug, Serialize)]
pub struct RuntimeSnapshot {
    pub running: Vec<RunningSessionRow>,
    pub retrying: Vec<RetryRow>,
    pub codex_totals: TokenTotals,
    pub rate_limits: Option<Value>,
    pub polling: PollingState,
}

#[derive(Debug, Serialize)]
pub struct RunningSessionRow {
    pub issue_id: String,
    pub identifier: String,
    pub state: String,
    pub session_id: Option<String>,
    pub turn_count: i32,
    pub last_event: Option<String>,
    pub last_message: Option<String>,
    pub started_at: String,
    pub last_event_at: Option<String>,
    pub tokens: TokenRow,
}

#[derive(Debug, Serialize)]
pub struct TokenRow {
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub total_tokens: i64,
}

#[derive(Debug, Serialize)]
pub struct RetryRow {
    pub issue_id: String,
    pub identifier: String,
    pub attempt: i32,
    pub due_in_ms: i64,
    pub error: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct PollingState {
    pub checking: bool,
    pub next_poll_in_ms: Option<i64>,
    pub poll_interval_ms: i64,
}

#[derive(Clone)]
struct AppState {
    runtime: Arc<Mutex<OrchestratorRuntimeState>>,
}

pub fn init_logging() {
    use tracing_subscriber::{fmt, prelude::*, EnvFilter};

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let _ = tracing_subscriber::registry()
        .with(fmt::layer().json())
        .with(filter)
        .try_init();
}

pub async fn run_http_server(
    port: u16,
    state: Arc<Mutex<OrchestratorRuntimeState>>,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
) {
    let app_state = AppState { runtime: state };

    let app = Router::new()
        .route("/", get(dashboard_handler))
        .route("/index.html", get(dashboard_handler))
        .route("/api/v1/state", get(state_handler))
        .route("/api/v1/refresh", post(refresh_handler))
        .layer(TraceLayer::new_for_http())
        .with_state(app_state);

    let addr = format!("127.0.0.1:{}", port);
    let listener = match TcpListener::bind(&addr).await {
        Ok(l) => l,
        Err(e) => {
            warn!(port = port, "Failed to bind HTTP server: {}", e);
            return;
        }
    };

    info!(port = port, "HTTP server listening on {}", addr);

    // Create a future that completes when shutdown signal is received
    let shutdown_future = async {
        let mut rx = shutdown_rx;
        while rx.changed().await.is_ok() {
            if *rx.borrow() {
                info!("HTTP server received shutdown signal");
                break;
            }
        }
    };

    // Race the server against the shutdown signal
    tokio::select! {
        result = axum::serve(listener, app) => {
            if let Err(e) = result {
                warn!(port = port, "HTTP server error: {}", e);
            }
        }
        _ = shutdown_future => {
            info!("HTTP server shutting down");
        }
    }
}

async fn dashboard_handler(State(state): State<AppState>) -> Html<String> {
    let runtime = state.runtime.lock().await;
    let running_count = runtime.running.len();
    let retrying_count = runtime.retry_attempts.len();
    let total_tokens = runtime.codex_totals.total_tokens;

    let body = format!(
        r#"<!DOCTYPE html>
<html>
<head><title>Symphony Dashboard</title></head>
<body>
<h1>Symphony</h1>
<p>Running: {} | Retrying: {} | Total Tokens: {}</p>
</body>
</html>"#,
        running_count, retrying_count, total_tokens
    );

    Html(body)
}

async fn state_handler(State(state): State<AppState>) -> Json<RuntimeSnapshot> {
    let runtime = state.runtime.lock().await;

    let snapshot = RuntimeSnapshot {
        running: runtime
            .running
            .values()
            .map(|entry| RunningSessionRow {
                issue_id: entry.issue.id.clone().unwrap_or_default(),
                identifier: entry.identifier.clone(),
                state: entry.issue.state.clone().unwrap_or_default(),
                session_id: entry.session_id.clone(),
                turn_count: entry.turn_count,
                last_event: entry.last_codex_event.clone(),
                last_message: entry.last_codex_message.clone(),
                started_at: entry.started_at.to_rfc3339(),
                last_event_at: entry
                    .last_codex_timestamp
                    .as_ref()
                    .map(|ts| ts.to_rfc3339()),
                tokens: TokenRow {
                    input_tokens: entry.codex_input_tokens,
                    output_tokens: entry.codex_output_tokens,
                    total_tokens: entry.codex_total_tokens,
                },
            })
            .collect(),
        retrying: runtime
            .retry_attempts
            .iter()
            .map(
                |(id, entry): (&String, &crate::domain::RetryEntry)| RetryRow {
                    issue_id: id.clone(),
                    identifier: entry.identifier.clone(),
                    attempt: entry.attempt,
                    due_in_ms: entry.due_at_ms - chrono::Utc::now().timestamp_millis(),
                    error: entry.error.clone(),
                },
            )
            .collect(),
        codex_totals: runtime.codex_totals.clone(),
        rate_limits: runtime.codex_rate_limits.clone(),
        polling: PollingState {
            checking: false,
            next_poll_in_ms: None,
            poll_interval_ms: runtime.poll_interval_ms,
        },
    };

    Json(snapshot)
}

async fn refresh_handler() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "queued": true }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{Issue, OrchestratorRuntimeState, RunningEntry};
    use chrono::Utc;

    fn create_test_state() -> OrchestratorRuntimeState {
        let issue = Issue {
            id: Some("TEST-1".to_string()),
            identifier: Some("TEST-1".to_string()),
            title: Some("Test Issue".to_string()),
            description: Some("A test issue".to_string()),
            priority: Some(2),
            state: Some("In Progress".to_string()),
            branch_name: Some("issue/TEST-1".to_string()),
            url: Some("https://linear.app/test/TEST-1".to_string()),
            assignee_id: None,
            labels: vec![],
            blocked_by: vec![],
            assigned_to_worker: true,
            created_at: Some(Utc::now()),
            updated_at: Some(Utc::now()),
        };

        let running_entry = RunningEntry {
            pid: 1234,
            identifier: "TEST-1".to_string(),
            issue,
            worker_host: None,
            workspace_path: Some("/tmp/workspace/test-1".to_string()),
            session_id: Some("session-abc".to_string()),
            last_codex_message: Some("Working on it".to_string()),
            last_codex_timestamp: Some(Utc::now()),
            last_codex_event: None,
            codex_app_server_pid: None,
            codex_input_tokens: 100,
            codex_output_tokens: 200,
            codex_total_tokens: 300,
            codex_last_reported_input_tokens: 100,
            codex_last_reported_output_tokens: 200,
            codex_last_reported_total_tokens: 300,
            turn_count: 5,
            retry_attempt: 0,
            started_at: Utc::now(),
        };

        let mut state = OrchestratorRuntimeState::default();
        state.running.insert("TEST-1".to_string(), running_entry);
        state.poll_interval_ms = 30_000;
        state.max_concurrent_agents = 10;
        state
    }

    #[tokio::test]
    async fn test_dashboard_handler() {
        let state = create_test_state();
        let app_state = AppState {
            runtime: Arc::new(Mutex::new(state)),
        };

        let response = dashboard_handler(State(app_state)).await;
        assert!(response.0.contains("Running: 1"));
        assert!(response.0.contains("Retrying: 0"));
    }

    #[tokio::test]
    async fn test_state_handler() {
        let state = create_test_state();
        let app_state = AppState {
            runtime: Arc::new(Mutex::new(state)),
        };

        let response = state_handler(State(app_state)).await;
        assert_eq!(response.running.len(), 1);
        assert_eq!(response.running[0].identifier, "TEST-1");
        assert_eq!(response.running[0].turn_count, 5);
        assert_eq!(response.polling.poll_interval_ms, 30_000);
    }

    #[tokio::test]
    async fn test_refresh_handler() {
        let response = refresh_handler().await;
        assert_eq!(response.0.get("queued"), Some(&serde_json::json!(true)));
    }
}
