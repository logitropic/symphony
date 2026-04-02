use crate::domain::{OrchestratorRuntimeState, TokenTotals};
use serde::Serialize;
use serde_json::Value;
use std::sync::Arc;
use thiserror::Error;
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

pub fn init_logging() {
    use tracing_subscriber::{fmt, prelude::*, EnvFilter};

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::registry()
        .with(fmt::layer().json())
        .with(filter)
        .init();
}

pub async fn run_http_server(port: u16, state: Arc<tokio::sync::Mutex<OrchestratorRuntimeState>>) {
    use tokio::net::TcpListener;

    let addr = format!("127.0.0.1:{}", port);
    let listener = match TcpListener::bind(&addr).await {
        Ok(l) => l,
        Err(e) => {
            warn!(port = port, "Failed to bind HTTP server: {}", e);
            return;
        }
    };

    info!(port = port, "HTTP server listening on {}", addr);

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let state = state.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_connection(stream, &state).await {
                        warn!(error = %e, "Connection handling error");
                    }
                });
            }
            Err(e) => {
                warn!("Failed to accept connection: {}", e);
                continue;
            }
        };
    }
}

async fn handle_connection(
    mut stream: tokio::net::TcpStream,
    state: &Arc<tokio::sync::Mutex<OrchestratorRuntimeState>>,
) -> Result<(), std::io::Error> {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt};

    // Read the full HTTP request (headers + body)
    let mut buf = Vec::new();
    let mut headers = [httparse::EMPTY_HEADER; 32];
    let mut reader = tokio::io::BufReader::new(&mut stream);

    // Parse request line and headers
    let mut req = httparse::Request::new(&mut headers);
    let bytes_read = reader.read_until(b'\n', &mut buf).await?;
    if bytes_read == 0 {
        return Ok(());
    }

    let req_start = String::from_utf8_lossy(&buf);
    let req_start = req_start.trim_end_matches('\n');

    // Handle empty request line (malformed)
    if req_start.is_empty() {
        return Ok(());
    }

    req.parse(req_start.as_bytes()).map_err(|_| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid HTTP request")
    })?;

    let method = req.method.unwrap_or("GET");
    let path = req.path.unwrap_or("/");

    // Read any remaining headers
    loop {
        let mut header_buf = Vec::new();
        reader.read_until(b'\n', &mut header_buf).await?;
        if header_buf == b"\r\n" || header_buf == b"\n" || header_buf.is_empty() {
            break;
        }
    }

    let response = match (method, path) {
        ("GET", "/") | ("GET", "/index.html") => get_dashboard(state).await,
        ("GET", "/api/v1/state") => get_state_json(state).await,
        ("POST", "/api/v1/refresh") => post_refresh(),
        _ => not_found(),
    };

    stream.write_all(response.as_bytes()).await?;
    stream.shutdown().await?;
    Ok(())
}

async fn get_dashboard(state: &Arc<tokio::sync::Mutex<OrchestratorRuntimeState>>) -> String {
    let state = state.lock().await;

    let running_count = state.running.len();
    let retrying_count = state.retry_attempts.len();
    let total_tokens = state.codex_totals.total_tokens;

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

    format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    )
}

async fn get_state_json(state: &Arc<tokio::sync::Mutex<OrchestratorRuntimeState>>) -> String {
    let state = state.lock().await;

    let snapshot = RuntimeSnapshot {
        running: state
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
        retrying: state
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
        codex_totals: state.codex_totals.clone(),
        rate_limits: state.codex_rate_limits.clone(),
        polling: PollingState {
            checking: false,
            next_poll_in_ms: None,
            poll_interval_ms: state.poll_interval_ms,
        },
    };

    let body = serde_json::to_string(&snapshot).unwrap_or_else(|_| "{}".to_string());
    format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    )
}

fn post_refresh() -> String {
    let body = r#"{"queued":true}"#;
    format!(
        "HTTP/1.1 202 Accepted\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    )
}

fn not_found() -> String {
    let body = r#"{"error":"not_found"}"#;
    format!(
        "HTTP/1.1 404 Not Found\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    )
}
