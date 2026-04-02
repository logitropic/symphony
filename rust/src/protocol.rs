use crate::config::CodexConfig;
use crate::domain::Issue;
use serde_json::Value;
use std::sync::Arc;
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command as AsyncCommand};

#[derive(Error, Debug)]
pub enum CodexError {
    #[error("Codex not found")]
    CodexNotFound,
    #[error("Invalid workspace cwd: {0}")]
    InvalidWorkspaceCwd(String),
    #[error("Response timeout")]
    ResponseTimeout,
    #[error("Turn timeout")]
    TurnTimeout,
    #[error("Port exit: {0}")]
    PortExit(i32),
    #[error("Response error: {0}")]
    ResponseError(String),
    #[error("Turn failed: {0}")]
    TurnFailed(String),
    #[error("Turn cancelled")]
    TurnCancelled,
    #[error("Turn input required")]
    TurnInputRequired,
    #[error("JSON parse error: {0}")]
    JsonParseError(String),
}

pub struct Session {
    pub thread_id: String,
    pub workspace: String,
    child: Arc<std::sync::Mutex<Option<Child>>>,
    reader: Arc<tokio::sync::Mutex<Option<BufReader<tokio::process::ChildStdout>>>>,
}

impl Session {
    fn new(thread_id: String, workspace: String) -> Self {
        Self {
            thread_id,
            workspace,
            child: Arc::new(std::sync::Mutex::new(None)),
            reader: Arc::new(tokio::sync::Mutex::new(None)),
        }
    }

    async fn set_child_reader(&self, child: Child, reader: BufReader<tokio::process::ChildStdout>) {
        *self.child.lock().unwrap() = Some(child);
        // At this point the lock was just created in `new()`, so it should be available
        let mut guard = self.reader.lock().await;
        *guard = Some(reader);
    }
}

#[derive(Clone)]
pub struct CodexClient {
    config: CodexConfig,
    workspace_root: String,
}

impl CodexClient {
    pub fn new(config: &CodexConfig, workspace_root: &str) -> Self {
        Self {
            config: config.clone(),
            workspace_root: workspace_root.to_string(),
        }
    }

    pub async fn start_session(&self, workspace: &str) -> Result<Arc<Session>, CodexError> {
        let canonical_workspace = std::path::Path::new(workspace)
            .canonicalize()
            .map_err(|e| CodexError::InvalidWorkspaceCwd(e.to_string()))?;

        let canonical_root = std::path::Path::new(&self.workspace_root)
            .canonicalize()
            .map_err(|e| CodexError::InvalidWorkspaceCwd(e.to_string()))?;

        let workspace_str = canonical_workspace.to_string_lossy();
        let root_str = canonical_root.to_string_lossy();

        if !workspace_str.starts_with(&format!("{}/", root_str)) {
            return Err(CodexError::InvalidWorkspaceCwd(format!(
                "{} not in {}",
                workspace, self.workspace_root
            )));
        }

        let mut child = AsyncCommand::new("bash")
            .args(["-lc", &self.config.command()])
            .current_dir(workspace)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map_err(|_| CodexError::CodexNotFound)?;

        // Read responses from the child's stdout using a BufReader
        let stdout = child.stdout.take().ok_or(CodexError::CodexNotFound)?;
        let mut reader: BufReader<tokio::process::ChildStdout> = BufReader::new(stdout);

        let init_msg = serde_json::json!({
            "method": "initialize",
            "id": 1,
            "params": {
                "clientInfo": {"name": "symphony", "version": "1.0"},
                "capabilities": {"experimentalApi": true}
            }
        });

        self.send_line(&mut child, &init_msg).await?;

        let _resp = self.read_response(&mut reader, 1).await?;

        let init_notif = serde_json::json!({"method": "initialized", "params": {}});
        self.send_line(&mut child, &init_notif).await?;

        let thread_msg = serde_json::json!({
            "method": "thread/start",
            "id": 2,
            "params": {
                "approvalPolicy": self.config.approval_policy.clone().unwrap_or_else(|| serde_json::json!("never")),
                "sandbox": self.config.thread_sandbox.as_deref().unwrap_or("workspace-write"),
                "cwd": workspace
            }
        });

        self.send_line(&mut child, &thread_msg).await?;

        let thread_resp = self.read_response(&mut reader, 2).await?;
        let thread_id = thread_resp
            .get("thread")
            .and_then(|t| t.get("id"))
            .and_then(|id| id.as_str())
            .ok_or_else(|| CodexError::ResponseError("Missing thread id".to_string()))?
            .to_string();

        let session = Session::new(thread_id, workspace.to_string());

        // Re-wrap stdout in a fresh BufReader (reader was consumed)
        let stdout = child.stdout.take().ok_or(CodexError::CodexNotFound)?;
        let reader = BufReader::new(stdout);
        session.set_child_reader(child, reader).await;

        Ok(Arc::new(session))
    }

    pub async fn run_turn<F>(
        &self,
        session: &Session,
        prompt: &str,
        issue: &Issue,
        mut on_message: F,
    ) -> Result<(), CodexError>
    where
        F: FnMut(Value) + Send + 'static,
    {
        let workspace = &session.workspace;

        let turn_msg = serde_json::json!({
            "method": "turn/start",
            "id": 3,
            "params": {
                "threadId": session.thread_id,
                "input": [{"type": "text", "text": prompt}],
                "cwd": workspace,
                "title": format!("{}: {}", issue.identifier.as_ref().unwrap_or(&"".to_string()), issue.title.as_ref().unwrap_or(&"".to_string())),
                "approvalPolicy": self.config.approval_policy.clone().unwrap_or_else(|| serde_json::json!("never")),
                "sandboxPolicy": self.config.turn_sandbox_policy.clone().unwrap_or_else(|| serde_json::json!({}))
            }
        });

        self.send_turn_message(session, &turn_msg).await?;
        let turn_resp = self.read_response_tokio(session, 3).await?;
        let turn_id = turn_resp
            .get("turn")
            .and_then(|t| t.get("id"))
            .and_then(|id| id.as_str())
            .ok_or_else(|| CodexError::ResponseError("Missing turn id".to_string()))?;

        on_message(serde_json::json!({
            "method": "turn/started",
            "params": { "turn": { "id": turn_id } }
        }));

        let timeout_ms = self.config.turn_timeout_ms() as u64;
        let timeout = std::time::Duration::from_millis(timeout_ms);

        loop {
            let event = tokio::time::timeout(timeout, self.read_event(session)).await;

            match event {
                Ok(Ok(Some(event))) => {
                    let method = event.get("method").and_then(|m| m.as_str()).unwrap_or("");

                    if method == "turn/completed" {
                        if let Some(u) = event.get("params").and_then(|p| p.get("usage")) {
                            on_message(serde_json::json!({
                                "method": "usage",
                                "params": u
                            }));
                        }
                        on_message(event);
                        return Ok(());
                    }

                    if method == "turn/failed" {
                        let reason = event
                            .get("params")
                            .map(|p| p.to_string())
                            .unwrap_or_default();
                        on_message(event);
                        return Err(CodexError::TurnFailed(reason));
                    }

                    if method == "turn/cancelled" {
                        on_message(event);
                        return Err(CodexError::TurnCancelled);
                    }

                    if method == "item/tool/call" {
                        // Echo tool call to caller
                        on_message(event.clone());
                        // Send empty success response for tool calls
                        if let Some(id) = event.get("id") {
                            let result = serde_json::json!({
                                "id": id,
                                "result": { "success": true, "output": "", "contentItems": [] }
                            });
                            self.send_turn_message(session, &result).await?;
                        }
                    } else {
                        on_message(event);
                    }
                }
                Ok(Ok(None)) => {
                    return Err(CodexError::ResponseTimeout);
                }
                Ok(Err(e)) => {
                    return Err(CodexError::ResponseError(e.to_string()));
                }
                Err(_) => {
                    return Err(CodexError::TurnTimeout);
                }
            }
        }
    }

    async fn send_turn_message(&self, session: &Session, msg: &Value) -> Result<(), CodexError> {
        let line =
            serde_json::to_string(msg).map_err(|e| CodexError::JsonParseError(e.to_string()))?;
        let mut stdin_opt = None;
        {
            let mut child_guard = session.child.lock().unwrap();
            if let Some(ref mut child) = *child_guard {
                stdin_opt = child.stdin.take();
            }
        }
        if let Some(ref mut stdin) = stdin_opt {
            stdin
                .write_all(line.as_bytes())
                .await
                .map_err(|e| CodexError::TurnFailed(e.to_string()))?;
            stdin
                .write_all(b"\n")
                .await
                .map_err(|e| CodexError::TurnFailed(e.to_string()))?;
        }
        Ok(())
    }

    async fn read_event(&self, session: &Session) -> Result<Option<Value>, CodexError> {
        let mut line = String::new();

        // Extract reader from mutex, drop lock before await, then restore
        let reader_opt = session.reader.lock().await.take();
        let mut reader = match reader_opt {
            Some(r) => r,
            None => {
                return Err(CodexError::ResponseError(
                    "Session reader not initialized".to_string(),
                ));
            }
        };

        let result = reader.read_line(&mut line).await;

        // Restore reader to mutex
        *session.reader.lock().await = Some(reader);

        match result {
            Ok(0) => Ok(None),
            Ok(_) => {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    return Ok(Some(serde_json::json!({})));
                }
                serde_json::from_str(trimmed)
                    .map_err(|e| CodexError::JsonParseError(e.to_string()))
                    .map(Some)
            }
            Err(e) => Err(CodexError::ResponseError(e.to_string())),
        }
    }

    async fn send_line(&self, child: &mut Child, msg: &Value) -> Result<(), CodexError> {
        let line =
            serde_json::to_string(msg).map_err(|e| CodexError::JsonParseError(e.to_string()))?;
        if let Some(ref mut stdin) = child.stdin {
            stdin
                .write_all(line.as_bytes())
                .await
                .map_err(|e| CodexError::TurnFailed(e.to_string()))?;
            stdin
                .write_all(b"\n")
                .await
                .map_err(|e| CodexError::TurnFailed(e.to_string()))?;
        }
        Ok(())
    }

    async fn read_response<R: AsyncBufReadExt + Unpin>(
        &self,
        reader: &mut R,
        _id: i32,
    ) -> Result<Value, CodexError> {
        let mut line = String::new();
        match tokio::time::timeout(
            std::time::Duration::from_secs(30),
            reader.read_line(&mut line),
        )
        .await
        {
            Ok(Ok(0)) => Err(CodexError::ResponseTimeout),
            Ok(Ok(_)) => serde_json::from_str(line.trim())
                .map_err(|e| CodexError::JsonParseError(e.to_string())),
            Ok(Err(e)) => Err(CodexError::ResponseError(e.to_string())),
            Err(_) => Err(CodexError::ResponseTimeout),
        }
    }

    async fn read_response_tokio(&self, session: &Session, _id: i32) -> Result<Value, CodexError> {
        let timeout_ms = self.config.read_timeout_ms() as u64;
        let timeout = std::time::Duration::from_millis(timeout_ms);
        let mut line = String::new();

        // Extract reader, drop lock before await, then restore
        let reader_opt = session.reader.lock().await.take();
        let mut reader = match reader_opt {
            Some(r) => r,
            None => {
                return Err(CodexError::ResponseError(
                    "Session reader not initialized".to_string(),
                ));
            }
        };

        let result = tokio::time::timeout(timeout, reader.read_line(&mut line)).await;

        // Restore reader
        *session.reader.lock().await = Some(reader);

        match result {
            Ok(Ok(0)) => Err(CodexError::ResponseTimeout),
            Ok(Ok(_)) => serde_json::from_str(line.trim())
                .map_err(|e| CodexError::JsonParseError(e.to_string())),
            Ok(Err(e)) => Err(CodexError::ResponseError(e.to_string())),
            Err(_) => Err(CodexError::ResponseTimeout),
        }
    }
}
