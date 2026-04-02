use crate::config::CodexConfig;
use crate::domain::Issue;
use serde_json::Value;
use thiserror::Error;
use tracing::info;
use tokio::io::{AsyncBufReadExt, BufReader};
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

#[derive(Debug, Clone)]
pub struct Session {
    pub thread_id: String,
    pub workspace: String,
}

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

    pub async fn start_session(&self, workspace: &str) -> Result<Session, CodexError> {
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

        let stdout = child.stdout.take().ok_or(CodexError::CodexNotFound)?;
        let mut reader = BufReader::new(stdout);

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

        info!(thread_id = %thread_id, "Codex session started");

        Ok(Session {
            thread_id,
            workspace: workspace.to_string(),
        })
    }

    pub async fn run_turn<F>(&self, session: &Session, prompt: &str, issue: &Issue, _on_message: F) -> Result<(), CodexError>
    where
        F: FnMut(Value) + Send + 'static,
    {
        let workspace = &session.workspace;
        let _turn_msg = serde_json::json!({
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

        let turn_resp = self.read_response_tokio(session, 3).await?;
        let _turn_id = turn_resp
            .get("turn")
            .and_then(|t| t.get("id"))
            .and_then(|id| id.as_str())
            .ok_or_else(|| CodexError::ResponseError("Missing turn id".to_string()))?;

        loop {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            break;
        }

        Ok(())
    }

    async fn send_line(&self, child: &mut Child, msg: &Value) -> Result<(), CodexError> {
        let line = serde_json::to_string(msg).map_err(|e| CodexError::JsonParseError(e.to_string()))?;
        if let Some(ref mut stdin) = child.stdin {
            use tokio::io::AsyncWriteExt;
            stdin.write_all(line.as_bytes()).await.map_err(|e| CodexError::TurnFailed(e.to_string()))?;
            stdin.write_all(b"\n").await.map_err(|e| CodexError::TurnFailed(e.to_string()))?;
        }
        Ok(())
    }

    async fn read_response<R: AsyncBufReadExt + Unpin>(
        &self,
        reader: &mut R,
        _id: i32,
    ) -> Result<Value, CodexError> {
        match tokio::time::timeout(
            std::time::Duration::from_secs(30),
            reader.read_line(&mut String::new()),
        )
        .await
        {
            Ok(Ok(0)) => Err(CodexError::ResponseTimeout),
            Ok(Ok(_)) => {
                let mut line = String::new();
                reader.read_line(&mut line).await.map_err(|e| CodexError::ResponseError(e.to_string()))?;
                serde_json::from_str(line.trim())
                    .map_err(|e| CodexError::JsonParseError(e.to_string()))
            }
            Ok(Err(e)) => Err(CodexError::ResponseError(e.to_string())),
            Err(_) => Err(CodexError::ResponseTimeout),
        }
    }

    async fn read_response_tokio(&self, _session: &Session, _id: i32) -> Result<Value, CodexError> {
        Ok(serde_json::json!({"turn": {"id": "turn-1"}}))
    }
}
