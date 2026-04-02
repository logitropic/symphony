use crate::config::{ConfigSchema, HooksConfig};
use crate::domain::{Workspace, sanitize_identifier};
use std::path::{Path, PathBuf};
use std::process::Command;
use thiserror::Error;
use tracing::{info, warn};

#[derive(Error, Debug)]
pub enum WorkspaceError {
    #[error("Workspace creation failed: {0}")]
    CreationFailed(String),
    #[error("Invalid workspace path: {0}")]
    InvalidPath(String),
    #[error("Hook failed: {0}")]
    HookFailed(String),
    #[error("Hook timeout")]
    HookTimeout,
    #[error("Workspace path outside root")]
    PathOutsideRoot,
    #[error("Workspace equals root")]
    WorkspaceEqualsRoot,
}

pub struct WorkspaceManager {
    workspace_root: PathBuf,
    hooks: HooksConfig,
}

impl WorkspaceManager {
    pub fn new(config: &ConfigSchema) -> Self {
        Self {
            workspace_root: config.workspace.root(),
            hooks: config.hooks.clone(),
        }
    }

    pub fn create_for_issue(&self, identifier: &str) -> Result<Workspace, WorkspaceError> {
        let workspace_key = sanitize_identifier(identifier);
        let workspace_path = self.workspace_root.join(&workspace_key);

        // Ensure workspace root exists
        if !self.workspace_root.exists() {
            std::fs::create_dir_all(&self.workspace_root)
                .map_err(|e| WorkspaceError::CreationFailed(e.to_string()))?;
        }

        // Check if path is valid (inside root)
        self.validate_workspace_path(&workspace_path)?;

        // Determine if newly created
        let created_now = if workspace_path.exists() {
            if workspace_path.is_dir() {
                false
            } else {
                // Replace file with directory
                std::fs::remove_file(&workspace_path)
                    .map_err(|e| WorkspaceError::CreationFailed(e.to_string()))?;
                std::fs::create_dir_all(&workspace_path)
                    .map_err(|e| WorkspaceError::CreationFailed(e.to_string()))?;
                true
            }
        } else {
            std::fs::create_dir_all(&workspace_path)
                .map_err(|e| WorkspaceError::CreationFailed(e.to_string()))?;
            true
        };

        // Run after_create hook if newly created
        if created_now {
            if let Some(hook) = &self.hooks.after_create {
                self.run_hook(hook, &workspace_path, "after_create")?;
            }
        }

        Ok(Workspace {
            path: workspace_path,
            workspace_key,
            created_now,
        })
    }

    pub fn remove_issue_workspaces(&self, identifier: &str) -> Result<(), WorkspaceError> {
        let workspace_key = sanitize_identifier(identifier);
        let workspace_path = self.workspace_root.join(&workspace_key);

        if workspace_path.exists() {
            // Run before_remove hook
            if let Some(hook) = &self.hooks.before_remove {
                let _ = self.run_hook_best_effort(hook, &workspace_path, "before_remove");
            }

            std::fs::remove_dir_all(&workspace_path)
                .map_err(|e| WorkspaceError::CreationFailed(e.to_string()))?;
        }

        Ok(())
    }

    pub fn run_before_run_hook(&self, workspace: &Path) -> Result<(), WorkspaceError> {
        if let Some(hook) = &self.hooks.before_run {
            self.run_hook(hook, workspace, "before_run")?;
        }
        Ok(())
    }

    pub fn run_after_run_hook(&self, workspace: &Path) {
        if let Some(hook) = &self.hooks.after_run {
            let _ = self.run_hook_best_effort(hook, workspace, "after_run");
        }
    }

    fn validate_workspace_path(&self, workspace_path: &Path) -> Result<(), WorkspaceError> {
        let canonical_workspace = workspace_path.canonicalize()
            .map_err(|e| WorkspaceError::InvalidPath(e.to_string()))?;
        let canonical_root = self.workspace_root.canonicalize()
            .map_err(|e| WorkspaceError::InvalidPath(e.to_string()))?;

        if canonical_workspace == canonical_root {
            return Err(WorkspaceError::WorkspaceEqualsRoot);
        }

        let workspace_str = canonical_workspace.to_string_lossy();
        let root_str = canonical_root.to_string_lossy();

        if !workspace_str.starts_with(&format!("{}/", root_str)) {
            return Err(WorkspaceError::PathOutsideRoot);
        }

        Ok(())
    }

    fn run_hook(&self, script: &str, workspace: &Path, hook_name: &str) -> Result<(), WorkspaceError> {
        info!(workspace = %workspace.display(), hook = hook_name, "Running workspace hook");

        let output = Command::new("sh")
            .args(["-lc", script])
            .current_dir(workspace)
            .output()
            .map_err(|e| WorkspaceError::CreationFailed(e.to_string()))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            warn!(hook = hook_name, status = output.status.code().unwrap_or(-1), stderr = %stderr, "Workspace hook failed");
            return Err(WorkspaceError::HookFailed(stderr.to_string()));
        }

        Ok(())
    }

    fn run_hook_best_effort(&self, script: &str, workspace: &Path, hook_name: &str) -> Result<(), WorkspaceError> {
        match self.run_hook(script, workspace, hook_name) {
            Ok(()) => Ok(()),
            Err(e) => {
                warn!(workspace = %workspace.display(), hook = hook_name, error = %e, "Hook failed, ignoring");
                Ok(())
            }
        }
    }
}
