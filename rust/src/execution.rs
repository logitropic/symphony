use crate::config::{ConfigSchema, HooksConfig};
use crate::domain::{sanitize_identifier, Workspace};
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

#[derive(Clone)]
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

    pub fn workspace_root(&self) -> &PathBuf {
        &self.workspace_root
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

    pub fn run_after_run_hook(&self, workspace: &Path) -> Result<(), WorkspaceError> {
        if let Some(hook) = &self.hooks.after_run {
            return self.run_hook_best_effort(hook, workspace, "after_run");
        }
        Ok(())
    }

    fn validate_workspace_path(&self, workspace_path: &Path) -> Result<(), WorkspaceError> {
        let canonical_workspace = workspace_path
            .canonicalize()
            .map_err(|e| WorkspaceError::InvalidPath(e.to_string()))?;
        let canonical_root = self
            .workspace_root
            .canonicalize()
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

    fn run_hook(
        &self,
        script: &str,
        workspace: &Path,
        hook_name: &str,
    ) -> Result<(), WorkspaceError> {
        info!(workspace = %workspace.display(), hook = hook_name, "Running workspace hook");

        // Security note: `script` is executed via `sh -lc` which interprets the script
        // according to shell semantics. This is safe when `script` comes from trusted
        // configuration (e.g., WORKFLOW.md YAML config controlled by operators), but could
        // be a vector for command injection if `script` ever derives from user-controlled
        // input. Ensure the WORKFLOW.md configuration is always from a trusted source.

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

    fn run_hook_best_effort(
        &self,
        script: &str,
        workspace: &Path,
        hook_name: &str,
    ) -> Result<(), WorkspaceError> {
        match self.run_hook(script, workspace, hook_name) {
            Ok(()) => Ok(()),
            Err(e) => {
                warn!(workspace = %workspace.display(), hook = hook_name, error = %e, "Hook failed, ignoring");
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod workspace_manager_tests {
    use super::*;
    use crate::config::{ConfigSchema, HooksConfig, WorkspaceConfig};

    fn temp_root() -> std::path::PathBuf {
        std::env::temp_dir().join(format!("symphony_test_{}", std::process::id()))
    }

    fn make_manager() -> WorkspaceManager {
        let root = temp_root();
        let config = ConfigSchema {
            workspace: WorkspaceConfig {
                root: Some(root.to_string_lossy().to_string()),
            },
            hooks: HooksConfig::default(),
            ..Default::default()
        };
        WorkspaceManager::new(&config)
    }

    fn cleanup() {
        let _ = std::fs::remove_dir_all(temp_root());
    }

    struct TestGuard;

    impl TestGuard {
        fn new() -> Self {
            cleanup();
            Self
        }
    }

    impl Drop for TestGuard {
        fn drop(&mut self) {
            cleanup();
        }
    }

    #[test]
    fn test_workspace_path_valid_inside_root() {
        let _guard = TestGuard::new();
        let manager = make_manager();
        let root = temp_root();
        std::fs::create_dir_all(&root).unwrap();

        let workspace_path = root.join("valid-issue");
        std::fs::create_dir_all(&workspace_path).unwrap();

        let result = manager.validate_workspace_path(&workspace_path);
        assert!(result.is_ok(), "valid nested path should be ok");
    }

    #[test]
    fn test_workspace_path_equals_root() {
        let _guard = TestGuard::new();
        let manager = make_manager();
        let root = temp_root();
        std::fs::create_dir_all(&root).ok();

        let result = manager.validate_workspace_path(&root);
        // Root path itself should be rejected (either WorkspaceEqualsRoot or other error)
        assert!(result.is_err(), "root path itself should be rejected");
    }

    #[test]
    fn test_workspace_path_outside_root_symlink() {
        let _guard = TestGuard::new();
        let manager = make_manager();
        let root = temp_root();
        let sibling = temp_root().parent().unwrap().join("symphony_test_sibling");
        std::fs::create_dir_all(&root).ok();
        std::fs::create_dir_all(&sibling).ok();

        // Create a symlink inside root pointing outside
        let symlink_path = root.join("escape");
        #[cfg(unix)]
        std::os::unix::fs::symlink(&sibling, &symlink_path).ok();
        #[cfg(not(unix))]
        std::fs::create_dir_all(&symlink_path).ok();

        let result = manager.validate_workspace_path(&symlink_path);
        assert!(result.is_err(), "symlink escaping root should be rejected");
    }

    #[test]
    fn test_workspace_path_outside_root_parent_traversal() {
        let _guard = TestGuard::new();
        let manager = make_manager();
        let root = temp_root();
        std::fs::create_dir_all(&root).ok();

        // Create a sibling directory
        let sibling = root.parent().unwrap().join("symphony_test_sibling");
        std::fs::create_dir_all(&sibling).ok();

        // The sibling path is outside root
        let result = manager.validate_workspace_path(&sibling);
        assert!(result.is_err(), "sibling path should be rejected");
    }

    #[test]
    fn test_workspace_path_deeply_nested_valid() {
        let _guard = TestGuard::new();
        let manager = make_manager();
        let root = temp_root();

        // Ensure root exists
        std::fs::create_dir_all(&root).ok();

        let workspace_path = root.join("a/b/c/d/e");
        // Create the nested directory
        std::fs::create_dir_all(&workspace_path).ok();

        let result = manager.validate_workspace_path(&workspace_path);
        assert!(
            result.is_ok(),
            "deeply nested path should be valid: {:?}",
            result
        );
    }
}
