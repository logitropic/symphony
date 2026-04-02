use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("Missing workflow file: {0}")]
    MissingWorkflowFile(String),
    #[error("Workflow parse error: {0}")]
    WorkflowParseError(String),
    #[error("Workflow front matter must be a map")]
    FrontMatterNotMap,
    #[error("Missing tracker kind")]
    MissingTrackerKind,
    #[error("Unsupported tracker kind: {0}")]
    UnsupportedTrackerKind(String),
    #[error("Missing Linear API token")]
    MissingApiKey,
    #[error("Missing Linear project slug")]
    MissingProjectSlug,
    #[error("Missing codex command")]
    MissingCodexCommand,
    #[error("Invalid config: {0}")]
    InvalidConfig(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TrackerConfig {
    pub kind: Option<String>,
    pub endpoint: Option<String>,
    pub api_key: Option<String>,
    pub project_slug: Option<String>,
    pub assignee: Option<String>,
    pub active_states: Option<Vec<String>>,
    pub terminal_states: Option<Vec<String>>,
}

impl TrackerConfig {
    pub fn endpoint(&self) -> String {
        self.endpoint.clone().unwrap_or_else(|| "https://api.linear.app/graphql".to_string())
    }
    pub fn active_states(&self) -> Vec<String> {
        self.active_states.clone().unwrap_or_else(|| vec!["Todo".to_string(), "In Progress".to_string()])
    }
    pub fn terminal_states(&self) -> Vec<String> {
        self.terminal_states.clone().unwrap_or_else(|| vec!["Closed".to_string(), "Cancelled".to_string(), "Canceled".to_string(), "Duplicate".to_string(), "Done".to_string()])
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PollingConfig {
    pub interval_ms: Option<i64>,
}

impl PollingConfig {
    pub fn interval_ms(&self) -> i64 {
        self.interval_ms.unwrap_or(30_000)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WorkspaceConfig {
    pub root: Option<String>,
}

impl WorkspaceConfig {
    pub fn root(&self) -> PathBuf {
        self.root
            .as_ref()
            .map(|s| expand_path(s))
            .unwrap_or_else(|| PathBuf::from(std::env::temp_dir()).join("symphony_workspaces"))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WorkerConfig {
    pub ssh_hosts: Option<Vec<String>>,
    pub max_concurrent_agents_per_host: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AgentConfig {
    pub max_concurrent_agents: Option<i32>,
    pub max_turns: Option<i32>,
    pub max_retry_backoff_ms: Option<i64>,
    pub max_concurrent_agents_by_state: Option<HashMap<String, i32>>,
}

impl AgentConfig {
    pub fn max_concurrent_agents(&self) -> i32 {
        self.max_concurrent_agents.unwrap_or(10)
    }
    pub fn max_turns(&self) -> i32 {
        self.max_turns.unwrap_or(20)
    }
    pub fn max_retry_backoff_ms(&self) -> i64 {
        self.max_retry_backoff_ms.unwrap_or(300_000)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CodexConfig {
    pub command: Option<String>,
    pub approval_policy: Option<serde_json::Value>,
    pub thread_sandbox: Option<String>,
    pub turn_sandbox_policy: Option<serde_json::Value>,
    pub turn_timeout_ms: Option<i64>,
    pub read_timeout_ms: Option<i64>,
    pub stall_timeout_ms: Option<i64>,
}

impl CodexConfig {
    pub fn command(&self) -> String {
        self.command.clone().unwrap_or_else(|| "codex app-server".to_string())
    }
    pub fn turn_timeout_ms(&self) -> i64 {
        self.turn_timeout_ms.unwrap_or(3_600_000)
    }
    pub fn read_timeout_ms(&self) -> i64 {
        self.read_timeout_ms.unwrap_or(5_000)
    }
    pub fn stall_timeout_ms(&self) -> i64 {
        self.stall_timeout_ms.unwrap_or(300_000)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct HooksConfig {
    pub after_create: Option<String>,
    pub before_run: Option<String>,
    pub after_run: Option<String>,
    pub before_remove: Option<String>,
    pub timeout_ms: Option<i64>,
}

impl HooksConfig {
    pub fn timeout_ms(&self) -> i64 {
        self.timeout_ms.unwrap_or(60_000)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ServerConfig {
    pub port: Option<i32>,
    pub host: Option<String>,
}

impl ServerConfig {
    pub fn port(&self) -> Option<i32> {
        self.port
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ObservabilityConfig {
    pub dashboard_enabled: Option<bool>,
    pub refresh_ms: Option<i64>,
    pub render_interval_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigSchema {
    pub tracker: TrackerConfig,
    pub polling: PollingConfig,
    pub workspace: WorkspaceConfig,
    pub worker: WorkerConfig,
    pub agent: AgentConfig,
    pub codex: CodexConfig,
    pub hooks: HooksConfig,
    pub server: ServerConfig,
    pub observability: ObservabilityConfig,
}

impl Default for ConfigSchema {
    fn default() -> Self {
        Self {
            tracker: TrackerConfig::default(),
            polling: PollingConfig::default(),
            workspace: WorkspaceConfig::default(),
            worker: WorkerConfig::default(),
            agent: AgentConfig::default(),
            codex: CodexConfig::default(),
            hooks: HooksConfig::default(),
            server: ServerConfig::default(),
            observability: ObservabilityConfig::default(),
        }
    }
}

impl ConfigSchema {
    pub fn from_workflow(workflow: &Workflow) -> Result<Self, ConfigError> {
        let config_value = &workflow.config;
        let mut config: ConfigSchema = serde_json::from_value(serde_json::to_value(config_value).unwrap_or(serde_json::json!({})))
            .map_err(|e| ConfigError::InvalidConfig(e.to_string()))?;

        // Apply environment indirection for api_key
        if let Some(api_key) = &config.tracker.api_key {
            config.tracker.api_key = Some(resolve_env_value(api_key));
        }

        // If api_key still missing, try LINEAR_API_KEY env var
        if config.tracker.api_key.is_none() {
            if let Ok(env_key) = std::env::var("LINEAR_API_KEY") {
                if !env_key.is_empty() {
                    config.tracker.api_key = Some(env_key);
                }
            }
        }

        Ok(config)
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        let tracker = &self.tracker;

        let kind = tracker.kind.as_ref().ok_or(ConfigError::MissingTrackerKind)?;
        if kind != "linear" {
            return Err(ConfigError::UnsupportedTrackerKind(kind.clone()));
        }

        if tracker.api_key.is_none() || tracker.api_key.as_ref().unwrap().is_empty() {
            return Err(ConfigError::MissingApiKey);
        }

        if tracker.project_slug.is_none() || tracker.project_slug.as_ref().unwrap().is_empty() {
            return Err(ConfigError::MissingProjectSlug);
        }

        if self.codex.command().trim().is_empty() {
            return Err(ConfigError::MissingCodexCommand);
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct Workflow {
    pub config: serde_yaml::Value,
    pub prompt_template: String,
}

impl Workflow {
    pub fn load(path: &PathBuf) -> Result<Self, ConfigError> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| ConfigError::MissingWorkflowFile(e.to_string()))?;
        Self::parse(&content)
    }

    pub fn parse(content: &str) -> Result<Self, ConfigError> {
        let lines: Vec<&str> = content.lines().collect::<Vec<_>>();

        let (front_matter_lines, prompt_lines) = if content.starts_with("---") {
            let rest: Vec<&str> = lines.iter().skip(1).cloned().collect();
            if let Some(end_idx) = rest.iter().position(|l| *l == "---") {
                (rest[..end_idx].to_vec(), rest[end_idx + 1..].to_vec())
            } else {
                (rest.clone(), vec![])
            }
        } else {
            (vec![], lines)
        };

        let config = if front_matter_lines.is_empty() || front_matter_lines.iter().all(|l| l.trim().is_empty()) {
            serde_yaml::Value::Mapping(serde_yaml::Mapping::new())
        } else {
            let yaml_str = front_matter_lines.join("\n");
            let parsed: serde_yaml::Value = serde_yaml::from_str(&yaml_str)
                .map_err(|e| ConfigError::WorkflowParseError(e.to_string()))?;
            if !parsed.is_mapping() {
                return Err(ConfigError::FrontMatterNotMap);
            }
            parsed
        };

        let prompt_template = prompt_lines.join("\n").trim().to_string();

        Ok(Self { config, prompt_template })
    }
}

pub fn expand_path(s: &str) -> PathBuf {
    let s = resolve_env_value(s);
    if s.starts_with('~') {
        if let Ok(home) = std::env::var("HOME") {
            let rest = &s[1..];
            return PathBuf::from(home).join(rest);
        }
    }
    PathBuf::from(s)
}

pub fn resolve_env_value(s: &str) -> String {
    if s.starts_with('$') {
        let var_name = &s[1..];
        std::env::var(var_name).unwrap_or_else(|_| s.to_string())
    } else {
        s.to_string()
    }
}
