use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{watch, Mutex};
use tracing::{error, info};

pub mod config;
pub mod coordination;
pub mod domain;
pub mod execution;
pub mod integration;
pub mod observability;
pub mod protocol;

use config::{ConfigSchema, Workflow};
use coordination::Orchestrator;
use domain::OrchestratorRuntimeState;
use execution::WorkspaceManager;
use integration::LinearClient;
use observability::{init_logging, run_http_server};
use protocol::CodexClient;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging
    init_logging();

    info!("Symphony Rust starting...");

    // Parse CLI arguments
    let workflow_path = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("WORKFLOW.md"));

    // Load workflow
    let workflow = Workflow::load(&workflow_path)
        .map_err(|e| anyhow::anyhow!("Failed to load workflow: {}", e))?;

    info!(path = %workflow_path.display(), "Workflow loaded");

    // Parse config
    let config = ConfigSchema::from_workflow(&workflow)
        .map_err(|e| anyhow::anyhow!("Invalid config: {}", e))?;

    // Validate config
    config
        .validate()
        .map_err(|e| anyhow::anyhow!("Config validation failed: {}", e))?;

    info!("Configuration validated");

    // Initialize components
    let tracker = LinearClient::new(&config)
        .map_err(|e| anyhow::anyhow!("Failed to create Linear client: {}", e))?;

    let workspace_manager = WorkspaceManager::new(&config);

    let codex_client = CodexClient::new(&config.codex, &config.workspace.root().to_string_lossy());

    // Create shutdown channel
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // Create shared state for HTTP server
    let shared_state: Arc<Mutex<OrchestratorRuntimeState>> =
        Arc::new(Mutex::new(OrchestratorRuntimeState::default()));

    // Start HTTP server if port configured
    if let Some(port) = config.server.port() {
        if port > 0 {
            let state_for_http = shared_state.clone();
            let shutdown_rx_http = shutdown_rx.clone();
            tokio::spawn(async move {
                run_http_server(port as u16, state_for_http, shutdown_rx_http).await;
            });
        }
    }

    // Create and run orchestrator
    let mut orchestrator = Orchestrator::new(
        config,
        tracker,
        workspace_manager,
        codex_client,
        shutdown_rx,
    );

    // Handle shutdown signals
    let shutdown_tx_clone = shutdown_tx.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        info!("Received shutdown signal");
        let _ = shutdown_tx_clone.send(true);
    });

    // Run orchestrator
    if let Err(e) = orchestrator.run().await {
        error!(error = %e, "Orchestrator error");
    }

    info!("Symphony Rust shutting down");
    Ok(())
}
