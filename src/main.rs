// ---------------------------------------------------------------------------
// Botto — Shared orchestration backend for Otto Chrome extensions.
//
// Single binary. Auto-configures. SQLite + WebSocket + Docker sandbox.
// ---------------------------------------------------------------------------

use botto::config;
use botto::db;
use botto::server;
use botto::services;
use botto::types;

use clap::Parser;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::info;

#[derive(Parser)]
#[command(name = "botto", version, about = "Shared backend for Otto extensions")]
struct Cli {
    /// Path to botto.toml config file (optional — auto-detects if missing)
    #[arg(short, long)]
    config: Option<PathBuf>,

    /// Data directory for SQLite database and temp files
    #[arg(short, long, default_value = "./data")]
    data_dir: PathBuf,

    /// Override listen address
    #[arg(long)]
    host: Option<String>,

    /// Override listen port
    #[arg(short, long)]
    port: Option<u16>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "botto=info,tower_http=info".into()),
        )
        .init();

    let cli = Cli::parse();

    // Load config (file + auto-detection + CLI overrides)
    let mut cfg = config::load(&cli.config, &cli.data_dir).await?;
    if let Some(host) = cli.host {
        cfg.server.host = host;
    }
    if let Some(port) = cli.port {
        cfg.server.port = port;
    }

    config::print_summary(&cfg);

    // Ensure data directory exists
    tokio::fs::create_dir_all(&cli.data_dir).await?;

    // Initialize database
    let db_path = cli.data_dir.join("botto.db");
    let pool = db::init(&db_path).await?;
    info!("database ready at {}", db_path.display());

    // Build shared application state
    let state = types::state::AppState::new(cfg.clone(), pool.clone());

    // Start background review queue manager
    let queue_shutdown = tokio_util::sync::CancellationToken::new();
    let state_for_queue = state.clone();
    let queue_broadcaster: Arc<dyn Fn(&types::state::MrRef, &str) + Send + Sync> = {
        let s = state_for_queue;
        Arc::new(move |mr, msg| s.broadcast_to_mr(mr, msg))
    };
    let queue_mgr = services::queue::manager::QueueManager::new(
        cfg.clone(),
        pool.clone(),
        state.event_bus().clone(),
        queue_broadcaster,
        state.ai_semaphore().clone(),
    );
    let queue_handle = {
        let shutdown = queue_shutdown.clone();
        tokio::spawn(async move {
            queue_mgr.run(shutdown).await;
        })
    };
    info!("review queue manager started");

    // Periodic cache cleanup (every hour)
    let pool_for_cleanup = pool.clone();
    let cleanup_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(3600));
        loop {
            interval.tick().await;
            match db::queries::purge_expired_reviews(&pool_for_cleanup).await {
                Ok(n) if n > 0 => info!("purged {} expired cache entries", n),
                _ => {}
            }
        }
    });

    // Start HTTP + WebSocket server (blocks until shutdown)
    let result = server::run(state).await;

    // Shutdown background tasks
    queue_shutdown.cancel();
    cleanup_handle.abort();
    let _ = queue_handle.await;

    result
}
