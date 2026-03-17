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

use clap::{Parser, Subcommand};
use std::path::PathBuf;
use std::sync::Arc;
use tracing::info;

#[derive(Parser)]
#[command(name = "botto", version, about = "Shared backend for Otto extensions")]
struct Cli {
    /// Path to botto.toml config file (optional — auto-detects if missing)
    #[arg(short, long, global = true)]
    config: Option<PathBuf>,

    /// Data directory for SQLite database and temp files
    #[arg(short, long, default_value = "./data", global = true)]
    data_dir: PathBuf,

    /// Override listen address
    #[arg(long)]
    host: Option<String>,

    /// Override listen port
    #[arg(short, long)]
    port: Option<u16>,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Self-evolving prompt engineering harness for the sandbox fix feature
    Harness {
        #[command(subcommand)]
        action: HarnessAction,
    },
}

#[derive(Subcommand)]
enum HarnessAction {
    /// Run the evolution loop
    Run {
        /// Number of evolution rounds
        #[arg(long)]
        rounds: Option<u32>,
        /// Number of prompt variants per round
        #[arg(long)]
        variants: Option<u32>,
        /// Max concurrent sandbox instances
        #[arg(long)]
        concurrency: Option<u32>,
        /// Number of test cases per variant
        #[arg(long)]
        test_cases: Option<u32>,
    },
    /// Generate test cases from GitLab MRs (without running evolution)
    GenerateCases,
    /// Show current best prompt and evolution history
    Report,
    /// Apply the best prompt variant to the production sandbox code
    Apply {
        /// Variant ID to apply (defaults to best)
        #[arg(long)]
        variant: Option<String>,
    },
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

    match cli.command {
        Some(Command::Harness { action }) => run_harness(cfg, &cli.data_dir, action).await,
        None => run_server(cfg, &cli.data_dir).await,
    }
}

/// Run the main Botto server (default behavior).
async fn run_server(cfg: config::BottoConfig, data_dir: &PathBuf) -> anyhow::Result<()> {
    config::print_summary(&cfg);

    // Ensure data directory exists
    tokio::fs::create_dir_all(data_dir).await?;

    // Initialize database
    let db_path = data_dir.join("botto.db");
    let pool = db::init(&db_path).await?;
    info!("database ready at {}", db_path.display());

    // Build shared application state
    let state = types::state::AppState::new(cfg.clone(), pool.clone());

    // Start background review queue manager
    let queue_shutdown = tokio_util::sync::CancellationToken::new();
    let queue_mgr = services::queue::manager::QueueManager::new(state.clone());
    state.set_queue_manager(queue_mgr.clone());
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

    // Warm container reaper — periodically evicts idle and expired containers.
    // Runs every 30s, checks each container against configured timeouts.
    let reaper_handle = if let Some(pool) = state.warm_pool().cloned() {
        let state_for_reaper = state.clone();
        Some(tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
            loop {
                interval.tick().await;
                let cfg = state_for_reaper.config();
                let reaped = pool.reap(
                    cfg.sandbox.warm_idle_timeout_secs,
                    cfg.sandbox.warm_max_lifetime_secs,
                );
                if reaped > 0 {
                    info!("warm pool reaper: evicted {} containers", reaped);
                }
            }
        }))
    } else {
        None
    };

    // Start HTTP + WebSocket server (blocks until shutdown)
    let result = server::run(state.clone()).await;

    // Shutdown background tasks
    queue_shutdown.cancel();
    cleanup_handle.abort();
    if let Some(h) = reaper_handle {
        h.abort();
    }
    let _ = queue_handle.await;

    // Clean up all warm containers on shutdown
    if let Some(pool) = state.warm_pool() {
        info!("shutting down: cleaning up warm containers...");
        pool.remove_all();
    }

    result
}

/// Run harness subcommands.
async fn run_harness(
    cfg: config::BottoConfig,
    data_dir: &PathBuf,
    action: HarnessAction,
) -> anyhow::Result<()> {
    // Harness needs DB for sandbox job tracking
    tokio::fs::create_dir_all(data_dir).await?;
    let db_path = data_dir.join("botto.db");
    let pool = db::init(&db_path).await?;

    // Lightweight event bus (no connected clients during harness runs)
    let event_bus = services::events::EventBus::new();

    match action {
        HarnessAction::Run {
            rounds,
            variants,
            concurrency,
            test_cases,
        } => {
            let opts = services::harness::orchestrator::RunOptions {
                max_rounds: rounds.unwrap_or(cfg.harness.max_rounds),
                variants_per_round: variants.unwrap_or(cfg.harness.variants_per_round),
                concurrency: concurrency.unwrap_or(cfg.harness.concurrency),
                test_case_count: test_cases.unwrap_or(cfg.harness.test_cases),
            };

            info!("starting harness evolution loop");
            info!(
                "  rounds={}, variants={}, concurrency={}, test_cases={}",
                opts.max_rounds, opts.variants_per_round, opts.concurrency, opts.test_case_count,
            );

            let summary =
                services::harness::orchestrator::run(&cfg, &pool, &event_bus, opts).await?;

            println!("\n=== Harness Evolution Complete ===");
            println!("Rounds completed:  {}", summary.rounds_completed);
            println!("Best variant:      {}", summary.best_variant_id);
            println!("Best score:        {:.1}", summary.best_score);
            println!("Baseline score:    {:.1}", summary.baseline_score);
            println!("Improvement:       {:+.1}", summary.improvement);
            println!("Total test runs:   {}", summary.total_test_runs);
            println!(
                "\nResults saved to: {}/",
                cfg.harness.memory_dir.display()
            );
        }

        HarnessAction::GenerateCases => {
            let memory_dir = &cfg.harness.memory_dir;
            services::harness::memory::init_dirs(memory_dir).await?;

            let seeds = services::harness::test_case::seed_test_cases();
            for tc in &seeds {
                services::harness::memory::save_test_case(memory_dir, tc).await?;
            }
            println!("Generated {} seed test cases", seeds.len());
            println!("Saved to: {}/test-cases/", memory_dir.display());
        }

        HarnessAction::Report => {
            let memory_dir = &cfg.harness.memory_dir;

            // Show latest round
            let latest = services::harness::memory::latest_round(memory_dir).await?;
            if latest == 0 {
                println!("No harness runs found. Run `botto harness run` first.");
                return Ok(());
            }

            // List all variants
            let variants = services::harness::memory::list_variants(memory_dir).await?;
            println!("=== Harness Report ===");
            println!("Rounds completed: {}", latest);
            println!("Variants saved:   {}", variants.len());
            println!();

            // Show summary
            let summary = services::harness::memory::read_summary(memory_dir).await?;
            if !summary.is_empty() {
                println!("{}", summary);
            }
        }

        HarnessAction::Apply { variant } => {
            let memory_dir = &cfg.harness.memory_dir;

            // Determine which variant to apply
            let variant_id = match variant {
                Some(id) => id,
                None => {
                    // Find the latest winner from summary
                    let variants = services::harness::memory::list_variants(memory_dir).await?;
                    if variants.is_empty() {
                        anyhow::bail!("No variants found. Run `botto harness run` first.");
                    }
                    // Use the last variant as a heuristic (highest generation)
                    variants.last().unwrap().clone()
                }
            };

            let variant =
                services::harness::memory::load_variant(memory_dir, &variant_id).await?;

            // Validate before applying
            let errors = services::harness::prompts::validate_variant(&variant);
            if !errors.is_empty() {
                println!("Variant {} has validation errors:", variant_id);
                for e in &errors {
                    println!("  - {}", e);
                }
                anyhow::bail!("Cannot apply invalid variant");
            }

            println!("=== Applying Variant {} ===", variant_id);
            println!("Generation:  {}", variant.generation);
            println!("Author:      {}", variant.metadata.author);
            println!("Strategy:    {}", variant.metadata.mutation_strategy.as_deref().unwrap_or("n/a"));
            println!("Notes:       {}", variant.metadata.notes);
            println!();
            println!("Code params:");
            println!("  Setup:  temperature={}, max_tokens={}", variant.code_params.setup.temperature, variant.code_params.setup.max_tokens);
            println!("  Fix:    temperature={}, max_tokens={}", variant.code_params.fix.temperature, variant.code_params.fix.max_tokens);
            println!("  Retry:  temperature={}, max_tokens={}", variant.code_params.retry.temperature, variant.code_params.retry.max_tokens);
            println!("  History: trim_threshold={}, keep_count={}", variant.code_params.history_trim_threshold, variant.code_params.history_keep_count);
            println!();

            // Write the prompts to a well-known location that the sandbox manager can load
            let active_path = memory_dir.join("active_variant.toml");
            let content = toml::to_string_pretty(&variant)?;
            tokio::fs::write(&active_path, &content).await?;

            println!("Variant {} written to {}", variant_id, active_path.display());
            println!("The sandbox manager will use these prompts on next restart.");
        }
    }

    Ok(())
}
