// ---------------------------------------------------------------------------
// Mentor Pruner — background task that decays confidence and removes stale
// knowledge entries.
//
// Runs on a configurable interval (default: daily). Each tick:
//   1. Decay confidence for entries not queried in the last 24h
//   2. Prune entries below the confidence threshold
//
// Spawned as a tokio task on startup. Cancelled via the shutdown token.
// ---------------------------------------------------------------------------

use tokio::time::{interval, Duration};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::services::mentor::client::MentorClient;

/// Configuration for the pruner, extracted from MentorConfig at startup.
#[derive(Debug, Clone)]
pub struct PrunerConfig {
    /// Prune entries with confidence below this threshold.
    pub prune_below_confidence: f64,
    /// How often to run the pruner (seconds).
    pub interval_secs: u64,
    /// Multiplicative decay factor applied each tick to un-queried entries.
    /// Default: 0.95 (5% decay per tick).
    pub decay_factor: f64,
}

impl Default for PrunerConfig {
    fn default() -> Self {
        Self {
            prune_below_confidence: 0.1,
            interval_secs: 86400,
            decay_factor: 0.95,
        }
    }
}

/// Spawn the mentor pruner as a background task.
/// Returns a `JoinHandle` that resolves when the task exits (via cancellation).
///
/// Validates config before starting. Invalid values are clamped to safe ranges:
/// - decay_factor: 0.01..=0.99
/// - interval_secs: minimum 60
/// - prune_below_confidence: 0.0..0.99
pub fn spawn(
    client: MentorClient,
    config: PrunerConfig,
    cancel: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    let config = validate_config(config);
    tokio::spawn(run_loop(client, config, cancel))
}

/// Clamp pruner config values to safe ranges to prevent destructive behavior.
fn validate_config(mut config: PrunerConfig) -> PrunerConfig {
    if config.decay_factor < 0.01 || config.decay_factor > 0.99 {
        warn!(
            original = config.decay_factor,
            clamped = config.decay_factor.clamp(0.01, 0.99),
            "mentor pruner: decay_factor out of range, clamping to 0.01..=0.99"
        );
        config.decay_factor = config.decay_factor.clamp(0.01, 0.99);
    }

    if config.interval_secs < 60 {
        warn!(
            original = config.interval_secs,
            clamped = 60,
            "mentor pruner: interval_secs too low, clamping to minimum 60s"
        );
        config.interval_secs = 60;
    }

    if config.prune_below_confidence < 0.0 || config.prune_below_confidence >= 1.0 {
        let clamped = config.prune_below_confidence.clamp(0.0, 0.99);
        warn!(
            original = config.prune_below_confidence,
            clamped,
            "mentor pruner: prune_below_confidence out of range, clamping to 0.0..0.99"
        );
        config.prune_below_confidence = clamped;
    }

    config
}

async fn run_loop(client: MentorClient, config: PrunerConfig, cancel: CancellationToken) {
    let mut tick = interval(Duration::from_secs(config.interval_secs));

    // The first tick fires immediately — skip it so we don't prune on startup.
    tick.tick().await;

    info!(
        interval_secs = config.interval_secs,
        threshold = config.prune_below_confidence,
        decay = config.decay_factor,
        "mentor pruner started"
    );

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!("mentor pruner shutting down");
                return;
            }
            _ = tick.tick() => {
                run_once(&client, &config).await;
            }
        }
    }
}

async fn run_once(client: &MentorClient, config: &PrunerConfig) {
    debug!("mentor pruner: running decay + prune cycle");

    // Step 1: Decay confidence for stale entries.
    match client.decay_confidence(config.decay_factor).await {
        Ok(decayed) => {
            if decayed > 0 {
                debug!(decayed, "mentor pruner: decayed confidence");
            }
        }
        Err(e) => {
            warn!("mentor pruner: decay failed: {e}");
            return;
        }
    }

    // Step 2: Prune entries below threshold.
    match client.prune_below_confidence(config.prune_below_confidence).await {
        Ok(pruned) => {
            if pruned > 0 {
                info!(pruned, threshold = config.prune_below_confidence, "mentor pruner: pruned stale entries");
            } else {
                debug!("mentor pruner: nothing to prune");
            }
        }
        Err(e) => {
            warn!("mentor pruner: prune failed: {e}");
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pruner_config_defaults() {
        let cfg = PrunerConfig::default();
        assert_eq!(cfg.interval_secs, 86400);
        assert!((cfg.prune_below_confidence - 0.1).abs() < f64::EPSILON);
        assert!((cfg.decay_factor - 0.95).abs() < f64::EPSILON);
    }

    #[test]
    fn validate_clamps_decay_factor() {
        let cfg = validate_config(PrunerConfig {
            decay_factor: 0.0,
            ..Default::default()
        });
        assert!((cfg.decay_factor - 0.01).abs() < f64::EPSILON);

        let cfg = validate_config(PrunerConfig {
            decay_factor: 1.0,
            ..Default::default()
        });
        assert!((cfg.decay_factor - 0.99).abs() < f64::EPSILON);

        // Valid value stays unchanged.
        let cfg = validate_config(PrunerConfig {
            decay_factor: 0.5,
            ..Default::default()
        });
        assert!((cfg.decay_factor - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn validate_clamps_interval_secs() {
        let cfg = validate_config(PrunerConfig {
            interval_secs: 0,
            ..Default::default()
        });
        assert_eq!(cfg.interval_secs, 60);

        let cfg = validate_config(PrunerConfig {
            interval_secs: 30,
            ..Default::default()
        });
        assert_eq!(cfg.interval_secs, 60);

        // Valid value stays unchanged.
        let cfg = validate_config(PrunerConfig {
            interval_secs: 3600,
            ..Default::default()
        });
        assert_eq!(cfg.interval_secs, 3600);
    }

    #[test]
    fn validate_clamps_prune_below() {
        let cfg = validate_config(PrunerConfig {
            prune_below_confidence: 1.0,
            ..Default::default()
        });
        assert!((cfg.prune_below_confidence - 0.99).abs() < f64::EPSILON);

        let cfg = validate_config(PrunerConfig {
            prune_below_confidence: -0.5,
            ..Default::default()
        });
        assert!(cfg.prune_below_confidence.abs() < f64::EPSILON);
    }
}
