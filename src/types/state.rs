// ---------------------------------------------------------------------------
// Application state — the single shared context passed to all handlers.
//
// Design: Arc-wrapped so it's cheaply cloneable across tasks and handlers.
// Every field is either immutable (config) or internally synchronized
// (pool, connections, event bus, in-flight reviews).
// ---------------------------------------------------------------------------

use crate::config::BottoConfig;
use crate::services::events::EventBus;
use arc_swap::ArcSwap;
use dashmap::DashMap;
use serde_json::Value;
use sqlx::SqlitePool;
use std::sync::Arc;
use tokio::sync::{broadcast, watch, Semaphore};

/// A connected Otto extension instance.
#[derive(Debug, Clone)]
pub struct Connection {
    pub id: String,
    pub user_id: Option<String>,
    pub authenticated: bool,
    pub viewing_mr: Option<MrRef>,
    pub tx: broadcast::Sender<String>,
}

/// Reference to a specific MR (used for presence + broadcast targeting).
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct MrRef {
    pub project_path: String,
    pub mr_iid: u64,
}

impl MrRef {
    pub fn key(&self) -> String {
        format!("{}:{}", self.project_path, self.mr_iid)
    }
}

/// Tracks an in-flight review so late-joiners can subscribe instead of
/// triggering a duplicate review.
#[derive(Clone)]
pub struct InFlightReview {
    /// Buffered chunks already emitted (for replay to late-joiners).
    pub replay_tx: watch::Sender<Vec<Value>>,
    /// Subscribe to live chunks as they arrive.
    pub live_tx: broadcast::Sender<Value>,
    /// True once the review has completed (STREAM_ALL_COMPLETE sent).
    pub completed: watch::Sender<bool>,
}

impl InFlightReview {
    pub fn new() -> Self {
        let (replay_tx, _) = watch::channel(Vec::new());
        let (live_tx, _) = broadcast::channel(512);
        let (completed, _) = watch::channel(false);
        Self {
            replay_tx,
            live_tx,
            completed,
        }
    }

    /// Record a chunk: broadcast live + append to replay buffer.
    pub fn emit(&self, chunk: Value) {
        let _ = self.live_tx.send(chunk.clone());
        self.replay_tx.send_modify(|buf| buf.push(chunk));
    }

    /// Mark the review as complete.
    pub fn finish(&self) {
        let _ = self.completed.send(true);
    }

    /// Get the current replay buffer (all chunks emitted so far).
    pub fn replay_buffer(&self) -> Vec<Value> {
        self.replay_tx.borrow().clone()
    }

    /// Subscribe to live chunks.
    pub fn subscribe_live(&self) -> broadcast::Receiver<Value> {
        self.live_tx.subscribe()
    }

    /// Check if the review is complete.
    pub fn is_complete(&self) -> bool {
        *self.completed.borrow()
    }
}

/// Shared application state. Cloned (Arc) into every handler.
#[derive(Clone)]
pub struct AppState {
    pub inner: Arc<AppStateInner>,
}

pub struct AppStateInner {
    pub config: ArcSwap<BottoConfig>,
    pub pool: SqlitePool,
    /// Active WebSocket connections, keyed by connection ID.
    pub connections: DashMap<String, Connection>,
    /// Event bus for cross-connection broadcasting.
    pub event_bus: EventBus,
    /// In-flight reviews, keyed by MrRef::key(). Prevents duplicate reviews
    /// and enables late-join replay.
    pub in_flight: DashMap<String, InFlightReview>,
    /// Limits how many MR reviews can run concurrently.
    pub review_semaphore: Arc<Semaphore>,
    /// Limits how many AI API calls can be in-flight across all reviews.
    pub ai_semaphore: Arc<Semaphore>,
}

impl AppState {
    pub fn new(config: BottoConfig, pool: SqlitePool) -> Self {
        let review_semaphore = Arc::new(Semaphore::new(config.server.max_concurrent_reviews));
        let ai_semaphore = Arc::new(Semaphore::new(config.server.max_concurrent_ai_calls));
        Self {
            inner: Arc::new(AppStateInner {
                config: ArcSwap::from_pointee(config),
                pool,
                connections: DashMap::new(),
                event_bus: EventBus::new(),
                in_flight: DashMap::new(),
                review_semaphore,
                ai_semaphore,
            }),
        }
    }

    /// Get the current config snapshot. Returns an Arc so it's safe to hold
    /// across await points — no guard lifetime issues.
    pub fn config(&self) -> Arc<BottoConfig> {
        self.inner.config.load_full()
    }

    /// Hot-swap the config. Takes effect immediately for all subsequent
    /// `config()` calls. In-flight operations that already loaded the old
    /// config will finish with the old values (which is correct).
    pub fn swap_config(&self, new_config: BottoConfig) {
        self.inner.config.store(Arc::new(new_config));
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.inner.pool
    }

    pub fn connections(&self) -> &DashMap<String, Connection> {
        &self.inner.connections
    }

    pub fn event_bus(&self) -> &EventBus {
        &self.inner.event_bus
    }

    pub fn in_flight(&self) -> &DashMap<String, InFlightReview> {
        &self.inner.in_flight
    }

    pub fn review_semaphore(&self) -> &Arc<Semaphore> {
        &self.inner.review_semaphore
    }

    pub fn ai_semaphore(&self) -> &Arc<Semaphore> {
        &self.inner.ai_semaphore
    }

    /// Get all connection IDs currently viewing a specific MR.
    pub fn viewers_of(&self, mr: &MrRef) -> Vec<String> {
        self.inner
            .connections
            .iter()
            .filter_map(|entry| {
                let conn = entry.value();
                if conn.authenticated {
                    if let Some(ref viewing) = conn.viewing_mr {
                        if viewing == mr {
                            return Some(conn.id.clone());
                        }
                    }
                }
                None
            })
            .collect()
    }

    /// Broadcast a JSON message to all authenticated connections viewing a specific MR.
    pub fn broadcast_to_mr(&self, mr: &MrRef, message: &str) {
        for entry in self.inner.connections.iter() {
            let conn = entry.value();
            if conn.authenticated {
                if let Some(ref viewing) = conn.viewing_mr {
                    if viewing == mr {
                        let _ = conn.tx.send(message.to_owned());
                    }
                }
            }
        }
    }

    /// Broadcast a JSON message to all authenticated connections except the sender.
    pub fn broadcast_to_mr_except(&self, mr: &MrRef, message: &str, except_id: &str) {
        for entry in self.inner.connections.iter() {
            let conn = entry.value();
            if conn.authenticated && conn.id != except_id {
                if let Some(ref viewing) = conn.viewing_mr {
                    if viewing == mr {
                        let _ = conn.tx.send(message.to_owned());
                    }
                }
            }
        }
    }
}
