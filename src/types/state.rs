// ---------------------------------------------------------------------------
// Application state — the single shared context passed to all handlers.
//
// Design: Arc-wrapped so it's cheaply cloneable across tasks and handlers.
// Every field is either immutable (config) or internally synchronized
// (pool, connections, event bus, in-flight reviews).
// ---------------------------------------------------------------------------

use crate::config::BottoConfig;
use crate::services::channels::bus::MessageBus;
use crate::services::events::EventBus;
use crate::services::queue::manager::QueueManager;
use crate::services::sandbox::manager::WarmPool;
use arc_swap::ArcSwap;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::SqlitePool;
use std::collections::HashSet;
use std::sync::{Arc, OnceLock};
use tokio::sync::{broadcast, watch, Semaphore};
use tracing::info;

/// A file (and optional line range) a user is currently viewing in a diff.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewingFile {
    pub path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_line: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_line: Option<u32>,
}

/// A connected Otto extension instance.
#[derive(Debug, Clone)]
pub struct Connection {
    pub id: String,
    pub user_id: Option<String>,
    /// Full display name from GitLab (e.g. "Gavin Smith"). Resolved on AUTH.
    pub display_name: Option<String>,
    /// GitLab avatar URL. Resolved on AUTH.
    pub avatar_url: Option<String>,
    pub authenticated: bool,
    pub viewing_mr: Option<MrRef>,
    /// Files currently visible in the user's viewport (updated via VIEWING_FILES).
    pub viewing_files: Vec<ViewingFile>,
    /// Last time viewing_files was updated — used for server-side rate limiting.
    pub files_updated_at: Option<tokio::time::Instant>,
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
    /// Secondary index: MrRef::key() → set of connection IDs viewing that MR.
    /// Maintained alongside `connections` on VIEWING_MR / LEFT_MR / disconnect.
    /// Makes broadcast_to_mr O(k) where k = viewers of that MR, instead of
    /// O(n) over all connections.
    pub mr_viewers: DashMap<String, HashSet<String>>,
    /// Event bus for cross-connection broadcasting.
    pub event_bus: EventBus,
    /// In-flight reviews, keyed by MrRef::key(). Prevents duplicate reviews
    /// and enables late-join replay.
    pub in_flight: DashMap<String, InFlightReview>,
    /// Limits how many MR reviews can run concurrently.
    pub review_semaphore: Arc<Semaphore>,
    /// Limits how many AI API calls can be in-flight across all reviews.
    pub ai_semaphore: Arc<Semaphore>,
    /// Warm container pool for sandbox fix reuse across fixes on the same MR.
    /// None if Docker is not available or warm containers are disabled.
    pub warm_pool: Option<Arc<WarmPool>>,
    /// Background review queue manager. Set once after construction via
    /// `set_queue_manager()` — OnceLock because QueueManager needs a
    /// broadcaster closure that captures AppState (chicken-and-egg).
    pub queue_manager: OnceLock<Arc<QueueManager>>,
    /// Lightweight cache: project_path → project_id. Avoids redundant GitLab
    /// API calls when resolving project IDs for conflict/cluster features.
    /// This mapping is stable (project paths don't change IDs), so no TTL needed.
    pub project_id_cache: DashMap<String, i64>,
    /// Per-MR mutex for webhook background tasks. Prevents concurrent file index
    /// writes for the same MR when rapid pushes trigger multiple webhooks.
    /// Keyed by "project_id:mr_iid". Entries are lightweight (Arc<Mutex<()>>)
    /// and bounded by the number of concurrently-active MR webhooks.
    pub mr_webhook_locks: DashMap<String, Arc<tokio::sync::Mutex<()>>>,
    /// Shared semaphore for workflow run concurrency (cron, event, and API triggers).
    /// Initialized from `config.workflows.max_concurrent_runs`.
    pub workflow_semaphore: Arc<Semaphore>,
    /// Channel adapter message bus for inbound/outbound message routing.
    /// None if channels are disabled.
    pub message_bus: Option<MessageBus>,
}

impl AppState {
    pub fn new(config: BottoConfig, pool: SqlitePool) -> Self {
        let review_semaphore = Arc::new(Semaphore::new(config.server.max_concurrent_reviews));
        let ai_semaphore = Arc::new(Semaphore::new(config.server.max_concurrent_ai_calls));
        let workflow_semaphore = Arc::new(Semaphore::new(config.workflows.max_concurrent_runs));

        // Initialize warm pool if sandbox + warm containers are both enabled
        let warm_pool = if config.sandbox.enabled && config.sandbox.warm_containers {
            WarmPool::new().map(|p| {
                info!(
                    "warm container pool enabled (idle={}s, max_lifetime={}s)",
                    config.sandbox.warm_idle_timeout_secs, config.sandbox.warm_max_lifetime_secs,
                );
                Arc::new(p)
            })
        } else {
            None
        };

        // Initialize message bus if channels are enabled
        let message_bus = if config.channels.enabled {
            Some(MessageBus::new())
        } else {
            None
        };

        Self {
            inner: Arc::new(AppStateInner {
                config: ArcSwap::from_pointee(config),
                pool,
                connections: DashMap::new(),
                mr_viewers: DashMap::new(),
                event_bus: EventBus::new(),
                in_flight: DashMap::new(),
                review_semaphore,
                ai_semaphore,
                warm_pool,
                queue_manager: OnceLock::new(),
                project_id_cache: DashMap::new(),
                mr_webhook_locks: DashMap::new(),
                workflow_semaphore,
                message_bus,
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

    pub fn warm_pool(&self) -> Option<&Arc<WarmPool>> {
        self.inner.warm_pool.as_ref()
    }

    pub fn workflow_semaphore(&self) -> &Arc<Semaphore> {
        &self.inner.workflow_semaphore
    }

    /// Get the channel adapter message bus, if channels are enabled.
    pub fn message_bus(&self) -> Option<&MessageBus> {
        self.inner.message_bus.as_ref()
    }

    /// Store the queue manager after construction. Called once from main.rs
    /// after both AppState and QueueManager are created.
    /// Panics if called more than once (programming error).
    pub fn set_queue_manager(&self, qm: Arc<QueueManager>) {
        if self.inner.queue_manager.set(qm).is_err() {
            panic!("queue_manager already set — set_queue_manager called twice");
        }
    }

    /// Get the queue manager, if set. Returns None only during the brief
    /// window between AppState creation and set_queue_manager() in main.rs.
    pub fn queue_manager(&self) -> Option<&Arc<QueueManager>> {
        self.inner.queue_manager.get()
    }

    /// Resolve a project_path to a project_id, using the in-memory cache.
    /// Falls back to a GitLab API call on cache miss. The mapping is stable
    /// (project paths don't change IDs), so entries never expire.
    pub async fn resolve_project_id(
        &self,
        project_path: &str,
    ) -> Option<i64> {
        // Check cache first
        if let Some(id) = self.inner.project_id_cache.get(project_path) {
            return Some(*id);
        }

        // Cache miss — fetch from GitLab
        let cfg = self.config();
        let gl_cfg = crate::services::gitlab::client::GitLabConfig {
            base_url: cfg.gitlab.url.clone(),
            token: cfg.gitlab.bot_token.clone(),
        };

        match crate::services::gitlab::client::fetch_project(&gl_cfg, project_path).await {
            Ok(project) => {
                self.inner.project_id_cache.insert(project_path.to_string(), project.id);
                Some(project.id)
            }
            Err(e) => {
                tracing::warn!("failed to resolve project_id for {}: {}", project_path, e);
                None
            }
        }
    }

    /// Get all connection IDs currently viewing a specific MR.
    /// O(k) where k = viewers of that MR (uses secondary index).
    pub fn viewers_of(&self, mr: &MrRef) -> Vec<String> {
        self.inner
            .mr_viewers
            .get(&mr.key())
            .map(|set| set.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Broadcast a JSON message to all authenticated connections viewing a specific MR.
    /// O(k) where k = viewers of that MR.
    pub fn broadcast_to_mr(&self, mr: &MrRef, message: &str) {
        if let Some(viewer_ids) = self.inner.mr_viewers.get(&mr.key()) {
            for conn_id in viewer_ids.iter() {
                if let Some(conn) = self.inner.connections.get(conn_id) {
                    if conn.authenticated {
                        let _ = conn.tx.send(message.to_owned());
                    }
                }
            }
        }
    }

    /// Broadcast a JSON message to all authenticated connections except the sender.
    /// O(k) where k = viewers of that MR.
    pub fn broadcast_to_mr_except(&self, mr: &MrRef, message: &str, except_id: &str) {
        if let Some(viewer_ids) = self.inner.mr_viewers.get(&mr.key()) {
            for conn_id in viewer_ids.iter() {
                if conn_id != except_id {
                    if let Some(conn) = self.inner.connections.get(conn_id) {
                        if conn.authenticated {
                            let _ = conn.tx.send(message.to_owned());
                        }
                    }
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Secondary index maintenance — call these whenever viewing_mr changes.
    // -----------------------------------------------------------------------

    /// Track a connection as viewing an MR. Removes from old MR if switching.
    /// Returns the old MrRef if the connection was previously viewing a different MR.
    pub fn track_viewer(&self, conn_id: &str, new_mr: &MrRef) -> Option<MrRef> {
        let old_mr = if let Some(mut conn) = self.inner.connections.get_mut(conn_id) {
            let old = conn.viewing_mr.take();
            conn.viewing_mr = Some(new_mr.clone());
            // Clear file-level presence when switching MRs
            conn.viewing_files.clear();
            conn.files_updated_at = None;
            old
        } else {
            return None;
        };

        // Remove from old MR's viewer set
        if let Some(ref old) = old_mr {
            if let Some(mut set) = self.inner.mr_viewers.get_mut(&old.key()) {
                set.remove(conn_id);
                if set.is_empty() {
                    drop(set);
                    self.inner.mr_viewers.remove(&old.key());
                }
            }
        }

        // Add to new MR's viewer set
        self.inner
            .mr_viewers
            .entry(new_mr.key())
            .or_insert_with(HashSet::new)
            .insert(conn_id.to_string());

        old_mr
    }

    /// Remove a connection from its current MR viewer set.
    /// Returns the MrRef it was viewing, if any.
    pub fn untrack_viewer(&self, conn_id: &str) -> Option<MrRef> {
        let old_mr = if let Some(mut conn) = self.inner.connections.get_mut(conn_id) {
            let old = conn.viewing_mr.take();
            conn.viewing_files.clear();
            conn.files_updated_at = None;
            old
        } else {
            return None;
        };

        if let Some(ref mr) = old_mr {
            if let Some(mut set) = self.inner.mr_viewers.get_mut(&mr.key()) {
                set.remove(conn_id);
                if set.is_empty() {
                    drop(set);
                    self.inner.mr_viewers.remove(&mr.key());
                }
            }
        }

        old_mr
    }

    /// Remove a connection entirely (on disconnect). Cleans up the viewer index.
    pub fn remove_connection(&self, conn_id: &str) -> Option<Connection> {
        let removed = self.inner.connections.remove(conn_id);
        if let Some((_, ref conn)) = removed {
            if let Some(ref mr) = conn.viewing_mr {
                if let Some(mut set) = self.inner.mr_viewers.get_mut(&mr.key()) {
                    set.remove(conn_id);
                    if set.is_empty() {
                        drop(set);
                        self.inner.mr_viewers.remove(&mr.key());
                    }
                }
            }
        }
        removed.map(|(_, c)| c)
    }

    /// Get the current file-level presence for all viewers of an MR (excluding one connection).
    /// Used to build PRESENCE_SNAPSHOT on join and PRESENCE_UPDATE on file changes.
    pub fn get_mr_presence(&self, mr: &MrRef, except_id: Option<&str>) -> Vec<Value> {
        let mut result = Vec::new();
        if let Some(viewer_ids) = self.inner.mr_viewers.get(&mr.key()) {
            for conn_id in viewer_ids.iter() {
                if except_id == Some(conn_id.as_str()) {
                    continue;
                }
                if let Some(conn) = self.inner.connections.get(conn_id) {
                    if conn.authenticated && !conn.viewing_files.is_empty() {
                        result.push(serde_json::json!({
                            "user_id": conn.user_id,
                            "display_name": conn.display_name,
                            "avatar_url": conn.avatar_url,
                            "files": conn.viewing_files,
                        }));
                    }
                }
            }
        }
        result
    }
}
