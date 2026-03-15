// ---------------------------------------------------------------------------
// Queue manager — background review queue with serial execution.
//
// Ported from Otto's queue-manager.ts. Key differences:
//   - Server-side: handles reviews for ALL connected Ottos
//   - SQLite persistence instead of chrome.storage.local
//   - Broadcasts progress to all Ottos viewing the queued MR
//   - Concurrency of 1 (serial) — same as Otto
//
// Lifecycle:
//   1. Otto (or webhook) enqueues an MR for review
//   2. Queue manager picks the highest-priority item
//   3. Builds MrContext via GitLab API
//   4. Runs the review orchestrator
//   5. Broadcasts results to all viewers
//   6. Advances to next item
// ---------------------------------------------------------------------------

use crate::config::BottoConfig;
use crate::services::events::{Event, EventBus, EventType};
use crate::services::gitlab::client as gitlab;
use crate::services::review::orchestrator;
use crate::types::review::{DiffFileData, MrContext};
use crate::types::state::MrRef;
use serde_json::json;
use sqlx::SqlitePool;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex, Notify, Semaphore};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

/// A queued review item (in-memory representation).
#[derive(Debug, Clone)]
struct QueueItem {
    project_path: String,
    mr_iid: u64,
    priority_score: f64,
    status: ItemStatus,
}

#[derive(Debug, Clone, PartialEq)]
enum ItemStatus {
    Queued,
    Running,
    Paused,
}

/// The queue manager. Owned by AppState, runs as a background task.
pub struct QueueManager {
    cfg: BottoConfig,
    pool: SqlitePool,
    event_bus: EventBus,
    /// In-memory queue sorted by priority (highest first).
    items: Mutex<Vec<QueueItem>>,
    /// Currently running review's cancellation token.
    active_cancel: Mutex<Option<CancellationToken>>,
    /// Notify when a new item is enqueued (wakes the run loop).
    notify: Notify,
    /// Broadcast function for sending chunks to connected Ottos.
    broadcaster: Arc<dyn Fn(&MrRef, &str) + Send + Sync>,
    /// Shared AI call semaphore (same instance as stream_review uses).
    ai_semaphore: Arc<Semaphore>,
}

impl QueueManager {
    pub fn new(
        cfg: BottoConfig,
        pool: SqlitePool,
        event_bus: EventBus,
        broadcaster: Arc<dyn Fn(&MrRef, &str) + Send + Sync>,
        ai_semaphore: Arc<Semaphore>,
    ) -> Arc<Self> {
        Arc::new(Self {
            cfg,
            pool,
            event_bus,
            items: Mutex::new(Vec::new()),
            active_cancel: Mutex::new(None),
            notify: Notify::new(),
            broadcaster,
            ai_semaphore,
        })
    }

    /// Enqueue an MR for background review.
    pub async fn enqueue(
        &self,
        project_path: &str,
        mr_iid: u64,
        priority_score: f64,
    ) -> Result<(), String> {
        let mut items = self.items.lock().await;

        // Check for duplicate
        if items
            .iter()
            .any(|i| i.project_path == project_path && i.mr_iid == mr_iid)
        {
            return Err("already queued".into());
        }

        items.push(QueueItem {
            project_path: project_path.to_string(),
            mr_iid,
            priority_score,
            status: ItemStatus::Queued,
        });

        // Sort by priority descending
        items.sort_by(|a, b| b.priority_score.partial_cmp(&a.priority_score).unwrap());

        // Persist to DB (review_queue table)
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let mr_context_json = serde_json::to_vec(&json!({})).unwrap();
        let _ = sqlx::query(
            "INSERT INTO review_queue (project_path, mr_iid, priority_score, status, mr_context, enqueued_at)
             VALUES (?, ?, ?, 'queued', ?, ?)
             ON CONFLICT(project_path, mr_iid) DO UPDATE SET
               priority_score = excluded.priority_score,
               status = 'queued',
               enqueued_at = excluded.enqueued_at",
        )
        .bind(project_path)
        .bind(mr_iid as i64)
        .bind(priority_score)
        .bind(&mr_context_json)
        .bind(now)
        .execute(&self.pool)
        .await;

        drop(items);

        info!("enqueued review: {}:!{} (priority={:.0})", project_path, mr_iid, priority_score);

        // Wake the run loop
        self.notify.notify_one();

        self.event_bus.publish(Event {
            event_type: EventType::ReviewStarted,
            project_path: project_path.to_string(),
            mr_iid: Some(mr_iid),
            user_id: None,
            payload: Some(json!({ "source": "queue", "priority": priority_score })),
        });

        Ok(())
    }

    /// Cancel a queued or running review.
    pub async fn cancel(&self, project_path: &str, mr_iid: u64) {
        let mut items = self.items.lock().await;

        // If it's the active review, cancel it
        if let Some(_item) = items.iter().find(|i| {
            i.project_path == project_path && i.mr_iid == mr_iid && i.status == ItemStatus::Running
        }) {
            if let Some(cancel) = self.active_cancel.lock().await.take() {
                cancel.cancel();
            }
        }

        // Remove from queue
        items.retain(|i| !(i.project_path == project_path && i.mr_iid == mr_iid));
    }

    /// Get current queue status.
    pub async fn status(&self) -> serde_json::Value {
        let items = self.items.lock().await;
        let queue: Vec<serde_json::Value> = items
            .iter()
            .map(|i| {
                json!({
                    "project_path": i.project_path,
                    "mr_iid": i.mr_iid,
                    "priority_score": i.priority_score,
                    "status": format!("{:?}", i.status).to_lowercase(),
                })
            })
            .collect();

        json!({
            "items": queue,
            "active": items.iter().find(|i| i.status == ItemStatus::Running).map(|i| {
                json!({ "project_path": i.project_path, "mr_iid": i.mr_iid })
            }),
        })
    }

    /// Main run loop — processes queued items serially.
    /// Call this once at startup; it runs forever.
    pub async fn run(self: Arc<Self>, shutdown: CancellationToken) {
        info!("queue manager started");

        loop {
            // Wait for work or shutdown
            tokio::select! {
                _ = shutdown.cancelled() => {
                    info!("queue manager shutting down");
                    break;
                }
                _ = self.notify.notified() => {}
            }

            // Process all queued items
            loop {
                if shutdown.is_cancelled() {
                    break;
                }

                // Pick the next queued item
                let next = {
                    let mut items = self.items.lock().await;
                    items
                        .iter_mut()
                        .find(|i| i.status == ItemStatus::Queued)
                        .map(|i| {
                            i.status = ItemStatus::Running;
                            (i.project_path.clone(), i.mr_iid)
                        })
                };

                let (project_path, mr_iid) = match next {
                    Some(item) => item,
                    None => break, // queue empty
                };

                info!("queue: starting review for {}:!{}", project_path, mr_iid);

                // Build MrContext from GitLab API
                let mr_context = self.build_mr_context(&project_path, mr_iid).await;

                match mr_context {
                    Some(ctx) => {
                        let cancel = CancellationToken::new();
                        *self.active_cancel.lock().await = Some(cancel.clone());

                        // Create chunk channel — we drain it to avoid blocking the
                        // orchestrator, but don't broadcast individual chunks.
                        // Queue reviews run server-side with no active stream listener.
                        // Results are cached by the orchestrator; we broadcast a
                        // CACHED_REVIEW notification when complete.
                        let (chunk_tx, mut chunk_rx) = mpsc::channel::<serde_json::Value>(128);

                        // Drain chunks (orchestrator blocks if channel is full)
                        let drainer = tokio::spawn(async move {
                            while chunk_rx.recv().await.is_some() {}
                        });

                        // Run the review
                        let tasks = orchestrator::all_tasks();
                        let result = orchestrator::execute_review(
                            &self.cfg,
                            &self.pool,
                            &ctx,
                            &tasks,
                            chunk_tx,
                            cancel,
                            false, // queue reviews always use cache
                            Some(self.ai_semaphore.clone()),
                        )
                        .await;

                        let _ = drainer.await;

                        // Notify all viewers that a cached review is available
                        if result.is_some() {
                            let mr_ref = MrRef {
                                project_path: project_path.clone(),
                                mr_iid,
                            };
                            let notification = json!({
                                "type": "EVENT_NOTIFICATION",
                                "event_type": "queued_review_complete",
                                "project_path": project_path,
                                "mr_iid": mr_iid,
                                "payload": { "source": "queue" }
                            });
                            (self.broadcaster)(&mr_ref, &notification.to_string());
                        }

                        self.event_bus.publish(Event {
                            event_type: EventType::ReviewComplete,
                            project_path: project_path.clone(),
                            mr_iid: Some(mr_iid),
                            user_id: None,
                            payload: Some(json!({ "source": "queue" })),
                        });
                    }
                    None => {
                        warn!(
                            "queue: failed to build context for {}:!{}",
                            project_path, mr_iid
                        );
                    }
                }

                // Remove completed item
                {
                    let mut items = self.items.lock().await;
                    items.retain(|i| {
                        !(i.project_path == project_path && i.mr_iid == mr_iid)
                    });
                }
                *self.active_cancel.lock().await = None;
            }
        }
    }

    /// Build an MrContext by fetching data from GitLab.
    async fn build_mr_context(&self, project_path: &str, mr_iid: u64) -> Option<MrContext> {
        let gl_cfg = gitlab::GitLabConfig {
            base_url: self.cfg.gitlab.url.clone(),
            token: self.cfg.gitlab.bot_token.clone(),
        };

        // Fetch project to get numeric ID
        let project = gitlab::fetch_project(&gl_cfg, project_path).await.ok()?;

        // Fetch MR changes
        let changes = gitlab::fetch_mr_changes(&gl_cfg, project.id, mr_iid).await.ok()?;

        let diff_files: Vec<DiffFileData> = changes
            .changes
            .into_iter()
            .map(|c| {
                let added = c.diff.lines().filter(|l| l.starts_with('+')).count() as u32;
                let removed = c.diff.lines().filter(|l| l.starts_with('-')).count() as u32;
                DiffFileData {
                    file_path: c.new_path.clone(),
                    old_path: if c.renamed_file {
                        Some(c.old_path)
                    } else {
                        None
                    },
                    is_new: c.new_file,
                    is_deleted: c.deleted_file,
                    is_renamed: c.renamed_file,
                    diff: c.diff,
                    added_lines: added,
                    removed_lines: removed,
                }
            })
            .collect();

        Some(MrContext {
            project_path: project_path.to_string(),
            project_id: Some(project.id),
            mr_iid,
            host_url: self.cfg.gitlab.url.clone(),
            title: changes.title,
            description: changes.description,
            source_branch: changes.source_branch,
            target_branch: changes.target_branch,
            author_username: None,
            diff_files,
        })
    }
}
