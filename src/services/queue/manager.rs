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

use crate::services::events::{Event, EventType};
use crate::services::gitlab::client as gitlab;
use crate::services::review::orchestrator;
use crate::types::review::{DiffFileData, MrContext};
use crate::types::state::{AppState, InFlightReview, MrRef};
use serde_json::json;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex, Notify};
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
    /// Shared application state — gives access to config (hot-swap safe),
    /// in-flight review map (dedup with interactive reviews), DB pool,
    /// event bus, broadcaster, and AI semaphore.
    state: AppState,
    /// In-memory queue sorted by priority (highest first).
    items: Mutex<Vec<QueueItem>>,
    /// Currently running review's cancellation token.
    active_cancel: Mutex<Option<CancellationToken>>,
    /// Notify when a new item is enqueued (wakes the run loop).
    notify: Notify,
}

impl QueueManager {
    pub fn new(state: AppState) -> Arc<Self> {
        Arc::new(Self {
            state,
            items: Mutex::new(Vec::new()),
            active_cancel: Mutex::new(None),
            notify: Notify::new(),
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

        // Also skip if an interactive review is already in-flight for this MR
        let mr_key = format!("{}:{}", project_path, mr_iid);
        if self.state.in_flight().contains_key(&mr_key) {
            return Err("review already in-flight".into());
        }

        items.push(QueueItem {
            project_path: project_path.to_string(),
            mr_iid,
            priority_score,
            status: ItemStatus::Queued,
        });

        // Sort by priority descending
        items.sort_by(|a, b| b.priority_score.partial_cmp(&a.priority_score).unwrap_or(std::cmp::Ordering::Equal));

        // Persist to DB (review_queue table)
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        let mr_context_json = serde_json::to_vec(&json!({})).unwrap_or_default();
        let pool = self.state.pool();
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
        .execute(pool)
        .await;

        drop(items);

        info!("enqueued review: {}:!{} (priority={:.0})", project_path, mr_iid, priority_score);

        // Wake the run loop
        self.notify.notify_one();

        self.state.event_bus().publish(Event {
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

                let mr_key = format!("{}:{}", project_path, mr_iid);

                // Check if an interactive review is already in-flight (race
                // between enqueue check and now). If so, skip — the interactive
                // review will cache the result.
                if self.state.in_flight().contains_key(&mr_key) {
                    info!("queue: skipping {}:!{} — interactive review already in-flight", project_path, mr_iid);
                    let mut items = self.items.lock().await;
                    items.retain(|i| !(i.project_path == project_path && i.mr_iid == mr_iid));
                    continue;
                }

                // Register in-flight so interactive stream_review requests
                // late-join instead of starting a duplicate review.
                let in_flight = InFlightReview::new();
                self.state.in_flight().insert(mr_key.clone(), in_flight.clone());

                // Read config fresh (hot-swap safe)
                let cfg = self.state.config();

                // Build MrContext from GitLab API (retry once on failure)
                let mr_context = match self.build_mr_context(&cfg, &project_path, mr_iid).await {
                    Some(ctx) => Some(ctx),
                    None => {
                        warn!("queue: first attempt to build context failed for {}:!{}, retrying...", project_path, mr_iid);
                        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                        self.build_mr_context(&cfg, &project_path, mr_iid).await
                    }
                };

                match mr_context {
                    Some(ctx) => {
                        let cancel = CancellationToken::new();
                        *self.active_cancel.lock().await = Some(cancel.clone());

                        // Create chunk channel. Forward chunks through the
                        // InFlightReview so late-joiners (humans opening the MR
                        // while the queued review runs) get replay + live stream.
                        let (chunk_tx, mut chunk_rx) = mpsc::channel::<serde_json::Value>(128);

                        let in_flight_for_fwd = in_flight.clone();
                        let forwarder = tokio::spawn(async move {
                            while let Some(chunk) = chunk_rx.recv().await {
                                in_flight_for_fwd.emit(chunk);
                            }
                        });

                        // Run the review
                        let tasks = orchestrator::all_tasks();
                        let result = orchestrator::execute_review(
                            &cfg,
                            self.state.pool(),
                            &ctx,
                            &tasks,
                            chunk_tx,
                            cancel,
                            false, // queue reviews always use cache
                            Some(self.state.ai_semaphore().clone()),
                        )
                        .await;

                        let _ = forwarder.await;

                        // Mark in-flight as complete so late-joiners know
                        // the review is done and can read from cache.
                        in_flight.finish();

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
                            self.state.broadcast_to_mr(&mr_ref, &notification.to_string());
                        }

                        self.state.event_bus().publish(Event {
                            event_type: EventType::ReviewComplete,
                            project_path: project_path.clone(),
                            mr_iid: Some(mr_iid),
                            user_id: None,
                            payload: Some(json!({ "source": "queue" })),
                        });
                    }
                    None => {
                        warn!(
                            "queue: failed to build context for {}:!{} after retry",
                            project_path, mr_iid
                        );
                        // Mark as error in DB so it's visible in queue status
                        let _ = crate::db::queries::update_queue_status(
                            self.state.pool(),
                            &project_path,
                            mr_iid as i64,
                            &["running"],
                            "error",
                        ).await;
                    }
                }

                // Clean up: remove in-flight entry and queue item
                self.state.in_flight().remove(&mr_key);
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
    async fn build_mr_context(&self, cfg: &crate::config::BottoConfig, project_path: &str, mr_iid: u64) -> Option<MrContext> {
        let gl_cfg = gitlab::GitLabConfig {
            base_url: cfg.gitlab.url.clone(),
            token: cfg.gitlab.bot_token.clone(),
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
            host_url: cfg.gitlab.url.clone(),
            title: changes.title,
            description: changes.description,
            source_branch: changes.source_branch,
            target_branch: changes.target_branch,
            author_username: None,
            diff_files,
        })
    }
}
