// ---------------------------------------------------------------------------
// GitLab webhook receiver — handles MR, push, and note events.
//
// Validates the webhook secret token, parses the event, and dispatches
// to the event bus for cache invalidation and notification broadcasting.
//
// Push events can optionally trigger auto-review for open MRs on the
// pushed branch (when review.auto_review_on_push is enabled).
// ---------------------------------------------------------------------------

use crate::services::gitlab::client as gitlab;
use crate::services::queue::priority;
use crate::types::state::{AppState, MrRef};
use axum::body::Bytes;
use axum::extract::State;
use std::sync::Arc;
use axum::http::{HeaderMap, StatusCode};
use tracing::{debug, info, warn};

pub async fn gitlab_webhook(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> StatusCode {
    // Validate webhook secret if configured
    if let Some(ref expected_secret) = state.config().gitlab.webhook_secret {
        let token = headers
            .get("X-Gitlab-Token")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        if token != expected_secret {
            warn!("webhook rejected: invalid secret token");
            return StatusCode::UNAUTHORIZED;
        }
    }

    let event_type = headers
        .get("X-Gitlab-Event")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("unknown");

    let payload: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            warn!("webhook rejected: invalid JSON: {}", e);
            return StatusCode::BAD_REQUEST;
        }
    };

    info!("webhook received: {}", event_type);

    match event_type {
        "Merge Request Hook" => handle_mr_event(&state, &payload).await,
        "Push Hook" => handle_push_event(&state, &payload).await,
        "Note Hook" => handle_note_event(&state, &payload).await,
        _ => {
            // Ignore events we don't care about
        }
    }

    StatusCode::OK
}

async fn handle_mr_event(state: &AppState, payload: &serde_json::Value) {
    let project_path = payload["project"]["path_with_namespace"]
        .as_str()
        .unwrap_or("");
    let mr_iid = payload["object_attributes"]["iid"].as_u64();
    let action = payload["object_attributes"]["action"]
        .as_str()
        .unwrap_or("");
    let project_id = payload["project"]["id"].as_i64();

    if project_path.is_empty() || mr_iid.is_none() {
        return;
    }

    let mr_iid = mr_iid.unwrap();
    info!(
        "MR event: {} !{} action={}",
        project_path, mr_iid, action
    );

    // Evict warm container on merge or close — the MR is done.
    match action {
        "merge" | "close" => {
            if let Some(pool) = state.warm_pool() {
                let mr_key = format!("{}:{}", project_path, mr_iid);
                if pool.remove(&mr_key) {
                    info!("warm pool: evicted container for {} (MR {})", mr_key, action);
                }
            }
        }
        _ => {}
    }

    // Publish event for cache invalidation and notification
    state.event_bus().publish(crate::services::events::Event {
        event_type: crate::services::events::EventType::MrUpdated,
        project_path: project_path.to_string(),
        mr_iid: Some(mr_iid),
        user_id: None,
        payload: Some(serde_json::json!({ "action": action })),
    });

    // --- File index + Conflict Radar + Cluster detection ---
    // Spawned as a background task so the webhook returns 200 immediately.
    // A per-MR mutex prevents concurrent tasks from interleaving file index
    // writes when rapid pushes trigger multiple webhooks for the same MR.
    if let Some(project_id) = project_id {
        let state = state.clone();
        let project_path = project_path.to_string();
        let action = action.to_string();

        tokio::spawn(async move {
            let lock_key = format!("{}:{}", project_id, mr_iid);
            let lock = state
                .inner
                .mr_webhook_locks
                .entry(lock_key)
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
                .clone();
            let _guard = lock.lock().await;

            if let Err(e) = update_file_index_and_detect(
                &state,
                &project_path,
                project_id,
                mr_iid,
                &action,
            )
            .await
            {
                warn!(
                    "file index/conflict/cluster update failed for {} !{}: {}",
                    project_path, mr_iid, e
                );
            }
        });
    }
}

async fn handle_push_event(state: &AppState, payload: &serde_json::Value) {
    let project_path = payload["project"]["path_with_namespace"]
        .as_str()
        .unwrap_or("");
    let branch = payload["ref"]
        .as_str()
        .unwrap_or("")
        .strip_prefix("refs/heads/")
        .unwrap_or("");

    if project_path.is_empty() || branch.is_empty() {
        return;
    }

    let after_sha = payload["after"].as_str().unwrap_or("");

    info!("push event: {} branch={}", project_path, branch);

    // Check if .otto.json was added, modified, or removed in this push.
    // Invalidate the cached repo config so the next review/fix re-fetches it.
    // We invalidate on ANY branch push that touches .otto.json — the cache is
    // project-level, and the next get_or_fetch() will re-fetch from whatever
    // branch the caller needs.
    if let Some(commits) = payload["commits"].as_array() {
        let touches_otto_json = commits.iter().any(|c| {
            ["added", "modified", "removed"].iter().any(|field| {
                c[field]
                    .as_array()
                    .map(|files| files.iter().any(|f| f.as_str() == Some(".otto.json")))
                    .unwrap_or(false)
            })
        });
        if touches_otto_json {
            info!("push event: .otto.json changed in {}, invalidating cached config", project_path);
            crate::services::repo_config::invalidate(state.pool(), project_path).await;
        }
    }

    // Warm container eviction on push.
    // If Botto pushed this commit (bot push), keep the container warm.
    // If the author pushed, the checkout is stale — evict.
    if let Some(pool) = state.warm_pool() {
        let mr_keys = pool.find_by_branch(project_path, branch);
        for mr_key in mr_keys {
            if !after_sha.is_empty() && pool.is_bot_push(&mr_key, after_sha) {
                info!("warm pool: keeping {} (Botto's own push {})", mr_key, &after_sha[..8.min(after_sha.len())]);
            } else {
                if pool.remove(&mr_key) {
                    info!("warm pool: evicted {} (author push to {})", mr_key, branch);
                }
            }
        }
    }

    // Invalidate any cached reviews for MRs with this source branch.
    // The actual cache invalidation happens in the event handler that
    // subscribes to MrUpdated events.
    state.event_bus().publish(crate::services::events::Event {
        event_type: crate::services::events::EventType::MrUpdated,
        project_path: project_path.to_string(),
        mr_iid: None,
        user_id: None,
        payload: Some(serde_json::json!({ "branch": branch, "trigger": "push" })),
    });

    // --- File index update on push ---
    // When a push lands on a branch with open MRs, the diff has changed.
    // Re-populate the file index and run conflict/cluster detection so
    // viewers get updated results without waiting for a separate MR webhook.
    // (GitLab sends Push Hook and Merge Request Hook independently — the MR
    // hook may arrive later or not at all if only Push events are configured.)
    {
        let cfg = state.config();
        if cfg.conflict.enabled || cfg.cluster.enabled {
            if let Some(project_id) = payload["project"]["id"].as_i64() {
                let state = state.clone();
                let project_path = project_path.to_string();
                let branch = branch.to_string();

                tokio::spawn(async move {
                    let cfg = state.config();
                    let gl_cfg = gitlab::GitLabConfig {
                        base_url: cfg.gitlab.url.clone(),
                        token: cfg.gitlab.bot_token.clone(),
                    };

                    // Find open MRs sourced from this branch
                    let open_mrs = match gitlab::fetch_open_mrs_for_branch(
                        &gl_cfg, project_id, &branch,
                    ).await {
                        Ok(mrs) => mrs,
                        Err(e) => {
                            warn!("push file index: failed to fetch open MRs for branch {}: {}", branch, e);
                            return;
                        }
                    };

                    for mr in &open_mrs {
                        // Force re-populate — the push means the diff changed.
                        if let Err(e) = crate::services::file_index::populate(
                            state.pool(), &gl_cfg, project_id, mr.iid,
                        ).await {
                            warn!("push file index: populate failed for !{}: {}", mr.iid, e);
                            continue;
                        }

                        let mr_ref = MrRef {
                            project_path: project_path.clone(),
                            mr_iid: mr.iid,
                        };

                        // Run conflict detection and broadcast
                        if cfg.conflict.enabled {
                            if let Ok(report) = crate::services::conflict::detector::detect_conflicts(
                                state.pool(), &gl_cfg, project_id, mr.iid,
                            ).await {
                                if !report.conflicts.is_empty() {
                                    let msg = crate::api::ws::WsOutbound::ConflictUpdated {
                                        project_id,
                                        mr_iid: mr.iid,
                                        conflicts: serde_json::to_value(&report).unwrap_or_default(),
                                    };
                                    state.broadcast_to_mr(&mr_ref, &serde_json::to_string(&msg).unwrap_or_default());

                                    // Also notify viewers of conflicting MRs
                                    let conflicting_iids: std::collections::HashSet<u64> = report
                                        .conflicts
                                        .iter()
                                        .flat_map(|fc| fc.conflicting_mrs.iter().map(|cm| cm.mr_iid))
                                        .collect();

                                    for other_iid in conflicting_iids {
                                        if let Ok(other_report) = crate::services::conflict::detector::detect_conflicts(
                                            state.pool(), &gl_cfg, project_id, other_iid,
                                        ).await {
                                            let other_ref = MrRef {
                                                project_path: project_path.clone(),
                                                mr_iid: other_iid,
                                            };
                                            let other_msg = crate::api::ws::WsOutbound::ConflictUpdated {
                                                project_id,
                                                mr_iid: other_iid,
                                                conflicts: serde_json::to_value(&other_report).unwrap_or_default(),
                                            };
                                            state.broadcast_to_mr(&other_ref, &serde_json::to_string(&other_msg).unwrap_or_default());
                                        }
                                    }
                                }
                            }
                        }

                        // Run cluster detection and broadcast
                        if cfg.cluster.enabled {
                            let ticket_strategy =
                                crate::services::cluster::strategies::ticket::TicketClusterStrategy;
                            let file_strategy =
                                crate::services::cluster::strategies::file_overlap::FileOverlapStrategy {
                                    jaccard_threshold: cfg.cluster.file_overlap_threshold,
                                    max_cluster_size: cfg.cluster.max_cluster_size,
                                };
                            let strategies: Vec<&dyn crate::services::cluster::strategies::ClusterStrategy> =
                                vec![&ticket_strategy, &file_strategy];

                            if let Ok(clusters) = crate::services::cluster::detector::detect_clusters(
                                state.pool(), &gl_cfg, project_id, mr.iid, &strategies,
                            ).await {
                                for cluster in &clusters {
                                    for member in &cluster.member_mrs {
                                        let member_ref = MrRef {
                                            project_path: project_path.clone(),
                                            mr_iid: member.mr_iid,
                                        };
                                        let msg = crate::api::ws::WsOutbound::ClusterUpdated {
                                            project_id,
                                            cluster: serde_json::to_value(cluster).unwrap_or_default(),
                                        };
                                        state.broadcast_to_mr(&member_ref, &serde_json::to_string(&msg).unwrap_or_default());
                                    }
                                }
                            }
                        }
                    }
                });
            }
        }
    }

    // --- Auto-review on push ---
    // If enabled, find open MRs for this branch and enqueue them for review.
    // Spawned as a background task so the webhook returns 200 immediately
    // (GitLab has a 10s webhook timeout).
    if state.config().review.auto_review_on_push {
        let state = state.clone();
        let project_path = project_path.to_string();
        let branch = branch.to_string();
        let project_id = payload["project"]["id"].as_i64();

        tokio::spawn(async move {
            if let Err(e) = trigger_auto_review(&state, &project_path, project_id, &branch).await {
                warn!("auto-review on push failed for {} branch={}: {}", project_path, branch, e);
            }
        });
    }
}

async fn handle_note_event(state: &AppState, payload: &serde_json::Value) {
    let project_path = payload["project"]["path_with_namespace"]
        .as_str()
        .unwrap_or("");
    let mr_iid = payload["merge_request"]["iid"].as_u64();
    let issue_iid = payload["issue"]["iid"].as_u64();

    if project_path.is_empty() || (mr_iid.is_none() && issue_iid.is_none()) {
        return;
    }

    match (mr_iid, issue_iid) {
        (Some(iid), _) => info!("note event: {} !{}", project_path, iid),
        (None, Some(iid)) => info!("note event: {} #{}", project_path, iid),
        _ => return,
    }

    // Channel adapter: parse @botto mentions and /botto commands
    if let Some(bus) = state.message_bus() {
        crate::services::channels::gitlab_input::parse_gitlab_comment(bus, payload);
    }

    // Notify connected Ottos that a new comment was posted
    if let Some(mr_iid) = mr_iid {
        let mr_ref = MrRef {
            project_path: project_path.to_string(),
            mr_iid,
        };

        let msg = serde_json::json!({
            "type": "EVENT_NOTIFICATION",
            "event_type": "note_added",
            "project_path": project_path,
            "mr_iid": mr_iid,
        });

        state.broadcast_to_mr(&mr_ref, &msg.to_string());
    }
}

// ---------------------------------------------------------------------------
// Auto-review on push — background task spawned from handle_push_event.
// ---------------------------------------------------------------------------

/// Find open MRs for the pushed branch and enqueue them for review.
/// Called as a spawned background task so the webhook handler returns fast.
async fn trigger_auto_review(
    state: &AppState,
    project_path: &str,
    project_id: Option<i64>,
    branch: &str,
) -> Result<(), String> {
    let queue_mgr = state
        .queue_manager()
        .ok_or("queue manager not initialized")?;

    let cfg = state.config();
    let gl_cfg = gitlab::GitLabConfig {
        base_url: cfg.gitlab.url.clone(),
        token: cfg.gitlab.bot_token.clone(),
    };

    // Resolve project ID — usually present in the push payload, but fall back
    // to an API call if missing (e.g. older GitLab versions).
    let project_id = match project_id {
        Some(id) => id,
        None => {
            let project = gitlab::fetch_project(&gl_cfg, project_path)
                .await
                .map_err(|e| format!("failed to fetch project: {}", e))?;
            project.id
        }
    };

    // Find open MRs whose source branch matches the pushed branch.
    let open_mrs = gitlab::fetch_open_mrs_for_branch(&gl_cfg, project_id, branch)
        .await
        .map_err(|e| format!("failed to fetch open MRs for branch {}: {}", branch, e))?;

    if open_mrs.is_empty() {
        debug!("auto-review: no open MRs for {} branch={}", project_path, branch);
        return Ok(());
    }

    for mr in &open_mrs {
        // Skip draft MRs — no point reviewing WIP.
        if mr.draft {
            debug!("auto-review: skipping draft MR {}:!{}", project_path, mr.iid);
            continue;
        }

        // Compute priority from available metadata. We don't have file/line
        // counts without fetching full changes (expensive), so use defaults.
        // The priority scorer handles this gracefully — label and draft signals
        // are the most impactful factors anyway.
        let has_risk_label = mr.labels.iter().any(|l| {
            let lower = l.to_lowercase();
            lower.contains("risk") || lower.contains("critical")
        });
        let has_security_label = mr.labels.iter().any(|l| {
            let lower = l.to_lowercase();
            lower.contains("security") || lower.contains("vulnerability")
        });

        let priority_input = priority::PriorityInput {
            files_changed: 0,     // unknown from list endpoint
            lines_added: 0,       // unknown from list endpoint
            lines_removed: 0,     // unknown from list endpoint
            has_risk_label,
            has_security_label,
            is_draft: false,      // already filtered above
            age_hours: 0.0,       // conservative default
            approvals_needed: 1,  // assume at least one needed
        };
        let score = priority::compute_score(&priority_input);

        info!(
            "auto-review: enqueuing {}:!{} (priority={:.0})",
            project_path, mr.iid, score
        );

        // Notify connected Ottos that an auto-review is starting.
        // This lets the UI show a "review in progress" indicator immediately,
        // even before the queue picks it up.
        let mr_ref = MrRef {
            project_path: project_path.to_string(),
            mr_iid: mr.iid,
        };
        let notification = serde_json::json!({
            "type": "EVENT_NOTIFICATION",
            "event_type": "auto_review_queued",
            "project_path": project_path,
            "mr_iid": mr.iid,
            "payload": {
                "source": "push",
                "branch": branch,
                "priority": score,
            }
        });
        state.broadcast_to_mr(&mr_ref, &notification.to_string());

        // Enqueue — duplicates are rejected gracefully by the queue manager.
        if let Err(e) = queue_mgr.enqueue(project_path, mr.iid, score).await {
            debug!("auto-review: enqueue skipped for {}:!{}: {}", project_path, mr.iid, e);
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// File index + Conflict Radar + Cluster detection — background task
// spawned from handle_mr_event.
// ---------------------------------------------------------------------------

/// Update the shared file index and run conflict/cluster detection.
/// Called as a spawned background task so the webhook handler returns fast.
async fn update_file_index_and_detect(
    state: &AppState,
    project_path: &str,
    project_id: i64,
    mr_iid: u64,
    action: &str,
) -> Result<(), String> {
    let cfg = state.config();
    let gl_cfg = gitlab::GitLabConfig {
        base_url: cfg.gitlab.url.clone(),
        token: cfg.gitlab.bot_token.clone(),
    };
    let pool = state.pool();

    match action {
        "merge" | "close" => {
            // MR is done — clean up the file index and clusters.
            let deleted = crate::db::queries::delete_mr_changed_files(pool, project_id, mr_iid as i64)
                .await
                .map_err(|e| format!("delete mr_changed_files: {}", e))?;

            if deleted > 0 {
                debug!(
                    "file index: removed {} entries for {} !{} ({})",
                    deleted, project_path, mr_iid, action
                );
            }

            // Run conflict detection for MRs that previously conflicted with this one.
            // Since this MR is gone, their conflicts may be resolved.
            if cfg.conflict.enabled {
                // We can't easily know which MRs conflicted without re-querying,
                // but the next time those MRs' pages are loaded, GET_CONFLICTS
                // will return the updated (resolved) state. For proactive push,
                // we'd need to track reverse conflict edges — deferred for now.
            }

            // Remove from clusters
            if cfg.cluster.enabled {
                match crate::services::cluster::detector::remove_mr_from_clusters(
                    pool, project_id, mr_iid,
                )
                .await
                {
                    Ok(affected_ids) => {
                        for cluster_id in &affected_ids {
                            debug!("cluster: updated/removed {} after !{} {}", cluster_id, mr_iid, action);
                        }
                        // Broadcast cluster updates to viewers of remaining member MRs
                        if !affected_ids.is_empty() {
                            state.event_bus().publish(crate::services::events::Event {
                                event_type: crate::services::events::EventType::ClusterUpdated,
                                project_path: project_path.to_string(),
                                mr_iid: Some(mr_iid),
                                user_id: None,
                                payload: Some(serde_json::json!({
                                    "action": "mr_removed",
                                    "affected_cluster_ids": affected_ids,
                                })),
                            });
                        }
                    }
                    Err(e) => {
                        warn!("cluster removal failed for !{}: {}", mr_iid, e);
                    }
                }
            }
        }

        "open" | "update" | "reopen" => {
            // Fetch MR changes from GitLab and (re-)populate the file index.
            // Uses file_index::populate (not ensure_populated) because webhooks
            // always indicate a change — we need to re-fetch, not skip.
            let file_count = crate::services::file_index::populate(
                pool, &gl_cfg, project_id, mr_iid,
            )
            .await
            .map_err(|e| format!("file index populate: {}", e))?;

            debug!(
                "file index: populated {} files for {} !{} (webhook: {})",
                file_count, project_path, mr_iid, action
            );

            // Run conflict detection and broadcast results
            if cfg.conflict.enabled {
                match crate::services::conflict::detector::detect_conflicts(
                    pool, &gl_cfg, project_id, mr_iid,
                )
                .await
                {
                    Ok(report) => {
                        if !report.conflicts.is_empty() {
                            info!(
                                "conflict radar: {} !{} has {} file conflicts",
                                project_path,
                                mr_iid,
                                report.conflicts.len()
                            );

                            // Broadcast to viewers of this MR
                            let mr_ref = MrRef {
                                project_path: project_path.to_string(),
                                mr_iid,
                            };
                            let msg = crate::api::ws::WsOutbound::ConflictUpdated {
                                project_id,
                                mr_iid,
                                conflicts: serde_json::to_value(&report).unwrap_or_default(),
                            };
                            state.broadcast_to_mr(&mr_ref, &serde_json::to_string(&msg).unwrap_or_default());

                            // Also broadcast to viewers of conflicting MRs
                            let conflicting_iids: std::collections::HashSet<u64> = report
                                .conflicts
                                .iter()
                                .flat_map(|fc| fc.conflicting_mrs.iter().map(|cm| cm.mr_iid))
                                .collect();

                            for other_iid in conflicting_iids {
                                // Re-detect for the other MR so they get their own perspective
                                if let Ok(other_report) =
                                    crate::services::conflict::detector::detect_conflicts(
                                        pool, &gl_cfg, project_id, other_iid,
                                    )
                                    .await
                                {
                                    let other_ref = MrRef {
                                        project_path: project_path.to_string(),
                                        mr_iid: other_iid,
                                    };
                                    let other_msg = crate::api::ws::WsOutbound::ConflictUpdated {
                                        project_id,
                                        mr_iid: other_iid,
                                        conflicts: serde_json::to_value(&other_report).unwrap_or_default(),
                                    };
                                    state.broadcast_to_mr(&other_ref, &serde_json::to_string(&other_msg).unwrap_or_default());
                                }
                            }
                        }
                    }
                    Err(e) => {
                        warn!("conflict detection failed for !{}: {}", mr_iid, e);
                    }
                }
            }

            // Run cluster detection and broadcast results
            if cfg.cluster.enabled {
                let ticket_strategy =
                    crate::services::cluster::strategies::ticket::TicketClusterStrategy;
                let file_strategy =
                    crate::services::cluster::strategies::file_overlap::FileOverlapStrategy {
                        jaccard_threshold: cfg.cluster.file_overlap_threshold,
                        max_cluster_size: cfg.cluster.max_cluster_size,
                    };
                let strategies: Vec<&dyn crate::services::cluster::strategies::ClusterStrategy> =
                    vec![&ticket_strategy, &file_strategy];

                match crate::services::cluster::detector::detect_clusters(
                    pool,
                    &gl_cfg,
                    project_id,
                    mr_iid,
                    &strategies,
                )
                .await
                {
                    Ok(clusters) => {
                        for cluster in &clusters {
                            info!(
                                "cluster detected: {} ({} MRs, ticket={:?})",
                                cluster.id,
                                cluster.member_mrs.len(),
                                cluster.ticket_key,
                            );

                            // Broadcast to viewers of all member MRs
                            for member in &cluster.member_mrs {
                                let mr_ref = MrRef {
                                    project_path: project_path.to_string(),
                                    mr_iid: member.mr_iid,
                                };
                                let msg = crate::api::ws::WsOutbound::ClusterUpdated {
                                    project_id,
                                    cluster: serde_json::to_value(cluster).unwrap_or_default(),
                                };
                                state.broadcast_to_mr(&mr_ref, &serde_json::to_string(&msg).unwrap_or_default());
                            }
                        }
                    }
                    Err(e) => {
                        warn!("cluster detection failed for !{}: {}", mr_iid, e);
                    }
                }
            }
        }

        _ => {
            // Other actions (e.g., "approved", "unapproved") don't affect the file index
        }
    }

    Ok(())
}
