// ---------------------------------------------------------------------------
// Workflow Orchestrator — adaptive DAG execution engine.
//
// Each workflow run gets its own orchestrator instance. The orchestrator:
//   1. Validates the DAG (cycle detection via Kahn's algorithm)
//   2. Walks the DAG in topological order, running parallel-ready steps
//   3. For each step: spawns the appropriate agent, handles retries with
//      optional mentor consultation, logs success_criteria warnings
//   4. Checkpoints state to SQLite after each step transition
//   5. Publishes events to the EventBus at key lifecycle transitions
//   6. Supports resume-from-checkpoint for crash recovery
//   7. Propagates dependency failures so steps never stay Pending forever
//
// The orchestrator is spawned as a tokio task by the scheduler or API handler.
// ---------------------------------------------------------------------------

use anyhow::{anyhow, Context as _, Result};
use chrono::Utc;
use serde_json::Value;
use sqlx::SqlitePool;
use std::collections::{HashMap, HashSet, VecDeque};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::services::events::{Event, EventBus, EventType};
use crate::services::mentor::client::MentorClient;
use crate::services::workflow::factory::{create_agent, AgentFactoryConfig};
use crate::types::workflow::{
    AgentResult, AgentStatus, RunStatus, StepInput, StepState, TriggerSource,
    WorkflowDefinition, WorkflowRun, WorkflowStep,
};

// ---------------------------------------------------------------------------
// Orchestrator
// ---------------------------------------------------------------------------

/// A single workflow run orchestrator.
pub struct Orchestrator {
    pool: SqlitePool,
    mentor: MentorClient,
    agent_config: AgentFactoryConfig,
    /// Maximum step timeout override (from WorkflowConfig).
    default_step_timeout_secs: u64,
    /// Composite nesting depth — 0 for top-level runs, incremented for nested.
    depth: u32,
    /// Optional event bus for publishing lifecycle events.
    event_bus: Option<EventBus>,
}

impl Orchestrator {
    pub fn new(
        pool: SqlitePool,
        mentor: MentorClient,
        agent_config: AgentFactoryConfig,
        default_step_timeout_secs: u64,
    ) -> Self {
        Self {
            pool,
            mentor,
            agent_config,
            default_step_timeout_secs,
            depth: 0,
            event_bus: None,
        }
    }

    /// Create an orchestrator with an EventBus for publishing lifecycle events.
    pub fn with_event_bus(
        pool: SqlitePool,
        mentor: MentorClient,
        agent_config: AgentFactoryConfig,
        default_step_timeout_secs: u64,
        event_bus: EventBus,
    ) -> Self {
        Self {
            pool,
            mentor,
            agent_config,
            default_step_timeout_secs,
            depth: 0,
            event_bus: Some(event_bus),
        }
    }

    /// Create an orchestrator for nested (composite) execution at the given depth.
    pub fn new_nested(
        pool: SqlitePool,
        mentor: MentorClient,
        agent_config: AgentFactoryConfig,
        default_step_timeout_secs: u64,
        depth: u32,
    ) -> Self {
        Self {
            pool,
            mentor,
            agent_config,
            default_step_timeout_secs,
            depth,
            event_bus: None,
        }
    }

    // -----------------------------------------------------------------------
    // DAG validation — cycle detection via Kahn's algorithm
    // -----------------------------------------------------------------------

    /// Validate that the workflow DAG has no cycles and all dependency
    /// references point to existing steps.
    ///
    /// Uses Kahn's algorithm: repeatedly remove nodes with zero in-degree.
    /// If we can't remove all nodes, the remaining ones form a cycle.
    /// Returns Ok(topological_order) or Err with the cycle participants.
    fn validate_dag(steps: &[WorkflowStep]) -> Result<Vec<String>> {
        if steps.is_empty() {
            debug!("validate_dag: empty DAG, nothing to validate");
            return Ok(Vec::new());
        }

        let step_ids: HashSet<&str> = steps.iter().map(|s| s.id.as_str()).collect();

        // Check for references to non-existent steps.
        for step in steps {
            for dep in &step.depends_on {
                if !step_ids.contains(dep.as_str()) {
                    return Err(anyhow!(
                        "step '{}' depends on '{}' which does not exist in the workflow",
                        step.id,
                        dep
                    ));
                }
            }
        }

        // Build in-degree map and adjacency list.
        let mut in_degree: HashMap<&str, usize> = HashMap::new();
        let mut dependents: HashMap<&str, Vec<&str>> = HashMap::new();

        for step in steps {
            in_degree.entry(step.id.as_str()).or_insert(0);
            for dep in &step.depends_on {
                *in_degree.entry(step.id.as_str()).or_insert(0) += 1;
                dependents
                    .entry(dep.as_str())
                    .or_default()
                    .push(step.id.as_str());
            }
        }

        // Seed the queue with zero in-degree nodes.
        let mut queue: VecDeque<&str> = in_degree
            .iter()
            .filter(|&(_, &deg)| deg == 0)
            .map(|(&id, _)| id)
            .collect();

        let mut sorted: Vec<String> = Vec::with_capacity(steps.len());

        while let Some(node) = queue.pop_front() {
            sorted.push(node.to_string());
            if let Some(deps) = dependents.get(node) {
                for &dep in deps {
                    if let Some(deg) = in_degree.get_mut(dep) {
                        *deg -= 1;
                        if *deg == 0 {
                            queue.push_back(dep);
                        }
                    }
                }
            }
        }

        if sorted.len() != steps.len() {
            let cycle_nodes: Vec<String> = in_degree
                .iter()
                .filter(|&(_, &deg)| deg > 0)
                .map(|(&id, _)| id.to_string())
                .collect();
            Err(anyhow!(
                "workflow DAG contains a cycle involving steps: [{}]",
                cycle_nodes.join(", ")
            ))
        } else {
            debug!(order = ?sorted, "validate_dag: topological order verified");
            Ok(sorted)
        }
    }

    /// Execute a workflow run from start to finish.
    ///
    /// Validates the DAG, creates a new WorkflowRun, walks the DAG,
    /// checkpoints after each step, and returns the completed run.
    pub async fn execute(
        &self,
        definition: &WorkflowDefinition,
        trigger: TriggerSource,
    ) -> WorkflowRun {
        self.execute_with_id(Uuid::new_v4(), definition, trigger).await
    }

    /// Execute a workflow run with a pre-assigned run ID.
    ///
    /// Used by the API trigger endpoint so the run ID can be returned to the
    /// caller before execution completes.
    pub async fn execute_with_id(
        &self,
        run_id: Uuid,
        definition: &WorkflowDefinition,
        trigger: TriggerSource,
    ) -> WorkflowRun {
        // Validate DAG before doing anything.
        if let Err(e) = Self::validate_dag(&definition.steps) {
            error!(
                workflow = %definition.name,
                error = %e,
                "orchestrator: DAG validation failed"
            );
            let now = Utc::now();
            return WorkflowRun {
                id: run_id,
                workflow_id: definition.id,
                trigger,
                status: RunStatus::Failed,
                step_states: HashMap::new(),
                started_at: now,
                completed_at: Some(now),
                final_verification: None,
                mentor_queries: Vec::new(),
            };
        }

        let now = Utc::now();
        let mut run = WorkflowRun {
            id: run_id,
            workflow_id: definition.id,
            trigger,
            status: RunStatus::Running,
            step_states: HashMap::new(),
            started_at: now,
            completed_at: None,
            final_verification: None,
            mentor_queries: Vec::new(),
        };

        // Initialize all steps as Pending.
        for step in &definition.steps {
            run.step_states
                .insert(step.id.clone(), StepState::Pending);
        }

        // Persist initial state.
        self.checkpoint(&run).await;

        info!(
            run_id = %run.id,
            workflow = %definition.name,
            steps = definition.steps.len(),
            "orchestrator: starting run"
        );

        self.publish_event(
            EventType::WorkflowRunStarted,
            &definition.project_id.to_string(),
            Some(serde_json::json!({
                "run_id": run.id.to_string(),
                "workflow": &definition.name,
                "steps": definition.steps.len(),
            })),
        );

        // Walk the DAG.
        self.walk_dag(definition, &mut run).await;

        // Determine final status.
        self.finalize_run(&mut run);

        // Final checkpoint — terminal state, must propagate errors.
        if let Err(e) = self.checkpoint_terminal(&run).await {
            error!(
                run_id = %run.id,
                error = %e,
                "orchestrator: CRITICAL final checkpoint failed, state may be lost"
            );
        }

        info!(
            run_id = %run.id,
            status = %run.status,
            "orchestrator: run finished"
        );

        self.publish_event(
            EventType::WorkflowRunCompleted,
            &definition.project_id.to_string(),
            Some(serde_json::json!({
                "run_id": run.id.to_string(),
                "status": run.status.to_string(),
            })),
        );

        run
    }

    // -----------------------------------------------------------------------
    // Resume — load an existing run from SQLite and continue
    // -----------------------------------------------------------------------

    /// Resume a workflow run from its last checkpoint.
    ///
    /// Loads the run from SQLite, resets any Running steps back to Pending
    /// (they were interrupted), and continues walking the DAG.
    pub async fn resume(
        &self,
        run_id: Uuid,
        definition: &WorkflowDefinition,
    ) -> Result<WorkflowRun> {
        // Validate DAG before resuming.
        Self::validate_dag(&definition.steps)?;

        let mut run = self
            .load_run(run_id)
            .await
            .context("failed to load run for resume")?;

        info!(
            run_id = %run.id,
            status = %run.status,
            completed_steps = run.step_states.values()
                .filter(|s| matches!(s, StepState::Completed { .. }))
                .count(),
            total_steps = run.step_states.len(),
            "orchestrator: resuming run"
        );

        // If the run is already terminal, nothing to do.
        if run.status.is_terminal() {
            info!(
                run_id = %run.id,
                status = %run.status,
                "orchestrator: run already terminal, nothing to resume"
            );
            return Ok(run);
        }

        // Reset any Running steps back to Pending — they were interrupted.
        for (step_id, state) in run.step_states.iter_mut() {
            if matches!(state, StepState::Running { .. }) {
                debug!(step_id = %step_id, "orchestrator: resetting interrupted step to Pending");
                *state = StepState::Pending;
            }
        }

        // Ensure any steps missing from step_states (e.g. definition changed)
        // are initialized as Pending.
        for step in &definition.steps {
            run.step_states
                .entry(step.id.clone())
                .or_insert(StepState::Pending);
        }

        run.status = RunStatus::Running;
        self.checkpoint(&run).await;

        // Continue walking the DAG.
        self.walk_dag(definition, &mut run).await;

        // Determine final status.
        self.finalize_run(&mut run);

        // Final checkpoint — terminal state, must propagate errors.
        self.checkpoint_terminal(&run).await?;

        info!(
            run_id = %run.id,
            status = %run.status,
            "orchestrator: resumed run finished"
        );

        Ok(run)
    }

    /// Load a workflow run from SQLite by run_id.
    async fn load_run(&self, run_id: Uuid) -> Result<WorkflowRun> {
        let row: (String, String, String, Option<String>, String, String, Option<String>, i64, Option<i64>) =
            sqlx::query_as(
                "SELECT id, workflow_id, trigger_type, trigger_data, status, step_states, final_verification, started_at, completed_at
                 FROM workflow_runs WHERE id = ?",
            )
            .bind(run_id.to_string())
            .fetch_one(&self.pool)
            .await
            .context("workflow run not found in database")?;

        let (_id_str, workflow_id_str, _trigger_type, trigger_data, status_str, step_states_json, verification_json, started_at_ts, completed_at_ts) = row;

        let workflow_id: Uuid = workflow_id_str.parse().context("invalid workflow_id in DB")?;

        let trigger: TriggerSource = trigger_data
            .as_deref()
            .map(serde_json::from_str)
            .transpose()
            .context("invalid trigger_data JSON in DB")?
            .unwrap_or(TriggerSource::Manual {
                user: "unknown".into(),
            });

        let status = match status_str.as_str() {
            "pending" => RunStatus::Pending,
            "running" => RunStatus::Running,
            "completed" => RunStatus::Completed,
            "failed" => RunStatus::Failed,
            "cancelled" => RunStatus::Cancelled,
            other => return Err(anyhow!("unknown run status in DB: {}", other)),
        };

        let step_states: HashMap<String, StepState> =
            serde_json::from_str(&step_states_json).context("invalid step_states JSON in DB")?;

        let final_verification = verification_json
            .as_deref()
            .map(serde_json::from_str)
            .transpose()
            .context("invalid final_verification JSON in DB")?;

        let started_at = chrono::DateTime::from_timestamp(started_at_ts, 0)
            .unwrap_or_else(Utc::now);
        let completed_at =
            completed_at_ts.and_then(|t| chrono::DateTime::from_timestamp(t, 0));

        Ok(WorkflowRun {
            id: run_id,
            workflow_id,
            trigger,
            status,
            step_states,
            started_at,
            completed_at,
            final_verification,
            mentor_queries: Vec::new(),
        })
    }

    /// Determine final run status from step states.
    fn finalize_run(&self, run: &mut WorkflowRun) {
        let all_done = run.step_states.values().all(|s| s.is_terminal());
        let any_failed = run
            .step_states
            .values()
            .any(|s| matches!(s, StepState::Failed { .. }));

        run.status = if !all_done {
            warn!(
                run_id = %run.id,
                pending = run.step_states.values()
                    .filter(|s| !s.is_terminal())
                    .count(),
                "orchestrator: run ending with non-terminal steps"
            );
            RunStatus::Failed
        } else if any_failed {
            RunStatus::Failed
        } else {
            RunStatus::Completed
        };
        run.completed_at = Some(Utc::now());

        debug!(
            run_id = %run.id,
            status = %run.status,
            completed = run.step_states.values()
                .filter(|s| matches!(s, StepState::Completed { .. }))
                .count(),
            failed = run.step_states.values()
                .filter(|s| matches!(s, StepState::Failed { .. }))
                .count(),
            skipped = run.step_states.values()
                .filter(|s| matches!(s, StepState::Skipped { .. }))
                .count(),
            "orchestrator: finalized run status"
        );
    }

    /// Cancel a running workflow.
    pub async fn cancel(&self, run: &mut WorkflowRun) {
        run.status = RunStatus::Cancelled;
        run.completed_at = Some(Utc::now());

        // Mark any pending/running steps as skipped.
        for state in run.step_states.values_mut() {
            if !state.is_terminal() {
                *state = StepState::Skipped {
                    reason: "workflow cancelled".into(),
                };
            }
        }

        self.checkpoint(run).await;
        info!(run_id = %run.id, "orchestrator: run cancelled");
    }

    // -----------------------------------------------------------------------
    // DAG walking
    // -----------------------------------------------------------------------

    /// Walk the DAG in topological order. Steps with no unmet dependencies
    /// are launched in parallel within each wave.
    async fn walk_dag(&self, definition: &WorkflowDefinition, run: &mut WorkflowRun) {
        loop {
            if run.status == RunStatus::Cancelled {
                debug!(run_id = %run.id, "walk_dag: cancelled, stopping");
                break;
            }

            // First pass: propagate skips for steps whose dependencies
            // have failed or been skipped. This prevents them from staying
            // Pending forever.
            self.propagate_dependency_failures(&definition.steps, run);

            // Second pass: find steps that are ready to run.
            let ready: Vec<WorkflowStep> = definition
                .steps
                .iter()
                .filter(|step| {
                    matches!(
                        run.step_states.get(&step.id),
                        Some(StepState::Pending)
                    )
                })
                .filter(|step| {
                    step.depends_on.iter().all(|dep_id| {
                        matches!(
                            run.step_states.get(dep_id),
                            Some(StepState::Completed { .. })
                        )
                    })
                })
                .cloned()
                .collect();

            if ready.is_empty() {
                debug!(
                    run_id = %run.id,
                    pending = run.step_states.values()
                        .filter(|s| matches!(s, StepState::Pending))
                        .count(),
                    "walk_dag: no ready steps, exiting loop"
                );
                break;
            }

            info!(
                run_id = %run.id,
                wave_size = ready.len(),
                steps = ?ready.iter().map(|s| s.id.as_str()).collect::<Vec<_>>(),
                "orchestrator: starting wave"
            );

            // Resolve inputs for all ready steps before spawning.
            let mut step_inputs: Vec<(WorkflowStep, HashMap<String, Value>)> =
                Vec::with_capacity(ready.len());
            for step in &ready {
                let resolved = self.resolve_inputs(step, &run.step_states).await;
                step_inputs.push((step.clone(), resolved));
            }

            if step_inputs.len() == 1 {
                // Single step — no need for spawn overhead.
                let (step, inputs) = step_inputs.into_iter().next().unwrap();
                self.publish_step_started(&step, run);
                self.execute_step(&step, inputs, run).await;
                self.publish_step_finished(&step, run);
                self.checkpoint(run).await;
            } else {
                // Multiple steps — run in parallel with tokio::spawn.
                let run_id = run.id;
                let workflow_id = run.workflow_id;
                let step_states_snapshot = run.step_states.clone();
                let _ = &step_states_snapshot; // reserved for future input resolution in parallel

                let mut handles = Vec::with_capacity(step_inputs.len());
                for (step, inputs) in step_inputs {
                    let pool = self.pool.clone();
                    let mentor = self.mentor.clone();
                    let agent_config = self.agent_config.clone();
                    let default_timeout = self.default_step_timeout_secs;
                    let depth = self.depth;

                    handles.push(tokio::spawn(async move {
                        let new_state = execute_step_standalone(
                            &step,
                            inputs,
                            run_id,
                            workflow_id,
                            &pool,
                            &mentor,
                            &agent_config,
                            default_timeout,
                            depth,
                        )
                        .await;
                        (step, new_state)
                    }));
                }

                // Wait for all steps in this wave to complete.
                let results = futures::future::join_all(handles).await;

                for join_result in results {
                    match join_result {
                        Ok((step, new_state)) => {
                            debug!(
                                run_id = %run.id,
                                step_id = %step.id,
                                terminal = new_state.is_terminal(),
                                "orchestrator: parallel step finished"
                            );
                            run.step_states.insert(step.id.clone(), new_state);
                        }
                        Err(e) => {
                            error!(
                                run_id = %run.id,
                                error = %e,
                                "orchestrator: parallel step task panicked"
                            );
                        }
                    }
                }

                // Checkpoint after the whole wave completes.
                self.checkpoint(run).await;
            }
        }
    }

    /// Propagate skips: if a pending step has any dependency that is
    /// Failed or Skipped, mark it as Skipped. Repeats until stable.
    fn propagate_dependency_failures(
        &self,
        all_steps: &[WorkflowStep],
        run: &mut WorkflowRun,
    ) {
        loop {
            let mut changed = false;
            for step in all_steps {
                if !matches!(run.step_states.get(&step.id), Some(StepState::Pending)) {
                    continue;
                }
                // Check if any dependency is failed or skipped.
                let failed_dep = step.depends_on.iter().find(|dep_id| {
                    matches!(
                        run.step_states.get(*dep_id),
                        Some(StepState::Failed { .. }) | Some(StepState::Skipped { .. })
                    )
                });
                if let Some(dep_id) = failed_dep {
                    debug!(
                        step_id = %step.id,
                        failed_dep = %dep_id,
                        "orchestrator: skipping step due to failed/skipped dependency"
                    );
                    run.step_states.insert(
                        step.id.clone(),
                        StepState::Skipped {
                            reason: format!("dependency '{}' failed/skipped", dep_id),
                        },
                    );
                    changed = true;
                }
            }
            if !changed {
                break;
            }
        }
    }

    /// Execute a single step with retries.
    async fn execute_step(
        &self,
        step: &WorkflowStep,
        inputs: HashMap<String, Value>,
        run: &mut WorkflowRun,
    ) {
        let agent_id = Uuid::new_v4();
        let started_at = Utc::now();

        // Mark as running.
        run.step_states.insert(
            step.id.clone(),
            StepState::Running {
                agent_id,
                started_at,
            },
        );

        info!(
            run_id = %run.id,
            step_id = %step.id,
            agent_type = %step.agent_type,
            "orchestrator: step started"
        );

        // Create the agent.
        let agent = match create_agent(&step.agent_type, &self.agent_config, self.depth).await {
            Some(a) => a,
            None => {
                warn!(
                    step_id = %step.id,
                    agent_type = %step.agent_type,
                    "orchestrator: no agent available"
                );
                run.step_states.insert(
                    step.id.clone(),
                    StepState::Failed {
                        error: format!(
                            "no agent available for type '{}'",
                            step.agent_type
                        ),
                        retries: 0,
                        duration_secs: 0.0,
                    },
                );
                return;
            }
        };

        let timeout = if step.timeout_secs > 0 {
            step.timeout_secs
        } else {
            self.default_step_timeout_secs
        };

        // Execute with retries.
        let max_retries = step.retry_policy.max_retries;
        let mut last_result: Option<AgentResult> = None;
        let mut retries = 0u32;

        for attempt in 0..=max_retries {
            if attempt > 0 {
                // Consult mentor before retrying if configured.
                if step.retry_policy.consult_mentor_on_failure {
                    let error_msg = last_result
                        .as_ref()
                        .and_then(|r| r.output.get("error"))
                        .and_then(|v| v.as_str())
                        .unwrap_or("unknown error");

                    let question = format!(
                        "Step '{}' (type: {}) failed with error: {}. \
                         This is retry attempt {} of {}. \
                         What recovery strategies should be tried?",
                        step.id, step.agent_type, error_msg, attempt, max_retries
                    );

                    info!(
                        step_id = %step.id,
                        attempt,
                        "orchestrator: consulting mentor before retry"
                    );

                    match self.mentor.query(&question, 3).await {
                        Ok(results) => {
                            if !results.is_empty() {
                                debug!(
                                    step_id = %step.id,
                                    results = results.len(),
                                    "orchestrator: mentor provided recovery suggestions"
                                );
                                run.mentor_queries.push(
                                    crate::types::workflow::MentorInteraction {
                                        step_id: step.id.clone(),
                                        question: question.clone(),
                                        results_count: results.len(),
                                        queried_at: Utc::now(),
                                    },
                                );
                            }
                        }
                        Err(e) => {
                            warn!(
                                step_id = %step.id,
                                error = %e,
                                "orchestrator: mentor consultation failed, retrying anyway"
                            );
                        }
                    }
                }

                // Backoff before retry.
                let delay = self.compute_backoff(&step.retry_policy, attempt);
                debug!(
                    step_id = %step.id,
                    attempt,
                    delay_ms = delay.as_millis() as u64,
                    "orchestrator: retrying step"
                );
                tokio::time::sleep(delay).await;
                retries = attempt;
            }

            // Run with timeout.
            let result = tokio::time::timeout(
                std::time::Duration::from_secs(timeout),
                agent.execute(&step.action, inputs.clone(), &self.mentor),
            )
            .await;

            match result {
                Ok(agent_result) => {
                    // Feed learnings back to Mentor.
                    for learning in &agent_result.learnings {
                        if let Err(e) = self
                            .mentor
                            .remember(
                                &learning.content,
                                &learning.scope,
                                "repo",
                                &learning.category.to_string(),
                                Some(&run.workflow_id.to_string()),
                                Some(&step.id),
                            )
                            .await
                        {
                            warn!(error = %e, "orchestrator: failed to store learning");
                        }
                    }

                    match agent_result.status {
                        AgentStatus::Success | AgentStatus::Partial => {
                            if agent_result.status == AgentStatus::Partial {
                                debug!(
                                    step_id = %step.id,
                                    "orchestrator: step returned Partial, treating as success"
                                );
                            }

                            // TODO: Evaluate success_criteria using AI service.
                            // When success_criteria is non-empty, the output should be
                            // validated against it. For now, we log a warning and skip
                            // evaluation. See: https://github.com/botto/issues/TBD
                            if !step.success_criteria.is_empty() {
                                warn!(
                                    step_id = %step.id,
                                    success_criteria = %step.success_criteria,
                                    "orchestrator: success_criteria is set but not evaluated \
                                     (AI evaluation not yet implemented)"
                                );
                            }

                            run.step_states.insert(
                                step.id.clone(),
                                StepState::Completed {
                                    output: agent_result.output,
                                    duration_secs: agent_result.duration_secs,
                                },
                            );
                            info!(
                                run_id = %run.id,
                                step_id = %step.id,
                                duration_secs = agent_result.duration_secs,
                                "orchestrator: step completed"
                            );
                            return;
                        }
                        AgentStatus::Failure => {
                            last_result = Some(agent_result);
                            // Continue to retry.
                        }
                    }
                }
                Err(_) => {
                    warn!(
                        step_id = %step.id,
                        timeout_secs = timeout,
                        "orchestrator: step timed out"
                    );
                    last_result = Some(AgentResult {
                        status: AgentStatus::Failure,
                        output: serde_json::json!({"error": "step timed out"}),
                        duration_secs: timeout as f64,
                        learnings: Vec::new(),
                    });
                }
            }
        }

        // All retries exhausted.
        let error_msg = last_result
            .as_ref()
            .and_then(|r| r.output.get("error"))
            .and_then(|v| v.as_str())
            .unwrap_or("step failed after all retries")
            .to_string();

        let duration = last_result
            .as_ref()
            .map(|r| r.duration_secs)
            .unwrap_or(0.0);

        warn!(
            run_id = %run.id,
            step_id = %step.id,
            retries,
            error = %error_msg,
            "orchestrator: step failed after all retries"
        );

        run.step_states.insert(
            step.id.clone(),
            StepState::Failed {
                error: error_msg,
                retries,
                duration_secs: duration,
            },
        );
    }

    // -----------------------------------------------------------------------
    // Input resolution
    // -----------------------------------------------------------------------

    /// Resolve step inputs: replace StepOutput references with actual values
    /// from completed steps.
    async fn resolve_inputs(
        &self,
        step: &WorkflowStep,
        step_states: &HashMap<String, StepState>,
    ) -> HashMap<String, Value> {
        let mut resolved = HashMap::new();

        for (key, input) in &step.inputs {
            let value = match input {
                StepInput::Static { value } => value.clone(),
                StepInput::StepOutput { step_id, field } => {
                    if let Some(StepState::Completed { output, .. }) = step_states.get(step_id) {
                        output
                            .get(field)
                            .cloned()
                            .unwrap_or(Value::Null)
                    } else {
                        Value::Null
                    }
                }
                StepInput::MentorQuery { question } => {
                    match self.mentor.query(question, 5).await {
                        Ok(results) => {
                            let entries: Vec<Value> = results
                                .iter()
                                .map(|r| {
                                    serde_json::json!({
                                        "content": r.content,
                                        "category": r.category,
                                        "confidence": r.confidence,
                                    })
                                })
                                .collect();
                            Value::Array(entries)
                        }
                        Err(e) => {
                            warn!(error = %e, "orchestrator: mentor query failed");
                            Value::Array(Vec::new())
                        }
                    }
                }
            };
            resolved.insert(key.clone(), value);
        }

        resolved
    }

    // -----------------------------------------------------------------------
    // Backoff
    // -----------------------------------------------------------------------

    fn compute_backoff(
        &self,
        policy: &crate::types::workflow::RetryPolicy,
        attempt: u32,
    ) -> std::time::Duration {
        match &policy.backoff {
            crate::types::workflow::BackoffStrategy::Fixed { delay_secs } => {
                std::time::Duration::from_secs(*delay_secs)
            }
            crate::types::workflow::BackoffStrategy::Exponential {
                base_secs,
                max_secs,
            } => {
                let delay = (*base_secs).saturating_mul(2u64.saturating_pow(attempt - 1));
                std::time::Duration::from_secs(delay.min(*max_secs))
            }
        }
    }

    // -----------------------------------------------------------------------
    // Checkpointing
    // -----------------------------------------------------------------------

    /// Persist the current run state to SQLite (non-terminal, warn on failure).
    async fn checkpoint(&self, run: &WorkflowRun) {
        if let Err(e) = self.checkpoint_inner(run).await {
            warn!(run_id = %run.id, error = %e, "orchestrator: checkpoint failed (non-terminal)");
        }
    }

    /// Persist terminal state to SQLite. Returns error on failure so callers
    /// can handle it — losing a terminal checkpoint means state is lost on crash.
    async fn checkpoint_terminal(&self, run: &WorkflowRun) -> Result<()> {
        self.checkpoint_inner(run)
            .await
            .context("terminal checkpoint failed — run state may be lost on crash")
    }

    /// Inner checkpoint implementation shared by both variants.
    async fn checkpoint_inner(&self, run: &WorkflowRun) -> Result<()> {
        let step_states_json =
            serde_json::to_string(&run.step_states).unwrap_or_else(|_| "{}".into());
        let verification_json = run
            .final_verification
            .as_ref()
            .and_then(|v| serde_json::to_string(v).ok());
        let trigger_type = match &run.trigger {
            TriggerSource::Cron { .. } => "cron",
            TriggerSource::Event { .. } => "event",
            TriggerSource::Manual { .. } => "manual",
        };
        let trigger_data = serde_json::to_string(&run.trigger).ok();
        let started_at = run.started_at.timestamp();
        let completed_at = run.completed_at.map(|t| t.timestamp());

        sqlx::query(
            "INSERT INTO workflow_runs
                (id, workflow_id, trigger_type, trigger_data, status, step_states, final_verification, started_at, completed_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(id) DO UPDATE SET
                status = excluded.status,
                step_states = excluded.step_states,
                final_verification = excluded.final_verification,
                completed_at = excluded.completed_at",
        )
        .bind(run.id.to_string())
        .bind(run.workflow_id.to_string())
        .bind(trigger_type)
        .bind(&trigger_data)
        .bind(run.status.to_string())
        .bind(&step_states_json)
        .bind(&verification_json)
        .bind(started_at)
        .bind(completed_at)
        .execute(&self.pool)
        .await
        .context("SQLite checkpoint write failed")?;

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Event publishing
    // -----------------------------------------------------------------------

    /// Publish an event to the EventBus if one is configured.
    fn publish_event(
        &self,
        event_type: EventType,
        project_path: &str,
        payload: Option<Value>,
    ) {
        if let Some(bus) = &self.event_bus {
            let event = Event {
                event_type,
                project_path: project_path.to_string(),
                mr_iid: None,
                user_id: None,
                payload,
            };
            let receivers = bus.publish(event);
            debug!(
                receivers,
                "orchestrator: published event"
            );
        }
    }

    /// Publish a WorkflowStepStarted event.
    fn publish_step_started(&self, step: &WorkflowStep, run: &WorkflowRun) {
        self.publish_event(
            EventType::WorkflowStepStarted,
            &run.workflow_id.to_string(),
            Some(serde_json::json!({
                "run_id": run.id.to_string(),
                "step_id": &step.id,
                "agent_type": step.agent_type.to_string(),
            })),
        );
    }

    /// Publish a WorkflowStepCompleted or WorkflowStepFailed event based on state.
    fn publish_step_finished(&self, step: &WorkflowStep, run: &WorkflowRun) {
        match run.step_states.get(&step.id) {
            Some(StepState::Completed { duration_secs, .. }) => {
                self.publish_event(
                    EventType::WorkflowStepCompleted,
                    &run.workflow_id.to_string(),
                    Some(serde_json::json!({
                        "run_id": run.id.to_string(),
                        "step_id": &step.id,
                        "duration_secs": duration_secs,
                    })),
                );
            }
            Some(StepState::Failed { error, retries, .. }) => {
                self.publish_event(
                    EventType::WorkflowStepFailed,
                    &run.workflow_id.to_string(),
                    Some(serde_json::json!({
                        "run_id": run.id.to_string(),
                        "step_id": &step.id,
                        "error": error,
                        "retries": retries,
                    })),
                );
            }
            _ => {}
        }
    }
}

// ---------------------------------------------------------------------------
// Standalone step execution — used by parallel wave execution.
//
// This function runs a single step without needing &mut WorkflowRun, so it
// can be spawned as an independent tokio task. Returns the resulting StepState.
// ---------------------------------------------------------------------------

/// Execute a single step in isolation (for parallel execution).
///
/// Does not modify the run — returns the resulting StepState for the caller
/// to merge back.
async fn execute_step_standalone(
    step: &WorkflowStep,
    inputs: HashMap<String, Value>,
    run_id: Uuid,
    workflow_id: Uuid,
    _pool: &SqlitePool,
    mentor: &MentorClient,
    agent_config: &AgentFactoryConfig,
    default_step_timeout_secs: u64,
    depth: u32,
) -> StepState {
    info!(
        run_id = %run_id,
        step_id = %step.id,
        agent_type = %step.agent_type,
        "orchestrator[parallel]: step started"
    );

    let agent = match create_agent(&step.agent_type, agent_config, depth).await {
        Some(a) => a,
        None => {
            warn!(
                step_id = %step.id,
                agent_type = %step.agent_type,
                "orchestrator[parallel]: no agent available"
            );
            return StepState::Failed {
                error: format!("no agent available for type '{}'", step.agent_type),
                retries: 0,
                duration_secs: 0.0,
            };
        }
    };

    let timeout = if step.timeout_secs > 0 {
        step.timeout_secs
    } else {
        default_step_timeout_secs
    };

    let max_retries = step.retry_policy.max_retries;
    let mut last_result: Option<AgentResult> = None;
    let mut retries = 0u32;

    for attempt in 0..=max_retries {
        if attempt > 0 {
            // Consult mentor before retrying if configured.
            if step.retry_policy.consult_mentor_on_failure {
                let error_msg = last_result
                    .as_ref()
                    .and_then(|r| r.output.get("error"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown error");

                let question = format!(
                    "Step '{}' (type: {}) failed with error: {}. \
                     This is retry attempt {} of {}. \
                     What recovery strategies should be tried?",
                    step.id, step.agent_type, error_msg, attempt, max_retries
                );

                info!(
                    step_id = %step.id,
                    attempt,
                    "orchestrator[parallel]: consulting mentor before retry"
                );

                match mentor.query(&question, 3).await {
                    Ok(results) => {
                        if !results.is_empty() {
                            debug!(
                                step_id = %step.id,
                                results = results.len(),
                                "orchestrator[parallel]: mentor provided recovery suggestions"
                            );
                        }
                    }
                    Err(e) => {
                        warn!(
                            step_id = %step.id,
                            error = %e,
                            "orchestrator[parallel]: mentor consultation failed, retrying anyway"
                        );
                    }
                }
            }

            let delay = compute_backoff_static(&step.retry_policy, attempt);
            debug!(
                step_id = %step.id,
                attempt,
                delay_ms = delay.as_millis() as u64,
                "orchestrator[parallel]: retrying step"
            );
            tokio::time::sleep(delay).await;
            retries = attempt;
        }

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(timeout),
            agent.execute(&step.action, inputs.clone(), mentor),
        )
        .await;

        match result {
            Ok(agent_result) => {
                // Feed learnings back to Mentor.
                for learning in &agent_result.learnings {
                    if let Err(e) = mentor
                        .remember(
                            &learning.content,
                            &learning.scope,
                            "repo",
                            &learning.category.to_string(),
                            Some(&workflow_id.to_string()),
                            Some(&step.id),
                        )
                        .await
                    {
                        warn!(error = %e, "orchestrator[parallel]: failed to store learning");
                    }
                }

                match agent_result.status {
                    AgentStatus::Success | AgentStatus::Partial => {
                        if agent_result.status == AgentStatus::Partial {
                            debug!(
                                step_id = %step.id,
                                "orchestrator[parallel]: step returned Partial, treating as success"
                            );
                        }

                        // TODO: Evaluate success_criteria using AI service.
                        if !step.success_criteria.is_empty() {
                            warn!(
                                step_id = %step.id,
                                success_criteria = %step.success_criteria,
                                "orchestrator[parallel]: success_criteria is set but not evaluated \
                                 (AI evaluation not yet implemented)"
                            );
                        }

                        info!(
                            run_id = %run_id,
                            step_id = %step.id,
                            duration_secs = agent_result.duration_secs,
                            "orchestrator[parallel]: step completed"
                        );
                        return StepState::Completed {
                            output: agent_result.output,
                            duration_secs: agent_result.duration_secs,
                        };
                    }
                    AgentStatus::Failure => {
                        last_result = Some(agent_result);
                    }
                }
            }
            Err(_) => {
                warn!(
                    step_id = %step.id,
                    timeout_secs = timeout,
                    "orchestrator[parallel]: step timed out"
                );
                last_result = Some(AgentResult {
                    status: AgentStatus::Failure,
                    output: serde_json::json!({"error": "step timed out"}),
                    duration_secs: timeout as f64,
                    learnings: Vec::new(),
                });
            }
        }
    }

    let error_msg = last_result
        .as_ref()
        .and_then(|r| r.output.get("error"))
        .and_then(|v| v.as_str())
        .unwrap_or("step failed after all retries")
        .to_string();

    let duration = last_result
        .as_ref()
        .map(|r| r.duration_secs)
        .unwrap_or(0.0);

    warn!(
        run_id = %run_id,
        step_id = %step.id,
        retries,
        error = %error_msg,
        "orchestrator[parallel]: step failed after all retries"
    );

    StepState::Failed {
        error: error_msg,
        retries,
        duration_secs: duration,
    }
}

/// Static backoff computation for use outside the Orchestrator impl.
fn compute_backoff_static(
    policy: &crate::types::workflow::RetryPolicy,
    attempt: u32,
) -> std::time::Duration {
    match &policy.backoff {
        crate::types::workflow::BackoffStrategy::Fixed { delay_secs } => {
            std::time::Duration::from_secs(*delay_secs)
        }
        crate::types::workflow::BackoffStrategy::Exponential {
            base_secs,
            max_secs,
        } => {
            let delay = (*base_secs).saturating_mul(2u64.saturating_pow(attempt - 1));
            std::time::Duration::from_secs(delay.min(*max_secs))
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::workflow::{BackoffStrategy, RetryPolicy};

    #[tokio::test]
    async fn backoff_fixed() {
        let pool = SqlitePool::connect_lazy("sqlite::memory:").unwrap();
        let mentor = MentorClient::new(pool.clone(), "test-repo".into());
        let orch = Orchestrator::new(
            pool.clone(),
            mentor,
            AgentFactoryConfig {
                gitlab: None,
                ai: None,
                ai_default_model: "test".into(),
                sandbox_max_memory_mb: 2048,
                pool,
                botto_config: None,
                event_bus: None,
            },
            300,
        );

        let policy = RetryPolicy {
            max_retries: 3,
            backoff: BackoffStrategy::Fixed { delay_secs: 5 },
            consult_mentor_on_failure: false,
        };

        assert_eq!(orch.compute_backoff(&policy, 1).as_secs(), 5);
        assert_eq!(orch.compute_backoff(&policy, 2).as_secs(), 5);
        assert_eq!(orch.compute_backoff(&policy, 3).as_secs(), 5);
    }

    #[tokio::test]
    async fn backoff_exponential() {
        let pool = SqlitePool::connect_lazy("sqlite::memory:").unwrap();
        let mentor = MentorClient::new(pool.clone(), "test-repo".into());
        let orch = Orchestrator::new(
            pool.clone(),
            mentor,
            AgentFactoryConfig {
                gitlab: None,
                ai: None,
                ai_default_model: "test".into(),
                sandbox_max_memory_mb: 2048,
                pool,
                botto_config: None,
                event_bus: None,
            },
            300,
        );

        let policy = RetryPolicy {
            max_retries: 3,
            backoff: BackoffStrategy::Exponential {
                base_secs: 2,
                max_secs: 30,
            },
            consult_mentor_on_failure: false,
        };

        assert_eq!(orch.compute_backoff(&policy, 1).as_secs(), 2);  // 2 * 2^0
        assert_eq!(orch.compute_backoff(&policy, 2).as_secs(), 4);  // 2 * 2^1
        assert_eq!(orch.compute_backoff(&policy, 3).as_secs(), 8);  // 2 * 2^2
    }

    #[tokio::test]
    async fn backoff_exponential_capped() {
        let pool = SqlitePool::connect_lazy("sqlite::memory:").unwrap();
        let mentor = MentorClient::new(pool.clone(), "test-repo".into());
        let orch = Orchestrator::new(
            pool.clone(),
            mentor,
            AgentFactoryConfig {
                gitlab: None,
                ai: None,
                ai_default_model: "test".into(),
                sandbox_max_memory_mb: 2048,
                pool,
                botto_config: None,
                event_bus: None,
            },
            300,
        );

        let policy = RetryPolicy {
            max_retries: 10,
            backoff: BackoffStrategy::Exponential {
                base_secs: 2,
                max_secs: 10,
            },
            consult_mentor_on_failure: false,
        };

        // 2 * 2^9 = 1024, capped at 10
        assert_eq!(orch.compute_backoff(&policy, 10).as_secs(), 10);
    }

    // -------------------------------------------------------------------
    // DAG validation tests
    // -------------------------------------------------------------------

    use crate::types::workflow::AgentType;

    fn make_step(id: &str, depends_on: Vec<&str>) -> WorkflowStep {
        WorkflowStep {
            id: id.to_string(),
            action: "test".to_string(),
            agent_type: AgentType::Script,
            inputs: HashMap::new(),
            success_criteria: String::new(),
            depends_on: depends_on.into_iter().map(String::from).collect(),
            retry_policy: RetryPolicy::default(),
            timeout_secs: 60,
        }
    }

    #[test]
    fn validate_dag_empty() {
        let result = Orchestrator::validate_dag(&[]);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn validate_dag_single_step() {
        let steps = vec![make_step("a", vec![])];
        let result = Orchestrator::validate_dag(&steps);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), vec!["a"]);
    }

    #[test]
    fn validate_dag_linear_chain() {
        let steps = vec![
            make_step("a", vec![]),
            make_step("b", vec!["a"]),
            make_step("c", vec!["b"]),
        ];
        let result = Orchestrator::validate_dag(&steps);
        assert!(result.is_ok());
        let order = result.unwrap();
        // a must come before b, b before c
        let pos_a = order.iter().position(|s| s == "a").unwrap();
        let pos_b = order.iter().position(|s| s == "b").unwrap();
        let pos_c = order.iter().position(|s| s == "c").unwrap();
        assert!(pos_a < pos_b);
        assert!(pos_b < pos_c);
    }

    #[test]
    fn validate_dag_parallel_steps() {
        let steps = vec![
            make_step("a", vec![]),
            make_step("b", vec![]),
            make_step("c", vec!["a", "b"]),
        ];
        let result = Orchestrator::validate_dag(&steps);
        assert!(result.is_ok());
        let order = result.unwrap();
        let pos_a = order.iter().position(|s| s == "a").unwrap();
        let pos_b = order.iter().position(|s| s == "b").unwrap();
        let pos_c = order.iter().position(|s| s == "c").unwrap();
        assert!(pos_a < pos_c);
        assert!(pos_b < pos_c);
    }

    #[test]
    fn validate_dag_detects_simple_cycle() {
        let steps = vec![
            make_step("a", vec!["b"]),
            make_step("b", vec!["a"]),
        ];
        let result = Orchestrator::validate_dag(&steps);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("cycle"), "error should mention cycle: {err}");
        assert!(err.contains("a") || err.contains("b"));
    }

    #[test]
    fn validate_dag_detects_three_node_cycle() {
        let steps = vec![
            make_step("a", vec!["c"]),
            make_step("b", vec!["a"]),
            make_step("c", vec!["b"]),
        ];
        let result = Orchestrator::validate_dag(&steps);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("cycle"));
    }

    #[test]
    fn validate_dag_detects_missing_dependency() {
        let steps = vec![
            make_step("a", vec![]),
            make_step("b", vec!["nonexistent"]),
        ];
        let result = Orchestrator::validate_dag(&steps);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("nonexistent"));
        assert!(err.contains("does not exist"));
    }

    #[test]
    fn validate_dag_partial_cycle_with_valid_prefix() {
        // a -> b -> c -> b (cycle), but 'a' is valid
        let steps = vec![
            make_step("a", vec![]),
            make_step("b", vec!["a", "c"]),
            make_step("c", vec!["b"]),
        ];
        let result = Orchestrator::validate_dag(&steps);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("cycle"));
    }

    // -------------------------------------------------------------------
    // Dependency failure propagation tests
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn propagate_skips_failed_dependency() {
        let pool = SqlitePool::connect_lazy("sqlite::memory:").unwrap();
        let mentor = MentorClient::new(pool.clone(), "test-repo".into());
        let orch = Orchestrator::new(
            pool.clone(),
            mentor,
            AgentFactoryConfig {
                gitlab: None,
                ai: None,
                ai_default_model: "test".into(),
                sandbox_max_memory_mb: 2048,
                pool,
                botto_config: None,
                event_bus: None,
            },
            300,
        );

        let steps = vec![
            make_step("a", vec![]),
            make_step("b", vec!["a"]),
            make_step("c", vec!["b"]),
        ];

        let mut run = WorkflowRun {
            id: Uuid::new_v4(),
            workflow_id: Uuid::new_v4(),
            trigger: TriggerSource::Manual { user: "test".into() },
            status: RunStatus::Running,
            step_states: HashMap::new(),
            started_at: Utc::now(),
            completed_at: None,
            final_verification: None,
            mentor_queries: Vec::new(),
        };

        // a failed, b and c are pending
        run.step_states.insert("a".into(), StepState::Failed {
            error: "boom".into(),
            retries: 0,
            duration_secs: 1.0,
        });
        run.step_states.insert("b".into(), StepState::Pending);
        run.step_states.insert("c".into(), StepState::Pending);

        orch.propagate_dependency_failures(&steps, &mut run);

        // b should be skipped because a failed
        assert!(matches!(run.step_states.get("b"), Some(StepState::Skipped { .. })));
        // c should be skipped because b was skipped (transitive)
        assert!(matches!(run.step_states.get("c"), Some(StepState::Skipped { .. })));
    }

    #[tokio::test]
    async fn propagate_skips_only_affected_branches() {
        let pool = SqlitePool::connect_lazy("sqlite::memory:").unwrap();
        let mentor = MentorClient::new(pool.clone(), "test-repo".into());
        let orch = Orchestrator::new(
            pool.clone(),
            mentor,
            AgentFactoryConfig {
                gitlab: None,
                ai: None,
                ai_default_model: "test".into(),
                sandbox_max_memory_mb: 2048,
                pool,
                botto_config: None,
                event_bus: None,
            },
            300,
        );

        // Diamond: a -> b, a -> c, b+c -> d
        // If b fails, d should be skipped, but c should remain pending
        let steps = vec![
            make_step("a", vec![]),
            make_step("b", vec!["a"]),
            make_step("c", vec!["a"]),
            make_step("d", vec!["b", "c"]),
        ];

        let mut run = WorkflowRun {
            id: Uuid::new_v4(),
            workflow_id: Uuid::new_v4(),
            trigger: TriggerSource::Manual { user: "test".into() },
            status: RunStatus::Running,
            step_states: HashMap::new(),
            started_at: Utc::now(),
            completed_at: None,
            final_verification: None,
            mentor_queries: Vec::new(),
        };

        run.step_states.insert("a".into(), StepState::Completed {
            output: serde_json::json!({}),
            duration_secs: 1.0,
        });
        run.step_states.insert("b".into(), StepState::Failed {
            error: "boom".into(),
            retries: 0,
            duration_secs: 1.0,
        });
        run.step_states.insert("c".into(), StepState::Pending);
        run.step_states.insert("d".into(), StepState::Pending);

        orch.propagate_dependency_failures(&steps, &mut run);

        // c should still be pending (its only dep 'a' completed)
        assert!(matches!(run.step_states.get("c"), Some(StepState::Pending)));
        // d should be skipped (dep 'b' failed)
        assert!(matches!(run.step_states.get("d"), Some(StepState::Skipped { .. })));
    }
}
