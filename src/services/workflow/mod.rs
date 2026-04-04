// ---------------------------------------------------------------------------
// Workflow agents — stateless workers spawned by the orchestrator for each
// workflow step.
//
// All agents implement the `WorkflowAgent` trait. The orchestrator doesn't
// care about the concrete type — it dispatches by `AgentType`.
// ---------------------------------------------------------------------------

pub mod ai;
pub mod coding;
pub mod composite;
pub mod connector;
pub mod crud;
pub mod decomposer;
pub mod escalation;
pub mod evaluator;
pub mod factory;
pub mod filter;
pub mod gitlab;
pub mod http;
pub mod generator;
pub mod orchestrator;
pub mod planner;
pub mod registry;
pub mod sandbox;
pub mod scheduler;
pub mod script;
pub mod session;
pub mod traits;
pub mod verification;
