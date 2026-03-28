// ---------------------------------------------------------------------------
// Mentor service — institutional memory that gets smarter with every
// workflow run.
//
// Backed by SQLite + FTS5 for full-text search. Knowledge is scoped per-repo,
// per-linked-set, or global. Entries have a confidence score that decays over
// time if never queried, enabling automatic self-pruning.
//
// The MentorClient is the primary interface for agents and the orchestrator.
// ---------------------------------------------------------------------------

pub mod client;
pub mod linker;
pub mod pruner;
