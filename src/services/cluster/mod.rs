// ---------------------------------------------------------------------------
// Cross-MR Cluster service — groups related MRs by ticket or file overlap.
//
// Two clustering strategies are composed by the ClusterDetector:
//   1. TicketClusterStrategy — groups MRs sharing a Jira/ticket key (strong signal)
//   2. FileOverlapStrategy — groups MRs with overlapping changed files (weaker signal)
//
// Clusters are persisted to SQLite and broadcast to connected Otto clients.
// AI-generated summaries and review orders are computed on demand, not eagerly.
// ---------------------------------------------------------------------------

pub mod detector;
pub mod strategies;
pub mod summary;
