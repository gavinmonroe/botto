// ---------------------------------------------------------------------------
// Conflict Radar — detects overlapping changes across in-flight MRs.
//
// Pure computation: queries the mr_changed_files index, compares hunk ranges,
// and enriches with MR metadata from GitLab. No AI involved in the core path.
//
// The optional SemanticConflictAnalyzer (in semantic.rs) adds AI-powered
// analysis for high-severity conflicts when enabled.
// ---------------------------------------------------------------------------

pub mod detector;
pub mod semantic;
