// ---------------------------------------------------------------------------
// Harness — Self-evolving prompt engineering test harness.
//
// Spins up N parallel sandbox instances with different prompt variants,
// grades their performance on real GitLab MR test cases, and iteratively
// evolves the prompts toward better fix success rates.
//
// The loop:
//   1. Judge generates test cases from real GitLab MRs
//   2. N harness instances run in parallel with different prompt variants
//   3. Each runs the sandbox fix pipeline (push disabled)
//   4. Judge grades results (pass/fail, iterations, quality)
//   5. Best prompt wins → saved to memory
//   6. Judge mutates the winning prompt → next round
//   7. Repeat until convergence or max rounds
// ---------------------------------------------------------------------------

pub mod grader;
pub mod judge;
pub mod memory;
pub mod orchestrator;
pub mod prompts;
pub mod runner;
pub mod test_case;
pub mod types;
