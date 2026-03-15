// ---------------------------------------------------------------------------
// Harness judge — AI-powered prompt mutation, analysis, and evolution.
//
// The judge uses Claude Opus 4.6 to:
//   1. Generate prompt mutations from a parent variant
//   2. Mutate code params (temperature, max_tokens, trim thresholds)
//   3. Analyze round results and extract learnings
//   4. Decide mutation strategy based on improvement trends
//
// The judge reads the summary.md file for context on past rounds so it
// can make informed decisions about what to try next.
// ---------------------------------------------------------------------------

use crate::config::BottoConfig;
use crate::services::ai::client::{
    self as ai_client, AiClientConfig, ChatCompletionRequest, ChatMessage,
};
use crate::services::harness::prompts::{self, validate_variant};
use crate::services::harness::types::{
    AgentParams, CodeParams, HarnessGrade, PromptMetadata, PromptVariant, RoundReport,
    VariantScore,
};
use anyhow::{Context, Result};
use chrono::Utc;
use tracing::warn;

/// Generate N prompt variant mutations from a parent variant.
/// One of the N variants is always the unchanged parent (control).
pub async fn generate_mutations(
    cfg: &BottoConfig,
    parent: &PromptVariant,
    count: u32,
    round: u32,
    history_summary: &str,
) -> Result<Vec<PromptVariant>> {
    let mut variants = Vec::with_capacity(count as usize);

    // Slot 0: unchanged parent as control
    let mut control = parent.clone();
    control.id = prompts::next_variant_id(&[parent.id.clone()]);
    control.generation = round;
    control.parent_id = Some(parent.id.clone());
    control.metadata = PromptMetadata {
        author: "control".into(),
        created_at: Utc::now(),
        notes: format!("Unchanged control copy of {}", parent.id),
        mutation_strategy: Some("control".into()),
    };
    let mut used_ids = vec![parent.id.clone(), control.id.clone()];
    variants.push(control);

    // Remaining slots: AI-generated mutations
    let mutations_needed = (count as usize).saturating_sub(1);
    if mutations_needed == 0 {
        return Ok(variants);
    }

    let ai_cfg = AiClientConfig {
        base_url: cfg.ai.base_url.clone(),
        api_key: cfg.ai.api_key.clone(),
    };

    let system_prompt = build_mutation_system_prompt(parent, history_summary);

    for i in 0..mutations_needed {
        let strategy = match i % 4 {
            0 => "structural",
            1 => "tonal",
            2 => "strategic",
            3 => "params",
            _ => unreachable!(),
        };

        let user_prompt = build_mutation_user_prompt(parent, strategy, i, mutations_needed);

        let request = ChatCompletionRequest {
            model: cfg.harness.judge_model.clone(),
            messages: vec![
                ChatMessage {
                    role: "system".into(),
                    content: Some(system_prompt.clone()),
                    tool_calls: None,
                    tool_call_id: None,
                },
                ChatMessage {
                    role: "user".into(),
                    content: Some(user_prompt),
                    tool_calls: None,
                    tool_call_id: None,
                },
            ],
            temperature: Some(0.7), // Higher creativity for mutations
            max_tokens: Some(4000),
            stream: None,
            tools: None,
            tool_choice: None,
        };

        match ai_client::chat_completion(&ai_cfg, request).await {
            Ok(resp) => {
                let text = resp
                    .choices
                    .first()
                    .and_then(|c| c.message.content.as_ref())
                    .map(|s| s.trim().to_string())
                    .unwrap_or_default();

                match parse_mutation_response(&text, parent, round, &used_ids, strategy) {
                    Ok(variant) => {
                        let errors = validate_variant(&variant);
                        if errors.is_empty() {
                            used_ids.push(variant.id.clone());
                            variants.push(variant);
                        } else {
                            warn!(
                                "judge produced invalid variant ({}): {:?}",
                                strategy, errors
                            );
                            // Fall back to a param-only mutation
                            let fallback = param_only_mutation(parent, round, &used_ids, i);
                            used_ids.push(fallback.id.clone());
                            variants.push(fallback);
                        }
                    }
                    Err(e) => {
                        warn!("failed to parse judge mutation ({}): {}", strategy, e);
                        let fallback = param_only_mutation(parent, round, &used_ids, i);
                        used_ids.push(fallback.id.clone());
                        variants.push(fallback);
                    }
                }
            }
            Err(e) => {
                warn!("judge AI call failed for mutation {}: {}", i, e);
                let fallback = param_only_mutation(parent, round, &used_ids, i);
                used_ids.push(fallback.id.clone());
                variants.push(fallback);
            }
        }
    }

    Ok(variants)
}

/// Analyze round results and produce a judge analysis + learnings.
pub async fn analyze_round(
    cfg: &BottoConfig,
    report: &RoundReport,
    history_summary: &str,
) -> Result<(String, Vec<String>)> {
    let ai_cfg = AiClientConfig {
        base_url: cfg.ai.base_url.clone(),
        api_key: cfg.ai.api_key.clone(),
    };

    let system = "You are an expert prompt engineer analyzing the results of a prompt evolution \
                  experiment for an AI-powered code fix system. Your job is to identify patterns \
                  in what makes prompts effective at guiding an AI to fix real code issues across \
                  multiple programming languages.\n\n\
                  Be specific and actionable. Focus on concrete observations, not vague generalities.";

    let user = format!(
        "Here are the results from round {} of prompt evolution:\n\n\
         ## Variant Scores\n{}\n\n\
         ## Detailed Grades\n{}\n\n\
         ## History\n{}\n\n\
         Provide your analysis in this exact format:\n\
         ANALYSIS:\n<your analysis of what worked and what didn't, 2-4 paragraphs>\n\n\
         LEARNINGS:\n- <learning 1>\n- <learning 2>\n- <learning 3>\n\
         (list 3-6 specific, actionable learnings)",
        report.round,
        format_variant_scores(&report.variant_scores),
        format_grades(&report.grades),
        if history_summary.is_empty() {
            "No prior rounds.".to_string()
        } else {
            history_summary.to_string()
        },
    );

    let request = ChatCompletionRequest {
        model: cfg.harness.judge_model.clone(),
        messages: vec![
            ChatMessage {
                role: "system".into(),
                content: Some(system.into()),
                tool_calls: None,
                tool_call_id: None,
            },
            ChatMessage {
                role: "user".into(),
                content: Some(user),
                tool_calls: None,
                tool_call_id: None,
            },
        ],
        temperature: Some(0.3),
        max_tokens: Some(2000),
        stream: None,
        tools: None,
        tool_choice: None,
    };

    match ai_client::chat_completion(&ai_cfg, request).await {
        Ok(resp) => {
            let text = resp
                .choices
                .first()
                .and_then(|c| c.message.content.as_ref())
                .map(|s| s.trim().to_string())
                .unwrap_or_default();

            let (analysis, learnings) = parse_analysis_response(&text);
            Ok((analysis, learnings))
        }
        Err(e) => {
            warn!("judge analysis AI call failed: {}", e);
            Ok((
                format!("Analysis unavailable (AI error: {})", e),
                vec!["Judge AI call failed — no learnings extracted".into()],
            ))
        }
    }
}

// ---------------------------------------------------------------------------
// Prompt builders
// ---------------------------------------------------------------------------

fn build_mutation_system_prompt(parent: &PromptVariant, history: &str) -> String {
    format!(
        "You are an expert prompt engineer evolving system prompts for an AI-powered code fix system.\n\n\
         The system has 3 AI agent loops inside Docker containers:\n\
         1. **Setup agent**: Sets up a project environment (installs deps, configures env)\n\
         2. **Fix agent**: Autonomously fixes code to make tests pass after a review suggestion is applied\n\
         3. **Retry agent**: Fixes environment/command failures during the pipeline\n\n\
         Each agent communicates via shell commands. The AI responds with either a command, a signal \
         (SETUP_DONE/RUN_TESTS/UNFIXABLE), or nothing.\n\n\
         The goal is to make these agents succeed at fixing real code issues across ALL programming \
         languages (Go, Python, Ruby, TypeScript, JavaScript, Rust, Java, etc.) and ALL issue types \
         (bugs, race conditions, security, performance, error handling, etc.).\n\n\
         CRITICAL RULES for mutations:\n\
         - Setup prompt MUST contain placeholders: {{project}}, {{file_path}}, {{test_cmd}}\n\
         - Fix prompt MUST contain placeholders: {{context}}, {{original}}, {{suggestion}}, {{test_cmd}}\n\
         - Retry prompt MUST contain placeholder: {{context}}\n\
         - Agents MUST respond with commands, signals, or UNFIXABLE — never explanations\n\
         - Do NOT remove the core protocol (command/signal/UNFIXABLE response format)\n\n\
         ## Current parent prompt (generation {})\n\n\
         ### Setup prompt:\n```\n{}\n```\n\n\
         ### Fix prompt:\n```\n{}\n```\n\n\
         ### Retry prompt:\n```\n{}\n```\n\n\
         ### Current code params:\n\
         - Setup: temperature={}, max_tokens={}\n\
         - Fix: temperature={}, max_tokens={}\n\
         - Retry: temperature={}, max_tokens={}\n\
         - History trim: threshold={}, keep={}\n\n\
         ## Evolution history\n{}\n",
        parent.generation,
        parent.setup_prompt,
        parent.fix_prompt,
        parent.retry_prompt,
        parent.code_params.setup.temperature,
        parent.code_params.setup.max_tokens,
        parent.code_params.fix.temperature,
        parent.code_params.fix.max_tokens,
        parent.code_params.retry.temperature,
        parent.code_params.retry.max_tokens,
        parent.code_params.history_trim_threshold,
        parent.code_params.history_keep_count,
        if history.is_empty() {
            "No prior rounds yet.".to_string()
        } else {
            history.to_string()
        },
    )
}

fn build_mutation_user_prompt(
    _parent: &PromptVariant,
    strategy: &str,
    index: usize,
    total: usize,
) -> String {
    let strategy_instruction = match strategy {
        "structural" => {
            "STRUCTURAL mutation: Reorganize the prompt structure. Try reordering instructions, \
             adding/removing sections, changing the information hierarchy, or restructuring how \
             context is presented. The core protocol must remain intact."
        }
        "tonal" => {
            "TONAL mutation: Change the communication style. Try being more/less directive, \
             more/less detailed, adding urgency, emphasizing different priorities, or changing \
             how errors should be approached. Keep the same structure but change the voice."
        }
        "strategic" => {
            "STRATEGIC mutation: Change the problem-solving approach. Try encouraging different \
             debugging strategies (read before write, test incrementally, check env first), \
             adding language-specific hints, or changing how the agent should prioritize actions. \
             Think about what makes a senior engineer effective across many languages."
        }
        "params" => {
            "PARAMS mutation: Focus on changing the code parameters (temperature, max_tokens, \
             history trim settings). You may also make minor prompt tweaks to complement the \
             param changes. Consider: higher temperature for more creative problem-solving, \
             more tokens for complex explanations, different trim thresholds for longer context."
        }
        _ => "General mutation: improve the prompt in any way you see fit.",
    };

    format!(
        "Generate mutation {} of {} using this strategy:\n\n{}\n\n\
         Respond in this EXACT format (no other text):\n\n\
         SETUP_PROMPT:\n```\n<the full setup prompt template>\n```\n\n\
         FIX_PROMPT:\n```\n<the full fix prompt template>\n```\n\n\
         RETRY_PROMPT:\n```\n<the full retry prompt template>\n```\n\n\
         PARAMS:\n\
         setup_temperature=<float>\n\
         setup_max_tokens=<int>\n\
         fix_temperature=<float>\n\
         fix_max_tokens=<int>\n\
         retry_temperature=<float>\n\
         retry_max_tokens=<int>\n\
         history_trim_threshold=<int>\n\
         history_keep_count=<int>\n\n\
         NOTES:\n<1-2 sentences explaining what you changed and why>",
        index + 1,
        total,
        strategy_instruction,
    )
}

// ---------------------------------------------------------------------------
// Response parsers
// ---------------------------------------------------------------------------

fn parse_mutation_response(
    text: &str,
    parent: &PromptVariant,
    round: u32,
    used_ids: &[String],
    strategy: &str,
) -> Result<PromptVariant> {
    // Extract prompts between ``` fences after each header
    let setup = extract_fenced_block(text, "SETUP_PROMPT:")
        .context("missing SETUP_PROMPT block")?;
    let fix = extract_fenced_block(text, "FIX_PROMPT:")
        .context("missing FIX_PROMPT block")?;
    let retry = extract_fenced_block(text, "RETRY_PROMPT:")
        .context("missing RETRY_PROMPT block")?;

    // Parse params
    let params = extract_section(text, "PARAMS:");
    let code_params = if let Some(params_text) = params {
        parse_code_params(&params_text, &parent.code_params)
    } else {
        parent.code_params.clone()
    };

    // Parse notes
    let notes = extract_section(text, "NOTES:")
        .unwrap_or_else(|| format!("{} mutation", strategy));

    let id = prompts::next_variant_id(used_ids);

    Ok(PromptVariant {
        id,
        generation: round,
        parent_id: Some(parent.id.clone()),
        setup_prompt: setup,
        fix_prompt: fix,
        retry_prompt: retry,
        code_params,
        metadata: PromptMetadata {
            author: "judge".into(),
            created_at: Utc::now(),
            notes,
            mutation_strategy: Some(strategy.into()),
        },
    })
}

fn extract_fenced_block(text: &str, header: &str) -> Option<String> {
    let header_pos = text.find(header)?;
    let after_header = &text[header_pos + header.len()..];

    // Find the opening ```
    let fence_start = after_header.find("```")?;
    let after_fence = &after_header[fence_start + 3..];

    // Skip optional language identifier on the first line
    let content_start = after_fence.find('\n').map(|p| p + 1).unwrap_or(0);
    let content = &after_fence[content_start..];

    // Find the closing ```
    let fence_end = content.find("```")?;
    Some(content[..fence_end].trim().to_string())
}

fn extract_section(text: &str, header: &str) -> Option<String> {
    let pos = text.find(header)?;
    let after = &text[pos + header.len()..];

    // Take until the next section header or end of text
    let end = after
        .find("\nSETUP_PROMPT:")
        .or_else(|| after.find("\nFIX_PROMPT:"))
        .or_else(|| after.find("\nRETRY_PROMPT:"))
        .or_else(|| after.find("\nPARAMS:"))
        .or_else(|| after.find("\nNOTES:"))
        .unwrap_or(after.len());

    let section = after[..end].trim();
    if section.is_empty() {
        None
    } else {
        Some(section.to_string())
    }
}

fn parse_code_params(text: &str, defaults: &CodeParams) -> CodeParams {
    let get = |key: &str| -> Option<&str> {
        text.lines()
            .find(|l| l.starts_with(key))
            .and_then(|l| l.split('=').nth(1))
            .map(|v| v.trim())
    };

    CodeParams {
        setup: AgentParams {
            temperature: get("setup_temperature")
                .and_then(|v| v.parse().ok())
                .unwrap_or(defaults.setup.temperature),
            max_tokens: get("setup_max_tokens")
                .and_then(|v| v.parse().ok())
                .unwrap_or(defaults.setup.max_tokens),
        },
        fix: AgentParams {
            temperature: get("fix_temperature")
                .and_then(|v| v.parse().ok())
                .unwrap_or(defaults.fix.temperature),
            max_tokens: get("fix_max_tokens")
                .and_then(|v| v.parse().ok())
                .unwrap_or(defaults.fix.max_tokens),
        },
        retry: AgentParams {
            temperature: get("retry_temperature")
                .and_then(|v| v.parse().ok())
                .unwrap_or(defaults.retry.temperature),
            max_tokens: get("retry_max_tokens")
                .and_then(|v| v.parse().ok())
                .unwrap_or(defaults.retry.max_tokens),
        },
        history_trim_threshold: get("history_trim_threshold")
            .and_then(|v| v.parse().ok())
            .unwrap_or(defaults.history_trim_threshold),
        history_keep_count: get("history_keep_count")
            .and_then(|v| v.parse().ok())
            .unwrap_or(defaults.history_keep_count),
    }
}

fn parse_analysis_response(text: &str) -> (String, Vec<String>) {
    let analysis = extract_section(text, "ANALYSIS:")
        .unwrap_or_else(|| text.to_string());

    let learnings_text = extract_section(text, "LEARNINGS:");
    let learnings = learnings_text
        .map(|t| {
            t.lines()
                .filter(|l| l.starts_with("- ") || l.starts_with("* "))
                .map(|l| l.trim_start_matches("- ").trim_start_matches("* ").trim().to_string())
                .filter(|l| !l.is_empty())
                .collect()
        })
        .unwrap_or_default();

    (analysis, learnings)
}

// ---------------------------------------------------------------------------
// Fallback mutations (when AI fails)
// ---------------------------------------------------------------------------

/// Simple param-only mutation that doesn't require AI.
/// Nudges temperature and max_tokens slightly from the parent.
fn param_only_mutation(
    parent: &PromptVariant,
    round: u32,
    used_ids: &[String],
    index: usize,
) -> PromptVariant {
    let mut params = parent.code_params.clone();

    // Deterministic but varied nudges based on index
    match index % 4 {
        0 => {
            // Slightly higher fix temperature
            params.fix.temperature = (params.fix.temperature + 0.05).min(1.0);
        }
        1 => {
            // More tokens for fix agent
            params.fix.max_tokens = (params.fix.max_tokens + 500).min(4000);
        }
        2 => {
            // Lower setup temperature (more deterministic)
            params.setup.temperature = (params.setup.temperature - 0.02).max(0.0);
        }
        3 => {
            // Larger history window
            params.history_trim_threshold += 10;
            params.history_keep_count += 8;
        }
        _ => unreachable!(),
    }

    let id = prompts::next_variant_id(used_ids);

    PromptVariant {
        id,
        generation: round,
        parent_id: Some(parent.id.clone()),
        setup_prompt: parent.setup_prompt.clone(),
        fix_prompt: parent.fix_prompt.clone(),
        retry_prompt: parent.retry_prompt.clone(),
        code_params: params,
        metadata: PromptMetadata {
            author: "fallback".into(),
            created_at: Utc::now(),
            notes: format!("Param-only fallback mutation (index {})", index),
            mutation_strategy: Some("params_fallback".into()),
        },
    }
}

// ---------------------------------------------------------------------------
// Formatting helpers
// ---------------------------------------------------------------------------

fn format_variant_scores(scores: &[VariantScore]) -> String {
    let mut out = String::new();
    out.push_str("| Variant | Mean Score | Pass Rate | Mean Iters |\n");
    out.push_str("|---------|-----------|-----------|------------|\n");
    for s in scores {
        out.push_str(&format!(
            "| {} | {:.1} | {}/{} | {:.1} |\n",
            s.variant_id, s.mean_score, s.pass_count, s.total_cases, s.mean_iterations,
        ));
    }
    out
}

fn format_grades(grades: &[HarnessGrade]) -> String {
    let mut out = String::new();
    out.push_str("| Variant | Test Case | Pass | Iters | Time | Score |\n");
    out.push_str("|---------|-----------|------|-------|------|-------|\n");
    for g in grades {
        out.push_str(&format!(
            "| {} | {} | {} | {} | {:.1}s | {:.1} |\n",
            g.variant_id,
            g.test_case_id,
            if g.passed { "yes" } else { "no" },
            g.iterations,
            g.wall_time_secs,
            g.score,
        ));
    }
    out
}
