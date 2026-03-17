// ---------------------------------------------------------------------------
// Reviewer preference learning — aggregates accept/dismiss patterns from
// comment_actions into team-wide preferences per project.
//
// These preferences are injected into the AI review prompt so the model
// learns which comment types this team values vs ignores. For example,
// if a team consistently dismisses "style:suggestion" comments, the AI
// will deprioritize those in future reviews.
//
// The aggregation only considers rows with non-NULL category/severity
// (i.e., actions sent by Otto versions that include these fields).
// ---------------------------------------------------------------------------

use anyhow::Result;
use sqlx::SqlitePool;

/// Minimum signals before a category:severity pair is considered meaningful.
const MIN_SIGNALS: i64 = 3;

/// Dismiss rate above which a pair is flagged as "low priority for this team".
const DISMISS_THRESHOLD: f64 = 0.7;

/// Dismiss rate below which a pair is flagged as "high priority for this team".
const ACCEPT_THRESHOLD: f64 = 0.3;

/// Staleness threshold — re-aggregate if prefs are older than this.
const STALE_SECS: i64 = 3600; // 1 hour

/// A single category:severity signal aggregate.
#[derive(Debug)]
pub(crate) struct SignalBucket {
    category: String,
    severity: String,
    accepted: i64,
    dismissed: i64,
}

impl SignalBucket {
    fn total(&self) -> i64 {
        self.accepted + self.dismissed
    }

    fn dismiss_rate(&self) -> f64 {
        if self.total() == 0 {
            return 0.0;
        }
        self.dismissed as f64 / self.total() as f64
    }
}

/// Aggregate accept/dismiss signals from comment_actions for a project.
async fn aggregate_signals(pool: &SqlitePool, project_path: &str) -> Result<Vec<SignalBucket>> {
    let rows: Vec<(String, String, String, i64)> = sqlx::query_as(
        "SELECT category, severity, action, COUNT(*) as cnt
         FROM comment_actions
         WHERE project_path = ? AND category IS NOT NULL AND severity IS NOT NULL
         GROUP BY category, severity, action",
    )
    .bind(project_path)
    .fetch_all(pool)
    .await?;

    // Pivot action rows into SignalBuckets keyed by "category:severity".
    let mut map: std::collections::HashMap<String, SignalBucket> = std::collections::HashMap::new();
    for (category, severity, action, count) in rows {
        let key = format!("{}:{}", category, severity);
        let bucket = map.entry(key).or_insert_with(|| SignalBucket {
            category: category.clone(),
            severity: severity.clone(),
            accepted: 0,
            dismissed: 0,
        });
        match action.as_str() {
            "accepted" => bucket.accepted += count,
            "dismissed" => bucket.dismissed += count,
            _ => {} // ignore unknown actions
        }
    }

    Ok(map.into_values().collect())
}

/// Format aggregated preferences into a prompt section.
/// Returns None if there aren't enough signals to be meaningful.
pub(crate) fn format_team_prefs(buckets: &[SignalBucket]) -> Option<String> {
    let meaningful: Vec<&SignalBucket> = buckets
        .iter()
        .filter(|b| b.total() >= MIN_SIGNALS)
        .collect();

    if meaningful.is_empty() {
        return None;
    }

    let low_priority: Vec<&SignalBucket> = meaningful
        .iter()
        .filter(|b| b.dismiss_rate() >= DISMISS_THRESHOLD)
        .copied()
        .collect();

    let high_priority: Vec<&SignalBucket> = meaningful
        .iter()
        .filter(|b| b.dismiss_rate() <= ACCEPT_THRESHOLD)
        .copied()
        .collect();

    if low_priority.is_empty() && high_priority.is_empty() {
        return None;
    }

    let mut sections = vec!["## Team Review Preferences (learned from past reviews)".to_string()];

    if !low_priority.is_empty() {
        sections.push(
            "This team tends to dismiss these types of comments — only flag them if truly important:"
                .to_string(),
        );
        for b in &low_priority {
            sections.push(format!(
                "- {} ({}): dismissed {}% of the time ({} reviews)",
                b.category,
                b.severity,
                (b.dismiss_rate() * 100.0).round() as i64,
                b.total(),
            ));
        }
    }

    if !high_priority.is_empty() {
        if !low_priority.is_empty() {
            sections.push(String::new());
        }
        sections.push(
            "This team values these types of comments — be thorough here:".to_string(),
        );
        for b in &high_priority {
            sections.push(format!(
                "- {} ({}): accepted {}% of the time ({} reviews)",
                b.category,
                b.severity,
                ((1.0 - b.dismiss_rate()) * 100.0).round() as i64,
                b.total(),
            ));
        }
    }

    Some(sections.join("\n"))
}

/// Get team preferences for a project, using cached prefs if fresh enough.
/// Re-aggregates from comment_actions if stale or missing.
pub async fn get_team_prefs(pool: &SqlitePool, project_path: &str) -> Result<Option<String>> {
    // Check if we have fresh cached prefs
    let now = crate::db::queries::epoch_secs_pub();
    let cached = crate::db::queries::get_reviewer_prefs(pool, project_path).await?;

    if let Some((prefs_text, updated_at)) = cached {
        if now - updated_at < STALE_SECS {
            if prefs_text.is_empty() {
                return Ok(None);
            }
            return Ok(Some(prefs_text));
        }
    }

    // Stale or missing — re-aggregate
    let buckets = aggregate_signals(pool, project_path).await?;
    let formatted = format_team_prefs(&buckets);
    let text = formatted.as_deref().unwrap_or("");

    // Cache the result (even empty string — so we don't re-aggregate every request)
    crate::db::queries::upsert_reviewer_prefs(pool, project_path, text).await?;

    Ok(formatted)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_empty_buckets() {
        assert!(format_team_prefs(&[]).is_none());
    }

    #[test]
    fn test_format_below_min_signals() {
        let buckets = vec![SignalBucket {
            category: "style".into(),
            severity: "suggestion".into(),
            accepted: 1,
            dismissed: 1,
        }];
        assert!(format_team_prefs(&buckets).is_none());
    }

    #[test]
    fn test_format_low_priority() {
        let buckets = vec![SignalBucket {
            category: "style".into(),
            severity: "suggestion".into(),
            accepted: 1,
            dismissed: 9,
        }];
        let text = format_team_prefs(&buckets).unwrap();
        assert!(text.contains("Team Review Preferences"));
        assert!(text.contains("style (suggestion): dismissed 90%"));
        assert!(text.contains("10 reviews"));
    }

    #[test]
    fn test_format_high_priority() {
        let buckets = vec![SignalBucket {
            category: "bug".into(),
            severity: "critical".into(),
            accepted: 9,
            dismissed: 1,
        }];
        let text = format_team_prefs(&buckets).unwrap();
        assert!(text.contains("values these types"));
        assert!(text.contains("bug (critical): accepted 90%"));
    }

    #[test]
    fn test_format_mixed() {
        let buckets = vec![
            SignalBucket {
                category: "style".into(),
                severity: "suggestion".into(),
                accepted: 1,
                dismissed: 9,
            },
            SignalBucket {
                category: "bug".into(),
                severity: "critical".into(),
                accepted: 9,
                dismissed: 1,
            },
            // This one is in the middle — neither high nor low priority
            SignalBucket {
                category: "performance".into(),
                severity: "warning".into(),
                accepted: 5,
                dismissed: 5,
            },
        ];
        let text = format_team_prefs(&buckets).unwrap();
        assert!(text.contains("style (suggestion)"));
        assert!(text.contains("bug (critical)"));
        assert!(!text.contains("performance"));
    }
}
