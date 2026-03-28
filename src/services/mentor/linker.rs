// ---------------------------------------------------------------------------
// Mentor Linker — syncs linked repo sets from config into SQLite.
//
// On startup (and config reload), this module reconciles the
// `mentor_repo_links` table with the `[mentor] linked_repos` config.
// The MentorClient's query resolution uses this table to fan out
// searches to sibling repos in linked sets.
// ---------------------------------------------------------------------------

use anyhow::{Context, Result};
use sqlx::SqlitePool;
use tracing::{debug, info};

use crate::config::LinkedRepoSet;

/// Sync linked repo sets from config into the `mentor_repo_links` table.
///
/// This is a full reconciliation: the table is cleared and repopulated.
/// Safe to call on every startup and on config reload.
pub async fn sync_linked_repos(pool: &SqlitePool, linked_repos: &[LinkedRepoSet]) -> Result<()> {
    let now = epoch_secs();

    // Clear existing links and repopulate in a transaction.
    let mut tx = pool.begin().await.context("mentor linker: begin tx")?;

    sqlx::query("DELETE FROM mentor_repo_links")
        .execute(&mut *tx)
        .await
        .context("mentor linker: clear old links")?;

    let mut total = 0usize;
    for set in linked_repos {
        for repo in &set.repos {
            // Fix #11: use INSERT OR IGNORE so duplicate repo paths in config
            // don't crash on the PRIMARY KEY (link_name, repo_path) constraint.
            let result = sqlx::query(
                "INSERT OR IGNORE INTO mentor_repo_links (link_name, repo_path, created_at)
                 VALUES (?, ?, ?)",
            )
            .bind(&set.name)
            .bind(repo)
            .bind(now)
            .execute(&mut *tx)
            .await
            .with_context(|| {
                format!(
                    "mentor linker: insert link {}:{}",
                    set.name, repo
                )
            })?;
            if result.rows_affected() > 0 {
                total += 1;
            } else {
                debug!(
                    link_name = %set.name,
                    repo = %repo,
                    "mentor linker: skipped duplicate repo path"
                );
            }
        }
    }

    tx.commit().await.context("mentor linker: commit tx")?;

    if total > 0 {
        info!(
            sets = linked_repos.len(),
            repos = total,
            "mentor linker: synced linked repo sets"
        );
    } else {
        debug!("mentor linker: no linked repo sets configured");
    }

    Ok(())
}

/// Look up which linked set(s) a repo belongs to.
/// Returns the set names (e.g., ["payments"]).
pub async fn linked_sets_for_repo(pool: &SqlitePool, repo_path: &str) -> Result<Vec<String>> {
    let rows: Vec<(String,)> = sqlx::query_as(
        "SELECT DISTINCT link_name FROM mentor_repo_links WHERE repo_path = ?",
    )
    .bind(repo_path)
    .fetch_all(pool)
    .await
    .context("mentor linker: lookup linked sets")?;

    Ok(rows.into_iter().map(|(name,)| name).collect())
}

/// Get all repos in a linked set by name.
pub async fn repos_in_set(pool: &SqlitePool, link_name: &str) -> Result<Vec<String>> {
    let rows: Vec<(String,)> = sqlx::query_as(
        "SELECT repo_path FROM mentor_repo_links WHERE link_name = ? ORDER BY repo_path",
    )
    .bind(link_name)
    .fetch_all(pool)
    .await
    .context("mentor linker: lookup repos in set")?;

    Ok(rows.into_iter().map(|(repo,)| repo).collect())
}

fn epoch_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    async fn test_pool() -> SqlitePool {
        let pool = SqlitePool::connect("sqlite::memory:")
            .await
            .expect("in-memory db");
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS mentor_repo_links (
                link_name TEXT NOT NULL,
                repo_path TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                PRIMARY KEY (link_name, repo_path)
            )",
        )
        .execute(&pool)
        .await
        .expect("create table");
        pool
    }

    #[tokio::test]
    async fn sync_empty() {
        let pool = test_pool().await;
        sync_linked_repos(&pool, &[]).await.unwrap();

        let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM mentor_repo_links")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count.0, 0);
    }

    #[tokio::test]
    async fn sync_and_lookup() {
        let pool = test_pool().await;
        let sets = vec![
            LinkedRepoSet {
                name: "payments".into(),
                repos: vec!["service-auth".into(), "service-billing".into()],
            },
            LinkedRepoSet {
                name: "frontend".into(),
                repos: vec!["web-app".into(), "design-system".into()],
            },
        ];

        sync_linked_repos(&pool, &sets).await.unwrap();

        // Check total rows
        let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM mentor_repo_links")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count.0, 4);

        // Lookup by repo
        let names = linked_sets_for_repo(&pool, "service-auth").await.unwrap();
        assert_eq!(names, vec!["payments"]);

        // Lookup repos in set
        let repos = repos_in_set(&pool, "payments").await.unwrap();
        assert_eq!(repos, vec!["service-auth", "service-billing"]);

        // Re-sync with different data replaces old
        let new_sets = vec![LinkedRepoSet {
            name: "payments".into(),
            repos: vec!["service-auth".into(), "service-users".into(), "service-billing".into()],
        }];
        sync_linked_repos(&pool, &new_sets).await.unwrap();

        let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM mentor_repo_links")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count.0, 3);

        // frontend set is gone
        let names = linked_sets_for_repo(&pool, "web-app").await.unwrap();
        assert!(names.is_empty());
    }
}
