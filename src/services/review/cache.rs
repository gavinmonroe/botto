// ---------------------------------------------------------------------------
// Review cache — SQLite-backed, gzip-compressed, diff-hash-keyed.
//
// Ported from Otto's review-cache.ts. Same cache key structure:
//   project_path + mr_iid + diff_hash (djb2 of all diffs combined)
//
// Supports:
//   - Exact match: same diff_hash → full cache hit
//   - Latest match: any diff_hash for same MR → incremental re-review
//     (per-file diff hashes compared to skip unchanged files)
//   - TTL expiration + max entry eviction
//   - Gzip compression for storage efficiency
// ---------------------------------------------------------------------------

use crate::db;
use crate::types::review::CachedReview;
use crate::util::hash;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use sqlx::SqlitePool;
use std::collections::HashMap;
use std::io::{Read, Write};
use tracing::{debug, warn};

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Compute the cache key hash from all diffs in the MR.
pub fn compute_diff_hash(diffs: &[&str]) -> String {
    let combined: String = diffs.join("\n---\n");
    hash::djb2(&combined)
}

/// Compute per-file diff hashes for incremental re-review.
pub fn compute_file_diff_hashes(files: &[(&str, &str)]) -> HashMap<String, String> {
    hash::compute_file_diff_hashes(files)
}

/// Try to load a cached review with an exact diff hash match.
pub async fn load_exact(
    pool: &SqlitePool,
    project_path: &str,
    mr_iid: u64,
    diff_hash: &str,
) -> Option<(CachedReview, HashMap<String, String>)> {
    match db::queries::get_cached_review(pool, project_path, mr_iid as i64, diff_hash).await {
        Ok(Some(row)) => {
            let data: Vec<u8> = row.0;
            let file_hashes_json: String = row.1;
            decode_cached_review(&data, &file_hashes_json)
        }
        Ok(None) => None,
        Err(e) => {
            warn!("cache read error: {}", e);
            None
        }
    }
}

/// Load the most recent cached review for an MR regardless of diff hash.
/// Returns the review + per-file hashes + the diff hash it was stored under.
/// Used for incremental re-review: compare per-file hashes to skip unchanged files.
pub async fn load_latest(
    pool: &SqlitePool,
    project_path: &str,
    mr_iid: u64,
) -> Option<(CachedReview, HashMap<String, String>, String)> {
    match db::queries::get_latest_cached_review(pool, project_path, mr_iid as i64).await {
        Ok(Some(row)) => {
            let data: Vec<u8> = row.0;
            let file_hashes_json: String = row.1;
            let diff_hash: String = row.2;
            decode_cached_review(&data, &file_hashes_json)
                .map(|(review, hashes)| (review, hashes, diff_hash))
        }
        Ok(None) => None,
        Err(e) => {
            warn!("cache read error (latest): {}", e);
            None
        }
    }
}

/// Save a review to the cache. Compresses with gzip before storing.
pub async fn save(
    pool: &SqlitePool,
    project_path: &str,
    mr_iid: u64,
    diff_hash: &str,
    review: &CachedReview,
    file_diff_hashes: &HashMap<String, String>,
    ttl_days: u32,
    max_per_project: u32,
) {
    let data = match encode_cached_review(review) {
        Some(d) => d,
        None => {
            warn!("failed to encode cached review for {}:{}", project_path, mr_iid);
            return;
        }
    };

    let file_hashes_json = serde_json::to_string(file_diff_hashes).unwrap_or_default();

    if let Err(e) = db::queries::save_cached_review(
        pool,
        project_path,
        mr_iid as i64,
        diff_hash,
        &data,
        &file_hashes_json,
        ttl_days,
    )
    .await
    {
        warn!("cache save error: {}", e);
        return;
    }

    // Evict old entries
    if let Err(e) = db::queries::evict_old_reviews(pool, project_path, max_per_project).await {
        warn!("cache eviction error: {}", e);
    }

    debug!(
        "cached review for {}:!{} (hash={})",
        project_path, mr_iid, diff_hash
    );
}

/// Delete all cached reviews for a specific MR (used for forced regeneration).
pub async fn delete(pool: &SqlitePool, project_path: &str, mr_iid: u64) {
    if let Err(e) = sqlx::query(
        "DELETE FROM review_cache WHERE project_path = ? AND mr_iid = ?",
    )
    .bind(project_path)
    .bind(mr_iid as i64)
    .execute(pool)
    .await
    {
        warn!("cache delete error: {}", e);
    }
}

/// Determine which files need re-review by comparing current per-file hashes
/// against the previously cached hashes. Returns file paths that changed.
pub fn changed_files(
    current_hashes: &HashMap<String, String>,
    cached_hashes: &HashMap<String, String>,
) -> Vec<String> {
    current_hashes
        .iter()
        .filter_map(|(path, hash)| {
            match cached_hashes.get(path) {
                Some(cached_hash) if cached_hash == hash => None, // unchanged
                _ => Some(path.clone()),                          // new or changed
            }
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Compression helpers
// ---------------------------------------------------------------------------

fn encode_cached_review(review: &CachedReview) -> Option<Vec<u8>> {
    let json = serde_json::to_vec(review).ok()?;
    let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
    encoder.write_all(&json).ok()?;
    encoder.finish().ok()
}

fn decode_cached_review(
    data: &[u8],
    file_hashes_json: &str,
) -> Option<(CachedReview, HashMap<String, String>)> {
    // Try gzip decompression, fall back to raw
    let json_bytes = {
        let mut decoder = GzDecoder::new(data);
        let mut decompressed = Vec::new();
        match decoder.read_to_end(&mut decompressed) {
            Ok(_) => decompressed,
            Err(_) => data.to_vec(),
        }
    };

    let review: CachedReview = serde_json::from_slice(&json_bytes).ok()?;
    let file_hashes: HashMap<String, String> =
        serde_json::from_str(file_hashes_json).unwrap_or_default();

    Some((review, file_hashes))
}
