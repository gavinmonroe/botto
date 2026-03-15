// ---------------------------------------------------------------------------
// Diff hashing — djb2 variant, matching Otto's implementation.
//
// Used for cache key generation (whole-MR hash) and incremental re-review
// (per-file hash comparison). Non-cryptographic, fast, base-36 encoded.
// ---------------------------------------------------------------------------

/// Compute a djb2 hash of the input string, returned as base-36.
/// Matches Otto's `computeDiffHash` in review-cache.ts.
pub fn djb2(input: &str) -> String {
    let mut hash: u64 = 5381;
    for byte in input.bytes() {
        // hash * 33 + byte
        hash = hash.wrapping_mul(33).wrapping_add(byte as u64);
    }
    format!("{}", radix_fmt(hash))
}

/// Compute a combined hash of all diffs (for the cache key).
pub fn compute_diff_hash(diffs: &[&str]) -> String {
    let combined: String = diffs.join("\n---\n");
    djb2(&combined)
}

/// Compute per-file diff hashes (for incremental re-review).
pub fn compute_file_diff_hashes(
    files: &[(&str, &str)],
) -> std::collections::HashMap<String, String> {
    files
        .iter()
        .map(|(path, diff)| (path.to_string(), djb2(diff)))
        .collect()
}

/// Simple base-36 encoding for u64.
fn radix_fmt(mut n: u64) -> String {
    if n == 0 {
        return "0".to_string();
    }
    const CHARS: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    let mut result = Vec::new();
    while n > 0 {
        result.push(CHARS[(n % 36) as usize]);
        n /= 36;
    }
    result.reverse();
    String::from_utf8(result).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_djb2_deterministic() {
        let a = djb2("hello world");
        let b = djb2("hello world");
        assert_eq!(a, b);
    }

    #[test]
    fn test_djb2_different_inputs() {
        let a = djb2("hello");
        let b = djb2("world");
        assert_ne!(a, b);
    }

    #[test]
    fn test_empty_string() {
        let h = djb2("");
        assert_eq!(h, radix_fmt(5381));
    }
}
