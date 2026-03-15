// ---------------------------------------------------------------------------
// JSON repair — handles truncated AI responses.
//
// Ported from Otto's ai-service.ts `repairTruncatedJson` and `extractJson`.
// AI models sometimes hit max_tokens mid-JSON. This module attempts to
// recover partial data rather than discarding the entire response.
// ---------------------------------------------------------------------------

/// Try to extract JSON from a string that may contain markdown fences or other wrapping.
pub fn extract_json(input: &str) -> Option<&str> {
    let trimmed = input.trim();

    // Try markdown fences: ```json ... ``` or ``` ... ```
    if let Some(start) = trimmed.find("```json") {
        let after_fence = &trimmed[start + 7..];
        if let Some(end) = after_fence.rfind("```") {
            let inner = after_fence[..end].trim();
            if !inner.is_empty() {
                return Some(inner);
            }
        }
        // No closing fence — take everything after the opening
        let inner = after_fence.trim();
        if !inner.is_empty() {
            return Some(inner);
        }
    }

    if let Some(start) = trimmed.find("```") {
        let after_fence = &trimmed[start + 3..];
        // Skip optional language tag on the same line
        let after_lang = if let Some(nl) = after_fence.find('\n') {
            &after_fence[nl + 1..]
        } else {
            after_fence
        };
        if let Some(end) = after_lang.rfind("```") {
            let inner = after_lang[..end].trim();
            if !inner.is_empty() {
                return Some(inner);
            }
        }
        let inner = after_lang.trim();
        if !inner.is_empty() {
            return Some(inner);
        }
    }

    // Try bare JSON (starts with { or [)
    if trimmed.starts_with('{') || trimmed.starts_with('[') {
        return Some(trimmed);
    }

    // Try to find JSON embedded in text
    if let Some(start) = trimmed.find('{') {
        return Some(&trimmed[start..]);
    }
    if let Some(start) = trimmed.find('[') {
        return Some(&trimmed[start..]);
    }

    None
}

/// Attempt to repair truncated JSON by closing open brackets/braces.
/// Specifically handles arrays of objects cut mid-element.
pub fn repair_truncated_json(input: &str) -> Option<String> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return None;
    }

    // If it already parses, no repair needed
    if serde_json::from_str::<serde_json::Value>(trimmed).is_ok() {
        return Some(trimmed.to_string());
    }

    // Strategy: find the last complete element in an array, truncate there, close brackets.
    let mut result = trimmed.to_string();

    // Track nesting
    let mut in_string = false;
    let mut escape_next = false;
    let mut stack: Vec<char> = Vec::new();
    let mut last_complete_comma = None;

    for (i, ch) in result.char_indices() {
        if escape_next {
            escape_next = false;
            continue;
        }
        if ch == '\\' && in_string {
            escape_next = true;
            continue;
        }
        if ch == '"' {
            in_string = !in_string;
            continue;
        }
        if in_string {
            continue;
        }

        match ch {
            '{' | '[' => stack.push(ch),
            '}' => {
                if stack.last() == Some(&'{') {
                    stack.pop();
                }
            }
            ']' => {
                if stack.last() == Some(&'[') {
                    stack.pop();
                }
            }
            ',' => {
                // A comma at array level (depth 1) marks the end of a complete element
                if stack.len() == 1 && stack[0] == '[' {
                    last_complete_comma = Some(i);
                }
            }
            _ => {}
        }
    }

    // If we have unclosed brackets and a last complete comma, truncate there
    if !stack.is_empty() {
        if let Some(comma_pos) = last_complete_comma {
            result.truncate(comma_pos);
            // Close all remaining open brackets
            for bracket in stack.iter().rev() {
                match bracket {
                    '{' => result.push('}'),
                    '[' => result.push(']'),
                    _ => {}
                }
            }
            // Verify the repair worked
            if serde_json::from_str::<serde_json::Value>(&result).is_ok() {
                return Some(result);
            }
        }

        // Fallback: just close all open brackets
        let fallback = trimmed.to_string();
        // Remove trailing partial content (anything after the last complete value)
        // Try removing characters from the end until we can close brackets
        for trim_len in 0..fallback.len().min(200) {
            let mut attempt = fallback[..fallback.len() - trim_len].to_string();
            // Remove trailing comma if present
            let attempt_trimmed = attempt.trim_end();
            if attempt_trimmed.ends_with(',') {
                attempt = attempt_trimmed[..attempt_trimmed.len() - 1].to_string();
            }
            // Close remaining brackets
            let mut test_stack: Vec<char> = Vec::new();
            let mut test_in_string = false;
            let mut test_escape = false;
            for ch in attempt.chars() {
                if test_escape {
                    test_escape = false;
                    continue;
                }
                if ch == '\\' && test_in_string {
                    test_escape = true;
                    continue;
                }
                if ch == '"' {
                    test_in_string = !test_in_string;
                    continue;
                }
                if test_in_string {
                    continue;
                }
                match ch {
                    '{' | '[' => test_stack.push(ch),
                    '}' => {
                        test_stack.pop();
                    }
                    ']' => {
                        test_stack.pop();
                    }
                    _ => {}
                }
            }
            for bracket in test_stack.iter().rev() {
                match bracket {
                    '{' => attempt.push('}'),
                    '[' => attempt.push(']'),
                    _ => {}
                }
            }
            if serde_json::from_str::<serde_json::Value>(&attempt).is_ok() {
                return Some(attempt);
            }
        }
    }

    None
}

/// Parse AI JSON response with fallback repair strategies.
/// Returns the parsed value or an error description.
pub fn parse_ai_json(input: &str) -> Result<serde_json::Value, String> {
    // 1. Try direct parse
    if let Ok(v) = serde_json::from_str(input) {
        return Ok(v);
    }

    // 2. Try extracting from markdown fences
    if let Some(extracted) = extract_json(input) {
        if let Ok(v) = serde_json::from_str(extracted) {
            return Ok(v);
        }

        // 3. Try repairing truncated JSON
        if let Some(repaired) = repair_truncated_json(extracted) {
            if let Ok(v) = serde_json::from_str(&repaired) {
                return Ok(v);
            }
        }
    }

    Err(format!(
        "failed to parse AI response as JSON (length={})",
        input.len()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_json_fenced() {
        let input = "Here's the result:\n```json\n{\"key\": \"value\"}\n```\nDone.";
        let extracted = extract_json(input).unwrap();
        assert_eq!(extracted, "{\"key\": \"value\"}");
    }

    #[test]
    fn test_extract_json_bare() {
        let input = "{\"key\": \"value\"}";
        let extracted = extract_json(input).unwrap();
        assert_eq!(extracted, "{\"key\": \"value\"}");
    }

    #[test]
    fn test_repair_truncated_array() {
        let input = r#"[{"a":1},{"b":2},{"c":3"#;
        let repaired = repair_truncated_json(input).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&repaired).unwrap();
        assert!(parsed.is_array());
    }

    #[test]
    fn test_parse_ai_json_valid() {
        let input = "{\"summary\": \"test\"}";
        let result = parse_ai_json(input);
        assert!(result.is_ok());
    }
}
