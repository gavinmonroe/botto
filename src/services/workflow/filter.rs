// ---------------------------------------------------------------------------
// Event filter evaluation — matches filter expressions against event payloads.
//
// Filter expressions are simple key-path comparisons used in workflow event
// triggers to narrow which events fire a workflow. Examples:
//
//   "project_path == 'team/repo'"
//   "mr_iid > 100"
//   "labels contains 'urgent'"
//   "source_branch starts_with 'feature/'"
//
// Grammar (intentionally simple — no nested logic):
//   filter     = condition ( ("&&" | "||") condition )*
//   condition  = key_path operator value
//   key_path   = identifier ("." identifier)*
//   operator   = "==" | "!=" | ">" | ">=" | "<" | "<=" | "contains" | "starts_with" | "ends_with"
//   value      = quoted_string | number | "true" | "false" | "null"
// ---------------------------------------------------------------------------

use serde_json::Value;
use tracing::warn;

/// Evaluate a filter expression against an event payload.
/// Returns true if the filter matches (or if the filter is empty/None).
pub fn evaluate(filter: Option<&str>, payload: &Value) -> bool {
    let filter = match filter {
        Some(f) if !f.trim().is_empty() => f.trim(),
        _ => return true, // No filter = always match.
    };

    match parse_and_eval(filter, payload) {
        Ok(result) => result,
        Err(e) => {
            warn!(filter, error = %e, "event filter: evaluation failed, defaulting to no-match");
            false
        }
    }
}

fn parse_and_eval(filter: &str, payload: &Value) -> Result<bool, String> {
    // Split on || first (lower precedence), then && within each group.
    let or_groups: Vec<&str> = split_preserving_strings(filter, "||");

    for or_group in &or_groups {
        let and_conditions: Vec<&str> = split_preserving_strings(or_group.trim(), "&&");
        let mut all_true = true;

        for condition in &and_conditions {
            if !eval_condition(condition.trim(), payload)? {
                all_true = false;
                break;
            }
        }

        if all_true {
            return Ok(true);
        }
    }

    Ok(false)
}

fn eval_condition(condition: &str, payload: &Value) -> Result<bool, String> {
    // Parse: key_path operator value
    //
    // Fix #7: Instead of naive `condition.find(op)` which matches inside
    // identifiers (e.g. "contains" in "container_id") and quoted strings,
    // we split on whitespace respecting quotes, then find the operator token.
    let tokens = tokenize_condition(condition);
    if tokens.len() < 3 {
        return Err(format!("not enough tokens in condition: '{condition}'"));
    }

    let operators = [
        "starts_with",
        "ends_with",
        "contains",
        "!=",
        ">=",
        "<=",
        "==",
        ">",
        "<",
    ];

    // Find the operator token index.
    let op_idx = tokens.iter().position(|t| operators.contains(&t.as_str()));
    let op_idx = match op_idx {
        Some(i) => i,
        None => return Err(format!("no operator found in condition: '{condition}'")),
    };

    if op_idx == 0 || op_idx >= tokens.len() - 1 {
        return Err(format!("operator at invalid position in condition: '{condition}'"));
    }

    // Everything before the operator is the key path (join in case of dotted paths with spaces,
    // though typically it's a single token).
    let key_path = tokens[..op_idx].join(".");
    let key_path = key_path.trim();
    let op = &tokens[op_idx];
    // Everything after the operator is the value (rejoin to handle quoted strings with spaces).
    let value_str = tokens[op_idx + 1..].join(" ");
    let value_str = value_str.trim();

    let actual = resolve_path(key_path, payload);
    let expected = parse_value(value_str)?;

    Ok(compare(&actual, op, &expected))
}

/// Tokenize a condition string, keeping quoted strings as single tokens.
/// "project_path == 'team/repo'" -> ["project_path", "==", "'team/repo'"]
/// "source_branch starts_with 'feature/'" -> ["source_branch", "starts_with", "'feature/'"]
fn tokenize_condition(s: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut in_single_quote = false;
    let mut in_double_quote = false;

    for ch in s.chars() {
        match ch {
            '\'' if !in_double_quote => {
                in_single_quote = !in_single_quote;
                current.push(ch);
            }
            '"' if !in_single_quote => {
                in_double_quote = !in_double_quote;
                current.push(ch);
            }
            c if c.is_whitespace() && !in_single_quote && !in_double_quote => {
                if !current.is_empty() {
                    tokens.push(std::mem::take(&mut current));
                }
            }
            _ => {
                current.push(ch);
            }
        }
    }
    if !current.is_empty() {
        tokens.push(current);
    }
    tokens
}

/// Resolve a dotted key path against a JSON value.
/// "payload.labels" → payload["payload"]["labels"]
fn resolve_path<'a>(path: &str, root: &'a Value) -> Value {
    let mut current = root;
    for key in path.split('.') {
        let key = key.trim();
        match current {
            Value::Object(map) => {
                current = map.get(key).unwrap_or(&Value::Null);
            }
            Value::Array(arr) => {
                if let Ok(idx) = key.parse::<usize>() {
                    current = arr.get(idx).unwrap_or(&Value::Null);
                } else {
                    return Value::Null;
                }
            }
            _ => return Value::Null,
        }
    }
    current.clone()
}

/// Parse a value literal from the filter expression.
fn parse_value(s: &str) -> Result<Value, String> {
    let s = s.trim();

    // Quoted string (single or double quotes).
    if (s.starts_with('\'') && s.ends_with('\'')) || (s.starts_with('"') && s.ends_with('"')) {
        return Ok(Value::String(s[1..s.len() - 1].to_string()));
    }

    // Boolean.
    if s == "true" {
        return Ok(Value::Bool(true));
    }
    if s == "false" {
        return Ok(Value::Bool(false));
    }

    // Null.
    if s == "null" {
        return Ok(Value::Null);
    }

    // Number (integer or float).
    if let Ok(n) = s.parse::<i64>() {
        return Ok(Value::Number(n.into()));
    }
    if let Ok(n) = s.parse::<f64>() {
        if let Some(num) = serde_json::Number::from_f64(n) {
            return Ok(Value::Number(num));
        }
    }

    Err(format!("cannot parse value: '{s}'"))
}

/// Compare two JSON values with the given operator.
fn compare(actual: &Value, op: &str, expected: &Value) -> bool {
    match op {
        "==" => values_equal(actual, expected),
        "!=" => !values_equal(actual, expected),
        ">" => numeric_cmp(actual, expected).is_some_and(|c| c > 0),
        ">=" => numeric_cmp(actual, expected).is_some_and(|c| c >= 0),
        "<" => numeric_cmp(actual, expected).is_some_and(|c| c < 0),
        "<=" => numeric_cmp(actual, expected).is_some_and(|c| c <= 0),
        "contains" => string_op(actual, expected, |a, e| a.contains(e)),
        "starts_with" => string_op(actual, expected, |a, e| a.starts_with(e)),
        "ends_with" => string_op(actual, expected, |a, e| a.ends_with(e)),
        _ => false,
    }
}

fn values_equal(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::String(a), Value::String(b)) => a == b,
        (Value::Number(a), Value::Number(b)) => {
            a.as_f64().zip(b.as_f64()).is_some_and(|(x, y)| (x - y).abs() < f64::EPSILON)
        }
        (Value::Bool(a), Value::Bool(b)) => a == b,
        (Value::Null, Value::Null) => true,
        // Coerce: compare string representation of number to string.
        (Value::Number(n), Value::String(s)) | (Value::String(s), Value::Number(n)) => {
            n.to_string() == *s
        }
        _ => false,
    }
}

fn numeric_cmp(a: &Value, b: &Value) -> Option<i8> {
    let a = a.as_f64()?;
    let b = b.as_f64()?;
    if (a - b).abs() < f64::EPSILON {
        Some(0)
    } else if a > b {
        Some(1)
    } else {
        Some(-1)
    }
}

fn string_op(actual: &Value, expected: &Value, f: impl Fn(&str, &str) -> bool) -> bool {
    match (actual, expected) {
        (Value::String(a), Value::String(e)) => f(a, e),
        // Also check if actual is an array and expected is a string (for "contains").
        (Value::Array(arr), Value::String(e)) => {
            arr.iter().any(|v| v.as_str().is_some_and(|s| s == e.as_str()))
        }
        _ => false,
    }
}

/// Split a string on a delimiter, but not inside quoted strings.
fn split_preserving_strings<'a>(s: &'a str, delimiter: &str) -> Vec<&'a str> {
    let mut parts = Vec::new();
    let mut start = 0;
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let bytes = s.as_bytes();
    let delim_bytes = delimiter.as_bytes();
    let delim_len = delim_bytes.len();

    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'\'' && !in_double_quote {
            in_single_quote = !in_single_quote;
        } else if bytes[i] == b'"' && !in_single_quote {
            in_double_quote = !in_double_quote;
        } else if !in_single_quote && !in_double_quote && i + delim_len <= bytes.len() {
            if &bytes[i..i + delim_len] == delim_bytes {
                parts.push(&s[start..i]);
                start = i + delim_len;
                i += delim_len;
                continue;
            }
        }
        i += 1;
    }
    parts.push(&s[start..]);
    parts
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn empty_filter_matches() {
        assert!(evaluate(None, &json!({})));
        assert!(evaluate(Some(""), &json!({})));
        assert!(evaluate(Some("  "), &json!({})));
    }

    #[test]
    fn string_equality() {
        let payload = json!({"project_path": "team/repo", "status": "opened"});
        assert!(evaluate(Some("project_path == 'team/repo'"), &payload));
        assert!(!evaluate(Some("project_path == 'other/repo'"), &payload));
    }

    #[test]
    fn string_inequality() {
        let payload = json!({"status": "opened"});
        assert!(evaluate(Some("status != 'closed'"), &payload));
        assert!(!evaluate(Some("status != 'opened'"), &payload));
    }

    #[test]
    fn numeric_comparison() {
        let payload = json!({"mr_iid": 42});
        assert!(evaluate(Some("mr_iid > 10"), &payload));
        assert!(evaluate(Some("mr_iid >= 42"), &payload));
        assert!(evaluate(Some("mr_iid == 42"), &payload));
        assert!(!evaluate(Some("mr_iid < 42"), &payload));
        assert!(evaluate(Some("mr_iid <= 42"), &payload));
    }

    #[test]
    fn nested_path() {
        let payload = json!({"author": {"username": "alice"}});
        assert!(evaluate(Some("author.username == 'alice'"), &payload));
        assert!(!evaluate(Some("author.username == 'bob'"), &payload));
    }

    #[test]
    fn string_contains() {
        let payload = json!({"source_branch": "feature/auth-flow"});
        assert!(evaluate(Some("source_branch contains 'auth'"), &payload));
        assert!(!evaluate(Some("source_branch contains 'deploy'"), &payload));
    }

    #[test]
    fn string_starts_with() {
        let payload = json!({"source_branch": "feature/auth-flow"});
        assert!(evaluate(Some("source_branch starts_with 'feature/'"), &payload));
        assert!(!evaluate(Some("source_branch starts_with 'hotfix/'"), &payload));
    }

    #[test]
    fn string_ends_with() {
        let payload = json!({"file_path": "src/main.rs"});
        assert!(evaluate(Some("file_path ends_with '.rs'"), &payload));
        assert!(!evaluate(Some("file_path ends_with '.ts'"), &payload));
    }

    #[test]
    fn array_contains() {
        let payload = json!({"labels": ["urgent", "bug", "frontend"]});
        assert!(evaluate(Some("labels contains 'urgent'"), &payload));
        assert!(!evaluate(Some("labels contains 'backend'"), &payload));
    }

    #[test]
    fn and_conditions() {
        let payload = json!({"status": "opened", "mr_iid": 42});
        assert!(evaluate(
            Some("status == 'opened' && mr_iid > 10"),
            &payload
        ));
        assert!(!evaluate(
            Some("status == 'closed' && mr_iid > 10"),
            &payload
        ));
    }

    #[test]
    fn or_conditions() {
        let payload = json!({"status": "closed"});
        assert!(evaluate(
            Some("status == 'opened' || status == 'closed'"),
            &payload
        ));
        assert!(!evaluate(
            Some("status == 'opened' || status == 'merged'"),
            &payload
        ));
    }

    #[test]
    fn boolean_comparison() {
        let payload = json!({"draft": false});
        assert!(evaluate(Some("draft == false"), &payload));
        assert!(!evaluate(Some("draft == true"), &payload));
    }

    #[test]
    fn null_comparison() {
        let payload = json!({"description": null});
        assert!(evaluate(Some("description == null"), &payload));
        assert!(evaluate(Some("nonexistent == null"), &payload));
    }

    #[test]
    fn invalid_filter_returns_false() {
        let payload = json!({"x": 1});
        assert!(!evaluate(Some("this is not a valid filter"), &payload));
    }

    #[test]
    fn operator_inside_identifier_ignored() {
        // "container_id" contains "contains" as a substring — must not match as operator.
        let payload = json!({"container_id": "abc-123"});
        assert!(evaluate(Some("container_id == 'abc-123'"), &payload));

        // "started_at" contains ">" as... no, but "greater_than" is not an op.
        // More importantly: a value like "'contains_stuff'" should not confuse the parser.
        let payload2 = json!({"status": "not_equal_to_anything"});
        assert!(evaluate(Some("status != 'closed'"), &payload2));
    }

    #[test]
    fn tokenize_condition_basic() {
        let tokens = tokenize_condition("project_path == 'team/repo'");
        assert_eq!(tokens, vec!["project_path", "==", "'team/repo'"]);
    }

    #[test]
    fn tokenize_condition_with_spaces_in_quotes() {
        let tokens = tokenize_condition("name == 'hello world'");
        assert_eq!(tokens, vec!["name", "==", "'hello world'"]);
    }

    #[test]
    fn quoted_strings_in_split() {
        let parts = split_preserving_strings("a == 'x && y' && b == 'z'", "&&");
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0].trim(), "a == 'x && y'");
        assert_eq!(parts[1].trim(), "b == 'z'");
    }
}
