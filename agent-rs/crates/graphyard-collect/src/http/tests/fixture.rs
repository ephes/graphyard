//! `fixtures/http_errors.json` (`fixtures/gen_http_errors.py`).

use std::sync::OnceLock;

use serde_json::Value;

const TEXT: &str = include_str!("../../../../../fixtures/http_errors.json");

/// The parsed fixture.
pub(crate) fn fixture() -> &'static Value {
    static FIXTURE: OnceLock<Value> = OnceLock::new();
    FIXTURE.get_or_init(|| serde_json::from_str(TEXT).expect("http_errors.json is valid JSON"))
}

/// A section that is a list.
pub(crate) fn cases(section: &str) -> &'static [Value] {
    fixture()[section]
        .as_array()
        .unwrap_or_else(|| panic!("section {section} is a list"))
}

/// A string field.
pub(crate) fn text<'a>(case: &'a Value, key: &str) -> &'a str {
    case[key]
        .as_str()
        .unwrap_or_else(|| panic!("field {key} is a string in {case}"))
}

/// An optional string field (`null` or absent is `None`).
pub(crate) fn opt_text<'a>(case: &'a Value, key: &str) -> Option<&'a str> {
    case.get(key).and_then(Value::as_str)
}

/// An unsigned integer field.
pub(crate) fn number(case: &Value, key: &str) -> u64 {
    case[key]
        .as_u64()
        .unwrap_or_else(|| panic!("field {key} is an unsigned integer in {case}"))
}

/// Decode the `body_hex` of a scripted reply.
pub(crate) fn hex_bytes(hex: &str) -> Vec<u8> {
    (0..hex.len())
        .step_by(2)
        .map(|index| u8::from_str_radix(&hex[index..index + 2], 16).expect("hex digit pair"))
        .collect()
}
