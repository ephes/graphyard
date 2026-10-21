//! Access to the Python-generated fixtures (`agent-rs/fixtures`) in tests.
//!
//! Fixture files are embedded with `include_str!` and parsed with
//! `serde_json` (`preserve_order`, so object key order is the order the
//! generator wrote).

use serde_json::Value;

/// Parse a fixture file.
pub(crate) fn load(text: &str) -> Value {
    serde_json::from_str(text).expect("fixture file is valid JSON")
}

/// Field accessors that panic with the field name on malformed fixtures.
pub(crate) trait Field {
    fn field(&self, key: &str) -> &Value;
    fn str_field(&self, key: &str) -> &str;
    fn list_field(&self, key: &str) -> &[Value];
    fn bool_field(&self, key: &str) -> bool;
    fn f64_field(&self, key: &str) -> f64;
}

impl Field for Value {
    fn field(&self, key: &str) -> &Value {
        self.get(key)
            .unwrap_or_else(|| panic!("fixture field {key:?} missing in {self}"))
    }

    fn str_field(&self, key: &str) -> &str {
        self.field(key)
            .as_str()
            .unwrap_or_else(|| panic!("fixture field {key:?} is not a string"))
    }

    fn list_field(&self, key: &str) -> &[Value] {
        self.field(key)
            .as_array()
            .unwrap_or_else(|| panic!("fixture field {key:?} is not a list"))
    }

    fn bool_field(&self, key: &str) -> bool {
        self.field(key)
            .as_bool()
            .unwrap_or_else(|| panic!("fixture field {key:?} is not a bool"))
    }

    fn f64_field(&self, key: &str) -> f64 {
        self.field(key)
            .as_f64()
            .unwrap_or_else(|| panic!("fixture field {key:?} is not a number"))
    }
}

/// The strings of a fixture list of strings.
pub(crate) fn strings(values: &[Value]) -> Vec<&str> {
    values
        .iter()
        .map(|value| value.as_str().expect("fixture list item is a string"))
        .collect()
}
