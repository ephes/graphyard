//! `json_path` against `fixtures/json_path.json` (built by
//! `fixtures/gen_json_path.py`).

use serde_json::Value;

use super::*;
use crate::fixture::{Field, load};
use crate::pycompat::{json_loads, py_repr};

fn fixture() -> Value {
    load(include_str!("../../../../fixtures/json_path.json"))
}

#[test]
fn json_paths_match_python() {
    let fixture = fixture();
    let cases = fixture.list_field("cases");
    assert!(cases.len() > 60, "fixture has the full corpus");
    for case in cases {
        let path = case.str_field("path");
        let payload = json_loads(case.str_field("payload")).expect("payload JSON");
        let context = format!("path {path:?}");
        let actual = resolve_json_path(&payload, path);
        match case.get("error") {
            Some(error) => {
                let err = actual.expect_err(&context);
                assert_eq!(err.python_class(), error.str_field("class"), "{context}");
                assert_eq!(err.message, error.str_field("message"), "{context}");
            }
            None => {
                let found = actual.expect(&context).map(py_repr);
                assert_eq!(found.as_deref(), case.field("found").as_str(), "{context}");
            }
        }
    }
}

#[test]
fn decimal_zeroes_match_the_python_unicode_database() {
    let fixture = fixture();
    let python: Vec<u64> = fixture
        .list_field("decimal_zeroes")
        .iter()
        .map(|code| code.as_u64().expect("code point"))
        .collect();
    let rust: Vec<u64> = DECIMAL_ZEROES.iter().map(|&code| u64::from(code)).collect();
    assert_eq!(rust, python, "regenerate DECIMAL_ZEROES from the fixture");
}
