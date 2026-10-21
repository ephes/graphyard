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
        // A deliberate divergence states the Rust result separately.
        let expected = case.get("rust").unwrap_or(case);
        let actual = resolve_json_path(&payload, path);
        match expected.get("error") {
            Some(error) => {
                let err = actual.expect_err(&context);
                assert_eq!(err.python_class(), error.str_field("class"), "{context}");
                assert_eq!(err.message, error.str_field("message"), "{context}");
            }
            None => {
                let found = actual.expect(&context).map(py_repr);
                assert_eq!(
                    found.as_deref(),
                    expected.field("found").as_str(),
                    "{context}"
                );
            }
        }
    }
}

#[test]
fn divergent_cases_name_their_plan_entry() {
    let fixture = fixture();
    let divergent: Vec<&Value> = fixture
        .list_field("cases")
        .iter()
        .filter(|case| case.get("divergence").is_some())
        .collect();
    assert_eq!(divergent.len(), 2);
    for case in divergent {
        assert_eq!(case.str_field("divergence"), "D-6");
    }
}
