//! `pycompat` against `fixtures/pycompat.json` (built by
//! `fixtures/gen_pycompat.py`).

use serde_json::Value;

use super::*;
use crate::fixture::{Field, load, strings};

fn fixture() -> Value {
    load(include_str!("../../../../fixtures/pycompat.json"))
}

fn float_of_bits(hex: &str) -> f64 {
    f64::from_bits(u64::from_str_radix(hex, 16).expect("fixture bits are hex"))
}

fn json(text: &str) -> Value {
    serde_json::from_str(text).expect("fixture JSON text parses")
}

/// Compare a float result with the fixture's `bits`/`repr` or `error`.
fn assert_float_result(case: &Value, actual: Result<f64, PyError>, input: &str) {
    match case.get("error") {
        Some(error) => {
            let actual = actual.expect_err(input);
            assert_eq!(actual.python_class(), error.str_field("class"), "{input}");
            assert_eq!(actual.message, error.str_field("message"), "{input}");
        }
        None => {
            let actual = actual.unwrap_or_else(|err| panic!("{input}: {err}"));
            let expected = float_of_bits(case.str_field("bits"));
            assert!(
                actual.to_bits() == expected.to_bits() || (actual.is_nan() && expected.is_nan()),
                "{input}: {actual} != {expected}"
            );
            assert_eq!(py_float_repr(actual), case.str_field("repr"), "{input}");
        }
    }
}

#[test]
fn int_digit_limit_matches_python() {
    assert_eq!(
        fixture().f64_field("max_int_str_digits"),
        MAX_INT_STR_DIGITS as f64
    );
}

#[test]
fn float_repr_and_format_g_match_python() {
    for case in fixture().list_field("floats") {
        let value = float_of_bits(case.str_field("bits"));
        assert_eq!(py_float_repr(value), case.str_field("repr"), "{value:e}");
        assert_eq!(py_float_repr(value), case.str_field("str"), "{value:e}");
        assert_eq!(py_format_g(value), case.str_field("format_g"), "{value:e}");
    }
}

#[test]
fn float_parsing_matches_python() {
    for case in fixture().list_field("float_parse") {
        let input = case.str_field("input");
        assert_float_result(case, py_float(input), input);
    }
}

#[test]
fn int_parsing_matches_python() {
    for case in fixture().list_field("int_parse") {
        let input = case.str_field("input");
        match case.get("error") {
            Some(error) => {
                let actual = py_int(input).expect_err(input);
                assert_eq!(actual.python_class(), error.str_field("class"));
                assert_eq!(actual.message, error.str_field("message"));
            }
            None => assert_eq!(
                py_int(input).expect(input).to_string(),
                case.str_field("value"),
                "{input:?}"
            ),
        }
    }
}

#[test]
fn value_str_repr_type_and_float_match_python() {
    for case in fixture().list_field("values") {
        let text = case.str_field("json");
        let value = json(text);
        assert_eq!(py_type_name(&value), case.str_field("type"), "{text}");
        assert_eq!(py_str(&value), case.str_field("str"), "{text}");
        assert_eq!(py_repr(&value), case.str_field("repr"), "{text}");
        assert_eq!(py_describe(&value), case.str_field("describe"), "{text}");
        assert_float_result(case.field("float"), py_float_of(&value), text);
    }
}

#[test]
fn describe_of_values_beyond_the_int_limit_matches_python() {
    for case in fixture().list_field("describe_beyond_int_limit") {
        let value = json(case.str_field("json"));
        assert_eq!(py_describe(&value), case.str_field("describe"));
    }
}

#[test]
fn strip_and_split_match_python() {
    for case in fixture().list_field("strip_split") {
        let input = case.str_field("input");
        assert_eq!(py_strip(input), case.str_field("strip"), "{input:?}");
        assert_eq!(
            py_split_whitespace(input),
            strings(case.list_field("split")),
            "{input:?}"
        );
    }
}

#[test]
fn equality_matches_python() {
    for case in fixture().list_field("eq") {
        let (left, right) = (case.str_field("left"), case.str_field("right"));
        let (a, b) = (json(left), json(right));
        assert_eq!(py_eq(&a, &b), case.bool_field("eq"), "{left} == {right}");
        assert_eq!(py_eq(&b, &a), case.bool_field("eq"), "{right} == {left}");
    }
}

#[test]
fn json_int_digit_limit_matches_python() {
    for case in fixture().list_field("int_limit") {
        let text = case.str_field("json");
        let actual = int_digit_limit_error(text);
        match case.field("error") {
            Value::Null => assert_eq!(actual, None, "{}", &text[..20]),
            error => {
                let actual = actual.expect("limit error");
                assert_eq!(actual.python_class(), error.str_field("class"));
                assert_eq!(actual.message, error.str_field("message"));
            }
        }
    }
}

#[test]
fn non_ascii_digits_are_rejected_d6() {
    // Python accepts Unicode decimal digits in int() and float(); the agent
    // accepts ASCII digits only (PLAN.md D-6).
    assert_eq!(
        py_int("\u{661}").unwrap_err().message,
        "invalid literal for int() with base 10: '\u{661}'"
    );
    assert_eq!(
        py_float("\u{ff11}\u{ff10}").unwrap_err().message,
        "could not convert string to float: '\u{ff11}\u{ff10}'"
    );
}

#[test]
fn repr_escapes_controls_and_separators_but_not_other_categories() {
    // Cc and Zs/Zl/Zp are escaped like CPython; format characters such as
    // U+200B are escaped by CPython but kept here (P2, PLAN.md 2.3).
    assert_eq!(
        py_str_repr("a\u{a0}b\u{2029}\u{9f}"),
        "'a\\xa0b\\u2029\\x9f'"
    );
    assert_eq!(py_str_repr("\u{200b}"), "'\u{200b}'");
}

#[test]
fn number_tokens_decode_like_json_loads() {
    let value = json("[1, 1.0, 1e2, -0, 1e400, 123456789012345678901234567890]");
    let items = value.as_array().expect("list");
    let decoded: Vec<PyNumber> = items
        .iter()
        .map(|item| py_number(item.as_number().expect("number")))
        .collect();
    assert_eq!(decoded[0], PyNumber::Int(1.into()));
    assert_eq!(decoded[1], PyNumber::Float(1.0));
    assert_eq!(decoded[2], PyNumber::Float(100.0));
    assert_eq!(decoded[3], PyNumber::Int(0.into()));
    assert_eq!(decoded[4], PyNumber::Float(f64::INFINITY));
    assert_eq!(
        py_repr(&items[5]),
        "123456789012345678901234567890",
        "big integers stay exact"
    );
    assert_eq!(bigint_to_i64_saturating(&py_int("-1000").unwrap()), -1000);
    assert_eq!(
        bigint_to_i64_saturating(&py_int("99999999999999999999").unwrap()),
        i64::MAX
    );
    assert_eq!(
        bigint_to_i64_saturating(&py_int("-99999999999999999999").unwrap()),
        i64::MIN
    );
}
