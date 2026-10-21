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
    json_loads(text).expect("fixture JSON text parses")
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

#[test]
fn json_loads_keeps_objects_that_spell_the_serde_number_token() {
    let text = r#"{"$serde_json::private::Number": "5"}"#;
    // The plain serde_json decoding would turn the dict into the number 5.
    let misread: Value = serde_json::from_str(text).unwrap();
    assert!(misread.is_number());
    let loaded = json_loads(text).unwrap();
    assert!(loaded.is_object());
    assert_eq!(py_type_name(&loaded), "dict");

    let escaped = json_loads(r#"[{"\u0024serde_json::private::Number": "1.5"}, 2, 2.5]"#).unwrap();
    assert_eq!(
        py_repr(&escaped),
        "[{'$serde_json::private::Number': '1.5'}, 2, 2.5]"
    );
}

#[test]
fn json_loads_accepts_and_rejects_like_serde_json() {
    let deep = |levels: usize| format!("{}1{}", "[".repeat(levels), "]".repeat(levels));
    let token_deep = |levels: usize| {
        format!(
            "{}{{\"$serde_json::private::Number\": 1}}{}",
            "[".repeat(levels - 1),
            "]".repeat(levels - 1)
        )
    };
    for text in [
        String::new(),
        "{".to_owned(),
        "[1,]".to_owned(),
        "NaN".to_owned(),
        r#""\ud800""#.to_owned(),
        r#"{"\ud800": 1}"#.to_owned(),
        deep(127),
        deep(128),
        r#"{"a": 1} x"#.to_owned(),
    ] {
        let expected = serde_json::from_str::<Value>(&text).map_err(|err| err.to_string());
        let actual = json_loads(&text).map_err(|err| err.to_string());
        assert_eq!(actual, expected, "{text}");
    }
    // The walk of a token document keeps serde_json's limits.
    assert!(json_loads(&token_deep(127)).is_ok());
    assert!(json_loads(&token_deep(128)).is_err());
    for text in [
        r#"{"$serde_json::private::Number": "\ud800"}"#,
        r#"{"$serde_json::private::Number": 1,}"#,
        r#"{"$serde_json::private::Number": 1} 2"#,
        r#"{"$serde_json::private::Number": NaN}"#,
    ] {
        assert!(json_loads(text).is_err(), "{text}");
    }
    let unicode = json_loads(r#"{"k\u00e9y": "v\u00e4l", "n": -0}"#).unwrap();
    assert_eq!(py_repr(&unicode), "{'k\u{e9}y': 'v\u{e4}l', 'n': 0}");
}

/// `int / 60` of CPython (values computed with Python 3.12).
#[test]
fn int_true_divide_rounds_like_python() {
    let cases = [
        ("0", "0.0"),
        ("1", "0.016666666666666666"),
        ("-1", "-0.016666666666666666"),
        ("59", "0.9833333333333333"),
        ("60", "1.0"),
        ("-61", "-1.0166666666666666"),
        ("9007199254740992", "150119987579016.53"),
        ("9007199254740993", "150119987579016.56"),
        ("1152921504606859321", "1.9215358410114324e+16"),
        ("-18446744073709551617", "-3.0744573456182586e+17"),
        ("1000000000000000000000000000007", "1.6666666666666667e+28"),
        ("3626777458843887524118557", "6.044629098073146e+22"),
        ("70835497243044678205470", "1.1805916207174113e+21"),
        ("70835497243044678205530", "1.1805916207174113e+21"),
    ];
    for (numerator, expected) in cases {
        let numerator: num_bigint::BigInt = numerator.parse().expect("integer");
        let quotient = py_int_true_divide(&numerator, 60).expect("in range");
        assert_eq!(py_float_repr(quotient), expected, "{numerator}");
    }
    let big = num_bigint::BigInt::from(10).pow(307) * 60;
    assert_eq!(
        py_float_repr(py_int_true_divide(&big, 60).expect("in range")),
        "1e+307"
    );
    // The double range ends at MAX + half an ulp (2^970), which rounds to
    // even, away from MAX.
    let max = (num_bigint::BigInt::from(2).pow(53) - 1) * num_bigint::BigInt::from(2).pow(971);
    let half_ulp = num_bigint::BigInt::from(2).pow(970);
    for below in [&max * 60 - 1, (&max + &half_ulp) * 60 - 1] {
        assert_eq!(
            py_int_true_divide(&below, 60).expect("rounds to max"),
            f64::MAX
        );
    }
    for over in [
        (&max + &half_ulp) * 60,
        num_bigint::BigInt::from(2).pow(1024) * 60 - 1,
    ] {
        let err = py_int_true_divide(&over, 60).expect_err("overflow");
        assert_eq!(err.python_class(), "OverflowError");
        assert_eq!(err.message, "integer division result too large for a float");
    }
}
