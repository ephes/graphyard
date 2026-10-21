//! Django's datetime column conversion against `fixtures/datetime_codec.json`
//! (built by `fixtures/gen_store.py`; PLAN.md 5.3).

mod common;

use chrono::{DateTime, Duration, NaiveDateTime};
use common::{DATETIME_CODEC_JSON, load};
use graphyard_store::DbDateTime;
use graphyard_store::datetime::parse_column;

fn naive(text: &str) -> NaiveDateTime {
    let format = if text.contains('.') {
        "%Y-%m-%d %H:%M:%S%.f"
    } else {
        "%Y-%m-%d %H:%M:%S"
    };
    NaiveDateTime::parse_from_str(text, format).unwrap()
}

#[test]
fn stored_text_converts_like_django() {
    let fixture = load(DATETIME_CODEC_JSON);
    let cases = fixture["parse"].as_array().unwrap();
    assert!(cases.len() > 1500);
    let mut failures = Vec::new();
    for case in cases {
        let text = case["text"].as_str().unwrap();
        let actual = parse_column(Some(text));
        if text.chars().any(|c| c.is_numeric() && !c.is_ascii_digit()) {
            // D-6: Django's `datetime_re` matches Unicode digits (`\d`) and
            // `int()` converts them; the Rust regex takes ASCII digits only,
            // so such text is unparseable (NULL). `fromisoformat` is
            // ASCII-only in both.
            if actual != Ok(None) {
                failures.push(format!("{text:?}: rust {actual:?}, expected NULL (D-6)"));
            }
            continue;
        }
        let ok = match (&actual, case.get("error")) {
            (Err(err), Some(expected)) => {
                err.class == expected["class"].as_str().unwrap()
                    && err.message == expected["message"].as_str().unwrap()
            }
            (Ok(_), Some(_)) | (Err(_), None) => false,
            (Ok(value), None) => match &case["loaded"] {
                serde_json::Value::Null => value.is_none(),
                loaded if value.is_none() => {
                    let _ = loaded;
                    false
                }
                loaded => {
                    let local = naive(loaded["local"].as_str().unwrap());
                    let offset = Duration::microseconds(loaded["offset_micros"].as_i64().unwrap());
                    let value = value.expect("loaded");
                    let instant_ok = value.naive_utc() == local - offset;
                    let stored = value.to_db_text();
                    let stored_ok = match (&stored, case.get("stored"), case.get("stored_error")) {
                        (Ok(text), Some(expected), None) => text == expected.as_str().unwrap(),
                        (Err(err), None, Some(expected)) => {
                            err.class == expected["class"].as_str().unwrap()
                                && err.message == expected["message"].as_str().unwrap()
                        }
                        _ => false,
                    };
                    instant_ok && stored_ok
                }
            },
        };
        if !ok {
            failures.push(format!("{text:?}: rust {actual:?}, python {case}"));
        }
    }
    assert!(
        failures.is_empty(),
        "{} of {} cases differ:\n{}",
        failures.len(),
        cases.len(),
        failures.join("\n")
    );
}

#[test]
fn clock_values_are_written_like_django() {
    let fixture = load(DATETIME_CODEC_JSON);
    for case in fixture["format"].as_array().unwrap() {
        let micros = case["timestamp_micros"].as_i64().unwrap();
        let value = DateTime::from_timestamp_micros(micros).unwrap();
        assert_eq!(
            DbDateTime::from_utc(value).to_db_text().unwrap(),
            case["stored"].as_str().unwrap()
        );
    }
}

#[test]
fn null_stays_null() {
    assert_eq!(parse_column(None).unwrap(), None);
}
