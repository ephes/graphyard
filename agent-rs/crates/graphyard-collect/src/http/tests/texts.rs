//! Status texts, exception classes and graphyard messages against the
//! fixture.

use super::fixture::{cases, fixture, number, opt_text, text};
use crate::http::client::{CLOSE_JOIN, MAX_ABANDONED, MAX_REDIRECTS};
use crate::http::deadline::{abandoned_error, deadline_error};
use crate::http::error::{ErrorKind, HttpError};
use crate::http::status::{reason_phrase, standard_reason_phrase, status_error_message};

#[test]
fn reason_phrases_match_httpx_codes() {
    let table = fixture()["reason_phrases"]
        .as_object()
        .expect("reason phrase table");
    for code in 100..1000u16 {
        let expected = table
            .get(&code.to_string())
            .and_then(|value| value.as_str())
            .unwrap_or("");
        assert_eq!(standard_reason_phrase(code), expected, "code {code}");
    }
}

#[test]
fn raise_for_status_texts_match_httpx() {
    for case in cases("raise_for_status") {
        let code = u16::try_from(number(case, "status")).expect("status");
        let reason = reason_phrase(text(case, "server_reason").as_bytes());
        assert_eq!(reason, text(case, "reason_phrase"), "{case}");
        let message =
            status_error_message(code, &reason, text(case, "url"), opt_text(case, "location"));
        assert_eq!(message.as_deref(), opt_text(case, "message"), "{case}");
    }
}

#[test]
fn non_ascii_reason_bytes_are_dropped() {
    assert_eq!(reason_phrase(b"Nicht gefunden \xc3\xbc"), "Nicht gefunden ");
    assert_eq!(reason_phrase(b"\xff"), "");
}

#[test]
fn exception_classes_match_python() {
    let rows = cases("exception_classes");
    for kind in ErrorKind::ALL {
        let row = rows
            .iter()
            .find(|row| text(row, "class") == kind.python_class())
            .unwrap_or_else(|| panic!("{} missing in the fixture", kind.python_class()));
        assert_eq!(
            kind.is_http_error(),
            row["http_error"].as_bool().expect("flag"),
            "{row}"
        );
        assert_eq!(
            kind.is_page_probe_warning(),
            row["page_probe_warning"].as_bool().expect("flag"),
            "{row}"
        );
    }
}

#[test]
fn graphyard_messages_and_limits_match_python() {
    let messages = &fixture()["messages"];
    assert_eq!(
        number(messages, "max_abandoned_workers"),
        u64::try_from(MAX_ABANDONED).expect("cap")
    );
    assert_eq!(
        text(messages, "close_join_seconds"),
        format!("{:?}", CLOSE_JOIN.as_secs_f64())
    );
    assert_eq!(number(messages, "max_redirects"), u64::from(MAX_REDIRECTS));
    for case in messages["deadline"].as_array().expect("deadline cases") {
        let total: f64 = text(case, "total").parse().expect("float text");
        let err = deadline_error(total);
        assert_eq!(err.kind, ErrorKind::DeadlineExceeded);
        assert_eq!(err.message, text(case, "message"));
    }
    for case in messages["abandoned"].as_array().expect("abandoned cases") {
        let count = usize::try_from(number(case, "count")).expect("count");
        let err = abandoned_error(count);
        assert_eq!(err.kind, ErrorKind::DeadlineExceeded);
        assert_eq!(err.message, text(case, "message"));
    }
    let limit = HttpError::body_limit(4);
    assert_eq!(limit.kind, ErrorKind::LimitExceeded);
    assert_eq!(limit.message, "response body exceeded max_body_bytes=4");
}
