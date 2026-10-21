//! Mode `v2` end to end against a fake InfluxDB replaying the scripts of
//! `fixtures/flux.json`: the request `influxdb_client` sends, the decoding
//! of annotated CSV (`FluxCsvParser`), configuration and HTTP errors.

mod common;

use common::{
    FakeInflux, assert_requests, assert_result, capture_logs, client_for, e2e_range, expected_logs,
    list, load, take_logs, text,
};
use serde_json::Value;

fn fixture() -> Value {
    load(include_str!("../../../fixtures/flux.json"))
}

/// The duplicated-columns warning of `FluxCsvParser`: Python prints it to
/// stdout and through `warnings` (once per process), Rust logs it (P2).
const DUPLICATE_COLUMNS: &str = "The response contains columns with duplicated names";

async fn run_section(section: &str) {
    let fixture = fixture();
    let cases = list(&fixture, section);
    assert!(!cases.is_empty());
    for case in cases {
        let name = text(case, "name");
        let server = FakeInflux::start(list(case, "responses")).await;
        let client = client_for(case, &server);
        capture_logs();
        let result = client.query_range(&e2e_range(case)).await;
        let mut logs = take_logs();
        assert_result(case, &result, &server.server(), None);
        let before = logs.len();
        logs.retain(|(_, message)| !message.starts_with(DUPLICATE_COLUMNS));
        assert_eq!(
            before - logs.len(),
            usize::from(name == "duplicate_column_names"),
            "{name}"
        );
        assert_eq!(logs, expected_logs(case, &server.server()), "{name}");
        assert_requests(case, &server.recorded());
    }
}

#[tokio::test]
async fn annotated_csv_matches_python() {
    run_section("csv").await;
}

#[tokio::test]
async fn v2_request_matches_python() {
    run_section("requests").await;
}

#[tokio::test]
async fn v2_errors_match_python() {
    run_section("errors").await;
}

#[test]
fn query_error_table_reprs_both_arguments() {
    let body = b"#datatype,string,string\r\n#group,true,true\r\n#default,,\r\n,error,reference\r\n,it's,\r\n";
    let err = graphyard_influx::flux::parse_annotated_csv(body).expect_err("error table");
    assert_eq!(err.python_class(), "FluxQueryException");
    assert_eq!(err.to_string(), "(\"it's\", '')");
}
