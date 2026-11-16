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
        let logs = take_logs();
        assert_result(case, &result, &server.server(), None);
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

#[test]
fn r102_default_expansion_is_bounded() {
    let body = format!(
        "#datatype,string,long,string\n#group,false,false,false\n#default,,,{default}\n,result,table,tag\n{rows}",
        default = "x".repeat(100_000),
        rows = ",,0,\n".repeat(1000)
    );
    let error = graphyard_influx::flux::parse_annotated_csv(body.as_bytes())
        .expect_err("expanded cell limit");
    assert!(error.to_string().contains("expanded cell"));
}

#[test]
fn r102_duplicate_column_diagnostic_has_no_remote_labels() {
    let label = format!("credential-sentinel{}", "x".repeat(100_000));
    let body = format!(
        "#datatype,string,long,string,string\n#group,false,false,false,false\n#default,,,,\n,result,table,{label},{label}\n,,0,a,b\n"
    );
    capture_logs();
    let _ = graphyard_influx::flux::parse_annotated_csv(body.as_bytes());
    let logs = take_logs();
    assert_eq!(logs.len(), 1);
    assert_eq!(logs[0].1, "InfluxDB Flux duplicate_columns count=1");
}

#[test]
fn r102_duplicate_decision_columns_refused_in_either_order() {
    for column in ["_value", "_time", "host", "custom_tag"] {
        for values in ["1,99", "99,1"] {
            let body = format!(
                "#datatype,string,long,string,string\n#group,false,false,false,false\n#default,,,,\n,result,table,{column},{column}\n,,0,{values}\n"
            );
            let error = graphyard_influx::flux::parse_annotated_csv(body.as_bytes())
                .expect_err("duplicate columns must be refused");
            assert_eq!(error.to_string(), "InfluxDB Flux duplicate column labels");
        }
    }
}

#[tokio::test]
async fn r102_parser_yields_and_honors_elapsed_deadline() {
    use graphyard_influx::flux::parse_annotated_csv_cooperative;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::Duration;
    let body = format!(
        "#datatype,string,long,string\n#group,false,false,false\n#default,,,\n,result,table,tag\n{}",
        ",,0,x\n".repeat(4000)
    );
    let done = AtomicBool::new(false);
    let ticks = AtomicUsize::new(0);
    let parse = async {
        let result = parse_annotated_csv_cooperative(
            body.as_bytes(),
            tokio::time::Instant::now() + Duration::from_secs(10),
        )
        .await;
        done.store(true, Ordering::SeqCst);
        result
    };
    let heartbeat = async {
        while !done.load(Ordering::SeqCst) {
            ticks.fetch_add(1, Ordering::SeqCst);
            tokio::task::yield_now().await;
        }
    };
    let (result, ()) = tokio::join!(parse, heartbeat);
    assert_eq!(result.unwrap().len(), 4000);
    assert!(
        ticks.load(Ordering::SeqCst) > 5,
        "parser monopolized runtime"
    );
    let error = parse_annotated_csv_cooperative(body.as_bytes(), tokio::time::Instant::now())
        .await
        .expect_err("expired parse");
    assert!(error.to_string().contains("total deadline"));
}

#[tokio::test(start_paused = true)]
async fn r102_parser_deadline_can_expire_after_admission() {
    use std::time::Duration;
    let body = format!(
        "#datatype,string,long,string\n#group,false,false,false\n#default,,,\n,result,table,tag\n{}",
        ",,0,x\n".repeat(4000)
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let parse = graphyard_influx::flux::parse_annotated_csv_cooperative(body.as_bytes(), deadline);
    let expire = async {
        tokio::time::advance(Duration::from_secs(31)).await;
    };
    let (result, ()) = tokio::join!(parse, expire);
    assert!(
        result
            .expect_err("deadline during parsing")
            .to_string()
            .contains("total deadline")
    );
}
