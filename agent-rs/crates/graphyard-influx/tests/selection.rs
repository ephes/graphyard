mod common;

use common::{FakeInflux, client_for};
use graphyard_core::utc::UtcDateTime;
use graphyard_influx::QueryRange;
use serde_json::{Value, json};

#[tokio::test]
async fn invalid_ranges_never_reach_any_transport() {
    let start = UtcDateTime::parse_rfc3339("2026-11-01T00:00:00Z").unwrap();
    let stop = start + std::time::Duration::from_secs(1);
    for (mode, body, path) in [
        ("v2", "", "/api/v2/query?org=synthetic"),
        ("v3", "[]", "/api/v3/query_sql"),
        ("auto", "", "/api/v2/query?org=synthetic"),
    ] {
        let server = FakeInflux::start(&[
            json!({"status": 200, "reason": "OK", "headers": [], "body": body}),
        ])
        .await;
        let case = json!({"settings": {"INFLUX_API_MODE": mode, "INFLUX_TOKEN": "synthetic", "INFLUX_ORG": "synthetic"}});
        let client = client_for(&case, &server);
        for (lower, upper) in [(start, start), (stop, start)] {
            let result = client
                .query_range(&QueryRange::new("cpu", lower, upper))
                .await;
            assert!(server.recorded().is_empty(), "invalid range sent in {mode}");
            let error = result.expect_err("invalid range");
            assert_eq!(error.python_class(), "InfluxConfigurationError");
            assert_eq!(
                error.to_string(),
                "Invalid InfluxDB query window: start must precede stop"
            );
        }
        let samples = client
            .query_range(&QueryRange::new("cpu", start, stop))
            .await
            .unwrap();
        assert!(samples.is_empty());
        let requests = server.recorded();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].target, path);
    }
}

#[tokio::test]
async fn complete_replies_are_bound_to_selection() {
    let start = UtcDateTime::parse_rfc3339("2026-11-01T00:00:00Z").unwrap();
    let stop = start + std::time::Duration::from_secs(3600);
    let mut range = QueryRange::new("cpu", start, stop);
    range.host = Some("edge".into());
    range.service = Some("web".into());
    range.subject_type = Some("host".into());
    range.subject_id = Some("edge".into());
    range.tags.insert("mountpoint".into(), "/".into());
    let good = json!({"time": "2026-11-01T00:00:00Z", "value": 99, "metric": "cpu", "host": "edge", "service": "web", "subject_type": "host", "subject_id": "edge", "mountpoint": "/"});
    for mode in ["v2", "v3"] {
        for (key, value) in [
            ("metric", "other"),
            ("metric", "__missing__"),
            ("host", "other"),
            ("service", "other"),
            ("subject_type", "sensor"),
            ("subject_id", "other"),
            ("mountpoint", "/other"),
            ("time", "2026-11-01T01:00:00Z"),
            ("time", "2026-11-01T02:00:00Z"),
            ("time", "2026-10-31T23:59:59Z"),
            ("metric", "cpu"),
        ] {
            let mut bad = good.clone();
            if value == "__missing__" {
                bad.as_object_mut().unwrap().remove(key);
            } else {
                bad[key] = Value::String(value.into());
            }
            let body = if mode == "v3" {
                json!([good, bad]).to_string()
            } else {
                let mut csv = String::from(
                    "#datatype,string,long,dateTime:RFC3339,double,string,string,string,string,string,string\n#group,false,false,false,false,true,true,true,true,true,true\n#default,_result,,,,,,,,,\n,result,table,_time,_value,metric,host,service,subject_type,subject_id,mountpoint\n",
                );
                for row in [&good, &bad] {
                    csv.push_str(&format!(
                        ",,0,{},99,{},{},{},{},{},{}\n",
                        row["time"].as_str().unwrap(),
                        row["metric"].as_str().unwrap_or_default(),
                        row["host"].as_str().unwrap(),
                        row["service"].as_str().unwrap(),
                        row["subject_type"].as_str().unwrap(),
                        row["subject_id"].as_str().unwrap(),
                        row["mountpoint"].as_str().unwrap()
                    ));
                }
                csv
            };
            let server = FakeInflux::start(&[
                json!({"status": 200, "reason": "OK", "headers": [], "body": body}),
            ])
            .await;
            let case = json!({"settings": {"INFLUX_API_MODE": mode, "INFLUX_TOKEN": "synthetic", "INFLUX_ORG": "synthetic"}});
            let result = client_for(&case, &server).query_range(&range).await;
            if key == "metric" && value == "cpu" {
                assert_eq!(result.unwrap().len(), 2);
            } else {
                assert_eq!(
                    result.expect_err("mismatched selection").to_string(),
                    "InfluxDB query result does not match selection"
                );
            }
        }
    }
    // Unfiltered dimensions may differ; absent series are not invented.
    let server = FakeInflux::start(&[
        json!({"status": 200, "reason": "OK", "headers": [], "body": json!([good]).to_string()}),
    ])
    .await;
    let case = json!({"settings": {"INFLUX_API_MODE": "v3", "INFLUX_TOKEN": "synthetic", "INFLUX_ORG": "synthetic"}});
    assert_eq!(
        client_for(&case, &server)
            .query_range(&QueryRange::new("cpu", start, stop))
            .await
            .unwrap()
            .len(),
        1
    );
}
