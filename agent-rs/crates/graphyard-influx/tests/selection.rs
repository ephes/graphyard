mod common;

use chrono::DateTime;
use common::{FakeInflux, client_for};
use graphyard_influx::QueryRange;
use serde_json::{Value, json};

#[tokio::test]
async fn complete_replies_are_bound_to_selection() {
    let start = DateTime::parse_from_rfc3339("2026-11-01T00:00:00Z")
        .unwrap()
        .to_utc();
    let stop = start + chrono::Duration::hours(1);
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
            bad[key] = Value::String(value.into());
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
                        row["metric"].as_str().unwrap(),
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
