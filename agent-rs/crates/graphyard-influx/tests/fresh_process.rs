//! A writer in a process where nothing installed a rustls crypto provider
//! before (its own test binary): building and using it must not panic.

use graphyard_core::model::MetricPoint;
use graphyard_core::settings::Settings;
use graphyard_influx::InfluxWriter;

#[tokio::test]
async fn writer_works_without_a_preinstalled_crypto_provider() {
    let writer = InfluxWriter::new().expect("client");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    drop(listener);
    let url = format!("http://{addr}");
    // A non-default timeout builds a second client.
    let env = [
        ("INFLUX_URL", url.as_str()),
        ("INFLUX_TOKEN", "t"),
        ("INFLUX_ORG", "o"),
        ("INFLUX_TIMEOUT_MS", "500"),
    ];
    let settings = Settings::from_lookup(|key| {
        env.iter()
            .find(|(name, _)| *name == key)
            .map(|(_, value)| (*value).to_owned())
    })
    .expect("settings");
    let point = MetricPoint::new(
        chrono::DateTime::UNIX_EPOCH,
        "host.cpu_usage",
        0.5,
        "host",
        "macmini",
        "vector",
        "vector",
        "macmini",
    );
    let err = writer.write_points(&settings, &[point]).await.unwrap_err();
    assert_eq!(err.python_class(), "NewConnectionError", "{err}");
}
