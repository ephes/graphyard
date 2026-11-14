//! Retained writer memory excludes caller-owned collector input.
use chrono::{TimeZone, Utc};
use graphyard_core::{line_protocol::prepare_write, model::MetricPoint};
#[test]
fn excessive_tags_and_text_do_not_get_cloned_or_logged_in_full() {
    let mut invalid = MetricPoint::new(
        Utc.timestamp_opt(0, 0).unwrap(),
        "x".repeat(1_000_000),
        1.0,
        "host",
        "edge",
        "test",
        "agent",
        "edge",
    );
    invalid.tags = Some(
        (0..10000)
            .map(|n| (n.to_string(), "v".repeat(256)))
            .collect(),
    );
    let valid = MetricPoint::new(
        Utc.timestamp_opt(0, 0).unwrap(),
        "ok",
        1.0,
        "host",
        "edge",
        "test",
        "agent",
        "edge",
    );
    // Admit neither a huge text field nor an independently excessive tag map.
    let mut excessive_tags = valid.clone();
    excessive_tags.tags = invalid.tags.take();
    let points = [invalid, excessive_tags, valid];
    let prepared = prepare_write("metrics", &points);
    let retained_bytes: usize = prepared.lines.iter().map(String::capacity).sum::<usize>()
        + prepared
            .events
            .iter()
            .map(|event| event.message.capacity())
            .sum::<usize>();
    assert!(retained_bytes < 4096, "{retained_bytes} retained bytes");
    assert_eq!((prepared.written(), prepared.rejected), (1, 2));
}
