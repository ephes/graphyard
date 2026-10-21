//! Tests of the executors: the `executor` scenarios of
//! `fixtures/page_probe.json` and `fixtures/unifi.json` and the scenarios of
//! `fixtures/ha_json_executors.json` replayed on fake servers with a
//! recording sink, the UniFi helper corpora, and the fake-server tests of
//! rules the scenarios cannot express (deadline, abandoned lookups, TLS,
//! warn-once per run). No test reaches the network.

mod ha_json;
mod ha_json_budget;
mod page_probe;
mod support;
mod unifi;
