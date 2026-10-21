//! Tests of the executors: the `executor` scenarios of
//! `fixtures/page_probe.json` and `fixtures/unifi.json` replayed on fake
//! servers with a recording sink, the UniFi helper corpora, and the
//! fake-server tests of rules the scenarios cannot express (deadline,
//! abandoned lookups, TLS). No test reaches the network.

mod page_probe;
mod support;
mod unifi;
