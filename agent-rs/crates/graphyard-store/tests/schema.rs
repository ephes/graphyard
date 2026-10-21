//! Schema contract verification and `--check-schema` (PLAN.md 5.1, D-2).

mod common;

use common::{AGENT_TABLES_JSON, SCHEMA_SQL, TestDb, default_settings, load};
use graphyard_store::{MIN_GRAPHYARD_MIGRATION, REQUIRED_COLUMNS, Store, check_schema};

fn names(value: &serde_json::Value) -> Vec<&str> {
    value
        .as_array()
        .unwrap()
        .iter()
        .map(|item| item.as_str().unwrap())
        .collect()
}

#[test]
fn contract_constants_match_the_generated_agent_tables() {
    let contract = load(AGENT_TABLES_JSON);
    let min = names(&contract["min_graphyard_migration"]);
    assert_eq!((min[0], min[1]), MIN_GRAPHYARD_MIGRATION);
    let tables = contract["tables"].as_object().unwrap();
    let table_names: Vec<&str> = tables.keys().map(String::as_str).collect();
    let required_names: Vec<&str> = REQUIRED_COLUMNS.iter().map(|(table, _)| *table).collect();
    assert_eq!(table_names, required_names);
    for (table, required) in REQUIRED_COLUMNS {
        assert_eq!(names(&tables[table]), required, "{table}");
    }
}

#[test]
fn schema_fixture_matches_the_generated_agent_tables() {
    let db = TestDb::new();
    let connection = db.raw();
    let contract = load(AGENT_TABLES_JSON);
    for (table, columns) in contract["tables"].as_object().unwrap() {
        let mut statement = connection
            .prepare("SELECT name FROM pragma_table_info(?)")
            .unwrap();
        let present: Vec<String> = statement
            .query_map([table], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(present, names(columns), "{table}");
    }
}

#[tokio::test]
async fn migrated_schema_passes() {
    let db = TestDb::new();

    let report = check_schema(db.path(), &default_settings()).unwrap();
    assert!(report.is_ok(), "{report}");
    assert_eq!(report.error_message(), None);
    assert_eq!(report.journal_mode, "delete");
    assert_eq!(
        report.to_string(),
        "database schema satisfies the agent contract (graphyard.0010_condition_stale_allowance, \
         journal_mode=delete)"
    );

    // The agent's own connection switched the file to WAL (the default
    // GRAPHYARD_SQLITE_JOURNAL_MODE) and verifies the same contract.
    let store = db.store();
    let verified = store.verify_schema().await.unwrap();
    assert!(verified.is_ok());
    assert_eq!(verified.journal_mode, "wal");
    store.close();
}

#[test]
fn a_migration_that_drops_an_agent_column_is_named_with_the_migration() {
    // The 0009-era database: no stale allowance columns, migrations recorded
    // up to 0009.
    let schema = SCHEMA_SQL
        .replace(
            ", \"alert_when_stale\" bool NOT NULL, \"stale_after_seconds\" integer unsigned NULL \
             CHECK (\"stale_after_seconds\" >= 0)",
            "",
        )
        .lines()
        .filter(|line| {
            !line.contains("'0010_condition_stale_allowance'")
                && !line.contains("'0011_")
                && !line.contains("'0012_")
        })
        .collect::<Vec<_>>()
        .join("\n");
    assert!(!schema.contains("stale_after_seconds"));
    let db = TestDb::with_schema(&schema);

    let report = check_schema(db.path(), &default_settings()).unwrap();

    assert!(!report.is_ok());
    assert!(report.missing_tables.is_empty());
    assert_eq!(
        report.missing_columns,
        [
            (
                "condition_definition".to_owned(),
                "alert_when_stale".to_owned()
            ),
            (
                "condition_definition".to_owned(),
                "stale_after_seconds".to_owned()
            ),
        ]
    );
    assert!(report.missing_migration);
    assert!(!report.missing_migrations_table);
    assert_eq!(
        report.error_message().unwrap(),
        "database schema is older than the agent contract; missing columns: \
         condition_definition.alert_when_stale, condition_definition.stale_after_seconds; \
         missing migration: graphyard.0010_condition_stale_allowance; run `manage.py migrate` \
         before starting the agent"
    );
}

#[test]
fn a_database_without_migrations_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db.sqlite3");
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute_batch("CREATE TABLE pipeline_heartbeat (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();

    let report = check_schema(&path, &default_settings()).unwrap();

    assert_eq!(
        report.missing_tables,
        [
            "metric_collection_spec",
            "condition_definition",
            "subject_registry",
            "host_registry",
            "service_registry"
        ]
    );
    assert_eq!(
        report.missing_columns,
        [
            "status",
            "last_success",
            "last_error",
            "details",
            "updated_at"
        ]
        .map(|column| ("pipeline_heartbeat".to_owned(), column.to_owned()))
    );
    assert!(report.missing_migrations_table && report.missing_migration);
    assert!(report.error_message().unwrap().contains(
        "missing migration: graphyard.0010_condition_stale_allowance (table django_migrations \
         does not exist)"
    ));
}

#[test]
fn check_schema_opens_read_only_and_never_creates() {
    let db = TestDb::new();
    let before = std::fs::read(db.path()).unwrap();
    check_schema(db.path(), &default_settings()).unwrap();
    assert_eq!(std::fs::read(db.path()).unwrap(), before);
    assert!(!db.path().with_extension("sqlite3-wal").exists());

    let missing = db.path().with_file_name("missing.sqlite3");
    assert!(check_schema(&missing, &default_settings()).is_err());
    assert!(Store::open(&missing, &default_settings()).is_err());
    assert!(!missing.exists());
}
