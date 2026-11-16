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
        "database schema satisfies the agent contract (graphyard.0015_alter_conditiondefinition_breach_minutes_and_more, \
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
                && !line.contains("'0015_alter_conditiondefinition_breach_minutes_and_more'")
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
        "database schema does not satisfy the agent contract; missing columns: \
         condition_definition.alert_when_stale, condition_definition.stale_after_seconds; \
         missing migration: graphyard.0015_alter_conditiondefinition_breach_minutes_and_more; stop the agent and run `manage.py migrate` \
         or restore the migrated schema before starting the agent"
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
        "missing migration: graphyard.0015_alter_conditiondefinition_breach_minutes_and_more (table django_migrations \
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

fn change_table(table: &str, from: &str, to: &str) -> String {
    let marker = format!("CREATE TABLE \"{table}\"");
    SCHEMA_SQL
        .lines()
        .map(|line| {
            if line.starts_with(&marker) {
                assert!(line.contains(from), "mutation must change {table}");
                line.replace(from, to)
            } else {
                line.to_owned()
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn readiness_rejects_missing_identity_uniqueness_in_every_agent_table() {
    for (table, key) in [
        (
            "metric_collection_spec",
            "\"name\" varchar(255) NOT NULL UNIQUE",
        ),
        (
            "condition_definition",
            "\"name\" varchar(255) NOT NULL UNIQUE",
        ),
        (
            "pipeline_heartbeat",
            "\"name\" varchar(128) NOT NULL UNIQUE",
        ),
        (
            "subject_registry",
            "CONSTRAINT \"subject_registry_unique_subject\" UNIQUE (\"subject_type\", \"subject_id\")",
        ),
        ("host_registry", "\"host_id\" varchar(128) NOT NULL UNIQUE"),
        (
            "service_registry",
            "\"service_id\" varchar(128) NOT NULL UNIQUE",
        ),
    ] {
        let replacement = if table == "subject_registry" {
            "CHECK (1)".to_owned()
        } else {
            key.replace(" UNIQUE", "")
        };
        let db = TestDb::with_schema(&change_table(table, key, &replacement));
        let report = check_schema(db.path(), &default_settings()).unwrap();
        assert!(!report.is_ok(), "missing unique key in {table}: {report}");
        assert!(report.to_string().contains(table));
    }
}

#[test]
fn readiness_rejects_wrong_affinity_nullability_and_row_identity() {
    for (from, to) in [
        ("\"enabled\" bool NOT NULL", "\"enabled\" bool NULL"),
        (
            "\"interval_seconds\" integer unsigned",
            "\"interval_seconds\" text",
        ),
        (
            "\"id\" integer NOT NULL PRIMARY KEY AUTOINCREMENT",
            "\"id\" bigint NOT NULL PRIMARY KEY",
        ),
        (
            "\"id\" integer NOT NULL PRIMARY KEY AUTOINCREMENT",
            "\"id\" integer NOT NULL",
        ),
    ] {
        let db = TestDb::with_schema(&change_table("metric_collection_spec", from, to));
        let report = check_schema(db.path(), &default_settings()).unwrap();
        assert!(!report.is_ok(), "{from} -> {to}: {report}");
        assert!(report.to_string().contains("metric_collection_spec"));
    }
}

#[test]
fn partial_expression_and_nonunique_indexes_do_not_supply_the_identity_key() {
    for index in [
        "CREATE UNIQUE INDEX replacement ON host_registry(host_id) WHERE enabled",
        "CREATE UNIQUE INDEX replacement ON host_registry(lower(host_id))",
        "CREATE INDEX replacement ON host_registry(host_id)",
    ] {
        let mut schema = change_table("host_registry", " NOT NULL UNIQUE", " NOT NULL");
        schema.push_str(&format!("\n{index};"));
        let db = TestDb::with_schema(&schema);
        assert!(
            !check_schema(db.path(), &default_settings())
                .unwrap()
                .is_ok(),
            "{index}"
        );
    }
}

#[test]
fn equivalent_affinity_and_extra_columns_indexes_are_allowed() {
    let mut schema = change_table(
        "metric_collection_spec",
        "\"interval_seconds\" integer unsigned",
        "\"interval_seconds\" BIGINT",
    );
    schema.push_str("\nALTER TABLE host_registry ADD COLUMN extension TEXT; CREATE INDEX extra ON host_registry(display_name);");
    let db = TestDb::with_schema(&schema);
    assert!(
        check_schema(db.path(), &default_settings())
            .unwrap()
            .is_ok()
    );
}

#[test]
fn equivalent_named_binary_identity_index_is_allowed() {
    let mut schema = change_table("host_registry", " NOT NULL UNIQUE", " NOT NULL");
    schema.push_str(
        "\nCREATE UNIQUE INDEX renamed_identity ON host_registry(host_id COLLATE binary);",
    );
    let db = TestDb::with_schema(&schema);
    assert!(
        check_schema(db.path(), &default_settings())
            .unwrap()
            .is_ok()
    );
}
