//! PLAN.md 5.7: the agent never reads or writes the ingest token tables.
//! The store holds all of the agent's SQL, so no source file of the store
//! may name them.

use std::path::Path;

fn sources(directory: &Path, files: &mut Vec<std::path::PathBuf>) {
    for entry in std::fs::read_dir(directory).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            sources(&path, files);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            files.push(path);
        }
    }
}

#[test]
fn store_sql_never_names_the_ingest_token_tables() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    sources(&root, &mut files);
    assert!(files.len() >= 8, "found {files:?}");
    for file in files {
        let text = std::fs::read_to_string(&file).unwrap().to_lowercase();
        assert!(
            !text.contains("ingest_token"),
            "{} names an ingest token table",
            file.display()
        );
    }
}
