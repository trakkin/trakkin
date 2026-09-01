use trakkin_mapping::MappingDocument;

use super::{MappingLifecycleError, canonical_row_set_diff};

#[test]
fn canonical_row_diff_is_sorted_and_revision_independent() {
    let current = MappingDocument::from_mappings(
        7,
        vec![
            vec![
                "example.media://z".to_owned(),
                "example.media://a".to_owned(),
            ],
            vec![
                "example.media://a".to_owned(),
                "example.media://b".to_owned(),
            ],
        ],
    )
    .expect("current document should parse");
    let proposed = MappingDocument::from_mappings(
        1,
        vec![
            vec![
                "example.media://a".to_owned(),
                "example.media://z".to_owned(),
            ],
            vec![
                "example.media://c".to_owned(),
                "example.media://d".to_owned(),
            ],
        ],
    )
    .expect("proposed document should parse");

    let diff = canonical_row_set_diff(&current, &proposed);
    assert_eq!(
        diff.added.iter().map(row_json).collect::<Vec<_>>(),
        vec![r#"["example.media://c","example.media://d"]"#]
    );
    assert_eq!(
        diff.removed.iter().map(row_json).collect::<Vec<_>>(),
        vec![r#"["example.media://a","example.media://b"]"#]
    );
}

fn row_json(row: &Vec<String>) -> String {
    serde_json::to_string(row).expect("canonical mapping row serializes")
}

#[test]
fn lifecycle_errors_preserve_mapping_diagnostics() {
    let error = MappingDocument::from_mappings(1, vec![vec!["example.media://a".to_owned()]])
        .expect_err("singleton row should fail");
    let lifecycle_error = MappingLifecycleError::<()>::InvalidDocument(error);
    assert!(matches!(
        lifecycle_error,
        MappingLifecycleError::InvalidDocument(error) if error.code() == "row_endpoint_count"
    ));
}
