use super::*;
use trakkin_mappings_corpus::{ExclusiveSourcePair, Resolution};

fn stored(text: &str, source: &str) -> StoredRecord {
    let record = read_record(text).unwrap();
    StoredRecord {
        claim_id: record.statement.claim_id().to_string(),
        origins: BTreeMap::from([(
            source.into(),
            OriginRecord {
                record,
                rejected: false,
            },
        )]),
    }
}
fn policy() -> ConflictPolicy {
    ConflictPolicy {
        format_version: 1,
        exclusive_source_pairs: vec![ExclusiveSourcePair {
            origin_source: "a".into(),
            target_source: "b".into(),
        }],
    }
}

#[test]
fn large_dynamic_bindings_index_only_members_with_shared_extent() {
    let mut record = stored(
        "a://show :: episode=* <~> b://show :: episode=*",
        "community",
    );
    let origin = &mut record.origins.get_mut("community").unwrap().record;
    let bindings = origin
        .statement
        .left
        .selections()
        .into_iter()
        .chain(origin.statement.right.selections())
        .map(|selection| {
            let count = if selection.source() == "a" {
                5000
            } else {
                4500
            };
            (
                selection.selection_key(),
                Resolution {
                    units: (1..=count)
                        .map(|ordinal| format!("{}://episode/{ordinal}", selection.source()))
                        .collect(),
                    ordered: true,
                    coordinates: None,
                    error: None,
                },
            )
        })
        .collect();
    Bindings(bindings).write(origin).unwrap();
    let index = MappingIndex::build(
        BTreeMap::from([(record.claim_id.clone(), record)]),
        &BTreeSet::new(),
    )
    .unwrap();
    assert_eq!(index.state().units, 9000);
    assert!(index.units.contains_key("a://episode/4500"));
    assert!(!index.units.contains_key("a://episode/4501"));
    assert!(index.units.contains_key("b://episode/4500"));
}

#[test]
fn source_urls_are_bounded_and_do_not_embed_credentials() {
    assert_eq!(
        trakkin_mappings_corpus::git::validate_remote("HTTP://EXAMPLE.COM:80/repo.git").unwrap(),
        "http://example.com/repo.git"
    );
    for value in [
        "https://example.com/feed\n.json",
        "https://user:password@example.com/repo.git",
        "https://example.com/repo.git?token=secret",
        "file:///private",
    ] {
        assert!(trakkin_mappings_corpus::git::validate_remote(value).is_err());
    }
}

#[test]
fn changing_one_claim_updates_conflict_status_without_affecting_other_claims() {
    let first = stored("a://1 <=> b://1\n", "community");
    let second = stored("a://2 <=> b://2\n", "community");
    let first_id = first.claim_id.clone();
    let second_id = second.claim_id.clone();
    let mut index = MappingIndex::build(
        BTreeMap::from([(first_id.clone(), first), (second_id.clone(), second)]),
        &BTreeSet::new(),
    )
    .unwrap();
    index
        .apply(BTreeMap::new(), &BTreeSet::new(), policy())
        .unwrap();
    let alternative = stored("a://1 <=> b://3\n", "other");
    let alternative_id = alternative.claim_id.clone();
    index
        .apply(
            BTreeMap::from([(alternative_id.clone(), Some(alternative))]),
            &BTreeSet::new(),
            policy(),
        )
        .unwrap();
    assert_eq!(index.state().conflicting, 2);
    assert_eq!(index.state().resolved, 1);
    assert_eq!(
        index.claims[&second_id].row.status,
        MappingRecordStatus::Resolved
    );
    assert_eq!(
        index.claims[&first_id].row.status,
        MappingRecordStatus::Conflicting
    );
    index
        .apply(
            BTreeMap::from([(alternative_id, None)]),
            &BTreeSet::new(),
            policy(),
        )
        .unwrap();
    assert_eq!(index.state().conflicting, 0);
    assert_eq!(index.state().resolved, 2);
    assert_eq!(index.state().units, 4);
    assert_eq!(
        index.claims[&first_id].row.status,
        MappingRecordStatus::Resolved
    );
}

#[test]
fn exclusivity_is_explicit_and_directional() {
    let records = [
        stored("a://1 <=> b://1\n", "one"),
        stored("a://1 <=> b://2\n", "two"),
        stored("a://2 <=> b://1\n", "three"),
    ]
    .into_iter()
    .map(|record| (record.claim_id.clone(), record))
    .collect();
    let mut index = MappingIndex::build(records, &BTreeSet::new()).unwrap();
    assert_eq!(index.state().resolved, 3);
    index
        .apply(BTreeMap::new(), &BTreeSet::new(), policy())
        .unwrap();
    assert_eq!(index.state().conflicting, 2);
    assert_eq!(index.state().resolved, 1);
}

#[test]
fn published_bindings_preserve_n_to_m_coverage_and_missing_bindings_are_unresolved() {
    let mut record =
        read_record("a://show :: episode=1..2 @2 <~> [b://1,b://2,b://3,b://4]\n").unwrap();
    assert_eq!(
        compile(&record, false, vec!["community".into()]).row.status,
        MappingRecordStatus::Unresolved
    );
    let selection = record
        .statement
        .left
        .selections()
        .into_iter()
        .chain(record.statement.right.selections())
        .find(|selection| selection.selector.is_some())
        .unwrap();
    let bindings = Bindings(BTreeMap::from([(
        selection.selection_key(),
        Resolution {
            units: vec!["a://episode/1".into(), "a://episode/2".into()],
            ordered: true,
            coordinates: None,
            error: None,
        },
    )]));
    bindings.write(&mut record).unwrap();
    let claim = compile(&record, false, vec!["community".into()]);
    assert_eq!(claim.row.status, MappingRecordStatus::Resolved);
    assert_eq!(claim.evaluation.unwrap().alignments.len(), 4);
}

#[test]
fn local_decisions_override_upstream_but_do_not_hide_invalid_origin_ids() {
    let mut record = stored("a://1 <=> b://1\n", "community");
    record.decide(true, &BTreeSet::new()).unwrap();
    assert!(
        record
            .effective(&BTreeSet::new())
            .unwrap()
            .unwrap()
            .rejected
    );
    record.origins.insert(
        "invalid".into(),
        OriginRecord {
            record: read_record("a://2 <=> b://2\n").unwrap(),
            rejected: false,
        },
    );
    assert!(record.effective(&BTreeSet::new()).is_err());
}

#[test]
fn upstream_binding_conflicts_are_explicit_and_local_decisions_ignore_disabled_origins() {
    let mut record = stored("a://show :: episode=* <~> b://1", "first");
    let key = record.origins["first"]
        .record
        .statement
        .left
        .selections()
        .into_iter()
        .chain(record.origins["first"].record.statement.right.selections())
        .find(|selection| selection.selector.is_some())
        .unwrap()
        .selection_key();
    let mut second = record.origins["first"].clone();
    for (origin, count) in [
        (&mut record.origins.get_mut("first").unwrap().record, 1),
        (&mut second.record, 2),
    ] {
        Bindings(BTreeMap::from([(
            key.clone(),
            Resolution {
                units: (1..=count)
                    .map(|ordinal| format!("a://episode/{ordinal}"))
                    .collect(),
                ordered: true,
                coordinates: None,
                error: None,
            },
        )]))
        .write(origin)
        .unwrap();
    }
    record.origins.insert("second".into(), second);
    let id = record.claim_id.clone();
    let mut index = MappingIndex::build(
        BTreeMap::from([(id.clone(), record.clone())]),
        &BTreeSet::new(),
    )
    .unwrap();
    assert_eq!(
        index.claims[&id].row.status,
        MappingRecordStatus::Conflicting
    );
    assert!(index.claims[&id].row.diagnostics[0].starts_with("origin conflict:"));
    assert_eq!(index.state().units, 0);
    assert!(record.decide(false, &BTreeSet::new()).is_err());
    let disabled = BTreeSet::from(["second".into()]);
    record.decide(false, &disabled).unwrap();
    assert_eq!(
        Bindings::read(&record.origins["local"].record).unwrap().0[&key]
            .units
            .len(),
        1
    );
    index
        .apply(
            BTreeMap::from([(id.clone(), Some(record))]),
            &disabled,
            ConflictPolicy::default(),
        )
        .unwrap();
    assert_eq!(index.claims[&id].row.status, MappingRecordStatus::Resolved);
}

#[test]
fn equivalent_bindings_merge_without_duplicate_resolution_annotations() {
    let mut record = stored("a://show :: episode=* <~> b://1", "first");
    let key = record.origins["first"]
        .record
        .statement
        .left
        .selections()
        .into_iter()
        .chain(record.origins["first"].record.statement.right.selections())
        .find(|selection| selection.selector.is_some())
        .unwrap()
        .selection_key();
    Bindings(BTreeMap::from([(
        key,
        Resolution {
            units: vec!["a://episode/1".into()],
            ordered: true,
            coordinates: None,
            error: None,
        },
    )]))
    .write(&mut record.origins.get_mut("first").unwrap().record)
    .unwrap();
    let mut second = record.origins["first"].clone();
    for line in &mut second.record.metadata {
        *line = line.replace("\"units\":", "\"units\": ");
    }
    record.origins.insert("second".into(), second);
    let effective = record.effective(&BTreeSet::new()).unwrap().unwrap();
    assert!(effective.binding_conflicts.is_empty());
    assert_eq!(
        effective
            .record
            .metadata
            .iter()
            .filter(|line| line.starts_with("#@resolve "))
            .count(),
        1
    );
    assert_eq!(
        compile(&effective.record, false, vec![]).row.status,
        MappingRecordStatus::Resolved
    );
}
