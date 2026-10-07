use super::records::OriginRecord;
use super::*;
use trakkin_mappings_language::parse_record;

#[test]
fn source_urls_are_bounded_and_canonical() {
    assert_eq!(
        trakkin_mappings_corpus::git::validate_remote("HTTP://EXAMPLE.COM:80/repository.git")
            .unwrap(),
        "http://example.com/repository.git"
    );
    for value in [
        "https://example.com/feed\n.json".to_owned(),
        format!("https://example.com/{}", "x".repeat(2048)),
        "https://user:password@example.com/feed.json".to_owned(),
        "https://example.com/feed.json?token=secret".to_owned(),
        "file:///tmp/feed.json".to_owned(),
    ] {
        assert!(trakkin_mappings_corpus::git::validate_remote(&value).is_err());
    }
}

#[test]
fn local_decisions_do_not_hide_inconsistent_origin_ids() {
    let mut records = imported();
    let stored = records.values_mut().next().unwrap();
    let local = stored.origins.values().next().unwrap().clone();
    stored.origins.insert("local".into(), local);
    stored.origins.insert(
        "inconsistent".into(),
        OriginRecord {
            canonical_record: CORRECTION.into(),
            rejected: false,
        },
    );
    assert!(stored.effective(&BTreeSet::new()).is_err());
}

#[test]
fn local_suppressions_are_valid_committed_corpus_decisions() {
    let mut records = imported();
    let claim_id = records.keys().next().unwrap().clone();
    records.get_mut(&claim_id).unwrap().decide(true).unwrap();
    let decisions = super::records::local_decisions(&mut records).unwrap();
    let corpus = trakkin_mappings_corpus::Corpus::default()
        .with_decisions(decisions)
        .unwrap();
    let entry = corpus.get(&claim_id.parse().unwrap()).unwrap();
    assert_eq!(entry.status, trakkin_mappings_corpus::Status::Rejected);
    assert!(
        entry
            .record
            .metadata
            .contains(&"#@note Explicitly suppressed in Trakkin".into())
    );
    assert_eq!(
        entry.record.canonical(),
        records[&claim_id].origins["local"].canonical_record
    );
    assert_eq!(records[&claim_id].origins.len(), 2);
}

#[test]
fn local_acceptance_overrides_upstream_rejection_but_preserves_local_suppression() {
    let mut records = BTreeMap::new();
    let record = parse_record("com.imdb://title/tt0133093 <=> org.themoviedb://movie/603").unwrap();
    let id = record.statement.claim_id().to_string();
    import_records(&mut records, "upstream", vec![], vec![record.clone()]).unwrap();
    save_record(&mut records, record.clone(), None).unwrap();
    assert!(!is_rejected(&records[&id]));
    let stored = records.get_mut(&id).unwrap();
    stored.decide(true).unwrap();
    save_record(&mut records, record, None).unwrap();
    assert!(is_rejected(&records[&id]));
}

#[test]
fn disabled_origins_do_not_execute_and_local_decisions_remain_active() {
    let mut records = BTreeMap::new();
    let record = parse_record("com.imdb://title/tt0133093 <=> org.themoviedb://movie/603").unwrap();
    let claim_id = record.statement.claim_id().to_string();
    import_records(
        &mut records,
        "import:upstream",
        vec![record.clone()],
        vec![],
    )
    .unwrap();
    let resolver = MappingResolver { snapshots: None };
    let enabled = MappingIndex::build(records.clone(), &resolver, &BTreeSet::new()).unwrap();
    let disabled = BTreeSet::from(["import:upstream".into()]);
    let hidden = MappingIndex::build(records.clone(), &resolver, &disabled).unwrap();
    assert_eq!(hidden.state().records, 0);
    assert!(hidden.units.is_empty());
    assert_eq!(hidden.records.len(), 1);
    assert_ne!(hidden.fingerprint, enabled.fingerprint);
    save_record(&mut records, record, None).unwrap();
    let local = MappingIndex::build(records, &resolver, &disabled).unwrap();
    assert_eq!(local.claims[0].row.claim_id, claim_id);
    assert_eq!(local.claims[0].row.sources, ["import:upstream", "local"]);
    assert_eq!(local.state().resolved, 1);
}

const ORIGINAL: &str = "com.imdb://title/tt0133093 <=> org.themoviedb://movie/603";
const CORRECTION: &str = "com.imdb://title/tt0133093 <=> org.themoviedb://movie/604";

fn is_rejected(record: &StoredRecord) -> bool {
    record.effective(&BTreeSet::new()).unwrap().unwrap().1
}

fn index(records: BTreeMap<String, StoredRecord>) -> MappingIndex {
    MappingIndex::build(
        records,
        &MappingResolver { snapshots: None },
        &BTreeSet::new(),
    )
    .unwrap()
}

fn imported() -> BTreeMap<String, StoredRecord> {
    let mut records = BTreeMap::new();
    import_records(
        &mut records,
        "import:community",
        vec![parse_record(ORIGINAL).unwrap()],
        Vec::new(),
    )
    .unwrap();
    records
}

#[test]
fn local_replacement_suppresses_original_without_removing_it() {
    let mut records = imported();
    let original = parse_record(ORIGINAL)
        .unwrap()
        .statement
        .claim_id()
        .to_string();
    save_record(
        &mut records,
        parse_record(CORRECTION).unwrap(),
        Some(&original),
    )
    .unwrap();
    assert_eq!(records.len(), 2);
    assert!(is_rejected(&records[&original]));
    let index = index(records);
    let resolved = index
        .resolve(ResolveMappingUnits {
            expected_fingerprint: None,
            unit_keys: vec!["com.imdb://title/tt0133093".into()],
        })
        .unwrap();
    assert_eq!(resolved.units[0].matches.len(), 1);
    assert!(
        resolved.units[0].matches[0].right.members[0]
            .unit_key
            .ends_with("/604")
    );
}

#[test]
fn replacement_cannot_implicitly_restore_a_suppressed_claim() {
    let mut records = imported();
    import_records(
        &mut records,
        "import:custom",
        Vec::new(),
        vec![parse_record(CORRECTION).unwrap()],
    )
    .unwrap();
    let previous = records.clone();
    let original = parse_record(ORIGINAL)
        .unwrap()
        .statement
        .claim_id()
        .to_string();
    assert!(
        save_record(
            &mut records,
            parse_record(CORRECTION).unwrap(),
            Some(&original)
        )
        .is_err()
    );
    assert_eq!(records, previous);
}

#[test]
fn refreshed_source_can_withdraw_rejection_without_erasing_local_decisions() {
    let mut records = imported();
    import_records(
        &mut records,
        "import:other",
        Vec::new(),
        vec![parse_record(ORIGINAL).unwrap()],
    )
    .unwrap();
    let original = parse_record(ORIGINAL)
        .unwrap()
        .statement
        .claim_id()
        .to_string();
    assert!(is_rejected(&records[&original]));
    import_records(
        &mut records,
        "import:other",
        vec![parse_record(ORIGINAL).unwrap()],
        Vec::new(),
    )
    .unwrap();
    assert!(!is_rejected(&records[&original]));
    records.get_mut(&original).unwrap().decide(true).unwrap();
    import_records(
        &mut records,
        "import:other",
        vec![parse_record(ORIGINAL).unwrap()],
        Vec::new(),
    )
    .unwrap();
    assert!(is_rejected(&records[&original]));
}

#[test]
fn refresh_preserves_local_decisions_and_duplicates_merge_origins() {
    let mut records = imported();
    import_records(
        &mut records,
        "import:custom",
        vec![parse_record(ORIGINAL).unwrap()],
        Vec::new(),
    )
    .unwrap();
    assert_eq!(records.len(), 1);
    let original = parse_record(ORIGINAL)
        .unwrap()
        .statement
        .claim_id()
        .to_string();
    save_record(
        &mut records,
        parse_record(CORRECTION).unwrap(),
        Some(&original),
    )
    .unwrap();
    import_records(
        &mut records,
        "import:community",
        vec![parse_record(ORIGINAL).unwrap()],
        Vec::new(),
    )
    .unwrap();
    assert!(is_rejected(&records[&original]));
    assert_eq!(
        records[&original]
            .origins
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["import:community", "import:custom", "local"]
    );
}

#[test]
fn conflicting_claims_are_visible_and_not_silently_prioritized() {
    let mut records = imported();
    save_record(&mut records, parse_record(CORRECTION).unwrap(), None).unwrap();
    let index = index(records);
    assert!(
        index
            .claims
            .iter()
            .all(|claim| claim.row.status == MappingRecordStatus::Conflicting)
    );
    let resolved = index
        .resolve(ResolveMappingUnits {
            expected_fingerprint: None,
            unit_keys: vec![
                "com.imdb://title/tt0133093".into(),
                "org.themoviedb://movie/603".into(),
            ],
        })
        .unwrap();
    assert!(resolved.units.iter().all(|unit| unit.matches.is_empty()));
}

#[test]
fn shared_evaluator_preserves_weighted_alignment_offsets() {
    let mut records = BTreeMap::new();
    save_record(&mut records, parse_record("com.imdb://title/tt0133093 @2 <~> [org.themoviedb://movie/603,org.themoviedb://movie/604]").unwrap(), None).unwrap();
    let index = index(records);
    let evaluation = index.claims[0].evaluation.as_ref().unwrap();
    assert_eq!(evaluation.alignments.len(), 2);
    assert_eq!(
        (
            evaluation.alignments[1].left_offset,
            evaluation.alignments[1].right_offset
        ),
        (0, 1)
    );
}

#[test]
fn selector_claims_remain_unresolved_without_native_metadata() {
    let mut records = BTreeMap::new();
    save_record(&mut records, parse_record("co.anilist://media/1 :: episode=1..2 <=> org.themoviedb://tv/10 :: season=1,episode=1..2").unwrap(), None).unwrap();
    let index = index(records);
    assert_eq!(index.claims[0].row.status, MappingRecordStatus::Unresolved);
    assert!(index.units.is_empty());
}

#[test]
fn native_metadata_resolves_ranges_through_the_shared_evaluator() {
    use serde_json::json;
    use std::fs;
    use trakkin_mappings_corpus::snapshots;

    let directory = tempfile::tempdir().unwrap();
    let mut requests = Vec::new();
    for (namespace, adapter, records) in [
        (
            "co.anilist",
            "anilist",
            json!({"1":{"id":1,"type":"ANIME","episodes":2}}),
        ),
        (
            "org.themoviedb",
            "tmdb",
            json!({
                "tv:10":{"id":10,"seasons":[{"id":20,"season_number":1}]},
                "season:20":{"id":20,"season_number":1,"episodes":[{"id":30,"season_number":1,"episode_number":1},{"id":31,"season_number":1,"episode_number":2}]}
            }),
        ),
    ] {
        let input = directory.path().join(namespace);
        fs::create_dir_all(&input).unwrap();
        fs::write(
            input.join("records.json"),
            serde_json::to_vec(&json!({"records":records})).unwrap(),
        )
        .unwrap();
        let request = input.join("capture.json");
        fs::write(&request, serde_json::to_vec(&json!({"sources":{namespace:{"file":"records.json","adapter":adapter,"scope":["fixture"],"complete":true,"captured_at":"2026-10-13T00:00:00Z"}}})).unwrap()).unwrap();
        requests.push(request);
    }
    let snapshots =
        snapshots::capture_many(&requests, &directory.path().join("artifacts/snapshot")).unwrap();
    let resolver = MappingResolver {
        snapshots: Some(snapshots),
    };
    let mut records = BTreeMap::new();
    save_record(&mut records, parse_record("co.anilist://media/1 :: episode=1..2 <=> org.themoviedb://tv/10 :: season=1,episode=1..2").unwrap(), None).unwrap();
    let index = MappingIndex::build(records, &resolver, &BTreeSet::new()).unwrap();
    assert_eq!(index.claims[0].row.status, MappingRecordStatus::Resolved);
    assert_eq!(
        index.claims[0]
            .evaluation
            .as_ref()
            .unwrap()
            .alignments
            .len(),
        2
    );
    assert_eq!(index.units.len(), 4);
}

#[test]
fn changed_records_invalidate_cursors_and_write_fingerprints() {
    let records = imported();
    let previous = index(records.clone()).fingerprint;
    let mut records = records;
    save_record(&mut records, parse_record(CORRECTION).unwrap(), None).unwrap();
    assert!(index(records).check_fingerprint(Some(&previous)).is_err());
}
