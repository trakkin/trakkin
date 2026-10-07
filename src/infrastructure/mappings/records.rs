use super::{MAXIMUM_CONTENT_BYTES, invalid_request};
use crate::application::error::ApplicationError;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use trakkin_mappings_language::{Decisions, Record, parse_record};

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub(super) struct OriginRecord {
    pub(super) canonical_record: String,
    pub(super) rejected: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(super) struct StoredRecord {
    pub(super) claim_id: String,
    pub(super) origins: BTreeMap<String, OriginRecord>,
}

impl StoredRecord {
    pub(super) fn effective(
        &self,
        disabled: &BTreeSet<String>,
    ) -> Result<Option<(Record, bool)>, ApplicationError> {
        let local = self.origins.contains_key("local") && !disabled.contains("local");
        let mut effective: Option<(Record, bool)> = None;
        for (source, origin) in &self.origins {
            let incoming = read_record(&origin.canonical_record)?;
            if incoming.statement.claim_id().to_string() != self.claim_id {
                return Err(invalid_request(
                    "mapping origin has an inconsistent claim ID",
                ));
            }
            if disabled.contains(source) || (local && source != "local") {
                continue;
            }
            if let Some((record, rejected)) = &mut effective {
                for annotation in incoming.metadata {
                    if !record.metadata.contains(&annotation) {
                        record.metadata.push(annotation);
                    }
                }
                *rejected |= origin.rejected;
            } else {
                effective = Some((incoming, origin.rejected));
            }
        }
        Ok(effective)
    }

    pub(super) fn decide(&mut self, rejected: bool) -> Result<(), ApplicationError> {
        let (record, _) = self
            .effective(&BTreeSet::new())?
            .ok_or_else(|| invalid_request("mapping claim has no origins"))?;
        self.origins.insert(
            "local".into(),
            OriginRecord {
                canonical_record: record.canonical(),
                rejected,
            },
        );
        Ok(())
    }
}

pub(super) fn save_record(
    records: &mut BTreeMap<String, StoredRecord>,
    record: Record,
    replaces: Option<&str>,
) -> Result<(), ApplicationError> {
    let claim_id = record.statement.claim_id().to_string();
    if replaces.is_some_and(|original| original != claim_id)
        && let Some(stored) = records.get(&claim_id)
        && stored
            .effective(&BTreeSet::new())?
            .is_some_and(|(_, rejected)| rejected)
    {
        return Err(invalid_request(
            "replacement claim is suppressed; restore it explicitly before replacing",
        ));
    }
    if let Some(original) = replaces {
        let stored = records
            .get_mut(original)
            .ok_or_else(|| invalid_request("replacement target does not exist"))?;
        if original != claim_id {
            stored.decide(true)?;
        }
    }
    let stored = records.entry(claim_id.clone()).or_insert(StoredRecord {
        claim_id,
        origins: BTreeMap::new(),
    });
    let rejected = stored
        .origins
        .get("local")
        .is_some_and(|origin| origin.rejected);
    stored.origins.insert(
        "local".into(),
        OriginRecord {
            canonical_record: record.canonical(),
            rejected,
        },
    );
    Ok(())
}

pub(super) fn import_records(
    records: &mut BTreeMap<String, StoredRecord>,
    source: &str,
    accepted: Vec<Record>,
    rejected: Vec<Record>,
) -> Result<(), ApplicationError> {
    let incoming = Decisions::from_records(accepted, rejected)
        .map_err(|error| invalid_request(&error.to_string()))?;
    if source != "local" {
        for record in records.values_mut() {
            record.origins.remove(source);
        }
        records.retain(|_, record| !record.origins.is_empty());
    }
    for (claim_id, decision) in incoming {
        let claim_id = claim_id.to_string();
        let stored = records.entry(claim_id.clone()).or_insert(StoredRecord {
            claim_id,
            origins: BTreeMap::new(),
        });
        stored.origins.insert(
            source.into(),
            OriginRecord {
                canonical_record: decision.record.canonical(),
                rejected: decision.rejected,
            },
        );
    }
    Ok(())
}

pub(super) fn read_record(text: &str) -> Result<Record, ApplicationError> {
    if text.len() > MAXIMUM_CONTENT_BYTES {
        return Err(invalid_request("mapping content exceeds 2 MiB"));
    }
    parse_record(text).map_err(|error| invalid_request(&error.to_string()))
}

pub(super) fn local_decisions(
    records: &mut BTreeMap<String, StoredRecord>,
) -> Result<Decisions, ApplicationError> {
    let mut accepted = Vec::new();
    let mut rejected = Vec::new();
    for stored in records.values_mut() {
        let Some(origin) = stored.origins.get_mut("local") else {
            continue;
        };
        let mut record = read_record(&origin.canonical_record)?;
        if origin.rejected {
            if !record.metadata.iter().any(|line| {
                line.strip_prefix("#@note ")
                    .is_some_and(|note| !note.trim().is_empty())
            }) {
                record
                    .metadata
                    .push("#@note Explicitly suppressed in Trakkin".into());
                origin.canonical_record = record.canonical();
            }
            rejected.push(record);
        } else {
            accepted.push(record);
        }
    }
    Decisions::from_records(accepted, rejected).map_err(|error| invalid_request(&error.to_string()))
}
