use super::{MAXIMUM_CONTENT_BYTES, invalid_request};
use crate::application::error::ApplicationError;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use trakkin_mappings_corpus::Bindings;
use trakkin_mappings_language::{Record, parse_record};

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub(super) struct OriginRecord {
    pub(super) record: Record,
    pub(super) rejected: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(super) struct StoredRecord {
    pub(super) claim_id: String,
    pub(super) origins: BTreeMap<String, OriginRecord>,
}

pub(super) struct EffectiveRecord {
    pub(super) record: Record,
    pub(super) rejected: bool,
    pub(super) binding_conflicts: BTreeSet<String>,
}

impl StoredRecord {
    pub(super) fn effective(
        &self,
        disabled: &BTreeSet<String>,
    ) -> Result<Option<EffectiveRecord>, ApplicationError> {
        let local = self.origins.contains_key("local") && !disabled.contains("local");
        let mut effective: Option<(Record, bool)> = None;
        let mut bindings = Bindings::default();
        let mut binding_conflicts = BTreeSet::new();
        for (source, origin) in &self.origins {
            if origin.record.statement.claim_id().to_string() != self.claim_id {
                return Err(invalid_request(
                    "mapping origin has an inconsistent claim ID",
                ));
            }
            if disabled.contains(source) || (local && source != "local") {
                continue;
            }
            for (key, resolution) in Bindings::read(&origin.record)
                .map_err(|error| invalid_request(&error.to_string()))?
                .0
            {
                if bindings
                    .0
                    .get(&key)
                    .is_some_and(|existing| existing != &resolution)
                {
                    binding_conflicts.insert(key.clone());
                }
                bindings.0.insert(key, resolution);
            }
            if let Some((record, rejected)) = &mut effective {
                for annotation in &origin.record.metadata {
                    if !annotation.starts_with("#@resolve ")
                        && !annotation.starts_with("#@decision ")
                        && !record.metadata.contains(annotation)
                    {
                        record.metadata.push(annotation.clone());
                    }
                }
                *rejected |= origin.rejected;
            } else {
                effective = Some((origin.record.clone(), origin.rejected));
            }
        }
        effective
            .map(|(mut record, rejected)| {
                for key in &binding_conflicts {
                    bindings.0.remove(key);
                }
                record
                    .metadata
                    .retain(|line| !line.starts_with("#@decision "));
                bindings
                    .write(&mut record)
                    .map_err(|error| invalid_request(&error.to_string()))?;
                Ok(EffectiveRecord {
                    record,
                    rejected,
                    binding_conflicts,
                })
            })
            .transpose()
    }
    pub(super) fn decide(
        &mut self,
        rejected: bool,
        disabled: &BTreeSet<String>,
    ) -> Result<(), ApplicationError> {
        let effective = self
            .effective(disabled)?
            .ok_or_else(|| invalid_request("mapping claim has no origins"))?;
        if !rejected && !effective.binding_conflicts.is_empty() {
            return Err(invalid_request(
                "upstream selector bindings disagree; author an explicit local mapping",
            ));
        }
        let mut record = effective.record;
        if rejected
            && !record
                .metadata
                .iter()
                .any(|line| line.starts_with("#@note "))
        {
            record
                .metadata
                .push("#@note Explicitly suppressed in Trakkin".into());
        }
        self.origins
            .insert("local".into(), OriginRecord { record, rejected });
        Ok(())
    }
}

pub(super) fn save_record(
    records: &mut BTreeMap<String, StoredRecord>,
    record: Record,
    replaces: Option<&str>,
    disabled: &BTreeSet<String>,
) -> Result<(), ApplicationError> {
    let claim_id = record.statement.claim_id().to_string();
    if replaces.is_some_and(|original| original != claim_id)
        && records.get(&claim_id).is_some_and(|stored| {
            stored
                .effective(disabled)
                .is_ok_and(|effective| effective.is_some_and(|effective| effective.rejected))
        })
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
            stored.decide(true, disabled)?;
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
    stored
        .origins
        .insert("local".into(), OriginRecord { record, rejected });
    Ok(())
}

pub(super) fn read_record(text: &str) -> Result<Record, ApplicationError> {
    if text.len() > MAXIMUM_CONTENT_BYTES {
        return Err(invalid_request("mapping content exceeds 2 MiB"));
    }
    parse_record(text).map_err(|error| invalid_request(&error.to_string()))
}
