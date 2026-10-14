use super::{MAXIMUM_BATCH, invalid_request, records::StoredRecord};
use crate::application::{
    error::{ApplicationError, ApplicationErrorCode},
    mappings::*,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use trakkin_mappings_corpus::{Bindings, ConflictPolicy};
use trakkin_mappings_language::{
    Evaluation, Record, ResolvedExpression, SemanticErrorKind, evaluate, semantic_revision, sha256,
};

#[derive(Clone)]
pub(super) struct Claim {
    pub(super) row: MappingResult,
    pub(super) evaluation: Option<Evaluation>,
    pub(super) left_expression: String,
    pub(super) right_expression: String,
}

pub(super) struct MappingIndex {
    pub(super) fingerprint: String,
    pub(super) records: BTreeMap<String, StoredRecord>,
    pub(super) claims: BTreeMap<String, Claim>,
    pub(super) units: BTreeMap<String, Vec<(String, MappingUnitSide, usize)>>,
    pub(super) position: i64,
    pub(super) disabled: BTreeSet<String>,
    targets: BTreeMap<(String, String), BTreeMap<String, BTreeSet<String>>>,
    policy: ConflictPolicy,
    counts: [u64; 5],
    active_units: BTreeSet<String>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PageCursor {
    fingerprint: String,
    query: String,
    position: usize,
}

pub(super) fn compile(record: &Record, rejected: bool, sources: Vec<String>) -> Claim {
    let mut row = MappingResult {
        claim_id: record.statement.claim_id().to_string(),
        canonical_statement: record.statement.canonical(),
        canonical_record: record.canonical(),
        operator: record.statement.operator.text().into(),
        metadata: record.metadata.clone(),
        rejected,
        sources,
        status: if rejected {
            MappingRecordStatus::Rejected
        } else {
            MappingRecordStatus::Resolved
        },
        diagnostics: Vec::new(),
    };
    let evaluation = if rejected {
        None
    } else {
        match Bindings::read(record)
            .map_err(|error| trakkin_mappings_language::SemanticError::invalid(error.to_string()))
            .and_then(|bindings| evaluate(&record.statement, &bindings))
        {
            Ok(evaluation) => {
                if evaluation.left_unmapped_extent != 0 || evaluation.right_unmapped_extent != 0 {
                    row.diagnostics.push(format!(
                        "unmapped extent: {} left, {} right",
                        evaluation.left_unmapped_extent, evaluation.right_unmapped_extent,
                    ));
                }
                Some(evaluation)
            }
            Err(error) => {
                row.status = match error.kind {
                    SemanticErrorKind::Unresolved => MappingRecordStatus::Unresolved,
                    SemanticErrorKind::Invalid => MappingRecordStatus::Invalid,
                };
                row.diagnostics.push(error.message);
                None
            }
        }
    };
    Claim {
        row,
        evaluation,
        left_expression: record.statement.left.canonical(),
        right_expression: record.statement.right.canonical(),
    }
}

impl MappingIndex {
    pub(super) fn build(
        records: BTreeMap<String, StoredRecord>,
        disabled: &BTreeSet<String>,
    ) -> Result<Self, ApplicationError> {
        let fingerprint = sha256(
            &serde_json::to_vec(&(semantic_revision(), &records, disabled))
                .map_err(|error| ApplicationError::internal(error.to_string()))?,
        );
        let mut index = Self {
            fingerprint,
            records: BTreeMap::new(),
            claims: BTreeMap::new(),
            units: BTreeMap::new(),
            position: 0,
            disabled: disabled.clone(),
            targets: BTreeMap::new(),
            policy: ConflictPolicy::default(),
            counts: [0; 5],
            active_units: BTreeSet::new(),
        };
        index.apply(
            records
                .into_iter()
                .map(|(id, record)| (id, Some(record)))
                .collect(),
            disabled,
            ConflictPolicy::default(),
        )?;
        Ok(index)
    }

    pub(super) fn apply(
        &mut self,
        changes: BTreeMap<String, Option<StoredRecord>>,
        disabled: &BTreeSet<String>,
        policy: ConflictPolicy,
    ) -> Result<(), ApplicationError> {
        self.disabled.clone_from(disabled);
        let mut affected = BTreeSet::new();
        let mut unit_keys = BTreeSet::new();
        for (id, record) in changes {
            affected.insert(id.clone());
            if let Some(old) = self.claims.remove(&id) {
                self.counts[status_index(old.row.status)] -= 1;
                if let Some(evaluation) = &old.evaluation {
                    for ((origin, source), targets) in self.policy.targets(evaluation) {
                        let key = (origin, source);
                        if let Some(alternatives) = self.targets.get_mut(&key) {
                            affected.extend(alternatives.values().flatten().cloned());
                            for target in targets {
                                if let Some(claims) = alternatives.get_mut(&target) {
                                    claims.remove(&id);
                                    if claims.is_empty() {
                                        alternatives.remove(&target);
                                    }
                                }
                            }
                            if alternatives.is_empty() {
                                self.targets.remove(&key);
                            }
                        }
                    }
                    for member in evaluation
                        .left
                        .members
                        .iter()
                        .chain(&evaluation.right.members)
                    {
                        unit_keys.insert(member.reference.clone());
                        if let Some(matches) = self.units.get_mut(&member.reference) {
                            matches.retain(|(claim, _, _)| claim != &id);
                            if matches.is_empty() {
                                self.units.remove(&member.reference);
                            }
                        }
                    }
                }
            }
            self.records.remove(&id);
            if let Some(stored) = record {
                if let Some(effective) = stored.effective(disabled)? {
                    let mut claim = compile(
                        &effective.record,
                        effective.rejected,
                        stored.origins.keys().cloned().collect(),
                    );
                    if !effective.rejected && !effective.binding_conflicts.is_empty() {
                        claim.row.status = MappingRecordStatus::Conflicting;
                        claim.row.diagnostics = effective
                            .binding_conflicts
                            .into_iter()
                            .map(|key| {
                                format!("origin conflict: different published bindings for {key}")
                            })
                            .collect();
                        claim.evaluation = None;
                    }
                    if let Some(evaluation) = &claim.evaluation {
                        let all_members = evaluation.alignments.is_empty();
                        let mut left_aligned = vec![all_members; evaluation.left.members.len()];
                        let mut right_aligned = vec![all_members; evaluation.right.members.len()];
                        for alignment in &evaluation.alignments {
                            left_aligned[alignment.left] = true;
                            right_aligned[alignment.right] = true;
                        }
                        for (side, expression, aligned) in [
                            (MappingUnitSide::Left, &evaluation.left, left_aligned),
                            (MappingUnitSide::Right, &evaluation.right, right_aligned),
                        ] {
                            for (ordinal, member) in expression.members.iter().enumerate() {
                                if aligned[ordinal] {
                                    self.units
                                        .entry(member.reference.clone())
                                        .or_default()
                                        .push((id.clone(), side, ordinal));
                                    unit_keys.insert(member.reference.clone());
                                }
                            }
                        }
                        for (key, targets) in self.policy.targets(evaluation) {
                            let alternatives = self.targets.entry(key).or_default();
                            for target in targets {
                                alternatives.entry(target).or_default().insert(id.clone());
                            }
                            affected.extend(alternatives.values().flatten().cloned());
                        }
                    }
                    self.counts[status_index(claim.row.status)] += 1;
                    self.claims.insert(id.clone(), claim);
                }
                self.records.insert(id, stored);
            }
        }
        if serde_json::to_value(&policy)
            .map_err(|error| ApplicationError::internal(error.to_string()))?
            != serde_json::to_value(&self.policy)
                .map_err(|error| ApplicationError::internal(error.to_string()))?
        {
            self.policy = policy;
            self.targets.clear();
            for (id, claim) in &self.claims {
                if let Some(evaluation) = &claim.evaluation {
                    for (key, targets) in self.policy.targets(evaluation) {
                        for target in targets {
                            self.targets
                                .entry(key.clone())
                                .or_default()
                                .entry(target)
                                .or_default()
                                .insert(id.clone());
                        }
                    }
                }
            }
            affected.extend(self.claims.keys().cloned());
        }
        for id in affected {
            let Some(claim) = self.claims.get_mut(&id) else {
                continue;
            };
            self.counts[status_index(claim.row.status)] -= 1;
            if let Some(evaluation) = &claim.evaluation {
                claim.row.status = MappingRecordStatus::Resolved;
                claim
                    .row
                    .diagnostics
                    .retain(|message| !message.starts_with("exclusive conflict: "));
                for (key, _) in self.policy.targets(evaluation) {
                    if self
                        .targets
                        .get(&key)
                        .is_some_and(|targets| targets.len() > 1)
                    {
                        claim.row.status = MappingRecordStatus::Conflicting;
                        claim.row.diagnostics.push(format!(
                            "exclusive conflict: {} has alternative exact targets in {}",
                            key.0, key.1
                        ));
                    }
                }
                unit_keys.extend(
                    evaluation
                        .left
                        .members
                        .iter()
                        .chain(&evaluation.right.members)
                        .map(|member| member.reference.clone()),
                );
            }
            self.counts[status_index(claim.row.status)] += 1;
        }
        for unit in unit_keys {
            let active = self.units.get(&unit).is_some_and(|matches| {
                matches.iter().any(|(id, _, _)| {
                    self.claims
                        .get(id)
                        .is_some_and(|claim| claim.row.status == MappingRecordStatus::Resolved)
                })
            });
            if active {
                self.active_units.insert(unit);
            } else {
                self.active_units.remove(&unit);
            }
        }
        Ok(())
    }

    pub(super) fn state(&self) -> MappingStateSnapshot {
        let rejected = self.counts[status_index(MappingRecordStatus::Rejected)];
        MappingStateSnapshot {
            fingerprint: self.fingerprint.clone(),
            records: self.claims.len() as u64,
            active: self.claims.len() as u64 - rejected,
            rejected,
            resolved: self.counts[0],
            unresolved: self.counts[1] + self.counts[2],
            conflicting: self.counts[3],
            units: self.active_units.len() as u64,
        }
    }

    pub(super) fn check_fingerprint(&self, expected: Option<&str>) -> Result<(), ApplicationError> {
        if expected.is_some_and(|expected| expected != self.fingerprint) {
            return Err(ApplicationError::new(
                ApplicationErrorCode::MappingsChanged,
                "mapping records changed; refresh before continuing",
            ));
        }
        Ok(())
    }

    pub(super) fn query(
        &self,
        query: QueryMappings,
    ) -> Result<MappingPageSnapshot, ApplicationError> {
        self.check_fingerprint(query.expected_fingerprint.as_deref())?;
        if query.limit == 0 || query.limit as usize > MAXIMUM_BATCH {
            return Err(invalid_request(
                "mapping query limit must be between 1 and 1000",
            ));
        }
        let query_hash = sha256(
            &serde_json::to_vec(&serde_json::json!([
                query.search,
                query.canonical_statement,
                query.claim_id,
                query.operator.as_ref().map(|operator| match operator {
                    MappingOperator::Exact => "<=>",
                    MappingOperator::Coverage => "<~>",
                    MappingOperator::Implication => "=>",
                }),
                query.endpoint,
                query.annotation_name,
                query.annotation_value,
            ]))
            .map_err(|error| ApplicationError::internal(error.to_string()))?,
        );
        let start = if let Some(cursor) = &query.cursor {
            if cursor.len() > 4096 {
                return Err(invalid_request("mapping cursor is too large"));
            }
            let cursor: PageCursor = serde_json::from_slice(
                &hex::decode(cursor).map_err(|_| invalid_request("invalid mapping cursor"))?,
            )
            .map_err(|_| invalid_request("invalid mapping cursor"))?;
            self.check_fingerprint(Some(&cursor.fingerprint))?;
            if cursor.query != query_hash || cursor.position > self.claims.len() {
                return Err(invalid_request("mapping cursor belongs to another query"));
            }
            cursor.position
        } else {
            0
        };
        let mut selected = self
            .claims
            .values()
            .enumerate()
            .skip(start)
            .filter(|(_, claim)| {
                let row = &claim.row;
                query.search.as_ref().is_none_or(|search| {
                    row.canonical_record
                        .to_lowercase()
                        .contains(&search.to_lowercase())
                }) && query
                    .claim_id
                    .as_ref()
                    .is_none_or(|value| value == &row.claim_id)
                    && query
                        .canonical_statement
                        .as_ref()
                        .is_none_or(|value| value == &row.canonical_statement)
                    && query.operator.as_ref().is_none_or(|operator| {
                        row.operator
                            == match operator {
                                MappingOperator::Exact => "<=>",
                                MappingOperator::Coverage => "<~>",
                                MappingOperator::Implication => "=>",
                            }
                    })
                    && query.endpoint.as_ref().is_none_or(|endpoint| {
                        claim.evaluation.as_ref().is_some_and(|evaluation| {
                            evaluation
                                .left
                                .members
                                .iter()
                                .chain(&evaluation.right.members)
                                .any(|member| &member.reference == endpoint)
                        })
                    })
                    && (query.annotation_name.is_none() && query.annotation_value.is_none()
                        || row.metadata.iter().any(|line| {
                            let Some(annotation) = line.strip_prefix("#@") else {
                                return false;
                            };
                            let (name, value) = annotation
                                .split_once(char::is_whitespace)
                                .unwrap_or((annotation, ""));
                            query
                                .annotation_name
                                .as_ref()
                                .is_none_or(|expected| expected == name)
                                && query
                                    .annotation_value
                                    .as_ref()
                                    .is_none_or(|expected| expected == value.trim())
                        }))
            });
        let mut items = Vec::new();
        let mut position = start;
        for _ in 0..query.limit {
            let Some((index, claim)) = selected.next() else {
                break;
            };
            items.push(claim.row.clone());
            position = index + 1;
        }
        let next_cursor = if selected.next().is_some() {
            Some(hex::encode(
                serde_json::to_vec(&PageCursor {
                    fingerprint: self.fingerprint.clone(),
                    query: query_hash,
                    position,
                })
                .map_err(|error| ApplicationError::internal(error.to_string()))?,
            ))
        } else {
            None
        };
        Ok(MappingPageSnapshot {
            fingerprint: self.fingerprint.clone(),
            items,
            next_cursor,
        })
    }

    pub(super) fn resolve(
        &self,
        command: ResolveMappingUnits,
    ) -> Result<MappingUnitResolutionBatchSnapshot, ApplicationError> {
        self.check_fingerprint(command.expected_fingerprint.as_deref())?;
        if command.unit_keys.len() > MAXIMUM_BATCH {
            return Err(invalid_request(
                "mapping resolution is limited to 1000 keys",
            ));
        }
        let mut units = Vec::new();
        for unit_key in command.unit_keys {
            trakkin_provider_sdk::validation::mapping_unit_key(&unit_key)
                .map_err(|_| invalid_request("invalid mapping unit key"))?;
            let matches = self
                .units
                .get(&unit_key)
                .into_iter()
                .flatten()
                .filter(|(id, _, _)| {
                    self.claims
                        .get(id)
                        .is_some_and(|claim| claim.row.status == MappingRecordStatus::Resolved)
                })
                .map(|(id, matched_side, ordinal)| {
                    let claim = &self.claims[id];
                    let evaluation = claim
                        .evaluation
                        .as_ref()
                        .expect("indexed claim has an evaluation");
                    MappingUnitMatchSnapshot {
                        claim_id: claim.row.claim_id.clone(),
                        operator: claim.row.operator.clone(),
                        matched_side: *matched_side,
                        member_ordinal: *ordinal as u64,
                        left: side(&evaluation.left, &claim.left_expression),
                        right: side(&evaluation.right, &claim.right_expression),
                        alignments: evaluation
                            .alignments
                            .iter()
                            .map(|segment| MappingAlignmentSegmentSnapshot {
                                left_ordinal: segment.left as u64,
                                right_ordinal: segment.right as u64,
                                left_offset: segment.left_offset,
                                right_offset: segment.right_offset,
                                extent: segment.extent,
                            })
                            .collect(),
                    }
                })
                .collect();
            units.push(MappingUnitResolutionSnapshot { unit_key, matches });
        }
        Ok(MappingUnitResolutionBatchSnapshot {
            fingerprint: self.fingerprint.clone(),
            units,
        })
    }
}

fn status_index(status: MappingRecordStatus) -> usize {
    match status {
        MappingRecordStatus::Resolved => 0,
        MappingRecordStatus::Unresolved => 1,
        MappingRecordStatus::Invalid => 2,
        MappingRecordStatus::Conflicting => 3,
        MappingRecordStatus::Rejected => 4,
    }
}

fn side(expression: &ResolvedExpression, canonical: &str) -> MappingRelationSideSnapshot {
    MappingRelationSideSnapshot {
        expression: canonical.into(),
        ordered: expression.ordered,
        total_extent: expression.total_extent,
        members: expression
            .members
            .iter()
            .enumerate()
            .map(|(ordinal, member)| MappingRelationMemberSnapshot {
                unit_key: member.reference.clone(),
                ordinal: ordinal as u64,
                extent: member.extent,
                coordinate: member.coordinate.clone(),
            })
            .collect(),
    }
}
