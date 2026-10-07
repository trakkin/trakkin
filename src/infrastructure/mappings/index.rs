use super::{MAXIMUM_BATCH, MappingResolver, invalid_request, records::StoredRecord};
use crate::application::{
    error::{ApplicationError, ApplicationErrorCode},
    mappings::*,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use trakkin_mappings_corpus::snapshots::SnapshotSet;
use trakkin_mappings_language::{
    Evaluation, Operator, Record, ResolvedExpression, SemanticErrorKind, evaluate,
    semantic_revision, sha256,
};

pub(super) struct Claim {
    pub(super) row: MappingResult,
    pub(super) evaluation: Option<Evaluation>,
    pub(super) left_expression: String,
    pub(super) right_expression: String,
}

pub(super) struct MappingIndex {
    pub(super) fingerprint: String,
    pub(super) records: BTreeMap<String, StoredRecord>,
    pub(super) claims: Vec<Claim>,
    pub(super) units: BTreeMap<String, Vec<(usize, MappingUnitSide, usize)>>,
    pub(super) metadata_snapshot: Option<String>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PageCursor {
    fingerprint: String,
    query: String,
    position: usize,
}

pub(super) fn compile(
    record: &Record,
    rejected: bool,
    sources: Vec<String>,
    resolver: &MappingResolver,
) -> Claim {
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
        match evaluate(&record.statement, resolver) {
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
        resolver: &MappingResolver,
        disabled: &BTreeSet<String>,
    ) -> Result<Self, ApplicationError> {
        let revision = resolver.snapshots.as_ref().map(SnapshotSet::id);
        let fingerprint = sha256(
            &serde_json::to_vec(&(semantic_revision(), revision, &records, disabled))
                .map_err(|error| ApplicationError::internal(error.to_string()))?,
        );
        let mut claims = Vec::new();
        let mut units = BTreeMap::<String, Vec<(usize, MappingUnitSide, usize)>>::new();
        for stored in records.values() {
            let origins = stored.origins.keys().cloned().collect();
            let Some((record, rejected)) = stored.effective(disabled)? else {
                continue;
            };
            let claim = compile(&record, rejected, origins, resolver);
            if let Some(evaluation) = &claim.evaluation {
                for (side, expression) in [
                    (MappingUnitSide::Left, &evaluation.left),
                    (MappingUnitSide::Right, &evaluation.right),
                ] {
                    for (ordinal, member) in expression.members.iter().enumerate() {
                        if evaluation.alignments.is_empty()
                            || evaluation.alignments.iter().any(|segment| match side {
                                MappingUnitSide::Left => segment.left == ordinal,
                                MappingUnitSide::Right => segment.right == ordinal,
                            })
                        {
                            units.entry(member.reference.clone()).or_default().push((
                                claims.len(),
                                side,
                                ordinal,
                            ));
                        }
                    }
                }
            }
            claims.push(claim);
        }
        mark_conflicts(&mut claims);
        units.retain(|_, matches| {
            matches
                .retain(|(index, _, _)| claims[*index].row.status == MappingRecordStatus::Resolved);
            !matches.is_empty()
        });
        Ok(Self {
            fingerprint,
            records,
            claims,
            units,
            metadata_snapshot: revision.map(str::to_owned),
        })
    }

    pub(super) fn state(&self) -> MappingStateSnapshot {
        let rejected = self
            .claims
            .iter()
            .filter(|claim| claim.row.rejected)
            .count() as u64;
        MappingStateSnapshot {
            fingerprint: self.fingerprint.clone(),
            records: self.claims.len() as u64,
            active: self.claims.len() as u64 - rejected,
            rejected,
            resolved: self
                .claims
                .iter()
                .filter(|claim| claim.row.status == MappingRecordStatus::Resolved)
                .count() as u64,
            unresolved: self
                .claims
                .iter()
                .filter(|claim| {
                    matches!(
                        claim.row.status,
                        MappingRecordStatus::Unresolved | MappingRecordStatus::Invalid
                    )
                })
                .count() as u64,
            conflicting: self
                .claims
                .iter()
                .filter(|claim| claim.row.status == MappingRecordStatus::Conflicting)
                .count() as u64,
            units: self.units.len() as u64,
            metadata_snapshot: self.metadata_snapshot.clone(),
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
            .iter()
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
                .map(|(index, matched_side, ordinal)| {
                    let claim = &self.claims[*index];
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

fn mark_conflicts(claims: &mut [Claim]) {
    let mut targets = BTreeMap::<(String, String), BTreeMap<String, BTreeSet<usize>>>::new();
    for (index, claim) in claims.iter().enumerate() {
        let Some(evaluation) = &claim.evaluation else {
            continue;
        };
        if evaluation.operator != Operator::Exact {
            continue;
        }
        for segment in &evaluation.alignments {
            let left = &evaluation.left.members[segment.left].reference;
            let right = &evaluation.right.members[segment.right].reference;
            for (origin, target) in [(left, right), (right, left)] {
                let namespace = target.split_once("://").expect("evaluated reference").0;
                targets
                    .entry((origin.clone(), namespace.into()))
                    .or_default()
                    .entry(target.clone())
                    .or_default()
                    .insert(index);
            }
        }
    }
    for ((origin, namespace), alternatives) in targets {
        if alternatives.len() <= 1 {
            continue;
        }
        for index in alternatives
            .values()
            .flatten()
            .copied()
            .collect::<BTreeSet<_>>()
        {
            claims[index].row.status = MappingRecordStatus::Conflicting;
            claims[index].row.diagnostics.push(format!(
                "{origin} has conflicting exact targets in {namespace}"
            ));
        }
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
