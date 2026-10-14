use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    future::Future,
    pin::Pin,
    sync::Arc,
};

use super::error::ApplicationError;

pub type MappingFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, ApplicationError>> + Send + 'a>>;
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MappingStateSnapshot {
    pub fingerprint: String,
    pub records: u64,
    pub active: u64,
    pub rejected: u64,
    pub resolved: u64,
    pub unresolved: u64,
    pub conflicting: u64,
    pub units: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MappingOperator {
    Exact,
    Coverage,
    Implication,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryMappings {
    pub expected_fingerprint: Option<String>,
    pub search: Option<String>,
    pub canonical_statement: Option<String>,
    pub claim_id: Option<String>,
    pub operator: Option<MappingOperator>,
    pub endpoint: Option<String>,
    pub annotation_name: Option<String>,
    pub annotation_value: Option<String>,
    pub limit: u32,
    pub cursor: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MappingResult {
    pub claim_id: String,
    pub canonical_statement: String,
    pub canonical_record: String,
    pub operator: String,
    pub metadata: Vec<String>,
    pub rejected: bool,
    pub sources: Vec<String>,
    pub status: MappingRecordStatus,
    pub diagnostics: Vec<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MappingRecordStatus {
    Resolved,
    Unresolved,
    Invalid,
    Conflicting,
    Rejected,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SaveMapping {
    pub expected_fingerprint: String,
    pub record: String,
    pub replaces: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DecideMapping {
    pub expected_fingerprint: String,
    pub claim_id: String,
    pub rejected: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeleteMapping {
    pub expected_fingerprint: String,
    pub claim_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MappingSourceKind {
    Local,
    Upstream,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MappingSource {
    pub id: String,
    pub display_name: String,
    pub kind: MappingSourceKind,
    pub enabled: bool,
    pub url: Option<String>,
    pub branch: Option<String>,
    pub repository_path: Option<String>,
    pub poll_interval_seconds: Option<u64>,
    pub cached_records: u64,
    pub revision: Option<String>,
    pub last_checked_at: Option<time::OffsetDateTime>,
    pub last_updated_at: Option<time::OffsetDateTime>,
    pub next_check_at: Option<time::OffsetDateTime>,
    pub consecutive_failures: u32,
    pub last_error: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MappingSourcesSnapshot {
    pub fingerprint: String,
    pub sources: Vec<MappingSource>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SaveMappingSource {
    pub expected_fingerprint: String,
    pub id: Option<String>,
    pub display_name: String,
    pub url: String,
    pub branch: String,
    pub poll_interval_seconds: u64,
    pub enabled: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChangeMappingSource {
    pub expected_fingerprint: String,
    pub id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MappingPageSnapshot {
    pub fingerprint: String,
    pub items: Vec<MappingResult>,
    pub next_cursor: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolveMappingUnits {
    pub expected_fingerprint: Option<String>,
    pub unit_keys: Vec<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MappingUnitSide {
    Left,
    Right,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MappingRelationMemberSnapshot {
    pub unit_key: String,
    pub ordinal: u64,
    pub extent: u64,
    pub coordinate: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MappingRelationSideSnapshot {
    pub expression: String,
    pub ordered: bool,
    pub total_extent: u64,
    pub members: Vec<MappingRelationMemberSnapshot>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MappingAlignmentSegmentSnapshot {
    pub left_ordinal: u64,
    pub right_ordinal: u64,
    pub left_offset: u64,
    pub right_offset: u64,
    pub extent: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MappingUnitMatchSnapshot {
    pub claim_id: String,
    pub operator: String,
    pub matched_side: MappingUnitSide,
    pub member_ordinal: u64,
    pub left: MappingRelationSideSnapshot,
    pub right: MappingRelationSideSnapshot,
    pub alignments: Vec<MappingAlignmentSegmentSnapshot>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MappingUnitResolutionSnapshot {
    pub unit_key: String,
    pub matches: Vec<MappingUnitMatchSnapshot>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MappingUnitResolutionBatchSnapshot {
    pub fingerprint: String,
    pub units: Vec<MappingUnitResolutionSnapshot>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RankedMappingUnitCandidate {
    pub unit_key: String,
    pub relevance_rank: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RankedMappingUnitSelection {
    Unresolved,
    Resolved {
        relevance_rank: u32,
        claim_id: String,
        resolutions: Vec<MappingUnitResolutionSnapshot>,
    },
    Ambiguous {
        relevance_rank: u32,
        claim_ids: Vec<String>,
        resolutions: Vec<MappingUnitResolutionSnapshot>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RankedMappingUnitSelectionSnapshot {
    pub fingerprint: String,
    pub selection: RankedMappingUnitSelection,
}

pub fn select_ranked_mapping_units(
    candidates: &[RankedMappingUnitCandidate],
    batch: MappingUnitResolutionBatchSnapshot,
) -> RankedMappingUnitSelectionSnapshot {
    let mut ranks_by_unit_key = HashMap::<&str, u32>::new();
    for candidate in candidates {
        ranks_by_unit_key
            .entry(&candidate.unit_key)
            .and_modify(|rank| *rank = (*rank).min(candidate.relevance_rank))
            .or_insert(candidate.relevance_rank);
    }

    let mut successful_ranks = BTreeMap::<u32, Vec<MappingUnitResolutionSnapshot>>::new();
    for unit in batch.units {
        if unit.matches.is_empty() {
            continue;
        }
        if let Some(relevance_rank) = ranks_by_unit_key.get(unit.unit_key.as_str()).copied() {
            successful_ranks
                .entry(relevance_rank)
                .or_default()
                .push(unit);
        }
    }

    let selection = successful_ranks.into_iter().next().map_or(
        RankedMappingUnitSelection::Unresolved,
        |(relevance_rank, resolutions)| {
            let claim_ids = resolutions
                .iter()
                .flat_map(|resolution| resolution.matches.iter())
                .map(|matched| matched.claim_id.clone())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();
            if claim_ids.len() == 1 {
                RankedMappingUnitSelection::Resolved {
                    relevance_rank,
                    claim_id: claim_ids.into_iter().next().expect("one relation key"),
                    resolutions,
                }
            } else {
                RankedMappingUnitSelection::Ambiguous {
                    relevance_rank,
                    claim_ids,
                    resolutions,
                }
            }
        },
    );

    RankedMappingUnitSelectionSnapshot {
        fingerprint: batch.fingerprint,
        selection,
    }
}

pub trait MappingApi: Send + Sync {
    fn state(&self) -> MappingFuture<'_, MappingStateSnapshot>;
    fn query(&self, query: QueryMappings) -> MappingFuture<'_, MappingPageSnapshot>;
    fn preview(&self, record: String) -> MappingFuture<'_, MappingResult>;
    fn save(&self, command: SaveMapping) -> MappingFuture<'_, MappingStateSnapshot>;
    fn decide(&self, command: DecideMapping) -> MappingFuture<'_, MappingStateSnapshot>;
    fn delete(&self, command: DeleteMapping) -> MappingFuture<'_, MappingStateSnapshot>;
    fn sources(&self) -> MappingFuture<'_, MappingSourcesSnapshot>;
    fn save_source(&self, command: SaveMappingSource) -> MappingFuture<'_, MappingStateSnapshot>;
    fn delete_source(
        &self,
        command: ChangeMappingSource,
    ) -> MappingFuture<'_, MappingStateSnapshot>;
    fn refresh_source(
        &self,
        command: ChangeMappingSource,
    ) -> MappingFuture<'_, MappingStateSnapshot>;
    fn revert(&self, command: DeleteMapping) -> MappingFuture<'_, MappingStateSnapshot>;
    fn resolve_units(
        &self,
        command: ResolveMappingUnits,
    ) -> MappingFuture<'_, MappingUnitResolutionBatchSnapshot>;
}

pub type SharedMappingApi = Arc<dyn MappingApi>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ranked_selection_falls_back_to_lowest_successful_relevance_rank() {
        let selected = select_ranked_mapping_units(
            &[
                candidate("missing://preferred", 0),
                candidate("mapped://fallback", 2),
            ],
            batch(vec![
                resolution("missing://preferred", &[]),
                resolution("mapped://fallback", &["relation-a"]),
            ]),
        );

        let RankedMappingUnitSelection::Resolved {
            relevance_rank,
            claim_id,
            resolutions,
        } = selected.selection
        else {
            panic!("fallback rank should resolve");
        };
        assert_eq!(selected.fingerprint, "generation-1");
        assert_eq!(relevance_rank, 2);
        assert_eq!(claim_id, "relation-a");
        assert_eq!(resolutions.len(), 1);
        assert_eq!(resolutions[0].unit_key, "mapped://fallback");
    }

    #[test]
    fn ranked_selection_preserves_missing_lower_ranks() {
        let selected = select_ranked_mapping_units(
            &[candidate("mapped://alternate", 2)],
            batch(vec![resolution("mapped://alternate", &["relation-a"])]),
        );

        let RankedMappingUnitSelection::Resolved { relevance_rank, .. } = selected.selection else {
            panic!("alternate should resolve without an exact candidate");
        };
        assert_eq!(relevance_rank, 2);
    }

    #[test]
    fn ranked_selection_is_unresolved_without_successful_candidates() {
        let selected = select_ranked_mapping_units(
            &[
                candidate("missing://preferred", 0),
                candidate("missing://fallback", 1),
            ],
            batch(vec![
                resolution("missing://preferred", &[]),
                resolution("missing://fallback", &[]),
            ]),
        );

        assert_eq!(selected.fingerprint, "generation-1");
        assert_eq!(selected.selection, RankedMappingUnitSelection::Unresolved);
    }

    #[test]
    fn ranked_selection_accepts_tied_units_for_one_identity() {
        let selected = select_ranked_mapping_units(
            &[
                candidate("mapped://first", 0),
                candidate("mapped://second", 0),
            ],
            batch(vec![
                resolution("mapped://first", &["relation-a"]),
                resolution("mapped://second", &["relation-a"]),
            ]),
        );

        let RankedMappingUnitSelection::Resolved {
            relevance_rank,
            claim_id,
            resolutions,
        } = selected.selection
        else {
            panic!("equivalent tied units should resolve");
        };
        assert_eq!(relevance_rank, 0);
        assert_eq!(claim_id, "relation-a");
        assert_eq!(resolutions.len(), 2);
    }

    #[test]
    fn ranked_selection_reports_best_rank_conflicts_and_ignores_lower_ranks() {
        let selected = select_ranked_mapping_units(
            &[
                candidate("mapped://first", 0),
                candidate("mapped://second", 0),
                candidate("mapped://lower", 1),
            ],
            batch(vec![
                resolution("mapped://first", &["relation-b"]),
                resolution("mapped://second", &["relation-a"]),
                resolution("mapped://lower", &["relation-c"]),
            ]),
        );

        let RankedMappingUnitSelection::Ambiguous {
            relevance_rank,
            claim_ids,
            resolutions,
        } = selected.selection
        else {
            panic!("conflicting tied units should be ambiguous");
        };
        assert_eq!(relevance_rank, 0);
        assert_eq!(claim_ids, ["relation-a", "relation-b"]);
        assert_eq!(resolutions.len(), 2);
        assert!(
            resolutions
                .iter()
                .all(|resolution| resolution.unit_key != "mapped://lower")
        );
    }

    fn candidate(unit_key: &str, relevance_rank: u32) -> RankedMappingUnitCandidate {
        RankedMappingUnitCandidate {
            unit_key: unit_key.to_owned(),
            relevance_rank,
        }
    }

    fn batch(units: Vec<MappingUnitResolutionSnapshot>) -> MappingUnitResolutionBatchSnapshot {
        MappingUnitResolutionBatchSnapshot {
            fingerprint: "generation-1".to_owned(),
            units,
        }
    }

    fn resolution(unit_key: &str, claim_ids: &[&str]) -> MappingUnitResolutionSnapshot {
        MappingUnitResolutionSnapshot {
            unit_key: unit_key.to_owned(),
            matches: claim_ids.iter().map(|claim_id| matched(claim_id)).collect(),
        }
    }

    fn matched(claim_id: &str) -> MappingUnitMatchSnapshot {
        MappingUnitMatchSnapshot {
            claim_id: claim_id.to_owned(),
            operator: "<=>".to_owned(),
            matched_side: MappingUnitSide::Left,
            member_ordinal: 0,
            left: side("mapped://source"),
            right: side("mapped://target"),
            alignments: vec![MappingAlignmentSegmentSnapshot {
                left_ordinal: 0,
                right_ordinal: 0,
                left_offset: 0,
                right_offset: 0,
                extent: 1,
            }],
        }
    }

    fn side(unit_key: &str) -> MappingRelationSideSnapshot {
        MappingRelationSideSnapshot {
            expression: unit_key.to_owned(),
            ordered: true,
            total_extent: 1,
            members: vec![MappingRelationMemberSnapshot {
                unit_key: unit_key.to_owned(),
                ordinal: 0,
                extent: 1,
                coordinate: None,
            }],
        }
    }
}
