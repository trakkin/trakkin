use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    future::Future,
    pin::Pin,
    sync::Arc,
};

use time::OffsetDateTime;
use tokio::io::AsyncRead;
use uuid::Uuid;

use super::error::ApplicationError;

pub type MappingFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, ApplicationError>> + Send + 'a>>;
pub type MappingContentReader = Pin<Box<dyn AsyncRead + Send>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MappingSourceKind {
    Local,
    Upload,
    Remote,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MappingSourceSnapshot {
    pub id: Uuid,
    pub source_key: String,
    pub display_name: String,
    pub kind: MappingSourceKind,
    pub remote_url: Option<String>,
    pub priority: i64,
    pub enabled: bool,
    pub content_hash: String,
    pub byte_length: i64,
    pub last_checked_at: Option<OffsetDateTime>,
    pub last_refresh_error: Option<String>,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MappingStateSnapshot {
    pub active_chain_fingerprint: Option<String>,
    pub sources: Vec<MappingSourceSnapshot>,
}

pub struct MappingSourceContent {
    pub content_hash: String,
    pub byte_length: u64,
    pub reader: MappingContentReader,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateUploadedMappingSource {
    pub source_id: Uuid,
    pub source_key: String,
    pub display_name: String,
    pub enabled: bool,
    pub created_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateRemoteMappingSource {
    pub source_id: Uuid,
    pub source_key: String,
    pub display_name: String,
    pub remote_url: String,
    pub enabled: bool,
    pub created_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UpdateMappingSource {
    pub source_id: Uuid,
    pub display_name: String,
    pub enabled: bool,
    pub updated_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateLocalMapping {
    pub record: String,
    pub created_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UpdateLocalMapping {
    pub statement_id: String,
    pub record: String,
    pub updated_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RemoveLocalMapping {
    pub statement_id: String,
    pub removed_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReorderMappingSources {
    pub source_ids: Vec<Uuid>,
    pub updated_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RemoveMappingSource {
    pub source_id: Uuid,
    pub removed_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RefreshRemoteMappingSource {
    pub source_id: Uuid,
    pub refreshed_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MappingOperator {
    Exact,
    Coverage,
    Implication,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryMappings {
    pub expected_chain_fingerprint: Option<String>,
    pub search: Option<String>,
    pub canonical_statement: Option<String>,
    pub statement_id: Option<String>,
    pub relation_key: Option<String>,
    pub operator: Option<MappingOperator>,
    pub source_key: Option<String>,
    pub endpoint: Option<String>,
    pub annotation_name: Option<String>,
    pub annotation_value: Option<String>,
    pub include_shadowed: bool,
    pub limit: u32,
    pub cursor: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MappingResult {
    pub relation_key: String,
    pub statement_id: String,
    pub canonical_statement: String,
    pub canonical_record: String,
    pub operator: String,
    pub source_key: String,
    pub source_content_hash: String,
    pub layer_position: u64,
    pub record_ordinal: u64,
    pub active: bool,
    pub occurrence_count: u64,
    pub evidence_fingerprint: String,
    pub metadata: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MappingPageSnapshot {
    pub chain_fingerprint: String,
    pub items: Vec<MappingResult>,
    pub next_cursor: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolveMappingUnits {
    pub expected_chain_fingerprint: Option<String>,
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
    pub relation_key: String,
    pub statement_id: String,
    pub operator: String,
    pub matched_side: MappingUnitSide,
    pub member_ordinal: u64,
    pub left: MappingRelationSideSnapshot,
    pub right: MappingRelationSideSnapshot,
    pub alignments: Vec<MappingAlignmentSegmentSnapshot>,
    pub evidence_fingerprint: String,
    pub source_key: String,
    pub source_content_hash: String,
    pub layer_position: u64,
    pub record_ordinal: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MappingUnitResolutionSnapshot {
    pub unit_key: String,
    pub matches: Vec<MappingUnitMatchSnapshot>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MappingUnitResolutionBatchSnapshot {
    pub chain_fingerprint: String,
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
        relation_key: String,
        resolutions: Vec<MappingUnitResolutionSnapshot>,
    },
    Ambiguous {
        relevance_rank: u32,
        relation_keys: Vec<String>,
        resolutions: Vec<MappingUnitResolutionSnapshot>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RankedMappingUnitSelectionSnapshot {
    pub chain_fingerprint: String,
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
            let relation_keys = resolutions
                .iter()
                .flat_map(|resolution| resolution.matches.iter())
                .map(|matched| matched.relation_key.clone())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();
            if relation_keys.len() == 1 {
                RankedMappingUnitSelection::Resolved {
                    relevance_rank,
                    relation_key: relation_keys.into_iter().next().expect("one relation key"),
                    resolutions,
                }
            } else {
                RankedMappingUnitSelection::Ambiguous {
                    relevance_rank,
                    relation_keys,
                    resolutions,
                }
            }
        },
    );

    RankedMappingUnitSelectionSnapshot {
        chain_fingerprint: batch.chain_fingerprint,
        selection,
    }
}

pub trait MappingApi: Send + Sync {
    fn state(&self) -> MappingFuture<'_, MappingStateSnapshot>;
    fn create_uploaded_source(
        &self,
        command: CreateUploadedMappingSource,
        content: MappingContentReader,
    ) -> MappingFuture<'_, MappingStateSnapshot>;
    fn create_remote_source(
        &self,
        command: CreateRemoteMappingSource,
    ) -> MappingFuture<'_, MappingStateSnapshot>;
    fn update_source(
        &self,
        command: UpdateMappingSource,
    ) -> MappingFuture<'_, MappingStateSnapshot>;
    fn create_local_mapping(
        &self,
        command: CreateLocalMapping,
    ) -> MappingFuture<'_, MappingStateSnapshot>;
    fn update_local_mapping(
        &self,
        command: UpdateLocalMapping,
    ) -> MappingFuture<'_, MappingStateSnapshot>;
    fn remove_local_mapping(
        &self,
        command: RemoveLocalMapping,
    ) -> MappingFuture<'_, MappingStateSnapshot>;
    fn reorder_sources(
        &self,
        command: ReorderMappingSources,
    ) -> MappingFuture<'_, MappingStateSnapshot>;
    fn remove_source(
        &self,
        command: RemoveMappingSource,
    ) -> MappingFuture<'_, MappingStateSnapshot>;
    fn refresh_remote_source(
        &self,
        command: RefreshRemoteMappingSource,
    ) -> MappingFuture<'_, MappingStateSnapshot>;
    fn read_source_content(&self, source_id: Uuid) -> MappingFuture<'_, MappingSourceContent>;
    fn query(&self, query: QueryMappings) -> MappingFuture<'_, MappingPageSnapshot>;
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
            relation_key,
            resolutions,
        } = selected.selection
        else {
            panic!("fallback rank should resolve");
        };
        assert_eq!(selected.chain_fingerprint, "generation-1");
        assert_eq!(relevance_rank, 2);
        assert_eq!(relation_key, "relation-a");
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

        assert_eq!(selected.chain_fingerprint, "generation-1");
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
            relation_key,
            resolutions,
        } = selected.selection
        else {
            panic!("equivalent tied units should resolve");
        };
        assert_eq!(relevance_rank, 0);
        assert_eq!(relation_key, "relation-a");
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
            relation_keys,
            resolutions,
        } = selected.selection
        else {
            panic!("conflicting tied units should be ambiguous");
        };
        assert_eq!(relevance_rank, 0);
        assert_eq!(relation_keys, ["relation-a", "relation-b"]);
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
            chain_fingerprint: "generation-1".to_owned(),
            units,
        }
    }

    fn resolution(unit_key: &str, relation_keys: &[&str]) -> MappingUnitResolutionSnapshot {
        MappingUnitResolutionSnapshot {
            unit_key: unit_key.to_owned(),
            matches: relation_keys
                .iter()
                .map(|relation_key| matched(relation_key))
                .collect(),
        }
    }

    fn matched(relation_key: &str) -> MappingUnitMatchSnapshot {
        MappingUnitMatchSnapshot {
            relation_key: relation_key.to_owned(),
            statement_id: format!("statement-{relation_key}"),
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
            evidence_fingerprint: format!("evidence-{relation_key}"),
            source_key: "test".to_owned(),
            source_content_hash: "content-hash".to_owned(),
            layer_position: 0,
            record_ordinal: 0,
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
