use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

use crate::{
    application::{
        adapter_runtime::{
            AdapterKey, AdapterLookupCandidate, AdapterLookupOutcome,
            AdapterPortableReferenceLookup,
        },
        catalog_queries::{CatalogEntry, CatalogPage, SourceStateInspection},
        error::ApplicationError,
        workspaces::MappingRootPolicy,
    },
    domain::acquisition::{
        BinaryAssetReference, CatalogItem, CatalogRelation, PortableReference, ProviderItem,
        ProviderItemKey, StateObservation, StateSubject, StateValue, Term,
    },
};

use super::timestamp;

pub(super) fn key_from_adapter(key: AdapterKey) -> KeyResponse {
    KeyResponse {
        namespace: key.namespace,
        value: key.value,
    }
}

#[derive(Clone, Debug, Deserialize, IntoParams)]
#[serde(rename_all = "camelCase")]
#[into_params(parameter_in = Query)]
pub struct CatalogQueryParameters {
    pub parent_namespace: Option<String>,
    pub parent_value: Option<String>,
    pub search: Option<String>,
    pub limit: Option<u32>,
    pub cursor: Option<String>,
}

#[derive(Clone, Debug, Deserialize, IntoParams)]
#[serde(rename_all = "camelCase")]
#[into_params(parameter_in = Query)]
pub struct StateQueryParameters {
    pub subject_kind: Option<String>,
    pub subject_namespace: Option<String>,
    pub subject_value: Option<String>,
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
pub struct StartSourceReadRequest {
    pub stream: String,
    pub mode: String,
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
pub struct StartSourceResetRequest {
    pub stream: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct CatalogPageResponse {
    pub publication_id: String,
    pub entries: Vec<CatalogEntryResponse>,
    pub next_cursor: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, ToSchema)]
pub struct CatalogEntryResponse {
    pub relation: CatalogRelationResponse,
    pub item: Option<CatalogItemResponse>,
    #[serde(rename = "hasChildren")]
    pub has_children: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct CatalogRelationResponse {
    pub key: KeyResponse,
    pub parent_key: Option<KeyResponse>,
    pub provider_item_key: Option<KeyResponse>,
    pub kind: TermResponse,
    pub order: Vec<i64>,
    #[schema(schema_with = free_form_object)]
    pub attributes: serde_json::Value,
}

#[derive(Clone, Debug, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct CatalogItemResponse {
    pub key: KeyResponse,
    pub kind: TermResponse,
    pub display_name: String,
    #[schema(schema_with = free_form_object)]
    pub attributes: serde_json::Value,
    pub portable_reference_candidates: Vec<PortableReferenceCandidateResponse>,
    pub binary_assets: Vec<BinaryAssetReferenceResponse>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
pub struct KeyResponse {
    pub namespace: String,
    pub value: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
pub struct TermResponse {
    pub namespace: String,
    pub name: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
pub struct PortableReferenceResponse {
    pub namespace: String,
    pub value: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct PortableReferenceCandidateResponse {
    pub namespace: String,
    pub value: Vec<u8>,
    pub provider_recommended: bool,
    pub effective_mapping_root: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
pub struct BinaryAssetReferenceResponse {
    pub key: KeyResponse,
    pub kind: TermResponse,
    pub href: String,
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
pub struct LookupPortableReferencesRequest {
    pub references: Vec<PortableReferenceRequest>,
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
pub struct PortableReferenceRequest {
    pub namespace: String,
    pub value: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Serialize, ToSchema)]
pub struct PortableReferenceLookupResponse {
    pub requested: PortableReferenceResponse,
    pub outcome: PortableReferenceLookupOutcomeResponse,
}

#[derive(Clone, Debug, PartialEq, Serialize, ToSchema)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum PortableReferenceLookupOutcomeResponse {
    Matched {
        candidate: Box<LookupCandidateResponse>,
    },
    NotFound,
    Ambiguous {
        candidates: Vec<LookupCandidateResponse>,
    },
    Unsupported,
}

#[derive(Clone, Debug, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct LookupCandidateResponse {
    pub provider_item: LookupProviderItemResponse,
    pub evidence: LookupEvidenceResponse,
}

#[derive(Clone, Debug, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct LookupProviderItemResponse {
    pub key: KeyResponse,
    pub kind: TermResponse,
    pub display_name: String,
    #[schema(schema_with = free_form_object)]
    pub attributes: serde_json::Value,
    pub portable_reference_candidates: Vec<PortableReferenceCandidateResponse>,
    pub binary_assets: Vec<LookupBinaryAssetReferenceResponse>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
pub struct LookupBinaryAssetReferenceResponse {
    pub key: KeyResponse,
    pub kind: TermResponse,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct LookupEvidenceResponse {
    pub adapter_revision: Vec<u8>,
    pub observed_at: String,
    pub expires_at: Option<String>,
    pub matched_references: Vec<PortableReferenceResponse>,
}

impl CatalogPageResponse {
    pub fn from_page(page: CatalogPage, workspace_id: uuid::Uuid, source_id: uuid::Uuid) -> Self {
        let publication_id = page.publication_id;
        Self {
            publication_id: publication_id.to_string(),
            entries: page
                .entries
                .into_iter()
                .map(|entry| {
                    CatalogEntryResponse::from_entry(entry, workspace_id, source_id, publication_id)
                })
                .collect(),
            next_cursor: page.next_cursor,
        }
    }
}

impl PortableReferenceLookupResponse {
    pub fn from_lookup(
        lookup: AdapterPortableReferenceLookup,
        mapping_root_policy: &MappingRootPolicy,
    ) -> Result<Self, ApplicationError> {
        Ok(Self {
            requested: portable_reference_response(&lookup.requested),
            outcome: match lookup.outcome {
                AdapterLookupOutcome::Matched(candidate) => {
                    PortableReferenceLookupOutcomeResponse::Matched {
                        candidate: Box::new(LookupCandidateResponse::from_candidate(
                            *candidate,
                            mapping_root_policy,
                        )?),
                    }
                }
                AdapterLookupOutcome::NotFound => PortableReferenceLookupOutcomeResponse::NotFound,
                AdapterLookupOutcome::Ambiguous(candidates) => {
                    PortableReferenceLookupOutcomeResponse::Ambiguous {
                        candidates: candidates
                            .into_iter()
                            .map(|candidate| {
                                LookupCandidateResponse::from_candidate(
                                    candidate,
                                    mapping_root_policy,
                                )
                            })
                            .collect::<Result<_, _>>()?,
                    }
                }
                AdapterLookupOutcome::Unsupported => {
                    PortableReferenceLookupOutcomeResponse::Unsupported
                }
            },
        })
    }
}

impl LookupCandidateResponse {
    fn from_candidate(
        candidate: AdapterLookupCandidate,
        mapping_root_policy: &MappingRootPolicy,
    ) -> Result<Self, ApplicationError> {
        Ok(Self {
            provider_item: LookupProviderItemResponse::from_item(
                candidate.provider_item,
                mapping_root_policy,
            ),
            evidence: LookupEvidenceResponse {
                adapter_revision: candidate.evidence.adapter_revision,
                observed_at: timestamp(candidate.evidence.observed_at)?,
                expires_at: candidate.evidence.expires_at.map(timestamp).transpose()?,
                matched_references: candidate
                    .evidence
                    .matched_references
                    .iter()
                    .map(portable_reference_response)
                    .collect(),
            },
        })
    }
}

impl LookupProviderItemResponse {
    fn from_item(item: ProviderItem, mapping_root_policy: &MappingRootPolicy) -> Self {
        Self {
            key: key_response(item.key()),
            kind: term_response(item.kind()),
            display_name: item.display_name().to_owned(),
            attributes: item.attributes().clone(),
            portable_reference_candidates: item
                .portable_reference_candidates()
                .iter()
                .map(|reference| {
                    let provider_recommended = item.recommended_mapping_roots().contains(reference);
                    portable_reference_candidate_response(
                        reference,
                        provider_recommended,
                        mapping_root_policy.selects(reference.namespace(), provider_recommended),
                    )
                })
                .collect(),
            binary_assets: item
                .binary_assets()
                .iter()
                .map(|asset| LookupBinaryAssetReferenceResponse {
                    key: KeyResponse {
                        namespace: asset.key().namespace().to_owned(),
                        value: asset.key().value().to_vec(),
                    },
                    kind: term_response(asset.kind()),
                })
                .collect(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SourceStateResponse {
    pub publication_id: String,
    pub observations: Vec<StateObservationResponse>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct StateObservationResponse {
    pub subject: StateSubjectResponse,
    pub field: StateFieldResponse,
    pub value: Option<StateValueResponse>,
    pub provider_revision: Vec<u8>,
    pub observed_at: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum StateSubjectResponse {
    ProviderItem { key: KeyResponse },
    CatalogRelation { key: KeyResponse },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
pub struct StateFieldResponse {
    pub field: TermResponse,
    pub unit: Option<TermResponse>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(tag = "kind", content = "value", rename_all = "camelCase")]
pub enum StateValueResponse {
    Boolean(bool),
    Integer(i64),
    Decimal(String),
    Text(String),
    Bytes(Vec<u8>),
    TimestampMilliseconds(i64),
}

impl SourceStateResponse {
    pub fn from_inspection(inspection: SourceStateInspection) -> Result<Self, ApplicationError> {
        Ok(Self {
            publication_id: inspection.publication_id.to_string(),
            observations: inspection
                .observations
                .into_iter()
                .map(StateObservationResponse::from_observation)
                .collect::<Result<_, _>>()?,
        })
    }
}

impl StateObservationResponse {
    fn from_observation(observation: StateObservation) -> Result<Self, ApplicationError> {
        Ok(Self {
            subject: StateSubjectResponse::from(observation.subject()),
            field: StateFieldResponse {
                field: term_response(observation.field().field()),
                unit: observation.field().unit().map(term_response),
            },
            value: observation.value().map(StateValueResponse::from),
            provider_revision: observation.provider_revision().to_vec(),
            observed_at: timestamp(observation.observed_at())?,
        })
    }
}

impl From<&StateSubject> for StateSubjectResponse {
    fn from(subject: &StateSubject) -> Self {
        match subject {
            StateSubject::ProviderItem(key) => Self::ProviderItem {
                key: key_response(key),
            },
            StateSubject::CatalogRelation(key) => Self::CatalogRelation {
                key: KeyResponse {
                    namespace: key.namespace().to_owned(),
                    value: key.value().to_vec(),
                },
            },
        }
    }
}

impl From<&StateValue> for StateValueResponse {
    fn from(value: &StateValue) -> Self {
        match value {
            StateValue::Boolean(value) => Self::Boolean(*value),
            StateValue::Integer(value) => Self::Integer(*value),
            StateValue::Decimal(value) => Self::Decimal(value.clone()),
            StateValue::Text(value) => Self::Text(value.clone()),
            StateValue::Bytes(value) => Self::Bytes(value.clone()),
            StateValue::TimestampMilliseconds(value) => Self::TimestampMilliseconds(*value),
        }
    }
}

impl CatalogEntryResponse {
    fn from_entry(
        entry: CatalogEntry,
        workspace_id: uuid::Uuid,
        source_id: uuid::Uuid,
        publication_id: uuid::Uuid,
    ) -> Self {
        let effective_mapping_roots = entry.effective_mapping_roots;
        Self {
            relation: CatalogRelationResponse::from(entry.relation),
            item: entry.item.map(|item| {
                CatalogItemResponse::from_item(
                    item,
                    effective_mapping_roots,
                    workspace_id,
                    source_id,
                    publication_id,
                )
            }),
            has_children: entry.has_children,
        }
    }
}

impl From<CatalogRelation> for CatalogRelationResponse {
    fn from(relation: CatalogRelation) -> Self {
        Self {
            key: KeyResponse {
                namespace: relation.key().namespace().to_owned(),
                value: relation.key().value().to_vec(),
            },
            parent_key: relation.parent_key().map(|key| KeyResponse {
                namespace: key.namespace().to_owned(),
                value: key.value().to_vec(),
            }),
            provider_item_key: relation.provider_item_key().map(key_response),
            kind: term_response(relation.kind()),
            order: relation.order().to_vec(),
            attributes: relation.attributes().clone(),
        }
    }
}

impl CatalogItemResponse {
    fn from_item(
        item: CatalogItem,
        effective_mapping_roots: Vec<PortableReference>,
        workspace_id: uuid::Uuid,
        source_id: uuid::Uuid,
        publication_id: uuid::Uuid,
    ) -> Self {
        Self {
            key: key_response(item.key()),
            kind: term_response(item.kind()),
            display_name: item.display_name().to_owned(),
            attributes: item.attributes().clone(),
            portable_reference_candidates: item
                .portable_reference_candidates()
                .iter()
                .map(|reference| {
                    portable_reference_candidate_response(
                        reference,
                        item.recommended_mapping_roots().contains(reference),
                        effective_mapping_roots.contains(reference),
                    )
                })
                .collect(),
            binary_assets: item
                .binary_assets()
                .iter()
                .map(|asset| {
                    binary_asset_response(
                        asset,
                        item.key(),
                        workspace_id,
                        source_id,
                        publication_id,
                    )
                })
                .collect(),
        }
    }
}

fn key_response(key: &ProviderItemKey) -> KeyResponse {
    KeyResponse {
        namespace: key.namespace().to_owned(),
        value: key.value().to_vec(),
    }
}

pub(super) fn term_response(term: &Term) -> TermResponse {
    TermResponse {
        namespace: term.namespace().to_owned(),
        name: term.name().to_owned(),
    }
}

fn portable_reference_response(reference: &PortableReference) -> PortableReferenceResponse {
    PortableReferenceResponse {
        namespace: reference.namespace().to_owned(),
        value: reference.value().to_vec(),
    }
}

fn portable_reference_candidate_response(
    reference: &PortableReference,
    provider_recommended: bool,
    effective_mapping_root: bool,
) -> PortableReferenceCandidateResponse {
    PortableReferenceCandidateResponse {
        namespace: reference.namespace().to_owned(),
        value: reference.value().to_vec(),
        provider_recommended,
        effective_mapping_root,
    }
}

fn binary_asset_response(
    asset: &BinaryAssetReference,
    provider_item_key: &ProviderItemKey,
    workspace_id: uuid::Uuid,
    source_id: uuid::Uuid,
    publication_id: uuid::Uuid,
) -> BinaryAssetReferenceResponse {
    BinaryAssetReferenceResponse {
        key: KeyResponse {
            namespace: asset.key().namespace().to_owned(),
            value: asset.key().value().to_vec(),
        },
        kind: term_response(asset.kind()),
        href: format!(
            "/api/v1/workspaces/{workspace_id}/sources/{source_id}/catalog/{publication_id}/assets/{}/{}/{}/{}",
            hex::encode(provider_item_key.namespace().as_bytes()),
            hex::encode(provider_item_key.value()),
            hex::encode(asset.key().namespace().as_bytes()),
            hex::encode(asset.key().value()),
        ),
    }
}
fn free_form_object() -> utoipa::openapi::schema::Object {
    use utoipa::openapi::schema::{AdditionalProperties, ObjectBuilder};

    ObjectBuilder::new()
        .additional_properties(Some(AdditionalProperties::FreeForm(true)))
        .build()
}
