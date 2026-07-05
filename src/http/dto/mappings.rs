use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use utoipa::{IntoParams, ToSchema};

use crate::{
    application::{
        error::ApplicationError,
        mapping_lifecycle::{
            MappingRevisionOrigin, MappingRevisionParent, PackageImportMetadata,
            WorkspaceMappingActivation,
        },
        mapping_queries::{
            MappingActivationView, MappingDetail, MappingHistoryPage, MappingRegistryPage,
            MappingRevisionSummary, MappingSummary,
        },
        mapping_resolution::{
            CoordinateBacking, EndpointLookupMatch, ExactSegment, ExactTransform,
            FiniteOrdinalWindow, MappingResolution, PublishedReferenceMatch,
            ResolutionCompleteness, ResolutionFinding, ResolutionFindingKind, ResolutionMode,
            ResolutionProof, ResolutionProofKind, ResolutionSemanticWork, ResolvedCorrespondence,
        },
    },
    http::error::FieldProblemResponse,
};

use super::{sources::KeyResponse, timestamp};

#[derive(Clone, Debug, Deserialize, IntoParams)]
#[serde(rename_all = "camelCase")]
#[into_params(parameter_in = Query)]
pub struct MappingPageParameters {
    pub limit: Option<u32>,
    pub cursor: Option<String>,
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
pub struct CreateMappingDocumentRequest {
    pub schema: String,
    pub mappings: Vec<Vec<String>>,
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct UpdateMappingDocumentRequest {
    pub base_content_hash: String,
    pub mappings: Vec<Vec<String>>,
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ActivateMappingRevisionRequest {
    pub content_hash: String,
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct MappingResolutionPreviewRequest {
    #[schema(value_type = MappingResolutionModeResponse)]
    pub mode: String,
    #[schema(min_items = 1, max_items = 256)]
    pub references: Vec<String>,
    pub window: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub enum MappingResolutionModeResponse {
    Direct,
    Transitive,
}

impl MappingResolutionModeResponse {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "direct" => Some(Self::Direct),
            "transitive" => Some(Self::Transitive),
            _ => None,
        }
    }

    pub fn into_domain(self) -> ResolutionMode {
        match self {
            Self::Direct => ResolutionMode::Direct,
            Self::Transitive => ResolutionMode::Transitive,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct MappingResolutionPreviewResponse {
    pub mapping_id: String,
    pub content_hash: String,
    pub mode: MappingResolutionModeResponse,
    pub resolved_at: String,
    pub completeness: ResolutionCompletenessResponse,
    pub correspondences: Vec<ResolvedCorrespondenceResponse>,
    pub findings: Vec<ResolutionFindingResponse>,
    pub published_evidence: Vec<PublishedResolutionEvidenceResponse>,
    pub lookup_evidence: Vec<LookupResolutionEvidenceResponse>,
    pub transport_lookup_calls: u32,
    pub semantic_work: ResolutionSemanticWorkResponse,
    pub next_work_key: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub enum ResolutionCompletenessResponse {
    Complete,
    Partial,
    Truncated,
    Indeterminate,
    Stale,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ResolvedCorrespondenceResponse {
    pub endpoints: Vec<String>,
    pub segment: Option<ExactSegmentResponse>,
    pub support_count: u32,
    pub proof: ResolutionProofResponse,
    pub supporting_proofs: Vec<ResolutionProofResponse>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ExactSegmentResponse {
    pub coordinate: String,
    pub source_start: String,
    pub source_end: String,
    pub target_coordinate: String,
    pub target_start_numerator: String,
    pub target_end_exclusive_numerator: String,
    pub target_denominator: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ResolutionProofResponse {
    pub key: String,
    pub kind: ResolutionProofKindResponse,
    pub revision_hash: String,
    pub row_hash: String,
    pub endpoint_hash: String,
    pub parent_proofs: Vec<String>,
    pub transform: ExactTransformResponse,
    pub catalog_publications: Vec<String>,
    pub coordinate_publications: Vec<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub enum ResolutionProofKindResponse {
    Direct,
    Derived,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
pub struct ExactTransformResponse {
    pub numerator: String,
    pub denominator: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ResolutionFindingResponse {
    pub endpoint: String,
    pub kind: ResolutionFindingKindResponse,
    pub span: Option<FiniteOrdinalWindowResponse>,
    pub proof_paths: Vec<ResolutionProofResponse>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ResolutionFindingKindResponse {
    Unresolved,
    NotFound,
    Unsupported,
    Ambiguous,
    MissingCoordinateEvidence,
    ExpiredEvidence,
    ContradictorySpans,
    Cycle,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
pub struct FiniteOrdinalWindowResponse {
    pub coordinate: String,
    pub start: String,
    pub end: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct PublishedResolutionEvidenceResponse {
    pub endpoint: String,
    pub provider_item_key: KeyResponse,
    pub catalog_publication_id: String,
    pub coordinate_publication_id: Option<String>,
    pub coordinate_evidence_revision: Option<Vec<u8>>,
    pub coordinate_observed_at: Option<String>,
    pub coordinate_expires_at: Option<String>,
    pub backing: Option<CoordinateBackingResponse>,
    pub membership_absent: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct LookupResolutionEvidenceResponse {
    pub endpoint: String,
    pub provider_item_key: KeyResponse,
    pub backing: Option<CoordinateBackingResponse>,
    pub coordinate_evidence_revision: Option<Vec<u8>>,
    pub lookup_evidence_revision: Vec<u8>,
    pub observed_at: String,
    pub expires_at: Option<String>,
    pub membership_absent: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub enum CoordinateBackingResponse {
    Materialized,
    Virtual,
    Aggregate,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ResolutionSemanticWorkResponse {
    pub endpoint_hits: u64,
    pub candidate_rows: u64,
    pub traversal_hops: u64,
    pub partitions: u64,
    pub lookup_references: u64,
    pub lookup_calls: u64,
    pub lookup_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct MappingHeadMovedResponse {
    pub code: String,
    pub message: String,
    pub field_problems: Vec<FieldProblemResponse>,
    pub current_head: String,
    pub added: Vec<Vec<String>>,
    pub removed: Vec<Vec<String>>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct MappingRegistryPageResponse {
    pub mappings: Vec<MappingSummaryResponse>,
    pub next_cursor: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct MappingSummaryResponse {
    pub mapping_id: String,
    pub head_content_hash: String,
    pub portable_revision: u64,
    pub row_count: u64,
    pub created_at: String,
    pub updated_at: String,
    pub activation: Option<MappingActivationResponse>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct MappingHistoryPageResponse {
    pub mapping_id: String,
    pub head_content_hash: String,
    pub revisions: Vec<MappingRevisionSummaryResponse>,
    pub next_cursor: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct MappingRevisionSummaryResponse {
    pub content_hash: String,
    pub portable_revision: u64,
    pub row_count: u64,
    pub origin: MappingRevisionOriginResponse,
    pub created_at: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub enum MappingRevisionOriginResponse {
    Created,
    Updated,
    Imported,
    Forked,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct MappingDetailResponse {
    pub mapping_id: String,
    pub document: MappingDocumentResponse,
    pub content_hash: String,
    pub parents: Vec<MappingRevisionParentResponse>,
    pub origin: MappingRevisionOriginResponse,
    pub package: Option<MappingPackageProvenanceResponse>,
    pub record_created_at: String,
    pub record_updated_at: String,
    pub revision_created_at: String,
    pub activation: Option<MappingActivationResponse>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
pub struct MappingDocumentResponse {
    pub schema: String,
    pub revision: u64,
    pub mappings: Vec<Vec<String>>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct MappingActivationResponse {
    pub active_content_hash: Option<String>,
    pub generation: i64,
    pub updated_at: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct MappingRevisionParentResponse {
    pub content_hash: String,
    pub verified: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct MappingPackageProvenanceResponse {
    pub package_id: String,
    pub document_path: String,
    pub roots: Vec<String>,
    pub license_expression: String,
    pub attribution: Vec<String>,
    pub signatures: Vec<MappingPackageSignatureResponse>,
    pub forge_commit: Option<String>,
    pub required_mapping_schema: String,
    pub required_resolver: String,
    pub required_coordinate_registry_digest: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
pub struct MappingPackageSignatureResponse {
    pub kind: String,
    pub digest: String,
}

impl MappingRegistryPageResponse {
    pub fn from_page(page: MappingRegistryPage) -> Result<Self, ApplicationError> {
        Ok(Self {
            mappings: page
                .mappings
                .into_iter()
                .map(MappingSummaryResponse::from_summary)
                .collect::<Result<_, _>>()?,
            next_cursor: page.next_cursor,
        })
    }
}

impl MappingSummaryResponse {
    fn from_summary(summary: MappingSummary) -> Result<Self, ApplicationError> {
        Ok(Self {
            mapping_id: summary.mapping_id.to_string(),
            head_content_hash: summary.head_content_hash,
            portable_revision: summary.portable_revision,
            row_count: summary.row_count,
            created_at: timestamp(summary.created_at)?,
            updated_at: timestamp(summary.updated_at)?,
            activation: summary
                .activation
                .map(MappingActivationResponse::from_view)
                .transpose()?,
        })
    }
}

impl MappingHistoryPageResponse {
    pub fn from_page(page: MappingHistoryPage) -> Result<Self, ApplicationError> {
        Ok(Self {
            mapping_id: page.mapping_id.to_string(),
            head_content_hash: page.head_content_hash,
            revisions: page
                .revisions
                .into_iter()
                .map(MappingRevisionSummaryResponse::from_summary)
                .collect::<Result<_, _>>()?,
            next_cursor: page.next_cursor,
        })
    }
}

impl MappingRevisionSummaryResponse {
    fn from_summary(summary: MappingRevisionSummary) -> Result<Self, ApplicationError> {
        Ok(Self {
            content_hash: summary.content_hash,
            portable_revision: summary.portable_revision,
            row_count: summary.row_count,
            origin: summary.origin.into(),
            created_at: timestamp(summary.created_at)?,
        })
    }
}

impl MappingDetailResponse {
    pub fn from_detail(detail: MappingDetail) -> Result<Self, ApplicationError> {
        let revision = detail.revision;
        Ok(Self {
            mapping_id: revision.mapping_id.to_string(),
            document: MappingDocumentResponse {
                schema: revision.document.schema().to_owned(),
                revision: revision.document.revision(),
                mappings: revision
                    .document
                    .mappings()
                    .iter()
                    .map(|row| row.iter().map(ToString::to_string).collect())
                    .collect(),
            },
            content_hash: revision.content_hash,
            parents: revision
                .parents
                .into_iter()
                .map(MappingRevisionParentResponse::from)
                .collect(),
            origin: revision.origin.into(),
            package: revision.package.map(MappingPackageProvenanceResponse::from),
            record_created_at: timestamp(detail.record_created_at)?,
            record_updated_at: timestamp(detail.record_updated_at)?,
            revision_created_at: timestamp(revision.created_at)?,
            activation: detail
                .activation
                .map(MappingActivationResponse::from_view)
                .transpose()?,
        })
    }
}

impl MappingActivationResponse {
    fn from_view(view: MappingActivationView) -> Result<Self, ApplicationError> {
        Ok(Self {
            active_content_hash: view.active_content_hash,
            generation: view.generation,
            updated_at: timestamp(view.updated_at)?,
        })
    }

    pub fn from_activation(
        activation: WorkspaceMappingActivation,
    ) -> Result<Self, ApplicationError> {
        Ok(Self {
            active_content_hash: activation.active_content_hash,
            generation: activation.generation,
            updated_at: timestamp(activation.updated_at)?,
        })
    }
}

impl MappingResolutionPreviewResponse {
    pub fn from_resolution(
        mapping_id: uuid::Uuid,
        content_hash: String,
        mode: MappingResolutionModeResponse,
        resolved_at: OffsetDateTime,
        resolution: MappingResolution,
    ) -> Result<Self, ApplicationError> {
        Ok(Self {
            mapping_id: mapping_id.to_string(),
            content_hash,
            mode,
            resolved_at: timestamp(resolved_at)?,
            completeness: ResolutionCompletenessResponse::from(resolution.completeness),
            correspondences: resolution
                .correspondences
                .into_iter()
                .map(ResolvedCorrespondenceResponse::from)
                .collect(),
            findings: resolution
                .findings
                .into_iter()
                .map(ResolutionFindingResponse::from)
                .collect(),
            published_evidence: resolution
                .published_matches
                .into_iter()
                .map(PublishedResolutionEvidenceResponse::from_match)
                .collect::<Result<_, _>>()?,
            lookup_evidence: resolution
                .endpoint_lookup_matches
                .into_iter()
                .map(LookupResolutionEvidenceResponse::from_match)
                .collect::<Result<_, _>>()?,
            transport_lookup_calls: resolution.lookup_calls,
            semantic_work: ResolutionSemanticWorkResponse::from(resolution.semantic_work),
            next_work_key: resolution.next_work_key,
        })
    }
}

impl From<ResolutionCompleteness> for ResolutionCompletenessResponse {
    fn from(completeness: ResolutionCompleteness) -> Self {
        match completeness {
            ResolutionCompleteness::Complete => Self::Complete,
            ResolutionCompleteness::Partial => Self::Partial,
            ResolutionCompleteness::Truncated => Self::Truncated,
            ResolutionCompleteness::Indeterminate => Self::Indeterminate,
            ResolutionCompleteness::Stale => Self::Stale,
        }
    }
}

impl From<ResolvedCorrespondence> for ResolvedCorrespondenceResponse {
    fn from(correspondence: ResolvedCorrespondence) -> Self {
        Self {
            endpoints: correspondence
                .endpoints
                .into_iter()
                .map(|endpoint| endpoint.as_str().to_owned())
                .collect(),
            segment: correspondence.segment.map(ExactSegmentResponse::from),
            support_count: correspondence.support_count,
            proof: ResolutionProofResponse::from(correspondence.proof),
            supporting_proofs: correspondence
                .supporting_proofs
                .into_iter()
                .map(ResolutionProofResponse::from)
                .collect(),
        }
    }
}

impl From<ExactSegment> for ExactSegmentResponse {
    fn from(segment: ExactSegment) -> Self {
        Self {
            coordinate: segment.coordinate,
            source_start: segment.source_start.to_string(),
            source_end: segment.source_end.to_string(),
            target_coordinate: segment.target_coordinate,
            target_start_numerator: segment.target_start_numerator.to_string(),
            target_end_exclusive_numerator: segment.target_end_exclusive_numerator.to_string(),
            target_denominator: segment.target_denominator.to_string(),
        }
    }
}

impl From<ResolutionProof> for ResolutionProofResponse {
    fn from(proof: ResolutionProof) -> Self {
        Self {
            key: proof.key,
            kind: ResolutionProofKindResponse::from(proof.kind),
            revision_hash: proof.revision_hash,
            row_hash: proof.row_hash,
            endpoint_hash: proof.endpoint_hash,
            parent_proofs: proof.parent_proofs,
            transform: ExactTransformResponse::from(proof.transform),
            catalog_publications: proof
                .catalog_publications
                .into_iter()
                .map(|publication| publication.to_string())
                .collect(),
            coordinate_publications: proof
                .coordinate_publications
                .into_iter()
                .map(|publication| publication.to_string())
                .collect(),
        }
    }
}

impl From<ResolutionProofKind> for ResolutionProofKindResponse {
    fn from(kind: ResolutionProofKind) -> Self {
        match kind {
            ResolutionProofKind::Direct => Self::Direct,
            ResolutionProofKind::Derived => Self::Derived,
        }
    }
}

impl From<ExactTransform> for ExactTransformResponse {
    fn from(transform: ExactTransform) -> Self {
        Self {
            numerator: transform.numerator.to_string(),
            denominator: transform.denominator.to_string(),
        }
    }
}

impl From<ResolutionFinding> for ResolutionFindingResponse {
    fn from(finding: ResolutionFinding) -> Self {
        Self {
            endpoint: finding.endpoint.as_str().to_owned(),
            kind: ResolutionFindingKindResponse::from(finding.kind),
            span: finding.span.map(FiniteOrdinalWindowResponse::from),
            proof_paths: finding
                .proof_paths
                .into_iter()
                .map(ResolutionProofResponse::from)
                .collect(),
        }
    }
}

impl From<ResolutionFindingKind> for ResolutionFindingKindResponse {
    fn from(kind: ResolutionFindingKind) -> Self {
        match kind {
            ResolutionFindingKind::Unresolved => Self::Unresolved,
            ResolutionFindingKind::NotFound => Self::NotFound,
            ResolutionFindingKind::Unsupported => Self::Unsupported,
            ResolutionFindingKind::Ambiguous => Self::Ambiguous,
            ResolutionFindingKind::MissingCoordinateEvidence => Self::MissingCoordinateEvidence,
            ResolutionFindingKind::ExpiredEvidence => Self::ExpiredEvidence,
            ResolutionFindingKind::ContradictorySpans => Self::ContradictorySpans,
            ResolutionFindingKind::Cycle => Self::Cycle,
        }
    }
}

impl From<FiniteOrdinalWindow> for FiniteOrdinalWindowResponse {
    fn from(window: FiniteOrdinalWindow) -> Self {
        Self {
            coordinate: window.coordinate,
            start: window.start.to_string(),
            end: window.end.to_string(),
        }
    }
}

impl PublishedResolutionEvidenceResponse {
    fn from_match(evidence: PublishedReferenceMatch) -> Result<Self, ApplicationError> {
        Ok(Self {
            endpoint: evidence.endpoint.as_str().to_owned(),
            provider_item_key: KeyResponse {
                namespace: evidence.item_key_namespace,
                value: evidence.item_key_value,
            },
            catalog_publication_id: evidence.catalog_publication_id.to_string(),
            coordinate_publication_id: evidence
                .coordinate_publication_id
                .map(|publication| publication.to_string()),
            coordinate_evidence_revision: evidence.coordinate_evidence_revision,
            coordinate_observed_at: evidence.coordinate_observed_at.map(timestamp).transpose()?,
            coordinate_expires_at: evidence.coordinate_expires_at.map(timestamp).transpose()?,
            backing: evidence.backing.map(CoordinateBackingResponse::from),
            membership_absent: false,
        })
    }
}

impl LookupResolutionEvidenceResponse {
    fn from_match(evidence: EndpointLookupMatch) -> Result<Self, ApplicationError> {
        Ok(Self {
            endpoint: evidence.endpoint.as_str().to_owned(),
            provider_item_key: KeyResponse {
                namespace: evidence.item_key_namespace,
                value: evidence.item_key_value,
            },
            backing: evidence.backing.map(CoordinateBackingResponse::from),
            coordinate_evidence_revision: evidence.coordinate_evidence_revision,
            lookup_evidence_revision: evidence.lookup_evidence_revision,
            observed_at: timestamp(evidence.observed_at)?,
            expires_at: evidence.expires_at.map(timestamp).transpose()?,
            membership_absent: true,
        })
    }
}

impl From<CoordinateBacking> for CoordinateBackingResponse {
    fn from(backing: CoordinateBacking) -> Self {
        match backing {
            CoordinateBacking::Materialized => Self::Materialized,
            CoordinateBacking::Virtual => Self::Virtual,
            CoordinateBacking::Aggregate => Self::Aggregate,
        }
    }
}

impl From<ResolutionSemanticWork> for ResolutionSemanticWorkResponse {
    fn from(work: ResolutionSemanticWork) -> Self {
        Self {
            endpoint_hits: work.endpoint_hits,
            candidate_rows: work.candidate_rows,
            traversal_hops: work.traversal_hops,
            partitions: work.partitions,
            lookup_references: work.lookup_references,
            lookup_calls: work.lookup_calls,
            lookup_bytes: work.lookup_bytes,
        }
    }
}

impl From<MappingRevisionParent> for MappingRevisionParentResponse {
    fn from(parent: MappingRevisionParent) -> Self {
        Self {
            content_hash: parent.content_hash,
            verified: parent.verified,
        }
    }
}

impl From<PackageImportMetadata> for MappingPackageProvenanceResponse {
    fn from(package: PackageImportMetadata) -> Self {
        Self {
            package_id: package.package_id.to_string(),
            document_path: package.document_path,
            roots: package.roots,
            license_expression: package.license_expression,
            attribution: package.attribution,
            signatures: package
                .signatures
                .into_iter()
                .map(|(kind, digest)| MappingPackageSignatureResponse { kind, digest })
                .collect(),
            forge_commit: package.forge_commit,
            required_mapping_schema: package.required_mapping_schema,
            required_resolver: package.required_resolver,
            required_coordinate_registry_digest: package.required_coordinate_registry_digest,
        }
    }
}

impl From<MappingRevisionOrigin> for MappingRevisionOriginResponse {
    fn from(origin: MappingRevisionOrigin) -> Self {
        match origin {
            MappingRevisionOrigin::Created => Self::Created,
            MappingRevisionOrigin::Updated => Self::Updated,
            MappingRevisionOrigin::Imported => Self::Imported,
            MappingRevisionOrigin::Forked => Self::Forked,
        }
    }
}
