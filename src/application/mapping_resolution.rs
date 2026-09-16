use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    marker::PhantomData,
    pin::Pin,
    time::Duration,
};

use sha2::{Digest, Sha256};
use thiserror::Error;
use time::OffsetDateTime;
use tokio::time::timeout;
use trakkin_mapping::{PortableEndpoint, PortableSelector, Selection};
use uuid::Uuid;

use crate::domain::acquisition::{
    CoordinateBacking as ProviderCoordinateBacking, PortableReference,
};

use super::{
    adapter_runtime::{
        AdapterEndpointLookupCandidate, AdapterEndpointLookupOutcome, AdapterLookupOutcome,
        AdapterPortableEndpointResolution, AdapterPortableReferenceLookup, AdapterSession,
        LookupAdapterPortableReferences, ResolveAdapterPortableEndpoints,
    },
    connection::ConnectionSessionRegistry,
    error::{ApplicationError, ApplicationErrorCode, IntoApplicationError},
    workspaces::MappingRootPolicy,
};

pub type MappingResolutionFuture<'a, T, E> =
    Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>;

pub const DEFAULT_RESOLUTION_DEADLINE: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResolutionMode {
    Direct,
    Transitive,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResolutionCompleteness {
    Complete,
    Partial,
    Truncated,
    Indeterminate,
    Stale,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResolutionFindingKind {
    Unresolved,
    NotFound,
    Unsupported,
    Ambiguous,
    MissingCoordinateEvidence,
    ExpiredEvidence,
    ContradictorySpans,
    Cycle,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResolutionProofKind {
    Direct,
    Derived,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResolutionSemanticLimits {
    pub endpoint_hits: u64,
    pub candidate_rows: u64,
    pub traversal_hops: u64,
    pub partitions: u64,
    pub lookup_references: u64,
    pub lookup_calls: u64,
    pub lookup_bytes: u64,
}

impl ResolutionSemanticLimits {
    pub const fn unbounded() -> Self {
        Self {
            endpoint_hits: u64::MAX,
            candidate_rows: u64::MAX,
            traversal_hops: u64::MAX,
            partitions: u64::MAX,
            lookup_references: u64::MAX,
            lookup_calls: u64::MAX,
            lookup_bytes: u64::MAX,
        }
    }
}

impl Default for ResolutionSemanticLimits {
    fn default() -> Self {
        Self {
            endpoint_hits: 100_000,
            candidate_rows: 50_000,
            traversal_hops: 10_000,
            partitions: 100_000,
            lookup_references: 256,
            lookup_calls: 256,
            lookup_bytes: 1_048_576,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ResolutionSemanticWork {
    pub endpoint_hits: u64,
    pub candidate_rows: u64,
    pub traversal_hops: u64,
    pub partitions: u64,
    pub lookup_references: u64,
    pub lookup_calls: u64,
    pub lookup_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExactTransform {
    pub numerator: i64,
    pub denominator: i64,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ExactSegment {
    pub coordinate: String,
    pub source_start: u64,
    pub source_end: u64,
    pub target_coordinate: String,
    pub target_start_numerator: u128,
    pub target_end_exclusive_numerator: u128,
    pub target_denominator: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolutionRevisionPin {
    pub mapping_id: Uuid,
    pub content_hash: String,
    pub activation_generation: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FrontierEndpoint {
    pub endpoint: PortableEndpoint,
    pub endpoint_hash: String,
    pub endpoint_ordinal: i32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectFrontierRow {
    pub pin: ResolutionRevisionPin,
    pub row_hash: String,
    pub row_ordinal: i32,
    pub requested_endpoint: PortableEndpoint,
    pub matched_endpoint_hash: String,
    pub endpoints: Vec<FrontierEndpoint>,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum CoordinateBacking {
    Materialized,
    Virtual,
    Aggregate,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublishedReferenceMatch {
    pub endpoint: PortableEndpoint,
    pub source_id: Uuid,
    pub item_key_namespace: String,
    pub item_key_value: Vec<u8>,
    pub catalog_publication_id: Uuid,
    pub coordinate_publication_id: Option<Uuid>,
    pub coordinate_evidence_revision: Option<Vec<u8>>,
    pub coordinate_observed_at: Option<OffsetDateTime>,
    pub coordinate_expires_at: Option<OffsetDateTime>,
    pub backing: Option<CoordinateBacking>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectFrontierSnapshot {
    pub rows: Vec<DirectFrontierRow>,
    pub published_matches: Vec<PublishedReferenceMatch>,
    pub pins: DirectResolutionPins,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectResolutionPins {
    pub revisions: Vec<ResolutionRevisionPin>,
    pub sources: Vec<ResolutionSourcePin>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolutionSourcePin {
    pub source_id: Uuid,
    pub connection_id: Uuid,
    pub attached_at: OffsetDateTime,
    pub mapping_root_namespace_allowlist: Option<Vec<String>>,
    pub mapping_root_policy_generation: i64,
    pub source_removed: bool,
    pub source_capability_revision: i64,
    pub connection_ready: bool,
    pub connection_deleted: bool,
    pub connection_capability_revision: i64,
    pub catalog_available: bool,
    pub catalog_publication_id: Option<Uuid>,
    pub catalog_evidence_revision: Option<Vec<u8>>,
    pub coordinate_publication_id: Option<Uuid>,
    pub coordinate_evidence_revision: Option<Vec<u8>>,
    pub coordinate_expires_at: Option<OffsetDateTime>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LoadDirectFrontier {
    pub workspace_id: Uuid,
    pub revision: Option<ExactMappingRevision>,
    pub references: Vec<PortableEndpoint>,
    pub window: Option<FiniteOrdinalWindow>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExactMappingRevision {
    pub mapping_id: Uuid,
    pub content_hash: String,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct FiniteOrdinalWindow {
    pub coordinate: String,
    pub start: u64,
    pub end: u64,
}

pub trait MappingResolutionRepository: Send + Sync {
    type Error;

    fn load_direct_frontier(
        &self,
        request: LoadDirectFrontier,
    ) -> MappingResolutionFuture<'_, DirectFrontierSnapshot, Self::Error>;

    fn recheck_direct_resolution(
        &self,
        workspace_id: Uuid,
        revision: Option<ExactMappingRevision>,
        expected: DirectResolutionPins,
    ) -> MappingResolutionFuture<'_, bool, Self::Error>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MappingEndpointLookupTarget {
    pub source_id: Uuid,
    pub connection_id: Uuid,
    pub source_key: super::adapter_runtime::AdapterKey,
    pub mapping_root_policy: MappingRootPolicy,
    pub bare_reference_namespaces: Vec<String>,
    pub maximum_bare_batch_size: u32,
    pub reference_namespaces: Vec<String>,
    pub coordinate_ids: Vec<String>,
    pub maximum_batch_size: u32,
    pub maximum_response_bytes: u64,
}

impl MappingEndpointLookupTarget {
    fn supports(&self, endpoint: &PortableEndpoint) -> bool {
        if !self
            .mapping_root_policy
            .may_select_namespace(endpoint.namespace())
        {
            return false;
        }
        if endpoint.selector().is_none() {
            return self
                .bare_reference_namespaces
                .iter()
                .any(|namespace| namespace == endpoint.namespace());
        }
        self.reference_namespaces
            .iter()
            .any(|namespace| namespace == endpoint.namespace())
            && endpoint.selector().is_some_and(|selector| {
                selector.components().iter().all(|component| {
                    self.coordinate_ids
                        .iter()
                        .any(|coordinate| coordinate == component.coordinate())
                })
            })
    }

    fn batch_size(&self, endpoint: &PortableEndpoint) -> u32 {
        if endpoint.selector().is_none() {
            self.maximum_bare_batch_size
        } else {
            self.maximum_batch_size
        }
    }

    fn accepts_mapping_root(
        &self,
        endpoint: &PortableEndpoint,
        provider_item: &crate::domain::acquisition::ProviderItem,
    ) -> bool {
        let matches = |reference: &crate::domain::acquisition::PortableReference| {
            reference.namespace() == endpoint.namespace() && reference.value() == endpoint.value()
        };
        if !provider_item
            .portable_reference_candidates()
            .iter()
            .any(matches)
        {
            return false;
        }
        self.mapping_root_policy.selects(
            endpoint.namespace(),
            provider_item
                .recommended_mapping_roots()
                .iter()
                .any(matches),
        )
    }
}

struct PlannedMappingEndpointLookup {
    target: MappingEndpointLookupTarget,
    batches: Vec<Vec<PortableEndpoint>>,
}

struct MappingEndpointLookupPlan {
    targets: Vec<PlannedMappingEndpointLookup>,
    next_work_key: Option<String>,
}

fn plan_mapping_endpoint_lookups(
    mut targets: Vec<MappingEndpointLookupTarget>,
    endpoints: &[PortableEndpoint],
    maximum_calls: u64,
) -> Result<MappingEndpointLookupPlan, ApplicationError> {
    targets.sort_by_key(|target| (target.connection_id, target.source_id));
    let mut planned_targets = Vec::new();
    let mut scheduled_calls = 0_u64;
    for target in targets {
        let supported = endpoints
            .iter()
            .filter(|endpoint| target.supports(endpoint))
            .cloned()
            .collect::<Vec<_>>();
        if supported.is_empty() {
            continue;
        }
        let mut batches = Vec::new();
        let mut next_work_key = None;
        for selected in [false, true] {
            let compatible = supported
                .iter()
                .filter(|endpoint| endpoint.selector().is_some() == selected)
                .cloned()
                .collect::<Vec<_>>();
            if compatible.is_empty() {
                continue;
            }
            let maximum_batch_size = target.batch_size(&compatible[0]);
            if maximum_batch_size == 0 {
                return Err(ApplicationError::new(
                    ApplicationErrorCode::InvalidProviderData,
                    "stored endpoint lookup capability is invalid",
                ));
            }
            if selected && target.maximum_response_bytes == 0 {
                return Err(ApplicationError::new(
                    ApplicationErrorCode::InvalidProviderData,
                    "stored endpoint lookup capability is invalid",
                ));
            }
            for batch in compatible.chunks(maximum_batch_size as usize) {
                if scheduled_calls == maximum_calls {
                    next_work_key = Some(format!("lookup-call:{}", batch[0]));
                    break;
                }
                scheduled_calls += 1;
                batches.push(batch.to_vec());
            }
            if next_work_key.is_some() {
                break;
            }
        }
        if !batches.is_empty() {
            planned_targets.push(PlannedMappingEndpointLookup { target, batches });
        }
        if next_work_key.is_some() {
            return Ok(MappingEndpointLookupPlan {
                targets: planned_targets,
                next_work_key,
            });
        }
    }
    Ok(MappingEndpointLookupPlan {
        targets: planned_targets,
        next_work_key: None,
    })
}

pub trait MappingEndpointLookupRepository: Send + Sync {
    type Error;

    fn resolve_mapping_endpoint_lookups(
        &self,
        workspace_id: Uuid,
    ) -> MappingResolutionFuture<'_, Vec<MappingEndpointLookupTarget>, Self::Error>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MappingEndpointLookupRequest {
    pub workspace_id: Uuid,
    pub endpoints: Vec<PortableEndpoint>,
    pub resolved_at: OffsetDateTime,
    pub recheck: bool,
    pub maximum_calls: u64,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ProcessInstancePin {
    pub connection_id: Uuid,
    pub process_instance_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EndpointLookupMatch {
    pub endpoint: PortableEndpoint,
    pub connection_id: Uuid,
    pub process_instance_id: String,
    pub item_key_namespace: String,
    pub item_key_value: Vec<u8>,
    pub backing: Option<CoordinateBacking>,
    pub coordinate_evidence_revision: Option<Vec<u8>>,
    pub lookup_evidence_revision: Vec<u8>,
    pub observed_at: OffsetDateTime,
    pub expires_at: Option<OffsetDateTime>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MappingEndpointLookupBatch {
    pub matches: Vec<EndpointLookupMatch>,
    pub findings: Vec<ResolutionFinding>,
    pub process_instances: Vec<ProcessInstancePin>,
    pub lookup_calls: u32,
    pub next_work_key: Option<String>,
    pub stale: bool,
}

pub trait MappingEndpointLookup: Send + Sync {
    fn lookup_endpoints(
        &self,
        request: MappingEndpointLookupRequest,
    ) -> MappingResolutionFuture<'_, MappingEndpointLookupBatch, ApplicationError>;
}

pub trait MappingResolutionInteraction: Send + Sync {
    fn resolve_workspace_mappings(
        &self,
        request: ResolutionRequest,
    ) -> MappingResolutionFuture<'_, MappingResolution, ApplicationError>;

    fn resolve_mapping_revision(
        &self,
        revision: ExactMappingRevision,
        request: ResolutionRequest,
    ) -> MappingResolutionFuture<'_, MappingResolution, ApplicationError>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct NoMappingEndpointLookup;

impl MappingEndpointLookup for NoMappingEndpointLookup {
    fn lookup_endpoints(
        &self,
        _request: MappingEndpointLookupRequest,
    ) -> MappingResolutionFuture<'_, MappingEndpointLookupBatch, ApplicationError> {
        Box::pin(async { Ok(MappingEndpointLookupBatch::default()) })
    }
}

pub struct AdapterMappingEndpointLookupService<R, G, S> {
    repository: R,
    sessions: G,
    session: PhantomData<fn() -> S>,
}

impl<R, G, S> AdapterMappingEndpointLookupService<R, G, S> {
    pub fn new(repository: R, sessions: G) -> Self {
        Self {
            repository,
            sessions,
            session: PhantomData,
        }
    }
}

impl<R, G, S> MappingEndpointLookup for AdapterMappingEndpointLookupService<R, G, S>
where
    R: MappingEndpointLookupRepository,
    R::Error: IntoApplicationError + Send,
    G: ConnectionSessionRegistry<S>,
    S: AdapterSession + 'static,
    S::Error: Send,
{
    fn lookup_endpoints(
        &self,
        request: MappingEndpointLookupRequest,
    ) -> MappingResolutionFuture<'_, MappingEndpointLookupBatch, ApplicationError> {
        Box::pin(async move {
            let targets = self
                .repository
                .resolve_mapping_endpoint_lookups(request.workspace_id)
                .await
                .map_err(IntoApplicationError::into_application_error)?;
            let plan =
                plan_mapping_endpoint_lookups(targets, &request.endpoints, request.maximum_calls)?;
            let mut batch = MappingEndpointLookupBatch {
                next_work_key: plan.next_work_key,
                ..MappingEndpointLookupBatch::default()
            };
            let mut attempted = BTreeSet::new();
            for planned in plan.targets {
                let target = planned.target;
                attempted.extend(planned.batches.iter().flatten().cloned());
                let mut session = self.sessions.remove(target.connection_id).ok_or_else(|| {
                    ApplicationError::new(
                        ApplicationErrorCode::ConnectionUnavailable,
                        "connection process is unavailable",
                    )
                })?;
                let process = ProcessInstancePin {
                    connection_id: target.connection_id,
                    process_instance_id: session.identity().process_instance_id.clone(),
                };
                batch.process_instances.push(process.clone());
                for (chunk_index, endpoints) in planned.batches.into_iter().enumerate() {
                    batch.lookup_calls = batch.lookup_calls.saturating_add(1);
                    let operation_kind = if request.recheck {
                        "recheck"
                    } else {
                        "resolve"
                    };
                    let operation_id = format!(
                        "mapping:{}:{}:{}:{operation_kind}:{chunk_index}",
                        request.workspace_id, target.connection_id, target.source_id,
                    )
                    .into_bytes();
                    let result = if endpoints[0].selector().is_none() {
                        let references = endpoints
                            .iter()
                            .map(|endpoint| {
                                PortableReference::new(
                                    endpoint.namespace(),
                                    endpoint.value().to_vec(),
                                )
                                .expect("portable endpoint is a valid portable reference")
                            })
                            .collect();
                        match session
                            .lookup_portable_references(LookupAdapterPortableReferences {
                                operation_id,
                                source_key: target.source_key.clone(),
                                references,
                            })
                            .await
                        {
                            Ok(Ok(lookups)) => {
                                Ok(Ok(reference_endpoint_results(endpoints, lookups)))
                            }
                            Ok(Err(error)) => Ok(Err(error)),
                            Err(error) => Err(error),
                        }
                    } else {
                        session
                            .resolve_portable_endpoints(ResolveAdapterPortableEndpoints {
                                operation_id,
                                source_key: target.source_key.clone(),
                                endpoints,
                                maximum_response_bytes: target.maximum_response_bytes,
                            })
                            .await
                    };
                    match result {
                        Ok(Ok(results)) => append_lookup_results(
                            &mut batch,
                            &target,
                            &process,
                            request.resolved_at,
                            results,
                        ),
                        Ok(Err(error)) => {
                            let _ = restore_mapping_session(
                                &self.sessions,
                                target.connection_id,
                                session,
                            )
                            .await;
                            return Err(error.into_application_error());
                        }
                        Err(_) => {
                            let _ = session.retire().await;
                            return Err(ApplicationError::new(
                                ApplicationErrorCode::Internal,
                                "mapping endpoint lookup adapter operation failed",
                            ));
                        }
                    }
                }
                if !restore_mapping_session(&self.sessions, target.connection_id, session).await {
                    batch.stale = true;
                    return Ok(batch);
                }
            }
            batch.process_instances.sort();
            batch.process_instances.dedup();
            batch.stale = batch.process_instances.iter().any(|expected| {
                self.sessions
                    .identity(expected.connection_id)
                    .is_none_or(|actual| actual.process_instance_id != expected.process_instance_id)
            });
            if batch.stale {
                return Ok(batch);
            }
            if batch.next_work_key.is_none() {
                batch.findings.extend(
                    request
                        .endpoints
                        .into_iter()
                        .filter(|endpoint| !attempted.contains(endpoint))
                        .map(|endpoint| ResolutionFinding {
                            endpoint,
                            kind: ResolutionFindingKind::Unsupported,
                            span: None,
                            proof_paths: Vec::new(),
                        }),
                );
            }
            Ok(batch)
        })
    }
}

fn reference_endpoint_results(
    endpoints: Vec<PortableEndpoint>,
    lookups: Vec<AdapterPortableReferenceLookup>,
) -> Vec<AdapterPortableEndpointResolution> {
    debug_assert_eq!(endpoints.len(), lookups.len());
    endpoints
        .into_iter()
        .zip(lookups)
        .map(|(requested, lookup)| {
            debug_assert_eq!(requested.namespace(), lookup.requested.namespace());
            debug_assert_eq!(requested.value(), lookup.requested.value());
            let outcome = match lookup.outcome {
                AdapterLookupOutcome::Matched(candidate) => AdapterEndpointLookupOutcome::Matched(
                    Box::new(AdapterEndpointLookupCandidate {
                        provider_item: candidate.provider_item,
                        binding: None,
                        evidence: candidate.evidence,
                    }),
                ),
                AdapterLookupOutcome::NotFound => AdapterEndpointLookupOutcome::NotFound,
                AdapterLookupOutcome::Ambiguous(candidates) => {
                    AdapterEndpointLookupOutcome::Ambiguous(
                        candidates
                            .into_iter()
                            .map(|candidate| AdapterEndpointLookupCandidate {
                                provider_item: candidate.provider_item,
                                binding: None,
                                evidence: candidate.evidence,
                            })
                            .collect(),
                    )
                }
                AdapterLookupOutcome::Unsupported => AdapterEndpointLookupOutcome::Unsupported,
            };
            AdapterPortableEndpointResolution { requested, outcome }
        })
        .collect()
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolutionRequest {
    pub workspace_id: Uuid,
    pub mode: ResolutionMode,
    pub references: Vec<PortableEndpoint>,
    pub window: Option<PortableSelector>,
    pub resolved_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolutionProof {
    pub key: String,
    pub kind: ResolutionProofKind,
    pub revision_hash: String,
    pub row_hash: String,
    pub endpoint_hash: String,
    pub parent_proofs: Vec<String>,
    pub transform: ExactTransform,
    pub catalog_publications: Vec<Uuid>,
    pub coordinate_publications: Vec<Uuid>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedCorrespondence {
    pub endpoints: Vec<PortableEndpoint>,
    pub segment: Option<ExactSegment>,
    pub support_count: u32,
    pub proof: ResolutionProof,
    pub supporting_proofs: Vec<ResolutionProof>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolutionFinding {
    pub endpoint: PortableEndpoint,
    pub kind: ResolutionFindingKind,
    pub span: Option<FiniteOrdinalWindow>,
    pub proof_paths: Vec<ResolutionProof>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MappingResolution {
    pub completeness: ResolutionCompleteness,
    pub pins: Option<DirectResolutionPins>,
    pub correspondences: Vec<ResolvedCorrespondence>,
    pub findings: Vec<ResolutionFinding>,
    pub published_matches: Vec<PublishedReferenceMatch>,
    pub endpoint_lookup_matches: Vec<EndpointLookupMatch>,
    pub lookup_calls: u32,
    pub semantic_work: ResolutionSemanticWork,
    pub next_work_key: Option<String>,
}

#[derive(Debug, Error)]
pub enum MappingResolutionError<E> {
    #[error("direct resolution requires at least one portable reference")]
    EmptyReferences,
    #[error("resolution window must be one finite ordinal coordinate")]
    InvalidWindow,
    #[error("mapping endpoint lookup failed")]
    EndpointLookup(ApplicationError),
    #[error("mapping resolution repository failed")]
    Repository(E),
}

#[derive(Clone)]
pub struct MappingResolutionService<R, L = NoMappingEndpointLookup> {
    repository: R,
    endpoint_lookup: L,
    semantic_limits: ResolutionSemanticLimits,
    deadline: Duration,
}

impl<R> MappingResolutionService<R, NoMappingEndpointLookup>
where
    R: MappingResolutionRepository,
{
    pub fn new(repository: R) -> Self {
        Self {
            repository,
            endpoint_lookup: NoMappingEndpointLookup,
            semantic_limits: ResolutionSemanticLimits::default(),
            deadline: DEFAULT_RESOLUTION_DEADLINE,
        }
    }
}

impl<R, L> MappingResolutionService<R, L>
where
    R: MappingResolutionRepository,
    L: MappingEndpointLookup,
{
    pub fn with_endpoint_lookup(repository: R, endpoint_lookup: L) -> Self {
        Self {
            repository,
            endpoint_lookup,
            semantic_limits: ResolutionSemanticLimits::default(),
            deadline: DEFAULT_RESOLUTION_DEADLINE,
        }
    }

    pub fn with_semantic_limits(mut self, semantic_limits: ResolutionSemanticLimits) -> Self {
        self.semantic_limits = semantic_limits;
        self
    }

    pub fn with_deadline(mut self, deadline: Duration) -> Self {
        self.deadline = deadline;
        self
    }

    async fn load_resolution_frontier(
        &self,
        workspace_id: Uuid,
        revision: Option<&ExactMappingRevision>,
        references: Vec<PortableEndpoint>,
        window: Option<FiniteOrdinalWindow>,
        mode: ResolutionMode,
        budget: &mut SemanticBudget,
    ) -> Result<FrontierLoad, MappingResolutionError<R::Error>> {
        let mut pending = references;
        let mut visited = BTreeSet::new();
        let mut rows = Vec::new();
        let mut published_matches = Vec::new();
        let mut expected_pins = None;
        let mut exhaustion = None;
        while !pending.is_empty() {
            pending.sort();
            pending.dedup();
            pending.retain(|endpoint| visited.insert(endpoint.clone()));
            if pending.is_empty() {
                break;
            }
            let mut snapshot = self
                .repository
                .load_direct_frontier(LoadDirectFrontier {
                    workspace_id,
                    revision: revision.cloned(),
                    references: pending.clone(),
                    window: window.clone(),
                })
                .await
                .map_err(MappingResolutionError::Repository)?;
            if expected_pins
                .as_ref()
                .is_some_and(|pins| *pins != snapshot.pins)
            {
                return Ok(FrontierLoad::Stale);
            }
            expected_pins.get_or_insert_with(|| snapshot.pins.clone());
            snapshot.rows.sort_by(|left, right| {
                (
                    &left.requested_endpoint,
                    &left.pin.content_hash,
                    &left.row_hash,
                    &left.matched_endpoint_hash,
                )
                    .cmp(&(
                        &right.requested_endpoint,
                        &right.pin.content_hash,
                        &right.row_hash,
                        &right.matched_endpoint_hash,
                    ))
            });
            let mut admitted_rows = Vec::new();
            for row in snapshot.rows {
                if let Err(next_work_key) = budget.admit_frontier_row(&row) {
                    exhaustion = Some(next_work_key);
                    break;
                }
                admitted_rows.push(row);
            }
            pending = if mode == ResolutionMode::Transitive {
                admitted_rows
                    .iter()
                    .flat_map(|row| {
                        row.endpoints
                            .iter()
                            .filter(|endpoint| endpoint.endpoint_hash != row.matched_endpoint_hash)
                            .map(|endpoint| endpoint.endpoint.clone())
                    })
                    .collect()
            } else {
                Vec::new()
            };
            rows.extend(admitted_rows);
            published_matches.extend(snapshot.published_matches);
            if exhaustion.is_some() {
                break;
            }
        }
        rows.sort_by(|left, right| {
            (
                &left.pin.content_hash,
                &left.row_hash,
                &left.matched_endpoint_hash,
                &left.requested_endpoint,
            )
                .cmp(&(
                    &right.pin.content_hash,
                    &right.row_hash,
                    &right.matched_endpoint_hash,
                    &right.requested_endpoint,
                ))
        });
        rows.dedup();
        published_matches.sort_by(|left, right| {
            (
                &left.endpoint,
                left.catalog_publication_id,
                left.coordinate_publication_id,
                left.source_id,
            )
                .cmp(&(
                    &right.endpoint,
                    right.catalog_publication_id,
                    right.coordinate_publication_id,
                    right.source_id,
                ))
        });
        published_matches.dedup();
        Ok(FrontierLoad::Loaded {
            snapshot: DirectFrontierSnapshot {
                rows,
                published_matches,
                pins: expected_pins.unwrap_or(DirectResolutionPins {
                    revisions: Vec::new(),
                    sources: Vec::new(),
                }),
            },
            exhaustion,
        })
    }

    pub async fn resolve(
        &self,
        request: ResolutionRequest,
    ) -> Result<MappingResolution, MappingResolutionError<R::Error>> {
        self.resolve_scoped(request, None).await
    }

    pub async fn resolve_revision(
        &self,
        revision: ExactMappingRevision,
        request: ResolutionRequest,
    ) -> Result<MappingResolution, MappingResolutionError<R::Error>> {
        self.resolve_scoped(request, Some(revision)).await
    }

    async fn resolve_scoped(
        &self,
        request: ResolutionRequest,
        revision: Option<ExactMappingRevision>,
    ) -> Result<MappingResolution, MappingResolutionError<R::Error>> {
        match timeout(
            self.deadline,
            self.resolve_within_deadline(request, revision),
        )
        .await
        {
            Ok(resolution) => resolution,
            Err(_) => Ok(indeterminate_resolution()),
        }
    }

    async fn resolve_within_deadline(
        &self,
        mut request: ResolutionRequest,
        revision: Option<ExactMappingRevision>,
    ) -> Result<MappingResolution, MappingResolutionError<R::Error>> {
        if request.references.is_empty() {
            return Err(MappingResolutionError::EmptyReferences);
        }
        request.references.sort();
        request.references.dedup();
        let window = request
            .window
            .as_ref()
            .map(finite_ordinal_window)
            .transpose()?
            .flatten();
        let mut budget = SemanticBudget::new(self.semantic_limits);

        let FrontierLoad::Loaded {
            mut snapshot,
            mut exhaustion,
        } = self
            .load_resolution_frontier(
                request.workspace_id,
                revision.as_ref(),
                request.references.clone(),
                window.clone(),
                request.mode,
                &mut budget,
            )
            .await?
        else {
            return Ok(stale_resolution(0, budget.work));
        };
        snapshot.rows.sort_by(|left, right| {
            (
                &left.pin.content_hash,
                &left.row_hash,
                &left.matched_endpoint_hash,
            )
                .cmp(&(
                    &right.pin.content_hash,
                    &right.row_hash,
                    &right.matched_endpoint_hash,
                ))
        });
        snapshot.published_matches.sort_by(|left, right| {
            (
                &left.endpoint,
                left.catalog_publication_id,
                left.coordinate_publication_id,
                left.source_id,
            )
                .cmp(&(
                    &right.endpoint,
                    right.catalog_publication_id,
                    right.coordinate_publication_id,
                    right.source_id,
                ))
        });

        let matched_references = snapshot
            .rows
            .iter()
            .map(|row| row.requested_endpoint.clone())
            .collect::<BTreeSet<_>>();
        let mut lookup_endpoints = request
            .references
            .iter()
            .filter(|endpoint| {
                endpoint.selector().is_some()
                    && matched_references.contains(*endpoint)
                    && !has_published_coordinate_evidence(
                        endpoint,
                        &snapshot.published_matches,
                        request.resolved_at,
                    )
            })
            .cloned()
            .collect::<Vec<_>>();
        if request.mode == ResolutionMode::Transitive {
            lookup_endpoints.extend(
                snapshot
                    .rows
                    .iter()
                    .flat_map(|row| row.endpoints.iter())
                    .map(|endpoint| endpoint.endpoint.clone())
                    .filter(|endpoint| {
                        !request
                            .references
                            .iter()
                            .any(|requested| endpoint_matches_request(requested, endpoint))
                    }),
            );
            lookup_endpoints.sort();
            lookup_endpoints.dedup();
        }
        lookup_endpoints = budget.admit_lookup_endpoints(lookup_endpoints);
        if exhaustion.is_none() {
            exhaustion = budget.next_work_key.clone();
        }
        let lookup_request = MappingEndpointLookupRequest {
            workspace_id: request.workspace_id,
            endpoints: lookup_endpoints.clone(),
            resolved_at: request.resolved_at,
            recheck: false,
            maximum_calls: budget.remaining_lookup_calls(),
        };
        let mut endpoint_lookup = if lookup_endpoints.is_empty() {
            MappingEndpointLookupBatch::default()
        } else {
            self.endpoint_lookup
                .lookup_endpoints(lookup_request.clone())
                .await
                .map_err(MappingResolutionError::EndpointLookup)?
        };
        sort_endpoint_lookup_batch(&mut endpoint_lookup);
        budget.record_lookup_batch(&endpoint_lookup);
        if exhaustion.is_none() {
            exhaustion = budget.next_work_key.clone();
        }
        if endpoint_lookup.stale {
            return Ok(stale_resolution(endpoint_lookup.lookup_calls, budget.work));
        }
        if !lookup_endpoints.is_empty() {
            let mut recheck_request = lookup_request;
            recheck_request.recheck = true;
            let mut rechecked_lookup = self
                .endpoint_lookup
                .lookup_endpoints(recheck_request)
                .await
                .map_err(MappingResolutionError::EndpointLookup)?;
            sort_endpoint_lookup_batch(&mut rechecked_lookup);
            let pins_match = lookup_pins_match(&endpoint_lookup, &rechecked_lookup);
            endpoint_lookup.lookup_calls = endpoint_lookup
                .lookup_calls
                .saturating_add(rechecked_lookup.lookup_calls);
            if rechecked_lookup.stale || !pins_match {
                return Ok(stale_resolution(endpoint_lookup.lookup_calls, budget.work));
            }
        }
        let current = self
            .repository
            .recheck_direct_resolution(request.workspace_id, revision, snapshot.pins.clone())
            .await
            .map_err(MappingResolutionError::Repository)?;
        if !current {
            return Ok(stale_resolution(endpoint_lookup.lookup_calls, budget.work));
        }
        let mut findings = request
            .references
            .iter()
            .filter_map(|endpoint| {
                let kind = if !matched_references.contains(endpoint) {
                    ResolutionFindingKind::Unresolved
                } else if endpoint.selector().is_some()
                    && !has_published_coordinate_evidence(
                        endpoint,
                        &snapshot.published_matches,
                        request.resolved_at,
                    )
                    && !endpoint_lookup
                        .matches
                        .iter()
                        .any(|evidence| evidence.endpoint == *endpoint)
                {
                    if has_expired_published_coordinate_evidence(
                        endpoint,
                        &snapshot.published_matches,
                        request.resolved_at,
                    ) {
                        ResolutionFindingKind::ExpiredEvidence
                    } else {
                        endpoint_lookup
                            .findings
                            .iter()
                            .filter(|finding| finding.endpoint == *endpoint)
                            .map(|finding| finding.kind)
                            .min_by_key(|kind| finding_priority(*kind))
                            .unwrap_or(ResolutionFindingKind::MissingCoordinateEvidence)
                    }
                } else {
                    return None;
                };
                Some(ResolutionFinding {
                    endpoint: endpoint.clone(),
                    kind,
                    span: None,
                    proof_paths: Vec::new(),
                })
            })
            .collect::<Vec<_>>();

        let blocked_traversal = blocked_traversal_endpoints(&mut endpoint_lookup);
        if request.mode == ResolutionMode::Transitive {
            findings.extend(
                endpoint_lookup
                    .findings
                    .iter()
                    .filter(|finding| blocked_traversal.contains(&finding.endpoint))
                    .cloned(),
            );
        }
        let (mut correspondences, mut segment_findings) =
            if request.mode == ResolutionMode::Transitive {
                build_transitive_correspondences(
                    &snapshot.rows,
                    &request.references,
                    &snapshot.published_matches,
                    &blocked_traversal,
                    &mut budget,
                )
            } else {
                build_direct_correspondences(
                    &snapshot.rows,
                    window.as_ref(),
                    &snapshot.published_matches,
                    &mut budget,
                )
            };
        findings.append(&mut segment_findings);
        if exhaustion.is_none() {
            exhaustion = budget.next_work_key.clone();
        }
        correspondences.sort_by(|left, right| {
            (left.segment.as_ref(), &left.proof.key, &left.endpoints).cmp(&(
                right.segment.as_ref(),
                &right.proof.key,
                &right.endpoints,
            ))
        });
        findings.sort_by(|left, right| {
            (
                &left.endpoint,
                left.span.as_ref(),
                finding_priority(left.kind),
            )
                .cmp(&(
                    &right.endpoint,
                    right.span.as_ref(),
                    finding_priority(right.kind),
                ))
        });

        Ok(MappingResolution {
            completeness: if exhaustion.is_some() {
                ResolutionCompleteness::Truncated
            } else if findings.is_empty() {
                ResolutionCompleteness::Complete
            } else {
                ResolutionCompleteness::Partial
            },
            pins: Some(snapshot.pins),
            correspondences,
            findings,
            published_matches: snapshot.published_matches,
            endpoint_lookup_matches: endpoint_lookup.matches,
            lookup_calls: endpoint_lookup.lookup_calls,
            semantic_work: budget.work,
            next_work_key: exhaustion,
        })
    }
}

impl<R, L> MappingResolutionInteraction for MappingResolutionService<R, L>
where
    R: MappingResolutionRepository,
    R::Error: IntoApplicationError,
    L: MappingEndpointLookup,
{
    fn resolve_workspace_mappings(
        &self,
        request: ResolutionRequest,
    ) -> MappingResolutionFuture<'_, MappingResolution, ApplicationError> {
        Box::pin(async move {
            self.resolve(request)
                .await
                .map_err(mapping_resolution_application_error)
        })
    }

    fn resolve_mapping_revision(
        &self,
        revision: ExactMappingRevision,
        request: ResolutionRequest,
    ) -> MappingResolutionFuture<'_, MappingResolution, ApplicationError> {
        Box::pin(async move {
            self.resolve_revision(revision, request)
                .await
                .map_err(mapping_resolution_application_error)
        })
    }
}

fn mapping_resolution_application_error<E>(error: MappingResolutionError<E>) -> ApplicationError
where
    E: IntoApplicationError,
{
    match error {
        MappingResolutionError::EmptyReferences => ApplicationError::field(
            ApplicationErrorCode::InvalidField,
            "mapping resolution request is invalid",
            "references",
            "required",
            "at least one portable endpoint is required",
        ),
        MappingResolutionError::InvalidWindow => ApplicationError::field(
            ApplicationErrorCode::InvalidField,
            "mapping resolution request is invalid",
            "window",
            "invalid_window",
            "window must be one finite ordinal coordinate",
        ),
        MappingResolutionError::EndpointLookup(error) => error,
        MappingResolutionError::Repository(error) => error.into_application_error(),
    }
}

enum FrontierLoad {
    Loaded {
        snapshot: DirectFrontierSnapshot,
        exhaustion: Option<String>,
    },
    Stale,
}

struct SemanticBudget {
    limits: ResolutionSemanticLimits,
    work: ResolutionSemanticWork,
    endpoint_hits: BTreeSet<(PortableEndpoint, String, String, String)>,
    candidate_rows: BTreeSet<(String, String)>,
    next_work_key: Option<String>,
}

impl SemanticBudget {
    fn new(limits: ResolutionSemanticLimits) -> Self {
        Self {
            limits,
            work: ResolutionSemanticWork::default(),
            endpoint_hits: BTreeSet::new(),
            candidate_rows: BTreeSet::new(),
            next_work_key: None,
        }
    }

    fn admit_frontier_row(&mut self, row: &DirectFrontierRow) -> Result<(), String> {
        let endpoint_hit = (
            row.requested_endpoint.clone(),
            row.pin.content_hash.clone(),
            row.row_hash.clone(),
            row.matched_endpoint_hash.clone(),
        );
        if !self.endpoint_hits.contains(&endpoint_hit) {
            if self.work.endpoint_hits == self.limits.endpoint_hits {
                return Err(frontier_endpoint_work_key(row));
            }
            self.endpoint_hits.insert(endpoint_hit);
            self.work.endpoint_hits += 1;
        }

        let candidate_row = (row.pin.content_hash.clone(), row.row_hash.clone());
        if !self.candidate_rows.contains(&candidate_row) {
            if self.work.candidate_rows == self.limits.candidate_rows {
                return Err(format!("row:{}", row.row_hash));
            }
            self.candidate_rows.insert(candidate_row);
            self.work.candidate_rows += 1;
        }
        Ok(())
    }

    fn admit_traversal_hop(&mut self, endpoint: &PortableEndpoint) -> Result<(), String> {
        if self.work.traversal_hops == self.limits.traversal_hops {
            let next_work_key = format!("endpoint:{endpoint}");
            self.next_work_key = Some(next_work_key.clone());
            return Err(next_work_key);
        }
        self.work.traversal_hops += 1;
        Ok(())
    }

    fn admit_partition(&mut self, next_work_key: String) -> bool {
        if self.work.partitions == self.limits.partitions {
            self.next_work_key = Some(next_work_key);
            return false;
        }
        self.work.partitions += 1;
        true
    }

    fn admit_lookup_endpoints(
        &mut self,
        mut endpoints: Vec<PortableEndpoint>,
    ) -> Vec<PortableEndpoint> {
        endpoints.sort();
        endpoints.dedup();
        let mut admitted = Vec::new();
        for endpoint in endpoints {
            if self.work.lookup_references == self.limits.lookup_references {
                self.next_work_key = Some(format!("reference:{endpoint}"));
                break;
            }
            let endpoint_bytes = endpoint.as_str().len() as u64;
            let remaining_bytes = self
                .limits
                .lookup_bytes
                .saturating_sub(self.work.lookup_bytes);
            if endpoint_bytes > remaining_bytes {
                self.work.lookup_bytes = self.work.lookup_bytes.saturating_add(remaining_bytes);
                self.next_work_key = Some(format!("reference:{endpoint}@byte:{remaining_bytes}"));
                break;
            }
            self.work.lookup_references += 1;
            self.work.lookup_bytes += endpoint_bytes;
            admitted.push(endpoint);
        }
        admitted
    }

    fn remaining_lookup_calls(&self) -> u64 {
        self.limits
            .lookup_calls
            .saturating_sub(self.work.lookup_calls)
    }

    fn record_lookup_batch(&mut self, batch: &MappingEndpointLookupBatch) {
        self.work.lookup_calls = self
            .work
            .lookup_calls
            .saturating_add(u64::from(batch.lookup_calls));
        if self.next_work_key.is_none() {
            self.next_work_key.clone_from(&batch.next_work_key);
        }
    }
}

fn frontier_endpoint_work_key(row: &DirectFrontierRow) -> String {
    let endpoint = row
        .endpoints
        .iter()
        .filter(|endpoint| endpoint.endpoint_hash != row.matched_endpoint_hash)
        .map(|endpoint| &endpoint.endpoint)
        .min()
        .unwrap_or(&row.requested_endpoint);
    format!("endpoint:{endpoint}")
}

fn finite_ordinal_window<E>(
    selector: &PortableSelector,
) -> Result<Option<FiniteOrdinalWindow>, MappingResolutionError<E>> {
    let [component] = selector.components() else {
        return Err(MappingResolutionError::InvalidWindow);
    };
    let (start, end) = match component.selection() {
        Selection::Ordinal(value) => (*value, *value),
        Selection::Range { start, end } => (*start, *end),
        _ => return Err(MappingResolutionError::InvalidWindow),
    };
    Ok(Some(FiniteOrdinalWindow {
        coordinate: component.coordinate().to_owned(),
        start,
        end,
    }))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ExactRational {
    numerator: i128,
    denominator: i128,
}

impl ExactRational {
    fn new(numerator: i128, denominator: i128) -> Self {
        debug_assert!(denominator > 0);
        let divisor = i128::try_from(greatest_common_divisor_u128(
            numerator.unsigned_abs(),
            denominator.unsigned_abs(),
        ))
        .expect("exact rational divisor fits i128");
        Self {
            numerator: numerator / divisor,
            denominator: denominator / divisor,
        }
    }

    fn multiply(self, other: Self) -> Option<Self> {
        Some(Self::new(
            self.numerator.checked_mul(other.numerator)?,
            self.denominator.checked_mul(other.denominator)?,
        ))
    }

    fn add(self, other: Self) -> Option<Self> {
        Some(Self::new(
            self.numerator
                .checked_mul(other.denominator)?
                .checked_add(other.numerator.checked_mul(self.denominator)?)?,
            self.denominator.checked_mul(other.denominator)?,
        ))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ExactAffine {
    scale: ExactRational,
    offset: ExactRational,
}

impl ExactAffine {
    fn identity() -> Self {
        Self {
            scale: ExactRational::new(1, 1),
            offset: ExactRational::new(0, 1),
        }
    }

    fn compose(self, edge: Self) -> Option<Self> {
        Some(Self {
            scale: edge.scale.multiply(self.scale)?,
            offset: edge.scale.multiply(self.offset)?.add(edge.offset)?,
        })
    }

    fn evaluate(self, value: u64) -> Option<ExactRational> {
        self.scale
            .multiply(ExactRational::new(i128::from(value), 1))?
            .add(self.offset)
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct TraversalNodeKey {
    namespace: String,
    value: Vec<u8>,
    coordinate: Option<String>,
}

#[derive(Clone)]
struct TraversalEdge {
    target: PortableEndpoint,
    affine: ExactAffine,
    proof: ResolutionProof,
    row_identity: (String, String),
}

#[derive(Clone)]
struct TraversalPath {
    origin: PortableEndpoint,
    current: PortableEndpoint,
    affine: ExactAffine,
    proofs: Vec<ResolutionProof>,
}

fn build_transitive_correspondences(
    rows: &[DirectFrontierRow],
    requested: &[PortableEndpoint],
    published_matches: &[PublishedReferenceMatch],
    blocked: &BTreeSet<PortableEndpoint>,
    budget: &mut SemanticBudget,
) -> (Vec<ResolvedCorrespondence>, Vec<ResolutionFinding>) {
    let mut edges = BTreeMap::<PortableEndpoint, Vec<TraversalEdge>>::new();
    let mut seen_edges = BTreeSet::new();
    for row in rows {
        let Some(source) = row
            .endpoints
            .iter()
            .find(|endpoint| endpoint.endpoint_hash == row.matched_endpoint_hash)
        else {
            continue;
        };
        for target in row
            .endpoints
            .iter()
            .filter(|endpoint| endpoint.endpoint_hash != row.matched_endpoint_hash)
        {
            let Some((affine, transform)) = traversal_transform(&source.endpoint, &target.endpoint)
            else {
                continue;
            };
            if !seen_edges.insert((
                row.pin.content_hash.clone(),
                row.row_hash.clone(),
                source.endpoint_hash.clone(),
                target.endpoint_hash.clone(),
            )) {
                continue;
            }
            edges
                .entry(source.endpoint.clone())
                .or_default()
                .push(TraversalEdge {
                    target: target.endpoint.clone(),
                    affine,
                    proof: direct_proof(row, source, transform, published_matches),
                    row_identity: (row.pin.content_hash.clone(), row.row_hash.clone()),
                });
        }
    }
    for targets in edges.values_mut() {
        targets.sort_by(|left, right| {
            (&left.target, &left.proof.key).cmp(&(&right.target, &right.proof.key))
        });
    }

    let mut correspondences = Vec::new();
    let mut findings = Vec::new();
    for root in requested {
        let Some(origin) = edges
            .keys()
            .filter(|endpoint| endpoint_matches_request(root, endpoint))
            .min()
            .cloned()
        else {
            continue;
        };
        let mut pending = BTreeMap::new();
        pending.insert(
            traversal_node_key(&origin),
            TraversalPath {
                origin: origin.clone(),
                current: origin,
                affine: ExactAffine::identity(),
                proofs: Vec::new(),
            },
        );
        let mut visited = BTreeMap::<TraversalNodeKey, TraversalPath>::new();
        let mut traversed_rows = BTreeSet::new();
        while let Some((node_key, path)) = pending.pop_first() {
            visited.entry(node_key).or_insert_with(|| path.clone());
            let Some(targets) = edges.get(&path.current) else {
                continue;
            };
            let targets = targets
                .iter()
                .filter(|edge| !traversed_rows.contains(&edge.row_identity))
                .collect::<Vec<_>>();
            if targets.is_empty() {
                continue;
            }
            if budget.admit_traversal_hop(&path.current).is_err() {
                return (correspondences, findings);
            }
            for edge in targets {
                traversed_rows.insert(edge.row_identity.clone());
                let Some(affine) = path.affine.compose(edge.affine) else {
                    continue;
                };
                let mut proofs = path.proofs.clone();
                proofs.push(edge.proof.clone());
                let target_key = traversal_node_key(&edge.target);
                if let Some(existing) = visited
                    .get(&target_key)
                    .or_else(|| pending.get(&target_key))
                {
                    if (existing.current != edge.target || existing.affine != affine)
                        && !existing.proofs.is_empty()
                    {
                        let mut proof_paths = vec![
                            path_proof(
                                &existing.origin,
                                &existing.current,
                                &existing.proofs,
                                existing.affine,
                            ),
                            path_proof(&path.origin, &edge.target, &proofs, affine),
                        ];
                        proof_paths.sort_by(|left, right| left.key.cmp(&right.key));
                        findings.push(ResolutionFinding {
                            endpoint: edge.target.clone(),
                            kind: ResolutionFindingKind::Cycle,
                            span: finite_ordinal_segment(&edge.target),
                            proof_paths,
                        });
                    }
                    continue;
                }
                let proof = if path.proofs.is_empty() {
                    edge.proof.clone()
                } else {
                    derived_proof(
                        &path.origin,
                        &edge.target,
                        &proofs,
                        exact_transform_from_affine(affine),
                    )
                };
                correspondences.push(ResolvedCorrespondence {
                    endpoints: vec![path.origin.clone(), edge.target.clone()],
                    segment: exact_segment_from_affine(&path.origin, &edge.target, affine),
                    support_count: 1,
                    proof,
                    supporting_proofs: if path.proofs.is_empty() {
                        Vec::new()
                    } else {
                        proofs.clone()
                    },
                });
                if !blocked
                    .iter()
                    .any(|endpoint| endpoint_matches_request(endpoint, &edge.target))
                {
                    pending.insert(
                        target_key,
                        TraversalPath {
                            origin: path.origin.clone(),
                            current: edge.target.clone(),
                            affine,
                            proofs,
                        },
                    );
                }
            }
        }
    }
    correspondences.sort_by(|left, right| {
        (&left.endpoints, &left.proof.key).cmp(&(&right.endpoints, &right.proof.key))
    });
    correspondences.dedup_by(|left, right| left.endpoints == right.endpoints);
    findings.sort_by(|left, right| {
        (&left.endpoint, left.span.as_ref()).cmp(&(&right.endpoint, right.span.as_ref()))
    });
    findings.dedup_by(|left, right| left.endpoint == right.endpoint && left.kind == right.kind);
    (correspondences, findings)
}

fn traversal_transform(
    source: &PortableEndpoint,
    target: &PortableEndpoint,
) -> Option<(ExactAffine, ExactTransform)> {
    match (
        finite_ordinal_segment(source),
        finite_ordinal_segment(target),
    ) {
        (None, None) if source.selector().is_none() && target.selector().is_none() => Some((
            ExactAffine::identity(),
            ExactTransform {
                numerator: 1,
                denominator: 1,
            },
        )),
        (Some(source), Some(target)) if source.coordinate == target.coordinate => {
            let source_length = source.end - source.start + 1;
            let target_length = target.end - target.start + 1;
            let scale = ExactRational::new(i128::from(target_length), i128::from(source_length));
            let offset = ExactRational::new(i128::from(target.start), 1)
                .add(ExactRational::new(-i128::from(source.start), 1).multiply(scale)?)?;
            Some((
                ExactAffine { scale, offset },
                exact_transform(&source, &target),
            ))
        }
        _ => None,
    }
}

fn endpoint_matches_request(requested: &PortableEndpoint, authored: &PortableEndpoint) -> bool {
    requested.namespace() == authored.namespace()
        && requested.value() == authored.value()
        && requested
            .selector()
            .is_none_or(|selector| authored.selector() == Some(selector))
}

fn traversal_node_key(endpoint: &PortableEndpoint) -> TraversalNodeKey {
    TraversalNodeKey {
        namespace: endpoint.namespace().to_owned(),
        value: endpoint.value().to_vec(),
        coordinate: finite_ordinal_segment(endpoint).map(|segment| segment.coordinate),
    }
}

fn path_proof(
    origin: &PortableEndpoint,
    target: &PortableEndpoint,
    proofs: &[ResolutionProof],
    affine: ExactAffine,
) -> ResolutionProof {
    if proofs.len() == 1 {
        proofs[0].clone()
    } else {
        derived_proof(origin, target, proofs, exact_transform_from_affine(affine))
    }
}

fn exact_transform_from_affine(affine: ExactAffine) -> ExactTransform {
    ExactTransform {
        numerator: i64::try_from(affine.scale.denominator)
            .expect("composed transform numerator fits i64"),
        denominator: i64::try_from(affine.scale.numerator)
            .expect("composed transform denominator fits i64"),
    }
}

fn exact_segment_from_affine(
    origin: &PortableEndpoint,
    target: &PortableEndpoint,
    affine: ExactAffine,
) -> Option<ExactSegment> {
    let source = finite_ordinal_segment(origin)?;
    let target = finite_ordinal_segment(target)?;
    let target_start = affine.evaluate(source.start)?;
    let target_end_exclusive = affine.evaluate(source.end + 1)?;
    let common_denominator =
        least_common_multiple_i128(target_start.denominator, target_end_exclusive.denominator)?;
    Some(ExactSegment {
        coordinate: source.coordinate,
        source_start: source.start,
        source_end: source.end,
        target_coordinate: target.coordinate,
        target_start_numerator: u128::try_from(
            target_start
                .numerator
                .checked_mul(common_denominator / target_start.denominator)?,
        )
        .ok()?,
        target_end_exclusive_numerator: u128::try_from(
            target_end_exclusive
                .numerator
                .checked_mul(common_denominator / target_end_exclusive.denominator)?,
        )
        .ok()?,
        target_denominator: u64::try_from(common_denominator).ok()?,
    })
}

fn least_common_multiple_i128(left: i128, right: i128) -> Option<i128> {
    let divisor = i128::try_from(greatest_common_divisor_u128(
        left.unsigned_abs(),
        right.unsigned_abs(),
    ))
    .ok()?;
    left.checked_div(divisor)?.checked_mul(right)
}

fn derived_proof(
    origin: &PortableEndpoint,
    target: &PortableEndpoint,
    parents: &[ResolutionProof],
    transform: ExactTransform,
) -> ResolutionProof {
    let last = parents
        .last()
        .expect("a derived proof has at least one parent");
    let parent_proofs = parents
        .iter()
        .map(|proof| proof.key.clone())
        .collect::<Vec<_>>();
    let mut digest = Sha256::new();
    digest.update(b"trakkin.mapping.derived-proof.v1\0");
    digest.update(origin.as_str().as_bytes());
    digest.update([0]);
    digest.update(target.as_str().as_bytes());
    for parent in &parent_proofs {
        digest.update([0]);
        digest.update(parent.as_bytes());
    }
    ResolutionProof {
        key: format!("sha256:{:x}", digest.finalize()),
        kind: ResolutionProofKind::Derived,
        revision_hash: last.revision_hash.clone(),
        row_hash: last.row_hash.clone(),
        endpoint_hash: last.endpoint_hash.clone(),
        parent_proofs,
        transform,
        catalog_publications: parents
            .iter()
            .flat_map(|proof| proof.catalog_publications.iter().copied())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect(),
        coordinate_publications: parents
            .iter()
            .flat_map(|proof| proof.coordinate_publications.iter().copied())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect(),
    }
}

#[derive(Clone, Debug)]
struct DirectSegmentProjection {
    source_endpoint: PortableEndpoint,
    target_endpoint: PortableEndpoint,
    source: FiniteOrdinalWindow,
    target: FiniteOrdinalWindow,
    proof: ResolutionProof,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ProjectionSignature {
    target_namespace: String,
    target_value: Vec<u8>,
    segment: ExactSegment,
    transform_numerator: i64,
    transform_denominator: i64,
}

fn build_direct_correspondences(
    rows: &[DirectFrontierRow],
    window: Option<&FiniteOrdinalWindow>,
    published_matches: &[PublishedReferenceMatch],
    budget: &mut SemanticBudget,
) -> (Vec<ResolvedCorrespondence>, Vec<ResolutionFinding>) {
    let mut seen_rows = BTreeSet::new();
    let mut units = rows
        .iter()
        .filter(|row| seen_rows.insert((row.pin.content_hash.clone(), row.row_hash.clone())))
        .map(|row| {
            if let Some(projection) = direct_segment_projection(row, window, published_matches) {
                let key = format!(
                    "segment:{}:{}:{}",
                    projection.source_endpoint,
                    projection.source.coordinate,
                    projection.source.start
                );
                (key, DirectPartitionUnit::Projection(projection))
            } else {
                let correspondence = unsegmented_correspondence(row, published_matches);
                let key = format!(
                    "correspondence:{}",
                    correspondence
                        .endpoints
                        .iter()
                        .map(PortableEndpoint::as_str)
                        .collect::<Vec<_>>()
                        .join("|")
                );
                (key, DirectPartitionUnit::Correspondence(correspondence))
            }
        })
        .collect::<Vec<_>>();
    units.sort_by(|left, right| left.0.cmp(&right.0));
    let mut correspondences = Vec::new();
    let mut projections = Vec::new();
    for (key, unit) in units {
        if !budget.admit_partition(key) {
            break;
        }
        match unit {
            DirectPartitionUnit::Correspondence(correspondence) => {
                correspondences.push(correspondence);
            }
            DirectPartitionUnit::Projection(projection) => projections.push(projection),
        }
    }
    let (mut segmented, findings) = partition_direct_segments(projections);
    correspondences.append(&mut segmented);
    (correspondences, findings)
}

enum DirectPartitionUnit {
    Correspondence(ResolvedCorrespondence),
    Projection(DirectSegmentProjection),
}

fn direct_segment_projection(
    row: &DirectFrontierRow,
    window: Option<&FiniteOrdinalWindow>,
    published_matches: &[PublishedReferenceMatch],
) -> Option<DirectSegmentProjection> {
    let [left, right] = row.endpoints.as_slice() else {
        return None;
    };
    let (source_endpoint, target_endpoint) = if left.endpoint_hash == row.matched_endpoint_hash {
        (left, right)
    } else if right.endpoint_hash == row.matched_endpoint_hash {
        (right, left)
    } else {
        return None;
    };
    let full_source = finite_ordinal_segment(&source_endpoint.endpoint)?;
    let mut source = full_source.clone();
    let target = finite_ordinal_segment(&target_endpoint.endpoint)?;
    if source.coordinate != target.coordinate {
        return None;
    }
    if let Some(window) = window {
        if window.coordinate != source.coordinate {
            return None;
        }
        source.start = source.start.max(window.start);
        source.end = source.end.min(window.end);
        if source.start > source.end {
            return None;
        }
    }
    let transform = exact_transform(&full_source, &target);
    Some(DirectSegmentProjection {
        source_endpoint: source_endpoint.endpoint.clone(),
        target_endpoint: target_endpoint.endpoint.clone(),
        source,
        target,
        proof: direct_proof(row, source_endpoint, transform, published_matches),
    })
}

fn finite_ordinal_segment(endpoint: &PortableEndpoint) -> Option<FiniteOrdinalWindow> {
    let [component] = endpoint.selector()?.components() else {
        return None;
    };
    let (start, end) = match component.selection() {
        Selection::Ordinal(value) => (*value, *value),
        Selection::Range { start, end } => (*start, *end),
        _ => return None,
    };
    Some(FiniteOrdinalWindow {
        coordinate: component.coordinate().to_owned(),
        start,
        end,
    })
}

fn exact_transform(source: &FiniteOrdinalWindow, target: &FiniteOrdinalWindow) -> ExactTransform {
    let source_length = source.end - source.start + 1;
    let target_length = target.end - target.start + 1;
    let divisor = greatest_common_divisor(source_length, target_length);
    ExactTransform {
        numerator: i64::try_from(source_length / divisor).expect("portable span length fits i64"),
        denominator: i64::try_from(target_length / divisor).expect("portable span length fits i64"),
    }
}

fn greatest_common_divisor(mut left: u64, mut right: u64) -> u64 {
    while right != 0 {
        let remainder = left % right;
        left = right;
        right = remainder;
    }
    left
}

fn partition_direct_segments(
    projections: Vec<DirectSegmentProjection>,
) -> (Vec<ResolvedCorrespondence>, Vec<ResolutionFinding>) {
    let mut groups = BTreeMap::<(String, Vec<u8>, String), Vec<DirectSegmentProjection>>::new();
    for projection in projections {
        groups
            .entry((
                projection.source_endpoint.namespace().to_owned(),
                projection.source_endpoint.value().to_vec(),
                projection.source.coordinate.clone(),
            ))
            .or_default()
            .push(projection);
    }
    let mut correspondences = Vec::new();
    let mut findings = Vec::new();
    for ((namespace, value, coordinate), mut projections) in groups {
        projections.sort_by(|left, right| left.proof.key.cmp(&right.proof.key));
        let mut cuts = projections
            .iter()
            .flat_map(|projection| [projection.source.start, projection.source.end + 1])
            .collect::<Vec<_>>();
        cuts.sort_unstable();
        cuts.dedup();
        for boundaries in cuts.windows(2) {
            let source_start = boundaries[0];
            let source_end = boundaries[1] - 1;
            let covering = projections
                .iter()
                .filter(|projection| {
                    projection.source.start <= source_start && projection.source.end >= source_end
                })
                .collect::<Vec<_>>();
            if covering.is_empty() {
                continue;
            }
            let mut signatures = BTreeMap::<
                ProjectionSignature,
                Vec<(&DirectSegmentProjection, ExactSegment)>,
            >::new();
            for projection in covering {
                let segment = project_segment(projection, source_start, source_end);
                signatures
                    .entry(ProjectionSignature {
                        target_namespace: projection.target_endpoint.namespace().to_owned(),
                        target_value: projection.target_endpoint.value().to_vec(),
                        segment: segment.clone(),
                        transform_numerator: projection.proof.transform.numerator,
                        transform_denominator: projection.proof.transform.denominator,
                    })
                    .or_default()
                    .push((projection, segment));
            }
            if signatures.len() == 1 {
                let support = signatures
                    .into_values()
                    .next()
                    .expect("one projection signature is present");
                let (primary, segment) = &support[0];
                correspondences.push(ResolvedCorrespondence {
                    endpoints: vec![
                        primary.source_endpoint.clone(),
                        primary.target_endpoint.clone(),
                    ],
                    segment: Some(segment.clone()),
                    support_count: u32::try_from(support.len())
                        .expect("direct support count fits u32"),
                    proof: primary.proof.clone(),
                    supporting_proofs: support
                        .iter()
                        .skip(1)
                        .map(|(projection, _)| projection.proof.clone())
                        .collect(),
                });
            } else {
                let mut proof_paths = signatures
                    .into_values()
                    .flatten()
                    .map(|(projection, _)| projection.proof.clone())
                    .collect::<Vec<_>>();
                proof_paths.sort_by(|left, right| left.key.cmp(&right.key));
                let span = FiniteOrdinalWindow {
                    coordinate: coordinate.clone(),
                    start: source_start,
                    end: source_end,
                };
                findings.push(ResolutionFinding {
                    endpoint: PortableEndpoint::from_parts(
                        &namespace,
                        &value,
                        Some(&finite_selector(&span)),
                    )
                    .expect("projected finite endpoint is valid"),
                    kind: ResolutionFindingKind::ContradictorySpans,
                    span: Some(span),
                    proof_paths,
                });
            }
        }
    }
    (correspondences, findings)
}

fn project_segment(
    projection: &DirectSegmentProjection,
    source_start: u64,
    source_end: u64,
) -> ExactSegment {
    let full_source = finite_ordinal_segment(&projection.source_endpoint)
        .expect("finite projection source remains finite");
    let source_length = u128::from(full_source.end - full_source.start + 1);
    let target_length = u128::from(projection.target.end - projection.target.start + 1);
    let target_origin = u128::from(projection.target.start) * source_length;
    let target_start = target_origin + u128::from(source_start - full_source.start) * target_length;
    let target_end_exclusive =
        target_origin + u128::from(source_end - full_source.start + 1) * target_length;
    let common = greatest_common_divisor_u128(
        greatest_common_divisor_u128(target_start, target_end_exclusive),
        source_length,
    );
    ExactSegment {
        coordinate: projection.source.coordinate.clone(),
        source_start,
        source_end,
        target_coordinate: projection.target.coordinate.clone(),
        target_start_numerator: target_start / common,
        target_end_exclusive_numerator: target_end_exclusive / common,
        target_denominator: u64::try_from(source_length / common)
            .expect("reduced target denominator fits u64"),
    }
}

fn greatest_common_divisor_u128(mut left: u128, mut right: u128) -> u128 {
    while right != 0 {
        let remainder = left % right;
        left = right;
        right = remainder;
    }
    left
}

fn finite_selector(segment: &FiniteOrdinalWindow) -> String {
    if segment.start == segment.end {
        format!("{}:{}", segment.coordinate, segment.start)
    } else {
        format!("{}:{}..{}", segment.coordinate, segment.start, segment.end)
    }
}

fn unsegmented_correspondence(
    row: &DirectFrontierRow,
    published_matches: &[PublishedReferenceMatch],
) -> ResolvedCorrespondence {
    let endpoint = row
        .endpoints
        .iter()
        .find(|endpoint| endpoint.endpoint_hash == row.matched_endpoint_hash)
        .expect("frontier match belongs to its mapping row");
    ResolvedCorrespondence {
        endpoints: row
            .endpoints
            .iter()
            .map(|endpoint| endpoint.endpoint.clone())
            .collect(),
        segment: None,
        support_count: 1,
        proof: direct_proof(
            row,
            endpoint,
            ExactTransform {
                numerator: 1,
                denominator: 1,
            },
            published_matches,
        ),
        supporting_proofs: Vec::new(),
    }
}

fn direct_proof(
    row: &DirectFrontierRow,
    endpoint: &FrontierEndpoint,
    transform: ExactTransform,
    published_matches: &[PublishedReferenceMatch],
) -> ResolutionProof {
    ResolutionProof {
        key: proof_key(row),
        kind: ResolutionProofKind::Direct,
        revision_hash: row.pin.content_hash.clone(),
        row_hash: row.row_hash.clone(),
        endpoint_hash: endpoint.endpoint_hash.clone(),
        parent_proofs: Vec::new(),
        transform,
        catalog_publications: publication_pins(row, published_matches, false),
        coordinate_publications: publication_pins(row, published_matches, true),
    }
}

fn has_published_coordinate_evidence(
    endpoint: &PortableEndpoint,
    matches: &[PublishedReferenceMatch],
    resolved_at: OffsetDateTime,
) -> bool {
    matches.iter().any(|evidence| {
        evidence.endpoint == *endpoint
            && evidence.coordinate_publication_id.is_some()
            && evidence.backing.is_some()
            && evidence
                .coordinate_expires_at
                .is_none_or(|expires_at| expires_at > resolved_at)
    })
}

fn has_expired_published_coordinate_evidence(
    endpoint: &PortableEndpoint,
    matches: &[PublishedReferenceMatch],
    resolved_at: OffsetDateTime,
) -> bool {
    matches.iter().any(|evidence| {
        evidence.endpoint == *endpoint
            && evidence.coordinate_publication_id.is_some()
            && evidence.backing.is_some()
            && evidence
                .coordinate_expires_at
                .is_some_and(|expires_at| expires_at <= resolved_at)
    })
}

fn stale_resolution(lookup_calls: u32, semantic_work: ResolutionSemanticWork) -> MappingResolution {
    MappingResolution {
        completeness: ResolutionCompleteness::Stale,
        pins: None,
        correspondences: Vec::new(),
        findings: Vec::new(),
        published_matches: Vec::new(),
        endpoint_lookup_matches: Vec::new(),
        lookup_calls,
        semantic_work,
        next_work_key: None,
    }
}

fn indeterminate_resolution() -> MappingResolution {
    MappingResolution {
        completeness: ResolutionCompleteness::Indeterminate,
        pins: None,
        correspondences: Vec::new(),
        findings: Vec::new(),
        published_matches: Vec::new(),
        endpoint_lookup_matches: Vec::new(),
        lookup_calls: 0,
        semantic_work: ResolutionSemanticWork::default(),
        next_work_key: None,
    }
}

fn sort_endpoint_lookup_batch(batch: &mut MappingEndpointLookupBatch) {
    batch.matches.sort_by(|left, right| {
        (
            &left.endpoint,
            left.connection_id,
            &left.item_key_namespace,
            &left.item_key_value,
        )
            .cmp(&(
                &right.endpoint,
                right.connection_id,
                &right.item_key_namespace,
                &right.item_key_value,
            ))
    });
    batch.findings.sort_by(|left, right| {
        (&left.endpoint, finding_priority(left.kind))
            .cmp(&(&right.endpoint, finding_priority(right.kind)))
    });
    batch.process_instances.sort();
    batch.process_instances.dedup();
}

fn lookup_pins_match(
    initial: &MappingEndpointLookupBatch,
    rechecked: &MappingEndpointLookupBatch,
) -> bool {
    initial.matches == rechecked.matches
        && initial.findings == rechecked.findings
        && initial.process_instances == rechecked.process_instances
        && initial.lookup_calls == rechecked.lookup_calls
        && initial.next_work_key == rechecked.next_work_key
}

fn blocked_traversal_endpoints(
    batch: &mut MappingEndpointLookupBatch,
) -> BTreeSet<PortableEndpoint> {
    let endpoints = batch
        .matches
        .iter()
        .map(|matched| matched.endpoint.clone())
        .chain(
            batch
                .findings
                .iter()
                .map(|finding| finding.endpoint.clone()),
        )
        .collect::<BTreeSet<_>>();
    let mut blocked = BTreeSet::new();
    for endpoint in endpoints {
        let match_count = batch
            .matches
            .iter()
            .filter(|matched| matched.endpoint == endpoint)
            .count();
        let explicit_ambiguity = batch.findings.iter().any(|finding| {
            finding.endpoint == endpoint && finding.kind == ResolutionFindingKind::Ambiguous
        });
        let absence = batch.findings.iter().any(|finding| {
            finding.endpoint == endpoint
                && matches!(
                    finding.kind,
                    ResolutionFindingKind::NotFound
                        | ResolutionFindingKind::MissingCoordinateEvidence
                        | ResolutionFindingKind::ExpiredEvidence
                )
        });
        if explicit_ambiguity || match_count > 1 || (match_count == 0 && absence) {
            if match_count > 1 && !explicit_ambiguity {
                batch.findings.push(ResolutionFinding {
                    endpoint: endpoint.clone(),
                    kind: ResolutionFindingKind::Ambiguous,
                    span: None,
                    proof_paths: Vec::new(),
                });
            }
            blocked.insert(endpoint);
        }
    }
    sort_endpoint_lookup_batch(batch);
    blocked
}

fn finding_priority(kind: ResolutionFindingKind) -> u8 {
    match kind {
        ResolutionFindingKind::Ambiguous => 0,
        ResolutionFindingKind::NotFound => 1,
        ResolutionFindingKind::Unsupported => 2,
        ResolutionFindingKind::MissingCoordinateEvidence => 3,
        _ => 4,
    }
}

fn append_lookup_results(
    batch: &mut MappingEndpointLookupBatch,
    target: &MappingEndpointLookupTarget,
    process: &ProcessInstancePin,
    resolved_at: OffsetDateTime,
    results: Vec<AdapterPortableEndpointResolution>,
) {
    for result in results {
        let outcome = match result.outcome {
            AdapterEndpointLookupOutcome::Ambiguous(candidates) => {
                let mut effective_candidates = candidates
                    .into_iter()
                    .filter(|candidate| {
                        target.accepts_mapping_root(&result.requested, &candidate.provider_item)
                    })
                    .collect::<Vec<_>>();
                match effective_candidates.len() {
                    0 => AdapterEndpointLookupOutcome::NotFound,
                    1 => AdapterEndpointLookupOutcome::Matched(Box::new(
                        effective_candidates
                            .pop()
                            .expect("candidate length checked"),
                    )),
                    _ => AdapterEndpointLookupOutcome::Ambiguous(effective_candidates),
                }
            }
            outcome => outcome,
        };
        match outcome {
            AdapterEndpointLookupOutcome::Matched(candidate) => {
                if !target.accepts_mapping_root(&result.requested, &candidate.provider_item) {
                    batch.findings.push(ResolutionFinding {
                        endpoint: result.requested,
                        kind: ResolutionFindingKind::NotFound,
                        span: None,
                        proof_paths: Vec::new(),
                    });
                    continue;
                }
                let coordinate = candidate.binding.as_ref().map(|binding| {
                    (
                        map_coordinate_backing(binding.backing()),
                        binding.evidence_revision().to_vec(),
                    )
                });
                if result.requested.selector().is_some() && coordinate.is_none() {
                    batch.findings.push(ResolutionFinding {
                        endpoint: result.requested,
                        kind: ResolutionFindingKind::MissingCoordinateEvidence,
                        span: None,
                        proof_paths: Vec::new(),
                    });
                    continue;
                }
                let item_key = candidate.provider_item.key();
                if candidate
                    .evidence
                    .expires_at
                    .is_some_and(|expires_at| expires_at <= resolved_at)
                {
                    batch.findings.push(ResolutionFinding {
                        endpoint: result.requested,
                        kind: ResolutionFindingKind::ExpiredEvidence,
                        span: None,
                        proof_paths: Vec::new(),
                    });
                    continue;
                }
                batch.matches.push(EndpointLookupMatch {
                    endpoint: result.requested,
                    connection_id: process.connection_id,
                    process_instance_id: process.process_instance_id.clone(),
                    item_key_namespace: item_key.namespace().to_owned(),
                    item_key_value: item_key.value().to_vec(),
                    backing: coordinate.as_ref().map(|(backing, _)| *backing),
                    coordinate_evidence_revision: coordinate
                        .map(|(_, evidence_revision)| evidence_revision),
                    lookup_evidence_revision: candidate.evidence.adapter_revision,
                    observed_at: candidate.evidence.observed_at,
                    expires_at: candidate.evidence.expires_at,
                });
            }
            AdapterEndpointLookupOutcome::NotFound => {
                batch.findings.push(ResolutionFinding {
                    endpoint: result.requested,
                    kind: ResolutionFindingKind::NotFound,
                    span: None,
                    proof_paths: Vec::new(),
                });
            }
            AdapterEndpointLookupOutcome::Ambiguous(_) => {
                batch.findings.push(ResolutionFinding {
                    endpoint: result.requested,
                    kind: ResolutionFindingKind::Ambiguous,
                    span: None,
                    proof_paths: Vec::new(),
                });
            }
            AdapterEndpointLookupOutcome::Unsupported => {
                batch.findings.push(ResolutionFinding {
                    endpoint: result.requested,
                    kind: ResolutionFindingKind::Unsupported,
                    span: None,
                    proof_paths: Vec::new(),
                });
            }
        }
    }
}

fn map_coordinate_backing(backing: ProviderCoordinateBacking) -> CoordinateBacking {
    match backing {
        ProviderCoordinateBacking::Materialized => CoordinateBacking::Materialized,
        ProviderCoordinateBacking::Virtual => CoordinateBacking::Virtual,
        ProviderCoordinateBacking::Aggregate => CoordinateBacking::Aggregate,
    }
}

async fn restore_mapping_session<G, S>(sessions: &G, connection_id: Uuid, session: S) -> bool
where
    G: ConnectionSessionRegistry<S>,
    S: AdapterSession,
{
    if let Some(unexpected) = sessions.replace(connection_id, session) {
        if let Some(installed) = sessions.remove(connection_id) {
            let _ = installed.retire().await;
        }
        let _ = unexpected.retire().await;
        return false;
    }
    true
}

fn publication_pins(
    row: &DirectFrontierRow,
    matches: &[PublishedReferenceMatch],
    coordinates: bool,
) -> Vec<Uuid> {
    let row_endpoints = row
        .endpoints
        .iter()
        .map(|endpoint| &endpoint.endpoint)
        .collect::<BTreeSet<_>>();
    let mut publications = matches
        .iter()
        .filter(|evidence| row_endpoints.contains(&evidence.endpoint))
        .filter_map(|evidence| {
            if coordinates {
                evidence.coordinate_publication_id
            } else {
                Some(evidence.catalog_publication_id)
            }
        })
        .collect::<Vec<_>>();
    publications.sort_unstable();
    publications.dedup();
    publications
}

fn proof_key(row: &DirectFrontierRow) -> String {
    let mut digest = Sha256::new();
    digest.update(row.pin.content_hash.as_bytes());
    digest.update([0]);
    digest.update(row.row_hash.as_bytes());
    digest.update([0]);
    digest.update(row.matched_endpoint_hash.as_bytes());
    format!("sha256:{}", hex::encode(digest.finalize()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::adapter_runtime::{
        AdapterEndpointLookupCandidate, AdapterLookupEvidence,
    };
    use crate::domain::acquisition::{
        PortableReference as ProviderPortableReference, ProviderItem, ProviderItemKey, Term,
    };

    struct PendingResolutionRepository;

    impl MappingResolutionRepository for PendingResolutionRepository {
        type Error = std::convert::Infallible;

        fn load_direct_frontier(
            &self,
            _request: LoadDirectFrontier,
        ) -> MappingResolutionFuture<'_, DirectFrontierSnapshot, Self::Error> {
            Box::pin(std::future::pending())
        }

        fn recheck_direct_resolution(
            &self,
            _workspace_id: Uuid,
            _revision: Option<ExactMappingRevision>,
            _expected: DirectResolutionPins,
        ) -> MappingResolutionFuture<'_, bool, Self::Error> {
            Box::pin(async { Ok(true) })
        }
    }

    #[test]
    fn default_semantic_limits_are_finite() {
        let limits = ResolutionSemanticLimits::default();

        assert_eq!(limits.endpoint_hits, 100_000);
        assert_eq!(limits.candidate_rows, 50_000);
        assert_eq!(limits.traversal_hops, 10_000);
        assert_eq!(limits.partitions, 100_000);
        assert_eq!(limits.lookup_references, 256);
        assert_eq!(limits.lookup_calls, 256);
        assert_eq!(limits.lookup_bytes, 1_048_576);
    }

    #[test]
    fn adapter_ambiguity_is_filtered_before_classification() {
        let reference = ProviderPortableReference::new("example.media", b"item".to_vec())
            .expect("reference is valid");
        let provider_item = |key: &str, recommended: bool| {
            ProviderItem::new(
                ProviderItemKey::new("fixture.item", key.as_bytes().to_vec())
                    .expect("provider item key is valid"),
                Term::new("example.media", "item").expect("provider item kind is valid"),
                key,
                serde_json::json!({}),
                vec![reference.clone()],
                recommended.then(|| reference.clone()).into_iter().collect(),
                Vec::new(),
            )
            .expect("provider item is valid")
        };
        let candidate = |provider_item| AdapterEndpointLookupCandidate {
            provider_item,
            binding: None,
            evidence: AdapterLookupEvidence {
                adapter_revision: b"lookup-1".to_vec(),
                observed_at: OffsetDateTime::UNIX_EPOCH,
                expires_at: None,
                matched_references: vec![reference.clone()],
            },
        };
        let endpoint =
            PortableEndpoint::parse("example.media://item").expect("lookup endpoint is valid");
        let target = MappingEndpointLookupTarget {
            source_id: Uuid::from_u128(2),
            connection_id: Uuid::from_u128(1),
            source_key: crate::application::adapter_runtime::AdapterKey {
                namespace: "fixture".to_owned(),
                value: b"source-a".to_vec(),
            },
            mapping_root_policy: MappingRootPolicy::ProviderRecommended,
            bare_reference_namespaces: vec!["example.media".to_owned()],
            maximum_bare_batch_size: 10,
            reference_namespaces: Vec::new(),
            coordinate_ids: Vec::new(),
            maximum_batch_size: 0,
            maximum_response_bytes: 1024,
        };
        let process = ProcessInstancePin {
            connection_id: target.connection_id,
            process_instance_id: "process-1".to_owned(),
        };
        let mut batch = MappingEndpointLookupBatch::default();

        append_lookup_results(
            &mut batch,
            &target,
            &process,
            OffsetDateTime::UNIX_EPOCH,
            vec![AdapterPortableEndpointResolution {
                requested: endpoint,
                outcome: AdapterEndpointLookupOutcome::Ambiguous(vec![
                    candidate(provider_item("candidate-only", false)),
                    candidate(provider_item("recommended", true)),
                ]),
            }],
        );

        assert!(batch.findings.is_empty());
        assert_eq!(batch.matches.len(), 1);
        assert_eq!(batch.matches[0].item_key_value, b"recommended");
    }

    #[tokio::test]
    async fn resolution_deadline_returns_empty_indeterminate_result() {
        let resolution = MappingResolutionService::new(PendingResolutionRepository)
            .with_deadline(Duration::from_millis(1))
            .resolve(ResolutionRequest {
                workspace_id: Uuid::from_u128(2),
                mode: ResolutionMode::Direct,
                references: vec![
                    PortableEndpoint::parse("example.media://a")
                        .expect("deadline endpoint is valid"),
                ],
                window: None,
                resolved_at: OffsetDateTime::UNIX_EPOCH,
            })
            .await
            .expect("deadline returns a closed result");

        assert_eq!(
            resolution,
            MappingResolution {
                completeness: ResolutionCompleteness::Indeterminate,
                pins: None,
                correspondences: Vec::new(),
                findings: Vec::new(),
                published_matches: Vec::new(),
                endpoint_lookup_matches: Vec::new(),
                lookup_calls: 0,
                semantic_work: ResolutionSemanticWork::default(),
                next_work_key: None,
            }
        );
    }

    #[test]
    fn adapter_lookup_plan_enforces_canonical_batch_call_limit() {
        let endpoints = vec![
            PortableEndpoint::parse("example.media://a[episode:1]")
                .expect("first endpoint is valid"),
            PortableEndpoint::parse("example.media://b[episode:1]")
                .expect("second endpoint is valid"),
        ];
        let target = MappingEndpointLookupTarget {
            source_id: Uuid::from_u128(2),
            connection_id: Uuid::from_u128(1),
            source_key: crate::application::adapter_runtime::AdapterKey {
                namespace: "fixture".to_owned(),
                value: b"source-a".to_vec(),
            },
            mapping_root_policy: MappingRootPolicy::ProviderRecommended,
            bare_reference_namespaces: Vec::new(),
            maximum_bare_batch_size: 1,
            reference_namespaces: vec!["example.media".to_owned()],
            coordinate_ids: vec!["episode".to_owned()],
            maximum_batch_size: 1,
            maximum_response_bytes: 1024,
        };

        let exact = plan_mapping_endpoint_lookups(vec![target.clone()], &endpoints, 2)
            .expect("exact call limit plans all batches");
        assert_eq!(exact.targets.len(), 1);
        assert_eq!(exact.targets[0].batches.len(), 2);
        assert_eq!(exact.next_work_key, None);

        let overflow = plan_mapping_endpoint_lookups(vec![target], &endpoints, 1)
            .expect("call overflow returns a bounded plan");
        assert_eq!(overflow.targets.len(), 1);
        assert_eq!(
            overflow.targets[0].batches,
            vec![vec![endpoints[0].clone()]]
        );
        assert_eq!(
            overflow.next_work_key.as_deref(),
            Some("lookup-call:example.media://b[episode:1]")
        );
    }

    #[test]
    fn adapter_lookup_plan_splits_reference_and_endpoint_rpcs() {
        let bare = PortableEndpoint::parse("example.media://a").expect("bare endpoint is valid");
        let selected = PortableEndpoint::parse("example.media://b[episode:1]")
            .expect("selected endpoint is valid");
        let target = MappingEndpointLookupTarget {
            source_id: Uuid::from_u128(2),
            connection_id: Uuid::from_u128(1),
            source_key: crate::application::adapter_runtime::AdapterKey {
                namespace: "fixture".to_owned(),
                value: b"source-a".to_vec(),
            },
            mapping_root_policy: MappingRootPolicy::ProviderRecommended,
            bare_reference_namespaces: vec!["example.media".to_owned()],
            maximum_bare_batch_size: 10,
            reference_namespaces: vec!["example.media".to_owned()],
            coordinate_ids: vec!["episode".to_owned()],
            maximum_batch_size: 10,
            maximum_response_bytes: 1024,
        };

        let exact = plan_mapping_endpoint_lookups(
            vec![target.clone()],
            &[bare.clone(), selected.clone()],
            2,
        )
        .expect("two calls cover both capability paths");
        assert_eq!(
            exact.targets[0].batches,
            vec![vec![bare.clone()], vec![selected.clone()]]
        );

        let bounded = plan_mapping_endpoint_lookups(vec![target], &[bare.clone(), selected], 1)
            .expect("one call returns a bounded plan");
        assert_eq!(bounded.targets[0].batches, vec![vec![bare]]);
        assert_eq!(
            bounded.next_work_key.as_deref(),
            Some("lookup-call:example.media://b[episode:1]")
        );
    }
}
