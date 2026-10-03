use std::{future::Future, pin::Pin};

use uuid::Uuid;

use crate::domain::acquisition::{
    CatalogItem, CatalogRelation, CatalogRelationKey, StateObservation, StateSubject,
};

pub type CatalogQueryFuture<'a, T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>;

#[derive(Clone, Debug, PartialEq)]
pub struct CatalogEntry {
    pub relation: CatalogRelation,
    pub item: Option<CatalogItem>,
    pub has_children: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryCatalogPage {
    pub workspace_id: Uuid,
    pub source_id: Uuid,
    pub parent: Option<CatalogRelationKey>,
    pub search: Option<String>,
    pub limit: u32,
    pub cursor: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CatalogPage {
    pub publication_id: Uuid,
    pub entries: Vec<CatalogEntry>,
    pub next_cursor: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InspectSourceState {
    pub workspace_id: Uuid,
    pub source_id: Uuid,
    pub subject: Option<StateSubject>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceStateInspection {
    pub publication_id: Uuid,
    pub observations: Vec<StateObservation>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolveSubjectUnits {
    pub workspace_id: Uuid,
    pub source_id: Uuid,
    pub subject: StateSubject,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SubjectUnitCandidate {
    pub unit_key: String,
    pub relevance_rank: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SubjectUnitCandidates {
    pub publication_id: Uuid,
    pub candidates: Vec<SubjectUnitCandidate>,
}

pub trait CatalogQueryRepository: Send + Sync {
    type Error;

    fn query_catalog_page(
        &self,
        query: QueryCatalogPage,
    ) -> CatalogQueryFuture<'_, CatalogPage, Self::Error>;

    fn inspect_source_state(
        &self,
        query: InspectSourceState,
    ) -> CatalogQueryFuture<'_, SourceStateInspection, Self::Error>;

    fn resolve_subject_units(
        &self,
        query: ResolveSubjectUnits,
    ) -> CatalogQueryFuture<'_, SubjectUnitCandidates, Self::Error>;
}
