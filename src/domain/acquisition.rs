use serde_json::Value;
use thiserror::Error;
use time::OffsetDateTime;
use trakkin_mapping::PortableEndpoint;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ProviderItemKey {
    namespace: String,
    value: Vec<u8>,
}

impl ProviderItemKey {
    pub fn new(
        namespace: impl Into<String>,
        value: impl Into<Vec<u8>>,
    ) -> Result<Self, AcquisitionValueError> {
        let namespace = namespace.into();
        let value = value.into();
        validate_namespaced_value(&namespace, &value)?;
        Ok(Self { namespace, value })
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn value(&self) -> &[u8] {
        &self.value
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct CatalogRelationKey {
    namespace: String,
    value: Vec<u8>,
}

impl CatalogRelationKey {
    pub fn new(
        namespace: impl Into<String>,
        value: impl Into<Vec<u8>>,
    ) -> Result<Self, AcquisitionValueError> {
        let namespace = namespace.into();
        let value = value.into();
        validate_namespaced_value(&namespace, &value)?;
        Ok(Self { namespace, value })
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn value(&self) -> &[u8] {
        &self.value
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct BinaryAssetKey {
    namespace: String,
    value: Vec<u8>,
}

impl BinaryAssetKey {
    pub fn new(
        namespace: impl Into<String>,
        value: impl Into<Vec<u8>>,
    ) -> Result<Self, AcquisitionValueError> {
        let namespace = namespace.into();
        let value = value.into();
        validate_namespaced_value(&namespace, &value)?;
        Ok(Self { namespace, value })
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn value(&self) -> &[u8] {
        &self.value
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Term {
    namespace: String,
    name: String,
}

impl Term {
    pub fn new(
        namespace: impl Into<String>,
        name: impl Into<String>,
    ) -> Result<Self, AcquisitionValueError> {
        let namespace = namespace.into();
        let name = name.into();
        if namespace.is_empty() {
            return Err(AcquisitionValueError::EmptyNamespace);
        }
        if name.is_empty() {
            return Err(AcquisitionValueError::EmptyName);
        }
        Ok(Self { namespace, name })
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct PortableReference {
    namespace: String,
    value: Vec<u8>,
}

impl PortableReference {
    pub fn new(
        namespace: impl Into<String>,
        value: impl Into<Vec<u8>>,
    ) -> Result<Self, AcquisitionValueError> {
        let namespace = namespace.into();
        let value = value.into();
        validate_namespaced_value(&namespace, &value)?;
        Ok(Self { namespace, value })
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn value(&self) -> &[u8] {
        &self.value
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BinaryAssetReference {
    key: BinaryAssetKey,
    kind: Term,
}

impl BinaryAssetReference {
    pub fn new(key: BinaryAssetKey, kind: Term) -> Self {
        Self { key, kind }
    }

    pub fn key(&self) -> &BinaryAssetKey {
        &self.key
    }

    pub fn kind(&self) -> &Term {
        &self.kind
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ProviderItem {
    key: ProviderItemKey,
    kind: Term,
    display_name: String,
    attributes: Value,
    portable_references: Vec<PortableReference>,
    binary_assets: Vec<BinaryAssetReference>,
}

impl ProviderItem {
    pub fn new(
        key: ProviderItemKey,
        kind: Term,
        display_name: impl Into<String>,
        attributes: Value,
        portable_references: Vec<PortableReference>,
        binary_assets: Vec<BinaryAssetReference>,
    ) -> Result<Self, AcquisitionValueError> {
        let display_name = display_name.into();
        if display_name.is_empty() {
            return Err(AcquisitionValueError::EmptyDisplayName);
        }
        if !attributes.is_object() {
            return Err(AcquisitionValueError::AttributesMustBeObject);
        }
        Ok(Self {
            key,
            kind,
            display_name,
            attributes,
            portable_references,
            binary_assets,
        })
    }

    pub fn key(&self) -> &ProviderItemKey {
        &self.key
    }

    pub fn kind(&self) -> &Term {
        &self.kind
    }

    pub fn display_name(&self) -> &str {
        &self.display_name
    }

    pub fn attributes(&self) -> &Value {
        &self.attributes
    }

    pub fn portable_references(&self) -> &[PortableReference] {
        &self.portable_references
    }

    pub fn binary_assets(&self) -> &[BinaryAssetReference] {
        &self.binary_assets
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct CatalogItem {
    provider_item: ProviderItem,
}

impl CatalogItem {
    pub fn new(
        key: ProviderItemKey,
        kind: Term,
        display_name: impl Into<String>,
        attributes: Value,
        portable_references: Vec<PortableReference>,
        binary_assets: Vec<BinaryAssetReference>,
    ) -> Result<Self, AcquisitionValueError> {
        ProviderItem::new(
            key,
            kind,
            display_name,
            attributes,
            portable_references,
            binary_assets,
        )
        .map(Self::from)
    }

    pub fn key(&self) -> &ProviderItemKey {
        self.provider_item.key()
    }

    pub fn kind(&self) -> &Term {
        self.provider_item.kind()
    }

    pub fn display_name(&self) -> &str {
        self.provider_item.display_name()
    }

    pub fn attributes(&self) -> &Value {
        self.provider_item.attributes()
    }

    pub fn portable_references(&self) -> &[PortableReference] {
        self.provider_item.portable_references()
    }

    pub fn binary_assets(&self) -> &[BinaryAssetReference] {
        self.provider_item.binary_assets()
    }
}

impl From<ProviderItem> for CatalogItem {
    fn from(provider_item: ProviderItem) -> Self {
        Self { provider_item }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct CatalogRelation {
    key: CatalogRelationKey,
    parent_key: Option<CatalogRelationKey>,
    provider_item_key: Option<ProviderItemKey>,
    kind: Term,
    order: Vec<i64>,
    attributes: Value,
}

impl CatalogRelation {
    pub fn new(
        key: CatalogRelationKey,
        parent_key: Option<CatalogRelationKey>,
        provider_item_key: Option<ProviderItemKey>,
        kind: Term,
        order: Vec<i64>,
        attributes: Value,
    ) -> Result<Self, AcquisitionValueError> {
        if !attributes.is_object() {
            return Err(AcquisitionValueError::AttributesMustBeObject);
        }
        Ok(Self {
            key,
            parent_key,
            provider_item_key,
            kind,
            order,
            attributes,
        })
    }

    pub fn key(&self) -> &CatalogRelationKey {
        &self.key
    }

    pub fn parent_key(&self) -> Option<&CatalogRelationKey> {
        self.parent_key.as_ref()
    }

    pub fn provider_item_key(&self) -> Option<&ProviderItemKey> {
        self.provider_item_key.as_ref()
    }

    pub fn kind(&self) -> &Term {
        &self.kind
    }

    pub fn order(&self) -> &[i64] {
        &self.order
    }

    pub fn attributes(&self) -> &Value {
        &self.attributes
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CoordinateBacking {
    Materialized,
    Virtual,
    Aggregate,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CoordinateSubject {
    ProviderItem(ProviderItemKey),
    CatalogRelation(CatalogRelationKey),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CoordinateBindingKey {
    endpoint: PortableEndpoint,
    subject: CoordinateSubject,
}

impl CoordinateBindingKey {
    pub fn new(endpoint: PortableEndpoint, subject: CoordinateSubject) -> Self {
        Self { endpoint, subject }
    }

    pub fn endpoint(&self) -> &PortableEndpoint {
        &self.endpoint
    }

    pub fn subject(&self) -> &CoordinateSubject {
        &self.subject
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CoordinateBinding {
    key: CoordinateBindingKey,
    backing: CoordinateBacking,
    evidence_revision: Vec<u8>,
}

impl CoordinateBinding {
    pub fn new(
        key: CoordinateBindingKey,
        backing: CoordinateBacking,
        evidence_revision: Vec<u8>,
    ) -> Result<Self, AcquisitionValueError> {
        if evidence_revision.is_empty() {
            return Err(AcquisitionValueError::EmptyCoordinateEvidenceRevision);
        }
        Ok(Self {
            key,
            backing,
            evidence_revision,
        })
    }

    pub fn key(&self) -> &CoordinateBindingKey {
        &self.key
    }

    pub fn backing(&self) -> CoordinateBacking {
        self.backing
    }

    pub fn evidence_revision(&self) -> &[u8] {
        &self.evidence_revision
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct CatalogBatch {
    sequence: u64,
    item_upserts: Vec<CatalogItem>,
    item_deletes: Vec<ProviderItemKey>,
    relation_upserts: Vec<CatalogRelation>,
    relation_deletes: Vec<CatalogRelationKey>,
    coordinate_binding_upserts: Vec<CoordinateBinding>,
    coordinate_binding_deletes: Vec<CoordinateBindingKey>,
}

impl CatalogBatch {
    pub fn new(
        sequence: u64,
        item_upserts: Vec<CatalogItem>,
        item_deletes: Vec<ProviderItemKey>,
        relation_upserts: Vec<CatalogRelation>,
        relation_deletes: Vec<CatalogRelationKey>,
    ) -> Self {
        Self {
            sequence,
            item_upserts,
            item_deletes,
            relation_upserts,
            relation_deletes,
            coordinate_binding_upserts: Vec::new(),
            coordinate_binding_deletes: Vec::new(),
        }
    }

    pub fn with_coordinate_bindings(
        mut self,
        upserts: Vec<CoordinateBinding>,
        deletes: Vec<CoordinateBindingKey>,
    ) -> Self {
        self.coordinate_binding_upserts = upserts;
        self.coordinate_binding_deletes = deletes;
        self
    }

    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    pub fn item_upserts(&self) -> &[CatalogItem] {
        &self.item_upserts
    }

    pub fn item_deletes(&self) -> &[ProviderItemKey] {
        &self.item_deletes
    }

    pub fn relation_upserts(&self) -> &[CatalogRelation] {
        &self.relation_upserts
    }

    pub fn relation_deletes(&self) -> &[CatalogRelationKey] {
        &self.relation_deletes
    }

    pub fn coordinate_binding_upserts(&self) -> &[CoordinateBinding] {
        &self.coordinate_binding_upserts
    }

    pub fn coordinate_binding_deletes(&self) -> &[CoordinateBindingKey] {
        &self.coordinate_binding_deletes
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StateSubject {
    ProviderItem(ProviderItemKey),
    CatalogRelation(CatalogRelationKey),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StateField {
    field: Term,
    unit: Option<Term>,
}

impl StateField {
    pub fn new(field: Term, unit: Option<Term>) -> Self {
        Self { field, unit }
    }

    pub fn field(&self) -> &Term {
        &self.field
    }

    pub fn unit(&self) -> Option<&Term> {
        self.unit.as_ref()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StateValue {
    Boolean(bool),
    Integer(i64),
    Decimal(String),
    Text(String),
    Bytes(Vec<u8>),
    TimestampMilliseconds(i64),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StateObservation {
    subject: StateSubject,
    field: StateField,
    value: Option<StateValue>,
    provider_revision: Vec<u8>,
    observed_at: OffsetDateTime,
}

impl StateObservation {
    pub fn set(
        subject: StateSubject,
        field: StateField,
        value: StateValue,
        provider_revision: impl Into<Vec<u8>>,
        observed_at: OffsetDateTime,
    ) -> Result<Self, AcquisitionValueError> {
        Self::new(subject, field, Some(value), provider_revision, observed_at)
    }

    pub fn delete(
        subject: StateSubject,
        field: StateField,
        provider_revision: impl Into<Vec<u8>>,
        observed_at: OffsetDateTime,
    ) -> Result<Self, AcquisitionValueError> {
        Self::new(subject, field, None, provider_revision, observed_at)
    }

    fn new(
        subject: StateSubject,
        field: StateField,
        value: Option<StateValue>,
        provider_revision: impl Into<Vec<u8>>,
        observed_at: OffsetDateTime,
    ) -> Result<Self, AcquisitionValueError> {
        let provider_revision = provider_revision.into();
        if provider_revision.is_empty() {
            return Err(AcquisitionValueError::EmptyProviderRevision);
        }
        Ok(Self {
            subject,
            field,
            value,
            provider_revision,
            observed_at,
        })
    }

    pub fn subject(&self) -> &StateSubject {
        &self.subject
    }

    pub fn field(&self) -> &StateField {
        &self.field
    }

    pub fn value(&self) -> Option<&StateValue> {
        self.value.as_ref()
    }

    pub fn provider_revision(&self) -> &[u8] {
        &self.provider_revision
    }

    pub fn observed_at(&self) -> OffsetDateTime {
        self.observed_at
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StateBatch {
    sequence: u64,
    observations: Vec<StateObservation>,
}

impl StateBatch {
    pub fn new(sequence: u64, observations: Vec<StateObservation>) -> Self {
        Self {
            sequence,
            observations,
        }
    }

    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    pub fn observations(&self) -> &[StateObservation] {
        &self.observations
    }
}

fn validate_namespaced_value(namespace: &str, value: &[u8]) -> Result<(), AcquisitionValueError> {
    if namespace.is_empty() {
        return Err(AcquisitionValueError::EmptyNamespace);
    }
    if value.is_empty() {
        return Err(AcquisitionValueError::EmptyValue);
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum AcquisitionValueError {
    #[error("namespace must not be empty")]
    EmptyNamespace,
    #[error("value must not be empty")]
    EmptyValue,
    #[error("name must not be empty")]
    EmptyName,
    #[error("display name must not be empty")]
    EmptyDisplayName,
    #[error("attributes must be a JSON object")]
    AttributesMustBeObject,
    #[error("provider revision must not be empty")]
    EmptyProviderRevision,
    #[error("coordinate evidence revision must not be empty")]
    EmptyCoordinateEvidenceRevision,
}
