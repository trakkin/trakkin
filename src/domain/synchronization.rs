use std::fmt;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

pub const STATE_REGISTRY_SCHEMA: &str = "trakkin.state/v1";
pub const SYNC_POLICY_SCHEMA: &str = "trakkin.sync-policy/v1";

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StateValueShape {
    Boolean,
    Integer,
    Rational,
    Term,
}

impl StateValueShape {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Boolean => "boolean",
            Self::Integer => "integer",
            Self::Rational => "rational",
            Self::Term => "term",
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StateUnit {
    Chapter,
    Count,
    Episode,
    Flag,
    Page,
    Ratio,
    Time,
}

impl StateUnit {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Chapter => "chapter",
            Self::Count => "count",
            Self::Episode => "episode",
            Self::Flag => "flag",
            Self::Page => "page",
            Self::Ratio => "ratio",
            Self::Time => "time",
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Reducer {
    All,
    Any,
    CoverageContiguousPrefix,
    CoverageIntersection,
    CoverageUnion,
    Equal,
    Maximum,
    Mean,
    Minimum,
    Sum,
}

impl Reducer {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::Any => "any",
            Self::CoverageContiguousPrefix => "coverage_contiguous_prefix",
            Self::CoverageIntersection => "coverage_intersection",
            Self::CoverageUnion => "coverage_union",
            Self::Equal => "equal",
            Self::Maximum => "maximum",
            Self::Mean => "mean",
            Self::Minimum => "minimum",
            Self::Sum => "sum",
        }
    }

    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "all" => Some(Self::All),
            "any" => Some(Self::Any),
            "coverage_contiguous_prefix" => Some(Self::CoverageContiguousPrefix),
            "coverage_intersection" => Some(Self::CoverageIntersection),
            "coverage_union" => Some(Self::CoverageUnion),
            "equal" => Some(Self::Equal),
            "maximum" => Some(Self::Maximum),
            "mean" => Some(Self::Mean),
            "minimum" => Some(Self::Minimum),
            "sum" => Some(Self::Sum),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Quantizer {
    Ceiling,
    Exact,
    Floor,
    Nearest,
}

impl Quantizer {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ceiling => "ceiling",
            Self::Exact => "exact",
            Self::Floor => "floor",
            Self::Nearest => "nearest",
        }
    }

    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "ceiling" => Some(Self::Ceiling),
            "exact" => Some(Self::Exact),
            "floor" => Some(Self::Floor),
            "nearest" => Some(Self::Nearest),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExactRational {
    numerator: u64,
    denominator: u64,
}

impl ExactRational {
    pub fn new(numerator: u64, denominator: u64) -> Result<Self, SyncComputationError> {
        if denominator == 0 {
            return Err(SyncComputationError::ZeroDenominator);
        }
        let divisor = greatest_common_divisor(numerator, denominator);
        Ok(Self {
            numerator: numerator / divisor,
            denominator: denominator / divisor,
        })
    }

    #[must_use]
    pub const fn numerator(self) -> u64 {
        self.numerator
    }

    #[must_use]
    pub const fn denominator(self) -> u64 {
        self.denominator
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct OrdinalSpan {
    coordinate: String,
    start: u64,
    end: u64,
}

impl OrdinalSpan {
    pub fn new(
        coordinate: impl Into<String>,
        start: u64,
        end: u64,
    ) -> Result<Self, SyncComputationError> {
        let coordinate = coordinate.into();
        if coordinate.is_empty() {
            return Err(SyncComputationError::EmptyCoordinate);
        }
        if start > end || end == u64::MAX {
            return Err(SyncComputationError::InvalidOrdinalSpan);
        }
        Ok(Self {
            coordinate,
            start,
            end,
        })
    }

    #[must_use]
    pub fn coordinate(&self) -> &str {
        &self.coordinate
    }

    #[must_use]
    pub const fn start(&self) -> u64 {
        self.start
    }

    #[must_use]
    pub const fn end(&self) -> u64 {
        self.end
    }

    #[must_use]
    pub const fn length(&self) -> u64 {
        self.end - self.start + 1
    }

    #[must_use]
    pub fn intersects(&self, other: &Self) -> bool {
        self.coordinate == other.coordinate && self.start <= other.end && other.start <= self.end
    }

    #[must_use]
    pub fn intersection(&self, other: &Self) -> Option<Self> {
        self.intersects(other).then(|| Self {
            coordinate: self.coordinate.clone(),
            start: self.start.max(other.start),
            end: self.end.min(other.end),
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QuantizedOrdinal {
    exact: ExactRational,
    intended: u64,
    lossy: bool,
}

impl QuantizedOrdinal {
    #[must_use]
    pub const fn exact(self) -> ExactRational {
        self.exact
    }

    #[must_use]
    pub const fn intended(self) -> u64 {
        self.intended
    }

    #[must_use]
    pub const fn lossy(self) -> bool {
        self.lossy
    }
}

pub fn aggregate_contiguous_prefix(
    partition: &OrdinalSpan,
    coverage: &[OrdinalSpan],
) -> Result<ExactRational, SyncComputationError> {
    let mut clipped = coverage
        .iter()
        .filter_map(|span| span.intersection(partition))
        .collect::<Vec<_>>();
    clipped.sort_unstable();

    let mut next = partition.start;
    for span in clipped {
        if span.start > next {
            break;
        }
        if span.end >= next {
            next = span.end + 1;
        }
        if next > partition.end {
            break;
        }
    }
    ExactRational::new(next.saturating_sub(partition.start), partition.length())
}

pub fn quantize_partition_coverage(
    coverage: ExactRational,
    partition_length: u64,
    minimum: u64,
    maximum: u64,
    step: u64,
    quantizer: Quantizer,
) -> Result<QuantizedOrdinal, SyncComputationError> {
    if step == 0 || minimum > maximum {
        return Err(SyncComputationError::InvalidReceiverRange);
    }
    let numerator = coverage
        .numerator
        .checked_mul(partition_length)
        .ok_or(SyncComputationError::ArithmeticOverflow)?;
    let denominator = coverage.denominator;
    let minimum_numerator = minimum
        .checked_mul(denominator)
        .ok_or(SyncComputationError::ArithmeticOverflow)?;
    let maximum_numerator = maximum
        .checked_mul(denominator)
        .ok_or(SyncComputationError::ArithmeticOverflow)?;
    if numerator < minimum_numerator || numerator > maximum_numerator {
        return Err(SyncComputationError::ReceiverRangeExceeded);
    }

    let offset_numerator = numerator - minimum_numerator;
    let step_numerator = step
        .checked_mul(denominator)
        .ok_or(SyncComputationError::ArithmeticOverflow)?;
    let lower_steps = offset_numerator / step_numerator;
    let remainder = offset_numerator % step_numerator;
    let selected_steps = match quantizer {
        Quantizer::Exact if remainder != 0 => {
            return Err(SyncComputationError::InexactTranslation);
        }
        Quantizer::Exact | Quantizer::Floor => lower_steps,
        Quantizer::Ceiling => lower_steps + u64::from(remainder != 0),
        Quantizer::Nearest => {
            lower_steps + u64::from(remainder.saturating_mul(2) >= step_numerator)
        }
    };
    let intended = selected_steps
        .checked_mul(step)
        .and_then(|offset| minimum.checked_add(offset))
        .ok_or(SyncComputationError::ArithmeticOverflow)?;
    if intended > maximum {
        return Err(SyncComputationError::ReceiverRangeExceeded);
    }
    let exact = ExactRational::new(numerator, denominator)?;
    Ok(QuantizedOrdinal {
        exact,
        intended,
        lossy: intended
            .checked_mul(denominator)
            .ok_or(SyncComputationError::ArithmeticOverflow)?
            != numerator,
    })
}

const fn greatest_common_divisor(mut left: u64, mut right: u64) -> u64 {
    while right != 0 {
        let remainder = left % right;
        left = right;
        right = remainder;
    }
    if left == 0 { 1 } else { left }
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum SyncComputationError {
    #[error("exact rational denominator must be nonzero")]
    ZeroDenominator,
    #[error("ordinal coordinate must not be empty")]
    EmptyCoordinate,
    #[error("ordinal span must be increasing and finitely bounded")]
    InvalidOrdinalSpan,
    #[error("receiver range and step must be valid")]
    InvalidReceiverRange,
    #[error("candidate is outside the receiver range")]
    ReceiverRangeExceeded,
    #[error("candidate cannot be represented exactly by the receiver step")]
    InexactTranslation,
    #[error("synchronization arithmetic overflowed")]
    ArithmeticOverflow,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StateFieldDefinition {
    id: &'static str,
    shape: StateValueShape,
    units: &'static [StateUnit],
    reducers: &'static [Reducer],
    quantizers: &'static [Quantizer],
}

impl StateFieldDefinition {
    #[must_use]
    pub const fn id(self) -> &'static str {
        self.id
    }

    #[must_use]
    pub const fn shape(self) -> StateValueShape {
        self.shape
    }

    #[must_use]
    pub const fn units(self) -> &'static [StateUnit] {
        self.units
    }

    #[must_use]
    pub const fn reducers(self) -> &'static [Reducer] {
        self.reducers
    }

    #[must_use]
    pub const fn quantizers(self) -> &'static [Quantizer] {
        self.quantizers
    }
}

const EXACT_QUANTIZER: &[Quantizer] = &[Quantizer::Exact];
const RATIONAL_QUANTIZERS: &[Quantizer] = &[
    Quantizer::Exact,
    Quantizer::Nearest,
    Quantizer::Floor,
    Quantizer::Ceiling,
];
const BOOLEAN_REDUCERS: &[Reducer] = &[Reducer::Equal, Reducer::Any, Reducer::All];
const PROGRESS_REDUCERS: &[Reducer] = &[
    Reducer::Equal,
    Reducer::Minimum,
    Reducer::Maximum,
    Reducer::CoverageUnion,
    Reducer::CoverageIntersection,
    Reducer::CoverageContiguousPrefix,
];
const RATING_REDUCERS: &[Reducer] = &[
    Reducer::Equal,
    Reducer::Minimum,
    Reducer::Maximum,
    Reducer::Mean,
];
const REWATCH_REDUCERS: &[Reducer] = &[
    Reducer::Equal,
    Reducer::Minimum,
    Reducer::Maximum,
    Reducer::Sum,
];
const STATUS_REDUCERS: &[Reducer] = &[Reducer::Equal];
const FLAG_UNIT: &[StateUnit] = &[StateUnit::Flag];
const PROGRESS_UNITS: &[StateUnit] = &[
    StateUnit::Episode,
    StateUnit::Chapter,
    StateUnit::Page,
    StateUnit::Time,
];
const RATIO_UNIT: &[StateUnit] = &[StateUnit::Ratio];
const COUNT_UNIT: &[StateUnit] = &[StateUnit::Count];
const NO_UNITS: &[StateUnit] = &[];

const CORE_STATE_FIELDS: &[StateFieldDefinition] = &[
    StateFieldDefinition {
        id: "trakkin.state:completed",
        shape: StateValueShape::Boolean,
        units: FLAG_UNIT,
        reducers: BOOLEAN_REDUCERS,
        quantizers: EXACT_QUANTIZER,
    },
    StateFieldDefinition {
        id: "trakkin.state:watched",
        shape: StateValueShape::Boolean,
        units: FLAG_UNIT,
        reducers: BOOLEAN_REDUCERS,
        quantizers: EXACT_QUANTIZER,
    },
    StateFieldDefinition {
        id: "trakkin.state:progress",
        shape: StateValueShape::Rational,
        units: PROGRESS_UNITS,
        reducers: PROGRESS_REDUCERS,
        quantizers: RATIONAL_QUANTIZERS,
    },
    StateFieldDefinition {
        id: "trakkin.state:rating",
        shape: StateValueShape::Rational,
        units: RATIO_UNIT,
        reducers: RATING_REDUCERS,
        quantizers: RATIONAL_QUANTIZERS,
    },
    StateFieldDefinition {
        id: "trakkin.state:rewatch-count",
        shape: StateValueShape::Integer,
        units: COUNT_UNIT,
        reducers: REWATCH_REDUCERS,
        quantizers: EXACT_QUANTIZER,
    },
    StateFieldDefinition {
        id: "trakkin.state:status",
        shape: StateValueShape::Term,
        units: NO_UNITS,
        reducers: STATUS_REDUCERS,
        quantizers: EXACT_QUANTIZER,
    },
];

#[derive(Clone, Copy, Debug, Default)]
pub struct StateRegistry;

impl StateRegistry {
    #[must_use]
    pub const fn v1() -> Self {
        Self
    }

    #[must_use]
    pub const fn schema(self) -> &'static str {
        STATE_REGISTRY_SCHEMA
    }

    #[must_use]
    pub const fn fields(self) -> &'static [StateFieldDefinition] {
        CORE_STATE_FIELDS
    }

    #[must_use]
    pub fn field(self, id: &str) -> Option<StateFieldDefinition> {
        CORE_STATE_FIELDS
            .iter()
            .copied()
            .find(|field| field.id == id)
    }

    pub fn validate_rule(
        self,
        field_id: &str,
        reducer: Reducer,
        quantizer: Quantizer,
    ) -> Result<(), StateRegistryError> {
        let field = self
            .field(field_id)
            .ok_or_else(|| StateRegistryError::UnsupportedField(field_id.to_owned()))?;
        if !field.reducers.contains(&reducer) {
            return Err(StateRegistryError::ReducerNotAllowed {
                field: field_id.to_owned(),
                reducer,
            });
        }
        if !field.quantizers.contains(&quantizer) {
            return Err(StateRegistryError::QuantizerNotAllowed {
                field: field_id.to_owned(),
                quantizer,
            });
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum StateRegistryError {
    #[error("state field {0} is not installed in the registry")]
    UnsupportedField(String),
    #[error("reducer {reducer} is not allowed for state field {field}")]
    ReducerNotAllowed { field: String, reducer: Reducer },
    #[error("quantizer {quantizer} is not allowed for state field {field}")]
    QuantizerNotAllowed { field: String, quantizer: Quantizer },
}

impl StateRegistryError {
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::UnsupportedField(_) => "unsupported_field",
            Self::ReducerNotAllowed { .. } => "reducer_not_allowed_for_field",
            Self::QuantizerNotAllowed { .. } => "quantizer_not_allowed_for_field",
        }
    }
}

impl fmt::Display for Reducer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl fmt::Display for Quantizer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ResolutionMode {
    Direct,
    Transitive,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PresenceDisposition {
    Clear,
    Ignore,
    Require,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BlockingPresenceDisposition {
    Block,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ConflictDisposition {
    RequireReducer,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct PresenceRules {
    absent: PresenceDisposition,
    deleted: PresenceDisposition,
    stale: BlockingPresenceDisposition,
    unknown: BlockingPresenceDisposition,
}

impl PresenceRules {
    #[must_use]
    pub const fn absent(&self) -> PresenceDisposition {
        self.absent
    }

    #[must_use]
    pub const fn deleted(&self) -> PresenceDisposition {
        self.deleted
    }

    #[must_use]
    pub const fn stale(&self) -> BlockingPresenceDisposition {
        self.stale
    }

    #[must_use]
    pub const fn unknown(&self) -> BlockingPresenceDisposition {
        self.unknown
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct TranslationRule {
    allow_lossy: bool,
    quantizer: Quantizer,
}

impl TranslationRule {
    #[must_use]
    pub const fn allow_lossy(&self) -> bool {
        self.allow_lossy
    }

    #[must_use]
    pub const fn quantizer(&self) -> Quantizer {
        self.quantizer
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct WriteConstraints {
    allow_clear: bool,
    allow_create_membership: bool,
    require_strong_precondition: bool,
}

impl WriteConstraints {
    #[must_use]
    pub const fn allow_clear(&self) -> bool {
        self.allow_clear
    }

    #[must_use]
    pub const fn allow_create_membership(&self) -> bool {
        self.allow_create_membership
    }

    #[must_use]
    pub const fn require_strong_precondition(&self) -> bool {
        self.require_strong_precondition
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SyncFieldRule {
    conflict: ConflictDisposition,
    presence: PresenceRules,
    reducer: Reducer,
    semantic_field: String,
    translation: TranslationRule,
    write_constraints: WriteConstraints,
}

impl SyncFieldRule {
    #[must_use]
    pub const fn conflict(&self) -> ConflictDisposition {
        self.conflict
    }

    #[must_use]
    pub const fn presence(&self) -> &PresenceRules {
        &self.presence
    }

    #[must_use]
    pub const fn reducer(&self) -> Reducer {
        self.reducer
    }

    #[must_use]
    pub fn semantic_field(&self) -> &str {
        &self.semantic_field
    }

    #[must_use]
    pub const fn translation(&self) -> &TranslationRule {
        &self.translation
    }

    #[must_use]
    pub const fn write_constraints(&self) -> &WriteConstraints {
        &self.write_constraints
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct RawSyncPolicyDocument {
    #[serde(default)]
    catalog_item_id: Option<serde_json::Value>,
    contributors: Vec<Uuid>,
    #[serde(default)]
    provider_item_id: Option<serde_json::Value>,
    receivers: Vec<Uuid>,
    resolution_mode: ResolutionMode,
    rules: Vec<SyncFieldRule>,
    schema: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CanonicalSyncPolicyDocument<'a> {
    contributors: &'a [Uuid],
    receivers: &'a [Uuid],
    resolution_mode: ResolutionMode,
    rules: &'a [SyncFieldRule],
    schema: &'static str,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyncPolicyDocument {
    contributors: Vec<Uuid>,
    receivers: Vec<Uuid>,
    resolution_mode: ResolutionMode,
    rules: Vec<SyncFieldRule>,
}

impl SyncPolicyDocument {
    pub fn from_json(input: &str, registry: StateRegistry) -> Result<Self, SyncPolicyError> {
        let raw: RawSyncPolicyDocument = serde_json::from_str(input).map_err(|error| {
            let message = error.to_string();
            let code = if message.contains("duplicate field") {
                "duplicate_member"
            } else if message.contains("unknown field") {
                "unknown_field"
            } else if message.contains("missing field") {
                "missing_field"
            } else {
                "invalid_policy"
            };
            SyncPolicyError::InvalidDocument { code, message }
        })?;
        Self::from_raw(raw, registry)
    }

    fn from_raw(
        raw: RawSyncPolicyDocument,
        registry: StateRegistry,
    ) -> Result<Self, SyncPolicyError> {
        if raw.provider_item_id.is_some() {
            return Err(SyncPolicyError::ForbiddenRuntimeIdentity {
                field: "providerItemId",
                reason: "provider item identity is runtime evidence",
            });
        }
        if raw.catalog_item_id.is_some() {
            return Err(SyncPolicyError::ForbiddenRuntimeIdentity {
                field: "catalogItemId",
                reason: "catalog item identity is runtime evidence",
            });
        }
        if raw.schema != SYNC_POLICY_SCHEMA {
            return Err(SyncPolicyError::UnsupportedSchema);
        }

        let mut contributors = raw.contributors;
        contributors.sort_unstable();
        contributors.dedup();
        if contributors.is_empty() {
            return Err(SyncPolicyError::ContributorsRequired);
        }

        let mut receivers = raw.receivers;
        receivers.sort_unstable();
        receivers.dedup();
        if receivers.is_empty() {
            return Err(SyncPolicyError::ReceiversRequired);
        }

        let mut rules = raw.rules;
        rules.sort_unstable_by(|left, right| left.semantic_field.cmp(&right.semantic_field));
        if rules.is_empty() {
            return Err(SyncPolicyError::RulesRequired);
        }
        if rules
            .windows(2)
            .any(|rules| rules[0].semantic_field == rules[1].semantic_field)
        {
            return Err(SyncPolicyError::DuplicateFieldRule);
        }
        for rule in &rules {
            registry
                .validate_rule(
                    &rule.semantic_field,
                    rule.reducer,
                    rule.translation.quantizer,
                )
                .map_err(SyncPolicyError::Registry)?;
            let clears = rule.presence.absent == PresenceDisposition::Clear
                || rule.presence.deleted == PresenceDisposition::Clear;
            if clears && !rule.write_constraints.allow_clear {
                return Err(SyncPolicyError::ClearNotAllowed {
                    field: rule.semantic_field.clone(),
                });
            }
        }

        Ok(Self {
            contributors,
            receivers,
            resolution_mode: raw.resolution_mode,
            rules,
        })
    }

    #[must_use]
    pub const fn schema(&self) -> &'static str {
        SYNC_POLICY_SCHEMA
    }

    #[must_use]
    pub fn contributors(&self) -> &[Uuid] {
        &self.contributors
    }

    #[must_use]
    pub fn receivers(&self) -> &[Uuid] {
        &self.receivers
    }

    #[must_use]
    pub const fn resolution_mode(&self) -> ResolutionMode {
        self.resolution_mode
    }

    #[must_use]
    pub fn rules(&self) -> &[SyncFieldRule] {
        &self.rules
    }

    #[must_use]
    pub fn contributes(&self, source_id: Uuid) -> bool {
        self.contributors.binary_search(&source_id).is_ok()
    }

    #[must_use]
    pub fn receives(&self, source_id: Uuid) -> bool {
        self.receivers.binary_search(&source_id).is_ok()
    }

    #[must_use]
    pub fn canonical_json(&self) -> Vec<u8> {
        serde_json::to_vec(&CanonicalSyncPolicyDocument {
            contributors: &self.contributors,
            receivers: &self.receivers,
            resolution_mode: self.resolution_mode,
            rules: &self.rules,
            schema: SYNC_POLICY_SCHEMA,
        })
        .expect("typed synchronization policy serializes")
    }

    #[must_use]
    pub fn content_hash(&self) -> String {
        format!(
            "sha256:{}",
            hex::encode(Sha256::digest(self.canonical_json()))
        )
    }
}

pub fn validate_sync_policy_hash(hash: &str) -> Result<(), SyncPolicyError> {
    let Some(digest) = hash.strip_prefix("sha256:") else {
        return Err(SyncPolicyError::InvalidContentHash);
    };
    if digest.len() != 64
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(SyncPolicyError::InvalidContentHash);
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum SyncPolicyError {
    #[error("synchronization policy schema is unsupported")]
    UnsupportedSchema,
    #[error("synchronization policy requires at least one contributor source")]
    ContributorsRequired,
    #[error("synchronization policy requires at least one receiver source")]
    ReceiversRequired,
    #[error("synchronization policy requires at least one field rule")]
    RulesRequired,
    #[error("synchronization policy contains a duplicate semantic field rule")]
    DuplicateFieldRule,
    #[error("state field {field} can clear evidence but writes do not allow clear")]
    ClearNotAllowed { field: String },
    #[error("{field} is forbidden: {reason}")]
    ForbiddenRuntimeIdentity {
        field: &'static str,
        reason: &'static str,
    },
    #[error(transparent)]
    Registry(StateRegistryError),
    #[error("synchronization policy content hash is invalid")]
    InvalidContentHash,
    #[error("invalid synchronization policy: {message}")]
    InvalidDocument { code: &'static str, message: String },
}

impl SyncPolicyError {
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::UnsupportedSchema => "unsupported_schema",
            Self::ContributorsRequired => "contributors_required",
            Self::ReceiversRequired => "receivers_required",
            Self::RulesRequired => "rules_required",
            Self::DuplicateFieldRule => "duplicate_field_rule",
            Self::ClearNotAllowed { .. } => "clear_not_allowed",
            Self::ForbiddenRuntimeIdentity { .. } => "forbidden_runtime_identity",
            Self::Registry(error) => error.code(),
            Self::InvalidContentHash => "invalid_content_hash",
            Self::InvalidDocument { code, .. } => code,
        }
    }
}
