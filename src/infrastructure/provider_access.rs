use serde_json::Value;
use sqlx::{FromRow, PgPool};
use thiserror::Error;
use uuid::Uuid;

use crate::{
    application::{
        adapter_runtime::AdapterKey,
        assets::{AssetFuture, BinaryAssetRepository, BinaryAssetTarget, ResolveBinaryAsset},
        error::{ApplicationError, ApplicationErrorCode, IntoApplicationError},
        mapping_resolution::{
            MappingEndpointLookupRepository, MappingEndpointLookupTarget, MappingResolutionFuture,
        },
        reference_lookup::{
            ReferenceLookupFuture, ReferenceLookupRepository, ReferenceLookupTarget,
        },
    },
    domain::acquisition::{BinaryAssetKey, ProviderItemKey},
};

#[derive(Clone)]
pub struct PostgresProviderAccessRepository {
    pool: PgPool,
}

impl PostgresProviderAccessRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    async fn resolve_asset(
        &self,
        command: ResolveBinaryAsset,
    ) -> Result<BinaryAssetTarget, ProviderAccessError> {
        let row = sqlx::query_as::<_, AssetTargetRow>(
            "SELECT
    source.connection_id,
    source.provider_key_namespace AS source_key_namespace,
    source.provider_key_value AS source_key_value,
    source.capabilities,
    stream.status AS stream_status,
    stream.publication_id,
    EXISTS(
        SELECT 1
        FROM catalog_assets AS asset
        WHERE
            asset.source_id = source.id
            AND asset.item_key_namespace = $3
            AND asset.item_key_value = $4
            AND asset.asset_key_namespace = $5
            AND asset.asset_key_value = $6
            AND asset.publication_id = $7
    ) AS asset_exists
FROM workspace_sources AS attachment
INNER JOIN sources AS source ON source.id = attachment.source_id
INNER JOIN source_streams AS stream
    ON
        stream.source_id = source.id
        AND stream.stream_kind = 'catalog'
WHERE
    attachment.workspace_id = $1
    AND attachment.source_id = $2
    AND source.availability <> 'removed'
",
        )
        .bind(command.workspace_id)
        .bind(command.source_id)
        .bind(command.provider_item_key.namespace())
        .bind(command.provider_item_key.value())
        .bind(command.asset_key.namespace())
        .bind(command.asset_key.value())
        .bind(command.publication_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or(ProviderAccessError::SourceNotAttached)?;
        if row.stream_status != "available" {
            return Err(ProviderAccessError::CatalogUnavailable);
        }
        if row.publication_id != Some(command.publication_id) {
            return Err(ProviderAccessError::PublicationExpired);
        }
        if !row.asset_exists {
            return Err(ProviderAccessError::AssetNotFound);
        }
        let (maximum_bytes, content_types) = parse_asset_capability(&row.capabilities)?;
        Ok(BinaryAssetTarget {
            connection_id: row.connection_id,
            source_key: AdapterKey {
                namespace: row.source_key_namespace,
                value: row.source_key_value,
            },
            provider_item_key: adapter_key(command.provider_item_key),
            asset_key: asset_key(command.asset_key),
            maximum_bytes,
            content_types,
        })
    }

    async fn resolve_lookup(
        &self,
        workspace_id: Uuid,
        source_id: Uuid,
    ) -> Result<ReferenceLookupTarget, ProviderAccessError> {
        let row = sqlx::query_as::<_, LookupTargetRow>(
            "SELECT source.connection_id, connection.capabilities
FROM workspace_sources AS attachment
INNER JOIN sources AS source ON source.id = attachment.source_id
INNER JOIN connections AS connection ON connection.id = source.connection_id
WHERE
    attachment.workspace_id = $1
    AND attachment.source_id = $2
    AND source.availability <> 'removed'
    AND connection.deleted_at IS NULL
    AND connection.status = 'ready'
",
        )
        .bind(workspace_id)
        .bind(source_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or(ProviderAccessError::SourceNotAttached)?;
        let (reference_namespaces, maximum_batch_size) =
            parse_lookup_capability(&row.capabilities)?;
        Ok(ReferenceLookupTarget {
            connection_id: row.connection_id,
            reference_namespaces,
            maximum_batch_size,
        })
    }

    async fn resolve_mapping_endpoint_lookups(
        &self,
        workspace_id: Uuid,
    ) -> Result<Vec<MappingEndpointLookupTarget>, ProviderAccessError> {
        let rows = sqlx::query_as::<_, EndpointLookupTargetRow>(
            "SELECT DISTINCT connection.id AS connection_id, connection.capabilities
FROM workspace_sources AS attachment
INNER JOIN sources AS source ON source.id = attachment.source_id
INNER JOIN connections AS connection ON connection.id = source.connection_id
WHERE
    attachment.workspace_id = $1
    AND source.availability <> 'removed'
    AND connection.deleted_at IS NULL
    AND connection.status = 'ready'
    AND (
        jsonb_typeof(connection.capabilities -> 'referenceLookup') = 'object'
        OR jsonb_typeof(connection.capabilities -> 'endpointLookup') = 'object'
    )
ORDER BY connection.id
",
        )
        .bind(workspace_id)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                let bare = row
                    .capabilities
                    .get("referenceLookup")
                    .filter(|capability| !capability.is_null())
                    .map(|_| parse_lookup_capability(&row.capabilities))
                    .transpose()?;
                let selected = row
                    .capabilities
                    .get("endpointLookup")
                    .filter(|capability| !capability.is_null())
                    .map(|_| parse_endpoint_lookup_capability(&row.capabilities))
                    .transpose()?;
                let (bare_reference_namespaces, maximum_bare_batch_size) = bare.unwrap_or_default();
                let (
                    reference_namespaces,
                    coordinate_ids,
                    maximum_batch_size,
                    maximum_response_bytes,
                ) = selected.unwrap_or_else(|| (Vec::new(), Vec::new(), 0, 1));
                Ok(MappingEndpointLookupTarget {
                    connection_id: row.connection_id,
                    bare_reference_namespaces,
                    maximum_bare_batch_size,
                    reference_namespaces,
                    coordinate_ids,
                    maximum_batch_size,
                    maximum_response_bytes,
                })
            })
            .collect()
    }

    async fn confirm_asset_publication(
        &self,
        workspace_id: Uuid,
        source_id: Uuid,
        publication_id: Uuid,
    ) -> Result<(), ProviderAccessError> {
        let current = sqlx::query_as::<_, (String, Option<Uuid>)>(
            "SELECT stream.status, stream.publication_id
FROM workspace_sources AS attachment
INNER JOIN source_streams AS stream
    ON
        stream.source_id = attachment.source_id
        AND stream.stream_kind = 'catalog'
WHERE
    attachment.workspace_id = $1
    AND attachment.source_id = $2
",
        )
        .bind(workspace_id)
        .bind(source_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or(ProviderAccessError::SourceNotAttached)?;
        if current != ("available".to_owned(), Some(publication_id)) {
            return Err(ProviderAccessError::PublicationExpired);
        }
        Ok(())
    }
}

impl BinaryAssetRepository for PostgresProviderAccessRepository {
    type Error = ProviderAccessError;

    fn resolve_binary_asset(
        &self,
        command: ResolveBinaryAsset,
    ) -> AssetFuture<'_, BinaryAssetTarget, Self::Error> {
        Box::pin(self.resolve_asset(command))
    }

    fn confirm_binary_asset_publication(
        &self,
        workspace_id: Uuid,
        source_id: Uuid,
        publication_id: Uuid,
    ) -> AssetFuture<'_, (), Self::Error> {
        Box::pin(self.confirm_asset_publication(workspace_id, source_id, publication_id))
    }
}

impl ReferenceLookupRepository for PostgresProviderAccessRepository {
    type Error = ProviderAccessError;

    fn resolve_reference_lookup(
        &self,
        workspace_id: Uuid,
        source_id: Uuid,
    ) -> ReferenceLookupFuture<'_, ReferenceLookupTarget, Self::Error> {
        Box::pin(self.resolve_lookup(workspace_id, source_id))
    }
}

impl MappingEndpointLookupRepository for PostgresProviderAccessRepository {
    type Error = ProviderAccessError;

    fn resolve_mapping_endpoint_lookups(
        &self,
        workspace_id: Uuid,
    ) -> MappingResolutionFuture<'_, Vec<MappingEndpointLookupTarget>, Self::Error> {
        Box::pin(self.resolve_mapping_endpoint_lookups(workspace_id))
    }
}

fn parse_asset_capability(capabilities: &Value) -> Result<(u64, Vec<String>), ProviderAccessError> {
    let capability = capabilities
        .get("assets")
        .filter(|capability| !capability.is_null())
        .ok_or(ProviderAccessError::AssetCapabilityUnavailable)?;
    let maximum_bytes = capability
        .get("maximumBytes")
        .and_then(Value::as_u64)
        .filter(|maximum| *maximum > 0)
        .ok_or(ProviderAccessError::InvalidCapabilities)?;
    let content_types = string_array(capability.get("contentTypes"))?;
    Ok((maximum_bytes, content_types))
}

fn parse_lookup_capability(
    capabilities: &Value,
) -> Result<(Vec<String>, u32), ProviderAccessError> {
    let capability = capabilities
        .get("referenceLookup")
        .filter(|capability| !capability.is_null())
        .ok_or(ProviderAccessError::LookupCapabilityUnavailable)?;
    let reference_namespaces = string_array(capability.get("referenceNamespaces"))?;
    let maximum_batch_size = capability
        .get("maximumBatchSize")
        .and_then(Value::as_u64)
        .and_then(|maximum| u32::try_from(maximum).ok())
        .filter(|maximum| *maximum > 0)
        .ok_or(ProviderAccessError::InvalidCapabilities)?;
    Ok((reference_namespaces, maximum_batch_size))
}

fn parse_endpoint_lookup_capability(
    capabilities: &Value,
) -> Result<(Vec<String>, Vec<String>, u32, u64), ProviderAccessError> {
    let capability = capabilities
        .get("endpointLookup")
        .filter(|capability| !capability.is_null())
        .ok_or(ProviderAccessError::LookupCapabilityUnavailable)?;
    let reference_namespaces = string_array(capability.get("referenceNamespaces"))?;
    let coordinate_ids = string_array(capability.get("coordinateIds"))?;
    let maximum_batch_size = capability
        .get("maximumBatchSize")
        .and_then(Value::as_u64)
        .and_then(|maximum| u32::try_from(maximum).ok())
        .filter(|maximum| *maximum > 0)
        .ok_or(ProviderAccessError::InvalidCapabilities)?;
    let maximum_response_bytes = capability
        .get("maximumResponseBytes")
        .and_then(Value::as_u64)
        .filter(|maximum| *maximum > 0)
        .ok_or(ProviderAccessError::InvalidCapabilities)?;
    Ok((
        reference_namespaces,
        coordinate_ids,
        maximum_batch_size,
        maximum_response_bytes,
    ))
}

fn string_array(value: Option<&Value>) -> Result<Vec<String>, ProviderAccessError> {
    let values = value
        .and_then(Value::as_array)
        .ok_or(ProviderAccessError::InvalidCapabilities)?;
    let strings = values
        .iter()
        .map(|value| {
            value
                .as_str()
                .filter(|value| !value.is_empty())
                .map(str::to_owned)
                .ok_or(ProviderAccessError::InvalidCapabilities)
        })
        .collect::<Result<Vec<_>, _>>()?;
    if strings.is_empty() {
        return Err(ProviderAccessError::InvalidCapabilities);
    }
    Ok(strings)
}

fn adapter_key(key: ProviderItemKey) -> AdapterKey {
    AdapterKey {
        namespace: key.namespace().to_owned(),
        value: key.value().to_vec(),
    }
}

fn asset_key(key: BinaryAssetKey) -> AdapterKey {
    AdapterKey {
        namespace: key.namespace().to_owned(),
        value: key.value().to_vec(),
    }
}

#[derive(FromRow)]
struct AssetTargetRow {
    connection_id: Uuid,
    source_key_namespace: String,
    source_key_value: Vec<u8>,
    capabilities: Value,
    stream_status: String,
    publication_id: Option<Uuid>,
    asset_exists: bool,
}

#[derive(FromRow)]
struct LookupTargetRow {
    connection_id: Uuid,
    capabilities: Value,
}

#[derive(FromRow)]
struct EndpointLookupTargetRow {
    connection_id: Uuid,
    capabilities: Value,
}

#[derive(Debug, Error)]
pub enum ProviderAccessError {
    #[error("source is not attached to the workspace")]
    SourceNotAttached,
    #[error("catalog has no available publication")]
    CatalogUnavailable,
    #[error("catalog publication changed")]
    PublicationExpired,
    #[error("binary asset was not found in the published catalog")]
    AssetNotFound,
    #[error("source does not advertise binary asset reads")]
    AssetCapabilityUnavailable,
    #[error("connection does not advertise portable-reference lookup")]
    LookupCapabilityUnavailable,
    #[error("stored provider capabilities are invalid")]
    InvalidCapabilities,
    #[error("provider access database operation failed: {0}")]
    Database(#[from] sqlx::Error),
}

impl IntoApplicationError for ProviderAccessError {
    fn into_application_error(self) -> ApplicationError {
        match self {
            Self::SourceNotAttached => ApplicationError::new(
                ApplicationErrorCode::SourceNotAttached,
                "source is not attached to the workspace",
            ),
            Self::CatalogUnavailable => ApplicationError::new(
                ApplicationErrorCode::StreamUnavailable,
                "catalog has no available publication",
            ),
            Self::PublicationExpired => ApplicationError::new(
                ApplicationErrorCode::CatalogPublicationExpired,
                "catalog publication changed",
            ),
            Self::AssetNotFound => ApplicationError::new(
                ApplicationErrorCode::BinaryAssetNotFound,
                "binary asset was not found",
            ),
            Self::AssetCapabilityUnavailable | Self::LookupCapabilityUnavailable => {
                ApplicationError::new(
                    ApplicationErrorCode::ProviderCapabilityUnavailable,
                    "provider capability is unavailable",
                )
            }
            Self::InvalidCapabilities => ApplicationError::new(
                ApplicationErrorCode::InvalidProviderData,
                "stored provider capabilities are invalid",
            ),
            Self::Database(_) => ApplicationError::new(
                ApplicationErrorCode::Internal,
                "provider access operation failed",
            ),
        }
    }
}
