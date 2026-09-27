use serde_json::Value;
use sqlx::{FromRow, PgPool};
use thiserror::Error;
use uuid::Uuid;

use crate::{
    application::{
        adapter_runtime::AdapterKey,
        assets::{AssetFuture, BinaryAssetRepository, BinaryAssetTarget, ResolveBinaryAsset},
        error::{ApplicationError, ApplicationErrorCode, IntoApplicationError},
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
            Self::AssetCapabilityUnavailable => ApplicationError::new(
                ApplicationErrorCode::ProviderCapabilityUnavailable,
                "provider capability is unavailable",
            ),
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
