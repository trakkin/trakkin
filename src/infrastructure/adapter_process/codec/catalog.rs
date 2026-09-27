use trakkin_provider_sdk::v1::{
    ReadCatalogRequest, ReadCatalogResponse as ProtoCatalogReadEvent, read_catalog_response,
};

use crate::{
    application::adapter_runtime::{AdapterReadEvent, ReadAdapterSource},
    domain::acquisition::{
        BinaryAssetReference, CatalogBatch, CatalogItem, CatalogRelation, ProviderItem,
    },
};

use super::{
    super::AdapterProcessError,
    common::{
        map_attributes, map_binary_asset_key, map_catalog_relation_key, map_domain_term,
        map_provider_item_key, map_read_completion, map_read_failure, map_read_request,
    },
};

pub(in super::super) fn map_read_catalog_request(
    command: ReadAdapterSource,
) -> Result<ReadCatalogRequest, AdapterProcessError> {
    let request = map_read_request(command)?;
    Ok(ReadCatalogRequest {
        operation_id: request.operation_id,
        source_key: Some(request.source_key),
        mode: request.mode as i32,
        prior_cursor: request.prior_cursor,
        preferred_batch_size: request.preferred_batch_size,
    })
}

pub(in super::super) fn map_catalog_event(
    event: ProtoCatalogReadEvent,
    operation_id: &[u8],
) -> Result<AdapterReadEvent<CatalogBatch>, AdapterProcessError> {
    match event.event.ok_or(AdapterProcessError::InvalidResponse(
        "catalog event is missing",
    ))? {
        read_catalog_response::Event::Heartbeat(heartbeat) => {
            if heartbeat.operation_id != operation_id {
                return Err(AdapterProcessError::InvalidResponse(
                    "catalog heartbeat operation ID does not match",
                ));
            }
            Ok(AdapterReadEvent::Heartbeat {
                records_emitted: heartbeat.records_emitted,
            })
        }
        read_catalog_response::Event::Batch(batch) => {
            Ok(AdapterReadEvent::Batch(map_catalog_batch(batch)?))
        }
        read_catalog_response::Event::Completed(completed) => {
            Ok(AdapterReadEvent::Completed(map_read_completion(completed)?))
        }
        read_catalog_response::Event::Failed(failed) => {
            Ok(AdapterReadEvent::Failed(map_read_failure(failed.error)?))
        }
        read_catalog_response::Event::Cancelled(_) => Ok(AdapterReadEvent::Cancelled),
    }
}

fn map_catalog_batch(
    batch: trakkin_provider_sdk::v1::CatalogBatch,
) -> Result<CatalogBatch, AdapterProcessError> {
    Ok(CatalogBatch::new(
        batch.sequence,
        batch
            .item_upserts
            .into_iter()
            .map(map_catalog_item)
            .collect::<Result<_, _>>()?,
        batch
            .item_deletes
            .into_iter()
            .map(|key| map_provider_item_key(Some(key)))
            .collect::<Result<_, _>>()?,
        batch
            .relation_upserts
            .into_iter()
            .map(map_catalog_relation)
            .collect::<Result<_, _>>()?,
        batch
            .relation_deletes
            .into_iter()
            .map(|key| map_catalog_relation_key(Some(key)))
            .collect::<Result<_, _>>()?,
    ))
}

fn map_catalog_item(
    item: trakkin_provider_sdk::v1::ProviderItem,
) -> Result<CatalogItem, AdapterProcessError> {
    map_provider_item(item).map(CatalogItem::from)
}

pub(super) fn map_provider_item(
    item: trakkin_provider_sdk::v1::ProviderItem,
) -> Result<ProviderItem, AdapterProcessError> {
    ProviderItem::new(
        map_provider_item_key(item.key)?,
        map_domain_term(item.kind)?,
        item.display_name,
        map_attributes(item.attributes)?,
        item.assets
            .into_iter()
            .map(|asset| {
                Ok(BinaryAssetReference::new(
                    map_binary_asset_key(asset.key)?,
                    map_domain_term(asset.kind)?,
                ))
            })
            .collect::<Result<_, AdapterProcessError>>()?,
    )
    .map_err(|_| AdapterProcessError::InvalidResponse("invalid provider item"))
}

fn map_catalog_relation(
    relation: trakkin_provider_sdk::v1::CatalogRelation,
) -> Result<CatalogRelation, AdapterProcessError> {
    CatalogRelation::new(
        map_catalog_relation_key(relation.key)?,
        relation
            .parent_key
            .map(|key| map_catalog_relation_key(Some(key)))
            .transpose()?,
        relation
            .provider_item_key
            .map(|key| map_provider_item_key(Some(key)))
            .transpose()?,
        map_domain_term(relation.kind)?,
        relation.order,
        map_attributes(relation.attributes)?,
    )
    .map_err(|_| AdapterProcessError::InvalidResponse("invalid catalog relation"))
}
