use trakkin_provider_sdk::v1::{
    CoordinateBacking as ProtoCoordinateBacking, PortableEndpoint as ProtoPortableEndpoint,
    ReadCatalogRequest, ReadCatalogResponse as ProtoCatalogReadEvent, read_catalog_response,
    subject_reference,
};

use crate::{
    application::adapter_runtime::{AdapterReadEvent, ReadAdapterSource},
    domain::acquisition::{
        BinaryAssetReference, CatalogBatch, CatalogItem, CatalogRelation, CoordinateBacking,
        CoordinateBinding, CoordinateBindingKey, CoordinateSubject, PortableReference,
        ProviderItem,
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
    let coordinate_binding_upserts = batch
        .coordinate_binding_upserts
        .into_iter()
        .map(map_coordinate_binding)
        .collect::<Result<_, _>>()?;
    let coordinate_binding_deletes = batch
        .coordinate_binding_deletes
        .into_iter()
        .map(map_coordinate_binding_key)
        .collect::<Result<_, _>>()?;
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
    )
    .with_coordinate_bindings(coordinate_binding_upserts, coordinate_binding_deletes))
}

pub(super) fn map_coordinate_binding(
    binding: trakkin_provider_sdk::v1::CoordinateBinding,
) -> Result<CoordinateBinding, AdapterProcessError> {
    let backing = match ProtoCoordinateBacking::try_from(binding.backing)
        .map_err(|_| AdapterProcessError::InvalidResponse("invalid coordinate backing"))?
    {
        ProtoCoordinateBacking::Materialized => CoordinateBacking::Materialized,
        ProtoCoordinateBacking::Virtual => CoordinateBacking::Virtual,
        ProtoCoordinateBacking::Aggregate => CoordinateBacking::Aggregate,
        ProtoCoordinateBacking::Unspecified => {
            return Err(AdapterProcessError::InvalidResponse(
                "coordinate backing is unspecified",
            ));
        }
    };
    CoordinateBinding::new(
        map_coordinate_binding_parts(binding.endpoint, binding.subject)?,
        backing,
        binding.evidence_revision,
    )
    .map_err(|_| AdapterProcessError::InvalidResponse("invalid coordinate binding"))
}

fn map_coordinate_binding_key(
    binding: trakkin_provider_sdk::v1::CoordinateBindingKey,
) -> Result<CoordinateBindingKey, AdapterProcessError> {
    map_coordinate_binding_parts(binding.endpoint, binding.subject)
}

fn map_coordinate_binding_parts(
    endpoint: Option<ProtoPortableEndpoint>,
    subject: Option<trakkin_provider_sdk::v1::SubjectReference>,
) -> Result<CoordinateBindingKey, AdapterProcessError> {
    let endpoint = map_coordinate_endpoint(endpoint)?;
    let subject = match subject.and_then(|subject| subject.subject).ok_or(
        AdapterProcessError::InvalidResponse("coordinate subject is missing"),
    )? {
        subject_reference::Subject::ProviderItemKey(key) => {
            CoordinateSubject::ProviderItem(map_provider_item_key(Some(key))?)
        }
        subject_reference::Subject::CatalogRelationKey(key) => {
            CoordinateSubject::CatalogRelation(map_catalog_relation_key(Some(key))?)
        }
    };
    Ok(CoordinateBindingKey::new(endpoint, subject))
}

fn map_coordinate_endpoint(
    endpoint: Option<ProtoPortableEndpoint>,
) -> Result<trakkin_mapping::PortableEndpoint, AdapterProcessError> {
    let endpoint = endpoint.ok_or(AdapterProcessError::InvalidResponse(
        "portable endpoint is missing",
    ))?;
    let reference = endpoint
        .reference
        .ok_or(AdapterProcessError::InvalidResponse(
            "portable endpoint reference is missing",
        ))?;
    trakkin_mapping::PortableEndpoint::from_parts(
        &reference.namespace,
        &reference.value,
        Some(&endpoint.selector),
    )
    .map_err(|_| AdapterProcessError::InvalidResponse("invalid portable endpoint"))
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
        item.portable_reference_candidates
            .into_iter()
            .map(|reference| {
                PortableReference::new(reference.namespace, reference.value).map_err(|_| {
                    AdapterProcessError::InvalidResponse("invalid portable reference candidate")
                })
            })
            .collect::<Result<_, _>>()?,
        item.recommended_mapping_roots
            .into_iter()
            .map(|reference| {
                PortableReference::new(reference.namespace, reference.value).map_err(|_| {
                    AdapterProcessError::InvalidResponse("invalid recommended mapping root")
                })
            })
            .collect::<Result<_, _>>()?,
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
