use trakkin_provider_sdk::v1::{
    PortableEndpoint as ProtoPortableEndpoint, portable_endpoint_resolution,
    portable_reference_lookup_result,
};

use crate::{
    application::adapter_runtime::{
        AdapterEndpointLookupCandidate, AdapterEndpointLookupOutcome, AdapterLookupCandidate,
        AdapterLookupEvidence, AdapterLookupOutcome, AdapterPortableEndpointResolution,
        AdapterPortableReferenceLookup,
    },
    domain::acquisition::PortableReference,
};

use super::{
    super::AdapterProcessError,
    catalog::{map_coordinate_binding, map_provider_item},
    common::map_milliseconds,
};

pub(in super::super) fn map_lookup_result(
    result: trakkin_provider_sdk::v1::PortableReferenceLookupResult,
) -> Result<AdapterPortableReferenceLookup, AdapterProcessError> {
    let requested = map_domain_portable_reference(result.requested)?;
    let outcome = match result.outcome.ok_or(AdapterProcessError::InvalidResponse(
        "lookup outcome is missing",
    ))? {
        portable_reference_lookup_result::Outcome::Matched(matched) => {
            AdapterLookupOutcome::Matched(Box::new(map_lookup_candidate(matched.candidate)?))
        }
        portable_reference_lookup_result::Outcome::NotFound(_) => AdapterLookupOutcome::NotFound,
        portable_reference_lookup_result::Outcome::Ambiguous(ambiguous) => {
            AdapterLookupOutcome::Ambiguous(
                ambiguous
                    .candidates
                    .into_iter()
                    .map(|candidate| map_lookup_candidate(Some(candidate)))
                    .collect::<Result<_, _>>()?,
            )
        }
        portable_reference_lookup_result::Outcome::Unsupported(_) => {
            AdapterLookupOutcome::Unsupported
        }
    };
    Ok(AdapterPortableReferenceLookup { requested, outcome })
}

fn map_lookup_candidate(
    candidate: Option<trakkin_provider_sdk::v1::LookupCandidate>,
) -> Result<AdapterLookupCandidate, AdapterProcessError> {
    let candidate = candidate.ok_or(AdapterProcessError::InvalidResponse(
        "lookup candidate is missing",
    ))?;
    Ok(AdapterLookupCandidate {
        provider_item: map_provider_item(candidate.provider_item.ok_or(
            AdapterProcessError::InvalidResponse("lookup provider item is missing"),
        )?)?,
        evidence: map_lookup_evidence(candidate.evidence)?,
    })
}

pub(in super::super) fn map_endpoint_result(
    result: trakkin_provider_sdk::v1::PortableEndpointResolution,
) -> Result<AdapterPortableEndpointResolution, AdapterProcessError> {
    let requested = map_domain_portable_endpoint(result.requested)?;
    let outcome = match result.outcome.ok_or(AdapterProcessError::InvalidResponse(
        "endpoint resolution outcome is missing",
    ))? {
        portable_endpoint_resolution::Outcome::Matched(matched) => {
            AdapterEndpointLookupOutcome::Matched(Box::new(map_endpoint_candidate(
                matched.candidate,
            )?))
        }
        portable_endpoint_resolution::Outcome::NotFound(_) => {
            AdapterEndpointLookupOutcome::NotFound
        }
        portable_endpoint_resolution::Outcome::Ambiguous(ambiguous) => {
            AdapterEndpointLookupOutcome::Ambiguous(
                ambiguous
                    .candidates
                    .into_iter()
                    .map(|candidate| map_endpoint_candidate(Some(candidate)))
                    .collect::<Result<_, _>>()?,
            )
        }
        portable_endpoint_resolution::Outcome::Unsupported(_) => {
            AdapterEndpointLookupOutcome::Unsupported
        }
    };
    Ok(AdapterPortableEndpointResolution { requested, outcome })
}

fn map_endpoint_candidate(
    candidate: Option<trakkin_provider_sdk::v1::EndpointLookupCandidate>,
) -> Result<AdapterEndpointLookupCandidate, AdapterProcessError> {
    let candidate = candidate.ok_or(AdapterProcessError::InvalidResponse(
        "endpoint lookup candidate is missing",
    ))?;
    Ok(AdapterEndpointLookupCandidate {
        provider_item: map_provider_item(candidate.provider_item.ok_or(
            AdapterProcessError::InvalidResponse("endpoint lookup provider item is missing"),
        )?)?,
        binding: Some(map_coordinate_binding(candidate.binding.ok_or(
            AdapterProcessError::InvalidResponse("endpoint lookup binding is missing"),
        )?)?),
        evidence: map_lookup_evidence(candidate.evidence)?,
    })
}

fn map_lookup_evidence(
    evidence: Option<trakkin_provider_sdk::v1::LookupEvidence>,
) -> Result<AdapterLookupEvidence, AdapterProcessError> {
    let evidence = evidence.ok_or(AdapterProcessError::InvalidResponse(
        "lookup evidence is missing",
    ))?;
    Ok(AdapterLookupEvidence {
        adapter_revision: evidence.adapter_revision,
        observed_at: map_milliseconds(evidence.observed_time_milliseconds, "lookup time")?,
        expires_at: evidence
            .expires_time_milliseconds
            .map(|value| map_milliseconds(value, "lookup expiry"))
            .transpose()?,
        matched_references: evidence
            .matched_references
            .into_iter()
            .map(|reference| map_domain_portable_reference(Some(reference)))
            .collect::<Result<_, _>>()?,
    })
}

pub(in super::super) fn map_portable_reference(
    reference: &PortableReference,
) -> trakkin_provider_sdk::v1::PortableReference {
    trakkin_provider_sdk::v1::PortableReference {
        namespace: reference.namespace().to_owned(),
        value: reference.value().to_vec(),
    }
}

pub(in super::super) fn map_portable_endpoint(
    endpoint: &trakkin_mapping::PortableEndpoint,
) -> ProtoPortableEndpoint {
    ProtoPortableEndpoint {
        reference: Some(trakkin_provider_sdk::v1::PortableReference {
            namespace: endpoint.namespace().to_owned(),
            value: endpoint.value().to_vec(),
        }),
        selector: endpoint
            .selector()
            .map(|selector| selector.as_str().to_owned())
            .unwrap_or_default(),
    }
}

fn map_domain_portable_reference(
    reference: Option<trakkin_provider_sdk::v1::PortableReference>,
) -> Result<PortableReference, AdapterProcessError> {
    let reference = reference.ok_or(AdapterProcessError::InvalidResponse(
        "portable reference is missing",
    ))?;
    PortableReference::new(reference.namespace, reference.value)
        .map_err(|_| AdapterProcessError::InvalidResponse("invalid portable reference"))
}

fn map_domain_portable_endpoint(
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
