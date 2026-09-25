use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::{IntoParams, ToSchema};

#[derive(Clone, Debug, Default, Deserialize, IntoParams)]
#[serde(rename_all = "camelCase")]
#[into_params(parameter_in = Query)]
pub struct LogQueryParameters {
    pub before: Option<String>,
    pub after: Option<String>,
    #[param(minimum = 1, maximum = 1000)]
    pub limit: Option<usize>,
}

#[derive(Clone, Debug, Default, Deserialize, IntoParams)]
#[serde(rename_all = "camelCase")]
#[into_params(parameter_in = Query)]
pub struct LogStreamQueryParameters {
    pub after: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct LogEntryResponse {
    #[serde(default)]
    #[schema(required = true)]
    pub cursor: String,
    pub timestamp: String,
    pub level: String,
    pub target: String,
    pub fields: BTreeMap<String, Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub span: Option<BTreeMap<String, Value>>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub spans: Vec<BTreeMap<String, Value>>,
}

#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct LogPageResponse {
    pub entries: Vec<LogEntryResponse>,
    pub has_more_before: bool,
    pub has_more_after: bool,
}
