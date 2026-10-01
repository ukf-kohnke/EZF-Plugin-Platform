#![allow(non_snake_case)]
use serde_json::Value;
use serde::{ Deserialize, Serialize };

#[derive(Default, Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FilterRoot {
    pub tablefilterquery: Tablefilterquery,
}

#[derive(Default, Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Tablefilterquery {
    #[serde(rename = "type")]
    pub type_field: String,
    pub properties: Properties,
    pub required: Vec<Value>,
}

#[derive(Default, Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Properties {
    pub sorting: Option<Sorting>,
    pub filtering: Option<Filtering>,
    pub startrow: Option<Startrow>,
    pub endrow: Option<Endrow>,
}

#[derive(Default, Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Sorting {
    #[serde(rename = "type")]
    pub type_field: String,
    pub column: String,
    pub order: String,
}

#[derive(Default, Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Filtering {
    #[serde(rename = "type")]
    pub type_field: String,
    pub description: String,
    pub conjunctions: Option<Vec<String>>,
    pub columns: Vec<Column>,
}

#[derive(Default, Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Column {
    #[serde(rename = "type")]
    pub type_field: String,
    pub table: String,
    pub pivot: String,
    pub expression: String,
}

#[derive(Default, Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Startrow {
    #[serde(rename = "type")]
    pub type_field: String,
    pub description: String,
    pub minimum: i64,
}

#[derive(Default, Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Endrow {
    #[serde(rename = "type")]
    pub type_field: String,
    pub description: String,
    pub maximum: i64,
}
