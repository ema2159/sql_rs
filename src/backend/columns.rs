#![allow(dead_code)]
use super::row::SQLType;
use std::collections::BTreeMap;
use std::ops::{Deref, DerefMut};

use bincode;
use serde::{Deserialize, Serialize};
use tracing::instrument;

pub trait ColumnType {
    fn validate(&self, input: &str) -> Option<SQLType>;
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum IntegerType {
    Int,
    UBigInt,
}

impl ColumnType for IntegerType {
    #[instrument(parent = None, ret, level = "trace")]
    fn validate(&self, input: &str) -> Option<SQLType> {
        match self {
            IntegerType::Int => Some(SQLType::Integer(input.parse::<i32>().ok()?)),
            IntegerType::UBigInt => Some(SQLType::UBigInt(input.parse::<u64>().ok()?)),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum TextType {
    Varchar(u8),
}

impl ColumnType for TextType {
    #[instrument(parent = None, ret, level = "trace")]
    fn validate(&self, input: &str) -> Option<SQLType> {
        match self {
            TextType::Varchar(max_size) => {
                if input.len() <= *max_size as usize {
                    Some(SQLType::Text(input.to_owned()))
                } else {
                    None
                }
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum ColumnItemType {
    Integer(IntegerType),
    Text(TextType),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Columns(pub BTreeMap<String, ColumnItemType>);

impl From<Vec<(&str, ColumnItemType)>> for Columns {
    #[instrument(parent = None, ret, level = "trace")]
    fn from(columns_vec: Vec<(&str, ColumnItemType)>) -> Self {
        let columns_map = columns_vec
            .into_iter()
            .map(|column| (column.0.to_owned(), column.1))
            .collect();
        Columns(columns_map)
    }
}

impl Deref for Columns {
    type Target = BTreeMap<String, ColumnItemType>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Columns {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Columns {
    const BINCODE_CONFIG: bincode::config::Configuration = bincode::config::standard();
    #[instrument(parent = None, ret, level = "trace")]
    pub fn new() -> Self {
        Self(BTreeMap::<String, ColumnItemType>::new())
    }

    #[instrument(parent = None, ret, level = "trace")]
    pub fn to_printable(&self) -> impl Iterator<Item = String> + '_ {
        self.0.keys().map(|key| key.to_owned())
    }
}


