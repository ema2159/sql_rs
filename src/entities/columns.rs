#![allow(dead_code)]
use std::collections::BTreeMap;
use std::ops::{Deref, DerefMut};

use bincode;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum IntegerType {
    Int,
}

impl IntegerType {
    fn validate(&self, input: &str) -> bool {
        match self {
            IntegerType::Int => input.parse::<i32>().is_ok(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum TextType {
    Varchar(u8),
}

impl TextType {
    fn validate(&self, input: &str) -> bool {
        match self {
            TextType::Varchar(max_size) => input.len() <= *max_size as usize,
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
    pub fn new(attributes: BTreeMap<String, ColumnItemType>) -> Self {
        Self(attributes)
    }

    pub fn serialize(self) -> Result<Vec<u8>, bincode::error::EncodeError> {
        bincode::serde::encode_to_vec::<BTreeMap<String, ColumnItemType>, _>(
            self.0,
            Self::BINCODE_CONFIG,
        )
    }

    pub fn deserialize(bytes: &[u8]) -> Result<Self, bincode::error::DecodeError> {
        let attributes = bincode::serde::decode_borrowed_from_slice::<
            BTreeMap<String, ColumnItemType>,
            _,
        >(bytes, Self::BINCODE_CONFIG)?;
        Ok(Self(attributes))
    }

    fn validate(input: &str, column_type: &ColumnItemType) -> bool {
        match column_type {
            ColumnItemType::Integer(int_type) => int_type.validate(input),

            ColumnItemType::Text(text_type) => text_type.validate(input),
        }
    }
}
