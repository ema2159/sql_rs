#![allow(dead_code)]
use bincode;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum SQLType {
    Int(i32),
    Varchar(String),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Row {
    attributes: Vec<SQLType>,
}

impl Row {
    const BINCODE_CONFIG: bincode::config::Configuration = bincode::config::standard();

    pub fn new(attributes: Vec<SQLType>) -> Self {
        Self { attributes }
    }

    pub fn serialize(self) -> Result<Vec<u8>, bincode::error::EncodeError> {
        bincode::serde::encode_to_vec::<Vec<SQLType>, _>(self.attributes, Self::BINCODE_CONFIG)
    }

    pub fn deserialize(bytes: &[u8]) -> Result<Self, bincode::error::DecodeError> {
        let attributes = bincode::serde::decode_borrowed_from_slice::<Vec<SQLType>, _>(
            bytes,
            Self::BINCODE_CONFIG,
        )?;
        Ok(Self { attributes })
    }
}
