#![allow(dead_code)]
use bincode;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum SQLType {
    Integer(i32),
    Text(String),
}

impl ToString for SQLType {
    // Required method
    fn to_string(&self) -> String {
        match self {
            SQLType::Integer(num) => num.to_string(),
            SQLType::Text(s) => s.to_owned(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Row {
    attributes: Vec<SQLType>,
}

impl Row {
    const BINCODE_CONFIG: bincode::config::Configuration = bincode::config::standard();

    pub fn new(attributes: Vec<SQLType>) -> Self {
        Self { attributes }
    }

    pub fn serialize_into(self, slot: &mut [u8]) -> Result<usize, bincode::error::EncodeError> {
        bincode::serde::encode_into_slice::<Vec<SQLType>, _>(
            self.attributes,
            slot,
            Self::BINCODE_CONFIG,
        )
    }

    pub fn deserialize(row_bytes: &[u8]) -> Result<Self, bincode::error::DecodeError> {
        let attributes = bincode::serde::decode_borrowed_from_slice::<Vec<SQLType>, _>(
            row_bytes,
            Self::BINCODE_CONFIG,
        )?;
        Ok(Self { attributes })
    }

    pub fn to_printable(&self) -> Vec<String> {
        self.attributes
            .iter()
            .map(|attribute| attribute.to_string())
            .collect()
    }
}

impl TryInto<Vec<u8>> for Row {
    type Error = bincode::error::EncodeError;

    fn try_into(self) -> Result<Vec<u8>, Self::Error> {
        let mut row_encoded = bincode::serde::encode_to_vec::<Vec<SQLType>, _>(
            self.attributes,
            Self::BINCODE_CONFIG,
        )?;

        // NOTE: Encode len with row in the meantime given without a free list it is not possible
        // to know when a cell ends when the subsequent cell is deleted.
        let encoded_len = ((row_encoded.len() + 2) as u16).to_be_bytes();

        row_encoded
            .splice(0..0, encoded_len.iter().cloned());

        Ok(row_encoded)
    }
}
