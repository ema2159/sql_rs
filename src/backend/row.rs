use std::rc::Rc;

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

    pub fn to_printable(&self) -> Vec<String> {
        self.attributes
            .iter()
            .map(|attribute| attribute.to_string())
            .collect()
    }
}

impl TryInto<Rc<[u8]>> for Row {
    type Error = ();

    fn try_into(self) -> Result<Rc<[u8]>, Self::Error> {
        let row_encoded =
            bincode::serde::encode_to_vec::<Vec<SQLType>, _>(self.attributes, Self::BINCODE_CONFIG)
                .map_err(|_| ())?;

        Ok(row_encoded.into())
    }
}

impl TryFrom<Rc<[u8]>> for Row {
    type Error = ();

    fn try_from(bytes: Rc<[u8]>) -> Result<Row, Self::Error> {
        let attributes = bincode::serde::decode_borrowed_from_slice::<Vec<SQLType>, _>(
            &bytes,
            Self::BINCODE_CONFIG,
        )
        .map_err(|_| ())?;
        Ok(Self { attributes })
    }
}
