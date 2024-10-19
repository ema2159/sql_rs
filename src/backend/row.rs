use std::rc::Rc;

use bincode;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum SQLType {
    UBigInt(u64),
    Integer(i32),
    Text(String),
}

impl ToString for SQLType {
    // Required method
    fn to_string(&self) -> String {
        match self {
            SQLType::UBigInt(num) => num.to_string(),
            SQLType::Integer(num) => num.to_string(),
            SQLType::Text(s) => s.to_owned(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Row {
    rowid: u64,
    attributes: Vec<SQLType>,
}

impl Row {
    const BINCODE_CONFIG: bincode::config::Configuration = bincode::config::standard();

    pub fn new(rowid:u64, attributes: Vec<SQLType>) -> Self {
        Self { rowid, attributes }
    }

    pub fn rowid(&self) -> u64 {
        self.rowid
    }

    pub fn to_printable(&self) -> Vec<String> {
        self.attributes
            .iter()
            .map(|attribute| attribute.to_string())
            .collect()
    }
}

impl TryInto<Box<[u8]>> for Row {
    type Error = ();

    fn try_into(self) -> Result<Box<[u8]>, Self::Error> {
        let row_encoded = bincode::serde::encode_to_vec::<(u64, Vec<SQLType>), _>(
            (self.rowid, self.attributes),
            Self::BINCODE_CONFIG,
        )
        .map_err(|_| ())?;

        Ok(row_encoded.into())
    }
}

impl TryFrom<&[u8]> for Row {
    type Error = ();

    fn try_from(bytes: &[u8]) -> Result<Row, Self::Error> {
        let (rowid, attributes) = bincode::serde::decode_borrowed_from_slice::<(u64, Vec<SQLType>), _>(
            &bytes,
            Self::BINCODE_CONFIG,
        )
        .map_err(|_| ())?;
        Ok(Self { rowid, attributes })
    }
}
