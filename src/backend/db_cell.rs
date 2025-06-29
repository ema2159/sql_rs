use std::io::Cursor;
use std::mem;
use std::rc::Rc;

use thiserror::Error;

use serde::{Deserialize, Serialize};
use tracing::instrument;

use super::page::INVALID_PAGE_NUM;

const PAYLOAD_SIZE_SIZE: usize = mem::size_of::<u16>();
const ID_SIZE: usize = mem::size_of::<u64>();
const LEFT_CHILD_SIZE: usize = mem::size_of::<u32>();

#[derive(Error, Debug)]
pub enum CellError {
    #[error("Could encode data into cell: {0}")]
    DataToPayload(#[from] bincode::error::EncodeError),
    #[error("Could not decode data from cell: {0}")]
    DataFromPayload(#[from] bincode::error::DecodeError),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DBCell {
    pub payload_size: u16,
    pub key: u64,
    pub payload: Box<[u8]>,
    pub left_child: u32,
}

impl DBCell {
    const BINCODE_CONFIG: bincode::config::Configuration<
        bincode::config::BigEndian,
        bincode::config::Fixint,
    > = bincode::config::standard()
        .with_big_endian()
        .with_fixed_int_encoding();

    #[instrument(parent = None, level = "trace")]
    pub fn new(key: u64, data: &[u8], left_child_optn: Option<u32>) -> Result<Self, CellError> {
        let left_child = left_child_optn.unwrap_or(INVALID_PAGE_NUM);
        Ok(Self {
            payload_size: data.len() as u16,
            key,
            payload: data.into(),
            left_child,
        })
    }

    pub fn id_from_slice(bytes: &[u8]) -> Result<u64, ()> {
        if bytes.len() < PAYLOAD_SIZE_SIZE + ID_SIZE {
            return Err(());
        }

        let mut cursor = Cursor::new(bytes);
        let _payload_size: u16 =
            bincode::serde::decode_from_std_read::<u16, _, _>(&mut cursor, Self::BINCODE_CONFIG)
                .map_err(|_| ())?;
        let id: u64 =
            bincode::serde::decode_from_std_read::<u64, _, _>(&mut cursor, Self::BINCODE_CONFIG)
                .map_err(|_| ())?;
        Ok(id)
    }
}

impl TryInto<Rc<[u8]>> for DBCell {
    type Error = CellError;

    fn try_into(self) -> Result<Rc<[u8]>, Self::Error> {
        let cell_encoded = bincode::serde::encode_to_vec::<DBCell, _>(self, Self::BINCODE_CONFIG)
            .map_err(CellError::DataToPayload)?;

        Ok(cell_encoded.into())
    }
}

impl TryFrom<&[u8]> for DBCell {
    type Error = ();

    fn try_from(bytes: &[u8]) -> Result<DBCell, ()> {
        if bytes.len() < PAYLOAD_SIZE_SIZE + ID_SIZE + LEFT_CHILD_SIZE {
            return Err(());
        }

        let db_cell =
            bincode::serde::decode_borrowed_from_slice::<DBCell, _>(bytes, Self::BINCODE_CONFIG)
                .map_err(|_| ())?;
        Ok(db_cell)
    }
}
