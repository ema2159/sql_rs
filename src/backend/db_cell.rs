use std::io::Cursor;
use std::mem;
use std::rc::Rc;

use thiserror::Error;

use serde::{Deserialize, Serialize};

const PAYLOAD_SIZE_SIZE: usize = mem::size_of::<u16>();
const ID_SIZE: usize = mem::size_of::<u64>();
const LEFT_CHILD_SIZE: usize = mem::size_of::<u32>();

#[derive(Error, Debug)]
pub enum CellError {
    #[error("Could not convert data into bytes to insert into cell.")]
    DataToPayloadError,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DBCell {
    payload_size_slot: u16,
    pub id: u64,
    pub value: Box<[u8]>,
    left_child: u32,
}

impl DBCell {
    const BINCODE_CONFIG: bincode::config::Configuration<
        bincode::config::BigEndian,
        bincode::config::Fixint,
    > = bincode::config::standard()
        .with_big_endian()
        .with_fixed_int_encoding();

    pub fn new<T>(id: u64, serializable_data: T) -> Result<Self, CellError>
    where
        T: TryInto<Box<[u8]>, Error = ()>,
    {
        let data = TryInto::<Box<[u8]>>::try_into(serializable_data)
            .map_err(|_| CellError::DataToPayloadError)?;

        Ok(Self {
            payload_size_slot: 0,
            id,
            value: data.into(),
            left_child: 0,
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
    type Error = ();

    fn try_into(self) -> Result<Rc<[u8]>, Self::Error> {
        let mut cell_encoded =
            bincode::serde::encode_to_vec::<DBCell, _>(self, Self::BINCODE_CONFIG)
                .map_err(|_| ())?;
        let payload_size: u16 = (cell_encoded.len() - PAYLOAD_SIZE_SIZE) as u16;

        cell_encoded[..PAYLOAD_SIZE_SIZE].copy_from_slice(&payload_size.to_be_bytes());

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
            bincode::serde::decode_borrowed_from_slice::<DBCell, _>(&bytes, Self::BINCODE_CONFIG)
                .map_err(|_| ())?;
        Ok(db_cell)
    }
}
