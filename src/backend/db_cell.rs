use std::mem;
use std::rc::Rc;

use thiserror::Error;

const PAYLOAD_SIZE_SIZE: usize = mem::size_of::<u16>();
const ID_SIZE: usize = mem::size_of::<u64>();
const LEFT_CHILD_SIZE: usize = mem::size_of::<u32>();

#[derive(Error, Debug)]
pub enum CellError {
    #[error("Could not convert data into bytes to insert into cell.")]
    DataToPayloadError,
}

pub trait DBCell: Into<Rc<[u8]>> {
    // add code here
}

#[derive(Debug)]
pub struct LeafCell {
    payload_size: u16,
    pub id: u64,
    pub data: Rc<[u8]>,
    left_child: u32,
}

#[derive(Debug)]
pub struct InteriorCell {
    left_child: u32,
    id: u64,
}

impl LeafCell {
    pub fn new<T>(serializable_data: T) -> Result<Self, CellError>
    where
        T: TryInto<Rc<[u8]>, Error = ()>,
    {
        let data = TryInto::<Rc<[u8]>>::try_into(serializable_data)
            .map_err(|_| CellError::DataToPayloadError)?;
        let payload_size = (data.len() + ID_SIZE + LEFT_CHILD_SIZE) as u16;

        Ok(Self {
            payload_size,
            id: 0,
            data,
            left_child: 0,
        })
    }
}

impl DBCell for LeafCell {}

impl From<LeafCell> for Rc<[u8]> {
    fn from(leaf_cell: LeafCell) -> Rc<[u8]> {
        let bytes_vec: Vec<u8> = leaf_cell
            .payload_size
            .to_be_bytes()
            .into_iter()
            .chain(leaf_cell.id.to_be_bytes())
            .chain(leaf_cell.data.iter().cloned())
            .chain(leaf_cell.left_child.to_be_bytes())
            .collect();

        bytes_vec.into()
    }
}

impl TryFrom<&[u8]> for LeafCell {
    type Error = ();

    fn try_from(bytes: &[u8]) -> Result<LeafCell, ()> {
        if bytes.len() < PAYLOAD_SIZE_SIZE + ID_SIZE + LEFT_CHILD_SIZE {
            return Err(());
        }

        let mut offset = 0;
        let payload_size = u16::from_be_bytes(
            bytes[offset..offset + PAYLOAD_SIZE_SIZE]
                .try_into()
                .unwrap(),
        );

        offset += PAYLOAD_SIZE_SIZE;

        let id = u64::from_be_bytes(
            bytes
                .get(offset..offset + ID_SIZE)
                .ok_or(())?
                .try_into()
                .map_err(|_| ())?,
        );

        offset += ID_SIZE;

        let data = bytes
            .get(offset..offset + payload_size as usize)
            .ok_or(())?
            .to_vec()
            .into();

        offset += payload_size as usize;

        let left_child = u32::from_be_bytes(
            bytes
                .get(offset..offset + LEFT_CHILD_SIZE)
                .ok_or(())?
                .try_into()
                .map_err(|_| ())?,
        );

        Ok(Self {
            payload_size,
            id,
            data,
            left_child,
        })
    }
}
