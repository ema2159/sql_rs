#![allow(dead_code)]
use std::mem::{self, MaybeUninit};

use thiserror::Error;

use super::row::Row;

pub const PAGE_SIZE: usize = 4096;

#[derive(Debug)]
pub struct Page {
    num_rows: u16,
    data: [u8; PAGE_SIZE],
    curr_slot: usize,
}

#[derive(Error, Debug)]
pub enum PageError {
    #[error("Cannot insert row. Remaining page capacity is smaller than the row size")]
    PageFull,
    #[error("Could not serialize row. The following error was encountered: {0}")]
    RowEncodingError(String),
    #[error("Could not deserialize page. The following error ocurred during deserialization: {0}")]
    DeserializingError(String),
    #[error(
        "The slice being deserialized does not correspond to a valid page. End of the slice
                reached during deserialization"
    )]
    EndOfSliceWhileDeserializing,
}

impl Page {
    const NUM_ROWS_SLOT_SIZE: usize = 2;
    const CURR_SLOT_SLOT_SIZE: usize = 2;
    const START_SLOT: usize = Self::NUM_ROWS_SLOT_SIZE + Self::CURR_SLOT_SLOT_SIZE;
    const ROW_SIZE_SLOT_SIZE: usize = 2;

    pub fn new() -> Self {
        /* For performance reasons, a page is initialized as an empty array.
        It is the responsibility of the implementation to read and write the data properly.
        */
        let uninitialized_array = {
            let uninitialized_array: [MaybeUninit<u8>; PAGE_SIZE] =
                unsafe { MaybeUninit::uninit().assume_init() };

            unsafe { mem::transmute::<_, [u8; PAGE_SIZE]>(uninitialized_array) }
        };

        Self {
            num_rows: 0,
            data: uninitialized_array,
            // First two slots left for row counter
            curr_slot: Self::START_SLOT,
        }
    }

    pub fn from_slice(bytes: &[u8]) -> Self {
        /* For performance reasons, a page is initialized as an empty array.
        It is the responsibility of the implementation to read and write the data properly.
        */
        let mut uninitialized_array = {
            let uninitialized_array: [MaybeUninit<u8>; PAGE_SIZE] =
                unsafe { MaybeUninit::uninit().assume_init() };

            unsafe { mem::transmute::<_, [u8; PAGE_SIZE]>(uninitialized_array) }
        };

        uninitialized_array.copy_from_slice(bytes);

        let data = uninitialized_array;

        // Extract number of rows
        let mut num_rows_bytes: [u8; Self::NUM_ROWS_SLOT_SIZE] = [0; Self::NUM_ROWS_SLOT_SIZE];
        num_rows_bytes.copy_from_slice(
            &bytes[0..Self::NUM_ROWS_SLOT_SIZE], // .ok_or(PageError::EndOfSliceWhileDeserializing)?,
        );
        let num_rows: u16 = u16::from_be_bytes(num_rows_bytes);

        // Extract curr_slot
        let mut curr_slot_bytes: [u8; Self::CURR_SLOT_SLOT_SIZE] = [0; Self::CURR_SLOT_SLOT_SIZE];
        curr_slot_bytes.copy_from_slice(
            &bytes[Self::NUM_ROWS_SLOT_SIZE..Self::NUM_ROWS_SLOT_SIZE + Self::CURR_SLOT_SLOT_SIZE],
        );
        let curr_slot: usize = u16::from_be_bytes(curr_slot_bytes) as usize;

        Page {
            curr_slot,
            num_rows,
            data,
        }
    }

    pub fn serialize(&mut self) -> &[u8; PAGE_SIZE] {
        self.write_header();
        &self.data
    }

    pub fn insert(&mut self, row: Row) -> Result<(), PageError> {
        const SIZE_SLOT_SIZE: usize = 2;
        // Insert data
        let data_slot = &mut self.data[self.curr_slot + SIZE_SLOT_SIZE..];
        let inserted_bytes = row.serialize_into(data_slot).map_err(|err| match err {
            bincode::error::EncodeError::UnexpectedEnd => PageError::PageFull,
            _ => PageError::RowEncodingError(err.to_string()),
        })?;

        // Insert size of serialized row in size slot at the beginning of the slot
        let size_slot = &mut self.data[self.curr_slot..self.curr_slot + SIZE_SLOT_SIZE];
        size_slot.copy_from_slice(&(inserted_bytes as u16).to_be_bytes());

        self.curr_slot = self.curr_slot + SIZE_SLOT_SIZE + inserted_bytes + 1;
        self.num_rows += 1;
        Ok(())
    }

    pub fn write_header(&mut self) -> u16 {
        let num_rows_slot = &mut self.data[..Self::NUM_ROWS_SLOT_SIZE];
        num_rows_slot.copy_from_slice(&self.num_rows.to_be_bytes());
        let curr_slot_slot = &mut self.data
            [Self::NUM_ROWS_SLOT_SIZE..Self::NUM_ROWS_SLOT_SIZE + Self::ROW_SIZE_SLOT_SIZE];
        curr_slot_slot.copy_from_slice(&(self.curr_slot as u16).to_be_bytes());
        self.num_rows
    }

    /* Page is serialized as follows:
    - First two bytes: number of rows in the page
    - All the serialized rows with the first two bytes of each row sequence
      corresponding to the serialized row size and the following bytes corresponding
      to the contents of the row serialized */
    pub fn deserialize_rows(&self) -> Result<Vec<Row>, PageError> {
        // Extract number of rows
        let mut num_rows_bytes: [u8; Self::NUM_ROWS_SLOT_SIZE] = [0; Self::NUM_ROWS_SLOT_SIZE];
        num_rows_bytes.copy_from_slice(
            self.data
                .get(0..Self::NUM_ROWS_SLOT_SIZE)
                .ok_or(PageError::EndOfSliceWhileDeserializing)?,
        );
        let num_rows: u16 = u16::from_be_bytes(num_rows_bytes);

        let mut curr_idx = Self::START_SLOT;
        let mut rows_vec: Vec<Row> = Vec::new();

        for _ in 0..num_rows {
            // Extract number of bytes per row
            let mut row_size_bytes: [u8; Self::ROW_SIZE_SLOT_SIZE] = [0; Self::ROW_SIZE_SLOT_SIZE];
            row_size_bytes.copy_from_slice(
                self.data
                    .get(curr_idx..curr_idx + Self::ROW_SIZE_SLOT_SIZE)
                    .ok_or(PageError::EndOfSliceWhileDeserializing)?,
            );
            let row_size: u16 = u16::from_be_bytes(row_size_bytes);

            // Deserialize row
            let (curr_row_start, curr_row_end) = (
                curr_idx + Self::ROW_SIZE_SLOT_SIZE,
                curr_idx + Self::ROW_SIZE_SLOT_SIZE + row_size as usize,
            );
            let curr_row = Row::deserialize(&self.data[curr_row_start..curr_row_end])
                .map_err(|err| PageError::DeserializingError(err.to_string()))?;
            rows_vec.push(curr_row);
            curr_idx = curr_row_end + 1;
        }

        Ok(rows_vec)
    }
}
