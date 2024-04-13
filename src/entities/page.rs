#![allow(dead_code)]
use core::fmt::Display;
use std::error::Error;
use std::mem::{self, MaybeUninit};

use bincode;

use super::row::Row;

pub const PAGE_SIZE: usize = 4096;

#[derive(Debug)]
pub struct Page {
    num_rows: u16,
    data: [u8; PAGE_SIZE],
    curr_slot: usize,
}

#[derive(Debug)]
pub enum PageError {
    PageFull,
    RowEncodingError(Box<dyn Error>),
    DeserializingError(Box<dyn Error>),
    EndOfSliceWhileDeserializing,
}

impl Display for PageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PageError::PageFull => write!(
                f,
                "Cannot insert row. Remaining page capacity is smaller than the row size"
            ),
            PageError::RowEncodingError(inner_error) => write!(
                f,
                "Could not serialize row. The following error was encountered: {}",
                inner_error
            ),
            PageError::DeserializingError(inner_error) => write!(
                f,
                "Could not deserialize page. The following error ocurred during deserialization: {}",
                inner_error
            ),
            PageError::EndOfSliceWhileDeserializing => write!(
                f,
                "The slice being deserialized does not correspond to a valid page. End of the slice
                reached during deserialization"
            ),
        }
    }
}

impl std::error::Error for PageError {}

impl Page {
    const NUM_ROWS_SLOT_SIZE: usize = 2;
    const START_SLOT: usize = Self::NUM_ROWS_SLOT_SIZE;

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

        Page {
            curr_slot: 0,
            num_rows: 0,
            data,
        }
    }

    pub fn get_data(&self) -> &[u8; PAGE_SIZE] {
        &self.data
    }

    pub fn insert(&mut self, row: Row) -> Result<(), PageError> {
        const SIZE_SLOT_SIZE: usize = 2;
        // Insert data
        let data_slot = &mut self.data[self.curr_slot + SIZE_SLOT_SIZE..];
        let inserted_bytes = row.serialize_into(data_slot).map_err(|err| match err {
            bincode::error::EncodeError::UnexpectedEnd => PageError::PageFull,
            _ => PageError::RowEncodingError(Box::new(err)),
        })?;

        // Insert size of serialized row in size slot at the beginning of the slot
        let size_slot = &mut self.data[self.curr_slot..self.curr_slot + SIZE_SLOT_SIZE];
        size_slot.copy_from_slice(&(inserted_bytes as u16).to_be_bytes());

        self.curr_slot = self.curr_slot + SIZE_SLOT_SIZE + inserted_bytes + 1;
        self.num_rows += 1;
        Ok(())
    }

    pub fn write_row_num(&mut self) -> u16 {
        let num_rows_slot = &mut self.data[..Self::NUM_ROWS_SLOT_SIZE];
        num_rows_slot.copy_from_slice(&self.num_rows.to_be_bytes());
        self.num_rows
    }

    /* Page is serialized as follows:
    - First two bytes: number of rows in the page
    - All the serialized rows with the first two bytes of each row sequence
      corresponding to the serialized row size and the following bytes corresponding
      to the contents of the row serialized */
    pub fn deserialize_rows(&self) -> Result<Vec<Row>, PageError> {
        // Extract number of rows
        let mut num_rows_bytes: [u8; 2] = [0; 2];
        num_rows_bytes.copy_from_slice(
            self.data
                .get(0..2)
                .ok_or_else(|| PageError::EndOfSliceWhileDeserializing)?,
        );
        let num_rows: u16 = u16::from_be_bytes(num_rows_bytes);

        let mut curr_idx = Self::START_SLOT;
        let mut rows_vec: Vec<Row> = Vec::new();

        for _ in 0..num_rows {
            // Extract number of bytes per row
            let mut row_size_bytes: [u8; 2] = [0; 2];
            row_size_bytes.copy_from_slice(
                self.data
                    .get(curr_idx..curr_idx + 2)
                    .ok_or_else(|| PageError::EndOfSliceWhileDeserializing)?,
            );
            let row_size: u16 = u16::from_be_bytes(row_size_bytes);

            // Deserialize row
            let (curr_row_start, curr_row_end) = (curr_idx + 2, curr_idx + 2 + row_size as usize);
            let curr_row = Row::deserialize(&self.data[curr_row_start..curr_row_end])
                .map_err(|err| PageError::DeserializingError(Box::new(err)))?;
            rows_vec.push(curr_row);
            curr_idx = curr_row_end + 1;
        }

        Ok(rows_vec)
    }
}
