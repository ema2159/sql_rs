#![allow(dead_code)]
use std::mem::{self, MaybeUninit};

use super::row::*;

const PAGE_SIZE: usize = 4096;

pub struct Page {
    num_rows: u16,
    data: [u8; PAGE_SIZE],
    curr_slot: usize,
}

pub enum PageError {
    PageFull,
}

impl Page {
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
            curr_slot: 2,
        }
    }

    pub fn insert(&mut self, row: Row) -> Result<(), bincode::error::EncodeError> {
        const SIZE_SLOT_SIZE: usize = 2;
        // Insert data
        let data_slot = &mut self.data[self.curr_slot + SIZE_SLOT_SIZE..];
        let inserted_bytes = row.serialize_into(data_slot)?;

        // Insert size of serialized row in size slot at the beginning of the slot
        let size_slot = &mut self.data[self.curr_slot..self.curr_slot + SIZE_SLOT_SIZE];
        size_slot.copy_from_slice(&(inserted_bytes as u16).to_be_bytes());

        self.curr_slot = self.curr_slot + SIZE_SLOT_SIZE + inserted_bytes + 1;
        self.num_rows += 1;

        Ok(())
    }
}
