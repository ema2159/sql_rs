#![allow(dead_code)]
use std::mem::{self, MaybeUninit};
use std::rc::Rc;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::db_cell::LeafCell;
use super::row::Row;

pub const PAGE_SIZE: usize = 4096;
const PAGE_HEADER_SIZE: usize = mem::size_of::<PageHeader>();

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize)]
struct PageHeader {
    page_type: u8,
    first_free_block: u16,
    num_cells: u16,
    cells_start: u16,
    fragmented_free_bytes: u8,
    right_pointer: u32,
}

impl PageHeader {
    fn set_page_type(&mut self, val: u8, header_slice: &mut [u8]) {
        self.page_type = val;
        header_slice[0] = val;
    }

    fn set_first_free_block(&mut self, val: u16, header_slice: &mut [u8]) {
        self.first_free_block = val;
        header_slice[1..3].copy_from_slice(&val.to_be_bytes());
    }

    fn set_num_cells(&mut self, val: u16, header_slice: &mut [u8]) {
        self.num_cells = val;
        header_slice[3..5].copy_from_slice(&val.to_be_bytes());
    }

    fn set_cells_start(&mut self, val: u16, header_slice: &mut [u8]) {
        self.cells_start = val;
        header_slice[5..7].copy_from_slice(&val.to_be_bytes());
    }

    fn set_fragmented_free_bytes(&mut self, val: u8, header_slice: &mut [u8]) {
        self.fragmented_free_bytes = val;
        header_slice[7] = val;
    }

    fn set_right_pointer(&mut self, val: u32, header_slice: &mut [u8]) {
        self.right_pointer = val;
        header_slice[8..12].copy_from_slice(&val.to_be_bytes());
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Page {
    header: PageHeader,
    data: [u8; PAGE_SIZE],
}

#[derive(Error, Debug)]
pub enum PageError {
    #[error("Cannot process page. The page data is corrupt.")]
    CorruptData,
    #[error("Cannot insert row. Remaining page capacity is smaller than the row size")]
    PageFull,
    #[error("Could not insert")]
    InsertError,
    #[error(
        "The slice being deserialized does not correspond to a valid page. End of the slice
                reached during deserialization"
    )]
    EndOfSliceWhileDeserializing,
}

impl Page {
    const BINCODE_CONFIG: bincode::config::Configuration = bincode::config::standard();
    const START_SLOT: u16 = PAGE_HEADER_SIZE as u16;
    const OFFSET_BYTE_SIZE: usize = 2;

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
            header: PageHeader {
                first_free_block: Self::START_SLOT,
                ..Default::default()
            },
            data: uninitialized_array,
        }
    }

    pub fn insert<T>(&mut self, data: T) -> Result<(), PageError>
    where
        T: TryInto<Rc<[u8]>, Error = ()>,
    {
        // NOTE: If first free block in the header is equal to 0, then page is full
        if self.header.first_free_block == 0 {
            Err(PageError::PageFull)?
        }

        // Check if page has enough space
        let cell_bytes: Rc<[u8]> = LeafCell::new(data)
            .map_err(|_| PageError::InsertError)?
            .into();
        let slot_start = self.header.first_free_block as usize;
        let slot_end = slot_start + cell_bytes.len();
        let start_of_offset_array =
            PAGE_SIZE - ((self.header.num_cells as usize) + 1) * Self::OFFSET_BYTE_SIZE;

        if slot_end >= start_of_offset_array {
            self.header
                .set_first_free_block(0, &mut self.data[..PAGE_HEADER_SIZE]);
            Err(PageError::PageFull)?
        }

        // Insert data into slot
        let data_slot = &mut self.data[slot_start..slot_end];
        data_slot.copy_from_slice(&cell_bytes);

        self.header
            .set_first_free_block(slot_end as u16, &mut self.data[..PAGE_HEADER_SIZE]);
        self.header.set_num_cells(
            self.header.num_cells + 1,
            &mut self.data[..PAGE_HEADER_SIZE],
        );
        self.write_slot_to_offset_array(start_of_offset_array, slot_start as u16);

        Ok(())
    }

    fn write_slot_to_offset_array(&mut self, start_of_offset_array: usize, slot: u16) {
        let bytes = slot.to_be_bytes();
        self.data[start_of_offset_array..start_of_offset_array + Self::OFFSET_BYTE_SIZE]
            .copy_from_slice(&bytes);
    }

    fn get_offset_array(&self) -> Vec<u16> {
        let offset_array_size = (self.header.num_cells as usize) * Self::OFFSET_BYTE_SIZE;
        let offset_bytes = &self.data[PAGE_SIZE - offset_array_size..PAGE_SIZE];

        offset_bytes
            .chunks_exact(2)
            .map(|chunk| u16::from_be_bytes([chunk[0], chunk[1]]))
            .rev()
            .collect()
    }

    pub fn deserialize_cells(&self) -> Result<Vec<Row>, PageError> {
        let offset_bytes = self.get_offset_array();

        let mut rows_vec: Vec<Row> = Vec::new();
        for offset in offset_bytes.into_iter().map(|x| x as usize) {
            let cell: LeafCell = self
                .data
                .get(offset..)
                .ok_or(PageError::CorruptData)?
                .try_into()
                .map_err(|_| PageError::CorruptData)?;

            let curr_row = Row::try_from(cell.data).unwrap();
            rows_vec.push(curr_row);
        }

        Ok(rows_vec)
    }
}

impl Default for Page {
    fn default() -> Self {
        Page::new()
    }
}

impl From<Page> for [u8; PAGE_SIZE] {
    fn from(page: Page) -> Self {
        page.data
    }
}
