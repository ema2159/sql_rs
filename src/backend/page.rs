#![allow(dead_code)]
use std::mem::{self, MaybeUninit};
use std::ops::{Deref, DerefMut};
use std::rc::Rc;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::cursor::DBCursor;
use super::db_cell::DBCell;
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

#[derive(Debug, Clone, Default)]
struct CellPtrArray(Vec<u16>);

impl CellPtrArray {
    const PTR_BYTE_SIZE: usize = 2;

    fn read_from_slice(num_cells: usize, cell_ptr_array_start: &[u8]) -> Self {
        let cell_ptr_array_size = num_cells * Self::PTR_BYTE_SIZE;
        let cell_ptr_array = &cell_ptr_array_start[..cell_ptr_array_size];

        Self(
            cell_ptr_array
                .chunks_exact(2)
                .map(|chunk| u16::from_be_bytes([chunk[0], chunk[1]]))
                .collect(),
        )
    }

    fn update_pointer_array_after_insert(
        &mut self,
        cell_ptr_array_start: &mut [u8],
        new_cell_ptr: u16,
        new_cell_byte_size: u16,
    ) {
        let insert_pos = self.partition_point(|&cell_ptr| cell_ptr <= new_cell_ptr);
        self.insert(insert_pos, new_cell_ptr);
        for elem in self[..insert_pos].iter_mut() {
            *elem -= new_cell_byte_size;
        }
        self.write_pointer_array(cell_ptr_array_start);
    }

    fn write_pointer_array(&self, cell_ptr_array_start: &mut [u8]) {
        for (idx, elem) in self.iter().enumerate() {
            Self::write_u16_in_bytes(cell_ptr_array_start, idx, *elem);
        }
    }

    fn write_u16_in_bytes(bytes: &mut [u8], index: usize, value: u16) {
        let start = index * 2;
        let new_bytes = value.to_le_bytes();
        bytes[start] = new_bytes[0];
        bytes[start + 1] = new_bytes[1];
    }
}

impl DerefMut for CellPtrArray {
    fn deref_mut<'a>(&'a mut self) -> &'a mut Vec<u16> {
        &mut self.0
    }
}

impl Deref for CellPtrArray {
    type Target = Vec<u16>;

    fn deref<'a>(&'a self) -> &'a Vec<u16> {
        &self.0
    }
}

#[derive(Debug, Clone)]
pub struct Page {
    header: PageHeader,
    data: [u8; PAGE_SIZE],
    cell_pointer_array: CellPtrArray,
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
        "The slice being deserialized does not correspond to a valid page. End of the slice reached during deserialization"
    )]
    EndOfSliceWhileDeserializing,
    #[error("Cannot insert element. Key already exists.")]
    DuplicateKey,
}

impl Page {
    const BINCODE_CONFIG: bincode::config::Configuration = bincode::config::standard();
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
                cells_start: PAGE_SIZE as u16 - 1,
                ..Default::default()
            },
            data: uninitialized_array,
            cell_pointer_array: CellPtrArray::default(),
        }
    }

    pub fn get_keys(&self) -> Result<Vec<u64>, PageError> {
        let keys_res_iter = self
            .cell_pointer_array
            .iter()
            .map(|&cell_ptr| DBCell::id_from_slice(&self.data[cell_ptr as usize..]));
        let keys_res: Result<Vec<u64>, ()> = keys_res_iter.collect();
        keys_res.map_err(|_| PageError::CorruptData)
    }

    pub fn leaf_find_partition(&self, new_key: u64) -> Result<usize, PageError> {
        let keys = self.get_keys()?;
        let partition_key = keys.partition_point(|&key| key < new_key);

        if partition_key >= keys.len() {
            return Ok(PAGE_SIZE);
        }

        if keys[partition_key] == new_key {
            return Err(PageError::DuplicateKey);
        }

        let partition_point = self.cell_pointer_array[partition_key];

        Ok(partition_point as usize)
    }

    pub fn insert<T>(
        &mut self,
        insert_cursor: &mut DBCursor,
        key: u64,
        value: &T,
    ) -> Result<(), PageError>
    where
        T: TryInto<Box<[u8]>, Error = ()> + Clone,
    {
        if self.header.cells_start == 0 {
            Err(PageError::PageFull)?
        }

        // Check if page has enough space
        let cell_bytes: Rc<[u8]> = DBCell::new(key, (*value).clone())
            .map_err(|_| PageError::InsertError)?
            .try_into()
            .map_err(|_| PageError::InsertError)?;
        let old_cells_start = self.header.cells_start as usize;
        let new_cells_start = old_cells_start - cell_bytes.len();
        let end_of_ptr_array_after_insert =
            PAGE_HEADER_SIZE + ((self.header.num_cells as usize) + 1) * Self::OFFSET_BYTE_SIZE;

        if new_cells_start <= end_of_ptr_array_after_insert {
            self.header
                .set_cells_start(0, &mut self.data[..PAGE_HEADER_SIZE]);
            Err(PageError::PageFull)?
        }

        // ------------------ Insert data into slot ------------------
        let partition_point = self.leaf_find_partition(key)?;
        insert_cursor.cell_ptr_pos = partition_point - cell_bytes.len();
        // Make room for cell content area
        let _ = &self.data[new_cells_start..partition_point].rotate_left(cell_bytes.len());
        // Insert cell in slot
        let _ =
            &self.data[insert_cursor.cell_ptr_pos..partition_point].copy_from_slice(&cell_bytes);

        // Update header
        self.header
            .set_cells_start(new_cells_start as u16, &mut self.data[..PAGE_HEADER_SIZE]);
        self.header.set_num_cells(
            self.header.num_cells + 1,
            &mut self.data[..PAGE_HEADER_SIZE],
        );
        // Update cell pointer array
        self.cell_pointer_array.update_pointer_array_after_insert(
            &mut self.data[PAGE_HEADER_SIZE..],
            insert_cursor.cell_ptr_pos as u16,
            cell_bytes.len() as u16,
        );

        Ok(())
    }

    pub fn deserialize_cells(&self) -> Result<Vec<Row>, PageError> {
        let pointer_bytes = &self.cell_pointer_array;

        let mut rows_vec: Vec<Row> = Vec::new();
        for pointer in pointer_bytes.iter().map(|x| *x as usize) {
            let cell: DBCell = self
                .data
                .get(pointer..)
                .ok_or(PageError::CorruptData)?
                .try_into()
                .map_err(|_| PageError::CorruptData)?;

            let curr_row = Row::try_from(&*cell.value).unwrap();
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
