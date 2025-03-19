#![allow(dead_code)]
use std::fmt;
use std::mem::{self, MaybeUninit};
use std::ops::{Deref, DerefMut};
use std::rc::Rc;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{instrument, trace};

use super::cursor::DBCursor;
use super::db_cell::DBCell;
use super::row::Row;

pub const PAGE_SIZE: usize = 4096;
const PAGE_HEADER_SIZE: usize = mem::size_of::<PageHeader>();

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize)]
struct PageHeader {
    page_type: PageType,
    first_free_block: u16,
    num_cells: u16,
    cells_start: u16,
    fragmented_free_bytes: u8,
    right_pointer: u32,
}

impl PageHeader {
    fn set_page_type(&mut self, val: PageType, header_slice: &mut [u8]) {
        self.page_type = val.clone();
        header_slice[0] = val as u8;
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

    #[instrument(parent = None, ret, level = "trace")]
    fn read_from_bytes(num_cells: usize, cell_ptr_array_start: &[u8]) -> Self {
        let cell_ptr_array_size = num_cells * Self::PTR_BYTE_SIZE;
        let cell_ptr_array = &cell_ptr_array_start[..cell_ptr_array_size];

        Self(
            cell_ptr_array
                .chunks_exact(2)
                .map(|chunk| u16::from_be_bytes([chunk[0], chunk[1]]))
                .collect(),
        )
    }

    #[instrument(parent = None, ret, level = "trace")]
    fn split_cell_ptr_array(&mut self) -> (Self, Self) {
        let mid_point = self.len() / 2;
        let (left, right) = self.split_at(mid_point);

        (left.into(), right.into())
    }

    #[instrument(parent = None, skip_all, ret, level = "trace")]
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

    #[instrument(parent = None, skip_all, ret, level = "trace")]
    fn update_pointer_array_after_delete(
        &mut self,
        cell_ptr_array_start: &mut [u8],
        partition: usize,
    ) {
        self.remove(partition);
        self.write_pointer_array(cell_ptr_array_start);
    }

    #[instrument(parent = None, skip_all, ret, level = "trace")]
    fn write_pointer_array(&self, cell_ptr_array_start: &mut [u8]) {
        for (idx, elem) in self.iter().enumerate() {
            Self::write_u16_in_bytes(cell_ptr_array_start, idx, *elem);
        }
    }

    #[instrument(parent = None, skip_all, ret, level = "trace")]
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

impl From<&[u16]> for CellPtrArray {
    fn from(data: &[u16]) -> Self {
        let cell_contents: Vec<u16> = data.into();
        Self(cell_contents)
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
    #[error("Trying to delete from empty page.")]
    DeleteFromEmpty,
    #[error("Cannot insert row. Remaining page capacity is smaller than the row size")]
    PageFull,
    #[error("Could not insert")]
    InsertError,
    #[error(
        "The record that is being updated cannot be updated in place. The new payload is of a different size."
    )]
    UpdateNotSameSize,
    #[error("Cannot update record of key {0}. Page doesn't contain a record with a matching key.")]
    UpdateNotSameKey(u64),
    #[error(
        "The slice being deserialized does not correspond to a valid page. End of the slice reached during deserialization"
    )]
    EndOfSliceWhileDeserializing,
    #[error("Cannot get first/last key of empty cell pointer array")]
    CannotGetFirstOrLastKey,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum PageType {
    #[default]
    Leaf = 0x0d,
    Interior = 0x05,
}

impl Page {
    const BINCODE_CONFIG: bincode::config::Configuration = bincode::config::standard();
    const CELL_PTR_BYTE_SIZE: usize = 2;

    #[instrument(parent = None, level = "trace")]
    pub fn new(page_type: PageType) -> Self {
        /* For performance reasons, a page is initialized as an empty array.
        It is the responsibility of the implementation to read and write the data properly.
        */
        let mut uninitialized_array = {
            let uninitialized_array: [MaybeUninit<u8>; PAGE_SIZE] =
                unsafe { MaybeUninit::uninit().assume_init() };

            unsafe { mem::transmute::<_, [u8; PAGE_SIZE]>(uninitialized_array) }
        };

        let mut header = PageHeader::default();
        header.set_page_type(page_type, &mut uninitialized_array);
        header.set_cells_start(PAGE_SIZE as u16, &mut uninitialized_array);

        Self {
            header,
            data: uninitialized_array,
            cell_pointer_array: CellPtrArray::default(),
        }
    }

    #[instrument(parent = None, ret, level = "trace")]
    pub fn new_from_split(
        mut cell_pointers: CellPtrArray,
        data: &[u8],
        shift: usize,
        page_type: PageType,
    ) -> Self {
        let mut new_page_bytes = {
            let uninitialized_array: [MaybeUninit<u8>; PAGE_SIZE] =
                unsafe { MaybeUninit::uninit().assume_init() };

            unsafe { mem::transmute::<_, [u8; PAGE_SIZE]>(uninitialized_array) }
        };

        let cells_start = PAGE_SIZE - data.len();
        new_page_bytes[cells_start..].copy_from_slice(data);

        // Adjust cell pointer array
        cell_pointers
            .iter_mut()
            .for_each(|ptr| *ptr += shift as u16);

        let mut header = PageHeader::default();
        header.set_page_type(page_type, &mut new_page_bytes);
        header.set_cells_start(cells_start as u16, &mut new_page_bytes);

        let mut new_page = Self {
            header,
            data: new_page_bytes,
            cell_pointer_array: cell_pointers,
        };

        // Write cell_pointer_array to new page bytes
        new_page
            .cell_pointer_array
            .write_pointer_array(&mut new_page.data[PAGE_HEADER_SIZE..]);

        // Write header fields to new page bytes
        new_page.header.set_cells_start(
            new_page.cell_pointer_array[0],
            &mut new_page.data[..PAGE_HEADER_SIZE],
        );
        new_page.header.set_num_cells(
            new_page.cell_pointer_array.len() as u16,
            &mut new_page.data[..PAGE_HEADER_SIZE],
        );

        new_page
    }

    #[instrument(parent = None, skip(self), ret, level = "trace")]
    pub fn get_page_type(&self) -> &PageType {
        &self.header.page_type
    }

    #[instrument(parent = None, skip(self), ret, level = "trace")]
    fn get_keys(&self) -> Result<Vec<u64>, PageError> {
        let keys_res_iter = self
            .cell_pointer_array
            .iter()
            .map(|&cell_ptr| DBCell::id_from_slice(&self.data[cell_ptr as usize..]));
        let keys_res: Result<Vec<u64>, ()> = keys_res_iter.collect();
        keys_res.map_err(|_| PageError::CorruptData)
    }

    #[instrument(parent = None, skip(self), ret, level = "trace")]
    pub fn get_first_key(&self) -> Result<u64, PageError> {
        if self.cell_pointer_array.len() == 0 {
            Err(PageError::CannotGetFirstOrLastKey)?
        }
        let first_pointer = self.cell_pointer_array[0];
        let key = DBCell::id_from_slice(&self.data[first_pointer as usize..]);
        key.map_err(|_| PageError::CorruptData)
    }

    #[instrument(parent = None, skip(self), ret, level = "trace")]
    pub fn get_last_key(&self) -> Result<u64, PageError> {
        if self.cell_pointer_array.len() == 0 {
            Err(PageError::CannotGetFirstOrLastKey)?
        }
        let last_pointer = self.cell_pointer_array[self.cell_pointer_array.len() - 1];
        let key = DBCell::id_from_slice(&self.data[last_pointer as usize..]);
        key.map_err(|_| PageError::CorruptData)
    }

    #[instrument(parent = None, skip(self), ret, level = "trace")]
    pub fn get_next_page_pointer(&self, key: u64) -> Result<u32, PageError> {
        let (partition, _) = self.find_partition(key)?;
        if partition >= PAGE_SIZE {
            return Ok(self.header.right_pointer);
        }
        let next_page_cell: DBCell = self.data[partition..]
            .try_into()
            .map_err(|_| PageError::CorruptData)?;
        Ok(next_page_cell.left_child)
    }

    #[instrument(parent = None, skip(self), ret, level = "trace")]
    pub fn move_last_left_child_to_right_pointer(&mut self) -> Result<(), PageError> {
        if self.cell_pointer_array.len() == 0 {
            Err(PageError::CannotGetFirstOrLastKey)?
        }
        let last_pointer = self.cell_pointer_array[self.cell_pointer_array.len() - 1];
        let last_cell: DBCell = self.data[last_pointer as usize..]
            .try_into()
            .map_err(|_| PageError::CorruptData)?;
        let last_cell_left_child = last_cell.left_child;
        self.set_right_pointer(last_cell_left_child);
        self.delete(self.cell_pointer_array.len() - 1)?;
        Ok(())
    }

    #[instrument(parent = None, skip(self), ret, level = "trace")]
    pub fn find_partition(&self, new_key: u64) -> Result<(usize, Option<u64>), PageError> {
        let keys = self.get_keys()?;
        let partition_key = keys.partition_point(|&key| key < new_key);

        if partition_key >= keys.len() {
            return Ok((PAGE_SIZE, None));
        }

        let curr_key_in_partition = keys[partition_key];

        let partition_point = self.cell_pointer_array[partition_key];

        Ok((partition_point as usize, Some(curr_key_in_partition)))
    }

    #[instrument(parent = None, skip(self), ret, level = "trace")]
    pub fn insert(
        &mut self,
        cell_ptr_pos: usize,
        key: u64,
        payload: &[u8],
        left_child: Option<u32>,
    ) -> Result<(), PageError> {
        // Check if page has enough space
        let cell_bytes: Rc<[u8]> = DBCell::new(key, payload, left_child)
            .map_err(|_| PageError::InsertError)?
            .try_into()
            .map_err(|_| PageError::InsertError)?;
        let old_cells_start = self.header.cells_start as usize;
        let new_cells_start = old_cells_start - cell_bytes.len();
        let end_of_ptr_array_after_insert =
            PAGE_HEADER_SIZE + ((self.header.num_cells as usize) + 1) * Self::CELL_PTR_BYTE_SIZE;

        if new_cells_start <= end_of_ptr_array_after_insert {
            self.header
                .set_cells_start(0, &mut self.data[..PAGE_HEADER_SIZE]);
            Err(PageError::PageFull)?
        }

        // ------------------ Insert data into slot ------------------
        let partition_point = cell_ptr_pos;
        let insert_position = partition_point - cell_bytes.len();
        // Make room for cell content area
        let _ = &self.data[new_cells_start..partition_point].rotate_left(cell_bytes.len());
        // Insert cell in slot
        let _ = &self.data[insert_position..partition_point].copy_from_slice(&cell_bytes);

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
            insert_position as u16,
            cell_bytes.len() as u16,
        );

        Ok(())
    }

    #[instrument(parent = None, skip(self), ret, level = "trace")]
    pub fn delete(&mut self, cell_ptr_pos: usize) -> Result<(), PageError> {
        if self.cell_pointer_array.is_empty() {
            return Err(PageError::DeleteFromEmpty);
        }
        if cell_ptr_pos as u16 == self.cell_pointer_array[self.cell_pointer_array.len() - 1] {
            let new_cells_start = if self.cell_pointer_array.len() <= 2 {
                PAGE_SIZE as u16
            } else {
                self.cell_pointer_array[self.cell_pointer_array.len() - 2]
            };
            self.header
                .set_cells_start(new_cells_start as u16, &mut self.data[..PAGE_HEADER_SIZE]);
        }
        self.header.set_num_cells(
            self.header.num_cells - 1,
            &mut self.data[..PAGE_HEADER_SIZE],
        );
        // Update cell pointer array
        self.cell_pointer_array
            .update_pointer_array_after_delete(&mut self.data[PAGE_HEADER_SIZE..], cell_ptr_pos);

        Ok(())
    }

    #[instrument(parent = None, skip(self), ret, level = "trace")]
    pub fn update_same_size(
        &mut self,
        cell_ptr_pos: usize,
        key: u64,
        payload: &[u8],
        left_child: Option<u32>,
    ) -> Result<(), PageError> {
        let partition_point = cell_ptr_pos;
        let cell_to_modify: DBCell = self.data[partition_point..]
            .try_into()
            .map_err(|_| PageError::CorruptData)?;

        if cell_to_modify.id != key {
            return Err(PageError::UpdateNotSameKey(key));
        }

        if cell_to_modify.payload_size != payload.len() as u16 {
            return Err(PageError::UpdateNotSameSize);
        }

        let new_payload_bytes: Rc<[u8]> = DBCell::new(key, payload, left_child)
            .map_err(|_| PageError::InsertError)?
            .try_into()
            .map_err(|_| PageError::InsertError)?;
        let _ = &self.data[partition_point..].copy_from_slice(&new_payload_bytes);

        Ok(())
    }

    #[instrument(parent = None, skip(self), ret, level = "trace")]
    pub fn split_page(mut self) -> (Self, Self) {
        let (left_ptr_array, right_ptr_array) = self.cell_pointer_array.split_cell_ptr_array();
        let (left_cells, right_cells) = (
            &self.data[left_ptr_array[0] as usize..right_ptr_array[0] as usize],
            &self.data[right_ptr_array[0] as usize..],
        );
        let right_node_cells_start = (PAGE_SIZE - right_cells.len()) as u16;

        // Create a new cell with the left-most records in the current Page, copying the records at
        // the end of the Page, as well as creating a new Cells Pointer Array pointing to them.
        let new_cell = Self::new_from_split(
            left_ptr_array,
            left_cells,
            right_cells.len(),
            *self.get_page_type(),
        );

        // Update cell pointer in current Page, leaving the right-most records in it. There is no
        // need to copy or move the actual records, as they are already present in the Page. Also,
        // update Page header
        self.cell_pointer_array = right_ptr_array.clone();
        self.cell_pointer_array
            .write_pointer_array(&mut self.data[PAGE_HEADER_SIZE..]);
        self.header.set_num_cells(
            self.cell_pointer_array.len() as u16,
            &mut self.data[..PAGE_HEADER_SIZE],
        );
        self.header
            .set_cells_start(right_node_cells_start, &mut self.data[..PAGE_HEADER_SIZE]);

        (new_cell, self)
    }

    #[instrument(parent = None, skip(self), ret, level = "trace")]
    pub fn set_right_pointer(&mut self, right_pointer: u32) {
        self.header
            .set_right_pointer(right_pointer, &mut self.data[..PAGE_HEADER_SIZE]);
    }

    #[instrument(parent = None, skip(self), level = "trace")]
    pub fn cells_iter(&self) -> impl Iterator<Item = Result<DBCell, PageError>> + '_ {
        self.cell_pointer_array
            .iter()
            .map(|pointer| *pointer as usize)
            .map(
                |pointer| match self.data.get(pointer..).ok_or(PageError::CorruptData) {
                    Ok(page_data) => page_data.try_into().map_err(|_| PageError::CorruptData),
                    Err(err) => Err(err),
                },
            )
    }

    #[instrument(parent = None, skip(self), level = "trace")]
    pub fn children_iter(&self) -> impl Iterator<Item = Result<u32, PageError>> + '_ {
        self.cells_iter()
            .map(|cell_result| match cell_result {
                Ok(cell) => Ok(cell.left_child),
                Err(err) => Err(err),
            })
            .chain(std::iter::once(Ok(self.header.right_pointer)))
            .filter_map(|child_num| match child_num {
                Ok(0) => None,
                Ok(cell_num) => Some(Ok(cell_num)),
                Err(err) => Some(Err(err)),
            })
    }

    #[instrument(parent = None, skip(self), ret, level = "trace")]
    pub fn rows_iter(&self) -> impl Iterator<Item = Result<Row, PageError>> + '_ {
        self.cell_pointer_array
            .iter()
            .map(|pointer| {
                match self
                    .data
                    .get(*pointer as usize..)
                    .ok_or(PageError::CorruptData)
                {
                    Ok(page_data) => page_data.try_into().map_err(|_| PageError::CorruptData),
                    Err(err) => Err(err),
                }
            })
            .map(|cell_result: Result<DBCell, PageError>| match cell_result {
                Ok(cell) => Row::try_from(&*cell.payload).map_err(|_| PageError::CorruptData),
                Err(err) => Err(err),
            })
    }
}

impl Default for Page {
    fn default() -> Self {
        Page::new(PageType::Leaf)
    }
}

impl From<Page> for [u8; PAGE_SIZE] {
    fn from(page: Page) -> Self {
        page.data
    }
}

impl fmt::Display for Page {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let keys = self.get_keys();
        if *self.get_page_type() == PageType::Interior {
            write!(f, "(Interior): [{:?}]", keys)
        } else {
            write!(f, "(Leaf): [{:?}]", keys)
        }
    }
}
