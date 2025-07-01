#![allow(dead_code)]
use std::fmt;
use std::io::Read;
use std::mem::{self, MaybeUninit};
use std::ops::{Deref, DerefMut};
use std::rc::Rc;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::instrument;

use super::db_cell::DBCell;
use super::pager::TABLE_MAX_PAGES;
use super::row::Row;

pub const PAGE_SIZE: usize = 4096;
const PAGE_HEADER_SIZE: usize = mem::size_of::<PageHeader>();
pub const INVALID_PAGE_NUM: u32 = TABLE_MAX_PAGES as u32;

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize)]
/// Struct representing the page's header, with several methods to write to the page's header
/// fields in the page's byte array.
///
/// * `page_type`: One-byte flag at offset 0 indicating the b-tree page type:
///
///                - Leaf: 0x0d
///                - Interior: 0x05
/// * `first_free_block`: Two-byte integer at offset 1 gives the start of the first freeblock on
///                       the page, or is zero if there are no freeblocks (UNUSED).
/// * `num_cells`: Two-byte integer at offset 3 gives the number of cells on the page.
/// * `cells_start`: Two-byte integer at offset 5 designates the start of the cell content area. A
///                  zero value for this integer is interpreted as 65536.
/// * `fragmented_free_bytes`: one-byte integer at offset 7 gives the number of fragmented free
///                            bytes within the cell content area. (UNUSED)
/// * `right_pointer`: The four-byte page number at offset 8 is the right-most pointer. This value
///                    is only used for interior pages.
struct PageHeader {
    page_type: PageType,
    first_free_block: u16,
    num_cells: u16,
    cells_start: u16,
    fragmented_free_bytes: u8,
    right_pointer: u32,
}

impl PageHeader {
    fn read_from_bytes(header_slice: &[u8]) -> Result<Self, PageError> {
        let mut header = Self::default();
        header.read_page_type(header_slice)?;
        header.read_first_free_block(header_slice)?;
        header.read_num_cells(header_slice)?;
        header.read_cells_start(header_slice)?;
        header.read_fragmented_free_bytes(header_slice)?;
        header.read_right_pointer(header_slice)?;
        Ok(header)
    }

    fn set_page_type(&mut self, val: PageType, header_slice: &mut [u8]) {
        self.page_type = val;
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

    fn read_page_type(&mut self, header_slice: &[u8]) -> Result<(), PageError> {
        self.page_type = header_slice
            .get(0)
            .ok_or(PageError::HeaderReadError)?
            .try_into()?;
        Ok(())
    }

    fn read_first_free_block(&mut self, header_slice: &[u8]) -> Result<(), PageError> {
        self.first_free_block = u16::from_be_bytes([
            *header_slice.get(1).ok_or(PageError::HeaderReadError)?,
            *header_slice.get(2).ok_or(PageError::HeaderReadError)?,
        ]);
        Ok(())
    }

    fn read_num_cells(&mut self, header_slice: &[u8]) -> Result<(), PageError> {
        self.num_cells = u16::from_be_bytes([
            *header_slice.get(3).ok_or(PageError::HeaderReadError)?,
            *header_slice.get(4).ok_or(PageError::HeaderReadError)?,
        ]);
        Ok(())
    }

    fn read_cells_start(&mut self, header_slice: &[u8]) -> Result<(), PageError> {
        self.cells_start = u16::from_be_bytes([
            *header_slice.get(5).ok_or(PageError::HeaderReadError)?,
            *header_slice.get(6).ok_or(PageError::HeaderReadError)?,
        ]);
        Ok(())
    }

    fn read_fragmented_free_bytes(&mut self, header_slice: &[u8]) -> Result<(), PageError> {
        self.fragmented_free_bytes = *header_slice.get(7).ok_or(PageError::HeaderReadError)?;
        Ok(())
    }

    fn read_right_pointer(&mut self, header_slice: &[u8]) -> Result<(), PageError> {
        self.right_pointer = u32::from_be_bytes([
            *header_slice.get(8).ok_or(PageError::HeaderReadError)?,
            *header_slice.get(9).ok_or(PageError::HeaderReadError)?,
            *header_slice.get(10).ok_or(PageError::HeaderReadError)?,
            *header_slice.get(11).ok_or(PageError::HeaderReadError)?,
        ]);
        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
/// `Vec<u16>` wrapper struct to read and write from a [`Page`] cell pointer array.
struct CellPtrArray(Vec<u16>);

impl CellPtrArray {
    const PTR_BYTE_SIZE: usize = 2;

    #[instrument(parent = None, ret, level = "trace")]
    fn read_from_bytes(num_cells: usize, cell_ptr_array_start: &[u8]) -> Result<Self, PageError> {
        let cell_ptr_array_size = num_cells * Self::PTR_BYTE_SIZE;
        let cell_ptr_array = &cell_ptr_array_start
            .get(..cell_ptr_array_size)
            .ok_or(PageError::CellPtrArrayReadError)?;

        Ok(Self(
            cell_ptr_array
                .chunks_exact(2)
                .map(|chunk| u16::from_be_bytes([chunk[0], chunk[1]]))
                .collect(),
        ))
    }

    #[instrument(parent = None, ret, level = "trace")]
    fn update_pointer_array_after_insert(
        &mut self,
        cell_ptr_array_start: &mut [u8],
        new_cell_ptr: u16,
        partition: usize,
    ) {
        self.insert(partition, new_cell_ptr);
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
    fn deref_mut(&mut self) -> &mut Vec<u16> {
        &mut self.0
    }
}

impl Deref for CellPtrArray {
    type Target = Vec<u16>;

    fn deref(&self) -> &Vec<u16> {
        &self.0
    }
}

impl From<&[u16]> for CellPtrArray {
    fn from(data: &[u16]) -> Self {
        let cell_contents: Vec<u16> = data.into();
        Self(cell_contents)
    }
}

/// Implementation of B+tree nodes containing N variable-size database entries.
/// ``` text
///      |----------------|
///      | page header    |   8 bytes for leaves.  12 bytes for interior nodes
///      |----------------|
///      | cell pointer   |   |  2 bytes per cell.  Sorted order.
///      | array          |   |  Grows downward
///      |                |   v
///      |----------------|
///      | unallocated    |
///      | space          |
///      |----------------|   ^  Grows upwards
///      | cell content   |   |  Arbitrary order
///      | area           |
///      |----------------|
/// ```
///
/// * `header`: Data structure managing the page's header.
/// * `data`: Serialized page data as an array of bytes.
/// * `cell_pointer_array`: Data structure managing the page's cell pointer array.
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
    #[error("Cannot insert record with key {0}. Record with the same key already exists.")]
    DuplicateKey(u64),
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
    #[error("Invalid page type {0:#04x}")]
    InvalidPageType(u8),
    #[error("Error while reading page header from reader.")]
    HeaderReadError,
    #[error("Error while reading page cell pointer array from reader.")]
    CellPtrArrayReadError,
    #[error("Error while getting page bytes from reader: {0}")]
    ReaderError(String),
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

impl TryFrom<&u8> for PageType {
    type Error = PageError;

    fn try_from(value: &u8) -> Result<Self, PageError> {
        match value {
            0x0d => Ok(PageType::Leaf),
            0x05 => Ok(PageType::Interior),
            _ => Err(PageError::InvalidPageType(*value)),
        }
    }
}

impl Page {
    const BINCODE_CONFIG: bincode::config::Configuration = bincode::config::standard();
    const CELL_PTR_BYTE_SIZE: usize = 2;

    fn create_uninit_page_bytes() -> [u8; PAGE_SIZE] {
        /* For performance reasons, a page is initialized as an empty array.
        It is the responsibility of the implementation to read and write the data properly.
        */
        let uninitialized_array: [MaybeUninit<u8>; PAGE_SIZE] =
            unsafe { MaybeUninit::uninit().assume_init() };

        unsafe {
            mem::transmute::<[std::mem::MaybeUninit<u8>; PAGE_SIZE], [u8; PAGE_SIZE]>(
                uninitialized_array,
            )
        }
    }

    #[instrument(parent = None, level = "trace")]
    pub fn new(page_type: PageType) -> Self {
        let mut uninitialized_array = Self::create_uninit_page_bytes();

        let mut header = PageHeader::default();
        header.set_page_type(page_type, &mut uninitialized_array);
        header.set_cells_start(PAGE_SIZE as u16, &mut uninitialized_array);
        header.set_right_pointer(INVALID_PAGE_NUM, &mut uninitialized_array);

        Self {
            header,
            data: uninitialized_array,
            cell_pointer_array: CellPtrArray::default(),
        }
    }

    #[instrument(parent = None, level = "trace")]
    /// Creates a page from a Read object i.e. a [File][std::fs::File] instance.
    ///
    /// * `read`: Instance of a type that implements the [Read][std::io::Read] trait.
    pub fn new_from_read<T>(read: &mut T) -> Result<Self, PageError>
    where
        T: Read + std::fmt::Debug,
    {
        let mut page_bytes = Self::create_uninit_page_bytes();
        read.read_exact(&mut page_bytes)
            .map_err(|err| PageError::ReaderError(err.to_string()))?;

        let header = PageHeader::read_from_bytes(&page_bytes[..PAGE_HEADER_SIZE])?;
        let cell_pointer_array = CellPtrArray::read_from_bytes(
            header.num_cells.into(),
            &page_bytes[PAGE_HEADER_SIZE..],
        )?;

        Ok(Self {
            header,
            data: page_bytes,
            cell_pointer_array,
        })
    }

    #[instrument(parent = None, skip(self), ret, level = "trace")]
    pub fn get_page_type(&self) -> &PageType {
        &self.header.page_type
    }

    #[instrument(parent = None, skip(self), ret, level = "trace")]
    /// Get a Vec with all the keys of the records in the page.
    fn get_keys(&self) -> Result<Vec<u64>, PageError> {
        let keys_res_iter = self
            .cell_pointer_array
            .iter()
            .map(|&cell_ptr| DBCell::id_from_slice(&self.data[cell_ptr as usize..]));
        let keys_res: Result<Vec<u64>, ()> = keys_res_iter.collect();
        keys_res.map_err(|_| PageError::CorruptData)
    }

    #[instrument(parent = None, skip(self), ret, level = "trace")]
    /// Retrieve key of first record in the page
    pub fn get_first_key(&self) -> Result<u64, PageError> {
        if self.cell_pointer_array.len() == 0 {
            Err(PageError::CannotGetFirstOrLastKey)?
        }
        let first_pointer = self.cell_pointer_array[0];
        let key = DBCell::id_from_slice(&self.data[first_pointer as usize..]);
        key.map_err(|_| PageError::CorruptData)
    }

    #[instrument(parent = None, skip(self), ret, level = "trace")]
    /// Retrieve key of last record in the page
    pub fn get_last_key(&self) -> Result<u64, PageError> {
        if self.cell_pointer_array.len() == 0 {
            Err(PageError::CannotGetFirstOrLastKey)?
        }
        let last_pointer = self.cell_pointer_array[self.cell_pointer_array.len() - 1];
        let key = DBCell::id_from_slice(&self.data[last_pointer as usize..]);
        key.map_err(|_| PageError::CorruptData)
    }

    #[instrument(parent = None, skip(self), ret, level = "trace")]
    /// When performing a B-Tree traversal looking for a record, return the page number of the next
    /// page that leads to the searched record.
    ///
    /// * `key`: Key of the record to find
    pub fn get_next_page_pointer(&self, key: u64) -> Result<u32, PageError> {
        let (partition, partition_key) = self.find_partition(key)?;
        if partition_key.is_none() {
            return Ok(self.header.right_pointer);
        }
        let next_page_cell: DBCell = self.data[partition..]
            .try_into()
            .map_err(|_| PageError::CorruptData)?;
        Ok(next_page_cell.left_child)
    }

    #[instrument(parent = None, skip(self), ret, level = "trace")]
    /// Removes the last record of an interior page and sets the page's right pointer to the
    /// record's left child
    /// Pre:
    /// ``` text
    ///         ┌─────────┌┐
    ///         │ 0 3 6 9 ││
    ///         └─────┬─┬─└┘
    ///               │ │
    ///     ┌─────────┘ └─────┐
    /// ┌───▼───┌┐        ┌───▼───┌┐
    /// │ 4 5 6 ││        │ 7 8 9 ││
    /// └───────└┘        └───────└┘
    /// ```
    /// Post:
    /// ``` text
    ///         ┌───────┌┐
    ///         │ 0 3 6 ││────┐
    ///         └─────┬─└┘    │
    ///               │       │
    ///     ┌─────────┘       │
    /// ┌───▼───┌┐        ┌───▼───┌┐
    /// │ 4 5 6 ││        │ 7 8 9 ││
    /// └───────└┘        └───────└┘
    /// ```
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
    /// Find partition in the sorted list of keys in which the specified key would be inserted.
    /// Returns the respective partition point, as well as the current record in that partition. If
    /// the partition point is at the very end, then None is returned instead of the current
    /// record.
    ///
    /// * `new_key`: Key of which we want to find its partition.
    pub fn find_partition(&self, new_key: u64) -> Result<(usize, Option<u64>), PageError> {
        let keys = self.get_keys()?;
        let partition_key = keys.partition_point(|&key| key < new_key);

        if partition_key >= keys.len() {
            return Ok((keys.len(), None));
        }

        let curr_key_in_partition = keys[partition_key];

        Ok((partition_key, Some(curr_key_in_partition)))
    }

    #[instrument(parent = None, skip(self), ret, level = "trace")]
    /// Insert record in page in order according to its key
    ///
    /// * `key`: Key of the record to insert.
    /// * `payload`: Payload to be inserted in the page.
    /// * `left_child`: Optional left child pointer of the record. If left_child is None, the
    /// pointer will be set to [INVALID_PAGE_NUM][crate::backend::db_cell::INVALID_PAGE_NUM].
    pub fn insert(
        &mut self,
        key: u64,
        payload: &[u8],
        left_child: Option<u32>,
    ) -> Result<(), PageError> {
        let (partition, partition_key_opt) = self.find_partition(key)?;
        if let Some(partition_key) = partition_key_opt {
            if partition_key == key {
                return Err(PageError::DuplicateKey(key));
            }
        }
        // Check if page has enough space
        let cell_bytes: Rc<[u8]> = DBCell::new(key, payload, left_child)
            .map_err(|_| PageError::InsertError)?
            .try_into()
            .map_err(|_| PageError::InsertError)?;
        let old_cells_start = self.header.cells_start as usize;
        let insert_position = old_cells_start - cell_bytes.len();
        let end_of_ptr_array_after_insert =
            PAGE_HEADER_SIZE + ((self.header.num_cells as usize) + 1) * Self::CELL_PTR_BYTE_SIZE;

        if insert_position <= end_of_ptr_array_after_insert {
            self.header
                .set_cells_start(0, &mut self.data[..PAGE_HEADER_SIZE]);
            Err(PageError::PageFull)?
        }

        // ------------------ Insert data into slot ------------------
        self.data[insert_position..old_cells_start].copy_from_slice(&cell_bytes);

        // Update header
        self.header
            .set_cells_start(insert_position as u16, &mut self.data[..PAGE_HEADER_SIZE]);
        self.header.set_num_cells(
            self.header.num_cells + 1,
            &mut self.data[..PAGE_HEADER_SIZE],
        );

        // Update cell pointer array
        self.cell_pointer_array.update_pointer_array_after_insert(
            &mut self.data[PAGE_HEADER_SIZE..],
            insert_position as u16,
            partition,
        );

        Ok(())
    }

    #[instrument(parent = None, skip(self), ret, level = "trace")]
    /// Deletes record from page at the specified position
    ///
    /// * `cell_ptr_pos`: The byte offset in the page that corresponds to the record
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
                .set_cells_start(new_cells_start, &mut self.data[..PAGE_HEADER_SIZE]);
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
    /// Updates a record in place, as long as the size of the old record matches the size of the
    /// new record.
    ///
    /// * `key_index`: Index of the cell pointer that points to the record to update
    /// * `key`: Key of the record to update
    /// * `payload`: New payload of the record to update
    /// * `left_child`: New left child of the record to update
    pub fn update_same_size(
        &mut self,
        key_index: usize,
        key: u64,
        payload: &[u8],
        left_child: Option<u32>,
    ) -> Result<(), PageError> {
        let partition_point = self.cell_pointer_array[key_index] as usize;
        let cell_to_modify: DBCell = self.data[partition_point..]
            .try_into()
            .map_err(|_| PageError::CorruptData)?;

        if cell_to_modify.key != key {
            return Err(PageError::UpdateNotSameKey(key));
        }

        if cell_to_modify.payload_size != payload.len() as u16 {
            return Err(PageError::UpdateNotSameSize);
        }

        let new_payload_bytes: Rc<[u8]> = DBCell::new(key, payload, left_child)
            .map_err(|_| PageError::InsertError)?
            .try_into()
            .map_err(|_| PageError::InsertError)?;

        self.data[partition_point..].copy_from_slice(&new_payload_bytes);

        Ok(())
    }

    #[instrument(parent = None, skip(self), ret, level = "trace")]
    /// Splits the current page into two halves, consuming it in the process.
    pub fn split_page(self) -> Result<(Self, Self), PageError> {
        let num_cells = self.cell_pointer_array.len();
        let curr_right_pointer = self.header.right_pointer;

        let mut left_split = Self::new(*self.get_page_type());
        let mut right_split = Self::new(*self.get_page_type());

        let cells_iter = self.cells_iter().flatten();

        for (i, cell) in cells_iter.enumerate() {
            if i <= num_cells / 2 {
                left_split.insert(cell.key, &cell.payload, Some(cell.left_child))?;
            } else {
                right_split.insert(cell.key, &cell.payload, Some(cell.left_child))?;
            }
        }

        right_split.set_right_pointer(curr_right_pointer);

        Ok((left_split, right_split))
    }

    #[instrument(parent = None, skip(self), ret, level = "trace")]
    /// Writes to the right pointer filed in the page's header
    pub fn set_right_pointer(&mut self, right_pointer: u32) {
        self.header
            .set_right_pointer(right_pointer, &mut self.data[..PAGE_HEADER_SIZE]);
    }

    #[instrument(parent = None, skip(self), level = "trace")]
    /// Returns an iterator of [`DBCell`] with the cells in the current page.
    fn cells_iter(&self) -> impl Iterator<Item = Result<DBCell, PageError>> + '_ {
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
    /// Returns an iterator with the pointers to the page's children.
    pub fn children_iter(&self) -> impl Iterator<Item = Result<u32, PageError>> + '_ {
        self.cells_iter()
            .map(|cell_result| match cell_result {
                Ok(cell) => Ok(cell.left_child),
                Err(err) => Err(err),
            })
            .chain(std::iter::once(Ok(self.header.right_pointer)))
            .filter_map(|child_num| match child_num {
                Ok(INVALID_PAGE_NUM) => None,
                Ok(cell_num) => Some(Ok(cell_num)),
                Err(err) => Some(Err(err)),
            })
    }

    #[instrument(parent = None, skip(self), ret, level = "trace")]
    /// Returns an iterator of all the cells in the page deserialized
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
