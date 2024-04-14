#![allow(dead_code)]
use core::fmt::Display;
use std::error::Error;
use std::fs::File;
use std::path::Path;

use std::io::Write;

use super::columns::*;
use super::page::{Page, PageError, PAGE_SIZE};
use super::row::Row;

const TABLE_MAX_PAGES: usize = 100;

#[derive(Debug)]
pub struct Table {
    columns: Columns,
    num_rows: usize,
    pages: [Option<Page>; TABLE_MAX_PAGES],
    curr_page_idx: usize,
}

#[derive(Debug)]
pub enum TableError {
    TableFull,
    EndOfSliceWhileDeserializing,
    PageRowInsertError(Box<dyn Error>),
    WriteToDiskError(Box<dyn Error>),
    ReadFromDiskError(Box<dyn Error>),
    PageDeserializingError(Box<dyn Error>),
    ColumnsSerializingError(Box<dyn Error>),
    ColumnsDeserializingError(Box<dyn Error>),
}

impl Display for TableError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TableError::TableFull => write!(f, "Cannot insert row. Pages limit was reached."),
            TableError::PageRowInsertError(inner_error) => {
                write!(f,
                "Could not insert row in page. The following error ocurred during insertion: {}",
                inner_error
                )
            }
            TableError::WriteToDiskError(inner_error) => {
                write!(
                    f,
                    "Could not write table to disk. The following error occurred during write: {}",
                    inner_error
                )
            }
            TableError::ReadFromDiskError(inner_error) => {
                write!(
                    f,
                    "Could not read table from disk. The following error occurred during read: {}",
                    inner_error
                )
            }
            TableError::PageDeserializingError(inner_error) => write!(
                f,
                "Error when deserializing page. The following error ocurred during deserialization: {}",
                inner_error
            ),
            TableError::ColumnsSerializingError(inner_error) => write!(
                f,
                "Error when serializing columns. The following error ocurred during serialization: {}",
                inner_error
            ),
            TableError::ColumnsDeserializingError(inner_error) => write!(
                f,
                "Error when deserializing columns. The following error ocurred during deserialization: {}",
                inner_error
            ),
            TableError::EndOfSliceWhileDeserializing => write!(
                f,
                "The slice being deserialized does not correspond to a table page. End of the slice
                reached during deserialization"
            ),
        }
    }
}

impl Table {
    const BINCODE_CONFIG: bincode::config::Configuration = bincode::config::standard();

    pub fn new(columns: Columns) -> Self {
        const INIT_NONE: Option<Page> = None;
        let mut pages_array: [Option<Page>; TABLE_MAX_PAGES] = [INIT_NONE; TABLE_MAX_PAGES];
        pages_array[0] = Some(Page::new());

        Table {
            columns,
            pages: pages_array,
            curr_page_idx: 0,
            num_rows: 0,
        }
    }

    pub fn insert(&mut self, row: Row) -> Result<(), TableError> {
        // First insertion attempt can fail due to current page being full
        let row_backup = row.clone();

        if let Some(Some(ref mut curr_page)) = self.pages.get_mut(self.curr_page_idx) {
            match curr_page.insert(row) {
                Err(PageError::PageFull) => {
                    curr_page.write_row_num();
                    self.curr_page_idx += 1;
                    if self.curr_page_idx >= TABLE_MAX_PAGES {
                        return Err(TableError::TableFull);
                    };
                    let mut new_page = Page::new();
                    new_page
                        .insert(row_backup)
                        .map_err(|err| TableError::PageRowInsertError(Box::new(err)))?;
                    self.pages[self.curr_page_idx] = Some(new_page);
                }
                Err(other_err) => return Err(TableError::PageRowInsertError(Box::new(other_err))),
                Ok(_) => (),
            }
        }

        self.num_rows += 1;
        Ok(())
    }

    pub fn free(&mut self) {
        *self = Self::new(self.columns.clone());
    }

    pub fn serialize(&self) -> Result<Vec<u8>, TableError> {
        const NUM_ROWS_SLOT_SIZE: usize = 2;

        let serialized_cols = self
            .columns
            .clone()
            .serialize()
            .map_err(|err| TableError::ColumnsSerializingError(Box::new(err)))?;
        let cols_slot_size = serialized_cols.len();

        let num_pages = self.curr_page_idx + 1;
        // Create vec of size num_pages + 2 bytes to store number of rows + size of serialized
        // columns
        let mut serialized_table =
            Vec::with_capacity(NUM_ROWS_SLOT_SIZE + cols_slot_size + PAGE_SIZE * num_pages);

        // Insert 2 bytes with number of rows
        serialized_table.extend_from_slice(&(self.num_rows as u16).to_be_bytes());

        // Insert serialized columns size and content
        serialized_table.extend_from_slice(&(cols_slot_size as u16).to_be_bytes());
        serialized_table.extend_from_slice(&serialized_cols);

        // Insert every non-None pages into serialized vector
        for page in self.pages.iter().filter_map(|p| p.as_ref()) {
            serialized_table.extend_from_slice(page.get_data());
        }

        Ok(serialized_table)
    }

    pub fn deserialize(bytes: &[u8]) -> Result<Self, TableError> {
        const NUM_ROWS_SLOT_SIZE: usize = 2;
        const COLS_SLOT_SIZE_SIZE: usize = 2;
        // Extract number of rows
        let mut num_rows_bytes: [u8; 2] = [0; NUM_ROWS_SLOT_SIZE];
        num_rows_bytes.copy_from_slice(
            bytes
                .get(0..NUM_ROWS_SLOT_SIZE)
                .ok_or_else(|| TableError::EndOfSliceWhileDeserializing)?,
        );
        let num_rows: u16 = u16::from_be_bytes(num_rows_bytes);

        // Extract columns
        let offset = NUM_ROWS_SLOT_SIZE;
        let mut col_size_bytes: [u8; 2] = [0; 2];
        col_size_bytes.copy_from_slice(
            bytes
                .get(offset..offset + COLS_SLOT_SIZE_SIZE)
                .ok_or_else(|| TableError::EndOfSliceWhileDeserializing)?,
        );
        let col_size: u16 = u16::from_be_bytes(col_size_bytes);

        let offset = offset + COLS_SLOT_SIZE_SIZE;
        let columns_deserialized = Columns::deserialize(&bytes[offset..offset + col_size as usize])
            .map_err(|err| TableError::ColumnsDeserializingError(Box::new(err)))?;

        let mut table = Table::new(columns_deserialized);
        table.num_rows = num_rows as usize;

        // Extract pages
        let offset = offset + col_size as usize;
        let mut page_insert_idx = 0;
        for page_bytes in bytes[offset..].chunks(PAGE_SIZE) {
            table.pages[page_insert_idx] = Some(Page::from_slice(page_bytes));
            page_insert_idx += 1;
        }
        table.curr_page_idx = page_insert_idx - 1;

        Ok(table)
    }

    pub fn deserialize_rows(&self) -> Result<Vec<Row>, TableError> {
        let mut rows: Vec<Row> = Vec::new();
        for page in self.pages.iter().filter_map(|p| p.as_ref()) {
            rows.append(
                &mut page
                    .deserialize_rows()
                    .map_err(|err| TableError::PageDeserializingError(Box::new(err)))?,
            );
        }
        Ok(rows)
    }

    pub fn save_to_disk(mut self, path: &Path) -> Result<(), TableError> {
        if let Some(Some(ref mut curr_page)) = self.pages.get_mut(self.curr_page_idx) {
            curr_page.write_row_num();
        }
        let mut file =
            File::create(path).map_err(|err| TableError::WriteToDiskError(Box::new(err)))?;

        let serialized = self.serialize()?;

        file.write_all(&serialized)
            .map_err(|err| TableError::WriteToDiskError(Box::new(err)))?;

        Ok(())
    }

    pub fn read_from_disk(path: &Path) -> Result<Self, TableError> {
        let data =
            std::fs::read(path).map_err(|err| TableError::ReadFromDiskError(Box::new(err)))?;

        let table = Table::deserialize(&data)?;

        Ok(table)
    }
}
