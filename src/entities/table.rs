#![allow(dead_code)]
use std::fs::File;
use std::path::Path;

use thiserror::Error;

use std::io::Write;

use super::columns::*;
use super::page::{Page, PageError, PAGE_SIZE};
use super::row::Row;

const TABLE_MAX_PAGES: usize = 100;

#[derive(Debug)]
pub struct Table {
    name: String,
    columns: Columns,
    num_rows: usize,
    pages: [Option<Page>; TABLE_MAX_PAGES],
    curr_page_idx: usize,
}

#[derive(Error, Debug)]
pub enum TableError {
    #[error("Cannot insert row. Pages limit was reached.")]
    TableFull,
    #[error("Could not insert row in page. The following error ocurred during insertion: {0}")]
    PageRowInsertError(String),
    #[error("Could not write table to disk. The following error occurred during write: {0}")]
    WriteToDiskError(String),
    #[error("Could not read table from disk. The following error occurred during read: {0}")]
    ReadFromDiskError(String),
    #[error(
        "The slice being deserialized does not correspond to a table page. End of the slice
                reached during deserialization"
    )]
    EndOfSliceWhileDeserializing,
    #[error("The following error ocurred during deserialization: {0}")]
    SerializationError(String),
    #[error("The following error ocurred during deserialization: {0}")]
    DeserializationError(String),
}

impl Table {
    const BINCODE_CONFIG: bincode::config::Configuration = bincode::config::standard();

    pub fn new(name: &str, columns: Columns) -> Self {
        const INIT_NONE: Option<Page> = None;
        let mut pages_array: [Option<Page>; TABLE_MAX_PAGES] = [INIT_NONE; TABLE_MAX_PAGES];
        pages_array[0] = Some(Page::new());

        Table {
            name: name.to_string(),
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
                        .map_err(|err| TableError::PageRowInsertError(err.to_string()))?;
                    self.pages[self.curr_page_idx] = Some(new_page);
                }
                Err(other_err) => {
                    return Err(TableError::PageRowInsertError(other_err.to_string()))
                }
                Ok(_) => (),
            }
        } else {
            return Err(TableError::TableFull);
        }

        self.num_rows += 1;
        Ok(())
    }

    pub fn free(&mut self) {
        *self = Self::new(&self.name, self.columns.clone());
    }

    pub fn serialize(&self) -> Result<Vec<u8>, TableError> {
        const ROW_NUM_SLOT_SIZE: usize = 2;
        const NAME_SLOT_SIZE_SIZE: usize = 2;
        const COLS_SLOT_SIZE_SIZE: usize = 2;

        let serialized_name = bincode::encode_to_vec(self.name.to_string(), Self::BINCODE_CONFIG)
            .map_err(|err| TableError::SerializationError(err.to_string()))?;

        let name_slot_size = serialized_name.len();

        let serialized_cols = self
            .columns
            .clone()
            .serialize()
            .map_err(|err| TableError::SerializationError(err.to_string()))?;
        let cols_slot_size = serialized_cols.len();

        let num_pages = self.curr_page_idx + 1;

        let mut serialized_table = Vec::with_capacity(
            ROW_NUM_SLOT_SIZE
                + NAME_SLOT_SIZE_SIZE
                + name_slot_size
                + COLS_SLOT_SIZE_SIZE
                + cols_slot_size
                + PAGE_SIZE * num_pages,
        );

        // Insert 2 bytes with number of rows
        serialized_table.extend_from_slice(&(self.num_rows as u16).to_be_bytes());

        // Insert serialized name size and content
        serialized_table.extend_from_slice(&(name_slot_size as u16).to_be_bytes());
        serialized_table.extend_from_slice(&serialized_name);

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
        const NAME_SLOT_SIZE_SIZE: usize = 2;
        const COLS_SLOT_SIZE_SIZE: usize = 2;

        // Extract number of rows
        let mut num_rows_bytes: [u8; NUM_ROWS_SLOT_SIZE] = [0; NUM_ROWS_SLOT_SIZE];
        num_rows_bytes.copy_from_slice(
            bytes
                .get(0..NUM_ROWS_SLOT_SIZE)
                .ok_or(TableError::EndOfSliceWhileDeserializing)?,
        );
        let num_rows: u16 = u16::from_be_bytes(num_rows_bytes);

        // Extract name
        let offset = NUM_ROWS_SLOT_SIZE;
        let mut name_size_bytes: [u8; NAME_SLOT_SIZE_SIZE] = [0; NAME_SLOT_SIZE_SIZE];
        name_size_bytes.copy_from_slice(
            bytes
                .get(offset..offset + NAME_SLOT_SIZE_SIZE)
                .ok_or(TableError::EndOfSliceWhileDeserializing)?,
        );
        let name_size: u16 = u16::from_be_bytes(name_size_bytes);

        let offset = offset + NAME_SLOT_SIZE_SIZE;
        let (name_deserialized, _): (String, usize) = bincode::decode_from_slice(
            &bytes[offset..offset + name_size as usize],
            Self::BINCODE_CONFIG,
        )
        .map_err(|err| TableError::DeserializationError(err.to_string()))?;

        // Extract columns
        let offset = offset + name_size as usize;
        let mut col_size_bytes: [u8; COLS_SLOT_SIZE_SIZE] = [0; COLS_SLOT_SIZE_SIZE];
        col_size_bytes.copy_from_slice(
            bytes
                .get(offset..offset + COLS_SLOT_SIZE_SIZE)
                .ok_or(TableError::EndOfSliceWhileDeserializing)?,
        );
        let col_size: u16 = u16::from_be_bytes(col_size_bytes);

        let offset = offset + COLS_SLOT_SIZE_SIZE;
        let columns_deserialized = Columns::deserialize(&bytes[offset..offset + col_size as usize])
            .map_err(|err| TableError::DeserializationError(err.to_string()))?;

        let mut table = Table::new(&name_deserialized, columns_deserialized);
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
                    .map_err(|err| TableError::DeserializationError(err.to_string()))?,
            );
        }
        Ok(rows)
    }

    pub fn save_to_disk(mut self, path_str: &str) -> Result<(), TableError> {
        let path = Path::new(path_str);

        if let Some(Some(ref mut curr_page)) = self.pages.get_mut(self.curr_page_idx) {
            curr_page.write_row_num();
        }
        let mut file =
            File::create(path).map_err(|err| TableError::WriteToDiskError(err.to_string()))?;

        let serialized = self.serialize()?;

        file.write_all(&serialized)
            .map_err(|err| TableError::WriteToDiskError(err.to_string()))?;

        Ok(())
    }

    pub fn read_from_disk(path_str: &str) -> Result<Self, TableError> {
        let path = Path::new(path_str);
        let data =
            std::fs::read(path).map_err(|err| TableError::ReadFromDiskError(err.to_string()))?;

        let table = Table::deserialize(&data)?;

        Ok(table)
    }
}
