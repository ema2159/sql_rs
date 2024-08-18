use std::cell::RefCell;
use std::fs::File;
use std::rc::Rc;

use thiserror::Error;

use super::columns::*;
use super::page::PageError;
use super::pager::{Pager, PagerError};
use super::row::Row;

#[derive(Debug)]
pub struct Table {
    pub name: String,
    pub columns: Columns,
    num_rows: usize,
    pager: Pager,
    curr_page_idx: usize,
}

#[derive(Error, Debug)]
pub enum TableError {
    #[error("Cannot insert row. Pages limit was reached.")]
    TableFull,
    #[error("Error when opening connection: {0}")]
    RowInsertError(PagerError),
    #[error("Error when flushing table to disk: {0}")]
    FlushError(PagerError),
    #[error(transparent)]
    PageError(#[from] PageError),
}

impl Table {
    const BINCODE_CONFIG: bincode::config::Configuration = bincode::config::standard();

    pub fn new(name: &str, columns: Columns, file: Rc<RefCell<File>>) -> Table {
        let pager = Pager::new(file);

        Table {
            name: name.to_string(),
            pager,
            columns,
            curr_page_idx: 0,
            num_rows: 0,
        }
    }

    fn new_page_and_insert(&mut self, row: Row) -> Result<(), TableError> {
        match self.pager.new_page(self.curr_page_idx) {
            Ok(()) => Ok(()),
            Err(PagerError::TableFull) => Err(TableError::TableFull),
            Err(_) => unreachable!(),
        }?;
        self.insert(row)
    }

    pub fn insert(&mut self, row: Row) -> Result<(), TableError> {
        match self.pager.insert(&row, self.curr_page_idx) {
            Ok(()) => Ok(()),
            Err(PagerError::PageFull) => {
                self.curr_page_idx += 1;
                self.new_page_and_insert(row)
            }
            Err(PagerError::CacheMiss) => self.new_page_and_insert(row),
            Err(PagerError::TableFull) => Err(TableError::TableFull),
            Err(other_err) => Err(TableError::RowInsertError(other_err)),
        }
    }

    pub fn deserialize_rows(&self) -> Result<Vec<Row>, TableError> {
        let mut rows: Vec<Row> = Vec::new();
        for page in self.pager.pages().filter_map(|p| p.as_ref()) {
            let deserialized_rows: Result<Vec<Row>, PageError> = page.deserialize_rows();
            rows.append(&mut deserialized_rows?);
        }
        Ok(rows)
    }

    pub fn flush(&mut self) -> Result<(), TableError> {
        self.pager.flush_all().map_err(TableError::FlushError)?;

        Ok(())
    }
}
