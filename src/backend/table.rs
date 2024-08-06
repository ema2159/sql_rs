#![allow(dead_code)]
use std::collections::HashMap;
use std::sync::Mutex;

use lazy_static::lazy_static;
use thiserror::Error;

use super::columns::*;
use super::pager::{Pager, PagerError};
use super::row::Row;

// HACK: This is a temporary global varialble in place of the sqlite_master table
lazy_static! {
    pub static ref COLUMNS: Mutex<HashMap<String, Columns>> = Mutex::new(HashMap::new());
}

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
    #[error("Pager error when opening connection: {0}")]
    PagerError(String),
    #[error("Table {0} does not exist")]
    TableDoesNotExist(String),
}

impl Table {
    const BINCODE_CONFIG: bincode::config::Configuration = bincode::config::standard();

    pub fn db_open(name: &str) -> Result<Self, TableError> {
        let pager = Pager::open(&(name.to_owned() + ".db"))
            .map_err(|err| TableError::PagerError(err.to_string()))?;

        let columns_map = COLUMNS.lock().unwrap();
        let columns = columns_map
            .get(name)
            .ok_or(TableError::TableDoesNotExist(name.to_string()))?
            .clone();

        Ok(Table {
            name: name.to_string(),
            pager,
            columns,
            curr_page_idx: 0,
            num_rows: 0,
        })
    }

    pub fn create(name: &str, columns: Columns) {
        let mut columns_map = COLUMNS.lock().unwrap();
        columns_map.insert(name.to_owned(), columns);
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
            Err(other_err) => Err(TableError::PagerError(other_err.to_string())),
        }
    }

    pub fn deserialize_rows(&self) -> Result<Vec<Row>, TableError> {
        let mut rows: Vec<Row> = Vec::new();
        for page in self.pager.pages_cache.iter().filter_map(|p| p.as_ref()) {
            println!("{:?}", page);
            rows.append(
                &mut page
                    .deserialize_rows()
                    .map_err(|err| TableError::PagerError(err.to_string()))?,
            );
        }
        Ok(rows)
    }

    pub fn db_close(mut self) -> Result<(), TableError> {
        self.pager
            .flush_all()
            .map_err(|err| TableError::PagerError(err.to_string()))?;

        Ok(())
    }
}
