#![allow(dead_code)]
use core::fmt::Display;
use std::error::Error;
use std::path::Path;
use std::fs::File;

use super::page::*;
use super::row::Row;

use serde::{Deserialize, Serialize};
use serde_big_array::BigArray;

const TABLE_MAX_PAGES: usize = 100;

#[derive(Debug, Serialize, Deserialize)]
pub struct Table {
    #[serde(with = "BigArray")]
    pages: [Option<Page>; TABLE_MAX_PAGES],
    #[serde(skip_serializing)]
    curr_page_idx: usize,
    num_rows: usize,
}

#[derive(Debug)]
pub enum TableError {
    TableFull,
    PageRowInsertError(Box<dyn Error>),
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
        }
    }
}

impl Table {
    const BINCODE_CONFIG: bincode::config::Configuration = bincode::config::standard();

    pub fn new() -> Self {
        const INIT_NONE: Option<Page> = None;
        let mut pages_array: [Option<Page>; TABLE_MAX_PAGES] = [INIT_NONE; TABLE_MAX_PAGES];
        pages_array[0] = Some(Page::new());

        Table {
            pages: pages_array,
            curr_page_idx: 0,
            num_rows: 0,
        }
    }

    pub fn insert(&mut self, row: Row) -> Result<(), TableError> {
        let row_backup = row.clone();

        if let Some(Some(ref mut curr_page)) = self.pages.get_mut(self.curr_page_idx) {
            match curr_page.insert(row) {
                Err(PageError::PageFull) => {
                    curr_page.complete();
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
        } else {
        }
        self.num_rows += 1;
        Ok(())
    }

    pub fn free(&mut self) {
        *self = Self::new();
    }

    pub fn save_to_disk(self, path: &Path) -> Result<(), Box<dyn Error>> {
        let mut file = File::create_new(path)?;
        bincode::serde::encode_into_std_write::<Self, _, File>(self, &mut file, Self::BINCODE_CONFIG)?;
        Ok(())
    }
}
