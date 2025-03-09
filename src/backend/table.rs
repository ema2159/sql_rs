use std::cell::RefCell;
use std::fmt;
use std::fs::File;
use std::rc::Rc;

use tabled::{builder::Builder, settings::style::Style};
use thiserror::Error;
use tracing::instrument;

use super::columns::*;
use super::cursor::DBCursor;
use super::page::{PageError, PageType};
use super::pager::{Pager, PagerError};
use super::row::Row;

#[derive(Debug)]
pub struct Table {
    pub name: String,
    pub columns: Columns,
    num_rows: usize,
    pager: RefCell<Pager>,
    root_page_num: u32,
}

#[derive(Error, Debug)]
pub enum TableError {
    #[error("Cannot insert row. Pages limit was reached.")]
    TableFull,
    #[error("Error when inserting record: {0}")]
    RowInsertError(PagerError),
    #[error("Error when serializing record")]
    RowSerializeError,
    #[error("Error when flushing table to disk: {0}")]
    FlushError(PagerError),
    #[error(transparent)]
    PageError(#[from] PageError),
}

impl Table {
    #[instrument(parent = None, level = "trace")]
    pub fn new(name: &str, columns: Columns, file: Rc<RefCell<File>>) -> Table {
        let root_page_num = 0;
        let pager = RefCell::new(Pager::new(file, root_page_num));

        Table {
            name: name.to_string(),
            pager,
            columns,
            root_page_num,
            num_rows: 0,
        }
    }

    #[instrument(parent = None, skip(self), ret, level = "trace")]
    pub fn insert(&self, row: Row) -> Result<(), TableError> {
        let row_id = row.rowid();
        let data =
            TryInto::<Box<[u8]>>::try_into(row).map_err(|_| TableError::RowSerializeError)?;
        let mut cursor = DBCursor::new(self);
        self.pager
            .borrow()
            .get_insertion_position(&mut cursor, row_id)
            .map_err(TableError::RowInsertError)?;

        match self.pager.borrow_mut().insert(&mut cursor, row_id, &data) {
            Ok(()) => Ok(()),
            Err(PagerError::TableFull) => Err(TableError::TableFull),
            Err(other_err) => Err(TableError::RowInsertError(other_err)),
        }
    }

    #[instrument(parent = None, skip(self), ret, level = "trace")]
    pub fn deserialize_rows(&self) -> Result<Vec<Row>, TableError> {
        let mut rows: Vec<Row> = Vec::new();
        for page in self.pager.borrow_mut().pages().filter_map(|p| p.as_ref()) {
            if *page.get_page_type() == PageType::Leaf {
                let deserialized_rows: Result<Vec<Row>, PageError> = page.rows_iter().collect();
                rows.append(&mut deserialized_rows?);
            }
        }
        Ok(rows)
    }

    #[instrument(parent = None, skip(self), ret, level = "trace")]
    pub fn flush(&mut self) -> Result<(), TableError> {
        self.pager
            .borrow_mut()
            .flush_all()
            .map_err(TableError::FlushError)?;

        Ok(())
    }
}

impl fmt::Display for Table {
    #[instrument(parent = None, skip(self, f), ret, level = "trace")]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let columns = &self.columns;
        let rows = self.deserialize_rows().unwrap();

        let rows_strings: Vec<_> = rows
            .iter()
            .map(|row| row.to_printable().collect::<Vec<_>>())
            .collect();

        let mut pretty_table_builder = Builder::from(rows_strings);
        pretty_table_builder.insert_record(0, columns.to_printable().collect::<Vec<_>>());

        let mut pretty_table = pretty_table_builder.build();
        pretty_table.with(Style::psql());

        write!(f, "{}", pretty_table)
    }
}
