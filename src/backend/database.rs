use std::cell::RefCell;
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, Seek};
use std::path::Path;
use std::rc::Rc;

use thiserror::Error;

use super::columns::Columns;
use super::table::Table;

#[derive(Debug)]
pub struct Database {
    file: Rc<RefCell<File>>,
    tables: HashMap<String, Table>,
}

#[derive(Error, Debug)]
pub enum DatabaseError {
    #[error("Could not read database from disk. The following error occurred during read: {0}")]
    Io(#[from] io::Error),
    #[error("Table already exists in database.")]
    DuplicateTable,
    #[error("Table does not exist in database.")]
    TableDoesNotExist,
}

impl Database {
    pub fn close(&mut self) {
        for table in self.tables.values_mut() {
            if let Err(err) = table.flush() {
                eprintln!("Error flushing table {} to disk: {}", table.name, err);
            }
        }
    }

    pub fn open(path_str: &str) -> Result<Self, DatabaseError> {
        let path = Path::new(path_str);
        let mut f = File::options().create(true).write(true).truncate(true).open(path)?;
        f.seek(io::SeekFrom::Start(0))?;
        let file = RefCell::new(f).into();

        let tables = HashMap::new();

        Ok(Self { file, tables })
    }

    pub fn add_table(&mut self, table_name: &str, columns: Columns) -> Result<(), DatabaseError> {
        if self.tables.contains_key(table_name) {
            return Err(DatabaseError::DuplicateTable);
        }

        let my_table = Table::new(table_name, columns, self.file.clone());
        self.tables.insert(table_name.to_string(), my_table);

        Ok(())
    }

    pub fn get_table(&mut self, table_name: &str) -> Result<&mut Table, DatabaseError> {
        if let Some(table) = self.tables.get_mut(table_name) {
            Ok(table)
        } else {
            Err(DatabaseError::TableDoesNotExist)
        }
    }
}
