use thiserror::Error;

#[derive(Error, Debug)]
pub enum VMError {
    #[error("Cannot create table {0}. Another table with the same name already exists")]
    DuplicatedTableName(String),
    #[error("Cannot create table. Two columns have the same name: {0}")]
    DuplicatedColumnName(String),
    #[error("Error while writing table to disk: {0}")]
    TableWriteError(String),
    #[error("Error while reading table from disk: {0}")]
    TableReadError(String),
    #[error("Mismatch between number of column names ({0}) and number of values passed ({1})")]
    ColumnNamesValuesMismatch(usize, usize),
    #[error("Column {0} not in table")]
    ColumnNotInTable(String),
    #[error("Duplcate columns in insert statement")]
    DuplicateColumns,
    #[error("Error when parsing value {0}")]
    ItemParsingError(String),
    #[error("Error when inserting row into table: {0}")]
    ItemInsertingError(String),
}

