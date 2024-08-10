use thiserror::Error;

#[derive(Error, Debug)]
pub enum VMError {
    #[error("Cannot perform operation. No database connection open.")]
    DBClosed,
    #[error("Cannot create table {0}. Another table with the same name already exists")]
    DuplicatedTableName(String),
    #[error("Cannot create table. Two columns have the same name: {0}")]
    DuplicatedColumnName(String),
    #[error("Error while writing to table {0}: {1}")]
    TableWriteError(String, String),
    #[error("Error while reading table {0}: {1}")]
    TableReadError(String, String),
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
