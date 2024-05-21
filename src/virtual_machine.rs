use core::fmt::Display;
use std::collections::BTreeMap;

use crate::sql_compiler::{CreateTokens, RowToInsert, Statement};

use crate::entities::columns::Columns;
use crate::entities::table::Table;

fn process_insert(row_to_insert: RowToInsert) -> Result<(), ExecuteError> {
    println!("This is the row to insert: {}", *row_to_insert);
    Ok(())
}

fn process_create(create_tokens: CreateTokens) -> Result<(), ExecuteError> {
    let CreateTokens {
        table_name,
        columns: columns_to_insert,
    } = create_tokens;

    // if tables.contains_key(table_name) {
    // return Err(ExecuteError::DuplicatedTableName(table_name.to_string()));
    // };

    let mut columns = Columns::new();

    for (column_name, column_type) in columns_to_insert.into_iter() {
        if columns
            .insert(column_name.to_string(), column_type)
            .is_some()
        {
            return Err(ExecuteError::DuplicatedColumnName(column_name.to_string()));
        }
    }

    let new_table = Table::new(table_name, columns);

    new_table
        .save_to_disk(table_name)
        .map_err(|_| ExecuteError::TableWriteError)?;

    println!("{:?}", Table::read_from_disk(table_name));

    Ok(())
}

pub fn execute_statement(statement: Statement) -> Result<(), ExecuteError> {
    match statement {
        Statement::Create(columns) => process_create(columns),
        Statement::Insert(row_to_insert) => process_insert(row_to_insert),
        Statement::Select => {
            println!("This is where we would do an select");
            Ok(())
        }
    }
}

#[derive(Debug)]
pub enum ExecuteError {
    DuplicatedTableName(String),
    DuplicatedColumnName(String),
    TableWriteError,
}

impl Display for ExecuteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExecuteError::DuplicatedTableName(table_name) => write!(
                f,
                "Cannot create table {}. Another table with the same name already exists",
                table_name
            ),
            ExecuteError::DuplicatedColumnName(column_name) => write!(
                f,
                "Cannot create table. Two columns have the same name: {}",
                column_name
            ),
            ExecuteError::TableWriteError => write!(f, "Error while writing table to disk"),
        }
    }
}

impl std::error::Error for ExecuteError {}
