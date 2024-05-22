use thiserror::Error;

use crate::sql_compiler::{CreateTokens, InsertTokens, Statement};

use crate::entities::columns::Columns;
use crate::entities::table::Table;

#[derive(Error, Debug)]
pub enum ExecuteError {
    #[error("Cannot create table {0}. Another table with the same name already exists")]
    DuplicatedTableName(String),
    #[error("Cannot create table. Two columns have the same name: {0}")]
    DuplicatedColumnName(String),
    #[error("Error while writing table to disk")]
    TableWriteError,
}

fn process_insert(insert_tokens: InsertTokens) -> Result<(), ExecuteError> {
    println!("Insert tokens: {:?}", insert_tokens);
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
        Statement::Create(createinsert_tokenstokens) => process_create(createinsert_tokenstokens),
        Statement::Insert(insert_tokens) => process_insert(insert_tokens),
        Statement::Select => {
            println!("This is where we would do an select");
            Ok(())
        }
    }
}
