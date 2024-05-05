use crate::sql_compiler::{RowToInsert, Statement, CreateTokens};

fn process_insert(row_to_insert: RowToInsert) {
    println!("This is the row to insert: {}", *row_to_insert)
}

fn process_create(table_tokens: CreateTokens) {
    println!("A table with the following properties will be created: {:?}", table_tokens)
}

pub fn execute_statement(statement: Statement) {
    match statement {
        Statement::Create(columns) => process_create(columns),
        Statement::Insert(row_to_insert) => process_insert(row_to_insert),
        Statement::Select => println!("This is where we would do an select"),
    }
}
