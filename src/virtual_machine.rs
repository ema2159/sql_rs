use crate::sql_compiler::{Statement, StatementContents, StatementType};

fn process_insert(insert_contents: StatementContents) {
    if let StatementContents::Insert(row_to_insert) = insert_contents {
        println!("This is the row to insert: {}", *row_to_insert)
    }
}

pub fn execute_statement(statement: Statement) {
    match statement.statement_type {
        StatementType::Insert => process_insert(statement.statement_contents),
        StatementType::Select => println!("This is where we would do an select"),
    }
}
