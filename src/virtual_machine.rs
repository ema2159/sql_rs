use crate::sql_compiler::{Statement, StatementType};

pub fn execute_statement(statement: Statement) {
    match statement.statement_type {
        StatementType::Insert => println!("This is where we would do an insert"),
        StatementType::Select => println!("This is where we would do an select"),
    }
}
