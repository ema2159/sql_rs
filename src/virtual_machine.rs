use crate::sql_compiler::Statement;

mod create;
mod insert;
mod vm_error;

use create::process_create;
use insert::process_insert;
use vm_error::VMError;

pub fn execute_statement(statement: Statement) -> Result<(), VMError> {
    match statement {
        Statement::Create(createinsert_tokenstokens) => process_create(createinsert_tokenstokens),
        Statement::Insert(insert_tokens) => process_insert(insert_tokens),
        Statement::Select => {
            println!("This is where we would do an select");
            Ok(())
        }
    }
}
