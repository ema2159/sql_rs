use crate::backend::database::Database;
use crate::sql_compiler::Statement;

mod create;
mod insert;
mod vm_error;

use create::process_create;
use insert::process_insert;
use vm_error::VMError;

pub fn execute_statement(
    statement: Statement,
    db_instance: Option<&mut Database>,
) -> Result<(), VMError> {
    match statement {
        Statement::Create(create_tokens) => process_create(create_tokens, db_instance),
        Statement::Insert(insert_tokens) => process_insert(insert_tokens, db_instance),
        Statement::Select => {
            println!("This is where we would do an select");
            Ok(())
        }
    }
}
