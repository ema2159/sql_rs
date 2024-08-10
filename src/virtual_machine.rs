use crate::backend::database::Database;
use crate::sql_compiler::Statement;

mod create;
mod insert;
mod select;
mod vm_error;

use create::process_create;
use insert::process_insert;
use select::process_select;
use vm_error::VMError;

pub fn execute_statement(
    statement: Statement,
    db_instance: Option<&mut Database>,
) -> Result<(), VMError> {
    match statement {
        Statement::Create(create_tokens) => process_create(create_tokens, db_instance),
        Statement::Insert(insert_tokens) => process_insert(insert_tokens, db_instance),
        Statement::Select(select_tokens) => process_select(select_tokens, db_instance),
    }
}
