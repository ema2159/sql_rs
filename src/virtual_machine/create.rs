use super::vm_error::VMError;
use crate::backend::columns::Columns;
use crate::backend::database::{Database, DatabaseError};
use crate::sql_compiler::CreateTokens;

pub(super) fn process_create (
    create_tokens: CreateTokens,
    db_instance: Option<&mut Database>,
) -> Result<(), VMError> {
    let CreateTokens {
        table_name,
        columns: columns_to_insert,
    } = create_tokens;

    let db_instance = db_instance.ok_or(VMError::DBClosed)?;

    let mut columns = Columns::new();

    for (column_name, column_type) in columns_to_insert.into_iter() {
        if columns
            .insert(column_name.to_string(), column_type)
            .is_some()
        {
            return Err(VMError::DuplicatedColumnName(column_name.to_string()));
        }
    }

    db_instance
        .add_table(table_name, columns)
        .map_err(|err| match err {
            DatabaseError::DuplicateTable => VMError::DuplicatedTableName(table_name.to_string()),
            _ => unreachable!(),
        })?;

    Ok(())
}
