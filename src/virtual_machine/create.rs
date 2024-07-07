use super::vm_error::VMError;
use crate::backend::columns::Columns;
use crate::backend::table::Table;
use crate::sql_compiler::CreateTokens;

pub(super) fn process_create(create_tokens: CreateTokens) -> Result<(), VMError> {
    let CreateTokens {
        table_name,
        columns: columns_to_insert,
    } = create_tokens;

    // if tables.contains_key(table_name) {
    // return Err(VMError::DuplicatedTableName(table_name.to_string()));
    // };

    let mut columns = Columns::new();

    for (column_name, column_type) in columns_to_insert.into_iter() {
        if columns
            .insert(column_name.to_string(), column_type)
            .is_some()
        {
            return Err(VMError::DuplicatedColumnName(column_name.to_string()));
        }
    }

    let new_table = Table::new(table_name, columns);

    let db_name = table_name.to_owned() + ".db";
    new_table
        .save_to_disk(&db_name)
        .map_err(|table_error| VMError::TableWriteError(table_error.to_string()))?;

    Ok(())
}
