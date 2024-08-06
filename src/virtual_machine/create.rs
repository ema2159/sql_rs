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

    Table::create(table_name, columns);

    Ok(())
}
