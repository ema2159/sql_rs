use super::vm_error::VMError;
use crate::backend::database::Database;
use crate::sql_compiler::SelectTokens;

pub(super) fn process_select(
    select_tokens: SelectTokens,
    db_instance: Option<&mut Database>,
) -> Result<(), VMError> {
    let SelectTokens { table_name } = select_tokens;

    let open_database = db_instance.ok_or(VMError::DBClosed)?;

    let table = open_database
        .get_table(table_name)
        .map_err(|err| VMError::TableReadError(table_name.to_string(), err.to_string()))?;

    println!("{:#?}", table.deserialize_rows());

    Ok(())
}
