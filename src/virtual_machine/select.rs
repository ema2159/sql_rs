use tabled::{
    builder::Builder,
    settings::style::Style,
};

use super::vm_error::VMError;
use crate::backend::database::Database;
use crate::backend::table::Table;
use crate::sql_compiler::SelectTokens;

fn print_table(table: &Table) {
    let columns = &table.columns;
    let rows = table.deserialize_rows().unwrap();

    let rows_strings: Vec<_> = rows.iter().map(|row| row.to_printable()).collect();

    let mut pretty_table_builder = Builder::from(rows_strings);
    pretty_table_builder.insert_record(0, columns.to_printable());

    let mut pretty_table = pretty_table_builder.build();
    pretty_table.with(Style::psql());

    println!("{}", pretty_table);
}

pub(super) fn process_select(
    select_tokens: SelectTokens,
    db_instance: Option<&mut Database>,
) -> Result<(), VMError> {
    let SelectTokens { table_name } = select_tokens;

    let open_database = db_instance.ok_or(VMError::DBClosed)?;

    let table = open_database
        .get_table(table_name)
        .map_err(|err| VMError::TableReadError(table_name.to_string(), err.to_string()))?;

    print_table(&table);

    Ok(())
}
