use tracing::instrument;

use super::vm_error::VMError;
use crate::backend::columns::{ColumnItemType, ColumnType, Columns};
use crate::backend::database::Database;
use crate::backend::row::{Row, SQLType};
use crate::sql_compiler::InsertTokens;

#[instrument(parent = None, ret, level = "trace")]
fn parse_value(input: &str, column_type: &ColumnItemType) -> Option<SQLType> {
    match column_type {
        ColumnItemType::Integer(int_type) => int_type.validate(input),

        ColumnItemType::Text(text_type) => text_type.validate(input),
    }
}

#[instrument(parent = None, ret, level = "trace")]
fn parse_values(
    columns: &Columns,
    items_to_add: &mut Vec<(&str, &str)>,
) -> Result<(u64, Vec<SQLType>), VMError> {
    let mut parsed_values = Vec::<SQLType>::new();

    let mut id_optn = None;

    for (name, value) in items_to_add {
        let column_item_type = columns
            .get(*name)
            .ok_or(VMError::ColumnNotInTable(name.to_string()))?;
        if let Some(parsed_value) = parse_value(value, column_item_type) {
            // NOTE: Harcoding ID-related stuff. This should change
            if *name == "id" {
                if let SQLType::UBigInt(val) = parsed_value {
                    id_optn = Some(val);
                }
            }
            parsed_values.push(parsed_value);
        } else {
            return Err(VMError::ItemParsingError(value.to_string()));
        }
    }

    if let Some(id) = id_optn {
        Ok((id, parsed_values))
    } else {
        Err(VMError::NoIdParsed)
    }
}

#[instrument(parent = None, ret, level = "trace")]
fn order_and_check_dup(items_to_add: &mut Vec<(&str, &str)>) -> Result<(), VMError> {
    // Sort elements to be added in a predictable way
    items_to_add.sort_unstable_by_key(|item| item.0);
    // If there are duplicate keys, return error immediately
    let items_len_before_dedup = items_to_add.len();
    items_to_add.dedup_by_key(|item| item.0);
    if items_to_add.len() != items_len_before_dedup {
        Err(VMError::DuplicateColumns)
    } else {
        Ok(())
    }
}

#[instrument(parent = None, ret, skip(db_instance), level = "trace")]
pub(super) fn process_insert(
    insert_tokens: InsertTokens,
    db_instance: Option<&mut Database>,
) -> Result<(), VMError> {
    let open_database = db_instance.ok_or(VMError::DBClosed)?;

    let InsertTokens {
        table_name,
        column_names,
        column_values,
    } = insert_tokens;

    let (names_len, values_len) = (column_names.len(), column_values.len());

    if names_len != values_len {
        return Err(VMError::ColumnNamesValuesMismatch(names_len, values_len));
    }

    let mut items_to_add: Vec<(&str, &str)> = column_names.into_iter().zip(column_values).collect();

    order_and_check_dup(&mut items_to_add)?;

    let table = open_database
        .get_table(table_name)
        .map_err(|err| VMError::TableWriteError(table_name.to_string(), err.to_string()))?;

    let columns = &table.columns;

    let (id, values) = parse_values(columns, &mut items_to_add)?;
    let row_to_insert = Row::new(id, values);

    table.insert(row_to_insert)?;

    // println!("{:?}", table);
    // println!("{:?}", table.deserialize_rows());

    Ok(())
}
