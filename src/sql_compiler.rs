use nom::Finish;

mod create;
mod insert;
mod select;
pub mod statement;

use create::*;
pub use insert::*;
use select::*;
pub use statement::*;

pub fn parse_statement(statement_str: &str) -> Result<Statement, ParseError> {
    if let Ok(("", columns)) = parse_create(statement_str).finish() {
        let parsed_statement = Statement::Create(columns);
        Ok(parsed_statement)
    } else if let Ok(("", row_to_insert)) = parse_insert(statement_str).finish() {
        let parsed_statement = Statement::Insert(RowToInsert(row_to_insert));
        Ok(parsed_statement)
    } else if let Ok(("", "")) = parse_select(statement_str).finish() {
        let parsed_statement = Statement::Select;
        Ok(parsed_statement)
    } else {
        Err(ParseError::UnknownStatement)
    }
}
