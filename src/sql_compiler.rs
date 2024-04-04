use nom::Finish;

mod insert;
mod select;
pub mod statement;

use insert::*;
use select::*;
pub use statement::*;

pub fn parse_statement(statement_str: &str) -> Result<Statement, ParseError> {
    if let Ok(("", row_to_insert)) = parse_insert(statement_str).finish() {
        let parsed_statement = Statement {
            statement_type: StatementType::Insert,
            statement_contents: StatementContents::Insert(RowToInsert(row_to_insert)),
        };
        Ok(parsed_statement)
    } else if let Ok(("", "")) = parse_select(statement_str).finish() {
        let parsed_statement = Statement {
            statement_type: StatementType::Select,
            statement_contents: StatementContents::Select,
        };
        Ok(parsed_statement)
    } else {
        Err(ParseError::UnknownStatement)
    }
}
