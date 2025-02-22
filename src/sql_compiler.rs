use nom::{
    branch::alt, bytes::complete::tag_no_case, character::complete::multispace0,
    combinator::map_res, error::VerboseError, IResult,
};

mod common_parsers;
mod create;
mod insert;
mod select;
pub mod statement;

use common_parsers::*;
pub use create::*;
pub use insert::*;
pub use select::*;
pub use statement::*;

fn parse_statement_type(statement_str: &str) -> IResult<&str, StatementType, VerboseError<&str>> {
    let (statement_str, _) = multispace0(statement_str)?;

    map_res(
        alt((
            tag_no_case("create"),
            tag_no_case("insert"),
            tag_no_case("select"),
        )),
        |s: &str| StatementType::try_from(s),
    )(statement_str)
}

pub fn parse_statement(statement_str: &str) -> Result<Statement, ParseError> {
    if let Ok((_, statement_type)) = parse_statement_type(statement_str) {
        match statement_type {
            StatementType::Create => validate_create(statement_str),
            StatementType::Insert => validate_insert(statement_str),
            StatementType::Select => validate_select(statement_str),
        }
    } else {
        Err(ParseError::UnknownStatement)
    }
}
