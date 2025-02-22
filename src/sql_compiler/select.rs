use nom::{
    bytes::complete::tag_no_case,
    character::complete::{char, multispace0},
    combinator::all_consuming,
    error::{convert_error, VerboseError},
    sequence::{pair, tuple},
    Finish, IResult,
};

use super::parse_identifier;
use super::statement::{ParseError, Statement};

#[derive(Debug)]
pub struct SelectTokens<'a> {
    pub table_name: &'a str,
}

fn parse_select(input: &str) -> IResult<&str, SelectTokens, VerboseError<&str>> {
    let (input, _) = tuple((tag_no_case("select"), multispace0))(input)?;
    let (input, _) = tuple((char('*'), multispace0))(input)?;
    let (input, _) = tuple((tag_no_case("from"), multispace0))(input)?;
    let (input, table_name) = parse_identifier(input)?;
    let (_, _) = all_consuming(pair(multispace0, char(';')))(input)?;
    Ok(("", SelectTokens { table_name }))
}

pub(super) fn validate_select(input: &str) -> Result<Statement, ParseError> {
    match parse_select(input).finish() {
        Err(e) => Err(ParseError::MalformedStatement(convert_error(input, e))),
        Ok((_, select_tokens)) => Ok(Statement::Select(select_tokens)),
    }
}
