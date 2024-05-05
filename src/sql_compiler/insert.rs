use nom::{
    bytes::complete::tag_no_case,
    character::complete::multispace0,
    error::{convert_error, VerboseError},
    sequence::tuple,
    Finish, IResult,
};

use super::statement::{ParseError, Statement};

#[derive(Debug)]
pub struct RowToInsert<'a>(pub &'a str);

impl<'a> core::ops::Deref for RowToInsert<'a> {
    type Target = &'a str;

    fn deref(&self) -> &'_ Self::Target {
        &self.0
    }
}

fn parse_insert(input: &str) -> IResult<&str, &str, VerboseError<&str>> {
    let (row_to_insert, _) = tuple((tag_no_case("insert"), multispace0))(input)?;
    Ok(("", row_to_insert))
}

pub(super) fn validate_insert(input: &str) -> Result<Statement, ParseError> {
    match parse_insert(input).finish() {
        Err(e) => Err(ParseError::MalformedStatement(convert_error(input, e))),
        Ok((_, row_to_insert)) => Ok(Statement::Insert(RowToInsert(row_to_insert))),
    }
}
