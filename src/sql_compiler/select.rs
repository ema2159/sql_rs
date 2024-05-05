use nom::{
    bytes::complete::tag_no_case,
    character::complete::multispace0,
    error::{convert_error, VerboseError},
    sequence::tuple,
    Finish, IResult,
};

use super::statement::{ParseError, Statement};

fn parse_select(input: &str) -> IResult<&str, &str, VerboseError<&str>> {
    let (_, _) = tuple((tag_no_case("select"), multispace0))(input)?;
    Ok(("", ""))
}

pub(super) fn validate_select(input: &str) -> Result<Statement, ParseError> {
    match parse_select(input).finish() {
        Err(e) => Err(ParseError::MalformedStatement(convert_error(input, e))),
        Ok((_, _)) => Ok(Statement::Select),
    }
}
