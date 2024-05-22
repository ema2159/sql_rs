use nom::{
    branch::alt,
    bytes::complete::tag_no_case,
    character::complete::{char, digit1, multispace0, multispace1},
    combinator::{all_consuming, cut},
    error::{convert_error, VerboseError},
    multi::separated_list1,
    sequence::{delimited, pair, tuple},
    Finish, IResult,
};

use super::statement::{ParseError, Statement};
use super::{escaped_string_double_quote, escaped_string_single_quote, parse_identifier};

#[derive(Debug)]
pub struct InsertTokens<'a> {
    pub table_name: &'a str,
    pub column_names: Vec<&'a str>,
    pub column_values: Vec<&'a str>,
}

fn parse_column_names(input: &str) -> IResult<&str, Vec<&str>, VerboseError<&str>> {
    separated_list1(
        char(','),
        cut(delimited(multispace0, parse_identifier, multispace0)),
    )(input)
}

fn parse_value(input: &str) -> IResult<&str, &str, VerboseError<&str>> {
    alt((
        digit1,
        delimited(char('\''), escaped_string_single_quote, char('\'')),
        delimited(char('"'), escaped_string_double_quote, char('"')),
    ))(input)
}

fn parse_column_values(input: &str) -> IResult<&str, Vec<&str>, VerboseError<&str>> {
    separated_list1(
        char(','),
        cut(delimited(multispace0, parse_value, multispace0)),
    )(input)
}

fn parse_insert(input: &str) -> IResult<&str, InsertTokens, VerboseError<&str>> {
    let (input, _) = tuple((
        multispace0,
        tag_no_case("insert"),
        multispace1,
        tag_no_case("into"),
        multispace1,
    ))(input)?;

    let (input, table_name) = parse_identifier(input)?;
    let (input, _) = multispace0(input)?;
    let (input, column_names) = delimited(char('('), parse_column_names, char(')'))(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("values")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, column_values) = delimited(char('('), parse_column_values, char(')'))(input)?;
    let (_, _) = all_consuming(pair(multispace0, char(';')))(input)?;

    Ok((
        "",
        InsertTokens {
            table_name,
            column_names,
            column_values,
        },
    ))
}

pub(super) fn validate_insert(input: &str) -> Result<Statement, ParseError> {
    match parse_insert(input).finish() {
        Err(e) => Err(ParseError::MalformedStatement(convert_error(input, e))),
        Ok((_, insert_tokens)) => Ok(Statement::Insert(insert_tokens)),
    }
}
