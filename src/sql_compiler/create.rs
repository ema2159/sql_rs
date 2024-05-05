use nom::{
    branch::alt,
    bytes::complete::{tag, tag_no_case},
    character::complete::{digit1, multispace0, multispace1},
    combinator::{all_consuming, cut, map_res},
    error::{convert_error, VerboseError},
    multi::separated_list1,
    sequence::{delimited, pair, separated_pair, tuple},
    Finish, IResult,
};

use super::parse_identifier;
use super::statement::{ParseError, Statement};
use crate::entities::columns::{ColumnItemType, IntegerType, TextType};

#[derive(Debug)]
pub struct CreateTokens<'a> {
    table_name: &'a str,
    columns: Vec<(&'a str, ColumnItemType)>,
}

fn parse_int_type(input: &str) -> IResult<&str, ColumnItemType, VerboseError<&str>> {
    let (remainder, _) = tag_no_case("int")(input)?;
    Ok((remainder, ColumnItemType::Integer(IntegerType::Int)))
}

fn parse_text_type(input: &str) -> IResult<&str, ColumnItemType, VerboseError<&str>> {
    let (remainder, num_characters) = delimited(
        tag_no_case("varchar("),
        map_res(digit1, |s: &str| s.parse::<u8>()),
        tag(")"),
    )(input)?;

    Ok((
        remainder,
        ColumnItemType::Text(TextType::Varchar(num_characters)),
    ))
}

fn parse_column_type(input: &str) -> IResult<&str, ColumnItemType, VerboseError<&str>> {
    alt((parse_int_type, parse_text_type))(input)
}

fn parse_columns(input: &str) -> IResult<&str, Vec<(&str, ColumnItemType)>, VerboseError<&str>> {
    separated_list1(
        tag(","),
        cut(delimited(
            multispace0,
            separated_pair(parse_identifier, multispace1, parse_column_type),
            multispace0,
        )),
    )(input)
}

fn parse_create(input: &str) -> IResult<&str, CreateTokens, VerboseError<&str>> {
    let (input, _) = tuple((
        multispace0,
        tag_no_case("create"),
        multispace1,
        tag_no_case("table"),
        multispace1,
    ))(input)?;

    let (input, table_name) = parse_identifier(input)?;
    let (input, _) = multispace1(input)?;
    let (input, columns_vec) = delimited(tag("("), parse_columns, tag(")"))(input)?;
    let (_, _) = all_consuming(pair(multispace0, tag(";")))(input)?;

    Ok((
        "",
        CreateTokens {
            table_name,
            columns: columns_vec,
        },
    ))
}

pub(super) fn validate_create(input: &str) -> Result<Statement, ParseError> {
    match parse_create(input).finish() {
        Err(e) => Err(ParseError::MalformedStatement(convert_error(input, e))),
        Ok((_, create_tokens)) => Ok(Statement::Create(create_tokens)),
    }
}
