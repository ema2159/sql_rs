use nom::{
    branch::alt,
    bytes::complete::{tag, tag_no_case},
    character::complete::{alphanumeric1, anychar, char, digit1, multispace0, multispace1},
    combinator::{all_consuming, map_res, opt, recognize, verify},
    multi::{many0_count, separated_list1},
    sequence::{delimited, pair, preceded, separated_pair, tuple},
    IResult,
};

use crate::{ColumnItemType, IntegerType, TextType};

fn parse_identifier(input: &str) -> IResult<&str, &str> {
    recognize(pair(
        verify(anychar, |&c: &char| c.is_alphabetic()),
        many0_count(preceded(opt(char('_')), alphanumeric1)),
    ))(input)
}

fn parse_int_type(input: &str) -> IResult<&str, ColumnItemType> {
    let (remainder, _) = tag("int")(input)?;
    Ok((remainder, ColumnItemType::Integer(IntegerType::Int)))
}

fn parse_text_type(input: &str) -> IResult<&str, ColumnItemType> {
    let (remainder, (_, num_characters)) = tuple((
        tag_no_case("varchar"),
        delimited(
            tag("("),
            map_res(digit1, |s: &str| s.parse::<u8>()),
            tag(")"),
        ),
    ))(input)?;

    Ok((
        remainder,
        ColumnItemType::Text(TextType::Varchar(num_characters)),
    ))
}

fn parse_column_type(input: &str) -> IResult<&str, ColumnItemType> {
    alt((parse_int_type, parse_text_type))(input)
}

fn parse_columns(input: &str) -> IResult<&str, Vec<(&str, ColumnItemType)>> {
    separated_list1(
        tag(","),
        delimited(
            multispace0,
            separated_pair(parse_identifier, multispace1, parse_column_type),
            multispace0,
        ),
    )(input)
}

type TableTokens<'a> = (&'a str, Vec<(&'a str, ColumnItemType)>);
pub(super) fn parse_create(input: &str) -> IResult<&str, TableTokens> {
    let (input, _) = tuple((
        tag_no_case("create"),
        multispace1,
        tag_no_case("table"),
        multispace1,
    ))(input)?;

    let (input, table_name) = parse_identifier(input)?;
    let (input, _) = multispace1(input)?;
    let (input, columns_vec) = delimited(tag("("), parse_columns, tag(")"))(input)?;
    let (_, _) = all_consuming(pair(multispace0, tag(";")))(input)?;

    Ok(("", (table_name, columns_vec)))
}
