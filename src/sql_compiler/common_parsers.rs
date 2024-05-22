use nom::{
    bytes::complete::escaped,
    character::complete::{alphanumeric1, anychar, char, none_of, one_of},
    combinator::{opt, recognize, verify},
    error::VerboseError,
    multi::many0_count,
    sequence::{pair, preceded},
    IResult,
};

pub(super) fn parse_identifier(input: &str) -> IResult<&str, &str, VerboseError<&str>> {
    recognize(pair(
        verify(anychar, |&c: &char| c.is_alphabetic()),
        many0_count(preceded(opt(char('_')), alphanumeric1)),
    ))(input)
}

pub(super) fn escaped_string_single_quote(input: &str) -> IResult<&str, &str, VerboseError<&str>> {
    escaped(none_of("\\\'"), '\\', one_of(r#""n\'"#))(input)
}

pub(super) fn escaped_string_double_quote(input: &str) -> IResult<&str, &str, VerboseError<&str>> {
    escaped(none_of("\\\""), '\\', one_of(r#""n\'"#))(input)
}
