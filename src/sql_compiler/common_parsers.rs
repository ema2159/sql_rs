use nom::{
    character::complete::{alphanumeric1, anychar, char},
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
