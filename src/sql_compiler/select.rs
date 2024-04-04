use nom::{bytes::complete::tag, character::complete::multispace0, sequence::tuple, IResult};

pub(super) fn parse_select(input: &str) -> IResult<&str, &str> {
    let (_, _) = tuple((tag("select"), multispace0))(input)?;
    Ok(("", ""))
}
