use nom::{bytes::complete::tag_no_case, character::complete::multispace0, sequence::tuple, IResult};

pub(super) fn parse_select(input: &str) -> IResult<&str, &str> {
    let (_, _) = tuple((tag_no_case("select"), multispace0))(input)?;
    Ok(("", ""))
}
