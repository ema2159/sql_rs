use nom::{bytes::complete::tag_no_case, character::complete::multispace0, sequence::tuple, IResult};

#[derive(Debug)]
pub struct RowToInsert<'a>(pub &'a str);

impl<'a> core::ops::Deref for RowToInsert<'a> {
    type Target = &'a str;

    fn deref(&self) -> &'_ Self::Target {
        &self.0
    }
}

pub(super) fn parse_insert(input: &str) -> IResult<&str, &str> {
    let (row_to_insert, _) = tuple((tag_no_case("insert"), multispace0))(input)?;
    Ok(("", row_to_insert))
}
