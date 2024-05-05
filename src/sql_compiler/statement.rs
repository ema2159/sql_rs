use super::insert::RowToInsert;
use crate::ColumnItemType;

#[derive(Debug)]
pub enum Statement<'a> {
    Create((&'a str, Vec<(&'a str, ColumnItemType)>)),
    Insert(RowToInsert<'a>),
    Select,
}

pub enum ParseError {
    UnknownStatement,
}
